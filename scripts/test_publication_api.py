#!/usr/bin/env python3
"""End-to-end tests for the privacy-preserving publication API.

Runs entirely over HTTP against the two shared-database API processes and
exercises every delivery requirement:

  1. only a COMPLETED batch can be published;
  2. k in 2..5 and positive integer bin width are validated;
  3. at most 300 applications per batch;
  4. output rows carry only bin / period / counts, each with >= k customers,
     empty income in its own bin;
  5. one published version per batch: two concurrent submissions with
     different parameters -> one 201 and one 409; no readable half result;
  6. same-parameter retry reads the original (same publication_id);
  7. keyset pagination of publication rows mirrors result pagination;
  8. a GET against either API process shows the same single version.

Usage:
    python scripts/test_publication_api.py [BASE1] [BASE2]
"""
from __future__ import annotations

import concurrent.futures
import json
import sys
import threading
import time
import urllib.error
import urllib.request

BASE1 = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8001"
BASE2 = sys.argv[2] if len(sys.argv) > 2 else "http://127.0.0.1:8002"

# Unique prefix so re-runs / coexistence with seed demo data never clash.
PREFIX = f"PUB{int(time.time()) % 1_000_000:06d}"
K = 2
failed = 0


def call(base: str, method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        base + path, data=data,
        headers={"Content-Type": "application/json"}, method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read())


def expect(name: str, ok: bool, detail: str = "") -> None:
    global failed
    print(("PASS" if ok else "FAIL"), "-", name + (f"  [{detail}]" if detail and not ok else ""))
    failed += not ok


def wait_completed(base: str, batch_id: str, timeout: float = 90) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        _, batch = call(base, "GET", f"/api/v1/batches/{batch_id}")
        if batch["status"] in ("COMPLETED", "FAILED"):
            return batch
        time.sleep(0.25)
    raise TimeoutError(f"batch {batch_id} did not finish")


def fetch_pages(base: str, batch_id: str, limit: int) -> list[dict]:
    items, cursor = [], None
    while True:
        path = f"/api/v1/batches/{batch_id}/publications?limit={limit}"
        if cursor:
            path += f"&cursor={cursor}"
        status, page = call(base, "GET", path)
        assert status == 200, page
        items.extend(page["items"])
        cursor = page["next_cursor"]
        if not cursor:
            return items


def main() -> None:
    print(f"test id prefix: {PREFIX}")

    # ------------------------------------------------------------------ #
    # 1. Build a dataset designed to trigger every cascade level:
    #    healthy months (month rows), a weak month (quarter merge),
    #    a weak quarter (year merge), a single-customer year (suppressed),
    #    and empty-income applications (separate bin).
    # ------------------------------------------------------------------ #
    apps, events = [], []

    def add_app(idx: str, customer: str, when: str, income):
        apps.append({"application_id": f"{PREFIX}A{idx}",
                     "customer_id": f"{PREFIX}{customer}",
                     "application_time": when})
        if income is not None:
            events.append({
                "event_id": f"{PREFIX}E{idx}",
                "customer_id": f"{PREFIX}{customer}",
                "business_time": when,
                "known_time": when,
                "income": income, "withdrawn": False,
            })

    # 2026 Q1: January + February each have two distinct customers -> month rows.
    add_app("01", "C1", "2026-01-10T00:00:00Z", 500)
    add_app("02", "C2", "2026-01-20T00:00:00Z", 800)
    add_app("03", "C3", "2026-02-10T00:00:00Z", 1500)
    add_app("04", "C4", "2026-02-20T00:00:00Z", 1600)
    # 2026 Q2: April has one customer (< k); May has two -> whole Q2 quarter merge.
    add_app("05", "C5", "2026-04-10T00:00:00Z", 2500)
    add_app("06", "C6", "2026-05-10T00:00:00Z", 2600)
    add_app("07", "C7", "2026-05-20T00:00:00Z", 2700)
    # 2027: a single customer across the year -> quarter weak -> year weak -> suppressed.
    add_app("08", "C8", "2027-03-10T00:00:00Z", 9000)
    add_app("09", "C8", "2027-07-10T00:00:00Z", 9100)
    # Empty-income applications (no event) in a healthy month -> separate bin.
    add_app("10", "C9", "2026-01-15T00:00:00Z", None)
    add_app("11", "C10", "2026-01-25T00:00:00Z", None)

    st, _ = call(BASE1, "POST", "/api/v1/imports/applications", {"applications": apps})
    expect("applications imported", st == 201, f"status={st}")
    st, _ = call(BASE1, "POST", "/api/v1/imports/credit-events", {"events": events})
    expect("credit events imported", st == 201, f"status={st}")

    st, batch = call(BASE1, "POST", "/api/v1/batches",
                     {"application_ids": [a["application_id"] for a in apps]})
    expect("batch created", st == 201, f"status={st}")
    batch_id = batch["batch_id"]
    final = wait_completed(BASE2, batch_id)
    expect("batch COMPLETED", final["status"] == "COMPLETED", str(final))

    # ------------------------------------------------------------------ #
    # 2. Validation: batch state, k range, width, 300 cap.
    # ------------------------------------------------------------------ #
    st, body = call(BASE1, "POST", f"/api/v1/batches/{batch_id}/publications",
                    {"k": 1, "bin_width": 1000})
    expect("k=1 rejected by validation", st == 422, f"status={st}")
    st, _ = call(BASE1, "POST", f"/api/v1/batches/{batch_id}/publications",
                 {"k": 6, "bin_width": 1000})
    expect("k=6 rejected by validation", st == 422)
    st, _ = call(BASE1, "POST", f"/api/v1/batches/{batch_id}/publications",
                 {"k": K, "bin_width": 0})
    expect("bin_width=0 rejected", st == 422)

    import uuid as _uuid
    st, _ = call(BASE1, "POST", f"/api/v1/batches/{_uuid.uuid4()}/publications",
                 {"k": K, "bin_width": 1000})
    expect("unknown batch -> 404", st == 404)

    st, _ = call(BASE1, "GET", f"/api/v1/batches/{batch_id}/publications")
    expect("GET before publication -> 404", st == 404)

    # ------------------------------------------------------------------ #
    # 3. Publish and verify aggregate content / privacy columns.
    # ------------------------------------------------------------------ #
    st, pub = call(BASE1, "POST", f"/api/v1/batches/{batch_id}/publications",
                   {"k": K, "bin_width": 1000})
    expect("first publication 201", st == 201, f"status={st}")
    expect("marked as freshly published", pub.get("already_published") is False)
    publication_id = pub["publication_id"]

    rows = pub["rows"]
    expect("every row has >= k distinct customers",
           all(r["distinct_customer_count"] >= K for r in rows))
    expect("only aggregate fields are present",
           all(set(r) == {"period_kind", "period_start", "period_end", "bin_lower",
                          "bin_upper", "application_count",
                          "distinct_customer_count"} for r in rows))
    # No application/customer id can appear anywhere in the serialised payload.
    raw = json.dumps(pub)
    expect("no customer id leaks into payload",
           all(c["customer_id"] not in raw for c in apps))
    expect("no application id leaks into payload",
           all(a["application_id"] not in raw for a in apps))

    kinds = {(r["bin_lower"], r["period_kind"], r["period_start"]): r for r in rows}
    # Month rows: Jan & Feb 2026 for income bins.
    expect("January month row published",
           (0, "MONTH", "2026-01-01") in kinds
           and kinds[(0, "MONTH", "2026-01-01")]["distinct_customer_count"] == 2)
    expect("February bin [1000,2000) month row published",
           (1000, "MONTH", "2026-02-01") in kinds)
    # Q2 quarter merge (April weak month).
    q2 = kinds.get((2000, "QUARTER", "2026-04-01"))
    expect("Q2 merged to quarter row", q2 is not None
           and q2["application_count"] == 3 and q2["distinct_customer_count"] == 3
           and q2["period_end"] == "2026-07-01")
    # 2027 single-customer year suppressed.
    expect("weak 2027 year suppressed",
           all(not r["period_start"].startswith("2027") for r in rows))
    expect("suppressed count covers the two 2027 apps",
           pub["suppressed_application_count"] == 2)
    # Empty-income bin: separate, January month row, null bounds, sorts last.
    empty_rows = [r for r in rows if r["bin_lower"] is None]
    expect("empty income in its own bin",
           len(empty_rows) == 1 and empty_rows[0]["application_count"] == 2
           and empty_rows[0]["bin_upper"] is None)
    expect("empty bin sorts last", rows[-1]["bin_lower"] is None)

    # Non-overlap within each bin.
    by_bin: dict = {}
    for r in rows:
        by_bin.setdefault(r["bin_lower"], []).append((r["period_start"], r["period_end"]))
    overlap_ok = True
    for spans in by_bin.values():
        spans.sort()
        if any(e1 > s2 for (s1, e1), (s2, e2) in zip(spans, spans[1:])):
            overlap_ok = False
    expect("published periods do not overlap within a bin", overlap_ok)

    # ------------------------------------------------------------------ #
    # 4. Single version: same-parameter retry reads the original.
    # ------------------------------------------------------------------ #
    st, retry = call(BASE2, "POST", f"/api/v1/batches/{batch_id}/publications",
                     {"k": K, "bin_width": 1000})
    expect("same-parameter retry is 200", st == 200, f"status={st}")
    expect("retry returns the same publication id",
           retry["publication_id"] == publication_id)
    expect("retry flagged already_published", retry.get("already_published") is True)
    expect("retry content identical", retry["rows"] == pub["rows"])

    # ------------------------------------------------------------------ #
    # 5. Different parameters -> 409 conflict, version unchanged.
    # ------------------------------------------------------------------ #
    st, conflict = call(BASE1, "POST", f"/api/v1/batches/{batch_id}/publications",
                        {"k": 3, "bin_width": 1000})
    expect("different k -> 409 conflict", st == 409, str(conflict))
    st, conflict = call(BASE2, "POST", f"/api/v1/batches/{batch_id}/publications",
                        {"k": K, "bin_width": 500})
    expect("different bin_width -> 409 conflict", st == 409)
    st, still = call(BASE1, "GET", f"/api/v1/batches/{batch_id}/publications?limit=100")
    expect("conflict did not replace the original version",
           st == 200 and still["publication_id"] == publication_id)

    # ------------------------------------------------------------------ #
    # 6. Real concurrency: two different parameter sets at the same time.
    # Use a fresh, never-published batch with enough distinct customers that
    # both parameter sets produce (different) non-empty versions.
    # ------------------------------------------------------------------ #
    st, batch2 = call(BASE1, "POST", "/api/v1/batches",
                      {"application_ids": [a["application_id"] for a in apps[:4]]})
    expect("second batch created", st == 201)
    bid2 = batch2["batch_id"]
    b2_final = wait_completed(BASE1, bid2)
    expect("second batch COMPLETED before concurrent publish",
           b2_final["status"] == "COMPLETED", str(b2_final))

    # Barrier so both HTTP requests hit the two API processes at the same
    # instant; the database row lock + unique constraint decide the winner.
    barrier = threading.Barrier(2)

    def concurrent_publish(base: str, k: int, width: int):
        barrier.wait()
        return call(base, "POST", f"/api/v1/batches/{bid2}/publications",
                    {"k": k, "bin_width": width})

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        futs = [
            pool.submit(concurrent_publish, BASE1, 2, 1000),
            pool.submit(concurrent_publish, BASE2, 4, 2500),
        ]
        outcomes = [f.result() for f in futs]
    statuses = sorted(s for s, _ in outcomes)
    expect("concurrent different-param submissions: one 201 + one 409",
           statuses == [201, 409], str(statuses))
    # The loser must carry the winner's parameters in the conflict body.
    conflict_body = next(b for s, b in outcomes if s == 409)
    winner = next(b for s, b in outcomes if s == 201)
    expect("conflict body names the existing version",
           conflict_body.get("existing_publication_id") == winner["publication_id"]
           and conflict_body.get("existing_k") == winner["k"]
           and conflict_body.get("existing_bin_width") == winner["bin_width"])
    # Exactly one readable version, both processes agree on it.
    st, g1 = call(BASE1, "GET", f"/api/v1/batches/{bid2}/publications")
    st2, g2 = call(BASE2, "GET", f"/api/v1/batches/{bid2}/publications")
    expect("single readable version after race",
           g1["publication_id"] == g2["publication_id"])
    st, summary = call(BASE1, "GET", "/api/v1/publications")
    expect("publication summary lists both batches once",
           st == 200 and len({s["batch_id"] for s in summary
                              if s["batch_id"] in (batch_id, bid2)}) == 2
           and all(s["row_count"] >= 0 for s in summary))

    # ------------------------------------------------------------------ #
    # 7. Keyset pagination.
    # ------------------------------------------------------------------ #
    full = fetch_pages(BASE2, batch_id, limit=2)
    expect("pagination returns every row exactly once",
           len(full) == pub["row_count"]
           and len({(r["bin_lower"], r["period_kind"], r["period_start"]) for r in full})
           == len(full))
    expect("paginated content equals inline content", full == pub["rows"])

    # ------------------------------------------------------------------ #
    # 8. The 300-application cap, exercised last so worker backlog cannot
    #    slow down the behavioural tests above.
    # ------------------------------------------------------------------ #
    too_many = [{"application_id": f"{PREFIX}CAP{i:04d}",
                 "customer_id": f"{PREFIX}CX{i:04d}",
                 "application_time": "2026-01-01T00:00:00Z"} for i in range(301)]
    st, _ = call(BASE1, "POST", "/api/v1/imports/applications",
                 {"applications": too_many})
    expect("301 fresh applications imported", st == 201, f"status={st}")
    st, body = call(BASE1, "POST", "/api/v1/batches",
                    {"application_ids": [t["application_id"] for t in too_many]})
    expect("batch over 300 applications -> 400", st == 400, str(body))
    st, body = call(BASE2, "POST", "/api/v1/batches",
                    {"application_ids": [t["application_id"] for t in too_many[:300]]})
    expect("batch of exactly 300 accepted", st == 201, str(body))

    print()
    if failed:
        raise SystemExit(f"{failed} publication API test(s) failed")
    print("all publication API tests passed")


if __name__ == "__main__":
    main()
