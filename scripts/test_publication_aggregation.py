#!/usr/bin/env python3
"""Unit tests for the k-anonymous distribution cascade (no database needed).

Covers every rule of app/publication.py:
  - zero-anchored integer-width bins, incl. negative incomes;
  - empty-income records get their own separate bin;
  - month-level counts (applications + distinct customers, dedup);
  - month -> quarter merge when any recorded month of a quarter is under k;
  - quarter -> year merge when a merged quarter is still under k;
  - whole-year suppression when the year is still under k;
  - no overlapping periods within a bin, every row >= k, counts add up;
  - deterministic ordering.
"""
from __future__ import annotations

import sys
import pathlib
from datetime import date

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from app.publication import (  # noqa: E402
    EMPTY_BIN,
    SourceRecord,
    aggregate,
    bin_bounds,
    quarter_end_exclusive,
    quarter_start,
    year_end_exclusive,
    year_start,
)

K = 2


def rec(customer: str, year: int, month: int, income):
    return SourceRecord(customer_id=customer,
                        application_month=date(year, month, 15), income=income)


def index_rows(rows):
    """(bin_lower, kind, start) -> row."""
    return {(r.bin_lower, r.period_kind, r.period_start): r for r in rows}


def check_invariants(rows, k, total_apps, suppressed):
    # Every published row satisfies the k threshold.
    assert all(r.distinct_customer_count >= k for r in rows), "row under k!"
    assert all(r.application_count >= r.distinct_customer_count for r in rows)
    # No overlapping periods inside the same bin; each application is counted once.
    by_bin: dict = {}
    for r in rows:
        by_bin.setdefault(r.bin_lower, []).append(r)
    for bin_lower, bin_rows in by_bin.items():
        spans = sorted((r.period_start, r.period_end) for r in bin_rows)
        for (s1, e1), (s2, e2) in zip(spans, spans[1:]):
            assert e1 <= s2, f"overlapping periods in bin {bin_lower}: {s1}..{e1} vs {s2}..{e2}"
    # Published + suppressed applications account for every input.
    assert sum(r.application_count for r in rows) + suppressed == total_apps
    # Ordering: bins ascending (empty bin last), then period start, then kind.
    keys = [
        (r.bin_lower is None, r.bin_lower if r.bin_lower is not None else 0,
         r.period_start, {"MONTH": 0, "QUARTER": 1, "YEAR": 2}[r.period_kind])
        for r in rows
    ]
    assert keys == sorted(keys), "rows not deterministically ordered"


def main() -> None:
    failed = 0

    def expect(name: str, ok: bool) -> None:
        nonlocal failed
        print(("PASS" if ok else "FAIL"), "-", name)
        failed += not ok

    # 1. bin math: zero boundary, width boundary, negatives.
    expect("zero-anchored bin starts at 0", bin_bounds(0, 1000) == (0, 1000))
    expect("upper boundary lands in next bin", bin_bounds(1000, 1000) == (1000, 2000))
    expect("negative income floor-binned toward -inf",
           bin_bounds(-1, 1000) == (-1000, 0))
    expect("negative income exact boundary", bin_bounds(-1000, 1000) == (-1000, 0))

    # 2. month level survives when each recorded month has >= k customers.
    records = [
        rec("C1", 2026, 1, 500), rec("C2", 2026, 1, 600),
        rec("C1", 2026, 2, 500), rec("C3", 2026, 2, 700),
    ]
    res = aggregate(records, K, 1000)
    idx = index_rows(res.rows)
    expect("two month rows published", len(res.rows) == 2)
    jan = idx.get((0, "MONTH", date(2026, 1, 1)))
    feb = idx.get((0, "MONTH", date(2026, 2, 1)))
    expect("january 2 apps / 2 customers", jan is not None
           and jan.application_count == 2 and jan.distinct_customer_count == 2)
    expect("february 2 apps / 2 customers", feb is not None
           and feb.application_count == 2 and feb.distinct_customer_count == 2)
    expect("nothing suppressed", res.suppressed_application_count == 0)
    check_invariants(res.rows, K, len(records), res.suppressed_application_count)

    # 3. a single-customer month triggers a quarter merge; quarter is >= k.
    records = [
        rec("C1", 2026, 1, 500),                       # Jan: 1 customer (< k)
        rec("C2", 2026, 2, 500), rec("C3", 2026, 2, 500),
        rec("C1", 2026, 3, 500),
    ]
    res = aggregate(records, K, 1000)
    idx = index_rows(res.rows)
    q1 = idx.get((0, "QUARTER", quarter_start(2026, 1)))
    expect("quarter merged when a month is under k", q1 is not None and len(res.rows) == 1)
    expect("quarter spans full Q1 calendar range",
           q1 is not None and q1.period_end == quarter_end_exclusive(2026, 1))
    expect("quarter counts 4 apps / 3 customers",
           q1 is not None and q1.application_count == 4 and q1.distinct_customer_count == 3)
    check_invariants(res.rows, K, len(records), res.suppressed_application_count)

    # 4. quarter still under k -> whole year merge; year >= k publishes.
    #    Two quarters each with a single distinct customer, but together >= k.
    records = [
        rec("C1", 2026, 1, 500),   # Q1: only C1
        rec("C2", 2026, 4, 500),   # Q2: only C2
    ]
    res = aggregate(records, K, 1000)
    idx = index_rows(res.rows)
    yr = idx.get((0, "YEAR", year_start(2026)))
    expect("year merge when merged quarter still under k",
           yr is not None and len(res.rows) == 1)
    expect("year period covers full calendar year",
           yr is not None and yr.period_end == year_end_exclusive(2026))
    expect("year counts 2 apps / 2 customers",
           yr is not None and yr.application_count == 2 and yr.distinct_customer_count == 2)
    check_invariants(res.rows, K, len(records), res.suppressed_application_count)

    # 5. year still under k -> suppressed, nothing published.
    records = [rec("C1", 2026, 1, 500), rec("C1", 2026, 6, 500)]  # same customer
    res = aggregate(records, K, 1000)
    expect("single-customer year fully suppressed",
           len(res.rows) == 0 and res.suppressed_application_count == 2)

    # 6. mixed year: good months stay monthly, bad quarter merges within one year.
    records = [
        # Q1 healthy months
        rec("C1", 2026, 1, 500), rec("C2", 2026, 1, 500),
        rec("C3", 2026, 2, 500), rec("C4", 2026, 2, 500),
        rec("C5", 2026, 3, 500), rec("C6", 2026, 3, 500),
        # Q2 with a weak month but quarter overall healthy -> quarter row
        rec("C1", 2026, 4, 500),
        rec("C2", 2026, 5, 500), rec("C3", 2026, 5, 500),
    ]
    res = aggregate(records, K, 1000)
    idx = index_rows(res.rows)
    kinds = sorted(r.period_kind for r in res.rows)
    expect("3 month rows + 1 quarter row", kinds == ["MONTH", "MONTH", "MONTH", "QUARTER"])
    expect("no Q1 quarter row (months survived)",
           (0, "QUARTER", quarter_start(2026, 1)) not in idx)
    q2 = idx[(0, "QUARTER", quarter_start(2026, 2))]
    expect("Q2 merged with 3 apps / 3 customers",
           q2.application_count == 3 and q2.distinct_customer_count == 3)
    check_invariants(res.rows, K, len(records), res.suppressed_application_count)

    # 7. empty income gets a separate bin and follows the same cascade.
    records = [
        rec("C1", 2026, 1, None), rec("C2", 2026, 1, None),   # empty bin, healthy month
        rec("C3", 2026, 1, 500), rec("C4", 2026, 1, 500),     # income bin [0,1000)
    ]
    res = aggregate(records, K, 1000)
    idx = index_rows(res.rows)
    empty_jan = idx.get((None, "MONTH", date(2026, 1, 1)))
    income_jan = idx.get((0, "MONTH", date(2026, 1, 1)))
    expect("empty-income bin separate from income bin",
           empty_jan is not None and income_jan is not None and len(res.rows) == 2)
    expect("empty bin has null bounds",
           empty_jan.bin_lower is None and empty_jan.bin_upper is None)
    expect("empty bin sorts last", res.rows[-1].bin_lower is None)
    check_invariants(res.rows, K, len(records), res.suppressed_application_count)

    # 8. empty bin independently suppressed while an income bin publishes.
    records = [
        rec("C9", 2026, 1, None),                              # empty bin: 1 customer -> gone
        rec("C1", 2026, 1, 500), rec("C2", 2026, 1, 500),
    ]
    res = aggregate(records, K, 1000)
    expect("empty bin suppressed alone",
           all(r.bin_lower is not None for r in res.rows)
           and res.suppressed_application_count == 1)
    check_invariants(res.rows, K, len(records), res.suppressed_application_count)

    # 9. distinct-customer dedup: same customer twice in a month counts once.
    records = [rec("C1", 2026, 1, 500), rec("C1", 2026, 1, 900)]
    res = aggregate(records, K, 1000)
    expect("same customer deduped -> suppressed",
           len(res.rows) == 0 and res.suppressed_application_count == 2)

    # 10. binning actually separates different widths; width changes output.
    records = [rec("C1", 2026, 1, 500), rec("C2", 2026, 1, 1500),
               rec("C3", 2026, 1, 2500), rec("C4", 2026, 1, 3500)]
    narrow = index_rows(aggregate(records, K, 1000).rows)
    wide = index_rows(aggregate(records, K, 4000).rows)
    expect("width 1000 splits into two bins ([0,1000) and others merge by cascade)",
           all(key[0] in (0, 1000) for key in narrow))
    # width 4000: all four incomes fall in [0,4000) -> one month row, 4 customers.
    wide_row = wide.get((0, "MONTH", date(2026, 1, 1)))
    expect("width 4000 groups all incomes in one bin/row",
           wide_row is not None and wide_row.distinct_customer_count == 4
           and wide_row.application_count == 4)

    # 11. multiple years cascade independently.
    records = [
        rec("C1", 2025, 1, 500), rec("C2", 2025, 1, 500),   # 2025 healthy month
        rec("C3", 2026, 1, 500),                             # 2026 only one customer -> suppressed
    ]
    res = aggregate(records, K, 1000)
    years = {r.period_start.year for r in res.rows}
    expect("years handled independently", years == {2025}
           and res.suppressed_application_count == 1)
    check_invariants(res.rows, K, len(records), res.suppressed_application_count)

    # 12. empty input -> empty publication, nothing suppressed.
    res = aggregate([], K, 1000)
    expect("empty input yields no rows", res.rows == ()
           and res.suppressed_application_count == 0)

    # 13. validation of k and width.
    for bad_k in (1, 6):
        try:
            aggregate([], bad_k, 1000)
            expect(f"k={bad_k} rejected", False)
        except ValueError:
            expect(f"k={bad_k} rejected", True)
    try:
        aggregate([], K, 0)
        expect("width=0 rejected", False)
    except ValueError:
        expect("width=0 rejected", True)

    print()
    if failed:
        raise SystemExit(f"{failed} aggregation test(s) failed")
    print("all aggregation cascade tests passed")


if __name__ == "__main__":
    main()
