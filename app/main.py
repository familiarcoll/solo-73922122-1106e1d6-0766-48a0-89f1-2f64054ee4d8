"""FastAPI application: imports, immutable batch generation, paged results.

Run two processes against the same DATABASE_URL (see docker-compose.yml);
each serves the API and runs one cooperating worker.
"""
from __future__ import annotations

import base64
import logging
import uuid
from contextlib import asynccontextmanager
from datetime import datetime

import asyncpg
from fastapi import FastAPI, HTTPException, Query, status
from fastapi.responses import JSONResponse

from . import config, publication
from .db import close_pool, get_pool, init_pool, init_schema
from .models import (
    ApplicationsImport,
    BatchCreateRequest,
    BatchResponse,
    CreditEventsImport,
    ImportAccepted,
    PublicationCreateRequest,
    PublicationPage,
    PublicationResponse,
    PublicationRowOut,
    PublicationSummary,
    ResultsPage,
    ResultRow,
)
from .publication import SourceRecord, aggregate
from .worker import Worker

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("income.api")

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _worker
    pool = await init_pool()
    await init_schema()
    _worker = Worker(pool, config.WORKER_ID)
    _worker.start()
    logger.info("api process up; worker id = %s", config.WORKER_ID)
    try:
        yield
    finally:
        await _worker.stop()
        await close_pool()


app = FastAPI(
    title="Point-in-time Income Backfill API",
    version="1.0.0",
    lifespan=lifespan,
    description=(
        "历史贷款申请的申请时点收入回填服务。征信事件按获知时间保留全部版本，"
        "生成批次固化受理时已提交的输入，两个 API 进程共享数据库并协作续算。"
    ),
)
_worker: Worker | None = None


@app.get("/health", tags=["ops"])
async def health() -> dict:
    pool = get_pool()
    async with pool.acquire() as conn:
        await conn.fetchval("SELECT 1")
    return {"status": "ok", "worker_id": config.WORKER_ID}


# --------------------------------------------------------------------------- #
# Imports
# --------------------------------------------------------------------------- #
async def _record_import(conn, kind: str, row_count: int, ok: bool, reason: str | None) -> uuid.UUID:
    ingest_id = uuid.uuid4()
    await conn.execute(
        """INSERT INTO import_batches
               (ingest_batch_id, import_kind, row_count, status, reason)
           VALUES ($1, $2, $3, $4, $5)""",
        ingest_id, kind, row_count, "accepted" if ok else "rejected", reason,
    )
    return ingest_id


@app.post(
    "/api/v1/imports/applications",
    response_model=ImportAccepted,
    status_code=status.HTTP_201_CREATED,
    tags=["imports"],
    summary="导入历史贷款申请（整批事务，失败全拒绝）",
)
async def import_applications(payload: ApplicationsImport) -> ImportAccepted:
    pool = get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            # Duplicates inside the payload.
            seen: set[str] = set()
            dupes = [a.application_id for a in payload.applications
                     if a.application_id in seen or seen.add(a.application_id)]
            if dupes:
                ingest_id = await _record_import(
                    conn, "applications", len(payload.applications), False,
                    f"duplicate application_id in payload: {sorted(set(dupes))}",
                )
                return JSONResponse(
                    status_code=409,
                    content={
                        "detail": "whole import rejected: duplicate application_id in payload",
                        "ingest_batch_id": str(ingest_id),
                        "conflicts": sorted(set(dupes)),
                    },
                )

            existing = await conn.fetch(
                "SELECT application_id FROM applications WHERE application_id = ANY($1)",
                [a.application_id for a in payload.applications],
            )
            if existing:
                ids = [r["application_id"] for r in existing]
                ingest_id = await _record_import(
                    conn, "applications", len(payload.applications), False,
                    f"application_id already imported: {ids}",
                )
                return JSONResponse(
                    status_code=409,
                    content={
                        "detail": "whole import rejected: application_id already exists",
                        "ingest_batch_id": str(ingest_id),
                        "conflicts": ids,
                    },
                )

            ingest_id = await _record_import(conn, "applications", len(payload.applications), True, None)
            await conn.executemany(
                """INSERT INTO applications
                       (application_id, customer_id, application_time,
                        ingested_at, ingest_batch_id)
                   VALUES ($1, $2, $3, now(), $4)""",
                [
                    (a.application_id, a.customer_id, a.application_time, ingest_id)
                    for a in payload.applications
                ],
            )
    return ImportAccepted(ingest_batch_id=ingest_id, imported=len(payload.applications), kind="applications")


@app.post(
    "/api/v1/imports/credit-events",
    response_model=ImportAccepted,
    status_code=status.HTTP_201_CREATED,
    tags=["imports"],
    summary="导入征信变更日志（同事件同获知时间异值则整批拒绝）",
)
async def import_credit_events(payload: CreditEventsImport) -> ImportAccepted:
    pool = get_pool()
    events = payload.events

    # ---- validation: identical duplicates collapse; differing versions on
    # the same (event_id, known_time) reject the entire import batch. ----
    versions: dict[tuple[str, datetime], dict] = {}
    conflicts: list[dict] = []
    customer_of_event: dict[str, str] = {}
    for e in events:
        key = (e.event_id, e.known_time)
        signature = {
            "customer_id": e.customer_id,
            "business_time": e.business_time,
            "income": e.income,
            "withdrawn": e.withdrawn,
        }
        if key in versions and versions[key] != signature:
            conflicts.append({"event_id": e.event_id, "known_time": e.known_time.isoformat(),
                              "reason": "different values for same event_id+known_time"})
        else:
            versions.setdefault(key, signature)
        prior_customer = customer_of_event.get(e.event_id)
        if prior_customer and prior_customer != e.customer_id:
            conflicts.append({"event_id": e.event_id,
                              "reason": "event_id mapped to multiple customer_id values"})
        customer_of_event.setdefault(e.event_id, e.customer_id)

    pool = get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            if conflicts:
                ingest_id = await _record_import(conn, "credit_events", len(events), False,
                                                 "conflicting versions in payload")
                return JSONResponse(
                    status_code=409,
                    content={"detail": "whole import rejected: conflicting event versions",
                             "ingest_batch_id": str(ingest_id), "conflicts": conflicts},
                )

            # Cross-check against already-stored versions.
            db_rows = await conn.fetch(
                """SELECT event_id, known_time, customer_id, business_time,
                          income, withdrawn
                     FROM credit_event_versions
                    WHERE (event_id, known_time) IN (
                        SELECT v.e, v.k FROM unnest($1::text[], $2::timestamptz[])
                            AS v(e, k)
                    )""",
                [k[0] for k in versions], [k[1] for k in versions],
            )
            for row in db_rows:
                sig = versions[(row["event_id"], row["known_time"])]
                if (sig["customer_id"] != row["customer_id"]
                        or sig["business_time"] != row["business_time"]
                        or sig["income"] != row["income"]
                        or sig["withdrawn"] != row["withdrawn"]):
                    conflicts.append({
                        "event_id": row["event_id"],
                        "known_time": row["known_time"].isoformat(),
                        "reason": "different values already stored for event_id+known_time",
                    })
            if conflicts:
                ingest_id = await _record_import(conn, "credit_events", len(events), False,
                                                 "conflicts with stored versions")
                return JSONResponse(
                    status_code=409,
                    content={"detail": "whole import rejected: conflicts with stored versions",
                             "ingest_batch_id": str(ingest_id), "conflicts": conflicts},
                )

            # Customer consistency for events that already exist under other versions.
            known_events = await conn.fetch(
                "SELECT DISTINCT event_id, customer_id FROM credit_event_versions WHERE event_id = ANY($1)",
                list(customer_of_event),
            )
            for row in known_events:
                if customer_of_event.get(row["event_id"]) != row["customer_id"]:
                    conflicts.append({"event_id": row["event_id"],
                                      "reason": "event_id belongs to another customer_id"})
            if conflicts:
                ingest_id = await _record_import(conn, "credit_events", len(events), False,
                                                 "event/customer mismatch")
                return JSONResponse(
                    status_code=409,
                    content={"detail": "whole import rejected: event/customer mismatch",
                             "ingest_batch_id": str(ingest_id), "conflicts": conflicts},
                )

            ingest_id = await _record_import(conn, "credit_events", len(events), True, None)
            inserted = 0
            for (event_id, known_time), sig in versions.items():
                # Skip exact duplicates already present (idempotent re-import).
                tag = await conn.execute(
                    """INSERT INTO credit_event_versions
                           (event_id, customer_id, business_time, known_time,
                            income, withdrawn, ingested_at, ingest_batch_id)
                       VALUES ($1,$2,$3,$4,$5,$6,now(),$7)
                       ON CONFLICT (event_id, known_time) DO NOTHING""",
                    event_id, sig["customer_id"], sig["business_time"], known_time,
                    sig["income"], sig["withdrawn"], ingest_id,
                )
                if tag.endswith("0 1"):
                    inserted += 1
    return ImportAccepted(ingest_batch_id=ingest_id, imported=inserted, kind="credit_events")


# --------------------------------------------------------------------------- #
# Batch generation
# --------------------------------------------------------------------------- #
@app.post(
    "/api/v1/batches",
    response_model=BatchResponse,
    status_code=status.HTTP_201_CREATED,
    tags=["batches"],
    summary="生成回填批次（固化受理时刻已提交的输入）",
)
async def create_batch(payload: BatchCreateRequest) -> BatchResponse:
    ids = payload.application_ids
    if len(set(ids)) != len(ids):
        raise HTTPException(400, "duplicate application_ids in request")
    if len(ids) > config.MAX_APPLICATIONS_PER_BATCH:
        raise HTTPException(
            400,
            f"a batch covers at most {config.MAX_APPLICATIONS_PER_BATCH} applications, "
            f"got {len(ids)}",
        )

    pool = get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            rows = await conn.fetch(
                "SELECT application_id FROM applications WHERE application_id = ANY($1) ORDER BY application_id",
                ids,
            )
            found = {r["application_id"] for r in rows}
            missing = sorted(set(ids) - found)
            if missing:
                raise HTTPException(400, f"unknown application_ids: {missing}")

            batch_id = uuid.uuid4()
            batch_row = await conn.fetchrow(
                """INSERT INTO batches (batch_id, frozen_ingest_at, total_items)
                   VALUES ($1, now(), $2)
                   RETURNING batch_id, created_at, frozen_ingest_at, status,
                             total_items, processed_items, worker_id, error""",
                batch_id, len(ids),
            )
            await conn.executemany(
                """INSERT INTO batch_items (batch_id, application_id, item_seq)
                   VALUES ($1, $2, $3)""",
                [(batch_id, app_id, seq) for seq, app_id in enumerate(sorted(ids))],
            )
    return BatchResponse(**dict(batch_row.items()))


@app.get("/api/v1/batches", response_model=list[BatchResponse], tags=["batches"],
         summary="列出全部批次及状态")
async def list_batches() -> list[BatchResponse]:
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT batch_id, created_at, frozen_ingest_at, status, total_items,
                      processed_items, worker_id, error
                 FROM batches ORDER BY created_at""",
        )
    return [BatchResponse(**dict(r.items())) for r in rows]


@app.get("/api/v1/batches/{batch_id}", response_model=BatchResponse, tags=["batches"],
         summary="查询单个批次状态")
async def get_batch(batch_id: uuid.UUID) -> BatchResponse:
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """SELECT batch_id, created_at, frozen_ingest_at, status, total_items,
                      processed_items, worker_id, error
                 FROM batches WHERE batch_id = $1""",
            batch_id,
        )
    if not row:
        raise HTTPException(404, "batch not found")
    return BatchResponse(**dict(row.items()))


@app.post("/api/v1/batches/{batch_id}/resume", response_model=BatchResponse, tags=["batches"],
          summary="将 FAILED 批次重新置为 PENDING（续算，不改变已固化输入）")
async def resume_batch(batch_id: uuid.UUID) -> BatchResponse:
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """UPDATE batches
                  SET status = 'PENDING', worker_id = NULL,
                      lease_expires_at = NULL, error = NULL
                WHERE batch_id = $1 AND status = 'FAILED'
            RETURNING batch_id, created_at, frozen_ingest_at, status, total_items,
                      processed_items, worker_id, error""",
            batch_id,
        )
        if not row:
            raise HTTPException(409, "only FAILED batches can be resumed")
        await conn.execute(
            """UPDATE batch_items
                  SET status = 'PENDING', worker_id = NULL, lease_expires_at = NULL
                WHERE batch_id = $1 AND status <> 'DONE'""",
            batch_id,
        )
    return BatchResponse(**dict(row.items()))


# --------------------------------------------------------------------------- #
# Results — keyset pagination, only after COMPLETED
# --------------------------------------------------------------------------- #
def _encode_cursor(item_seq: int) -> str:
    return base64.urlsafe_b64encode(str(item_seq).encode()).decode()


def _decode_cursor(cursor: str) -> int:
    try:
        return int(base64.urlsafe_b64decode(cursor.encode()).decode())
    except Exception as exc:
        raise HTTPException(400, "invalid cursor") from exc


@app.get("/api/v1/batches/{batch_id}/results", response_model=ResultsPage, tags=["results"],
         summary="分页查询结果（批次全部完成后才开放；成功批次内容不再变化）")
async def get_results(
    batch_id: uuid.UUID,
    limit: int = Query(default=config.DEFAULT_PAGE_SIZE, ge=1, le=config.MAX_PAGE_SIZE),
    cursor: str | None = Query(default=None, description="上一页返回的 next_cursor"),
) -> ResultsPage:
    after_seq = _decode_cursor(cursor) if cursor else -1
    pool = get_pool()
    async with pool.acquire() as conn:
        b = await conn.fetchrow("SELECT status, error FROM batches WHERE batch_id = $1", batch_id)
        if not b:
            raise HTTPException(404, "batch not found")
        if b["status"] != "COMPLETED":
            raise HTTPException(
                409,
                f"results not open: batch is {b['status']}"
                + (f" ({b['error']})" if b["error"] else ""),
            )
        rows = await conn.fetch(
            """SELECT application_id, customer_id, application_time, income,
                      source_event_id, source_version_known_time,
                      source_business_time, source_ingest_batch_id, item_seq
                 FROM batch_results
                WHERE batch_id = $1 AND item_seq > $2
                ORDER BY item_seq
                LIMIT $3""",
            batch_id, after_seq, limit + 1,
        )
    next_cursor = None
    if len(rows) > limit:
        next_cursor = _encode_cursor(rows[limit - 1]["item_seq"])
        rows = rows[:limit]
    items = [
        ResultRow(
            application_id=r["application_id"],
            customer_id=r["customer_id"],
            application_time=r["application_time"],
            income=r["income"],
            source_event_id=r["source_event_id"],
            source_version_known_time=r["source_version_known_time"],
            source_business_time=r["source_business_time"],
            source_ingest_batch_id=r["source_ingest_batch_id"],
            basis="event" if r["source_event_id"] else "no_known_event",
        )
        for r in rows
    ]
    return ResultsPage(batch_id=batch_id, status="COMPLETED", limit=limit,
                       next_cursor=next_cursor, items=items)


# --------------------------------------------------------------------------- #
# Privacy-preserving income-distribution publications
#
# One published version per completed batch. Two API processes share the
# database, so the "first successful submitter wins" rule is enforced inside a
# single transaction with:
#   1. a FOR UPDATE row lock on the batch (serialises same-batch submitters),
#   2. a UNIQUE (batch_id) constraint as the hard backstop.
# Any failure rolls the whole transaction back: no readable half result is
# ever left behind. Re-submitting the same parameters returns the stored
# original version.
# --------------------------------------------------------------------------- #
def _row_to_out(r) -> PublicationRowOut:
    return PublicationRowOut(
        period_kind=r["period_kind"],
        period_start=r["period_start"],
        period_end=r["period_end"],
        bin_lower=r["bin_lower"],
        bin_upper=r["bin_upper"],
        application_count=r["application_count"],
        distinct_customer_count=r["distinct_customer_count"],
    )


async def _load_publication(conn, batch_id: uuid.UUID) -> dict | None:
    pub = await conn.fetchrow(
        """SELECT publication_id, batch_id, k, bin_width, created_at,
                  row_count, suppressed_application_count
             FROM publications WHERE batch_id = $1""",
        batch_id,
    )
    return dict(pub.items()) if pub else None


async def _fetch_pub_rows(conn, publication_id: uuid.UUID) -> list[PublicationRowOut]:
    rows = await conn.fetch(
        """SELECT period_kind, period_start, period_end, bin_lower, bin_upper,
                  application_count, distinct_customer_count
             FROM publication_rows
            WHERE publication_id = $1
            ORDER BY row_seq""",
        publication_id,
    )
    return [_row_to_out(r) for r in rows]


@app.post(
    "/api/v1/batches/{batch_id}/publications",
    response_model=PublicationResponse,
    status_code=status.HTTP_201_CREATED,
    tags=["publications"],
    summary="发布收入分布（每批仅一版；并发下先成功提交者为准）",
)
async def create_publication(batch_id: uuid.UUID, payload: PublicationCreateRequest):
    pool = get_pool()
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                # Row lock serialises concurrent publications of the same batch.
                batch = await conn.fetchrow(
                    "SELECT status, total_items FROM batches "
                    "WHERE batch_id = $1 FOR UPDATE",
                    batch_id,
                )
                if not batch:
                    raise HTTPException(404, "batch not found")
                if batch["status"] != "COMPLETED":
                    raise HTTPException(
                        409,
                        f"publication requires a COMPLETED batch; batch is {batch['status']}",
                    )
                if batch["total_items"] > config.MAX_APPLICATIONS_PER_BATCH:
                    raise HTTPException(
                        400,
                        f"a published batch covers at most "
                        f"{config.MAX_APPLICATIONS_PER_BATCH} applications; "
                        f"this batch has {batch['total_items']}",
                    )

                existing = await _load_publication(conn, batch_id)
                if existing is not None:
                    # Single version rule. Same parameters => idempotent read of
                    # the original; different parameters => conflict.
                    if existing["k"] != payload.k or existing["bin_width"] != payload.bin_width:
                        return JSONResponse(
                            status_code=409,
                            content={
                                "detail": "a publication with different parameters already "
                                          "exists for this batch; only one version is allowed",
                                "batch_id": str(batch_id),
                                "existing_publication_id": str(existing["publication_id"]),
                                "existing_k": existing["k"],
                                "existing_bin_width": existing["bin_width"],
                            },
                        )
                    rows = await _fetch_pub_rows(conn, existing["publication_id"])
                    # Same parameters: idempotent read of the stored original.
                    return JSONResponse(
                        status_code=200,
                        content=PublicationResponse(
                            **existing, already_published=True, rows=rows,
                        ).model_dump(mode="json"),
                    )

                # Build the aggregation from the immutable batch results. Only
                # aggregate-safe columns leave the database; no ids are published.
                result_rows = await conn.fetch(
                    """SELECT customer_id, application_time, income
                         FROM batch_results WHERE batch_id = $1""",
                    batch_id,
                )
                records = [
                    SourceRecord(
                        customer_id=r["customer_id"],
                        application_month=r["application_time"].date(),  # UTC date
                        income=r["income"],
                    )
                    for r in result_rows
                ]
                result: publication.AggregationResult = aggregate(
                    records, payload.k, payload.bin_width
                )

                publication_id = uuid.uuid4()
                await conn.execute(
                    """INSERT INTO publications
                           (publication_id, batch_id, k, bin_width, row_count,
                            suppressed_application_count)
                       VALUES ($1, $2, $3, $4, $5, $6)""",
                    publication_id, batch_id, payload.k, payload.bin_width,
                    len(result.rows), result.suppressed_application_count,
                )
                await conn.executemany(
                    """INSERT INTO publication_rows
                           (publication_id, row_seq, period_kind, period_start,
                            period_end, bin_lower, bin_upper, application_count,
                            distinct_customer_count)
                       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)""",
                    [
                        (
                            publication_id, seq, row.period_kind, row.period_start,
                            row.period_end, row.bin_lower, row.bin_upper,
                            row.application_count, row.distinct_customer_count,
                        )
                        for seq, row in enumerate(result.rows)
                    ],
                )
                saved = await _load_publication(conn, batch_id)
                out_rows = [
                    PublicationRowOut(
                        period_kind=row.period_kind,
                        period_start=row.period_start,
                        period_end=row.period_end,
                        bin_lower=row.bin_lower,
                        bin_upper=row.bin_upper,
                        application_count=row.application_count,
                        distinct_customer_count=row.distinct_customer_count,
                    )
                    for row in result.rows
                ]
                return PublicationResponse(
                    **saved, already_published=False, rows=out_rows,
                )
    except asyncpg.UniqueViolationError:
        # Hard backstop: a concurrent transaction committed first. Re-read and
        # apply the same single-version / idempotent rules.
        async with pool.acquire() as conn:
            existing = await _load_publication(conn, batch_id)
        if existing is None:  # pragma: no cover - constraint guarantees a row
            raise HTTPException(500, "publication conflict but no stored version found")
        if existing["k"] != payload.k or existing["bin_width"] != payload.bin_width:
            return JSONResponse(
                status_code=409,
                content={
                    "detail": "a publication with different parameters already exists for "
                              "this batch; the concurrent request committed first",
                    "batch_id": str(batch_id),
                    "existing_publication_id": str(existing["publication_id"]),
                    "existing_k": existing["k"],
                    "existing_bin_width": existing["bin_width"],
                },
            )
        async with pool.acquire() as conn:
            rows = await _fetch_pub_rows(conn, existing["publication_id"])
        return JSONResponse(
            status_code=200,
            content=PublicationResponse(
                **existing, already_published=True, rows=rows,
            ).model_dump(mode="json"),
        )


@app.get(
    "/api/v1/batches/{batch_id}/publications",
    response_model=PublicationPage,
    tags=["publications"],
    summary="分页查看已发布收入分布（keyset 分页，沿用结果分页方式）",
)
async def get_publication(
    batch_id: uuid.UUID,
    limit: int = Query(default=config.DEFAULT_PAGE_SIZE, ge=1, le=config.MAX_PAGE_SIZE),
    cursor: str | None = Query(default=None, description="上一页返回的 next_cursor"),
) -> PublicationPage:
    after_seq = _decode_cursor(cursor) if cursor else -1
    pool = get_pool()
    async with pool.acquire() as conn:
        pub = await conn.fetchrow(
            """SELECT publication_id, batch_id, k, bin_width, created_at,
                      row_count, suppressed_application_count
                 FROM publications WHERE batch_id = $1""",
            batch_id,
        )
        if not pub:
            raise HTTPException(404, "no publication exists for this batch")
        rows = await conn.fetch(
            """SELECT row_seq, period_kind, period_start, period_end, bin_lower,
                      bin_upper, application_count, distinct_customer_count
                 FROM publication_rows
                WHERE publication_id = $1 AND row_seq > $2
                ORDER BY row_seq
                LIMIT $3""",
            pub["publication_id"], after_seq, limit + 1,
        )
    next_cursor = None
    if len(rows) > limit:
        next_cursor = _encode_cursor(rows[limit - 1]["row_seq"])
        rows = rows[:limit]
    return PublicationPage(
        publication_id=pub["publication_id"],
        batch_id=pub["batch_id"],
        k=pub["k"],
        bin_width=pub["bin_width"],
        created_at=pub["created_at"],
        row_count=pub["row_count"],
        suppressed_application_count=pub["suppressed_application_count"],
        limit=limit,
        next_cursor=next_cursor,
        items=[_row_to_out(r) for r in rows],
    )


@app.get(
    "/api/v1/publications",
    response_model=list[PublicationSummary],
    tags=["publications"],
    summary="列出全部已发布收入分布汇总",
)
async def list_publications() -> list[PublicationSummary]:
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT publication_id, batch_id, k, bin_width, created_at,
                      row_count, suppressed_application_count
                 FROM publications ORDER BY created_at""",
        )
    return [PublicationSummary(**dict(r.items())) for r in rows]


# --------------------------------------------------------------------------- #
# Debug / inspection helpers (used by the demo script and /docs)
# --------------------------------------------------------------------------- #
@app.get("/api/v1/applications", tags=["inspect"], summary="查看已导入申请")
async def list_applications() -> list[dict]:
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT application_id, customer_id, application_time,
                      ingested_at, ingest_batch_id
                 FROM applications ORDER BY application_id""",
        )
    return [dict(r.items()) for r in rows]


@app.get("/api/v1/events", tags=["inspect"], summary="查看征信事件全部版本")
async def list_events() -> list[dict]:
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT event_id, customer_id, business_time, known_time, income,
                      withdrawn, ingested_at, ingest_batch_id
                 FROM credit_event_versions
                ORDER BY event_id, known_time""",
        )
    return [dict(r.items()) for r in rows]
