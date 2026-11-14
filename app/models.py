"""Pydantic request/response models. All datetimes are timezone-aware UTC."""
from __future__ import annotations

import uuid
from datetime import date, datetime, timezone

from pydantic import BaseModel, Field, field_validator


def _check_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("datetime must include an explicit timezone (use UTC, e.g. 2026-01-10T08:00:00Z)")
    return value.astimezone(timezone.utc)


class ApplicationIn(BaseModel):
    application_id: str = Field(min_length=1, max_length=128)
    customer_id: str = Field(min_length=1, max_length=128)
    application_time: datetime

    @field_validator("application_time")
    @classmethod
    def _utc(cls, v: datetime) -> datetime:
        return _check_utc(v)


class ApplicationsImport(BaseModel):
    applications: list[ApplicationIn] = Field(min_length=1)


class CreditEventIn(BaseModel):
    event_id: str = Field(min_length=1, max_length=128)
    customer_id: str = Field(min_length=1, max_length=128)
    business_time: datetime
    known_time: datetime
    income: int | None = Field(default=None)
    withdrawn: bool = False

    @field_validator("business_time", "known_time")
    @classmethod
    def _utc(cls, v: datetime) -> datetime:
        return _check_utc(v)

    @field_validator("withdrawn")
    @classmethod
    def _income_consistency(cls, withdrawn: bool, info) -> bool:
        income = info.data.get("income")
        if withdrawn and income is not None:
            raise ValueError("withdrawn event must not carry an income")
        if not withdrawn and income is None:
            raise ValueError("non-withdrawn event requires an integer income")
        return withdrawn


class CreditEventsImport(BaseModel):
    events: list[CreditEventIn] = Field(min_length=1)


class RejectedResponse(BaseModel):
    detail: str
    ingest_batch_id: uuid.UUID
    conflicts: list[dict] | None = None


class BatchCreateRequest(BaseModel):
    application_ids: list[str] = Field(min_length=1)


class BatchResponse(BaseModel):
    batch_id: uuid.UUID
    status: str
    total_items: int
    processed_items: int
    frozen_ingest_at: datetime
    created_at: datetime
    worker_id: str | None = None
    error: str | None = None


class ResultRow(BaseModel):
    application_id: str
    customer_id: str
    application_time: datetime
    income: int | None
    source_event_id: str | None
    source_version_known_time: datetime | None
    source_business_time: datetime | None
    source_ingest_batch_id: uuid.UUID | None
    basis: str  # "event" or "no_known_event"


class ResultsPage(BaseModel):
    batch_id: uuid.UUID
    status: str
    limit: int
    next_cursor: str | None
    items: list[ResultRow]


class ImportAccepted(BaseModel):
    ingest_batch_id: uuid.UUID
    imported: int
    kind: str


# --------------------------------------------------------------------------- #
# Privacy-preserving income-distribution publications
# --------------------------------------------------------------------------- #
class PublicationCreateRequest(BaseModel):
    k: int = Field(ge=2, le=5, description="每个发布行至少包含的去重客户数（2～5）")
    bin_width: int = Field(ge=1, description="正整数收入分箱宽度，以零为边界")


class PublicationRowOut(BaseModel):
    period_kind: str            # MONTH | QUARTER | YEAR
    period_start: date          # UTC 时间段首日（含）
    period_end: date            # UTC 时间段末日的次日（不含）
    bin_lower: int | None       # null = 空收入单独箱
    bin_upper: int | None
    application_count: int
    distinct_customer_count: int


class PublicationResponse(BaseModel):
    publication_id: uuid.UUID
    batch_id: uuid.UUID
    k: int
    bin_width: int
    created_at: datetime
    row_count: int
    suppressed_application_count: int
    already_published: bool     # True = 同参数重试读取的既有版本
    rows: list[PublicationRowOut]


class PublicationPage(BaseModel):
    batch_id: uuid.UUID
    publication_id: uuid.UUID
    k: int
    bin_width: int
    created_at: datetime
    row_count: int
    suppressed_application_count: int
    limit: int
    next_cursor: str | None
    items: list[PublicationRowOut]


class PublicationSummary(BaseModel):
    publication_id: uuid.UUID
    batch_id: uuid.UUID
    k: int
    bin_width: int
    created_at: datetime
    row_count: int
    suppressed_application_count: int
