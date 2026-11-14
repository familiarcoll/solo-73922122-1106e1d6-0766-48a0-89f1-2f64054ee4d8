"""k-anonymous income-distribution aggregation (pure functions, no I/O).

The external evaluation party receives only aggregate counts: no individual
application or customer identifier ever leaves through this path.

Bin rule
--------
Incomes are integer-width bins anchored at zero::

    bin_index = floor_div(income, width)
    [bin_index * width, bin_index * width + width)

so 0 is a bin boundary. Applications whose backfilled income is empty
(``income IS NULL``) go into a separate empty-income bin.

Time-period cascade (per income bin)
------------------------------------
1. Every application is first counted by the UTC month in which it was
   applied; months without records produce nothing.
2. Quarter level: if any recorded month inside a quarter holds fewer than
   ``k`` distinct customers, *all* months of that quarter are merged into one
   quarter period; otherwise the months stay published individually.
3. Year level: if a merged quarter still holds fewer than ``k`` distinct
   customers, every quarter of that year is merged (together with the
   month-level rows already surviving step 2) into one year period.
4. If the whole year still holds fewer than ``k`` distinct customers for the
   bin, that year's records are not published at all.

Consequences guaranteed by the construction:

- every published row has ``distinct_customer_count >= k``;
- within one bin, published periods never overlap (an application belongs to
  exactly one published period, or is suppressed);
- a bin and a period are never double-counted.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Iterable, Optional, Sequence

# Bins are keyed by bin index; the empty-income bin uses a sentinel.
EMPTY_BIN: object = object()


@dataclass(frozen=True)
class SourceRecord:
    """One input row of the aggregation: one completed batch result."""

    customer_id: str
    application_month: date  # any date inside the UTC application month; day is ignored
    income: Optional[int]    # None == empty income, separate bin


@dataclass(frozen=True)
class PublicationAggregate:
    """One publishable aggregate row. Period end is exclusive."""

    period_kind: str          # MONTH | QUARTER | YEAR
    period_start: date
    period_end: date
    bin_lower: Optional[int]  # None for the empty-income bin
    bin_upper: Optional[int]
    application_count: int
    distinct_customer_count: int


@dataclass(frozen=True)
class AggregationResult:
    rows: tuple[PublicationAggregate, ...]
    suppressed_application_count: int


# --------------------------------------------------------------------------- #
# Calendar helpers
# --------------------------------------------------------------------------- #
def month_start(d: date) -> date:
    return date(d.year, d.month, 1)


def next_month(d: date) -> date:
    if d.month == 12:
        return date(d.year + 1, 1, 1)
    return date(d.year, d.month + 1, 1)


def quarter_index(month: int) -> int:
    """1..4."""
    return (month - 1) // 3 + 1


def quarter_start(year: int, quarter: int) -> date:
    return date(year, (quarter - 1) * 3 + 1, 1)


def quarter_end_exclusive(year: int, quarter: int) -> date:
    last_month = quarter * 3
    if last_month == 12:
        return date(year + 1, 1, 1)
    return date(year, last_month + 1, 1)


def year_start(year: int) -> date:
    return date(year, 1, 1)


def year_end_exclusive(year: int) -> date:
    return date(year + 1, 1, 1)


def bin_bounds(income: int, width: int) -> tuple[int, int]:
    index = income // width  # floor division: correct for negative integers too
    lower = index * width
    return lower, lower + width


# --------------------------------------------------------------------------- #
# Core aggregation
# --------------------------------------------------------------------------- #
def _aggregate_period(records: Sequence[SourceRecord]) -> tuple[int, set[str]]:
    return len(records), {r.customer_id for r in records}


def aggregate(records: Iterable[SourceRecord], k: int, width: int) -> AggregationResult:
    """Run the full bin -> month -> quarter -> year cascade.

    ``k`` is validated by the API model (2..5); ``width`` is a positive int.
    """
    if not 2 <= k <= 5:
        raise ValueError("k must be between 2 and 5")
    if width <= 0:
        raise ValueError("bin width must be a positive integer")

    # bin key -> month -> records (only months that actually have records)
    bins: dict[object, dict[date, list[SourceRecord]]] = {}
    for rec in records:
        if rec.income is None:
            key: object = EMPTY_BIN
        else:
            key = bin_bounds(rec.income, width)[0]  # bin lower bound doubles as key
        m = month_start(rec.application_month)
        bins.setdefault(key, {}).setdefault(m, []).append(rec)

    rows: list[PublicationAggregate] = []
    suppressed = 0

    for key in sorted(bins, key=lambda b: (b is EMPTY_BIN, b if b is not EMPTY_BIN else 0)):
        if key is EMPTY_BIN:
            lower: Optional[int] = None
            upper: Optional[int] = None
        else:
            lower = int(key)
            upper = lower + width

        months: dict[date, list[SourceRecord]] = bins[key]
        years = sorted({m.year for m in months})

        for year in years:
            year_months = sorted(m for m in months if m.year == year)
            # Quarter of each recorded month (1..4).
            quarters = sorted({quarter_index(m.month) for m in year_months})

            # ---- Step 2: month vs. quarter decision ------------------------
            # Surviving month-level periods, and quarters that must be merged.
            surviving_months: list[date] = []
            merged_quarters: list[int] = []
            for q in quarters:
                q_months = [m for m in year_months if quarter_index(m.month) == q]
                if any(len({r.customer_id for r in months[m]}) < k for m in q_months):
                    merged_quarters.append(q)          # whole quarter merges
                else:
                    surviving_months.extend(q_months)  # every month is >= k on its own

            # ---- Step 3: quarter vs. year decision -------------------------
            # A merged quarter stays at quarter level only if it itself holds
            # >= k distinct customers; otherwise the whole year merges.
            surviving_quarters: list[int] = []
            year_merge = False
            for q in merged_quarters:
                q_records = [r for m in year_months if quarter_index(m.month) == q
                             for r in months[m]]
                if len({r.customer_id for r in q_records}) >= k:
                    surviving_quarters.append(q)
                else:
                    year_merge = True

            if year_merge:
                # All recorded months of this year for this bin collapse into
                # one year-level period (months/quarters are no longer emitted
                # for this bin-year, so periods cannot overlap).
                year_records = [r for m in year_months for r in months[m]]
                app_count, customers = _aggregate_period(year_records)
                if len(customers) >= k:
                    rows.append(PublicationAggregate(
                        period_kind="YEAR",
                        period_start=year_start(year),
                        period_end=year_end_exclusive(year),
                        bin_lower=lower, bin_upper=upper,
                        application_count=app_count,
                        distinct_customer_count=len(customers),
                    ))
                else:
                    # Step 4: year still under k -> not published.
                    suppressed += app_count
                continue

            # No year merge: emit month-level survivors and merged quarters.
            for m in surviving_months:
                app_count, customers = _aggregate_period(months[m])
                rows.append(PublicationAggregate(
                    period_kind="MONTH",
                    period_start=m,
                    period_end=next_month(m),
                    bin_lower=lower, bin_upper=upper,
                    application_count=app_count,
                    distinct_customer_count=len(customers),
                ))
            for q in surviving_quarters:
                q_records = [r for m in year_months if quarter_index(m.month) == q
                             for r in months[m]]
                app_count, customers = _aggregate_period(q_records)
                rows.append(PublicationAggregate(
                    period_kind="QUARTER",
                    period_start=quarter_start(year, q),
                    period_end=quarter_end_exclusive(year, q),
                    bin_lower=lower, bin_upper=upper,
                    application_count=app_count,
                    distinct_customer_count=len(customers),
                ))

    rows.sort(key=lambda r: (
        r.bin_lower is None,              # empty-income bin last
        r.bin_lower if r.bin_lower is not None else 0,
        r.period_start,
        {"MONTH": 0, "QUARTER": 1, "YEAR": 2}[r.period_kind],
    ))
    return AggregationResult(tuple(rows), suppressed)
