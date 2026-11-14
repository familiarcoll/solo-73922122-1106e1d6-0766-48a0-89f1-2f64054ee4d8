-- ============================================================================
-- Point-in-time income backfill — PostgreSQL schema
-- All timestamps are stored as timestamptz (UTC).
-- ============================================================================

-- Imported loan applications ------------------------------------------------
CREATE TABLE IF NOT EXISTS applications (
    application_id   TEXT PRIMARY KEY,
    customer_id      TEXT NOT NULL,
    application_time TIMESTAMPTZ NOT NULL,
    ingested_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    ingest_batch_id  UUID NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_app_customer ON applications (customer_id, application_time);

-- Credit bureau change log ---------------------------------------------------
-- One row = one known version of one event.
-- (event_id, known_time) is the natural version key and is unique.
CREATE TABLE IF NOT EXISTS credit_event_versions (
    event_id        TEXT NOT NULL,
    customer_id     TEXT NOT NULL,
    business_time   TIMESTAMPTZ NOT NULL,   -- UTC business time of the change
    known_time      TIMESTAMPTZ NOT NULL,   -- UTC time at which we learned it
    income          INTEGER,                -- NULL when withdrawn = true
    withdrawn       BOOLEAN NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL,
    ingest_batch_id UUID NOT NULL,
    PRIMARY KEY (event_id, known_time)
);
CREATE INDEX IF NOT EXISTS idx_cev_customer_lookup
    ON credit_event_versions (customer_id, business_time, event_id, known_time);

-- Audit of every import batch ------------------------------------------------
CREATE TABLE IF NOT EXISTS import_batches (
    ingest_batch_id UUID PRIMARY KEY,
    import_kind     TEXT NOT NULL CHECK (import_kind IN ('applications', 'credit_events')),
    row_count       INTEGER NOT NULL,
    status          TEXT NOT NULL CHECK (status IN ('accepted', 'rejected')),
    reason          TEXT,
    received_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Generation batches (immutable work units) ----------------------------------
CREATE TABLE IF NOT EXISTS batches (
    batch_id          UUID PRIMARY KEY,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    frozen_ingest_at  TIMESTAMPTZ NOT NULL,  -- visibility fence
    status            TEXT NOT NULL DEFAULT 'PENDING'
        CHECK (status IN ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED')),
    total_items       INTEGER NOT NULL,
    processed_items   INTEGER NOT NULL DEFAULT 0,
    worker_id         TEXT,
    lease_expires_at  TIMESTAMPTZ,
    error             TEXT
);
CREATE INDEX IF NOT EXISTS idx_batches_work ON batches (status, lease_expires_at);

-- Snapshot of the input of each batch ----------------------------------------
CREATE TABLE IF NOT EXISTS batch_items (
    batch_id        UUID NOT NULL REFERENCES batches (batch_id),
    application_id  TEXT NOT NULL,
    item_seq        BIGINT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'PENDING'
        CHECK (status IN ('PENDING', 'RUNNING', 'DONE')),
    worker_id       TEXT,
    lease_expires_at TIMESTAMPTZ,
    attempts        INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (batch_id, application_id),
    UNIQUE (batch_id, item_seq)
);
CREATE INDEX IF NOT EXISTS idx_batch_items_claim
    ON batch_items (batch_id, status, lease_expires_at);

-- Final results; only exposed once the batch is COMPLETED --------------------
CREATE TABLE IF NOT EXISTS batch_results (
    batch_id        UUID NOT NULL,
    application_id  TEXT NOT NULL,
    customer_id     TEXT NOT NULL,
    application_time TIMESTAMPTZ NOT NULL,
    income          INTEGER,
    source_event_id TEXT,
    source_version_known_time TIMESTAMPTZ,
    source_ingest_batch_id UUID,
    source_business_time TIMESTAMPTZ,
    item_seq        BIGINT NOT NULL,
    PRIMARY KEY (batch_id, application_id)
);
CREATE INDEX IF NOT EXISTS idx_batch_results_page
    ON batch_results (batch_id, item_seq);

-- Privacy-preserving income-distribution publications ------------------------
-- Exactly one publication per completed batch (UNIQUE batch_id is the hard
-- concurrency backstop; the API also takes a FOR UPDATE row lock on batches).
CREATE TABLE IF NOT EXISTS publications (
    publication_id              UUID PRIMARY KEY,
    batch_id                    UUID NOT NULL UNIQUE REFERENCES batches (batch_id),
    k                           INTEGER NOT NULL CHECK (k BETWEEN 2 AND 5),
    bin_width                   INTEGER NOT NULL CHECK (bin_width > 0),
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
    row_count                   INTEGER NOT NULL DEFAULT 0,
    suppressed_application_count INTEGER NOT NULL DEFAULT 0
);

-- One published, k-anonymous aggregate row. No application or customer id is
-- ever stored here; distinct_customer_count is always >= the requested k.
CREATE TABLE IF NOT EXISTS publication_rows (
    publication_id            UUID NOT NULL REFERENCES publications (publication_id),
    row_seq                   BIGINT NOT NULL,
    period_kind               TEXT NOT NULL CHECK (period_kind IN ('MONTH', 'QUARTER', 'YEAR')),
    period_start              DATE NOT NULL,  -- first UTC day of the period (inclusive)
    period_end                DATE NOT NULL,  -- first UTC day after the period (exclusive)
    bin_lower                 BIGINT,         -- NULL marks the separate empty-income bin
    bin_upper                 BIGINT,         -- NULL marks the separate empty-income bin
    application_count         INTEGER NOT NULL CHECK (application_count >= 0),
    distinct_customer_count   INTEGER NOT NULL CHECK (distinct_customer_count >= 2),
    PRIMARY KEY (publication_id, row_seq)
);
CREATE INDEX IF NOT EXISTS idx_publication_rows_page
    ON publication_rows (publication_id, row_seq);
