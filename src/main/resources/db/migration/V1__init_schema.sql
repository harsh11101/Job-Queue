CREATE TABLE IF NOT EXISTS jobs (
    id UUID PRIMARY KEY,
    idempotency_key VARCHAR(200) NOT NULL,
    request_hash VARCHAR(128) NOT NULL,
    type VARCHAR(100) NOT NULL,
    priority VARCHAR(20) NOT NULL,
    status VARCHAR(40) NOT NULL,
    payload_json TEXT NOT NULL,
    result_json TEXT,
    error_message TEXT,
    failure_class VARCHAR(200),
    trace_id VARCHAR(100) NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 4,
    next_retry_at TIMESTAMPTZ,
    locked_at TIMESTAMPTZ,
    leased_until TIMESTAMPTZ,
    worker_id VARCHAR(120),
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    version BIGINT NOT NULL DEFAULT 0,
    CONSTRAINT uq_jobs_idempotency_key UNIQUE (idempotency_key)
);

CREATE INDEX IF NOT EXISTS ix_jobs_status_created_at ON jobs(status, created_at DESC);
CREATE INDEX IF NOT EXISTS ix_jobs_next_retry_at ON jobs(next_retry_at) WHERE next_retry_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_jobs_priority_status ON jobs(priority, status, created_at DESC);

CREATE TABLE IF NOT EXISTS side_effect_receipts (
    id UUID PRIMARY KEY,
    effect_key VARCHAR(300) NOT NULL,
    job_id UUID NOT NULL REFERENCES jobs(id),
    job_type VARCHAR(100) NOT NULL,
    payload_hash VARCHAR(128) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT uq_side_effect_receipts_effect_key UNIQUE (effect_key)
);

CREATE INDEX IF NOT EXISTS ix_side_effect_receipts_job_id ON side_effect_receipts(job_id);

CREATE TABLE IF NOT EXISTS job_outbox (
    id UUID PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES jobs(id),
    topic VARCHAR(100) NOT NULL,
    message_key VARCHAR(200) NOT NULL,
    payload_json TEXT NOT NULL,
    headers_json TEXT NOT NULL DEFAULT '{}',
    status VARCHAR(40) NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    not_before TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    published_at TIMESTAMPTZ,
    last_error TEXT
);

CREATE INDEX IF NOT EXISTS ix_job_outbox_pending_due
    ON job_outbox(status, not_before, created_at)
    WHERE status = 'PENDING';

CREATE INDEX IF NOT EXISTS ix_job_outbox_job_id ON job_outbox(job_id);

CREATE TABLE IF NOT EXISTS job_attempts (
    id UUID PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES jobs(id),
    attempt INTEGER NOT NULL,
    status VARCHAR(40) NOT NULL,
    topic VARCHAR(100) NOT NULL,
    error_message TEXT,
    failure_class VARCHAR(200),
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    CONSTRAINT uq_job_attempts_job_attempt UNIQUE (job_id, attempt)
);

CREATE INDEX IF NOT EXISTS ix_job_attempts_job_id ON job_attempts(job_id, attempt DESC);
