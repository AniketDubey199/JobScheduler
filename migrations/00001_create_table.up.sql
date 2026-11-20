create extension if not exists "pgcrypto";

create table jobs (
    id UUID primary key default gen_random_uuid(),
    type varchar(100) not null,
    payload JSONB not null,
    status varchar(20) not null default 'Pending' check (status in ('Pending', 'Running' , 'Completed' , 'Failed', 'Dead')),
    idempotency_key varchar(50) unique,
    scheduled_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_jobs_status_created_at ON jobs (status, created_at DESC);

CREATE INDEX idx_jobs_stuck_running ON jobs (started_at) WHERE status = 'Running';