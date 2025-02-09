-- +goose Up
CREATE TYPE pgqueue_status AS ENUM (
    'new',
    'in_progress',
    'retry',
    'failed',
    'succeeded'
    );

CREATE TABLE pgqueue
(
    id                bigserial primary key,
    kind              smallint                               not null,
    key               text,
    payload           text                                   not null,
    status            pgqueue_status           default 'new' not null,
    attempts_left     smallint                               not null,
    attempts_elapsed  smallint                 default 0     not null,
    next_attempt_time timestamp with time zone,
    created_at        timestamp with time zone default now() not null,
    updated_at        timestamp with time zone default now() not null
) WITH (fillfactor = 90);

CREATE UNIQUE INDEX pgqueue_key_kind_unique_idx ON pgqueue (key, kind)
    WHERE key IS NOT NULL;

CREATE INDEX pgqueue_pending_tasks_idx ON pgqueue (kind, next_attempt_time)
    WHERE status in ('new', 'in_progress', 'retry');

CREATE INDEX pgqueue_clean_tasks_idx ON pgqueue (kind, updated_at)
    WHERE status in ('failed', 'succeeded');

-- +goose Down
DROP TABLE pgqueue;

DROP TYPE pgqueue_status;
