-- +goose Up
-- +goose StatementBegin
CREATE TYPE pgqueue_status AS ENUM (
    'new',
    'in_progress',
    'retry',
    'failed',
    'succeeded'
    );
-- +goose StatementEnd

-- +goose StatementBegin
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
) WITH (fillfactor = 75);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE UNIQUE INDEX pgqueue_key_kind_unique_idx ON pgqueue (key, kind)
    WHERE key IS NOT NULL;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX pgqueue_waiting_tasks_idx ON pgqueue (kind, next_attempt_time)
    WHERE status in ('new', 'retry');
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE pgqueue;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TYPE pgqueue_status;
-- +goose StatementEnd
