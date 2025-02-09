-- +goose Up
CREATE TABLE pgqueue_job
(
    name       text primary key,
    updated_at timestamp with time zone default now() not null
);

-- +goose Down
DROP TABLE pgqueue_job;
