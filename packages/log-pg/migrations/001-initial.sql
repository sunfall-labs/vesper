-- Vesper PostgreSQL append-only conversation log schema.
CREATE SCHEMA IF NOT EXISTS ai_log;

CREATE TABLE IF NOT EXISTS ai_log.streams (
  path                   text PRIMARY KEY,
  identity               text NOT NULL,
  epoch                  bigint NOT NULL DEFAULT 0,
  producer_id            text,
  next_sequence          bigint NOT NULL DEFAULT 0,
  next_producer_sequence bigint NOT NULL DEFAULT 0,
  last_fingerprint       text NOT NULL DEFAULT '',
  last_offset            text NOT NULL DEFAULT '-1',
  created_at             timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ai_log.records (
  path              text NOT NULL,
  seq               bigint NOT NULL,
  record_offset     text NOT NULL,
  producer_id       text NOT NULL,
  producer_epoch    bigint NOT NULL,
  producer_sequence bigint NOT NULL,
  batch_index       integer NOT NULL,
  conversation_id   text NOT NULL,
  record_timestamp  bigint NOT NULL,
  record            jsonb NOT NULL,
  written_at        timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT ai_log_records_pkey PRIMARY KEY (path, seq)
);

CREATE UNIQUE INDEX IF NOT EXISTS ai_log_records_producer_batch_unique
  ON ai_log.records (
    path, producer_id, producer_epoch, producer_sequence, batch_index
  );

CREATE INDEX IF NOT EXISTS ai_log_records_path_offset_idx
  ON ai_log.records (path, record_offset COLLATE "C");
