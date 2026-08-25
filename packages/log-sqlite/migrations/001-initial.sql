-- Vesper SQLite append-only conversation log schema.
-- `next_sequence` and `seq` are text so JS drivers cannot round them through
-- IEEE-754 numbers. Offsets remain the canonical ordering key.
CREATE TABLE IF NOT EXISTS vesper_log_streams (
  path TEXT PRIMARY KEY,
  identity TEXT NOT NULL,
  epoch INTEGER NOT NULL DEFAULT 0,
  producer_id TEXT,
  next_sequence TEXT NOT NULL DEFAULT '0',
  next_producer_sequence INTEGER NOT NULL DEFAULT 0,
  last_fingerprint TEXT NOT NULL DEFAULT '',
  last_offset TEXT NOT NULL DEFAULT '-1'
);

CREATE TABLE IF NOT EXISTS vesper_log_records (
  path TEXT NOT NULL,
  seq TEXT NOT NULL,
  record_offset TEXT NOT NULL,
  producer_id TEXT NOT NULL,
  producer_epoch INTEGER NOT NULL,
  producer_sequence INTEGER NOT NULL,
  batch_index INTEGER NOT NULL,
  conversation_id TEXT NOT NULL,
  record_timestamp INTEGER NOT NULL,
  record TEXT NOT NULL,
  PRIMARY KEY (path, record_offset)
);

CREATE UNIQUE INDEX IF NOT EXISTS vesper_log_records_producer_batch_unique
  ON vesper_log_records (path, producer_id, producer_epoch, producer_sequence, batch_index);
CREATE INDEX IF NOT EXISTS vesper_log_records_path_offset_idx
  ON vesper_log_records (path, record_offset);
