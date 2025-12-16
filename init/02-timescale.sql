-- TimescaleDB Extension and Hypertable Configuration
-- Runs after 01-schema.sql creates the base tables

-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Convert ES table to hypertable
-- Using datetime as the time column for time-series partitioning
SELECT create_hypertable('"ES"', 'datetime',
    chunk_time_interval => INTERVAL '1 month',
    migrate_data => true,
    if_not_exists => true
);

-- Convert NQ table to hypertable
SELECT create_hypertable('"NQ"', 'datetime',
    chunk_time_interval => INTERVAL '1 month',
    migrate_data => true,
    if_not_exists => true
);

-- Enable compression on ES table
ALTER TABLE "ES" SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'contract',
    timescaledb.compress_orderby = 'datetime DESC'
);

-- Enable compression on NQ table
ALTER TABLE "NQ" SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'contract',
    timescaledb.compress_orderby = 'datetime DESC'
);

-- Create compression policy: compress chunks older than 7 days
SELECT add_compression_policy('"ES"', INTERVAL '7 days', if_not_exists => true);
SELECT add_compression_policy('"NQ"', INTERVAL '7 days', if_not_exists => true);

-- Optional: View hypertable information after setup
-- SELECT * FROM timescaledb_information.hypertables;
-- SELECT * FROM timescaledb_information.compression_settings;
