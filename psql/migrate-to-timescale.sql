-- TimescaleDB In-Place Migration Script
-- Run this on existing PostgreSQL database with data
--
-- Usage:
-- docker-compose exec index-postgresql psql -U postgres -d future_index -f /path/to/migrate-to-timescale.sql
-- OR copy into container and run

-- Step 1: Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Step 2: Prepare tables by dropping constraints that don't include partitioning key
-- TimescaleDB requires unique constraints to include the partitioning column (datetime)
ALTER TABLE "ES" DROP CONSTRAINT IF EXISTS "ES_pkey";
ALTER TABLE "ES" DROP CONSTRAINT IF EXISTS es_unique_tick;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'NQ') THEN
        ALTER TABLE "NQ" DROP CONSTRAINT IF EXISTS "NQ_pkey";
        ALTER TABLE "NQ" DROP CONSTRAINT IF EXISTS nq_unique_tick;
    END IF;
END $$;

-- Step 3: Convert ES table to hypertable
-- This migrates existing data into chunks
SELECT create_hypertable(
    '"ES"',
    'datetime',
    chunk_time_interval => INTERVAL '1 month',
    migrate_data => true,
    if_not_exists => true
);

-- Step 4: Convert NQ table to hypertable (if exists with data)
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'NQ') THEN
        PERFORM create_hypertable(
            '"NQ"',
            'datetime',
            chunk_time_interval => INTERVAL '1 month',
            migrate_data => true,
            if_not_exists => true
        );
    END IF;
END $$;

-- Step 5: Re-add constraints as composite keys (including datetime)
ALTER TABLE "ES" ADD PRIMARY KEY (datetime, id);
ALTER TABLE "ES" ADD CONSTRAINT es_unique_tick UNIQUE (datetime, raw_time);

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM timescaledb_information.hypertables WHERE hypertable_name = 'NQ') THEN
        ALTER TABLE "NQ" ADD PRIMARY KEY (datetime, id);
        ALTER TABLE "NQ" ADD CONSTRAINT nq_unique_tick UNIQUE (datetime, raw_time);
    END IF;
END $$;

-- Step 6: Enable compression on ES
ALTER TABLE "ES" SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'contract',
    timescaledb.compress_orderby = 'datetime DESC'
);

-- Step 7: Enable compression on NQ (if exists)
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM timescaledb_information.hypertables WHERE hypertable_name = 'NQ') THEN
        EXECUTE 'ALTER TABLE "NQ" SET (
            timescaledb.compress,
            timescaledb.compress_segmentby = ''contract'',
            timescaledb.compress_orderby = ''datetime DESC''
        )';
    END IF;
END $$;

-- Step 8: Add compression policies (compress chunks older than 7 days)
SELECT add_compression_policy('"ES"', INTERVAL '7 days', if_not_exists => true);

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM timescaledb_information.hypertables WHERE hypertable_name = 'NQ') THEN
        PERFORM add_compression_policy('"NQ"', INTERVAL '7 days', if_not_exists => true);
    END IF;
END $$;

-- Step 9: Manually compress all existing chunks
SELECT compress_chunk(c) FROM show_chunks('"ES"') c;

