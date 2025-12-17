-- Finalize import: Re-enable compression policy and compress all data
-- Run this AFTER python data_sync.py completes

-- 1. Re-add compression policies (compress data older than 7 days)
DO $$
DECLARE
    policy_exists BOOLEAN;
BEGIN
    -- Add ES compression policy if not exists
    SELECT EXISTS(
        SELECT 1 FROM timescaledb_information.jobs
        WHERE hypertable_name = 'ES' AND proc_name = 'policy_compression'
    ) INTO policy_exists;

    IF NOT policy_exists THEN
        -- Check if ES table exists and is a hypertable
        IF EXISTS (SELECT 1 FROM timescaledb_information.hypertables WHERE hypertable_name = 'ES') THEN
            PERFORM add_compression_policy('"ES"', INTERVAL '7 days');
            RAISE NOTICE 'Added compression policy for ES (7 days)';
        ELSE
            RAISE NOTICE 'ES is not a hypertable, skipping compression policy';
        END IF;
    ELSE
        RAISE NOTICE 'Compression policy already exists for ES';
    END IF;

    -- Add NQ compression policy if not exists
    SELECT EXISTS(
        SELECT 1 FROM timescaledb_information.jobs
        WHERE hypertable_name = 'NQ' AND proc_name = 'policy_compression'
    ) INTO policy_exists;

    IF NOT policy_exists THEN
        IF EXISTS (SELECT 1 FROM timescaledb_information.hypertables WHERE hypertable_name = 'NQ') THEN
            PERFORM add_compression_policy('"NQ"', INTERVAL '7 days');
            RAISE NOTICE 'Added compression policy for NQ (7 days)';
        ELSE
            RAISE NOTICE 'NQ is not a hypertable, skipping compression policy';
        END IF;
    ELSE
        RAISE NOTICE 'Compression policy already exists for NQ';
    END IF;
END $$;

-- 2. Compress all uncompressed chunks immediately
SELECT 'Compressing ES chunks...' AS status;
SELECT compress_chunk(c, if_not_compressed => true)
FROM show_chunks('"ES"') c;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM timescaledb_information.hypertables WHERE hypertable_name = 'NQ') THEN
        PERFORM compress_chunk(c, if_not_compressed => true)
        FROM show_chunks('"NQ"') c;
    END IF;
END $$;

-- 3. Show compression results
SELECT
    hypertable_name,
    COUNT(*) FILTER (WHERE is_compressed = true) AS compressed_chunks,
    COUNT(*) FILTER (WHERE is_compressed = false) AS uncompressed_chunks,
    COUNT(*) AS total_chunks
FROM timescaledb_information.chunks
WHERE hypertable_name IN ('ES', 'NQ')
GROUP BY hypertable_name;

-- 4. Show final sizes
SELECT
    hypertable_name,
    pg_size_pretty(hypertable_size(format('"%s"', hypertable_name))) AS total_size
FROM timescaledb_information.hypertables
WHERE hypertable_name IN ('ES', 'NQ');

SELECT 'Import finalized! Compression policies restored and all data compressed.' AS status;
