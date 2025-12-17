-- Compress all chunks for ES and NQ hypertables
-- Run this after completing a large data import

-- Compress all ES chunks
SELECT compress_chunk(c, if_not_compressed => true)
FROM show_chunks('"ES"') c;

-- Compress all NQ chunks (if table exists)
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM timescaledb_information.hypertables WHERE hypertable_name = 'NQ') THEN
        PERFORM compress_chunk(c, if_not_compressed => true)
        FROM show_chunks('"NQ"') c;
    END IF;
END $$;

-- Show compression results
SELECT
    hypertable_name,
    chunk_name,
    is_compressed,
    pg_size_pretty(before_compression_total_bytes) AS before_compression,
    pg_size_pretty(after_compression_total_bytes) AS after_compression
FROM timescaledb_information.chunks
WHERE hypertable_name IN ('ES', 'NQ')
ORDER BY hypertable_name, range_start;

-- Show total hypertable sizes
SELECT
    hypertable_name,
    pg_size_pretty(hypertable_size(format('"%s"', hypertable_name))) AS total_size
FROM timescaledb_information.hypertables
WHERE hypertable_name IN ('ES', 'NQ');
