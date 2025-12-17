-- Prepare database for fast import
-- Run this BEFORE python data_sync.py

-- Remove compression policy (stops auto-compression during import)
DO $$
DECLARE
    policy_exists BOOLEAN;
BEGIN
    -- Check and remove ES compression policy
    SELECT EXISTS(
        SELECT 1 FROM timescaledb_information.jobs
        WHERE hypertable_name = 'ES' AND proc_name = 'policy_compression'
    ) INTO policy_exists;

    IF policy_exists THEN
        PERFORM remove_compression_policy('"ES"');
        RAISE NOTICE 'Removed compression policy for ES';
    ELSE
        RAISE NOTICE 'No compression policy found for ES';
    END IF;

    -- Check and remove NQ compression policy
    SELECT EXISTS(
        SELECT 1 FROM timescaledb_information.jobs
        WHERE hypertable_name = 'NQ' AND proc_name = 'policy_compression'
    ) INTO policy_exists;

    IF policy_exists THEN
        PERFORM remove_compression_policy('"NQ"');
        RAISE NOTICE 'Removed compression policy for NQ';
    ELSE
        RAISE NOTICE 'No compression policy found for NQ';
    END IF;
END $$;

-- Decompress any chunks that need new data inserted
-- This is optional but can help if inserting into time ranges with existing compressed data
-- Uncomment if needed:
-- SELECT decompress_chunk(c) FROM show_chunks('"ES"') c
-- WHERE c IN (SELECT chunk_name FROM timescaledb_information.chunks WHERE is_compressed = true);

SELECT 'Database ready for fast import. Run: python data_sync.py' AS status;
