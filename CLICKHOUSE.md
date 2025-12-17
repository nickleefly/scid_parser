# ClickHouse Setup

Import Sierra Chart SCID tick data into ClickHouse for extreme query speed.

## Quick Start

```bash
# 1. Start container
docker-compose up -d index-clickhouse

# 2. Install driver
pip install clickhouse-driver

# 3. Import data
python clickhouse_sync.py --symbol ES
```

## Verify Data

```bash
# Record count
docker-compose exec index-clickhouse clickhouse-client --query "SELECT count() FROM future_index.ES"

# Sample data
docker-compose exec index-clickhouse clickhouse-client --query "SELECT * FROM future_index.ES ORDER BY datetime DESC LIMIT 5"

# Table size
docker-compose exec index-clickhouse clickhouse-client --query "SELECT formatReadableSize(sum(bytes_on_disk)) FROM system.parts WHERE database = 'future_index'"
```

## Resume & Deduplication

Progress is saved per-file in `checkpoint_clickhouse.json`. If you stop mid-file and restart:

```bash
# Deduplicate after restart
docker-compose exec index-clickhouse clickhouse-client --query "OPTIMIZE TABLE future_index.ES FINAL"
```

Tables use `ReplacingMergeTree` engine — duplicates are removed during OPTIMIZE or background merges.

## Data Storage

Data is stored on the Windows host at `D:/clickhousedb`. Note that while running ClickHouse on Windows host mounts can have performance implications or issues with atomic renames, it allows for direct data access.

## Popular Commands

Here is a cheat sheet of useful commands for managing your ClickHouse instance.

### System & Status

```sql
-- Check Server Version and Uptime
SELECT version(), uptime();

-- Show Running Queries (Process List)
SHOW PROCESSLIST;

-- Kill a long running query
KILL QUERY WHERE query_id = 'query_id_here';
```

### Disk Usage & Sizes

```sql
-- Database Sizes
SELECT database, formatReadableSize(sum(bytes_on_disk)) AS size
FROM system.parts
GROUP BY database;

-- Table Sizes and Row Counts
SELECT
    table,
    formatReadableSize(sum(bytes_on_disk)) AS size,
    sum(rows) AS rows
FROM system.parts
WHERE active
GROUP BY table;

-- Compression Ratio & Column Sizes (Great for optimization)
SELECT
    column,
    formatReadableSize(sum(column_data_compressed_bytes)) AS compressed,
    formatReadableSize(sum(column_data_uncompressed_bytes)) AS uncompressed,
    round(sum(column_data_uncompressed_bytes) / sum(column_data_compressed_bytes), 2) AS ratio
FROM system.parts_columns
WHERE table = 'ES' AND active
GROUP BY column
ORDER BY sum(column_data_compressed_bytes) DESC;
```

### Partition Management

The `ES` table is partitioned by month (`YYYYMM`).

```sql
-- List Partitions
SELECT partition, name, rows, formatReadableSize(bytes_on_disk) AS size
FROM system.parts
WHERE table = 'ES' AND active
ORDER BY partition DESC;

-- Drop a specific month of data
-- ALTER TABLE future_index.ES DROP PARTITION '202312';

-- Detach a partition (safely remove from queryable state without deleting)
-- ALTER TABLE future_index.ES DETACH PARTITION '202312';
```

### CLI Quick Reference

```bash
# Enter interactive SQL shell
docker exec -it index-clickhouse clickhouse-client

# Run a query from command line and save to CSV
docker exec index-clickhouse clickhouse-client --query "SELECT * FROM future_index.ES LIMIT 1000" --format CSV > export.csv

# Import from CSV (example)
# Get-Content data.csv | docker exec -i index-clickhouse clickhouse-client --query "INSERT INTO future_index.ES FORMAT CSV"
```
