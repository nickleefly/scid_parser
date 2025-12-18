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

### 1-Minute Aggregation

To efficiently query 1-minute OHLCV candles from your tick data:

> **Note:** In SCID tick data, the `open` field acts as a marker (often 0 or a large negative number for bundle indicators), while the `close` field holds the actual trade price. Therefore, we use `close` to calculate the Open, High, and Low of the candle.

```sql
SELECT
    candle_time,
    open,
    high,
    low,
    close,
    volume,
    trades
FROM future_index.ES_1min
WHERE candle_time >= '2018-01-01' AND candle_time < '2018-01-02'
ORDER BY candle_time ASC;
```

#### Materialized View (Recommended for Speed)

For instant results on massive datasets, create a Materialized View that pre-calculates this in the background.

```sql
```sql
-- 1. Create the target table for storing 1-min bars
CREATE TABLE future_index.ES_1min
(
    candle_time DateTime,
    open Float64,
    high Float64,
    low Float64,
    close Float64,
    volume UInt64,
    trades UInt64
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(candle_time)
ORDER BY candle_time;

-- 2. Create the Materialized View to update it automatically
CREATE MATERIALIZED VIEW future_index.ES_1min_mv TO future_index.ES_1min
AS SELECT
    toStartOfMinute(datetime) AS candle_time,
    argMin(close, (datetime, raw_time)) AS open,
    max(high) AS high,
    min(low) AS low,
    argMax(close, (datetime, raw_time)) AS close,
    sum(volume) AS volume,
    sum(num_trades) AS trades
FROM future_index.ES
GROUP BY candle_time;
```

### Backfill History

The Materialized View only processes **newly inserted** data. To populate the table with existing history:

```sql
INSERT INTO future_index.ES_1min
SELECT
    toStartOfMinute(datetime) AS candle_time,
    argMin(close, (datetime, raw_time)) AS open,
    max(high) AS high,
    min(low) AS low,
    argMax(close, (datetime, raw_time)) AS close,
    sum(volume) AS volume,
    sum(num_trades) AS trades
FROM future_index.ES
GROUP BY candle_time;
```
