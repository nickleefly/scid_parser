# SCID Database Manager

This project manages the import of Sierra Chart Intraday Data (.scid) into a TimescaleDB (PostgreSQL) database.

| Feature | TimescaleDB | ClickHouse | HDF5 / Parquet |
| :--- | :--- | :--- | :--- |
| **Disk Size (100M Ticks)** | ~400 MB (Compressed) | ~300 MB | ~500 MB |
| **Ingest Speed** | High (50k-100k rows/sec) | Extreme (Millions/sec) | N/A (File I/O speed) |
| **Query/Backtest Speed** | Fast | Instant | Instant (after load) |
| **SQL Support** | Full PostgreSQL | Good (Analytical) | No (Python/API only) |
| **Management** | Easy (It's just Postgres) | Medium (Cluster mgmt) | Zero (It's a file) |
| **Use Case** | Complex queries, long-term storage | High-speed analytics | Quick local research |

## File Overview

| File | Description |
| :--- | :--- |
| `data_sync.py` | Main entry point for syncing SCID data to TimescaleDB. Supports resume and contract rollovers. |
| `clickhouse_sync.py` | Main entry point for syncing SCID data to ClickHouse. Optimized for extreme ingestion speed. |
| `resample_scid.py` | High-performance script using `numpy.memmap` to convert SCID ticks to 1-minute OHLCV bars (CSV/HDF5). |
| `h5_to_csv.py` | Utility to convert resampled HDF5 files (.h5) back to CSV format. |
| `parser.py` | Core binary parser for Sierra Chart SCID files; handles date conversion and bundle trade markers. |
| `db_manager.py` | Manages TimescaleDB connections, schema, and high-speed `COPY` commands. |
| `clickhouse_manager.py` | Manages ClickHouse connections and schema management. |
| `config.py` | Loads and validates your `config.json` setup. |
| `verify_scid.py` | Quick utility to verify your SCID path and date range settings from `config.json`. |
| `scid_to_h5_ticks.py` | Exports raw tick data from SCID to HDF5, respecting `config.json` date ranges. |

## How to Run

### 1. Prerequisites
- **Docker**: Used to run the TimescaleDB database.
- **Python 3.8+**: Required for the import scripts.

### 2. Installation
Install the required Python packages using `uv`:
```bash
uv sync
```
*Tip: `uv` will automatically manage your virtual environment and install all dependencies listed in `pyproject.toml`.*

### 3. Database Setup
Start the PostgreSQL/TimescaleDB container:
```bash
docker-compose up -d
```
The database will be available at `localhost:5433` with user/password `postgres`/`postgres`.

**Note**: If this is a fresh installation or you are migrating existing data to TimescaleDB, you may need to run the migration script:
```bash
# Read the file and pipe it:
cat psql/migrate-to-timescale.sql | docker-compose exec -T index-postgresql psql -U postgres -d future_index
```

### 4. Configuration
Edit `config.json` to point to your local SCID files and define the date ranges for each contract.
```json
{
    "database": { ... },
    "symbols": {
        "ES": {
            "contracts": [
                {
                    "file": "C:\\Path\\To\\ESH18.scid",
                    "start_date": "2017-12-14",
                    "end_date": "2018-03-08"
                }
            ]
        }
    }
}
```

### 5. Running the Import
Run the synchronization script to parse SCID files and insert data into the database.
```bash
# Sync all symbols defined in config.json
python data_sync.py

# Sync a specific symbol only
python data_sync.py --symbol ES
```
The script supports resuming from where it left off (via `checkpoint.json`).

### 6. Fast Import Workflow (Recommended for Large Imports)
For fastest import speed, disable compression before importing and re-enable after:

```bash
# Step 1: Prepare database (disable compression policies)
cat psql/prepare_import.sql | docker-compose exec -T index-postgresql psql -U postgres -d future_index

# Step 2: Run the import
python data_sync.py

# Step 3: Finalize (re-enable compression and compress all data)
cat psql/finalize_import.sql | docker-compose exec -T index-postgresql psql -U postgres -d future_index
```

**Why this is faster:** Inserting into compressed TimescaleDB chunks requires decompress → insert → recompress, which is slow. Disabling compression during import avoids this overhead.

## Database Verification & Monitoring

Useful commands to check the status of your TimescaleDB instance.

### 1. Check Table Size (Usage)
View the total size of the `ES` table, including all compressed chunks.
```bash
docker-compose exec index-postgresql psql -U postgres -d future_index -c "SELECT pg_size_pretty(hypertable_size('\"ES\"'));"
```

### 2. Check Compression Status
See if chunks are compressed and how many.
```bash
# Count compressed chunks
docker-compose exec index-postgresql psql -U postgres -d future_index -c "SELECT count(*) FROM timescaledb_information.chunks WHERE is_compressed = true;"

# Detailed compression stats per chunk
docker-compose exec index-postgresql psql -U postgres -d future_index -c "SELECT chunk_name, is_compressed, range_start, range_end FROM timescaledb_information.chunks WHERE hypertable_name = 'ES' ORDER BY range_start DESC LIMIT 5;"
```

### 3. Check Record Counts
Count total rows in the table.
```bash
docker-compose exec index-postgresql psql -U postgres -d future_index -c 'SELECT count(*) FROM "ES";'
```

### 4. Check Ingestion Progress
Watch the database for active queries (useful during imports).
```bash
docker-compose exec index-postgresql psql -U postgres -d future_index -c "SELECT pid, state, now() - query_start as duration, query FROM pg_stat_activity WHERE state != 'idle';"
```

### 5. View Sample Data
View the most recent 5 records from the `ES` table.
```bash

docker-compose exec index-postgresql psql -U postgres -d future_index -c "SELECT * FROM \"ES\" ORDER BY datetime DESC LIMIT 5;"
```

---

## Resampling & Format Conversion

This project provides utilities for high-performance data processing outside of the database workflows.

### 1-Minute OHLCV Resampling
Use `resample_scid.py` to generate 1-minute bars from raw SCID tick data. It uses memory mapping for extreme speed and supports CSV or HDF5 output.

**Command Structure:**
```bash
python resample_scid.py <input_scid_file> [output_file.csv|.h5] [price_multiplier]
```

**Terminal Examples:**
```bash
# Basic conversion to CSV with 0.01 multiplier (for ES/NQ)
python resample_scid.py C:\\SierraChart\\Data\\ESZ25_FUT_CME.scid ESZ25_1min.csv 0.01

# Conversion to compact HDF5 format
python resample_scid.py C:\\SierraChart\\Data\\ESZ25_FUT_CME.scid ESZ25_1min.h5 0.01
```

> [!TIP]
> If you don't provide an output path, the script will perform the conversion in memory and display the first/last few rows for verification.

### HDF5 to CSV Conversion
If you use the compact HDF5 format for storage, use `h5_to_csv.py` to convert it back to CSV. The script automatically detects if the file contains raw ticks (from `scid_to_h5_ticks.py`) or resampled bars (from `resample_scid.py`).

**Command Structure:**
```bash
python h5_to_csv.py <input_h5_file> <output_csv_file> [key]
```

**Terminal Example:**
```bash
# Automatically detects 'ticks' or 'data' key
python h5_to_csv.py ESZ25_ticks.h5 ESZ25_ticks.csv

# Or specify a custom key if needed
python h5_to_csv.py ESZ25_test.h5 ESZ25_reconverted.csv data
```

### Raw Tick Export
Use `scid_to_h5_ticks.py` to export every single tick within your `config.json` date range to a high-performance HDF5 file.

**Command Structure:**
```bash
python scid_to_h5_ticks.py <input_scid_file> <output_h5_file> [price_multiplier]
```

**Terminal Example:**
```bash
python scid_to_h5_ticks.py C:\\SierraChart\\Data\\ESZ25_FUT_CME.scid ESZ25_ticks.h5 0.01
```

---

## Maintenance Tasks

### Manual Compression
The database is configured to automatically compress data older than 7 days. To manually compress a specific chunk (e.g., if you just imported old data):
```sql
-- Inside psql session
CALL run_job((SELECT job_id FROM timescaledb_information.jobs WHERE proc_name = 'policy_compression'));
```

### Refresh Constraints
If you need to update constraints, remember that TimescaleDB hypertables require unique constraints to include the partitioning key (`datetime`).

```sql
ALTER TABLE "ES" ADD CONSTRAINT es_unique_tick UNIQUE (datetime, raw_time);
```

---

## ClickHouse Setup (Alternative)

ClickHouse offers extreme ingest speeds (millions of rows/sec) and is optimized for analytical queries.

### 1. Start ClickHouse Container
```bash
docker-compose up -d index-clickhouse
```
The database will be available at `localhost:8123` (HTTP) and `localhost:9000` (native).

### 2. Install ClickHouse Driver
```bash
pip install clickhouse-driver
```

### 3. Run Import to ClickHouse
```bash
# Sync all symbols
python clickhouse_sync.py

# Sync specific symbol
python clickhouse_sync.py --symbol ES
```
Progress is tracked in `checkpoint_clickhouse.json` (separate from TimescaleDB checkpoint).

### 4. Verify Data
```bash
# Check record count
docker-compose exec index-clickhouse clickhouse-client --query "SELECT count() FROM future_index.ES"

# View sample data
docker-compose exec index-clickhouse clickhouse-client --query "SELECT * FROM future_index.ES ORDER BY datetime DESC LIMIT 5"

# Check table size
docker-compose exec index-clickhouse clickhouse-client --query "SELECT formatReadableSize(sum(bytes_on_disk)) FROM system.parts WHERE database = 'future_index'"
```

### 5. Deduplication
Tables use `ReplacingMergeTree` engine for safe resume. If you stop mid-import and restart, duplicates may exist temporarily. Run this to deduplicate:
```bash
docker-compose exec index-clickhouse clickhouse-client --query "OPTIMIZE TABLE future_index.ES FINAL"
```
