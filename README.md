# SCID Database Manager

This project manages the import of Sierra Chart Intraday Data (.scid) into a TimescaleDB (PostgreSQL) database.

## How to Run

### 1. Prerequisites
- **Docker**: Used to run the TimescaleDB database.
- **Python 3.8+**: Required for the import scripts.

### 2. Installation
Install the required Python packages:
```bash
pip install -r requirements.txt
```

### 3. Database Setup
Start the PostgreSQL/TimescaleDB container:
```bash
docker-compose up -d
```
The database will be available at `localhost:5433` with user/password `postgres`/`postgres`.

**Note**: If this is a fresh installation or you are migrating existing data to TimescaleDB, you may need to run the migration script:
```bash
docker-compose exec index-postgresql psql -U postgres -d future_index -f /migrate-to-timescale.sql
# Note: You'll need to copy the SQL file into the container or mount it first if it's not already available.
# Alternatively, read the file and pipe it:
cat migrate-to-timescale.sql | docker-compose exec -T index-postgresql psql -U postgres -d future_index
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
