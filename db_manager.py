"""
Database Manager for SCID tick data.

Handles async PostgreSQL connections and batch inserts for ES and NQ tables.
"""

import asyncio
import asyncpg
from typing import List, Optional, Tuple
from dataclasses import dataclass
import datetime


@dataclass
class DBConfig:
    """Database connection configuration."""
    host: str = "localhost"
    port: int = 5432
    user: str = "postgres"
    password: str = "postgres"
    database: str = "future_index"


class DBManager:
    """
    Async PostgreSQL database manager for tick data.

    Provides efficient batch insert operations for ES and NQ tables.
    """

    # Column names for insert operations
    COLUMNS = [
        "datetime", "raw_time", "open", "high", "low", "close",
        "num_trades", "volume", "bid_volume", "ask_volume", "contract"
    ]

    def __init__(self, config: DBConfig = None):
        """
        Initialize database manager.

        Args:
            config: Database configuration. Uses defaults if not provided.
        """
        self.config = config or DBConfig()
        self.pool: Optional[asyncpg.Pool] = None
        self._conn: Optional[asyncpg.Connection] = None

    async def connect(self) -> asyncpg.Connection:
        """
        Establish database connection.

        Returns:
            asyncpg Connection object
        """
        self._conn = await asyncpg.connect(
            host=self.config.host,
            port=self.config.port,
            user=self.config.user,
            password=self.config.password,
            database=self.config.database
        )
        return self._conn

    async def create_pool(self, min_size: int = 5, max_size: int = 20) -> asyncpg.Pool:
        """
        Create a connection pool for concurrent operations.

        Args:
            min_size: Minimum pool connections
            max_size: Maximum pool connections

        Returns:
            asyncpg Pool object
        """
        self.pool = await asyncpg.create_pool(
            host=self.config.host,
            port=self.config.port,
            user=self.config.user,
            password=self.config.password,
            database=self.config.database,
            min_size=min_size,
            max_size=max_size
        )
        return self.pool

    async def close(self) -> None:
        """Close database connection and pool."""
        if self._conn:
            await self._conn.close()
            self._conn = None
        if self.pool:
            await self.pool.close()
            self.pool = None

    async def insert_records(
        self,
        table_name: str,
        records: List[Tuple],
        batch_size: int = 1000
    ) -> int:
        """
        Insert tick records into the specified table.

        Args:
            table_name: Table name (ES or NQ)
            records: List of tuples from SCIDRecord.to_db_tuple()
            batch_size: Number of records per batch insert

        Returns:
            Number of records inserted
        """
        if not records:
            return 0

        conn = self._conn or await self.connect()

        # Build INSERT statement with ON CONFLICT for upsert
        columns = ", ".join(self.COLUMNS)
        placeholders = ", ".join(f"${i+1}" for i in range(len(self.COLUMNS)))

        insert_sql = f"""
            INSERT INTO "{table_name}" ({columns})
            VALUES ({placeholders})
            ON CONFLICT (datetime, raw_time) DO NOTHING
        """

        inserted = 0

        # Process in batches
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            try:
                result = await conn.executemany(insert_sql, batch)
                inserted += len(batch)
            except Exception as e:
                print(f"Error inserting batch {i//batch_size}: {e}")
                # Try inserting records one by one to find problematic ones
                for record in batch:
                    try:
                        await conn.execute(insert_sql, *record)
                        inserted += 1
                    except Exception as record_error:
                        print(f"  Skipping record: {record_error}")

        return inserted

    async def get_last_timestamp(self, table_name: str) -> Optional[int]:
        """
        Get the last raw_time value in the table.

        Args:
            table_name: Table name (ES or NQ)

        Returns:
            Last raw_time value or None if table is empty
        """
        conn = self._conn or await self.connect()

        result = await conn.fetchval(
            f'SELECT MAX(raw_time) FROM "{table_name}"'
        )

        return result

    async def get_record_count(self, table_name: str) -> int:
        """
        Get total record count in the table.

        Args:
            table_name: Table name (ES or NQ)

        Returns:
            Number of records
        """
        conn = self._conn or await self.connect()

        result = await conn.fetchval(
            f'SELECT COUNT(*) FROM "{table_name}"'
        )

        return result or 0

    async def get_date_range(self, table_name: str) -> Tuple[Optional[datetime.datetime], Optional[datetime.datetime]]:
        """
        Get the date range of records in the table.

        Args:
            table_name: Table name (ES or NQ)

        Returns:
            Tuple of (min_datetime, max_datetime) or (None, None) if empty
        """
        conn = self._conn or await self.connect()

        result = await conn.fetchrow(
            f'SELECT MIN(datetime), MAX(datetime) FROM "{table_name}"'
        )

        if result:
            return result[0], result[1]
        return None, None

    async def get_contracts(self, table_name: str) -> List[str]:
        """
        Get list of distinct contracts in the table.

        Args:
            table_name: Table name (ES or NQ)

        Returns:
            List of contract symbols
        """
        conn = self._conn or await self.connect()

        results = await conn.fetch(
            f'SELECT DISTINCT contract FROM "{table_name}" ORDER BY contract'
        )

        return [r['contract'] for r in results if r['contract']]


# --- Usage Example ---
if __name__ == "__main__":
    async def main():
        db = DBManager()

        try:
            await db.connect()
            print("Connected to database")

            for table in ["ES", "NQ"]:
                count = await db.get_record_count(table)
                min_dt, max_dt = await db.get_date_range(table)
                contracts = await db.get_contracts(table)

                print(f"\n{table} Table:")
                print(f"  Records: {count:,}")
                print(f"  Date Range: {min_dt} to {max_dt}")
                print(f"  Contracts: {contracts}")

        finally:
            await db.close()
            print("\nConnection closed")

    asyncio.run(main())
