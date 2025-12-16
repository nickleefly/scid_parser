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
        # Allow unlimited decompression for inserts into compressed chunks
        try:
            await self._conn.execute("SET timescaledb.max_tuples_decompressed_per_dml_transaction = 0;")
        except Exception:
            # Ignore if parameter doesn't exist (e.g. standard Postgres)
            pass
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
        batch_size: int = 10000
    ) -> int:
        """
        Insert tick records into the specified table using COPY for high performance.

        Uses a temporary staging table to handle ON CONFLICT DO NOTHING requirements
        while leveraging the speed of the COPY protocol.

        Args:
            table_name: Table name (ES or NQ)
            records: List of tuples from SCIDRecord.to_db_tuple()
            batch_size: Not used in COPY mode but kept for compatibility

        Returns:
            Number of records inserted (approximate, as ON CONFLICT ignores duplicates)
        """
        if not records:
            return 0

        conn = self._conn or await self.connect()

        # Create a unique temp table name
        temp_table = f"temp_{table_name.lower()}_{id(records)}"

        # Columns string for the query
        columns_str = ", ".join(self.COLUMNS)

        try:
            # 1. Create temporary staging table
            # We use LIKE to copy structure, but make it UNLOGGED for speed
            await conn.execute(f"""
                CREATE TEMP TABLE IF NOT EXISTS "{temp_table}"
                (LIKE "{table_name}" INCLUDING DEFAULTS)
            """)

            # 2. Bulk load data into staging table using COPY
            # This is significantly faster than INSERT
            await conn.copy_records_to_table(
                temp_table,
                records=records,
                columns=self.COLUMNS
            )

            # 3. Move from staging to actual table with conflict handling
            # This preserves the idempotency required for resuming imports
            query = f"""
                INSERT INTO "{table_name}" ({columns_str})
                SELECT {columns_str}
                FROM "{temp_table}"
                ON CONFLICT (datetime, raw_time) DO NOTHING
            """

            result = await conn.execute(query)

            # Extract number of inserted rows from command tag (e.g., "INSERT 0 100")
            inserted_count = 0
            if result:
                parts = result.split()
                if len(parts) > 2:
                    inserted_count = int(parts[-1])

            return inserted_count

        except Exception as e:
            print(f"Error during bulk insert: {e}")
            # Fallback to slow insert if COPY fails (e.g., type mismatch)
            # This ensures robustness
            return await self._fallback_insert(conn, table_name, records)
        finally:
            # Clean up temp table
            try:
                await conn.execute(f'DROP TABLE IF EXISTS "{temp_table}"')
            except:
                pass

    async def _fallback_insert(
        self,
        conn: asyncpg.Connection,
        table_name: str,
        records: List[Tuple]
    ) -> int:
        """Fallback slow insert method for error recovery."""
        columns = ", ".join(self.COLUMNS)
        placeholders = ", ".join(f"${i+1}" for i in range(len(self.COLUMNS)))

        insert_sql = f"""
            INSERT INTO "{table_name}" ({columns})
            VALUES ({placeholders})
            ON CONFLICT (datetime, raw_time) DO NOTHING
        """

        inserted = 0
        for record in records:
            try:
                await conn.execute(insert_sql, *record)
                inserted += 1
            except Exception as e:
                print(f"  Skipping bad record: {e}")
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
