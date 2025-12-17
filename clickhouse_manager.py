"""
ClickHouse Database Manager for SCID tick data.

Handles ClickHouse connections and batch inserts for ES and NQ tables.
Uses the native protocol for maximum insert speed.
"""

from typing import List, Optional, Tuple
from dataclasses import dataclass
import datetime

from clickhouse_driver import Client


@dataclass
class ClickHouseConfig:
    """ClickHouse connection configuration."""
    host: str = "localhost"
    port: int = 9000
    database: str = "future_index"
    user: str = "default"
    password: str = ""


class ClickHouseManager:
    """
    ClickHouse database manager for tick data.

    Provides efficient batch insert operations for ES and NQ tables.
    Uses native protocol for maximum performance.
    """

    # Column names for insert operations
    COLUMNS = [
        "datetime", "raw_time", "open", "high", "low", "close",
        "num_trades", "volume", "bid_volume", "ask_volume", "contract"
    ]

    def __init__(self, config: ClickHouseConfig = None):
        """
        Initialize ClickHouse manager.

        Args:
            config: Database configuration. Uses defaults if not provided.
        """
        self.config = config or ClickHouseConfig()
        self._client: Optional[Client] = None

    def connect(self) -> Client:
        """
        Establish ClickHouse connection.

        Returns:
            clickhouse_driver Client object
        """
        self._client = Client(
            host=self.config.host,
            port=self.config.port,
            database=self.config.database,
            user=self.config.user,
            password=self.config.password,
            settings={
                'insert_block_size': 1000000,
                'max_insert_block_size': 1000000,
            }
        )
        return self._client

    def close(self) -> None:
        """Close ClickHouse connection."""
        if self._client:
            self._client.disconnect()
            self._client = None

    def insert_records(
        self,
        table_name: str,
        records: List[Tuple],
        batch_size: int = 100000
    ) -> int:
        """
        Insert tick records into the specified table.

        Uses native protocol bulk insert for maximum speed.

        Args:
            table_name: Table name (ES or NQ)
            records: List of tuples from SCIDRecord.to_db_tuple()
            batch_size: Not used, kept for API compatibility

        Returns:
            Number of records inserted
        """
        if not records:
            return 0

        client = self._client or self.connect()

        # Convert records to list of dicts for clickhouse-driver
        columns_str = ", ".join(self.COLUMNS)

        # Insert using native protocol (fastest method)
        client.execute(
            f"INSERT INTO {table_name} ({columns_str}) VALUES",
            records,
            types_check=True
        )

        return len(records)

    def get_record_count(self, table_name: str) -> int:
        """
        Get total record count in the table.

        Args:
            table_name: Table name (ES or NQ)

        Returns:
            Number of records
        """
        client = self._client or self.connect()
        result = client.execute(f"SELECT count() FROM {table_name}")
        return result[0][0] if result else 0

    def get_date_range(self, table_name: str) -> Tuple[Optional[datetime.datetime], Optional[datetime.datetime]]:
        """
        Get the date range of records in the table.

        Args:
            table_name: Table name (ES or NQ)

        Returns:
            Tuple of (min_datetime, max_datetime) or (None, None) if empty
        """
        client = self._client or self.connect()
        result = client.execute(f"SELECT min(datetime), max(datetime) FROM {table_name}")

        if result and result[0][0]:
            return result[0][0], result[0][1]
        return None, None

    def get_contracts(self, table_name: str) -> List[str]:
        """
        Get list of distinct contracts in the table.

        Args:
            table_name: Table name (ES or NQ)

        Returns:
            List of contract symbols
        """
        client = self._client or self.connect()
        results = client.execute(f"SELECT DISTINCT contract FROM {table_name} ORDER BY contract")
        return [r[0] for r in results if r[0]]


# --- Usage Example ---
if __name__ == "__main__":
    db = ClickHouseManager()

    try:
        db.connect()
        print("Connected to ClickHouse")

        for table in ["ES", "NQ"]:
            try:
                count = db.get_record_count(table)
                min_dt, max_dt = db.get_date_range(table)
                contracts = db.get_contracts(table)

                print(f"\n{table} Table:")
                print(f"  Records: {count:,}")
                print(f"  Date Range: {min_dt} to {max_dt}")
                print(f"  Contracts: {contracts}")
            except Exception as e:
                print(f"\n{table} Table: Not accessible ({e})")

    finally:
        db.close()
        print("\nConnection closed")
