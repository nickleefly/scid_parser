"""
Data Synchronization for SCID to PostgreSQL.

Imports tick data from SCID files into ES and NQ tables.
Supports checkpointing for incremental updates.
"""

import asyncio
import json
import time
from pathlib import Path
from typing import Dict, List, Optional
import datetime

from parser import SCIDParser, MultiContractParser, SCIDRecord
from db_manager import DBManager, DBConfig
from config import Config


class Checkpoint:
    """
    Manages checkpoint state for incremental imports.

    Tracks last position per file to support resumable imports.
    """

    def __init__(self, checkpoint_path: str = None):
        """
        Initialize checkpoint manager.

        Args:
            checkpoint_path: Path to checkpoint.json
        """
        if checkpoint_path is None:
            checkpoint_path = Path(__file__).parent / "checkpoint.json"

        self.path = Path(checkpoint_path)
        self._data: Dict = {}
        self.load()

    def load(self) -> None:
        """Load checkpoint from file."""
        if self.path.exists():
            with open(self.path, 'r') as f:
                self._data = json.load(f)
        else:
            self._data = {}

    def save(self) -> None:
        """Save checkpoint to file."""
        with open(self.path, 'w') as f:
            json.dump(self._data, f, indent=4)

    def get_last_position(self, symbol: str, file_path: str) -> int:
        """Get last processed position for a file."""
        filename = Path(file_path).name
        return self._data.get(symbol, {}).get("files", {}).get(filename, {}).get("last_position", 0)

    def set_last_position(self, symbol: str, file_path: str, position: int) -> None:
        """Set last processed position for a file."""
        filename = Path(file_path).name

        if symbol not in self._data:
            self._data[symbol] = {"files": {}}
        if "files" not in self._data[symbol]:
            self._data[symbol]["files"] = {}
        if filename not in self._data[symbol]["files"]:
            self._data[symbol]["files"][filename] = {}

        self._data[symbol]["files"][filename]["last_position"] = position
        self._data[symbol]["files"][filename]["last_updated"] = datetime.datetime.now().isoformat()

    def is_completed(self, symbol: str, file_path: str) -> bool:
        """Check if a file has been fully processed."""
        filename = Path(file_path).name
        return self._data.get(symbol, {}).get("files", {}).get(filename, {}).get("completed", False)

    def set_completed(self, symbol: str, file_path: str, completed: bool = True) -> None:
        """Mark a file as completed."""
        filename = Path(file_path).name

        if symbol not in self._data:
            self._data[symbol] = {"files": {}}
        if "files" not in self._data[symbol]:
            self._data[symbol]["files"] = {}
        if filename not in self._data[symbol]["files"]:
            self._data[symbol]["files"][filename] = {}

        self._data[symbol]["files"][filename]["completed"] = completed


class DataSync:
    """
    Synchronizes SCID tick data to PostgreSQL.

    Reads from configured SCID files and inserts into ES/NQ tables.
    """

    def __init__(self, config: Config = None, checkpoint: Checkpoint = None):
        """
        Initialize data synchronizer.

        Args:
            config: Configuration object
            checkpoint: Checkpoint manager
        """
        self.config = config or Config()
        self.checkpoint = checkpoint or Checkpoint()

        db_config = self.config.database
        self.db = DBManager(DBConfig(
            host=db_config.host,
            port=db_config.port,
            user=db_config.user,
            password=db_config.password,
            database=db_config.database
        ))

    async def sync_symbol(
        self,
        symbol: str,
        batch_size: int = 10000,
        progress_interval: int = 100000
    ) -> Dict:
        """
        Sync all contracts for a symbol.

        Args:
            symbol: Symbol name (ES, NQ)
            batch_size: Records per database batch insert
            progress_interval: Records between progress updates

        Returns:
            Dict with import statistics
        """
        sym_config = self.config.get_symbol_config(symbol)
        if not sym_config:
            print(f"No configuration found for symbol: {symbol}")
            return {"error": f"No config for {symbol}"}

        table_name = sym_config.table_name
        print(f"\n{'='*60}")
        print(f"Syncing {symbol} ({len(sym_config.contracts)} contracts)")
        print(f"Table: {table_name}")
        print(f"{'='*60}")

        total_inserted = 0
        total_processed = 0
        start_time = time.time()

        await self.db.connect()

        try:
            for contract_cfg in sym_config.contracts:
                file_path = contract_cfg.file

                if not Path(file_path).exists():
                    print(f"\nWarning: File not found: {file_path}")
                    continue

                # Parse contract name from file
                parser = SCIDParser(file_path)
                contract_name = parser.contract

                print(f"\n--- Contract: {contract_name} ---")
                print(f"File: {file_path}")
                print(f"Date filter: {contract_cfg.start_date} to {contract_cfg.end_date}")

                # Parse dates
                start_date = None
                end_date = None

                if contract_cfg.start_date:
                    start_date = datetime.datetime.strptime(
                        contract_cfg.start_date, "%Y-%m-%d"
                    ).replace(tzinfo=datetime.timezone.utc)

                if contract_cfg.end_date:
                    end_date = datetime.datetime.strptime(
                        contract_cfg.end_date, "%Y-%m-%d"
                    ).replace(tzinfo=datetime.timezone.utc)

                # Collect records in batches
                batch: List[tuple] = []
                contract_count = 0

                for record in parser.read_records(start_date=start_date, end_date=end_date):
                    batch.append(record.to_db_tuple())
                    contract_count += 1
                    total_processed += 1

                    # Insert batch when full
                    if len(batch) >= batch_size:
                        inserted = await self.db.insert_records(table_name, batch)
                        total_inserted += inserted
                        batch = []

                    # Progress update
                    if total_processed % progress_interval == 0:
                        elapsed = time.time() - start_time
                        rate = total_processed / elapsed if elapsed > 0 else 0
                        print(f"  Progress: {total_processed:,} processed, {total_inserted:,} inserted ({rate:,.0f} rec/sec)")

                # Insert remaining records
                if batch:
                    inserted = await self.db.insert_records(table_name, batch)
                    total_inserted += inserted

                print(f"  Contract {contract_name}: {contract_count:,} records")

                # Mark as completed
                self.checkpoint.set_completed(symbol, file_path, True)

            self.checkpoint.save()

        finally:
            await self.db.close()

        elapsed = time.time() - start_time

        stats = {
            "symbol": symbol,
            "table": table_name,
            "processed": total_processed,
            "inserted": total_inserted,
            "elapsed_seconds": elapsed,
            "records_per_second": total_processed / elapsed if elapsed > 0 else 0
        }

        print(f"\n{'='*60}")
        print(f"Summary for {symbol}:")
        print(f"  Total Processed: {total_processed:,}")
        print(f"  Total Inserted:  {total_inserted:,}")
        print(f"  Time Elapsed:    {elapsed:.2f} seconds")
        print(f"  Rate:            {stats['records_per_second']:,.0f} records/sec")
        print(f"{'='*60}")

        return stats

    async def sync_all(self, batch_size: int = 10000) -> Dict:
        """
        Sync all configured symbols.

        Args:
            batch_size: Records per database batch insert

        Returns:
            Dict with statistics for all symbols
        """
        results = {}

        for symbol in self.config.get_all_symbols():
            results[symbol] = await self.sync_symbol(symbol, batch_size=batch_size)

        return results


async def main():
    """Main entry point for data synchronization."""
    import argparse

    parser = argparse.ArgumentParser(description="Sync SCID tick data to PostgreSQL")
    parser.add_argument("--symbol", "-s", help="Symbol to sync (ES, NQ). If not specified, syncs all.")
    parser.add_argument("--batch-size", "-b", type=int, default=10000, help="Batch size for inserts")
    parser.add_argument("--config", "-c", help="Path to config.json")

    args = parser.parse_args()

    config = Config(args.config) if args.config else Config()
    sync = DataSync(config=config)

    if args.symbol:
        await sync.sync_symbol(args.symbol, batch_size=args.batch_size)
    else:
        await sync.sync_all(batch_size=args.batch_size)


if __name__ == "__main__":
    asyncio.run(main())
