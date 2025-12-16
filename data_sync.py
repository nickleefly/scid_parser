import asyncio
import json
import time
import concurrent.futures
from pathlib import Path
from typing import Dict, List, Optional
import datetime
import multiprocessing

from parser import SCIDParser, MultiContractParser, SCIDRecord
from db_manager import DBManager, DBConfig
from config import Config


def process_contract_worker(
    db_config_data: dict,
    symbol: str,
    contract_file: str,
    start_date_str: Optional[str],
    end_date_str: Optional[str],
    batch_size: int,
    table_name: str
) -> Dict:
    """
    Worker function to process a single contract file in a separate process.
    Initializes its own DB connection and Event Loop.
    """
    async def _async_work():
        db_cfg = DBConfig(**db_config_data)
        db = DBManager(db_cfg)
        await db.connect()

        stats = {
            "contract": Path(contract_file).name,
            "processed": 0,
            "inserted": 0,
            "elapsed": 0.0
        }

        try:
            if not Path(contract_file).exists():
                print(f"File not found: {contract_file}")
                return stats

            start_time = time.time()
            parser = SCIDParser(contract_file)

            # Parse dates
            s_date = datetime.datetime.strptime(start_date_str, "%Y-%m-%d").replace(tzinfo=datetime.timezone.utc) if start_date_str else None
            e_date = datetime.datetime.strptime(end_date_str, "%Y-%m-%d").replace(tzinfo=datetime.timezone.utc) if end_date_str else None

            print(f"Starting import: {Path(contract_file).name} -> {table_name}")

            batch = []

            # Use a local counter for progress updates in this process
            last_print = 0

            for record in parser.read_records(start_date=s_date, end_date=e_date, buffer_size=256*1024):
                batch.append(record.to_db_tuple())
                stats["processed"] += 1

                if len(batch) >= batch_size:
                    inserted = await db.insert_records(table_name, batch)
                    stats["inserted"] += inserted
                    batch = []

                    if stats["processed"] - last_print >= 100000:
                        print(f"[{symbol}] {Path(contract_file).name}: {stats['processed']:,} rows processed")
                        last_print = stats["processed"]

            if batch:
                inserted = await db.insert_records(table_name, batch)
                stats["inserted"] += inserted

            stats["elapsed"] = time.time() - start_time
            print(f"Completed {Path(contract_file).name}: {stats['inserted']:,} inserted in {stats['elapsed']:.2f}s")

        except Exception as e:
            print(f"Error processing {contract_file}: {e}")
        finally:
            await db.close()

        return stats

    # Run the async worker in this process
    return asyncio.run(_async_work())


class Checkpoint:
    """
    Manages checkpoint state for incremental imports.
    Tracks last position per file to support resumable imports.
    """

    def __init__(self, checkpoint_path: str = None):
        if checkpoint_path is None:
            checkpoint_path = Path(__file__).parent / "checkpoint.json"

        self.path = Path(checkpoint_path)
        self._data: Dict = {}
        self.load()

    def load(self) -> None:
        if self.path.exists():
            with open(self.path, 'r') as f:
                self._data = json.load(f)
        else:
            self._data = {}

    def save(self) -> None:
        with open(self.path, 'w') as f:
            json.dump(self._data, f, indent=4)

    def set_completed(self, symbol: str, file_path: str, completed: bool = True) -> None:
        filename = Path(file_path).name
        if symbol not in self._data:
            self._data[symbol] = {"files": {}}
        if "files" not in self._data[symbol]:
            self._data[symbol]["files"] = {}
        if filename not in self._data[symbol]["files"]:
            self._data[symbol]["files"][filename] = {}
        self._data[symbol]["files"][filename]["completed"] = completed

    def is_completed(self, symbol: str, file_path: str) -> bool:
        """Check if a file has already been completed for a symbol."""
        filename = Path(file_path).name
        if symbol not in self._data:
            return False
        if "files" not in self._data[symbol]:
            return False
        if filename not in self._data[symbol]["files"]:
            return False
        return self._data[symbol]["files"][filename].get("completed", False)


class DataSync:
    """
    Synchronizes SCID tick data to PostgreSQL.
    Reads from configured SCID files and inserts into ES/NQ tables.
    """

    def __init__(self, config: Config = None, checkpoint: Checkpoint = None):
        self.config = config or Config()
        self.checkpoint = checkpoint or Checkpoint()

    async def sync_symbol(
        self,
        symbol: str,
        batch_size: int = 100000,
        progress_interval: int = 100000
    ) -> Dict:
        """
        Sync all contracts for a symbol in PARALLEL.
        """
        sym_config = self.config.get_symbol_config(symbol)
        if not sym_config:
            print(f"No configuration found for symbol: {symbol}")
            return {"error": f"No config for {symbol}"}

        table_name = sym_config.table_name
        print(f"\n{'='*60}")
        print(f"Syncing {symbol} ({len(sym_config.contracts)} contracts) in PARALLEL")
        print(f"Table: {table_name}")
        print(f"{'='*60}")

        start_time = time.time()

        # Prepare arguments for workers
        tasks = []
        db_config_dict = {
            "host": self.config.database.host,
            "port": self.config.database.port,
            "user": self.config.database.user,
            "password": self.config.database.password,
            "database": self.config.database.database
        }

        loop = asyncio.get_running_loop()
        # Limit workers to CPU count to avoid thrashing
        max_workers = min(multiprocessing.cpu_count(), len(sym_config.contracts))

        with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for contract_cfg in sym_config.contracts:
                file_path = contract_cfg.file

                # Check checkpoint
                if self.checkpoint.is_completed(symbol, file_path):
                    print(f"Skipping {Path(file_path).name} (already completed)")
                    continue

                # Create a task for each file
                future = loop.run_in_executor(
                    executor,
                    process_contract_worker,
                    db_config_dict,
                    symbol,
                    file_path,
                    contract_cfg.start_date,
                    contract_cfg.end_date,
                    batch_size,
                    table_name
                )
                futures.append(future)

            # Wait for all files to complete
            results = await asyncio.gather(*futures)

        # Aggregating results
        total_processed = sum(r['processed'] for r in results)
        total_inserted = sum(r['inserted'] for r in results)

        # Update checkpoints
        for contract_cfg in sym_config.contracts:
            self.checkpoint.set_completed(symbol, contract_cfg.file, True)
        self.checkpoint.save()

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
        print(f"  Rate:            {stats['records_per_second']:,.0f} records/sec (Aggregate)")
        print(f"{'='*60}")

        return stats

    async def sync_all(self, batch_size: int = 100000) -> Dict:
        results = {}
        for symbol in self.config.get_all_symbols():
            results[symbol] = await self.sync_symbol(symbol, batch_size=batch_size)
        return results


async def main():
    """Main entry point for data synchronization."""
    import argparse

    parser = argparse.ArgumentParser(description="Sync SCID tick data to PostgreSQL")
    parser.add_argument("--symbol", "-s", help="Symbol to sync (ES, NQ). If not specified, syncs all.")
    parser.add_argument("--batch-size", "-b", type=int, default=100000, help="Batch size for inserts (default: 100000)")
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
