"""
Data synchronization script for ClickHouse.

Imports SCID tick data into ClickHouse with pipeline parallelism
for maximum ingest speed.
"""

import json
import time
import threading
import queue
from pathlib import Path
from typing import Dict, Optional
import datetime

from parser import SCIDParser
from clickhouse_manager import ClickHouseManager, ClickHouseConfig
from config import Config


class ClickHouseCheckpoint:
    """
    Manages checkpoint state for incremental imports to ClickHouse.
    Tracks last position per file to support resumable imports.
    """

    def __init__(self, checkpoint_path: str = None):
        if checkpoint_path is None:
            checkpoint_path = Path(__file__).parent / "checkpoint_clickhouse.json"

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


def process_contract(
    config: ClickHouseConfig,
    symbol: str,
    contract_file: str,
    start_date_str: Optional[str],
    end_date_str: Optional[str],
    batch_size: int,
    table_name: str
) -> Dict:
    """
    Process a single contract file with pipeline parallelism.
    One thread parses while main thread inserts.
    """
    db = ClickHouseManager(config)
    db.connect()

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

        # Parse dates
        s_date = datetime.datetime.strptime(start_date_str, "%Y-%m-%d").replace(tzinfo=datetime.timezone.utc) if start_date_str else None
        e_date = datetime.datetime.strptime(end_date_str, "%Y-%m-%d").replace(tzinfo=datetime.timezone.utc) if end_date_str else None

        print(f"Starting ClickHouse import: {Path(contract_file).name} -> {table_name} (pipelined)")

        # Queue for batches (max 3 batches buffered to limit memory)
        batch_queue = queue.Queue(maxsize=3)
        parse_done = threading.Event()
        parse_error = [None]

        def parser_thread():
            """Background thread that reads and parses SCID file into batches."""
            try:
                parser = SCIDParser(contract_file)
                batch = []
                count = 0

                for record in parser.read_records(start_date=s_date, end_date=e_date, buffer_size=256*1024):
                    batch.append(record.to_db_tuple())
                    count += 1

                    if len(batch) >= batch_size:
                        batch_queue.put((batch, count))
                        batch = []

                # Put remaining batch
                if batch:
                    batch_queue.put((batch, count))

            except Exception as e:
                parse_error[0] = e
            finally:
                parse_done.set()

        # Start parser thread
        parser_t = threading.Thread(target=parser_thread, daemon=True)
        parser_t.start()

        # Main loop consumes batches and inserts
        last_print = 0

        while True:
            if parse_done.is_set() and batch_queue.empty():
                break

            try:
                batch, count = batch_queue.get(timeout=0.1)
            except queue.Empty:
                continue

            stats["processed"] = count
            inserted = db.insert_records(table_name, batch)
            stats["inserted"] += inserted

            if stats["processed"] - last_print >= 100000:
                print(f"[{symbol}] {Path(contract_file).name}: {stats['processed']:,} rows processed")
                last_print = stats["processed"]

        # Wait for parser thread to finish
        parser_t.join()

        if parse_error[0]:
            raise parse_error[0]

        stats["elapsed"] = time.time() - start_time
        print(f"Completed {Path(contract_file).name}: {stats['inserted']:,} inserted in {stats['elapsed']:.2f}s")

    except Exception as e:
        print(f"Error processing {contract_file}: {e}")
    finally:
        db.close()

    return stats


class ClickHouseSync:
    """
    Synchronizes SCID tick data to ClickHouse.
    Reads from configured SCID files and inserts into ES/NQ tables.
    """

    def __init__(self, config: Config = None, checkpoint: ClickHouseCheckpoint = None):
        self.config = config or Config()
        self.checkpoint = checkpoint or ClickHouseCheckpoint()

    def sync_symbol(
        self,
        symbol: str,
        batch_size: int = 100000,
        progress_interval: int = 100000
    ) -> Dict:
        """
        Sync all contracts for a symbol.
        """
        sym_config = self.config.get_symbol_config(symbol)
        if not sym_config:
            print(f"No configuration found for symbol: {symbol}")
            return {"error": f"No config for {symbol}"}

        table_name = sym_config.table_name
        print(f"\n{'='*60}")
        print(f"Syncing {symbol} to ClickHouse ({len(sym_config.contracts)} contracts)")
        print(f"Table: {table_name}")
        print(f"{'='*60}")

        start_time = time.time()

        # ClickHouse config from app config
        ch_config = ClickHouseConfig(
            host=self.config.database.host,
            port=9000,  # ClickHouse native port
        )

        total_processed = 0
        total_inserted = 0

        for contract_cfg in sym_config.contracts:
            file_path = contract_cfg.file

            # Check checkpoint
            if self.checkpoint.is_completed(symbol, file_path):
                print(f"Skipping {Path(file_path).name} (already completed)")
                continue

            result = process_contract(
                ch_config,
                symbol,
                file_path,
                contract_cfg.start_date,
                contract_cfg.end_date,
                batch_size,
                table_name
            )

            total_processed += result['processed']
            total_inserted += result['inserted']

            if result['processed'] > 0 and result['inserted'] > 0:
                # Mark as completed
                self.checkpoint.set_completed(symbol, file_path, True)
                self.checkpoint.save()
            else:
                print(f"Failed to sync {Path(file_path).name} (no rows inserted)")

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
        print(f"Summary for {symbol} (ClickHouse):")
        print(f"  Total Processed: {total_processed:,}")
        print(f"  Total Inserted:  {total_inserted:,}")
        print(f"  Time Elapsed:    {elapsed:.2f} seconds")
        print(f"  Rate:            {stats['records_per_second']:,.0f} records/sec")
        print(f"{'='*60}")

        return stats

    def sync_all(self, batch_size: int = 100000) -> Dict:
        results = {}
        for symbol in self.config.get_all_symbols():
            results[symbol] = self.sync_symbol(symbol, batch_size=batch_size)
        return results


def main():
    """Main entry point for ClickHouse data synchronization."""
    import argparse

    parser = argparse.ArgumentParser(description="Sync SCID tick data to ClickHouse")
    parser.add_argument("--symbol", "-s", help="Symbol to sync (ES, NQ). If not specified, syncs all.")
    parser.add_argument("--batch-size", "-b", type=int, default=100000, help="Batch size for inserts (default: 100000)")
    parser.add_argument("--config", "-c", help="Path to config.json")

    args = parser.parse_args()

    config = Config(args.config) if args.config else Config()
    sync = ClickHouseSync(config=config)

    if args.symbol:
        sync.sync_symbol(args.symbol, batch_size=args.batch_size)
    else:
        sync.sync_all(batch_size=args.batch_size)


if __name__ == "__main__":
    main()
