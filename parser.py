"""
SCID Parser for PostgreSQL Import

Parses Sierra Chart Intraday Data (.scid) files for importing into PostgreSQL.
Supports multiple contract files with per-contract date filtering for rollovers.

Bundle Trade Markers in Open field:
- First trade in bundle: -19990009513251226345509817234554355712.00
- Last trade in bundle:  -19990019654456028171345029208179998720.00
- Regular trade: 0.00
"""

import struct
import os
import datetime
import re
from dataclasses import dataclass
from typing import Generator, Tuple, Optional, List, Dict
from pathlib import Path

# Constants
HEADER_FORMAT = '<4s2I2HI36s'
HEADER_SIZE = 56
RECORD_FORMAT = '<Q4f4I'
RECORD_SIZE = 40
SC_EPOCH = datetime.datetime(1899, 12, 30, tzinfo=datetime.timezone.utc)

# Bundle trade markers
FIRST_BUNDLE_TRADE = -19990009513251226345509817234554355712.0
LAST_BUNDLE_TRADE = -19990019654456028171345029208179998720.0


@dataclass
class SCIDHeader:
    """SCID file header structure."""
    file_type_id: str
    header_size: int
    record_size: int
    version: int
    unused1: int
    utc_start_index: int
    reserve: bytes


@dataclass
class SCIDRecord:
    """SCID tick record with all OHLC fields."""
    datetime: datetime.datetime
    raw_time: int
    open: float
    high: float
    low: float
    close: float
    num_trades: int
    volume: int
    bid_volume: int
    ask_volume: int
    contract: str = ""

    def to_db_tuple(self) -> tuple:
        """Convert to tuple for database insertion."""
        return (
            self.datetime,
            self.raw_time,
            self.open,
            self.high,
            self.low,
            self.close,
            self.num_trades,
            self.volume,
            self.bid_volume,
            self.ask_volume,
            self.contract
        )

    @property
    def is_first_bundle(self) -> bool:
        """Check if this is the first trade in a bundle."""
        return abs(self.open - FIRST_BUNDLE_TRADE) < 1e10

    @property
    def is_last_bundle(self) -> bool:
        """Check if this is the last trade in a bundle."""
        return abs(self.open - LAST_BUNDLE_TRADE) < 1e10

    @property
    def is_regular_trade(self) -> bool:
        """Check if this is a regular (non-bundled) trade."""
        return abs(self.open) < 1e10


class SCIDParser:
    """
    Parses a single Sierra Chart SCID file.

    Efficiently streams records using a generator for memory efficiency.
    """

    def __init__(self, file_path: str, contract: str = None):
        """
        Initialize parser.

        Args:
            file_path: Path to the SCID file
            contract: Optional contract name override. If not provided,
                      extracts from filename (e.g., ESZ24 from ESZ24_FUT_CME.scid)
        """
        self.file_path = file_path
        self.header: Optional[SCIDHeader] = None

        # Extract contract from filename if not provided
        if contract:
            self.contract = contract
        else:
            self.contract = self._extract_contract_from_path(file_path)

    @staticmethod
    def _extract_contract_from_path(file_path: str) -> str:
        """Extract contract symbol from SCID filename."""
        filename = Path(file_path).stem  # e.g., ESZ24_FUT_CME
        # Match pattern like ESZ24, NQH25, etc.
        match = re.match(r'^([A-Z]{2,3}[A-Z]\d{2})', filename)
        if match:
            return match.group(1)
        # Fallback: use first part before underscore
        parts = filename.split('_')
        return parts[0] if parts else filename

    @staticmethod
    def _convert_sc_timestamp(sc_time_val: int) -> datetime.datetime:
        """
        Convert Sierra Chart 64-bit timestamp to datetime.
        The value is microseconds since 1899-12-30.
        """
        return SC_EPOCH + datetime.timedelta(microseconds=sc_time_val)

    def parse_header(self, f) -> SCIDHeader:
        """Read and parse the 56-byte file header."""
        data = f.read(HEADER_SIZE)

        if len(data) < HEADER_SIZE:
            raise ValueError("File is too small to contain a valid header.")

        unpacked = struct.unpack(HEADER_FORMAT, data)

        self.header = SCIDHeader(
            file_type_id=unpacked[0].decode('utf-8').rstrip('\x00'),
            header_size=unpacked[1],
            record_size=unpacked[2],
            version=unpacked[3],
            unused1=unpacked[4],
            utc_start_index=unpacked[5],
            reserve=unpacked[6]
        )
        return self.header

    def read_records(
        self,
        start_date: Optional[datetime.datetime] = None,
        end_date: Optional[datetime.datetime] = None,
        offset: int = 0
    ) -> Generator[SCIDRecord, None, None]:
        """
        Generator that yields SCIDRecord objects.

        Args:
            start_date: Optional start date filter (inclusive)
            end_date: Optional end date filter (exclusive)
            offset: Byte offset to start reading from (for resuming)

        Yields:
            SCIDRecord objects matching the date filter
        """
        if not os.path.exists(self.file_path):
            raise FileNotFoundError(f"File not found: {self.file_path}")

        with open(self.file_path, 'rb') as f:
            # Parse header
            self.parse_header(f)

            # Seek to offset if provided (must be >= HEADER_SIZE)
            if offset > HEADER_SIZE:
                f.seek(offset)

            # Read records
            while True:
                buffer = f.read(RECORD_SIZE)

                if len(buffer) < RECORD_SIZE:
                    break

                # Unpack: <Q4f4I = little-endian, uint64, 4 floats, 4 uint32
                fields = struct.unpack(RECORD_FORMAT, buffer)

                raw_time = fields[0]
                dt_val = self._convert_sc_timestamp(raw_time)

                # Apply date filters
                if start_date and dt_val < start_date:
                    continue
                if end_date and dt_val >= end_date:
                    continue

                record = SCIDRecord(
                    datetime=dt_val,
                    raw_time=raw_time,
                    open=fields[1],
                    high=fields[2],
                    low=fields[3],
                    close=fields[4],
                    num_trades=fields[5],
                    volume=fields[6],
                    bid_volume=fields[7],
                    ask_volume=fields[8],
                    contract=self.contract
                )

                yield record

    def get_file_position(self, f) -> int:
        """Get current file position for checkpointing."""
        return f.tell()


class MultiContractParser:
    """
    Handles parsing multiple SCID files for the same symbol (contract rollovers).

    Each contract can have its own start/end date for proper rollover handling.
    """

    def __init__(self, contracts: List[Dict]):
        """
        Initialize with list of contract configurations.

        Args:
            contracts: List of contract configs, each with:
                - file: Path to SCID file
                - start_date: Start date string (YYYY-MM-DD) or datetime
                - end_date: End date string or datetime (optional, None for current)
        """
        self.contracts = contracts

    @staticmethod
    def _parse_date(date_val) -> Optional[datetime.datetime]:
        """Parse date string or return datetime as-is."""
        if date_val is None:
            return None
        if isinstance(date_val, datetime.datetime):
            return date_val
        if isinstance(date_val, str):
            dt = datetime.datetime.strptime(date_val, "%Y-%m-%d")
            return dt.replace(tzinfo=datetime.timezone.utc)
        return None

    def read_all_records(self) -> Generator[SCIDRecord, None, None]:
        """
        Yield records from all contract files in chronological order.

        Records are filtered by each contract's start/end dates.
        """
        for contract_config in self.contracts:
            file_path = contract_config.get('file')
            start_date = self._parse_date(contract_config.get('start_date'))
            end_date = self._parse_date(contract_config.get('end_date'))

            if not file_path or not os.path.exists(file_path):
                print(f"Warning: SCID file not found: {file_path}")
                continue

            parser = SCIDParser(file_path)

            for record in parser.read_records(start_date=start_date, end_date=end_date):
                yield record


# --- Usage Example ---
if __name__ == "__main__":
    # Example: Parse a single SCID file
    test_file = r"C:\SierraChart\Data\ESZ25_FUT_CME.scid"

    if os.path.exists(test_file):
        parser = SCIDParser(test_file)

        # Start from a specific date
        start = datetime.datetime(2025, 9, 15, tzinfo=datetime.timezone.utc)

        count = 0
        for record in parser.read_records(start_date=start):
            if count == 0:
                print(f"Contract: {record.contract}")
                print("-" * 100)
                print(f"{'DateTime':40} | {'Open':15} | {'High':10} | {'Low':10} | {'Close':10} | {'Vol':6}")
                print("-" * 100)

            print(
                f"{record.datetime} | "
                f"{record.open:15.2f} | "
                f"{record.high:10.2f} | "
                f"{record.low:10.2f} | "
                f"{record.close:10.2f} | "
                f"{record.volume:6}"
            )

            count += 1
            if count >= 10:
                break

        print(f"\nDisplayed {count} records")
    else:
        print(f"Test file not found: {test_file}")
        print("Update the test_file path to run the example.")
