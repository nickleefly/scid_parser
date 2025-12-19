import sys
import os
import json
from pathlib import Path
import numpy as np
import pandas as pd
import time

def get_dates_from_config(file_path, config_path="config.json"):
    """
    Search config.json for a contract matching the given file path.
    Returns (start_date, end_date) if found.
    """
    if not os.path.exists(config_path):
        return None, None

    try:
        with open(config_path, 'r') as f:
            config = json.load(f)

        target_path = str(Path(file_path).absolute()).lower()

        for symbol, data in config.get("symbols", {}).items():
            for contract in data.get("contracts", []):
                contract_file = str(Path(contract.get("file", "")).absolute()).lower()
                if contract_file == target_path:
                    print(f"Found config for {symbol} contract: {contract.get('start_date')} to {contract.get('end_date')}")
                    return contract.get("start_date"), contract.get("end_date")
    except Exception as e:
        print(f"Warning: Could not read config.json: {e}")

    return None, None

def resample_scid_to_1min(file_path, output_path=None, start_date=None, end_date=None, limit=None, price_multiplier=1.0, use_config=True):
    """
    Highly efficient conversion of SCID tick data to 1-minute OHLCV data.
    Uses memory mapping and vectorized operations to maximize performance.
    """
    f = Path(file_path)
    if not f.exists():
        print(f"Error: {f} not found")
        return None

    # Constants
    HEADER_SIZE = 56
    RECORD_SIZE = 40
    SC_EPOCH_US = 2209161600000000 # Microseconds from 1970-01-01 back to 1899-12-30
    MINUTE_US = 60_000_000

    # Bundle trade markers (as defined in parser.py)
    FIRST_BUNDLE_TRADE = -19990009513251226345509817234554355712.0
    LAST_BUNDLE_TRADE = -19990019654456028171345029208179998720.0

    # Precise dtypes matching parser.py (<Q4f4I)
    sciddtype = np.dtype([
        ("Time", "<u8"),      # Q
        ("Open", "<f4"),      # f
        ("High", "<f4"),      # f
        ("Low", "<f4"),       # f
        ("Close", "<f4"),     # f
        ("Trades", "<u4"),    # I
        ("Volume", "<u4"),    # I
        ("BidVolume", "<u4"), # I
        ("AskVolume", "<u4"), # I
    ])

    stat = f.stat()
    file_size = stat.st_size

    if file_size < HEADER_SIZE:
        print(f"Error: {f} is too small to be a valid SCID file")
        return None

    print(f"Loading {f.name} ({file_size / 1024 / 1024:.2f} MB)...")
    start_time = time.perf_counter()

    # Use memmap for instant access without loading entire file into RAM
    data = np.memmap(f, dtype=sciddtype, offset=HEADER_SIZE, mode="r")

    if limit:
        print(f"Limiting to first {limit:,} records for verification...")
        data = data[:limit]

    # Create DataFrame from memmap (copy=False is critical for performance)
    df = pd.DataFrame(data, copy=False)

    # Try to get dates from config if not provided
    if use_config and not start_date and not end_date:
        start_date, end_date = get_dates_from_config(file_path)

    # Fast filtering by raw timestamp if dates provided
    if start_date or end_date:
        # Convert date strings/objects to SC raw microseconds
        # SC Time = (Unix Time + SC_EPOCH_US)
        def to_raw(dt_str):
            ts = int(pd.to_datetime(dt_str, utc=True).timestamp() * 1_000_000)
            return ts + SC_EPOCH_US

        if start_date:
            raw_start = to_raw(start_date)
            df = df[df['Time'] >= raw_start]
        if end_date:
            raw_end = to_raw(end_date)
            df = df[df['Time'] < raw_end]

    if df.empty:
        print("No data found matching criteria.")
        return None

    # Handle Bundle Trade Markers (SCID specific)
    # The Open field can contain markers like -1.99e37. We must clean these
    # before resampling to avoid corrupting OHLC values.
    # Regular trades have abs(Open) < 1e10
    mask = np.abs(df['Open']) < 1e10

    # For OHLC, we only want rows with valid prices.
    # For Volume/Trades, we keep everything usually, but the markers are typically
    # attached to rows where Volume is valid anyway.
    df_clean = df[mask].copy()

    # Apply Price Multiplier (e.g., 0.01 to convert 653100 to 6531.00)
    if price_multiplier != 1.0:
        print(f"Applying price multiplier: {price_multiplier}")
        for col in ['Open', 'High', 'Low', 'Close']:
            df_clean[col] = df_clean[col] * price_multiplier

    if df_clean.empty:
        print("No valid price data found after filtering bundle markers.")
        return None

    # IMPROVEMENT: Handle 0.0 Open prices
    # Sierra Chart often uses 0.0 for regular trades, with the real price in High/Low/Close.
    # We update Open to match Close for these ticks so the 1-minute Open is accurate.
    df_clean.loc[df_clean['Open'] == 0.0, 'Open'] = df_clean['Close']

    # VECTORIZED OPTIMIZATION:
    # Instead of converting every tick to datetime (slow),
    # we work with integer minutes.
    # Minute index = Time // 60,000,000
    df_clean['MinuteIndex'] = df_clean['Time'] // MINUTE_US

    print(f"Aggregating {len(df_clean):,} valid ticks...")

    # Group by MinuteIndex and aggregate
    resampled = df_clean.groupby('MinuteIndex').agg({
        'Open': 'first',
        'High': 'max',
        'Low': 'min',
        'Close': 'last',
        'Trades': 'sum',
        'Volume': 'sum',
        'BidVolume': 'sum',
        'AskVolume': 'sum'
    })

    # Only now do we convert the resulting minute indices to actual datetimes
    # This happens only once per minute (e.g., 1,440 times per day) instead of per tick
    resampled.index = pd.to_datetime((resampled.index * MINUTE_US) - SC_EPOCH_US, unit='us', utc=True)
    resampled.index.name = 'DateTime'

    end_time = time.perf_counter()
    print(f"Conversion completed in {end_time - start_time:.4f} seconds.")
    print(f"Ticks processed: {len(df_clean):,}")
    print(f"Minutes generated: {len(resampled):,}")

    if output_path:
        out = Path(output_path)
        if out.suffix.lower() in ['.h5', '.hdf5']:
            save_to_hdf5(resampled, output_path)
        else:
            resampled.to_csv(output_path)
            print(f"Saved to {output_path}")

    return resampled

def save_to_hdf5(df, output_path, key='data'):
    """
    Save the resampled DataFrame to an HDF5 file.
    Uses pandas.to_hdf (requires 'tables') if available,
    otherwise falls back to 'h5py' for a custom export.
    """
    try:
        # Preferred method (PyTables)
        import tables
        df.to_hdf(output_path, key=key, mode='w', format='table', complib='blosc', complevel=9)
        print(f"Saved to HDF5 (via tables): {output_path} (key='{key}')")
    except ImportError:
        # Fallback method (h5py)
        try:
            import h5py
            with h5py.File(output_path, 'w') as hf:
                # Save data values
                group = hf.create_group(key)
                group.create_dataset('values', data=df.values, compression='gzip', compression_opts=9)
                # Save column names as UTF-8 strings
                group.create_dataset('columns', data=df.columns.values.astype('S'), compression='gzip')
                # Save index (timestamps) as nanoseconds since Unix Epoch
                # This is a common way to store time in direct HDF5
                group.create_dataset('index', data=df.index.view(np.int64), compression='gzip')

            print(f"Saved to HDF5 (via h5py fallback): {output_path} (key='{key}')")
            print("Note: This fallback format stores [values, columns, index] as separate internal datasets.")
        except ImportError:
            print("Error: Could not save to HDF5. Please install 'tables' or 'h5py'.")
            print("Run: pip install h5py")
        except Exception as e:
            print(f"Error saving to HDF5 via h5py: {e}")
    except Exception as e:
        print(f"Error saving to HDF5 via tables: {e}")

if __name__ == "__main__":
    # Example usage:
    # Adjust this path to a real file on your system
    test_file = r"C:\SierraChart\Data\ESZ25_FUT_CME.scid"
    # df_1min = resample_scid_to_1min(test_file, start_date="2025-09-15", output_path="ESZ25_1min.csv")

    if len(sys.argv) > 1:
        input_file = sys.argv[1]
        output_file = sys.argv[2] if len(sys.argv) > 2 else None
        multiplier = float(sys.argv[3]) if len(sys.argv) > 3 else 1.0
        df = resample_scid_to_1min(input_file, output_path=output_file, price_multiplier=multiplier)
    else:
        print(f"Running with default test file: {test_file}")
        # Automatically save to HDF5 if user wants a quick test of the format
        # test_h5 = "test_export.h5"
        # df = resample_scid_to_1min(test_file, output_path=test_h5, price_multiplier=0.01)
        df = resample_scid_to_1min(test_file, price_multiplier=0.01)
        print("\nUsage example: python resample_scid.py <input_scid_file> [output_file.csv|.h5] [multiplier]")

    if df is not None:
        print("\n--- Resampled 1-Minute Data ---")
        print(df.head(10))
        print("...")
        print(df.tail(10))
