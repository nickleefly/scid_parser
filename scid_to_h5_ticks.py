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

def export_scid_ticks_to_h5(file_path, output_path, start_date=None, end_date=None, limit=None, price_multiplier=1.0, use_config=True):
    """
    Export raw tick data from SCID to HDF5.
    Uses memory mapping and vectorized operations.
    """
    f = Path(file_path)
    if not f.exists():
        print(f"Error: {f} not found")
        return None

    # Constants
    HEADER_SIZE = 56
    RECORD_SIZE = 40
    SC_EPOCH_US = 2209161600000000 # Microseconds from 1970-01-01 back to 1899-12-30

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

    # Use memmap for instant access
    data = np.memmap(f, dtype=sciddtype, offset=HEADER_SIZE, mode="r")

    if limit:
        print(f"Limiting to first {limit:,} records...")
        data = data[:limit]

    # Create DataFrame from memmap
    df = pd.DataFrame(data, copy=False)

    # Try to get dates from config if not provided
    if use_config and not start_date and not end_date:
        start_date, end_date = get_dates_from_config(file_path)

    # Fast filtering by raw timestamp
    if start_date or end_date:
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
    # Filter out rows that are just markers if needed, or just clean the prices.
    # Regular trades have abs(Open) < 1e10
    mask = np.abs(df['Open']) < 1e10
    df_clean = df[mask].copy()

    # Apply Price Multiplier
    if price_multiplier != 1.0:
        print(f"Applying price multiplier: {price_multiplier}")
        for col in ['Open', 'High', 'Low', 'Close']:
            df_clean[col] = df_clean[col] * price_multiplier

    # Sierra Chart often uses 0.0 for regular trades in Open, copy from Close
    df_clean.loc[df_clean['Open'] == 0.0, 'Open'] = df_clean['Close']

    # Convert Time to actual Datetime index
    print(f"Converting timestamps for {len(df_clean):,} tokens...")
    df_clean['DateTime'] = pd.to_datetime(df_clean['Time'] - SC_EPOCH_US, unit='us', utc=True)
    df_clean.set_index('DateTime', inplace=True)

    # Drop the raw Time column
    df_clean.drop(columns=['Time'], inplace=True)

    end_time = time.perf_counter()
    print(f"Processing completed in {end_time - start_time:.4f} seconds.")

    if output_path:
        save_to_hdf5(df_clean, output_path)

    return df_clean

def save_to_hdf5(df, output_path, key='ticks'):
    """
    Save the DataFrame to an HDF5 file.
    """
    try:
        import tables
        df.to_hdf(output_path, key=key, mode='w', format='table', complib='blosc', complevel=9)
        print(f"Saved to HDF5 (via tables): {output_path} (key='{key}')")
    except ImportError:
        try:
            import h5py
            with h5py.File(output_path, 'w') as hf:
                group = hf.create_group(key)
                group.create_dataset('values', data=df.values, compression='gzip', compression_opts=9)
                group.create_dataset('columns', data=df.columns.values.astype('S'), compression='gzip')
                group.create_dataset('index', data=df.index.view(np.int64), compression='gzip')
            print(f"Saved to HDF5 (via h5py fallback): {output_path} (key='{key}')")
        except ImportError:
            print("Error: Could not save to HDF5. Please install 'tables' or 'h5py'.")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python scid_to_h5_ticks.py <input_scid_file> <output_h5_file> [multiplier]")
    else:
        input_file = sys.argv[1]
        output_file = sys.argv[2]
        multiplier = float(sys.argv[3]) if len(sys.argv) > 3 else 1.0
        export_scid_ticks_to_h5(input_file, output_file, price_multiplier=multiplier)
