import sys
import pandas as pd
import numpy as np
from pathlib import Path

def read_h5(file_path, key=None):
    """
    Read a DataFrame from an HDF5 file.
    Supports standard pandas.to_hdf (via 'tables') and the h5py fallback format.
    If key is None, it tries 'ticks' and 'data'.
    """
    if not Path(file_path).exists():
        print(f"Error: {file_path} not found")
        return None

    # Common keys to try if none provided
    potential_keys = [key] if key else ['ticks', 'data']

    try:
        # 1. Try standard pandas reading (requires 'tables')
        import tables

        # Open file to check keys if not provided
        if key is None:
            with pd.HDFStore(file_path, mode='r') as store:
                # store.keys() returns keys starting with /
                store_keys = [k.lstrip('/') for k in store.keys()]
                for pk in ['ticks', 'data']:
                    if pk in store_keys:
                        key = pk
                        break
                if key is None and store_keys:
                    key = store_keys[0] # Fallback to first available

        target_key = key or 'ticks' # Default if still None
        df = pd.read_hdf(file_path, key=target_key)
        print(f"Successfully read HDF5 (key='{target_key}') via tables: {file_path}")
        return df
    except ImportError:
        # 2. Fallback to reading the custom h5py structure
        try:
            import h5py
            with h5py.File(file_path, 'r') as hf:
                if key is None:
                    # Try to find a valid key
                    for pk in ['ticks', 'data']:
                        if pk in hf:
                            key = pk
                            break
                    if key is None and list(hf.keys()):
                        key = list(hf.keys())[0]

                target_key = key or 'ticks'
                if target_key not in hf:
                    print(f"Error: Key '{target_key}' not found in HDF5 file. Available keys: {list(hf.keys())}")
                    return None

                group = hf[target_key]
                values = group['values'][:]
                columns = group['columns'][:].astype(str)
                index_raw = group['index'][:]

                # Convert index back to datetime (assuming nanoseconds view(np.int64))
                index = pd.to_datetime(index_raw, unit='ns', utc=True)

                df = pd.DataFrame(values, columns=columns, index=index)
                df.index.name = 'DateTime'

                print(f"Successfully read HDF5 (key='{target_key}') via h5py fallback: {file_path}")
                return df
        except ImportError:
            print("Error: Neither 'tables' nor 'h5py' is available to read the HDF5 file.")
            return None
        except Exception as e:
            print(f"Error reading HDF5 via h5py: {e}")
            return None
    except Exception as e:
        if key is None:
             print(f"Error reading HDF5: {e}")
        else:
             print(f"Error reading HDF5 (key='{key}'): {e}")
        return None

def convert_h5_to_csv(h5_path, csv_path, key=None):
    print(f"Converting {h5_path} to {csv_path}...")
    df = read_h5(h5_path, key=key)

    if df is not None:
        df.to_csv(csv_path)
        print(f"Successfully saved to {csv_path}")
        print("\n--- First 5 rows ---")
        print(df.head())
    else:
        print("Conversion failed.")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python h5_to_csv.py <input_h5_file> <output_csv_file> [key]")
        print("Note: The script will automatically try 'ticks' and 'data' if [key] is omitted.")

        # Default test if no arguments
        test_input = "ESZ25_test.h5"
        test_output = "ESZ25_reconverted.csv"
        if Path(test_input).exists():
            print(f"\nRunning default test: {test_input} -> {test_output}")
            convert_h5_to_csv(test_input, test_output)
    else:
        h5_file = sys.argv[1]
        csv_file = sys.argv[2]
        key = sys.argv[3] if len(sys.argv) > 3 else None
        convert_h5_to_csv(h5_file, csv_file, key=key)
