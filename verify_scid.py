from resample_scid import resample_scid_to_1min
import os

test_file = r"C:\SierraChart\Data\ESZ25_FUT_CME.scid"

if os.path.exists(test_file):
    # Enabling config filter to verify it reads from config.json
    # We remove the limit so it can find data within the contract's specific range
    df = resample_scid_to_1min(test_file, price_multiplier=0.01, use_config=True)

    if df is not None:
        print("\n--- Resampled 1-Minute Data (Filtered by config range) ---")
        print(df.head(10))
        print("...")
        print(df.tail(10))
        print("\nVerification successful!")
else:
    print(f"File not found: {test_file}")
    print("Please ensure the path is correct.")
