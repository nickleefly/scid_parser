import pandas as pd
import glob
import os
import gc
import numpy as np
import time
from pathlib import Path
from datetime import timedelta

# --- Configuration ---
TARGETS = [4, 7.5, 10, 15, 20] # Targets to analyze for "MAE on Winners"
FILE_PATTERN = "*_ticks.h5"

def get_trade_stats(day_data):
    if len(day_data) < 2:
        return None

    # session_start is 09:30:00 (Local New York Time)
    current_date = day_data.index[0].date()
    session_start = pd.Timestamp(year=current_date.year, month=current_date.month, day=current_date.day,
                                 hour=9, minute=30, second=0).tz_localize('US/Eastern')
    or_end_time = session_start + timedelta(seconds=30)
    session_end = session_start.replace(hour=16, minute=0, second=0, microsecond=0)
    time_stop_limit = session_start.replace(hour=11, minute=30, second=0)

    # 1. Slice Opening Range (Exactly 9:30:00 to 9:30:30)
    or_slice = day_data[(day_data.index >= session_start) & (day_data.index <= or_end_time)]

    # RTH slice for Daily High/Low (9:30:00 to 16:00:00)
    rth_slice = day_data[(day_data.index >= session_start) & (day_data.index <= session_end)]

    # post_or_slice starts strictly AFTER the OR
    post_or_slice = day_data[(day_data.index > or_end_time) & (day_data.index <= session_end)]

    if or_slice.empty or rth_slice.empty:
        return None

    # 2. Determine Levels
    or_high = or_slice['Price'].max()
    or_low = or_slice['Price'].min()
    or_width = or_high - or_low
    daily_high = rth_slice['Price'].max()
    daily_low = rth_slice['Price'].min()

    # 3. Find First Breakout
    break_up_mask = post_or_slice['Price'] > or_high
    break_down_mask = post_or_slice['Price'] < or_low

    t_up = post_or_slice.index[break_up_mask].min() if break_up_mask.any() else pd.NaT
    t_down = post_or_slice.index[break_down_mask].min() if break_down_mask.any() else pd.NaT

    direction = None
    entry_time = None
    entry_price = 0.0

    if pd.notna(t_up) and pd.notna(t_down):
        if t_up < t_down:
            direction = 'Long'
            entry_time = t_up
            entry_price = or_high
        else:
            direction = 'Short'
            entry_time = t_down
            entry_price = or_low
    elif pd.notna(t_up):
        direction = 'Long'
        entry_time = t_up
        entry_price = or_high
    elif pd.notna(t_down):
        direction = 'Short'
        entry_time = t_down
        entry_price = or_low

    # 4. Strategy Analysis
    mfe = 0.0
    mae = 0.0

    # Strat 1: Vol Filter
    vol_filter_pnl = 0.0
    vol_filter_triggered = False

    # Strat 2: Short Bias
    short_bias_pnl = 0.0
    short_bias_triggered = False

    # Strat 3: Deep Heat Fade (Peak Reversion)
    fade_pnl = 0.0
    fade_triggered = False

    # Strat 4: Time-Based Exit
    time_exit_pnl = 0.0
    time_exit_triggered = False

    # Strat 5: Master Alpha (S1 + S4)
    master_pnl = 0.0
    master_triggered = False

    if direction:
        trade_path = post_or_slice[post_or_slice.index >= entry_time]
        if not trade_path.empty:
            # --- S1: Volatility Filter (< 5pt OR, 15/15) ---
            if or_width < 5.0:
                vol_filter_triggered = True
                hit_t = False; hit_s = False
                for p in trade_path['Price']:
                    if direction == 'Long':
                        if p >= entry_price + 15.0: hit_t = True; break
                        if p <= entry_price - 15.0: hit_s = True; break
                    else:
                        if p <= entry_price - 15.0: hit_t = True; break
                        if p >= entry_price + 15.0: hit_s = True; break
                vol_filter_pnl = 15.0 if hit_t else (-15.0 if hit_s else ((trade_path['Price'].iloc[-1]-entry_price if direction=='Long' else entry_price-trade_path['Price'].iloc[-1])))

            # --- S2: Short Bias Momentum (Filtered & Confirmed) ---
            # 1. Only trade if or_width < 5.0
            # 2. Entry at or_low - 2.0 (Momentum Confirmation)
            # 3. Target: 30.0 pts | Stop: 15.0 pts
            if direction == 'Short' and or_width < 5.0:
                # Find the specific entry time for the 2.0pt buffer
                s2_entry_price = or_low - 2.0
                s2_entry_idx = trade_path.index[trade_path['Price'] <= s2_entry_price].min()

                if pd.notna(s2_entry_idx):
                    short_bias_triggered = True
                    s2_path = post_or_slice[post_or_slice.index >= s2_entry_idx]

                    s2_hit_t = False; s2_hit_s = False
                    for p in s2_path['Price']:
                        if p <= s2_entry_price - 30.0: s2_hit_t = True; break
                        if p >= s2_entry_price + 15.0: s2_hit_s = True; break

                    short_bias_pnl = 30.0 if s2_hit_t else (-15.0 if s2_hit_s else (s2_entry_price - s2_path['Price'].iloc[-1]))

            # --- S3: Stretched Fade ---
            # 1. Must move 3.0x OR Width from Entry
            # 2. Must happen after 10:00 AM ET
            # 3. Trigger is 1.0x OR Width reversal from peak/trough
            fade_start_time = session_start.replace(hour=10, minute=0, second=0)
            extension_reached = False
            extreme_price = trade_path['Price'].iloc[0]

            fade_entry_idx = None
            fade_entry_p = 0.0
            fade_stop_p = 0.0

            trigger_dist = max(5.0, or_width) # Minimum 5pt trigger to avoid noise on tiny ranges
            min_stretch = or_width * 3.0

            for t, p in trade_path['Price'].items():
                if direction == 'Long':
                    extreme_price = max(extreme_price, p)
                    if not extension_reached and p >= entry_price + min_stretch:
                        extension_reached = True

                    if extension_reached and t >= fade_start_time:
                        if p <= extreme_price - trigger_dist:
                            fade_entry_idx = t
                            fade_entry_p = p
                            fade_stop_p = extreme_price + 2.0 # Peak + 2pt
                            break
                else: # Short
                    extreme_price = min(extreme_price, p)
                    if not extension_reached and p <= entry_price - min_stretch:
                        extension_reached = True

                    if extension_reached and t >= fade_start_time:
                        if p >= extreme_price + trigger_dist:
                            fade_entry_idx = t
                            fade_entry_p = p
                            fade_stop_p = extreme_price - 2.0 # Trough - 2pt
                            break

            if fade_entry_idx:
                fade_triggered = True
                fade_path = post_or_slice[post_or_slice.index >= fade_entry_idx]
                f_hit_t = False; f_hit_s = False
                for fp in fade_path['Price']:
                    if direction == 'Long': # Entering SHORT
                        if fp <= fade_entry_p - 15.0: f_hit_t = True; break
                        if fp >= fade_stop_p: f_hit_s = True; break
                    else: # Entering LONG
                        if fp >= fade_entry_p + 15.0: f_hit_t = True; break
                        if fp <= fade_stop_p: f_hit_s = True; break

                if f_hit_t: fade_pnl = 15.0
                elif f_hit_s: fade_pnl = (fade_entry_p - fade_stop_p) if direction == 'Long' else (fade_stop_p - fade_entry_p)
                else:
                    final_p = trade_path['Price'].iloc[-1]
                    fade_pnl = (fade_entry_p - final_p) if direction == 'Long' else (final_p - fade_entry_p)

            # --- S4: Time-Based Exit (30/25 Baseline) ---
            time_exit_triggered = True
            hit_t = False; hit_s = False; time_out = False
            exit_p = 0.0
            for t, p in trade_path['Price'].items():
                # Check target/stop first
                if direction == 'Long':
                    if p >= entry_price + 30.0: hit_t = True; exit_p = entry_price + 30.0; break
                    if p <= entry_price - 25.0: hit_s = True; exit_p = entry_price - 25.0; break
                else:
                    if p <= entry_price - 30.0: hit_t = True; exit_p = entry_price - 30.0; break
                    if p >= entry_price + 25.0: hit_s = True; exit_p = entry_price + 25.0; break

                # Check time stop: 11:30 AM ET
                if t >= time_stop_limit:
                    curr_profit = (p - entry_price) if direction == 'Long' else (entry_price - p)
                    if curr_profit < 5.0:
                        time_out = True
                        exit_p = p
                        break

            if hit_t: time_exit_pnl = 30.0
            elif hit_s: time_exit_pnl = -25.0
            elif time_out: time_exit_pnl = (exit_p - entry_price) if direction == 'Long' else (entry_price - exit_p)
            else: time_exit_pnl = (trade_path['Price'].iloc[-1]-entry_price if direction=='Long' else entry_price-trade_path['Price'].iloc[-1])

            # --- S5: Master Alpha (S1 + S4) ---
            if or_width < 5.0:
                master_triggered = True
                m_hit_t = False; m_hit_s = False; m_time_out = False
                m_exit_p = 0.0
                for t, p in trade_path['Price'].items():
                    if direction == 'Long':
                        if p >= entry_price + 30.0: m_hit_t = True; m_exit_p = entry_price + 30.0; break
                        if p <= entry_price - 25.0: m_hit_s = True; m_exit_p = entry_price - 25.0; break
                    else:
                        if p <= entry_price - 30.0: m_hit_t = True; m_exit_p = entry_price - 30.0; break
                        if p >= entry_price + 25.0: m_hit_s = True; m_exit_p = entry_price - 25.0; break

                    if t >= time_stop_limit:
                        m_curr_profit = (p - entry_price) if direction == 'Long' else (entry_price - p)
                        if m_curr_profit < 5.0:
                            m_time_out = True
                            m_exit_p = p
                            break
                if m_hit_t: master_pnl = 30.0
                elif m_hit_s: master_pnl = -25.0
                elif m_time_out: master_pnl = (m_exit_p - entry_price) if direction == 'Long' else (entry_price - m_exit_p)
                else: master_pnl = (trade_path['Price'].iloc[-1]-entry_price if direction=='Long' else entry_price-trade_path['Price'].iloc[-1])

            # --- MFE/MAE ---
            path_high = trade_path['Price'].max()
            path_low = trade_path['Price'].min()
            if direction == 'Long':
                mfe = path_high - entry_price
                mae = entry_price - path_low
            else:
                mfe = entry_price - path_low
                mae = path_high - entry_price

    return {
        'Date': current_date,
        'Direction': direction,
        'OR_High': or_high,
        'OR_Low': or_low,
        'OR_Width': or_width,
        'Daily_High': daily_high,
        'Daily_Low': daily_low,
        'MFE': max(0.0, mfe),
        'MAE': max(0.0, mae),
        'S1_Vol_PnL': vol_filter_pnl,
        'S1_Vol_Trig': vol_filter_triggered,
        'S2_Short_PnL': short_bias_pnl,
        'S2_Short_Trig': short_bias_triggered,
        'S3_Fade_PnL': fade_pnl,
        'S3_Fade_Trig': fade_triggered,
        'S4_Time_PnL': time_exit_pnl,
        'S4_Time_Trig': time_exit_triggered,
        'S5_Master_PnL': master_pnl,
        'S5_Master_Trig': master_triggered
    }

def process_file(filepath):
    print(f"Processing: {os.path.basename(filepath)}...")
    trades = []
    start_time = time.perf_counter()

    try:
        # Check if we can read with pytables
        try:
            df = pd.read_hdf(filepath, key='ticks')
        except Exception as e:
            # Fallback to h5py for direct reading
            import h5py
            with h5py.File(filepath, 'r') as f:
                # Look for 'ticks' group or similar
                key = 'ticks' if 'ticks' in f else ('data' if 'data' in f else None)
                if not key:
                    # Search for any group with 'values', 'columns', 'index'
                    for k in f.keys():
                        if isinstance(f[k], h5py.Group) and 'values' in f[k]:
                            key = k
                            break

                if key:
                    g = f[key]
                    data = g['values'][:]
                    cols = [c.decode('utf-8') if isinstance(c, bytes) else c for c in g['columns'][:]]
                    idx = g['index'][:]
                    df = pd.DataFrame(data, columns=cols, index=pd.to_datetime(idx, unit='ns', utc=True))
                else:
                    print(f"  Error: No valid keys found in {filepath}. Ensure it's a valid HDF5 file.")
                    return []

        # Standardize Columns
        if 'Price' not in df.columns and 'Close' in df.columns:
            df['Price'] = df['Close']

        if not isinstance(df.index, pd.DatetimeIndex):
            for col in ['Timestamp', 'Date', 'DateTime']:
                if col in df.columns:
                    df.set_index(col, inplace=True)
                    break
            df.index = pd.to_datetime(df.index)

        # IMPORTANT: Convert to US/Eastern to align with RTH Open (9:30 AM ET)
        if df.index.tz is None:
            df.index = df.index.tz_localize('UTC')
        df.index = df.index.tz_convert('US/Eastern')

        if df.empty:
            return []

        # Split into days based on Local New York Date
        grouped = df.groupby(df.index.date)
        skipped_count = 0

        print(f"  {'Date':<12} | {'Direction':<10} | {'30s OR High/Low':<28} | {'Daily High/Low':<28}")
        print(f"  {'-'*12}-+-{'-'*10}-+-{'-'*28}-+-{'-'*28}")

        for date, day_data in grouped:
            stat = get_trade_stats(day_data)
            if stat:
                dir_label = stat['Direction'] if stat['Direction'] else 'Inside'
                if stat['Direction']:
                    trades.append(stat)
                else:
                    skipped_count += 1

                or_str = f"{stat['OR_High']:>7.2f} / {stat['OR_Low']:<7.2f}"
                day_str = f"{stat['Daily_High']:>7.2f} / {stat['Daily_Low']:<7.2f}"
                print(f"  {str(date):<12} | {dir_label:<10} | {or_str:<28} | {day_str:<28}")
            else:
                skipped_count += 1

        end_time = time.perf_counter()
        print(f"  Processed {len(grouped)} days in {end_time - start_time:.2f}s")

        del df
        gc.collect()

    except Exception as e:
        print(f"Error: {e}")

    return trades

def run_risk_analysis():
    all_trades = []
    files = glob.glob(FILE_PATTERN)
    print(f"Found {len(files)} files.")
    for f in files:
        all_trades.extend(process_file(f))

    if not all_trades:
        print("\nNo trades found.")
        return

    df = pd.DataFrame(all_trades)

    def print_strategy_comparison(df):
        print("\n" + "="*90)
        print(f"{'STRATEGY COMPARISON':^90}")
        print("="*90)
        print(f"{'Strategy':<25} | {'Win Rate %':<12} | {'Total Pts':<12} | {'Avg Trade':<10} | {'Trades (N)':<10}")
        print("-" * 90)

        strats = [
            ('S1: Vol Filter (<5pt)', 'S1_Vol_PnL', 'S1_Vol_Trig'),
            ('S2: Short Bias (30/20)', 'S2_Short_PnL', 'S2_Short_Trig'),
            ('S3: Peak Fade (15/15)', 'S3_Fade_PnL', 'S3_Fade_Trig'),
            ('S4: Time Exit (11:30)', 'S4_Time_PnL', 'S4_Time_Trig'),
            ('S5: Master (S1 + S4)', 'S5_Master_PnL', 'S5_Master_Trig')
        ]

        for name, pnl_col, trig_col in strats:
            triggered = df[df[trig_col] == True]
            n = len(triggered)
            if n > 0:
                wins = len(triggered[triggered[pnl_col] > 0])
                total_pts = triggered[pnl_col].sum()
                wr = (wins / n) * 100
                avg = total_pts / n
                print(f"{name:<25} | {wr:<12.1f} | {total_pts:<12.2f} | {avg:<10.2f} | {n:<10}")
            else:
                print(f"{name:<25} | {'N/A':<12} | {'N/A':<12} | {'N/A':<10} | {0:<10}")

    def print_stats_table(df, title):
        total = len(df)
        if total == 0: return
        print("\n" + "="*80)
        print(f"{title} (N={total})")
        print("="*80)
        print(f"{'Target':<8} | {'Win Rate':<10} | {'Avg MAE (Heat)':<15} | {'90% Safe Stop':<15}")
        print("-" * 80)
        for target in TARGETS:
            winners = df[df['MFE'] >= target]
            if winners.empty:
                print(f"{target:<8} | 0.0%       | N/A             | N/A")
                continue
            win_rate = (len(winners) / total) * 100
            avg_heat = winners['MAE'].mean()
            safe_stop = np.percentile(winners['MAE'], 90)
            print(f"{target:<8} | {win_rate:<9.1f}% | {avg_heat:<15.2f} | {safe_stop:<15.2f}")

    print_stats_table(df[df['Direction'] == 'Long'], "LONG BREAKOUT ANALYSIS")
    print_stats_table(df[df['Direction'] == 'Short'], "SHORT BREAKOUT ANALYSIS")
    print_strategy_comparison(df)

    output_file = "backtest_results.csv"
    df.to_csv(output_file, index=False)
    print(f"\nFull results exported to {output_file}")

if __name__ == "__main__":
    run_risk_analysis()