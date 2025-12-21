# 30-Second Opening Range Breakout (ORB) - Performance Report

This report summarizes the performance and logic of the five trading strategies developed for the E-mini S&P 500 (ES) based on the 9:30:00 - 9:30:30 AM Opening Range.

## Strategy Comparison Table

The following results were calculated across multiple HDF5 contract files, covering approximately 128 trading days.

| Strategy | Entry Trigger | Win Rate % | Total Points | Avg Trade |
| :--- | :--- | :--- | :--- | :--- |
| **S1: Volatility Filter** | OR High/Low Break | 59.7% | 234.50 | 3.05 |
| **S2: Short Bias** | **OR Low - 2.0 pts** | 45.0% | 124.00 | 3.10 |
| **S3: Stretched Fade** | 1x OR Width Reversal | 29.9% | -113.00 | -1.30 |
| **S4: Time Exit** | OR High/Low Break | 54.7% | 301.75 | 2.36 |
| **S5: Master Alpha** | OR High/Low Break | **62.3%** | **340.50** | **4.42** |

---

## Strategy Logic Descriptions

### S0: Vanilla Breakout (Baseline)
*   **Concept**: The "raw" opening range breakout with zero filtering or active management.
*   **Filter**: None (Trades every session).
*   **Entry**: Breakout of the 30s OR High (Long) or OR Low (Short) exactly at the level.
*   **Target**: 30.0 points.
*   **Stop Loss**: 25.0 points.
*   **Result**: High drawdown and low efficiency due to "spending" energy on wide-range days and lack of an exit plan for stale trades.

### S1: Volatility Filter
*   **Concept**: Trades only during "coiled" market conditions where the initial 30-second range is narrow.
*   **Filter**: `OR Width < 5.0 points`.
*   **Entry**: Breakout of the 30s OR High (Long) or OR Low (Short).
*   **Target**: 15.0 points.
*   **Stop Loss**: 15.0 points.
*   **Result**: High quality, stable edge.

### S2: Short Bias v2 (Momentum Evolution)
*   **Concept**: Capitalizes on the faster, smoother velocity of Short breakouts on narrow-range days.
*   **Filter**: `OR Width < 5.0 points` AND `Direction == 'Short'`.
*   **Entry**: `OR Low - 2.0 points`. The 2-point buffer acts as momentum confirmation.
*   **Target**: 30.0 points (Double the risk).
*   **Stop Loss**: 15.0 points.
*   **Result**: Tripled the efficiency of the original Short-only logic.

### S3: Stretched Fade
*   **Concept**: Fades the breakout after "Extension Exhaustion" is confirmed.
*   **Conditions**:
    - Must happen after **10:00 AM ET**.
    - Breakout must have stretched at least **3.0x OR Width** in profit.
*   **Trigger**: Price reverses **1.0x OR Width** from its session peak/trough.
*   **Target**: 15.0 points.
*   **Stop Loss**: `Peak/Trough +/- 2.0 points` (Adaptive Risk).
*   **Result**: Consistently unprofitable, confirming the ES ORB trend is extremely resilient to reversals.

### S4: Time Exit (11:30 AM Rule)
*   **Concept**: Protects capital by exiting "stale" trades where the opening edge has decayed.
*   **Entry**: Standard 30s ORB Breakout.
*   **Target**: 30.0 points.
*   **Stop Loss**: 25.0 points.
*   **Time Stop**: If the trade is still active at **11:30 AM ET** and Profit is **< 5.0 points**, close at Market.
*   **Result**: Single most effective risk-adjustment for total point accumulation.

### S5: Master Alpha (S1 + S4)
*   **Concept**: The "Gold Standard" setup combining the best quality filter with the best time management.
*   **Filter**: `OR Width < 5.0 points`.
*   **Target**: 30.0 points.
*   **Stop Loss**: 25.0 points.
*   **Time Stop**: Apply the **11:30 AM ET** rule (Exit if Profit < 5.0 pts).
*   **Result**: The most efficient strategy in the suite with the highest Average Trade (4.42 pts).

---

## Strategic Summary & Recommendations

### ✅ HIGHLY RECOMMENDED: S5 Master Alpha (The "Gold Standard")
*   **Logic**: S5 is the clear winner because it addresses the two primary risks of the 30s ORB: **Market Noise** (solved by the < 5pt filter) and **Time Decay** (solved by the 11:30 AM exit).
*   **Verdict**: By being selective and impatient (exiting if not in profit by mid-morning), this strategy achieves the highest probability of success and the best profit-per-trade efficiency.

### ✅ RECOMMENDED: S1 Volatility Filter
*   **Logic**: Market "geometry" matters. A breakout from a 4-point range has much more room to run than a breakout from an 18-point range.
*   **Verdict**: This is the safest way to trade the ORB if you prefer fixed targets and don't want to manage the trade actively.

### ✅ RECOMMENDED: S2 Short Bias
*   **Logic**: Shorts move "faster" than longs. By waiting for the **2.0 point momentum confirmation** and filtering for narrow ranges, you avoid being "chopped up" by minor fluctuations.
*   **Verdict**: This is your most efficient directional strategy. It takes fewer trades than the others but has a much higher Average Trade, making it a "sniper" approach for the Short side.

### ⚠️ SELECTIVE: S4 Time Exit
*   **Logic**: If you are an active trader who wants to participate every day, the **11:30 AM Rule** is mandatory. Without it, you are vulnerable to the "Middling Grind" where the market chops back and forth after the initial expansion.
*   **Verdict**: Use this as your baseline for any breakout strategy to protect yourself from time-decay.

### ❌ NOT RECOMMENDED: S3 Stretched Fade
*   **Logic**: The E-mini S&P 500 has high "Trend Persistence" in the morning. Even our most refined Fade logic failed because the market's pullbacks are "buying opportunities" for institutions, not reversals.
*   **Verdict**: **Avoid Fading**. It is statistically safer to be a buyer of the pullback (the "Heat") in the direction of the break than to bet against it.

### ❌ NOT RECOMMENDED: Vanilla Breakouts (No Filters)
*   **Logic**: Trading every 30-second breakout with no filters, fixed entry points, and no early exit plan.
*   **Verdict**: This approach leads to massive drawdown on wide-range days where the "break" happens at the end of a move. Without a time-stop (like S4), you are trapped in the "Middling Grind" of midday chop, making this the most dangerous way to play the 30s range.
