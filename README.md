# BOS + Order Block Strategy — NQ Futures

## Results (1 year Databento data, March 2025 - March 2026)
- **600 trades | 61% WR | PF 1.70 | $12,361/month | 2.3 trades/day**
- **12/13 positive months**
- **3 contracts | 1.1R | Zero look-ahead**

## Strategy Rules

### Setup: Break of Structure (BOS)
1. Track 5m swing highs and swing lows (min 5pt depth between swings)
2. BOS = candle BODY CLOSES beyond the last swing high (bull) or below last swing low (bear)
3. Must be in confirmed trend: HH+HL for bull BOS, LL+LH for bear BOS
4. BOS is ONLY confirmed at the candle CLOSE — wicks don't count

### Entry Zones (3 types, all from the BOS displacement)
1. **5m Order Block (OB)**: Last opposing candle before the BOS bar. Zone = candle body. Stop = candle low - 1pt.
2. **BOS Bar Body**: The BOS candle's own body (open to close). Stop = candle low - 1pt.
3. **1m Order Block**: Last opposing 1m candle WITHIN the BOS 5m period. Zone = 1m candle body. Stop = 1m candle low - 1pt.

### Entry
- Wait for 1m bar to retrace to any active zone
- 1m bar must touch the zone AND close within 5pts of zone edge
- Entry at 1m bar close + 0.50 slip (market order at candle close)
- Zone only available AFTER the BOS 5m bar closes (created_ns gate)

### Stop
- At the order block candle's LOW - 1pt (bull) or HIGH + 1pt (bear)
- Structural stop — if price takes out the OB candle's extreme, the setup is invalid

### Target
- 1.1R from entry (risk * 1.1)

### Risk Management
- Max risk per trade: 30pts
- Max 8 trades per day
- 2 minute cooldown between entries
- 1 trade at a time (no stacking positions)

### Session
- 7:30 AM - 1:00 PM CT only (cut h13 — afternoon chop kills WR)

### Data Requirements
- 5m bars for structure and OB detection
- 1m bars for entry timing and simulation
- No indicators, no volume, no tick data needed

## Zero Look-Ahead Verification
Every piece of data is from COMPLETED bars:
- Swings: from bar[cursor-1] confirmed by bar[cursor] — all completed
- BOS: bar[cursor] close vs past swing — completed bar vs past data
- 5m OB: backward scan from cursor — all past bars
- 1m OB: bars WITHIN the BOS 5m period — all completed BEFORE BOS bar closes
  - Uses bisect_LEFT to exclude boundary bars (fix applied)
- Entry: 1m bar after created_ns — next bar after BOS closes
- Sim: starts from bar AFTER entry bar (bisect_right)
- Stop: from OB candle's low — past data

## Files
- `backtest.py` — Full backtest code, self-contained
- `build_bars.py` — Build 5m/1m bars from Databento tick data
- `README.md` — This file

## Reproduction
1. Get Databento NQ tick data (glbx-mdp3 trades, 1 year)
2. Run `build_bars.py` to create `databento_bars_clean.pkl`
3. Run `backtest.py` to reproduce results
