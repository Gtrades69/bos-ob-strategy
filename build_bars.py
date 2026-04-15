"""
Build clean 5m and 1m OHLC bars from Databento NQ tick data.
Filters for front-month contract only.
Output: databento_bars_clean.pkl with keys 'b5' and 'b1'.
"""
import databento as db
import os, pickle
from collections import Counter

TICK_DIR = "/Users/tradingbot/trading-bot/tick_data_1yr"  # Path to .dbn.zst files
OUTPUT = "databento_bars_clean.pkl"

files = sorted([f for f in os.listdir(TICK_DIR) if f.endswith(".dbn.zst")])
print(f"Processing {len(files)} files...")

all_5m = {}
all_1m = {}

for fi, fname in enumerate(files):
    path = os.path.join(TICK_DIR, fname)
    try:
        store = db.DBNStore.from_file(path)
        # Find front month (most traded, price > 1000)
        iid_counts = Counter()
        for trade in store:
            p = trade.price / 1e9
            if p > 1000:
                iid_counts[trade.instrument_id] += 1
        if not iid_counts:
            continue
        front_iid = iid_counts.most_common(1)[0][0]

        # Build bars from front month
        store2 = db.DBNStore.from_file(path)
        from datetime import datetime
        from zoneinfo import ZoneInfo
        CT = ZoneInfo("America/Chicago")

        for trade in store2:
            if trade.instrument_id != front_iid:
                continue
            ts = trade.ts_event
            price = trade.price / 1e9
            if price < 10000:
                continue
            dt = datetime.fromtimestamp(ts / 1e9, tz=CT)

            # 1m bar
            m1_key = ts - (ts % (60 * 10**9))
            if m1_key not in all_1m:
                all_1m[m1_key] = {"time_ns": m1_key, "open": price, "high": price,
                    "low": price, "close": price, "hour": dt.hour, "minute": dt.minute}
            else:
                b = all_1m[m1_key]
                if price > b["high"]: b["high"] = price
                if price < b["low"]: b["low"] = price
                b["close"] = price

            # 5m bar
            m5_key = ts - (ts % (300 * 10**9))
            m5_dt = datetime.fromtimestamp(m5_key / 1e9, tz=CT)
            if m5_key not in all_5m:
                all_5m[m5_key] = {"time_ns": m5_key, "open": price, "high": price,
                    "low": price, "close": price, "hour": m5_dt.hour, "minute": m5_dt.minute}
            else:
                b = all_5m[m5_key]
                if price > b["high"]: b["high"] = price
                if price < b["low"]: b["low"] = price
                b["close"] = price
    except Exception as ex:
        print(f"  Skip {fname}: {ex}")
        continue

    if (fi + 1) % 50 == 0:
        print(f"  {fi + 1}/{len(files)}")

b5 = [all_5m[k] for k in sorted(all_5m.keys())]
b1 = [all_1m[k] for k in sorted(all_1m.keys())]

print(f"\n5m={len(b5)} 1m={len(b1)}")
with open(OUTPUT, "wb") as f:
    pickle.dump({"b5": b5, "b1": b1}, f)
print(f"Saved {OUTPUT}")
