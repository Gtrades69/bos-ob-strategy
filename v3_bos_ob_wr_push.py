"""
BOS+OB WR PUSH — Find what separates winners from losers
=========================================================
Phase 1: Analyze every trade's features (time, risk, zone size, ATR, DOW, etc.)
Phase 2: Test promising filters on the best config
"""
import databento as db
import pandas as pd
import bisect
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from collections import defaultdict
import time as _time

CT = ZoneInfo("America/Chicago")
NS_MIN = 60_000_000_000
PV = 20; CTS = 3; SLIP = 0.5; FEES = 8.40

t0 = _time.time()
print("Loading...", flush=True)

store = db.DBNStore.from_file(
    "/Users/gtrades/Downloads/GLBX-20260402-69B7Y7TYGY/glbx-mdp3-20210331-20260330.ohlcv-1m.dbn.zst"
)
raw = store.to_df().reset_index()
if "ts_event" in raw.columns:
    raw = raw.rename(columns={"ts_event": "time"})
elif raw.columns[0] != "time":
    raw = raw.rename(columns={raw.columns[0]: "time"})
raw["time"] = pd.to_datetime(raw["time"], utc=True)
if raw["close"].iloc[1000] > 1e6:
    for col in ["open", "high", "low", "close"]:
        raw[col] = raw[col] * 1e-9
raw = raw[raw["close"] > 5000]
raw["time_ct"] = raw["time"].dt.tz_convert("US/Central")
raw = raw.sort_values(["time", "volume"], ascending=[True, False])
raw = raw.drop_duplicates(subset=["time"], keep="first")
raw = raw.sort_values("time").reset_index(drop=True)

b1 = []
seen1 = set()
for _, r in raw.iterrows():
    dt = r["time_ct"]
    ns = int(r["time"].value)
    ns_a = ns - (ns % (60 * 10**9))
    if ns_a not in seen1:
        seen1.add(ns_a)
        b1.append({"time_ns": ns_a, "open": r["open"], "high": r["high"],
                    "low": r["low"], "close": r["close"],
                    "hour": dt.hour, "minute": dt.minute})
b1 = [b for b in b1 if b["high"] - b["low"] < 100 and b["low"] > 10000]
b1_ns = [b["time_ns"] for b in b1]

b5_agg = {}
for b in b1:
    key = b["time_ns"] - (b["time_ns"] % (300 * 10**9))
    if key not in b5_agg:
        dt = datetime.fromtimestamp(key / 1e9, tz=CT)
        b5_agg[key] = {"time_ns": key, "open": b["open"], "high": b["high"],
                        "low": b["low"], "close": b["close"],
                        "hour": dt.hour, "minute": dt.minute}
    else:
        a = b5_agg[key]
        if b["high"] > a["high"]: a["high"] = b["high"]
        if b["low"] < a["low"]: a["low"] = b["low"]
        a["close"] = b["close"]
b5 = [b5_agg[k] for k in sorted(b5_agg.keys())]
b5 = [b for b in b5 if b["high"] - b["low"] < 200 and b["low"] > 10000]

def tday(ns):
    dt = datetime.fromtimestamp(ns / 1e9, tz=CT)
    return (dt + timedelta(days=1)).date() if dt.hour >= 17 else dt.date()

day_r = {}
for i, bar in enumerate(b5):
    d = tday(bar["time_ns"])
    if d not in day_r: day_r[d] = [i, i + 1]
    else: day_r[d][1] = i + 1
ad = sorted(day_r.keys())

# 15m bars
b15_agg = {}
for b in b1:
    key = b["time_ns"] - (b["time_ns"] % (900 * 10**9))
    if key not in b15_agg:
        b15_agg[key] = {"time_ns": key, "open": b["open"], "high": b["high"],
                         "low": b["low"], "close": b["close"]}
    else:
        a = b15_agg[key]
        if b["high"] > a["high"]: a["high"] = b["high"]
        if b["low"] < a["low"]: a["low"] = b["low"]
        a["close"] = b["close"]
b15 = [b15_agg[k] for k in sorted(b15_agg.keys())]
b15_ns = [b["time_ns"] for b in b15]

print(f"Ready in {_time.time()-t0:.1f}s — 5m={len(b5):,} 1m={len(b1):,}\n")


# ═══════════════════════════════════════════════════════════════
# PHASE 1: FEATURE-RICH BACKTEST — capture everything about each trade
# ═══════════════════════════════════════════════════════════════
# Using London+NY bp35 skip7/12/14 RR1.1 as base
RR = 1.1; MAX_RISK = 30; MAX_ED = 5; MIN_BODY_PCT = 0.35
SESSION_START = 120; SESSION_END = 870; SKIP_HOURS = {7, 12, 14}
COOLDOWN_S = 120

def get_atr(cursor, lookback=20):
    """ATR from completed 5m bars."""
    start = max(0, cursor - lookback)
    ranges = [b5[i]["high"] - b5[i]["low"] for i in range(start, cursor + 1)]
    return sum(ranges) / len(ranges) if ranges else 0

def get_15m_trend(ns):
    idx = bisect.bisect_left(b15_ns, ns) - 1
    if idx < 4: return 0
    hh = 0; ll = 0
    for i in range(idx - 3, idx + 1):
        if i > 0:
            if b15[i]["high"] > b15[i-1]["high"]: hh += 1
            if b15[i]["low"] < b15[i-1]["low"]: ll += 1
    if hh > ll: return 1
    if ll > hh: return -1
    return 0

def get_swing_count(shs, sls, direction):
    """Count consecutive trend swings."""
    if direction == "bull":
        cnt = 0
        for i in range(len(shs)-1, 0, -1):
            if shs[i][0] > shs[i-1][0]: cnt += 1
            else: break
        return cnt
    else:
        cnt = 0
        for i in range(len(sls)-1, 0, -1):
            if sls[i][0] < sls[i-1][0]: cnt += 1
            else: break
        return cnt

print("Running feature-rich backtest...")
entries = []

for di, dd in enumerate(ad):
    if di < 2: continue
    ds, de = day_r[dd]
    de2 = b5[min(de - 1, len(b5) - 1)]["time_ns"] + 5 * NS_MIN
    shs = []; sls = []; used = set(); active = []; last_ens = 0; day_n = 0
    filled = set()

    for cursor in range(ds + 3, min(de, len(b5))):
        bar = b5[cursor]
        t = bar["hour"] * 60 + bar["minute"]
        p1, p2 = b5[cursor - 1], b5[cursor - 2]

        if p1["high"] > p2["high"] and p1["high"] > bar["high"]:
            depth = p1["high"] - sls[-1][0] if sls else 999
            if depth >= 5: shs.append((p1["high"], cursor - 1))
        if p1["low"] < p2["low"] and p1["low"] < bar["low"]:
            depth = shs[-1][0] - p1["low"] if shs else 999
            if depth >= 5: sls.append((p1["low"], cursor - 1))

        if not (SESSION_START <= t < SESSION_END): continue
        if bar["hour"] in SKIP_HOURS: continue
        if len(shs) < 2 or len(sls) < 2: continue
        if day_n >= 8: continue

        body = abs(bar["close"] - bar["open"])
        rng = bar["high"] - bar["low"]
        if rng <= 0 or body / rng < MIN_BODY_PCT: continue

        hh = shs[-1][0] > shs[-2][0]; hl = sls[-1][0] > sls[-2][0]
        ll = sls[-1][0] < sls[-2][0]; lh = shs[-1][0] < shs[-2][0]
        direction = None
        if hh and hl and bar["close"] > shs[-1][0]: direction = "bull"
        elif ll and lh and bar["close"] < sls[-1][0]: direction = "bear"
        if direction is None or cursor in used: continue
        used.add(cursor)
        created_ns = bar["time_ns"] + 5 * NS_MIN

        # Compute features at BOS time
        atr = get_atr(cursor)
        trend_15m = get_15m_trend(bar["time_ns"])
        swing_cnt = get_swing_count(shs, sls, direction)
        body_pct = body / rng
        disp_range = rng  # displacement candle total range
        dow = dd.weekday()  # 0=Mon, 4=Fri
        entry_hour = bar["hour"]

        # How far past the swing did price close?
        if direction == "bull":
            bos_excess = bar["close"] - shs[-1][0]
        else:
            bos_excess = sls[-1][0] - bar["close"]

        # Zone 2: BODY
        if direction == "bull":
            bzt, bzb, bsp = bar["close"], bar["open"], bar["low"] - 1
        else:
            bzt, bzb, bsp = bar["open"], bar["close"], bar["high"] + 1
        if bzt > bzb + 1:
            zone_size = bzt - bzb
            zk = ("BODY", direction, round(bzt, 1), round(bzb, 1))
            if zk not in filled:
                active.append({"side": direction, "zt": bzt, "zb": bzb, "sp": bsp,
                    "created_ns": created_ns, "cursor": cursor, "date": dd,
                    "type": "BODY", "zk": zk,
                    "atr": atr, "trend_15m": trend_15m, "swing_cnt": swing_cnt,
                    "body_pct": body_pct, "disp_range": disp_range, "dow": dow,
                    "bos_hour": entry_hour, "bos_excess": bos_excess,
                    "zone_size": zone_size})

        # Zone 3: 1m OB
        disp_start = b5[max(cursor - 1, ds)]["time_ns"]
        disp_end = bar["time_ns"] + 5 * NS_MIN
        si_s = bisect.bisect_left(b1_ns, disp_start)
        si_e = bisect.bisect_left(b1_ns, disp_end)
        for k in range(min(si_e - 1, len(b1) - 1), max(si_s, 0), -1):
            kb = b1[k]
            if direction == "bull" and kb["close"] < kb["open"] and kb["high"] - kb["low"] > 1:
                zone_size = kb["open"] - kb["close"]
                zk = ("1mOB", direction, round(kb["open"], 1), round(kb["close"], 1))
                if zk not in filled:
                    active.append({"side": direction, "zt": kb["open"], "zb": kb["close"],
                        "sp": kb["low"] - 1, "created_ns": created_ns, "cursor": cursor,
                        "date": dd, "type": "1mOB", "zk": zk,
                        "atr": atr, "trend_15m": trend_15m, "swing_cnt": swing_cnt,
                        "body_pct": body_pct, "disp_range": disp_range, "dow": dow,
                        "bos_hour": entry_hour, "bos_excess": bos_excess,
                        "zone_size": zone_size})
                break
            elif direction == "bear" and kb["close"] > kb["open"] and kb["high"] - kb["low"] > 1:
                zone_size = kb["close"] - kb["open"]
                zk = ("1mOB", direction, round(kb["close"], 1), round(kb["open"], 1))
                if zk not in filled:
                    active.append({"side": direction, "zt": kb["close"], "zb": kb["open"],
                        "sp": kb["high"] + 1, "created_ns": created_ns, "cursor": cursor,
                        "date": dd, "type": "1mOB", "zk": zk,
                        "atr": atr, "trend_15m": trend_15m, "swing_cnt": swing_cnt,
                        "body_pct": body_pct, "disp_range": disp_range, "dow": dow,
                        "bos_hour": entry_hour, "bos_excess": bos_excess,
                        "zone_size": zone_size})
                break

        # 1m retrace
        cursor_end = bar["time_ns"] + 5 * NS_MIN
        prev_end = b5[cursor - 1]["time_ns"] + 5 * NS_MIN if cursor > ds else 0
        si = bisect.bisect_left(b1_ns, prev_end)
        new_active = []
        for z in active:
            if cursor - z["cursor"] >= 40 or z["zk"] in filled: continue
            found = False
            for bi in range(si, len(b1)):
                c1 = b1[bi]
                if c1["time_ns"] >= cursor_end: break
                if c1["time_ns"] < z["created_ns"]: continue
                if c1["time_ns"] - last_ens < COOLDOWN_S * 10**9: continue
                touched = False
                if z["side"] == "bull" and c1["low"] <= z["zt"] and c1["close"] >= z["zb"]:
                    if abs(c1["close"] - z["zt"]) > MAX_ED: continue
                    ep = c1["close"] + SLIP; risk = ep - z["sp"]
                    if 0 < risk <= MAX_RISK: tp = ep + risk * RR; touched = True
                elif z["side"] == "bear" and c1["high"] >= z["zb"] and c1["close"] <= z["zt"]:
                    if abs(c1["close"] - z["zb"]) > MAX_ED: continue
                    ep = c1["close"] - SLIP; risk = z["sp"] - ep
                    if 0 < risk <= MAX_RISK: tp = ep - risk * RR; touched = True
                if touched:
                    entry_dist = abs(c1["close"] - (z["zt"] if z["side"]=="bull" else z["zb"]))
                    bars_to_entry = (c1["time_ns"] - z["created_ns"]) / NS_MIN

                    filled.add(z["zk"])
                    found = True; last_ens = c1["time_ns"]; day_n += 1
                    entries.append({"date": z["date"], "side": z["side"], "ep": ep,
                        "sp": z["sp"], "tp": tp, "risk": risk, "ens": c1["time_ns"],
                        "de2": de2, "type": z["type"],
                        # Features
                        "atr": z["atr"], "trend_15m": z["trend_15m"],
                        "swing_cnt": z["swing_cnt"], "body_pct": z["body_pct"],
                        "disp_range": z["disp_range"], "dow": z["dow"],
                        "bos_hour": z["bos_hour"], "bos_excess": z["bos_excess"],
                        "zone_size": z["zone_size"], "entry_dist": entry_dist,
                        "bars_to_entry": bars_to_entry,
                        "entry_hour": c1["hour"]})
                    break
            if not found: new_active.append(z)
        active = new_active

# Simulate
entries.sort(key=lambda x: x["ens"])
results = []; cur_day = None; in_pos = False; pos_exit = 0; cooldown = 0

for e in entries:
    if e["date"] != cur_day:
        cur_day = e["date"]; in_pos = False; pos_exit = 0; cooldown = 0
    if in_pos and e["ens"] < pos_exit: continue
    if e["ens"] < cooldown: continue
    si = bisect.bisect_right(b1_ns, e["ens"])
    res = "OPEN"; pnl = 0
    for bi in range(si, min(si + 500, len(b1))):
        b = b1[bi]
        if b["time_ns"] >= e["de2"]:
            pnl = ((b["close"]-e["ep"]) if e["side"]=="bull" else (e["ep"]-b["close"])) * PV * CTS
            res = "EOD"; break
        if e["side"] == "bull":
            if b["low"] <= e["sp"]: res="LOSS"; pnl=(e["sp"]-e["ep"])*PV*CTS; break
            if b["high"] >= e["tp"]: res="WIN"; pnl=(e["tp"]-e["ep"])*PV*CTS; break
        else:
            if b["high"] >= e["sp"]: res="LOSS"; pnl=(e["ep"]-e["sp"])*PV*CTS; break
            if b["low"] <= e["tp"]: res="WIN"; pnl=(e["ep"]-e["tp"])*PV*CTS; break
    pnl -= FEES
    if bi < len(b1):
        in_pos = True; pos_exit = b1[bi]["time_ns"]
        cooldown = pos_exit + COOLDOWN_S * 10**9
    results.append({**e, "result": res, "pnl": pnl})

w_trades = [x for x in results if x["result"] == "WIN"]
l_trades = [x for x in results if x["result"] == "LOSS"]
total = len(results)
w = len(w_trades); l = len(l_trades)
wr = 100*w/(w+l)
tot_pnl = sum(x["pnl"] for x in results)
mo = len(set(str(x["date"])[:7] for x in results))
print(f"\nBaseline: {total} tr | {w}W/{l}L = {wr:.1f}% | ${tot_pnl/mo:+,.0f}/mo\n")


# ═══════════════════════════════════════════════════════════════
# PHASE 1: FEATURE ANALYSIS — What separates W from L?
# ═══════════════════════════════════════════════════════════════
print("="*70)
print("  FEATURE ANALYSIS: Winners vs Losers")
print("="*70)

def analyze_feature(name, key_fn, buckets=None):
    """Analyze WR by feature buckets."""
    if buckets:
        print(f"\n  {name}:")
        for label, lo, hi in buckets:
            bw = sum(1 for x in w_trades if lo <= key_fn(x) < hi)
            bl = sum(1 for x in l_trades if lo <= key_fn(x) < hi)
            bn = bw + bl
            if bn >= 30:
                bpnl = sum(x["pnl"] for x in results if x["result"] in ("WIN","LOSS") and lo <= key_fn(x) < hi)
                print(f"    {label:<20s}: {bw}W/{bl}L = {100*bw/bn:.0f}% WR  ({bn:4d} tr)  ${bpnl/mo:+,.0f}/mo")
            elif bn > 0:
                print(f"    {label:<20s}: {bn} trades (too few)")
    else:
        vals_w = [key_fn(x) for x in w_trades]
        vals_l = [key_fn(x) for x in l_trades]
        print(f"\n  {name}: W avg={sum(vals_w)/len(vals_w):.2f}  L avg={sum(vals_l)/len(vals_l):.2f}")

# 1. Hour of BOS
analyze_feature("BOS Hour", lambda x: x["bos_hour"], [
    (f"h{h:02d}", h, h+1) for h in range(2, 15)
])

# 2. Entry Hour
analyze_feature("Entry Hour", lambda x: x["entry_hour"], [
    (f"h{h:02d}", h, h+1) for h in range(2, 15)
])

# 3. Day of Week
dow_names = ["Mon", "Tue", "Wed", "Thu", "Fri"]
analyze_feature("Day of Week", lambda x: x["dow"], [
    (dow_names[d], d, d+1) for d in range(5)
])

# 4. ATR at BOS time
analyze_feature("ATR (5m, 20-bar)", lambda x: x["atr"], [
    ("ATR < 3", 0, 3),
    ("ATR 3-5", 3, 5),
    ("ATR 5-8", 5, 8),
    ("ATR 8-12", 8, 12),
    ("ATR 12-20", 12, 20),
    ("ATR 20+", 20, 999),
])

# 5. Risk size
analyze_feature("Risk (pts)", lambda x: x["risk"], [
    ("risk < 5", 0, 5),
    ("risk 5-10", 5, 10),
    ("risk 10-15", 10, 15),
    ("risk 15-20", 15, 20),
    ("risk 20-30", 20, 30),
])

# 6. Zone size
analyze_feature("Zone Size (pts)", lambda x: x["zone_size"], [
    ("zone < 3", 0, 3),
    ("zone 3-6", 3, 6),
    ("zone 6-10", 6, 10),
    ("zone 10-20", 10, 20),
    ("zone 20+", 20, 999),
])

# 7. Displacement range (total candle size)
analyze_feature("Disp Range (pts)", lambda x: x["disp_range"], [
    ("disp < 5", 0, 5),
    ("disp 5-10", 5, 10),
    ("disp 10-20", 10, 20),
    ("disp 20-40", 20, 40),
    ("disp 40+", 40, 999),
])

# 8. Body % of displacement
analyze_feature("Body % of Disp", lambda x: x["body_pct"], [
    ("35-50%", 0.35, 0.50),
    ("50-65%", 0.50, 0.65),
    ("65-80%", 0.65, 0.80),
    ("80-100%", 0.80, 1.01),
])

# 9. BOS excess (how far past swing)
analyze_feature("BOS Excess (pts)", lambda x: x["bos_excess"], [
    ("< 2", 0, 2),
    ("2-5", 2, 5),
    ("5-10", 5, 10),
    ("10-20", 10, 20),
    ("20+", 20, 999),
])

# 10. 15m trend alignment
analyze_feature("15m Trend", lambda x: x["trend_15m"], [
    ("counter (-1 or +1 wrong)", -2, 0),
    ("neutral (0)", 0, 1),
    ("aligned (+1 right)", 1, 2),
])
# More precise alignment
print(f"\n  15m Trend (directional):")
for side in ["bull", "bear"]:
    for trend in [-1, 0, 1]:
        bw = sum(1 for x in w_trades if x["side"]==side and x["trend_15m"]==trend)
        bl = sum(1 for x in l_trades if x["side"]==side and x["trend_15m"]==trend)
        bn = bw + bl
        if bn >= 20:
            label = "aligned" if (side=="bull" and trend==1) or (side=="bear" and trend==-1) else \
                    "counter" if (side=="bull" and trend==-1) or (side=="bear" and trend==1) else "neutral"
            print(f"    {side} {label:>8s} (15m={trend:+d}): {bw}W/{bl}L = {100*bw/bn:.0f}% ({bn} tr)")

# 11. Swing count (trend strength)
analyze_feature("Swing Count", lambda x: x["swing_cnt"], [
    ("1 swing", 1, 2),
    ("2 swings", 2, 3),
    ("3+ swings", 3, 99),
])

# 12. Entry distance from zone edge
analyze_feature("Entry Dist (pts)", lambda x: x["entry_dist"], [
    ("< 1", 0, 1),
    ("1-2", 1, 2),
    ("2-3", 2, 3),
    ("3-5", 3, 5),
])

# 13. Bars to entry (how long zone waited)
analyze_feature("Bars to Entry (1m)", lambda x: x["bars_to_entry"], [
    ("< 5 min", 0, 5),
    ("5-15 min", 5, 15),
    ("15-30 min", 15, 30),
    ("30-60 min", 30, 60),
    ("60-120 min", 60, 120),
    ("120+ min", 120, 9999),
])

# 14. Zone type breakdown
analyze_feature("Zone Type", lambda x: 0 if x["type"]=="BODY" else 1, [
    ("BODY", 0, 1),
    ("1mOB", 1, 2),
])

# ═══════════════════════════════════════════════════════════════
# PHASE 2: TEST PROMISING FILTERS
# ═══════════════════════════════════════════════════════════════
print(f"\n\n{'='*70}")
print(f"  PHASE 2: FILTER TESTING")
print(f"{'='*70}")

def test_filter(name, keep_fn):
    """Test a filter on the results."""
    kept = [x for x in results if keep_fn(x)]
    if len(kept) < 50:
        print(f"  {name}: {len(kept)} trades (too few)")
        return None
    kw = sum(1 for x in kept if x["result"]=="WIN")
    kl = sum(1 for x in kept if x["result"]=="LOSS")
    kn = kw + kl
    if kn < 30:
        print(f"  {name}: {kn} decided trades (too few)")
        return None
    kwr = 100*kw/kn
    kpnl = sum(x["pnl"] for x in kept)
    kwp = sum(x["pnl"] for x in kept if x["result"]=="WIN")
    klp = sum(x["pnl"] for x in kept if x["result"]=="LOSS")
    kpf = abs(kwp/klp) if klp else 99
    kmo = len(set(str(x["date"])[:7] for x in kept)) or 1

    mp = defaultdict(float)
    for x in kept: mp[str(x["date"])[:7]] += x["pnl"]
    neg = sum(1 for v in mp.values() if v < 0)

    delta_wr = kwr - wr
    delta_pnl = kpnl/kmo - tot_pnl/mo

    tag = ""
    if delta_wr > 1 and delta_pnl > -500: tag = " <-- GOOD"
    if delta_wr > 2 and delta_pnl > 0: tag = " <-- GREAT"
    if delta_wr > 3 and delta_pnl > 0: tag = " <-- EXCELLENT"

    print(f"  {name:<50s}: {len(kept):4d}tr {kwr:.0f}% PF{kpf:.2f} ${kpnl/kmo:+,.0f}/mo "
          f"(WR {delta_wr:+.1f}% ${delta_pnl:+,.0f}/mo) {kn-neg}/{kmo}+mo{tag}")
    return {"wr": kwr, "pf": kpf, "pnl_mo": kpnl/kmo, "n": len(kept), "neg": neg}

# Test individual filters
print(f"\n  --- Single Filters ---")

# ATR filters
test_filter("ATR >= 5", lambda x: x["atr"] >= 5)
test_filter("ATR >= 8", lambda x: x["atr"] >= 8)
test_filter("ATR 3-20", lambda x: 3 <= x["atr"] <= 20)
test_filter("ATR < 15", lambda x: x["atr"] < 15)

# Risk filters
test_filter("Risk >= 5", lambda x: x["risk"] >= 5)
test_filter("Risk 5-20", lambda x: 5 <= x["risk"] <= 20)
test_filter("Risk < 15", lambda x: x["risk"] < 15)
test_filter("Risk < 20", lambda x: x["risk"] < 20)

# Body % filters
test_filter("Body >= 50%", lambda x: x["body_pct"] >= 0.50)
test_filter("Body >= 60%", lambda x: x["body_pct"] >= 0.60)
test_filter("Body >= 70%", lambda x: x["body_pct"] >= 0.70)

# Zone size
test_filter("Zone 3-20", lambda x: 3 <= x["zone_size"] <= 20)
test_filter("Zone >= 3", lambda x: x["zone_size"] >= 3)
test_filter("Zone < 15", lambda x: x["zone_size"] < 15)

# Displacement range
test_filter("Disp >= 10", lambda x: x["disp_range"] >= 10)
test_filter("Disp >= 15", lambda x: x["disp_range"] >= 15)
test_filter("Disp 5-40", lambda x: 5 <= x["disp_range"] <= 40)

# BOS excess
test_filter("Excess < 10", lambda x: x["bos_excess"] < 10)
test_filter("Excess < 15", lambda x: x["bos_excess"] < 15)
test_filter("Excess 2-10", lambda x: 2 <= x["bos_excess"] < 10)

# 15m alignment
test_filter("15m aligned", lambda x: (x["side"]=="bull" and x["trend_15m"]==1) or
                                       (x["side"]=="bear" and x["trend_15m"]==-1))
test_filter("15m not counter", lambda x: not ((x["side"]=="bull" and x["trend_15m"]==-1) or
                                               (x["side"]=="bear" and x["trend_15m"]==1)))

# Entry distance
test_filter("Entry dist < 3", lambda x: x["entry_dist"] < 3)
test_filter("Entry dist < 2", lambda x: x["entry_dist"] < 2)

# Bars to entry
test_filter("Entry < 60 min", lambda x: x["bars_to_entry"] < 60)
test_filter("Entry < 30 min", lambda x: x["bars_to_entry"] < 30)
test_filter("Entry 5-120 min", lambda x: 5 <= x["bars_to_entry"] <= 120)

# Day of week
test_filter("No Monday", lambda x: x["dow"] != 0)
test_filter("No Friday", lambda x: x["dow"] != 4)
test_filter("Tue-Thu", lambda x: x["dow"] in (1,2,3))

# Hour filters
test_filter("No h02-h03", lambda x: x["entry_hour"] not in (2,3))
test_filter("No h13-h14", lambda x: x["entry_hour"] not in (13,14))
test_filter("BOS h03-h12", lambda x: 3 <= x["bos_hour"] <= 12)

# Swing count
test_filter("Swing >= 2", lambda x: x["swing_cnt"] >= 2)

# BODY-only variants
test_filter("BODY only", lambda x: x["type"] == "BODY")
test_filter("BODY body>=50%", lambda x: x["type"] == "BODY" and x["body_pct"] >= 0.50)

# ═══════════════════════════════════════════════════════════════
# PHASE 3: COMBINE BEST FILTERS
# ═══════════════════════════════════════════════════════════════
print(f"\n  --- Combined Filters ---")

test_filter("Body>=50% + Risk<20",
    lambda x: x["body_pct"] >= 0.50 and x["risk"] < 20)

test_filter("Body>=50% + 15m_not_counter",
    lambda x: x["body_pct"] >= 0.50 and not ((x["side"]=="bull" and x["trend_15m"]==-1) or
                                               (x["side"]=="bear" and x["trend_15m"]==1)))

test_filter("Body>=50% + Entry<60min",
    lambda x: x["body_pct"] >= 0.50 and x["bars_to_entry"] < 60)

test_filter("Body>=50% + Excess<10",
    lambda x: x["body_pct"] >= 0.50 and x["bos_excess"] < 10)

test_filter("Body>=50% + Zone>=3",
    lambda x: x["body_pct"] >= 0.50 and x["zone_size"] >= 3)

test_filter("Body>=60% + 15m_not_counter",
    lambda x: x["body_pct"] >= 0.60 and not ((x["side"]=="bull" and x["trend_15m"]==-1) or
                                               (x["side"]=="bear" and x["trend_15m"]==1)))

test_filter("Body>=50% + 15m_not_ctr + Risk<20",
    lambda x: x["body_pct"] >= 0.50 and x["risk"] < 20 and
              not ((x["side"]=="bull" and x["trend_15m"]==-1) or
                   (x["side"]=="bear" and x["trend_15m"]==1)))

test_filter("Body>=50% + 15m_not_ctr + Excess<15",
    lambda x: x["body_pct"] >= 0.50 and x["bos_excess"] < 15 and
              not ((x["side"]=="bull" and x["trend_15m"]==-1) or
                   (x["side"]=="bear" and x["trend_15m"]==1)))

test_filter("Body>=50% + 15m_align",
    lambda x: x["body_pct"] >= 0.50 and ((x["side"]=="bull" and x["trend_15m"]==1) or
                                           (x["side"]=="bear" and x["trend_15m"]==-1)))

test_filter("Body>=60% + 15m_align",
    lambda x: x["body_pct"] >= 0.60 and ((x["side"]=="bull" and x["trend_15m"]==1) or
                                           (x["side"]=="bear" and x["trend_15m"]==-1)))

test_filter("Body>=50% + Zone>=3 + Risk<20 + 15m_not_ctr",
    lambda x: x["body_pct"] >= 0.50 and x["zone_size"] >= 3 and x["risk"] < 20 and
              not ((x["side"]=="bull" and x["trend_15m"]==-1) or
                   (x["side"]=="bear" and x["trend_15m"]==1)))

test_filter("Body>=50% + Zone>=3 + Entry<60 + 15m_not_ctr",
    lambda x: x["body_pct"] >= 0.50 and x["zone_size"] >= 3 and x["bars_to_entry"] < 60 and
              not ((x["side"]=="bull" and x["trend_15m"]==-1) or
                   (x["side"]=="bear" and x["trend_15m"]==1)))

test_filter("BODY + Body>=50%",
    lambda x: x["type"]=="BODY" and x["body_pct"] >= 0.50)

test_filter("BODY + Body>=50% + 15m_not_ctr",
    lambda x: x["type"]=="BODY" and x["body_pct"] >= 0.50 and
              not ((x["side"]=="bull" and x["trend_15m"]==-1) or
                   (x["side"]=="bear" and x["trend_15m"]==1)))

test_filter("All + Body>=50% + ATR>=5 + 15m_not_ctr",
    lambda x: x["body_pct"] >= 0.50 and x["atr"] >= 5 and
              not ((x["side"]=="bull" and x["trend_15m"]==-1) or
                   (x["side"]=="bear" and x["trend_15m"]==1)))

test_filter("All + Body>=50% + Risk 5-20 + 15m_not_ctr + Entry<60",
    lambda x: x["body_pct"] >= 0.50 and 5 <= x["risk"] <= 20 and x["bars_to_entry"] < 60 and
              not ((x["side"]=="bull" and x["trend_15m"]==-1) or
                   (x["side"]=="bear" and x["trend_15m"]==1)))

test_filter("Body>=50% + No_Fri + 15m_not_ctr",
    lambda x: x["body_pct"] >= 0.50 and x["dow"] != 4 and
              not ((x["side"]=="bull" and x["trend_15m"]==-1) or
                   (x["side"]=="bear" and x["trend_15m"]==1)))

print(f"\nRuntime: {_time.time()-t0:.1f}s")
