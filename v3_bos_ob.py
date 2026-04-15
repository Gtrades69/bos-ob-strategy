"""
BOS + Order Block Strategy — Adapted for 5-year Databento OHLCV-1m data.
Direct port from Gtrades69/bos-ob-strategy/backtest.py

Original: 600t | 61% WR | PF 1.70 | $12,361/mo on 1yr tick data
This version: runs on 5yr Databento OHLCV-1m (.dbn.zst) resampled to 5m + raw 1m

Strategy:
  - 5m BOS with trend confirmation (HH+HL or LL+LH)
  - 3 zone types: 5m OB, BOS body, 1m OB
  - 1m retrace entry with MAX_ED 5 filter
  - Stop at OB candle extreme
  - Session 7:30-13:00 CT
"""
import databento as db
import pandas as pd
import numpy as np
import bisect
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from collections import defaultdict
import time as _time

CT = ZoneInfo("America/Chicago")
NS_MIN = 60_000_000_000
PV = 20; CTS = 3; SLIP = 0.5; FEES = 8.40
RR = 1.1; MAX_RISK = 30; MAX_ED = 5

t0 = _time.time()
print("Loading Databento OHLCV-1m...", flush=True)

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
raw = raw[raw["close"] > 5000]  # filter spreads/micros
raw["time_ct"] = raw["time"].dt.tz_convert("US/Central")
raw = raw.sort_values(["time", "volume"], ascending=[True, False])
raw = raw.drop_duplicates(subset=["time"], keep="first")
raw = raw.sort_values("time").reset_index(drop=True)

# Build 1m bar dicts (same format as original)
print("Building bar arrays...", flush=True)
b1 = []
for _, r in raw.iterrows():
    dt = r["time_ct"]
    ns = int(r["time"].value)
    # Align to minute boundary
    ns_aligned = ns - (ns % (60 * 10**9))
    b1.append({
        "time_ns": ns_aligned,
        "open": r["open"], "high": r["high"], "low": r["low"], "close": r["close"],
        "hour": dt.hour, "minute": dt.minute,
    })

# Dedup 1m bars (keep first per time_ns)
seen = set()
b1_clean = []
for b in b1:
    if b["time_ns"] not in seen:
        seen.add(b["time_ns"])
        b1_clean.append(b)
b1 = b1_clean

# Filter noise bars
b1 = [b for b in b1 if b["high"] - b["low"] < 100 and b["low"] > 10000]
b1_ns = [b["time_ns"] for b in b1]

# Build 5m bars from 1m bars
b5_agg = {}
for b in b1:
    key = b["time_ns"] - (b["time_ns"] % (300 * 10**9))
    if key not in b5_agg:
        dt = datetime.fromtimestamp(key / 1e9, tz=CT)
        b5_agg[key] = {
            "time_ns": key, "open": b["open"], "high": b["high"],
            "low": b["low"], "close": b["close"],
            "hour": dt.hour, "minute": dt.minute,
        }
    else:
        a = b5_agg[key]
        if b["high"] > a["high"]: a["high"] = b["high"]
        if b["low"] < a["low"]: a["low"] = b["low"]
        a["close"] = b["close"]

b5 = [b5_agg[k] for k in sorted(b5_agg.keys())]
# Filter noise
b5 = [b for b in b5 if b["high"] - b["low"] < 200 and b["low"] > 10000]

print(f"Ready in {_time.time()-t0:.1f}s — 5m={len(b5):,}  1m={len(b1):,}", flush=True)

# ═══════════════════════════════════════════════════════════════
# Day ranges (same logic as original)
# ═══════════════════════════════════════════════════════════════
def tday(ns):
    dt = datetime.fromtimestamp(ns / 1e9, tz=CT)
    return (dt + timedelta(days=1)).date() if dt.hour >= 17 else dt.date()

day_r = {}
for i, bar in enumerate(b5):
    d = tday(bar["time_ns"])
    if d not in day_r:
        day_r[d] = [i, i + 1]
    else:
        day_r[d][1] = i + 1
ad = sorted(day_r.keys())
print(f"Days={len(ad)}", flush=True)

# ═══════════════════════════════════════════════════════════════
# BACKTEST — exact port from bos-ob-strategy/backtest.py
# ═══════════════════════════════════════════════════════════════
entries = []
for di, dd in enumerate(ad):
    if di < 2:
        continue
    ds, de = day_r[dd]
    de2 = b5[min(de - 1, len(b5) - 1)]["time_ns"] + 5 * NS_MIN
    shs = []; sls = []; used = set(); active = []; last_ens = 0; day_n = 0
    filled = set()

    for cursor in range(ds + 3, min(de, len(b5))):
        bar = b5[cursor]
        t = bar["hour"] * 60 + bar["minute"]
        p1, p2 = b5[cursor - 1], b5[cursor - 2]

        # Swing detection (all completed bars)
        if p1["high"] > p2["high"] and p1["high"] > bar["high"]:
            depth = p1["high"] - sls[-1][0] if sls else 999
            if depth >= 5:
                shs.append((p1["high"], cursor - 1))
        if p1["low"] < p2["low"] and p1["low"] < bar["low"]:
            depth = shs[-1][0] - p1["low"] if shs else 999
            if depth >= 5:
                sls.append((p1["low"], cursor - 1))

        # Session: 7:30 AM - 1:00 PM CT
        if not (7 * 60 + 30 <= t < 13 * 60):
            continue
        if len(shs) < 2 or len(sls) < 2:
            continue
        if day_n >= 8:
            continue

        # BOS detection
        hh = shs[-1][0] > shs[-2][0]; hl = sls[-1][0] > sls[-2][0]
        ll = sls[-1][0] < sls[-2][0]; lh = shs[-1][0] < shs[-2][0]
        direction = None
        if hh and hl and bar["close"] > shs[-1][0]:
            direction = "bull"
        elif ll and lh and bar["close"] < sls[-1][0]:
            direction = "bear"
        if direction is None or cursor in used:
            continue
        used.add(cursor)
        created_ns = bar["time_ns"] + 5 * NS_MIN

        # Zone 1: 5m Order Block
        for k in range(cursor, max(cursor - 6, ds), -1):
            kb = b5[k]
            if direction == "bull" and kb["close"] < kb["open"] and kb["high"] - kb["low"] > 2:
                zk = ("OB", direction, round(kb["open"], 1), round(kb["close"], 1))
                if zk not in filled:
                    active.append({"side": direction, "zt": kb["open"], "zb": kb["close"],
                        "sp": kb["low"] - 1, "created_ns": created_ns, "cursor": cursor,
                        "date": dd, "type": "OB", "zk": zk})
                break
            elif direction == "bear" and kb["close"] > kb["open"] and kb["high"] - kb["low"] > 2:
                zk = ("OB", direction, round(kb["close"], 1), round(kb["open"], 1))
                if zk not in filled:
                    active.append({"side": direction, "zt": kb["close"], "zb": kb["open"],
                        "sp": kb["high"] + 1, "created_ns": created_ns, "cursor": cursor,
                        "date": dd, "type": "OB", "zk": zk})
                break

        # Zone 2: BOS bar body
        if direction == "bull":
            bzt, bzb, bsp = bar["close"], bar["open"], bar["low"] - 1
        else:
            bzt, bzb, bsp = bar["open"], bar["close"], bar["high"] + 1
        if bzt > bzb + 1:
            zk = ("BODY", direction, round(bzt, 1), round(bzb, 1))
            if zk not in filled:
                active.append({"side": direction, "zt": bzt, "zb": bzb, "sp": bsp,
                    "created_ns": created_ns, "cursor": cursor, "date": dd,
                    "type": "BODY", "zk": zk})

        # Zone 3: 1m OB within BOS displacement
        disp_start = b5[max(cursor - 1, ds)]["time_ns"]
        disp_end = bar["time_ns"] + 5 * NS_MIN
        si_s = bisect.bisect_left(b1_ns, disp_start)
        si_e = bisect.bisect_left(b1_ns, disp_end)
        for k in range(min(si_e - 1, len(b1) - 1), max(si_s, 0), -1):
            kb = b1[k]
            if direction == "bull" and kb["close"] < kb["open"] and kb["high"] - kb["low"] > 1:
                zk = ("1mOB", direction, round(kb["open"], 1), round(kb["close"], 1))
                if zk not in filled:
                    active.append({"side": direction, "zt": kb["open"], "zb": kb["close"],
                        "sp": kb["low"] - 1, "created_ns": created_ns, "cursor": cursor,
                        "date": dd, "type": "1mOB", "zk": zk})
                break
            elif direction == "bear" and kb["close"] > kb["open"] and kb["high"] - kb["low"] > 1:
                zk = ("1mOB", direction, round(kb["close"], 1), round(kb["open"], 1))
                if zk not in filled:
                    active.append({"side": direction, "zt": kb["close"], "zb": kb["open"],
                        "sp": kb["high"] + 1, "created_ns": created_ns, "cursor": cursor,
                        "date": dd, "type": "1mOB", "zk": zk})
                break

        # 1m retrace to active zones
        cursor_end = bar["time_ns"] + 5 * NS_MIN
        prev_end = b5[cursor - 1]["time_ns"] + 5 * NS_MIN if cursor > ds else 0
        si = bisect.bisect_left(b1_ns, prev_end)
        new_active = []
        for z in active:
            if cursor - z["cursor"] >= 40 or z["zk"] in filled:
                continue
            found = False
            for bi in range(si, len(b1)):
                c1 = b1[bi]
                if c1["time_ns"] >= cursor_end:
                    break
                if c1["time_ns"] < z["created_ns"]:
                    continue
                if c1["time_ns"] - last_ens < 120 * 10**9:
                    continue
                touched = False
                if z["side"] == "bull" and c1["low"] <= z["zt"] and c1["close"] >= z["zb"]:
                    if abs(c1["close"] - z["zt"]) > MAX_ED:
                        continue
                    ep = c1["close"] + SLIP; risk = ep - z["sp"]
                    if 0 < risk <= MAX_RISK:
                        tp = ep + risk * RR; touched = True
                elif z["side"] == "bear" and c1["high"] >= z["zb"] and c1["close"] <= z["zt"]:
                    if abs(c1["close"] - z["zb"]) > MAX_ED:
                        continue
                    ep = c1["close"] - SLIP; risk = z["sp"] - ep
                    if 0 < risk <= MAX_RISK:
                        tp = ep - risk * RR; touched = True
                if touched:
                    filled.add(z["zk"])
                    found = True; last_ens = c1["time_ns"]; day_n += 1
                    entries.append({"date": z["date"], "side": z["side"], "ep": ep,
                        "sp": z["sp"], "tp": tp, "risk": risk, "ens": c1["time_ns"],
                        "de2": de2, "type": z["type"]})
                    break
            if not found:
                new_active.append(z)
        active = new_active

print(f"\nEntry scan done: {len(entries)} entries in {_time.time()-t0:.1f}s", flush=True)

# ═══════════════════════════════════════════════════════════════
# SIMULATE
# ═══════════════════════════════════════════════════════════════
entries.sort(key=lambda x: x["ens"])
results = []; cur_day = None; in_pos = False; pos_exit = 0; cooldown = 0

for e in entries:
    if e["date"] != cur_day:
        cur_day = e["date"]; in_pos = False; pos_exit = 0; cooldown = 0
    if in_pos and e["ens"] < pos_exit:
        continue
    if e["ens"] < cooldown:
        continue

    si = bisect.bisect_right(b1_ns, e["ens"])
    res = "OPEN"; pnl = 0
    for bi in range(si, min(si + 500, len(b1))):
        b = b1[bi]
        if b["time_ns"] >= e["de2"]:
            pnl = ((b["close"] - e["ep"]) if e["side"] == "bull" else (e["ep"] - b["close"])) * PV * CTS
            res = "EOD"; break
        if e["side"] == "bull":
            if b["low"] <= e["sp"]:
                res = "LOSS"; pnl = (e["sp"] - e["ep"]) * PV * CTS; break
            if b["high"] >= e["tp"]:
                res = "WIN"; pnl = (e["tp"] - e["ep"]) * PV * CTS; break
        else:
            if b["high"] >= e["sp"]:
                res = "LOSS"; pnl = (e["ep"] - e["sp"]) * PV * CTS; break
            if b["low"] <= e["tp"]:
                res = "WIN"; pnl = (e["ep"] - e["tp"]) * PV * CTS; break
    pnl -= FEES
    if bi < len(b1):
        in_pos = True; pos_exit = b1[bi]["time_ns"]
        cooldown = pos_exit + 120 * 10**9
    results.append({**e, "result": res, "pnl": pnl})

# ═══════════════════════════════════════════════════════════════
# RESULTS
# ═══════════════════════════════════════════════════════════════
w = sum(1 for x in results if x["result"] == "WIN")
l = sum(1 for x in results if x["result"] == "LOSS")
eod = sum(1 for x in results if x["result"] == "EOD")
tot = sum(x["pnl"] for x in results)
wp = sum(x["pnl"] for x in results if x["result"] == "WIN")
lp = sum(x["pnl"] for x in results if x["result"] == "LOSS")
tr = w + l; wr = 100 * w / tr if tr else 0
pf = abs(wp / lp) if lp else 0
mo = len(set(str(x["date"])[:7] for x in results)) or 1
aw = wp / w if w else 0; al2 = lp / l if l else 0
ar = sum(x["risk"] for x in results) / len(results) if results else 0

print(f"\n{'='*60}")
print(f"BOS + OB + Body + 1mOB | {len(ad)} days | {CTS}ct | {RR}R")
print(f"{'='*60}")
print(f"Trades: {len(results)} | W{w} L{l} EOD{eod}")
print(f"Win Rate: {wr:.0f}% | Profit Factor: {pf:.2f}")
print(f"Avg Win: ${aw:+,.0f} | Avg Loss: ${al2:+,.0f} | Avg Risk: {ar:.0f}pts")
print(f"Total PnL: ${tot:+,.0f} | Monthly: ${tot/mo:+,.0f}")
print(f"Trades/Day: {len(results)/len(ad):.1f}")

print(f"\nBy zone type:")
for zt in sorted(set(x["type"] for x in results)):
    zw = sum(1 for x in results if x["result"] == "WIN" and x["type"] == zt)
    zl = sum(1 for x in results if x["result"] == "LOSS" and x["type"] == zt)
    zt_n = zw + zl
    print(f"  {zt:<6}: {zw}W/{zl}L = {100*zw/zt_n:.0f}% WR" if zt_n > 0 else f"  {zt:<6}: 0 trades")

print(f"\nBy side:")
for s in ["bull", "bear"]:
    sw = sum(1 for x in results if x["result"] == "WIN" and x["side"] == s)
    sl = sum(1 for x in results if x["result"] == "LOSS" and x["side"] == s)
    sn = sw + sl
    if sn:
        print(f"  {s}: {sw}W/{sl}L = {100*sw/sn:.0f}% WR | ${sum(x['pnl'] for x in results if x['side']==s):+,.0f}")

print(f"\nBy year:")
yd = defaultdict(lambda: {"w": 0, "l": 0, "pnl": 0.0, "n": 0})
for x in results:
    y = x["date"].year
    yd[y]["n"] += 1
    yd[y]["pnl"] += x["pnl"]
    if x["result"] == "WIN": yd[y]["w"] += 1
    elif x["result"] == "LOSS": yd[y]["l"] += 1
for y in sorted(yd):
    d = yd[y]; yn = d["w"] + d["l"]
    ywr = 100 * d["w"] / yn if yn else 0
    print(f"  {y}: {d['n']:4d} tr | {ywr:.0f}% WR | ${d['pnl']:+,.0f}")

print(f"\nMonthly:")
mp = defaultdict(float); mw = defaultdict(int); ml = defaultdict(int); mc = defaultdict(int)
for x in results:
    k = str(x["date"])[:7]; mp[k] += x["pnl"]; mc[k] += 1
    if x["result"] == "WIN": mw[k] += 1
    elif x["result"] == "LOSS": ml[k] += 1
for k in sorted(mp.keys()):
    flag = " <<<" if mp[k] < 0 else ""
    print(f"  {k}: {mw[k]:3d}W/{ml[k]:3d}L ${mp[k]:>+9,.0f} ({mc[k]:2d}t){flag}")
pos = sum(1 for v in mp.values() if v > 0)
print(f"\nPositive months: {pos}/{len(mp)}")

# Max drawdown
eq = peak = max_dd = 0
for x in results:
    eq += x["pnl"]
    if eq > peak: peak = eq
    dd = peak - eq
    if dd > max_dd: max_dd = dd
print(f"Max drawdown: ${max_dd:,.0f}")

print(f"\nTotal runtime: {_time.time()-t0:.1f}s")
