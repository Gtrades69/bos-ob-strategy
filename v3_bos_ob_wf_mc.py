"""
Walk-Forward + Monte Carlo — BOS+OB Final Config
=================================================
1. Walk-forward: train 2021-2023, test 2024-2026 (frozen params)
2. Walk-forward: train 2021-2022, test 2023-2026 (even harsher)
3. Monte Carlo: 10,000 shuffles of trade order → confidence intervals
"""
import databento as db
import pandas as pd
import bisect, random
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

print(f"Ready in {_time.time()-t0:.1f}s — 5m={len(b5):,} 1m={len(b1):,} days={len(ad)}\n")


def run_backtest(rr=1.1, max_risk=30, max_ed=5, min_risk=5,
    min_body_pct=0.35, session_start=120, session_end=870,
    skip_hours={7, 12, 14}, cooldown_s=120, max_daily=8,
    zone_max_age_min=45, date_start=None, date_end=None):

    entries = []
    zone_age_ns = zone_max_age_min * NS_MIN

    for di, dd in enumerate(ad):
        if di < 2: continue
        if date_start and dd < date_start: continue
        if date_end and dd >= date_end: continue
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

            if not (session_start <= t < session_end): continue
            if bar["hour"] in skip_hours: continue
            if len(shs) < 2 or len(sls) < 2: continue
            if day_n >= max_daily: continue

            body = abs(bar["close"] - bar["open"])
            rng = bar["high"] - bar["low"]
            if rng <= 0 or body / rng < min_body_pct: continue

            hh = shs[-1][0] > shs[-2][0]; hl = sls[-1][0] > sls[-2][0]
            ll = sls[-1][0] < sls[-2][0]; lh = shs[-1][0] < shs[-2][0]
            direction = None
            if hh and hl and bar["close"] > shs[-1][0]: direction = "bull"
            elif ll and lh and bar["close"] < sls[-1][0]: direction = "bear"
            if direction is None or cursor in used: continue
            used.add(cursor)
            created_ns = bar["time_ns"] + 5 * NS_MIN

            # BODY zone
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

            # 1mOB zone
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

            # 1m retrace
            cursor_end = bar["time_ns"] + 5 * NS_MIN
            prev_end = b5[cursor - 1]["time_ns"] + 5 * NS_MIN if cursor > ds else 0
            si = bisect.bisect_left(b1_ns, prev_end)
            new_active = []
            for z in active:
                if z["zk"] in filled: continue
                if cursor_end - z["created_ns"] > zone_age_ns: continue
                found = False
                for bi in range(si, len(b1)):
                    c1 = b1[bi]
                    if c1["time_ns"] >= cursor_end: break
                    if c1["time_ns"] < z["created_ns"]: continue
                    if c1["time_ns"] - z["created_ns"] > zone_age_ns: break
                    if c1["time_ns"] - last_ens < cooldown_s * 10**9: continue
                    touched = False
                    if z["side"] == "bull" and c1["low"] <= z["zt"] and c1["close"] >= z["zb"]:
                        if abs(c1["close"] - z["zt"]) > max_ed: continue
                        ep = c1["close"] + SLIP; risk = ep - z["sp"]
                        if risk < min_risk or risk <= 0 or risk > max_risk: continue
                        tp = ep + risk * rr; touched = True
                    elif z["side"] == "bear" and c1["high"] >= z["zb"] and c1["close"] <= z["zt"]:
                        if abs(c1["close"] - z["zb"]) > max_ed: continue
                        ep = c1["close"] - SLIP; risk = z["sp"] - ep
                        if risk < min_risk or risk <= 0 or risk > max_risk: continue
                        tp = ep - risk * rr; touched = True
                    if touched:
                        filled.add(z["zk"])
                        found = True; last_ens = c1["time_ns"]; day_n += 1
                        entries.append({"date": z["date"], "side": z["side"], "ep": ep,
                            "sp": z["sp"], "tp": tp, "risk": risk, "ens": c1["time_ns"],
                            "de2": de2, "type": z["type"]})
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
            cooldown = pos_exit + cooldown_s * 10**9
        results.append({**e, "result": res, "pnl": pnl})
    return results


def report(label, results):
    w = sum(1 for x in results if x["result"] == "WIN")
    l = sum(1 for x in results if x["result"] == "LOSS")
    tr = w + l
    if tr < 10:
        print(f"  {label}: {tr} trades (too few)")
        return
    wr = 100 * w / tr
    tot = sum(x["pnl"] for x in results)
    wp = sum(x["pnl"] for x in results if x["result"] == "WIN")
    lp = sum(x["pnl"] for x in results if x["result"] == "LOSS")
    pf = abs(wp / lp) if lp else 99
    mo = len(set(str(x["date"])[:7] for x in results)) or 1
    days = len(set(x["date"] for x in results))

    mp = defaultdict(float)
    for x in results: mp[str(x["date"])[:7]] += x["pnl"]
    neg = sum(1 for v in mp.values() if v < 0)

    eq = peak = dd = 0
    for x in results:
        eq += x["pnl"]
        if eq > peak: peak = eq
        if peak - eq > dd: dd = peak - eq

    print(f"\n  {label}")
    print(f"  {len(results)} tr ({len(results)/max(days,1):.1f}/day) | {w}W/{l}L = {wr:.0f}% WR | PF {pf:.2f}")
    print(f"  ${tot/mo:+,.0f}/mo | ${tot:+,.0f} total | DD ${dd:,.0f} | {mo-neg}/{mo} +months")

    for zt in sorted(set(x["type"] for x in results)):
        zw = sum(1 for x in results if x["result"] == "WIN" and x["type"] == zt)
        zl = sum(1 for x in results if x["result"] == "LOSS" and x["type"] == zt)
        zn = zw + zl
        if zn: print(f"    {zt:<6}: {zw}W/{zl}L = {100*zw/zn:.0f}%")

    print(f"\n  Monthly:")
    for k in sorted(mp):
        flag = " <<<" if mp[k] < 0 else ""
        print(f"    {k}: ${mp[k]:>+9,.0f}{flag}")
    return {"wr": wr, "pf": pf, "per_mo": tot/mo, "dd": dd, "neg": neg, "n": len(results)}


# ═══════════════════════════════════════════════════════════════
# WALK-FORWARD TESTS
# ═══════════════════════════════════════════════════════════════
from datetime import date

print("=" * 70)
print("  WALK-FORWARD TEST #1: Train 2021-2023, Test 2024-2026")
print("=" * 70)

print("\n--- IN-SAMPLE: 2021-04 to 2023-12 ---")
r_is1 = run_backtest(date_start=date(2021, 4, 1), date_end=date(2024, 1, 1))
report("IN-SAMPLE (2021-2023)", r_is1)

print("\n--- OUT-OF-SAMPLE: 2024-01 to 2026-03 ---")
r_oos1 = run_backtest(date_start=date(2024, 1, 1), date_end=date(2026, 4, 1))
report("OUT-OF-SAMPLE (2024-2026)", r_oos1)

print(f"\n{'='*70}")
print(f"  WALK-FORWARD TEST #2: Train 2021-2022, Test 2023-2026")
print(f"{'='*70}")

print("\n--- IN-SAMPLE: 2021-04 to 2022-12 ---")
r_is2 = run_backtest(date_start=date(2021, 4, 1), date_end=date(2023, 1, 1))
report("IN-SAMPLE (2021-2022)", r_is2)

print("\n--- OUT-OF-SAMPLE: 2023-01 to 2026-03 ---")
r_oos2 = run_backtest(date_start=date(2023, 1, 1), date_end=date(2026, 4, 1))
report("OUT-OF-SAMPLE (2023-2026)", r_oos2)

print(f"\n{'='*70}")
print(f"  WALK-FORWARD TEST #3: Even split halves")
print(f"{'='*70}")

print("\n--- FIRST HALF: 2021-04 to 2023-06 ---")
r_h1 = run_backtest(date_start=date(2021, 4, 1), date_end=date(2023, 7, 1))
report("FIRST HALF", r_h1)

print("\n--- SECOND HALF: 2023-07 to 2026-03 ---")
r_h2 = run_backtest(date_start=date(2023, 7, 1), date_end=date(2026, 4, 1))
report("SECOND HALF", r_h2)


# ═══════════════════════════════════════════════════════════════
# MONTE CARLO SIMULATION
# ═══════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print(f"  MONTE CARLO — 10,000 equity curve shuffles")
print(f"{'='*70}")

# Get all trade PnLs from full backtest
r_full = run_backtest()
pnls = [x["pnl"] for x in r_full]
n_trades = len(pnls)
actual_total = sum(pnls)
actual_mo = len(set(str(x["date"])[:7] for x in r_full))

# Actual equity curve stats
eq = 0; peak = 0; actual_dd = 0
for p in pnls:
    eq += p
    if eq > peak: peak = eq
    if peak - eq > actual_dd: actual_dd = peak - eq

print(f"\n  Actual: {n_trades} trades | ${actual_total:+,.0f} total | DD ${actual_dd:,.0f}")

# Monte Carlo: shuffle trade order 10,000 times
random.seed(42)
N_SIMS = 10000
mc_totals = []
mc_dds = []
mc_worst_runs = []

for _ in range(N_SIMS):
    shuffled = pnls.copy()
    random.shuffle(shuffled)

    eq = 0; peak = 0; dd = 0
    worst_run = 0; current_run = 0
    for p in shuffled:
        eq += p
        if eq > peak: peak = eq
        if peak - eq > dd: dd = peak - eq
        if p < 0:
            current_run += 1
            if current_run > worst_run: worst_run = current_run
        else:
            current_run = 0

    mc_totals.append(eq)
    mc_dds.append(dd)
    mc_worst_runs.append(worst_run)

mc_totals.sort()
mc_dds.sort()
mc_worst_runs.sort()

def percentile(arr, p):
    idx = int(len(arr) * p / 100)
    return arr[min(idx, len(arr)-1)]

print(f"\n  Monte Carlo Results ({N_SIMS:,} simulations):")
print(f"\n  Total PnL (all sims have same total since same trades):")
print(f"    All: ${mc_totals[0]:+,.0f} (same trades, same total)")

print(f"\n  Max Drawdown distribution:")
print(f"    5th percentile:  ${percentile(mc_dds, 5):,.0f}")
print(f"    25th percentile: ${percentile(mc_dds, 25):,.0f}")
print(f"    50th (median):   ${percentile(mc_dds, 50):,.0f}")
print(f"    75th percentile: ${percentile(mc_dds, 75):,.0f}")
print(f"    95th percentile: ${percentile(mc_dds, 95):,.0f}")
print(f"    99th percentile: ${percentile(mc_dds, 99):,.0f}")
print(f"    Actual DD:       ${actual_dd:,.0f}")

print(f"\n  Max Consecutive Losses:")
print(f"    5th percentile:  {percentile(mc_worst_runs, 5)}")
print(f"    50th (median):   {percentile(mc_worst_runs, 50)}")
print(f"    95th percentile: {percentile(mc_worst_runs, 95)}")
print(f"    99th percentile: {percentile(mc_worst_runs, 99)}")

# Monthly PnL distribution via MC
# Simulate monthly-like chunks (55 trades per "month")
trades_per_month = n_trades // actual_mo
mc_monthly = []
for _ in range(N_SIMS):
    shuffled = pnls.copy()
    random.shuffle(shuffled)
    month_pnl = sum(shuffled[:trades_per_month])
    mc_monthly.append(month_pnl)
mc_monthly.sort()

print(f"\n  Monthly PnL distribution (random {trades_per_month}-trade months):")
print(f"    1st percentile:  ${percentile(mc_monthly, 1):+,.0f}")
print(f"    5th percentile:  ${percentile(mc_monthly, 5):+,.0f}")
print(f"    25th percentile: ${percentile(mc_monthly, 25):+,.0f}")
print(f"    50th (median):   ${percentile(mc_monthly, 50):+,.0f}")
print(f"    75th percentile: ${percentile(mc_monthly, 75):+,.0f}")
print(f"    95th percentile: ${percentile(mc_monthly, 95):+,.0f}")

# Probability of losing month
losing_months = sum(1 for m in mc_monthly if m < 0)
print(f"\n  P(losing month): {100*losing_months/N_SIMS:.1f}%")

# Risk of ruin: P(DD > $50K)
ruin_50k = sum(1 for d in mc_dds if d > 50000)
ruin_30k = sum(1 for d in mc_dds if d > 30000)
ruin_20k = sum(1 for d in mc_dds if d > 20000)
print(f"  P(DD > $20K): {100*ruin_20k/N_SIMS:.1f}%")
print(f"  P(DD > $30K): {100*ruin_30k/N_SIMS:.1f}%")
print(f"  P(DD > $50K): {100*ruin_50k/N_SIMS:.1f}%")

# Profit factor confidence
w_pnls = [p for p in pnls if p > 0]
l_pnls = [p for p in pnls if p < 0]
mc_pfs = []
for _ in range(N_SIMS):
    # Bootstrap: sample with replacement
    sample_w = random.choices(w_pnls, k=len(w_pnls))
    sample_l = random.choices(l_pnls, k=len(l_pnls))
    sw = sum(sample_w)
    sl = abs(sum(sample_l))
    mc_pfs.append(sw / sl if sl > 0 else 99)
mc_pfs.sort()

print(f"\n  Profit Factor (bootstrap CI):")
print(f"    2.5th percentile: {percentile(mc_pfs, 2.5):.2f}")
print(f"    50th (median):    {percentile(mc_pfs, 50):.2f}")
print(f"    97.5th percentile: {percentile(mc_pfs, 97.5):.2f}")

# Win rate confidence (bootstrap)
outcomes = [1 if p > 0 else 0 for p in pnls]
mc_wrs = []
for _ in range(N_SIMS):
    sample = random.choices(outcomes, k=len(outcomes))
    mc_wrs.append(100 * sum(sample) / len(sample))
mc_wrs.sort()

print(f"\n  Win Rate (bootstrap 95% CI):")
print(f"    2.5th percentile: {percentile(mc_wrs, 2.5):.1f}%")
print(f"    50th (median):    {percentile(mc_wrs, 50):.1f}%")
print(f"    97.5th percentile: {percentile(mc_wrs, 97.5):.1f}%")


# ═══════════════════════════════════════════════════════════════
# SUMMARY
# ═══════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print(f"  WALK-FORWARD SUMMARY")
print(f"{'='*70}")
print(f"  {'Period':<25s} {'Trades':>7s} {'WR':>5s} {'PF':>6s} {'$/mo':>10s} {'DD':>10s} {'+mo':>6s}")
print(f"  {'-'*70}")

for label, r in [
    ("Full (2021-2026)", r_full),
    ("IS: 2021-2023", r_is1),
    ("OOS: 2024-2026", r_oos1),
    ("IS: 2021-2022", r_is2),
    ("OOS: 2023-2026", r_oos2),
    ("First half", r_h1),
    ("Second half", r_h2),
]:
    w = sum(1 for x in r if x["result"] == "WIN")
    l = sum(1 for x in r if x["result"] == "LOSS")
    tr = w + l
    if tr < 10: continue
    wr = 100 * w / tr
    tot = sum(x["pnl"] for x in r)
    wp = sum(x["pnl"] for x in r if x["result"] == "WIN")
    lp = sum(x["pnl"] for x in r if x["result"] == "LOSS")
    pf = abs(wp / lp) if lp else 99
    mo = len(set(str(x["date"])[:7] for x in r)) or 1
    mp = defaultdict(float)
    for x in r: mp[str(x["date"])[:7]] += x["pnl"]
    neg = sum(1 for v in mp.values() if v < 0)
    eq = peak = dd = 0
    for x in r:
        eq += x["pnl"]
        if eq > peak: peak = eq
        if peak - eq > dd: dd = peak - eq
    print(f"  {label:<25s} {len(r):>7d} {wr:>4.0f}% {pf:>6.2f} ${tot/mo:>+9,.0f} ${dd:>9,.0f} {mo-neg:>3d}/{mo}")

print(f"\nRuntime: {_time.time()-t0:.1f}s")
