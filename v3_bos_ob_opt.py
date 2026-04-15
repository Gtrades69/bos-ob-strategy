"""
BOS + OB Strategy — OPTIMIZED
Port from bos-ob-strategy repo + V4 enhancements:
  - Drop weak OB zone (54% WR)
  - Displacement body % filter
  - Key level proximity for BODY zones
  - 15m alignment
  - Skip hours
  - RR sweep
  - Tighter session
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

# 1m bars
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

# 5m bars
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

# 15m bars (for alignment)
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

def tday(ns):
    dt = datetime.fromtimestamp(ns / 1e9, tz=CT)
    return (dt + timedelta(days=1)).date() if dt.hour >= 17 else dt.date()

day_r = {}
for i, bar in enumerate(b5):
    d = tday(bar["time_ns"])
    if d not in day_r: day_r[d] = [i, i + 1]
    else: day_r[d][1] = i + 1
ad = sorted(day_r.keys())

# ═══════════════════════════════════════════════════════════════
# KEY LEVELS (from prior sessions — zero look-ahead)
# ═══════════════════════════════════════════════════════════════
def compute_levels():
    levels = {}
    for di, dd in enumerate(ad):
        if di < 2: continue
        ds, de = day_r[dd]

        # Prior day
        pd1 = ad[di - 1]
        pd1s, pd1e = day_r[pd1]
        pd1_ny = [b5[i] for i in range(pd1s, pd1e)
                   if 7 * 60 + 30 <= b5[i]["hour"] * 60 + b5[i]["minute"] < 15 * 60]
        if len(pd1_ny) < 5: continue
        pdh = max(b["high"] for b in pd1_ny)
        pdl = min(b["low"] for b in pd1_ny)

        # 2 days ago
        pd2 = ad[di - 2]
        pd2s, pd2e = day_r[pd2]
        pd2_ny = [b5[i] for i in range(pd2s, pd2e)
                   if 7 * 60 + 30 <= b5[i]["hour"] * 60 + b5[i]["minute"] < 15 * 60]
        p2h = max(b["high"] for b in pd2_ny) if len(pd2_ny) >= 5 else pdh
        p2l = min(b["low"] for b in pd2_ny) if len(pd2_ny) >= 5 else pdl

        # Asia (17:00 prev - 02:00 CT = overnight, approximate with 00:00-02:00)
        asia = [b5[i] for i in range(ds, de) if b5[i]["hour"] < 2]
        asia_h = max(b["high"] for b in asia) if len(asia) >= 2 else None
        asia_l = min(b["low"] for b in asia) if len(asia) >= 2 else None

        # London (02:00-05:00)
        london = [b5[i] for i in range(ds, de)
                  if 2 * 60 <= b5[i]["hour"] * 60 + b5[i]["minute"] < 5 * 60]
        london_h = max(b["high"] for b in london) if len(london) >= 3 else None
        london_l = min(b["low"] for b in london) if len(london) >= 3 else None

        hi = [pdh, p2h]
        lo = [pdl, p2l]
        if asia_h: hi.append(asia_h); lo.append(asia_l)
        if london_h: hi.append(london_h); lo.append(london_l)
        levels[dd] = {"hi": hi, "lo": lo}
    return levels

level_cache = compute_levels()

def near_key_level(price, levels_d, tol=15):
    """Check if price is within tol pts of any key level."""
    if levels_d is None: return False
    for p in levels_d["hi"] + levels_d["lo"]:
        if abs(price - p) <= tol: return True
    return False

def get_15m_trend(ns):
    """15m trend: count HH vs LL on last 4 completed 15m bars."""
    idx = bisect.bisect_left(b15_ns, ns) - 1
    if idx < 4: return 0
    hh = 0; ll = 0
    for i in range(idx - 3, idx + 1):
        if i > 0:
            if b15[i]["high"] > b15[i-1]["high"]: hh += 1
            if b15[i]["low"] < b15[i-1]["low"]: ll += 1
    if hh > ll: return 1   # bull
    if ll > hh: return -1  # bear
    return 0

print(f"Ready in {_time.time()-t0:.1f}s — 5m={len(b5):,} 1m={len(b1):,} 15m={len(b15):,} days={len(ad)}\n")


# ═══════════════════════════════════════════════════════════════
# CONFIGURABLE BACKTEST
# ═══════════════════════════════════════════════════════════════
def run_backtest(
    rr=1.1,
    max_risk=30,
    max_ed=5,
    use_ob=True,
    use_body=True,
    use_1mob=True,
    min_body_pct=0.0,       # displacement body % filter (0 = off)
    session_start=450,      # 7:30 = 7*60+30 = 450
    session_end=780,        # 13:00 = 13*60 = 780
    skip_hours=set(),       # e.g. {12}
    require_key_body=False, # BODY zone needs key level proximity
    require_15m_body=False, # BODY zone needs 15m alignment
    require_key_1mob=False, # 1mOB needs key level
    require_15m_1mob=False, # 1mOB needs 15m alignment
    key_tol=15,             # key level tolerance pts
    cooldown_s=120,
    max_daily=8,
    delay=0,                # proof test: delay entry by N 1m bars
    reverse=False,          # proof test: flip direction
):
    entries = []
    for di, dd in enumerate(ad):
        if di < 2: continue
        ds, de = day_r[dd]
        de2 = b5[min(de - 1, len(b5) - 1)]["time_ns"] + 5 * NS_MIN
        shs = []; sls = []; used = set(); active = []; last_ens = 0; day_n = 0
        filled = set()
        lvl = level_cache.get(dd)

        for cursor in range(ds + 3, min(de, len(b5))):
            bar = b5[cursor]
            t = bar["hour"] * 60 + bar["minute"]
            p1, p2 = b5[cursor - 1], b5[cursor - 2]

            # Swing detection
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

            # Displacement body % filter
            if min_body_pct > 0:
                body = abs(bar["close"] - bar["open"])
                rng = bar["high"] - bar["low"]
                if rng <= 0 or body / rng < min_body_pct:
                    continue

            # BOS with trend confirmation
            hh = shs[-1][0] > shs[-2][0]; hl = sls[-1][0] > sls[-2][0]
            ll = sls[-1][0] < sls[-2][0]; lh = shs[-1][0] < shs[-2][0]
            direction = None
            if hh and hl and bar["close"] > shs[-1][0]: direction = "bull"
            elif ll and lh and bar["close"] < sls[-1][0]: direction = "bear"
            if direction is None or cursor in used: continue
            used.add(cursor)
            created_ns = bar["time_ns"] + 5 * NS_MIN

            # Zone 1: 5m OB
            if use_ob:
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
            if use_body:
                body_ok = True
                if require_key_body and lvl:
                    mid = (bar["close"] + bar["open"]) / 2
                    body_ok = near_key_level(mid, lvl, key_tol)
                if require_15m_body:
                    trend = get_15m_trend(bar["time_ns"])
                    if direction == "bull" and trend != 1: body_ok = False
                    if direction == "bear" and trend != -1: body_ok = False

                if body_ok:
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

            # Zone 3: 1m OB
            if use_1mob:
                mob_ok = True
                if require_key_1mob and lvl:
                    mob_ok = near_key_level(bar["close"], lvl, key_tol)
                if require_15m_1mob:
                    trend = get_15m_trend(bar["time_ns"])
                    if direction == "bull" and trend != 1: mob_ok = False
                    if direction == "bear" and trend != -1: mob_ok = False

                if mob_ok:
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
                if cursor - z["cursor"] >= 40 or z["zk"] in filled: continue
                found = False
                for bi in range(si, len(b1)):
                    c1 = b1[bi]
                    if c1["time_ns"] >= cursor_end: break
                    if c1["time_ns"] < z["created_ns"]: continue
                    if c1["time_ns"] - last_ens < cooldown_s * 10**9: continue
                    touched = False
                    if z["side"] == "bull" and c1["low"] <= z["zt"] and c1["close"] >= z["zb"]:
                        if abs(c1["close"] - z["zt"]) > max_ed: continue
                        ep = c1["close"] + SLIP; risk = ep - z["sp"]
                        if 0 < risk <= max_risk: tp = ep + risk * rr; touched = True
                    elif z["side"] == "bear" and c1["high"] >= z["zb"] and c1["close"] <= z["zt"]:
                        if abs(c1["close"] - z["zb"]) > max_ed: continue
                        ep = c1["close"] - SLIP; risk = z["sp"] - ep
                        if 0 < risk <= max_risk: tp = ep - risk * rr; touched = True
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

        # Apply delay for proof test
        if delay > 0:
            si_entry = bisect.bisect_right(b1_ns, e["ens"])
            si_entry = min(si_entry + delay, len(b1) - 1)
            # Re-price entry from delayed bar
            db = b1[si_entry]
            side = e["side"]
            if reverse: side = "bull" if side == "bear" else "bear"
            ep = db["close"] + SLIP if side == "bull" else db["close"] - SLIP
            risk = abs(ep - e["sp"])
            if risk <= 0 or risk > max_risk: continue
            # Check stop on correct side
            if side == "bull" and e["sp"] >= ep: continue
            if side == "bear" and e["sp"] <= ep: continue
            tp = ep + risk * rr if side == "bull" else ep - risk * rr
            si = si_entry + 1
        elif reverse:
            side = "bull" if e["side"] == "bear" else "bear"
            ep = e["ep"]  # same price
            if side == "bull" and e["sp"] >= ep: continue
            if side == "bear" and e["sp"] <= ep: continue
            risk = abs(ep - e["sp"])
            tp = ep + risk * rr if side == "bull" else ep - risk * rr
            si = bisect.bisect_right(b1_ns, e["ens"])
        else:
            side = e["side"]; ep = e["ep"]; tp = e["tp"]; risk = e["risk"]
            si = bisect.bisect_right(b1_ns, e["ens"])

        res = "OPEN"; pnl = 0
        for bi in range(si, min(si + 500, len(b1))):
            b = b1[bi]
            if b["time_ns"] >= e["de2"]:
                pnl = ((b["close"] - ep) if side == "bull" else (ep - b["close"])) * PV * CTS
                res = "EOD"; break
            if side == "bull":
                if b["low"] <= e["sp"]: res = "LOSS"; pnl = (e["sp"] - ep) * PV * CTS; break
                if b["high"] >= tp: res = "WIN"; pnl = (tp - ep) * PV * CTS; break
            else:
                if b["high"] >= e["sp"]: res = "LOSS"; pnl = (ep - e["sp"]) * PV * CTS; break
                if b["low"] <= tp: res = "WIN"; pnl = (ep - tp) * PV * CTS; break
        pnl -= FEES
        if bi < len(b1):
            in_pos = True; pos_exit = b1[bi]["time_ns"]
            cooldown = pos_exit + cooldown_s * 10**9
        results.append({**e, "result": res, "pnl": pnl, "_side": side})

    return results


def stats(label, results, detail=False):
    w = sum(1 for x in results if x["result"] == "WIN")
    l = sum(1 for x in results if x["result"] == "LOSS")
    tr = w + l
    if tr < 20:
        print(f"  {label}: {tr} trades (too few)")
        return {"wr": 0, "pf": 0, "per_mo": 0, "n": tr}
    wr = 100 * w / tr
    tot = sum(x["pnl"] for x in results)
    wp = sum(x["pnl"] for x in results if x["result"] == "WIN")
    lp = sum(x["pnl"] for x in results if x["result"] == "LOSS")
    pf = abs(wp / lp) if lp else 99
    mo = len(set(str(x["date"])[:7] for x in results)) or 1
    per_mo = tot / mo
    tpd = len(results) / len(ad)

    mp = defaultdict(float)
    for x in results:
        mp[str(x["date"])[:7]] += x["pnl"]
    neg = sum(1 for v in mp.values() if v < 0)
    pos = len(mp) - neg

    eq = peak = dd = 0
    for x in results:
        eq += x["pnl"]
        if eq > peak: peak = eq
        if peak - eq > dd: dd = peak - eq

    tag = ""
    if wr >= 58 and pf >= 2.3 and per_mo >= 20000: tag = " *** TARGET ***"
    elif wr >= 58 and per_mo >= 15000: tag = " **"
    elif wr >= 55 and per_mo >= 10000: tag = " *"

    print(f"\n  {label}{tag}")
    print(f"  {len(results)} tr ({tpd:.1f}/day) | {w}W/{l}L = {wr:.0f}% WR | PF {pf:.2f}")
    print(f"  ${per_mo:+,.0f}/mo | ${tot:+,.0f} | DD ${dd:,.0f} | {pos}/{len(mp)} +months")

    # Zone breakdown
    for zt in sorted(set(x["type"] for x in results)):
        zw = sum(1 for x in results if x["result"] == "WIN" and x["type"] == zt)
        zl = sum(1 for x in results if x["result"] == "LOSS" and x["type"] == zt)
        zn = zw + zl
        if zn > 0:
            zpnl = sum(x["pnl"] for x in results if x["type"] == zt)
            print(f"    {zt:<6}: {zw}W/{zl}L = {100*zw/zn:.0f}% | ${zpnl/mo:+,.0f}/mo")

    # Side breakdown
    for s in ["bull", "bear"]:
        sw = sum(1 for x in results if x["result"] == "WIN" and x.get("_side", x["side"]) == s)
        sl = sum(1 for x in results if x["result"] == "LOSS" and x.get("_side", x["side"]) == s)
        sn = sw + sl
        if sn: print(f"    {s}: {sn} tr | {100*sw/sn:.0f}% WR")

    if detail:
        yd = defaultdict(lambda: {"w": 0, "l": 0, "pnl": 0.0})
        for x in results:
            y = x["date"].year
            yd[y]["pnl"] += x["pnl"]
            if x["result"] == "WIN": yd[y]["w"] += 1
            elif x["result"] == "LOSS": yd[y]["l"] += 1
        print(f"\n  By year:")
        for y in sorted(yd):
            d = yd[y]; yn = d["w"] + d["l"]
            print(f"    {y}: {yn:4d} tr | {100*d['w']/yn:.0f}% WR | ${d['pnl']:+,.0f}" if yn else f"    {y}: 0")

        print(f"\n  Monthly:")
        for k in sorted(mp):
            flag = " <<<" if mp[k] < 0 else ""
            print(f"    {k}: ${mp[k]:>+9,.0f}{flag}")

    return {"wr": wr, "pf": pf, "per_mo": per_mo, "n": len(results), "dd": dd, "neg": neg}


# ═══════════════════════════════════════════════════════════════
# OPTIMIZATION SWEEP
# ═══════════════════════════════════════════════════════════════
print("=" * 70)
print("  BOS+OB OPTIMIZATION — 5yr Databento OHLCV")
print("=" * 70)

configs = []

# ── Phase 1: Zone type combos ──
print("\n--- PHASE 1: Zone combos (baseline RR=1.1) ---")
combos = [
    ("ALL zones",           dict(use_ob=True,  use_body=True,  use_1mob=True)),
    ("BODY+1mOB (no OB)",   dict(use_ob=False, use_body=True,  use_1mob=True)),
    ("BODY only",           dict(use_ob=False, use_body=True,  use_1mob=False)),
    ("1mOB only",           dict(use_ob=False, use_body=False, use_1mob=True)),
]
for name, kw in combos:
    r = run_backtest(**kw)
    s = stats(name, r)
    configs.append((name, s, kw))

# ── Phase 2: Displacement body % filter ──
print("\n--- PHASE 2: Displacement body % filter (BODY+1mOB) ---")
for bp in [0.25, 0.35, 0.45]:
    r = run_backtest(use_ob=False, min_body_pct=bp)
    s = stats(f"body>={int(bp*100)}%", r)
    configs.append((f"bp{int(bp*100)}", s, {"use_ob": False, "min_body_pct": bp}))

# ── Phase 3: Session / skip hours ──
print("\n--- PHASE 3: Session tuning (BODY+1mOB) ---")
for sh, se, skip, label in [
    (450, 720, set(),   "7:30-12:00"),
    (450, 780, {12},    "7:30-13:00 skip h12"),
    (480, 780, set(),   "8:00-13:00"),
    (480, 720, set(),   "8:00-12:00"),
]:
    r = run_backtest(use_ob=False, session_start=sh, session_end=se, skip_hours=skip)
    s = stats(label, r)

# ── Phase 4: RR sweep (BODY+1mOB) ──
print("\n--- PHASE 4: RR sweep (BODY+1mOB, no OB) ---")
for rr_val in [1.1, 1.3, 1.5, 2.0, 2.5]:
    r = run_backtest(use_ob=False, rr=rr_val)
    s = stats(f"RR={rr_val}", r)
    configs.append((f"rr{rr_val}", s, {"use_ob": False, "rr": rr_val}))

# ── Phase 5: Key levels + 15m alignment for BODY ──
print("\n--- PHASE 5: V4-style filters (BODY+1mOB) ---")
for kl, a15, label in [
    (False, False, "no filters"),
    (True,  False, "BODY key_level"),
    (False, True,  "BODY 15m_align"),
    (True,  True,  "BODY key+15m"),
]:
    r = run_backtest(use_ob=False, require_key_body=kl, require_15m_body=a15)
    s = stats(label, r)

# Also try key+15m for 1mOB
print("\n  --- 1mOB filters ---")
for kl, a15, label in [
    (True,  True,  "1mOB key+15m"),
    (True,  False, "1mOB key_level"),
]:
    r = run_backtest(use_ob=False, require_key_1mob=kl, require_15m_1mob=a15)
    s = stats(label, r)

# ── Phase 6: Combined best ──
print("\n--- PHASE 6: Combined optimizations ---")
best_combos = [
    ("BODY+1mOB bp35 skip12 rr1.1",
     dict(use_ob=False, min_body_pct=0.35, skip_hours={12})),
    ("BODY+1mOB bp35 skip12 rr1.3",
     dict(use_ob=False, min_body_pct=0.35, skip_hours={12}, rr=1.3)),
    ("BODY+1mOB bp35 skip12 rr1.5",
     dict(use_ob=False, min_body_pct=0.35, skip_hours={12}, rr=1.5)),
    ("BODY(key+15m)+1mOB rr1.1",
     dict(use_ob=False, require_key_body=True, require_15m_body=True)),
    ("BODY(key+15m)+1mOB rr1.3",
     dict(use_ob=False, require_key_body=True, require_15m_body=True, rr=1.3)),
    ("BODY(key+15m)+1mOB bp35 rr1.1",
     dict(use_ob=False, require_key_body=True, require_15m_body=True, min_body_pct=0.35)),
    ("BODY(key+15m)+1mOB bp35 rr1.3",
     dict(use_ob=False, require_key_body=True, require_15m_body=True, min_body_pct=0.35, rr=1.3)),
    ("BODY(key+15m)+1mOB(key+15m) rr1.1",
     dict(use_ob=False, require_key_body=True, require_15m_body=True,
          require_key_1mob=True, require_15m_1mob=True)),
    ("BODY+1mOB maxed=3 rr1.1",
     dict(use_ob=False, max_ed=3)),
    ("BODY+1mOB maxed=3 bp35 rr1.3",
     dict(use_ob=False, max_ed=3, min_body_pct=0.35, rr=1.3)),
    ("ALL zones bp35 skip12 rr1.1",
     dict(min_body_pct=0.35, skip_hours={12})),
    ("BODY+1mOB 8:00-12:00 bp35 rr1.1",
     dict(use_ob=False, min_body_pct=0.35, session_start=480, session_end=720)),
]
best_results = []
for name, kw in best_combos:
    r = run_backtest(**kw)
    s = stats(name, r)
    best_results.append((name, s, kw, r))

# ═══════════════════════════════════════════════════════════════
# PHASE 7: DEEP SWEEP — push $/mo and WR
# ═══════════════════════════════════════════════════════════════
print("\n--- PHASE 7: Deep sweep (volume + precision) ---")

phase7 = []

# 7a: bp30 — slightly more trades than bp35
for rr_val in [1.1, 1.3, 1.5]:
    r = run_backtest(use_ob=False, min_body_pct=0.30, skip_hours={12}, rr=rr_val)
    name = f"bp30 skip12 rr{rr_val}"
    s = stats(name, r)
    phase7.append((name, s, {"use_ob": False, "min_body_pct": 0.30, "skip_hours": {12}, "rr": rr_val}, r))

# 7b: London session added (02:00-14:30 with skip hours)
for bp in [0.30, 0.35]:
    for rr_val in [1.1, 1.3]:
        r = run_backtest(use_ob=False, min_body_pct=bp, session_start=120, session_end=870,
                         skip_hours={7, 12, 14}, rr=rr_val)
        name = f"London+NY bp{int(bp*100)} skip7/12/14 rr{rr_val}"
        s = stats(name, r)
        kw = {"use_ob": False, "min_body_pct": bp, "session_start": 120, "session_end": 870,
              "skip_hours": {7, 12, 14}, "rr": rr_val}
        phase7.append((name, s, kw, r))

# 7c: MAX_ED sweep with bp35
for med in [3, 4]:
    for rr_val in [1.1, 1.3]:
        r = run_backtest(use_ob=False, min_body_pct=0.35, skip_hours={12}, max_ed=med, rr=rr_val)
        name = f"bp35 maxed={med} skip12 rr{rr_val}"
        s = stats(name, r)
        kw = {"use_ob": False, "min_body_pct": 0.35, "skip_hours": {12}, "max_ed": med, "rr": rr_val}
        phase7.append((name, s, kw, r))

# 7d: Wider NY session with bp35 (7:30-14:30 like V4)
for rr_val in [1.1, 1.3]:
    r = run_backtest(use_ob=False, min_body_pct=0.35, session_start=450, session_end=870,
                     skip_hours={12, 14}, rr=rr_val)
    name = f"7:30-14:30 bp35 skip12/14 rr{rr_val}"
    s = stats(name, r)
    kw = {"use_ob": False, "min_body_pct": 0.35, "session_start": 450, "session_end": 870,
          "skip_hours": {12, 14}, "rr": rr_val}
    phase7.append((name, s, kw, r))

# 7e: Higher max_daily to allow more trades
for md in [12, 15]:
    r = run_backtest(use_ob=False, min_body_pct=0.35, skip_hours={12}, max_daily=md)
    name = f"bp35 skip12 maxdaily={md}"
    s = stats(name, r)
    kw = {"use_ob": False, "min_body_pct": 0.35, "skip_hours": {12}, "max_daily": md}
    phase7.append((name, s, kw, r))

# 7f: Include OB zone with bp35 filters + higher RR to compensate OB's lower WR
for rr_val in [1.3, 1.5]:
    r = run_backtest(use_ob=True, min_body_pct=0.35, skip_hours={12}, rr=rr_val)
    name = f"ALL bp35 skip12 rr{rr_val}"
    s = stats(name, r)
    kw = {"use_ob": True, "min_body_pct": 0.35, "skip_hours": {12}, "rr": rr_val}
    phase7.append((name, s, kw, r))

# 7g: bp35 + shorter cooldown for more fills
for cd in [60, 90]:
    r = run_backtest(use_ob=False, min_body_pct=0.35, skip_hours={12}, cooldown_s=cd)
    name = f"bp35 skip12 cooldown={cd}s"
    s = stats(name, r)
    kw = {"use_ob": False, "min_body_pct": 0.35, "skip_hours": {12}, "cooldown_s": cd}
    phase7.append((name, s, kw, r))

# 7h: London-only session (02:00-07:30)
for rr_val in [1.1, 1.3]:
    r = run_backtest(use_ob=False, min_body_pct=0.35, session_start=120, session_end=450, rr=rr_val)
    name = f"London-only 2:00-7:30 bp35 rr{rr_val}"
    s = stats(name, r)
    kw = {"use_ob": False, "min_body_pct": 0.35, "session_start": 120, "session_end": 450, "rr": rr_val}
    phase7.append((name, s, kw, r))

# ═══════════════════════════════════════════════════════════════
# PHASE 8: SURGICAL — push PF and $/mo
# ═══════════════════════════════════════════════════════════════
print("\n--- PHASE 8: Final surgical sweep ---")

phase8 = []

# 8a: London+NY BODY-only (highest WR zone, no 1mOB dilution)
for rr_val in [1.1, 1.3, 1.5]:
    r = run_backtest(use_ob=False, use_1mob=False, min_body_pct=0.35,
                     session_start=120, session_end=870, skip_hours={7, 12, 14}, rr=rr_val)
    name = f"London+NY BODY-only bp35 skip rr{rr_val}"
    s = stats(name, r)
    phase8.append((name, s, {"use_ob": False, "use_1mob": False, "min_body_pct": 0.35,
                   "session_start": 120, "session_end": 870, "skip_hours": {7, 12, 14}, "rr": rr_val}, r))

# 8b: London+NY bp40 (tighter body filter → higher WR)
for rr_val in [1.1, 1.3]:
    r = run_backtest(use_ob=False, min_body_pct=0.40, session_start=120, session_end=870,
                     skip_hours={7, 12, 14}, rr=rr_val)
    name = f"London+NY bp40 skip rr{rr_val}"
    s = stats(name, r)
    phase8.append((name, s, {"use_ob": False, "min_body_pct": 0.40, "session_start": 120,
                   "session_end": 870, "skip_hours": {7, 12, 14}, "rr": rr_val}, r))

# 8c: London+NY bp35 with maxed=4 (tighter entry distance)
for rr_val in [1.1, 1.3]:
    r = run_backtest(use_ob=False, min_body_pct=0.35, max_ed=4, session_start=120, session_end=870,
                     skip_hours={7, 12, 14}, rr=rr_val)
    name = f"London+NY bp35 maxed=4 skip rr{rr_val}"
    s = stats(name, r)
    phase8.append((name, s, {"use_ob": False, "min_body_pct": 0.35, "max_ed": 4,
                   "session_start": 120, "session_end": 870, "skip_hours": {7, 12, 14}, "rr": rr_val}, r))

# 8d: London+NY bp35 cooldown=60s
for rr_val in [1.1, 1.3]:
    r = run_backtest(use_ob=False, min_body_pct=0.35, cooldown_s=60, session_start=120, session_end=870,
                     skip_hours={7, 12, 14}, rr=rr_val)
    name = f"London+NY bp35 cd60 skip rr{rr_val}"
    s = stats(name, r)
    phase8.append((name, s, {"use_ob": False, "min_body_pct": 0.35, "cooldown_s": 60,
                   "session_start": 120, "session_end": 870, "skip_hours": {7, 12, 14}, "rr": rr_val}, r))

# 8e: London+NY BODY-only bp40 (max WR push)
for rr_val in [1.1, 1.3]:
    r = run_backtest(use_ob=False, use_1mob=False, min_body_pct=0.40,
                     session_start=120, session_end=870, skip_hours={7, 12, 14}, rr=rr_val)
    name = f"London+NY BODY-only bp40 skip rr{rr_val}"
    s = stats(name, r)
    phase8.append((name, s, {"use_ob": False, "use_1mob": False, "min_body_pct": 0.40,
                   "session_start": 120, "session_end": 870, "skip_hours": {7, 12, 14}, "rr": rr_val}, r))

# 8f: NY-only (no London) with wider session 7:30-14:30 bp35 cooldown=60
for rr_val in [1.1, 1.3]:
    r = run_backtest(use_ob=False, min_body_pct=0.35, cooldown_s=60,
                     session_start=450, session_end=870, skip_hours={12, 14}, rr=rr_val)
    name = f"NY 7:30-14:30 bp35 cd60 skip12/14 rr{rr_val}"
    s = stats(name, r)
    phase8.append((name, s, {"use_ob": False, "min_body_pct": 0.35, "cooldown_s": 60,
                   "session_start": 450, "session_end": 870, "skip_hours": {12, 14}, "rr": rr_val}, r))

# 8g: London+NY with 15m alignment on BODY only (keep 1mOB unfiltered)
for rr_val in [1.1, 1.3]:
    r = run_backtest(use_ob=False, min_body_pct=0.35, require_15m_body=True,
                     session_start=120, session_end=870, skip_hours={7, 12, 14}, rr=rr_val)
    name = f"London+NY bp35 BODY-15m skip rr{rr_val}"
    s = stats(name, r)
    phase8.append((name, s, {"use_ob": False, "min_body_pct": 0.35, "require_15m_body": True,
                   "session_start": 120, "session_end": 870, "skip_hours": {7, 12, 14}, "rr": rr_val}, r))

# 8h: London+NY BODY-only bp35 with 15m alignment
for rr_val in [1.1, 1.3]:
    r = run_backtest(use_ob=False, use_1mob=False, min_body_pct=0.35, require_15m_body=True,
                     session_start=120, session_end=870, skip_hours={7, 12, 14}, rr=rr_val)
    name = f"London+NY BODY-only bp35 15m skip rr{rr_val}"
    s = stats(name, r)
    phase8.append((name, s, {"use_ob": False, "use_1mob": False, "min_body_pct": 0.35,
                   "require_15m_body": True, "session_start": 120, "session_end": 870,
                   "skip_hours": {7, 12, 14}, "rr": rr_val}, r))

# 8i: RR 1.2 sweet spot with London+NY bp35
r = run_backtest(use_ob=False, min_body_pct=0.35, session_start=120, session_end=870,
                 skip_hours={7, 12, 14}, rr=1.2)
name = "London+NY bp35 skip rr1.2"
s = stats(name, r)
phase8.append((name, s, {"use_ob": False, "min_body_pct": 0.35, "session_start": 120,
               "session_end": 870, "skip_hours": {7, 12, 14}, "rr": 1.2}, r))

# Combine all
all_results = best_results + phase7 + phase8

# ═══════════════════════════════════════════════════════════════
# BEST CONFIG — DETAIL + PROOF
# ═══════════════════════════════════════════════════════════════
# Sort: prioritize configs with WR >= 60% and high $/mo
all_results.sort(key=lambda x: -(x[1]["per_mo"] if x[1]["wr"] >= 58 else x[1]["per_mo"] * 0.5))

if all_results and all_results[0][1]["n"] >= 50:
    best_name, best_s, best_kw, best_r = all_results[0]
    print(f"\n{'='*70}")
    print(f"  BEST CONFIG: {best_name}")
    print(f"{'='*70}")
    stats(f"BEST — {best_name}", best_r, detail=True)

    # Proof tests
    print(f"\n{'='*70}")
    print(f"  PROOF TESTS — {best_name}")
    print(f"{'='*70}")

    r_d1 = run_backtest(**best_kw, delay=1)
    stats("DELAY +1 bar", r_d1)

    r_d3 = run_backtest(**best_kw, delay=3)
    stats("DELAY +3 bars", r_d3)

    r_rev = run_backtest(**best_kw, reverse=True)
    stats("REVERSED", r_rev)

# Show top 10
print(f"\n{'='*70}")
print(f"  TOP 10 CONFIGS (sorted by $/mo, WR >= 58%)")
print(f"{'='*70}")
high_wr = [x for x in all_results if x[1]["wr"] >= 58]
high_wr.sort(key=lambda x: -x[1].get("per_mo", 0))
for name, s, kw, _ in high_wr[:10]:
    tag = "***" if s["wr"] >= 65 and s["pf"] >= 2.0 and s["per_mo"] >= 15000 else ""
    print(f"  {name:<50s} {s['n']:5d}tr {s['wr']:.0f}% PF{s['pf']:.2f} ${s['per_mo']:+,.0f}/mo DD${s.get('dd',0):,.0f} {tag}")

# Also show best by WR
print(f"\n  TOP 5 by Win Rate:")
all_results.sort(key=lambda x: -x[1].get("wr", 0))
for name, s, kw, _ in all_results[:5]:
    print(f"  {name:<50s} {s['n']:5d}tr {s['wr']:.0f}% PF{s['pf']:.2f} ${s['per_mo']:+,.0f}/mo")

# Show best by PF
print(f"\n  TOP 5 by Profit Factor:")
all_results.sort(key=lambda x: -x[1].get("pf", 0))
for name, s, kw, _ in all_results[:5]:
    print(f"  {name:<50s} {s['n']:5d}tr {s['wr']:.0f}% PF{s['pf']:.2f} ${s['per_mo']:+,.0f}/mo")

print(f"\nTotal runtime: {_time.time()-t0:.1f}s")
