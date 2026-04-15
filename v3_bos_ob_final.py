"""
BOS+OB FINAL — Implement discovered filters in actual backtest
===============================================================
New filters from feature analysis:
  1. Zone max age (minutes) — stale zones lose edge
  2. Min risk (pts) — tiny risk = noise stops
  3. Min zone size (pts) — thin zones = weak
Then sweep + proof test.
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

print(f"Ready in {_time.time()-t0:.1f}s — 5m={len(b5):,} 1m={len(b1):,} days={len(ad)}\n")


def run_backtest(
    rr=1.1, max_risk=30, max_ed=5, min_risk=0,
    use_body=True, use_1mob=True,
    min_body_pct=0.35,
    session_start=120, session_end=870,
    skip_hours={7, 12, 14},
    cooldown_s=120, max_daily=8,
    zone_max_age_min=200,  # NEW: max minutes before zone expires
    min_zone_size=0,       # NEW: minimum zone size in pts
    delay=0, reverse=False,
):
    entries = []
    zone_age_ns = zone_max_age_min * NS_MIN

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
            if use_body:
                if direction == "bull":
                    bzt, bzb, bsp = bar["close"], bar["open"], bar["low"] - 1
                else:
                    bzt, bzb, bsp = bar["open"], bar["close"], bar["high"] + 1
                zsize = bzt - bzb
                if zsize > 1 and zsize >= min_zone_size:
                    zk = ("BODY", direction, round(bzt, 1), round(bzb, 1))
                    if zk not in filled:
                        active.append({"side": direction, "zt": bzt, "zb": bzb, "sp": bsp,
                            "created_ns": created_ns, "cursor": cursor, "date": dd,
                            "type": "BODY", "zk": zk})

            # 1mOB zone
            if use_1mob:
                disp_start = b5[max(cursor - 1, ds)]["time_ns"]
                disp_end = bar["time_ns"] + 5 * NS_MIN
                si_s = bisect.bisect_left(b1_ns, disp_start)
                si_e = bisect.bisect_left(b1_ns, disp_end)
                for k in range(min(si_e - 1, len(b1) - 1), max(si_s, 0), -1):
                    kb = b1[k]
                    if direction == "bull" and kb["close"] < kb["open"] and kb["high"] - kb["low"] > 1:
                        zsize = kb["open"] - kb["close"]
                        if zsize >= min_zone_size:
                            zk = ("1mOB", direction, round(kb["open"], 1), round(kb["close"], 1))
                            if zk not in filled:
                                active.append({"side": direction, "zt": kb["open"], "zb": kb["close"],
                                    "sp": kb["low"] - 1, "created_ns": created_ns, "cursor": cursor,
                                    "date": dd, "type": "1mOB", "zk": zk})
                        break
                    elif direction == "bear" and kb["close"] > kb["open"] and kb["high"] - kb["low"] > 1:
                        zsize = kb["close"] - kb["open"]
                        if zsize >= min_zone_size:
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
                # Zone expiry: time-based instead of cursor-based
                if z["zk"] in filled: continue
                # Check zone age at current cursor time
                if cursor_end - z["created_ns"] > zone_age_ns: continue

                found = False
                for bi in range(si, len(b1)):
                    c1 = b1[bi]
                    if c1["time_ns"] >= cursor_end: break
                    if c1["time_ns"] < z["created_ns"]: continue
                    # Check zone age at entry time
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

        if delay > 0:
            si_entry = bisect.bisect_right(b1_ns, e["ens"])
            si_entry = min(si_entry + delay, len(b1) - 1)
            db_bar = b1[si_entry]
            side = e["side"]
            if reverse: side = "bull" if side == "bear" else "bear"
            ep = db_bar["close"] + SLIP if side == "bull" else db_bar["close"] - SLIP
            risk = abs(ep - e["sp"])
            if risk <= 0 or risk > max_risk: continue
            if side == "bull" and e["sp"] >= ep: continue
            if side == "bear" and e["sp"] <= ep: continue
            tp = ep + risk * rr if side == "bull" else ep - risk * rr
            si = si_entry + 1
        elif reverse:
            side = "bull" if e["side"] == "bear" else "bear"
            ep = e["ep"]
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
    for x in results: mp[str(x["date"])[:7]] += x["pnl"]
    neg = sum(1 for v in mp.values() if v < 0)
    pos = len(mp) - neg

    eq = peak = dd = 0
    for x in results:
        eq += x["pnl"]
        if eq > peak: peak = eq
        if peak - eq > dd: dd = peak - eq

    tag = ""
    if wr >= 66 and pf >= 2.2 and per_mo >= 15000: tag = " *** MONSTER ***"
    elif wr >= 65 and pf >= 2.0 and per_mo >= 15000: tag = " ** GREAT **"
    elif wr >= 64 and per_mo >= 15000: tag = " *"

    print(f"\n  {label}{tag}")
    print(f"  {len(results)} tr ({tpd:.1f}/day) | {w}W/{l}L = {wr:.0f}% WR | PF {pf:.2f}")
    print(f"  ${per_mo:+,.0f}/mo | ${tot:+,.0f} | DD ${dd:,.0f} | {pos}/{len(mp)} +months")

    for zt in sorted(set(x["type"] for x in results)):
        zw = sum(1 for x in results if x["result"] == "WIN" and x["type"] == zt)
        zl = sum(1 for x in results if x["result"] == "LOSS" and x["type"] == zt)
        zn = zw + zl
        if zn > 0:
            zpnl = sum(x["pnl"] for x in results if x["type"] == zt)
            print(f"    {zt:<6}: {zw}W/{zl}L = {100*zw/zn:.0f}% | ${zpnl/mo:+,.0f}/mo")
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
# SWEEP
# ═══════════════════════════════════════════════════════════════
print("=" * 70)
print("  FINAL OPTIMIZATION — Zone freshness + min risk + zone size")
print("=" * 70)

all_results = []
base = dict(min_body_pct=0.35, session_start=120, session_end=870, skip_hours={7, 12, 14})

# ── Baseline (no new filters) ──
print("\n--- Baseline (no new filters) ---")
r = run_backtest(**base)
s = stats("London+NY bp35 (baseline)", r)
all_results.append(("baseline", s, base, r))

# ── Zone max age sweep ──
print("\n--- Zone max age sweep ---")
for age in [30, 45, 60, 90, 120]:
    r = run_backtest(**base, zone_max_age_min=age)
    name = f"max_age={age}min"
    s = stats(name, r)
    all_results.append((name, s, {**base, "zone_max_age_min": age}, r))

# ── Min risk sweep ──
print("\n--- Min risk sweep ---")
for mr in [3, 5, 7]:
    r = run_backtest(**base, min_risk=mr)
    name = f"min_risk={mr}"
    s = stats(name, r)
    all_results.append((name, s, {**base, "min_risk": mr}, r))

# ── Min zone size sweep ──
print("\n--- Min zone size sweep ---")
for mz in [2, 3, 5]:
    r = run_backtest(**base, min_zone_size=mz)
    name = f"min_zone={mz}"
    s = stats(name, r)
    all_results.append((name, s, {**base, "min_zone_size": mz}, r))

# ── Combined: age + risk ──
print("\n--- Combined: age + risk ---")
for age in [45, 60, 90]:
    for mr in [3, 5]:
        r = run_backtest(**base, zone_max_age_min=age, min_risk=mr)
        name = f"age={age} risk>={mr}"
        s = stats(name, r)
        all_results.append((name, s, {**base, "zone_max_age_min": age, "min_risk": mr}, r))

# ── Combined: age + risk + zone size ──
print("\n--- Combined: age + risk + zone size ---")
for age in [45, 60, 90]:
    for mr in [3, 5]:
        for mz in [2, 3]:
            r = run_backtest(**base, zone_max_age_min=age, min_risk=mr, min_zone_size=mz)
            name = f"age={age} risk>={mr} zone>={mz}"
            s = stats(name, r)
            all_results.append((name, s, {**base, "zone_max_age_min": age,
                                "min_risk": mr, "min_zone_size": mz}, r))

# ── Best combos with RR sweep ──
print("\n--- Best combos with RR sweep ---")
for age in [60, 90]:
    for rr_val in [1.1, 1.2, 1.3]:
        r = run_backtest(**base, zone_max_age_min=age, min_risk=5, rr=rr_val)
        name = f"age={age} risk>=5 rr{rr_val}"
        s = stats(name, r)
        all_results.append((name, s, {**base, "zone_max_age_min": age,
                            "min_risk": 5, "rr": rr_val}, r))

# ── NY-only with new filters ──
print("\n--- NY-only with new filters ---")
ny_base = dict(min_body_pct=0.35, session_start=450, session_end=780, skip_hours={12})
for age in [60, 90]:
    for mr in [3, 5]:
        r = run_backtest(**ny_base, zone_max_age_min=age, min_risk=mr)
        name = f"NY age={age} risk>={mr}"
        s = stats(name, r)
        all_results.append((name, s, {**ny_base, "zone_max_age_min": age, "min_risk": mr}, r))


# ═══════════════════════════════════════════════════════════════
# BEST CONFIG — DETAIL + PROOF
# ═══════════════════════════════════════════════════════════════
# Rank by: (WR * per_mo) to balance WR and money
all_results.sort(key=lambda x: -(x[1]["wr"] * x[1]["per_mo"]) if x[1]["n"] >= 100 else 0)

print(f"\n{'='*70}")
print(f"  TOP 10 CONFIGS (WR * $/mo)")
print(f"{'='*70}")
for name, s, kw, _ in all_results[:10]:
    score = s["wr"] * s["per_mo"] / 1000
    print(f"  {name:<40s} {s['n']:5d}tr {s['wr']:.0f}% PF{s['pf']:.2f} ${s['per_mo']:+,.0f}/mo DD${s.get('dd',0):,.0f} score={score:.0f}")

# Detail + proof on best
if all_results:
    best_name, best_s, best_kw, best_r = all_results[0]
    print(f"\n{'='*70}")
    print(f"  BEST: {best_name}")
    print(f"{'='*70}")
    stats(f"BEST — {best_name}", best_r, detail=True)

    print(f"\n{'='*70}")
    print(f"  PROOF TESTS — {best_name}")
    print(f"{'='*70}")

    r_d1 = run_backtest(**best_kw, delay=1)
    stats("DELAY +1 bar", r_d1)

    r_d3 = run_backtest(**best_kw, delay=3)
    stats("DELAY +3 bars", r_d3)

    r_rev = run_backtest(**best_kw, reverse=True)
    stats("REVERSED", r_rev)

    # Also show #2 config detail
    if len(all_results) > 1:
        name2, s2, kw2, r2 = all_results[1]
        print(f"\n{'='*70}")
        print(f"  #2: {name2}")
        print(f"{'='*70}")
        stats(f"#2 — {name2}", r2, detail=True)

        print(f"\n  PROOF:")
        r2_d1 = run_backtest(**kw2, delay=1)
        stats("DELAY +1 bar", r2_d1)

print(f"\nRuntime: {_time.time()-t0:.1f}s")
