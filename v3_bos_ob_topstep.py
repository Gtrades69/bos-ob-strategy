"""
BOS+OB Backtest on TopstepX API bars
=====================================
Fetches all available historical data from TopstepX (~2 months),
runs the exact same BOS+OB strategy from v3_bos_ob_final.py.
Config: age=45, risk>=5, bp35, London+NY, skip 7/12/14, RR 1.1
"""
import os, sys, time as _time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from collections import defaultdict
import bisect

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "tsxapi4py", "src"))

from tsxapipy import authenticate, APIClient

CT = ZoneInfo("America/Chicago")
UTC = ZoneInfo("UTC")
NS_MIN = 60_000_000_000
PV = 20; CTS = 3; SLIP = 0.5; FEES = 8.40

# NQ contracts: H26 covers ~Feb-Mar, M26 covers ~Mar-Jun
CONTRACTS_NQ = [
    ("CON.F.US.ENQ.H26", datetime(2026, 2, 15, 0, 0, tzinfo=CT), datetime(2026, 3, 21, 0, 0, tzinfo=CT)),
    ("CON.F.US.ENQ.M26", datetime(2026, 3, 15, 0, 0, tzinfo=CT), datetime(2026, 4, 16, 0, 0, tzinfo=CT)),
]


def fetch_bars(api, cid, tf_minutes, start_ct, end_ct):
    """Fetch bars in chunks, return list of dicts."""
    all_bars = []
    s = start_ct
    chunk_days = 2 if tf_minutes <= 1 else 3

    while s < end_ct:
        chunk_end = min(s + timedelta(days=chunk_days), end_ct)
        for attempt in range(3):
            try:
                s_utc = s.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S")
                e_utc = chunk_end.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S")
                resp = api.get_historical_bars(
                    contract_id=cid, start_time_iso=s_utc, end_time_iso=e_utc,
                    unit=2, unit_number=tf_minutes, limit=10000, live=False,
                )
                if resp and resp.bars:
                    all_bars.extend(resp.bars)
                break
            except Exception as ex:
                if attempt == 2:
                    print(f"  WARN: chunk fetch failed {cid} {s.date()}-{chunk_end.date()}: {ex}")
                else:
                    _time.sleep(2)
        s = chunk_end
        if s < end_ct:
            _time.sleep(0.3)

    seen = set()
    result = []
    for b in all_bars:
        try:
            t = b.t if hasattr(b.t, 'astimezone') else datetime.fromisoformat(str(b.t))
            t = t.astimezone(CT)
            ns = int(t.timestamp() * 1e9)
            # Align to minute boundary
            ns_a = ns - (ns % (tf_minutes * 60 * 10**9))
            if ns_a in seen:
                continue
            seen.add(ns_a)
            result.append({
                "time_ns": ns_a, "open": float(b.o), "high": float(b.h),
                "low": float(b.l), "close": float(b.c),
                "hour": t.hour, "minute": t.minute,
            })
        except Exception:
            continue
    result.sort(key=lambda x: x["time_ns"])
    return result


def fetch_all_bars(api, tf_minutes):
    """Fetch from all contracts, merge with rollover dedup."""
    all_bars = []
    for cid, start, end in CONTRACTS_NQ:
        print(f"  Fetching {cid} {tf_minutes}m: {start.date()} to {end.date()}...")
        bars = fetch_bars(api, cid, tf_minutes, start, end)
        print(f"    → {len(bars)} bars")
        all_bars.extend(bars)

    # Dedup by time_ns, keep last (front-month priority)
    seen = {}
    for b in all_bars:
        seen[b["time_ns"]] = b
    merged = [seen[k] for k in sorted(seen.keys())]
    return merged


def tday(ns):
    dt = datetime.fromtimestamp(ns / 1e9, tz=CT)
    return (dt + timedelta(days=1)).date() if dt.hour >= 17 else dt.date()


def run_backtest(b5, b1, b1_ns, day_r, ad,
    rr=1.1, max_risk=30, max_ed=5, min_risk=0,
    use_body=True, use_1mob=True,
    min_body_pct=0.35,
    session_start=120, session_end=870,
    skip_hours={7, 12, 14},
    cooldown_s=120, max_daily=8,
    zone_max_age_min=200,
    min_zone_size=0,
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


def stats(label, results, ad_count, detail=False):
    w = sum(1 for x in results if x["result"] == "WIN")
    l = sum(1 for x in results if x["result"] == "LOSS")
    tr = w + l
    if tr < 5:
        print(f"  {label}: {tr} trades (too few)")
        return {"wr": 0, "pf": 0, "per_mo": 0, "n": tr}
    wr = 100 * w / tr
    tot = sum(x["pnl"] for x in results)
    wp = sum(x["pnl"] for x in results if x["result"] == "WIN")
    lp = sum(x["pnl"] for x in results if x["result"] == "LOSS")
    pf = abs(wp / lp) if lp else 99
    mo = len(set(str(x["date"])[:7] for x in results)) or 1
    per_mo = tot / mo
    tpd = len(results) / max(ad_count, 1)

    mp = defaultdict(float)
    for x in results: mp[str(x["date"])[:7]] += x["pnl"]
    neg = sum(1 for v in mp.values() if v < 0)
    pos = len(mp) - neg

    eq = peak = dd = 0
    for x in results:
        eq += x["pnl"]
        if eq > peak: peak = eq
        if peak - eq > dd: dd = peak - eq

    print(f"\n  {label}")
    print(f"  {len(results)} tr ({tpd:.1f}/day) | {w}W/{l}L = {wr:.0f}% WR | PF {pf:.2f}")
    print(f"  ${per_mo:+,.0f}/mo | ${tot:+,.0f} total | DD ${dd:,.0f} | {pos}/{len(mp)} +months")

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
        print(f"\n  Daily:")
        daily = defaultdict(lambda: {"w": 0, "l": 0, "pnl": 0.0, "n": 0})
        for x in results:
            d = str(x["date"])
            daily[d]["pnl"] += x["pnl"]
            daily[d]["n"] += 1
            if x["result"] == "WIN": daily[d]["w"] += 1
            elif x["result"] == "LOSS": daily[d]["l"] += 1
        running = 0
        for d in sorted(daily):
            v = daily[d]
            running += v["pnl"]
            print(f"    {d} | {v['n']} tr ({v['w']}W {v['l']}L) | ${v['pnl']:>+8,.0f} | running ${running:>+9,.0f}")

        print(f"\n  Monthly:")
        for k in sorted(mp):
            flag = " <<<" if mp[k] < 0 else ""
            print(f"    {k}: ${mp[k]:>+9,.0f}{flag}")

    return {"wr": wr, "pf": pf, "per_mo": per_mo, "n": len(results), "dd": dd, "neg": neg}


def main():
    t0 = _time.time()
    print("=" * 70)
    print("  BOS+OB on TopstepX bars — Independent data validation")
    print("=" * 70)

    # Auth
    token, tt = authenticate()
    api = APIClient(initial_token=token, token_acquired_at=tt)
    print("Authenticated.\n")

    # Fetch all available 5m + 1m bars
    print("Fetching 5m bars...")
    b5_raw = fetch_all_bars(api, 5)
    print(f"  Total 5m: {len(b5_raw)}\n")

    print("Fetching 1m bars...")
    b1_raw = fetch_all_bars(api, 1)
    print(f"  Total 1m: {len(b1_raw)}\n")

    # Filter bad bars
    b5 = [b for b in b5_raw if b["high"] - b["low"] < 200 and b["low"] > 10000]
    b1 = [b for b in b1_raw if b["high"] - b["low"] < 100 and b["low"] > 10000]
    b1_ns = [b["time_ns"] for b in b1]

    # Build day ranges
    day_r = {}
    for i, bar in enumerate(b5):
        d = tday(bar["time_ns"])
        if d not in day_r: day_r[d] = [i, i + 1]
        else: day_r[d][1] = i + 1
    ad = sorted(day_r.keys())

    print(f"Ready in {_time.time()-t0:.1f}s — 5m={len(b5):,} 1m={len(b1):,} days={len(ad)}")
    print(f"Date range: {ad[0]} to {ad[-1]}")
    print()

    # ── WINNING CONFIG: age=45, risk>=5, bp35, London+NY, skip 7/12/14 ──
    cfg = dict(
        rr=1.1, max_risk=30, max_ed=5, min_risk=5,
        use_body=True, use_1mob=True,
        min_body_pct=0.35,
        session_start=120, session_end=870,
        skip_hours={7, 12, 14},
        cooldown_s=120, max_daily=8,
        zone_max_age_min=45,
    )

    print("=" * 70)
    print("  WINNING CONFIG on TopstepX data")
    print("  age=45 risk>=5 bp35 London+NY skip{7,12,14} RR1.1")
    print("=" * 70)

    r = run_backtest(b5, b1, b1_ns, day_r, ad, **cfg)
    s = stats("TopstepX — Winning Config", r, len(ad), detail=True)

    # ── PROOF TESTS ──
    print(f"\n{'='*70}")
    print("  PROOF TESTS")
    print(f"{'='*70}")

    r_d1 = run_backtest(b5, b1, b1_ns, day_r, ad, **cfg, delay=1)
    stats("DELAY +1 bar", r_d1, len(ad))

    r_d3 = run_backtest(b5, b1, b1_ns, day_r, ad, **cfg, delay=3)
    stats("DELAY +3 bars", r_d3, len(ad))

    r_rev = run_backtest(b5, b1, b1_ns, day_r, ad, **cfg, reverse=True)
    stats("REVERSED", r_rev, len(ad))

    # ── COMPARISON: Databento same period ──
    # For the user to compare — the Databento backtest covers this same 2-month window
    # within its 5-year dataset. TopstepX should match closely.

    # ── Also run baseline (no filters) for reference ──
    print(f"\n{'='*70}")
    print("  BASELINE (no zone age / risk filters)")
    print(f"{'='*70}")
    cfg_base = dict(
        rr=1.1, max_risk=30, max_ed=5, min_risk=0,
        use_body=True, use_1mob=True,
        min_body_pct=0.35,
        session_start=120, session_end=870,
        skip_hours={7, 12, 14},
        cooldown_s=120, max_daily=8,
        zone_max_age_min=200,
    )
    r_base = run_backtest(b5, b1, b1_ns, day_r, ad, **cfg_base)
    stats("TopstepX — Baseline (no filters)", r_base, len(ad), detail=True)

    print(f"\nTotal runtime: {_time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()
