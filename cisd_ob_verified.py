"""
CISD + OB — Reversal Entry Model
==================================
Same mechanic as BOS+OB but catches trend REVERSALS instead of continuations.

CISD = Change In State of Delivery:
- Price was trending bearish (LL + LH on 5m swings)
- Displacement candle closes ABOVE the last lower high → BULL CISD
- OR: Price was trending bullish (HH + HL)
- Displacement candle closes BELOW the last higher low → BEAR CISD

Then: create zones (BODY + 1mOB), wait for 1m pullback, enter.
Same stop/target/zone logic as BOS+OB v3.
"""
import pickle, bisect
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from collections import defaultdict

CT = ZoneInfo("America/Chicago")
NS_MIN = 60_000_000_000
PV = 20; CTS = 3; SLIP = 0.5; FEES = 8.40
RR = 1.1
MIN_BODY_PCT = 0.35
SESSION_START = 120   # London start
SESSION_END = 870     # 14:30 CT
SKIP_HOURS = {7, 12, 14}
ZONE_MAX_AGE = 45
MIN_RISK = 5
MAX_RISK = 30
MAX_ED = 5
COOLDOWN_S = 120
MAX_DAILY = 8

with open("/tmp/v6-nq-bot/strat_repo/databento_bars_clean.pkl", "rb") as f:
    dat = pickle.load(f)
b5 = [b for b in dat["b5"] if b["high"]-b["low"] < 200 and b["low"] > 10000]
b1 = [b for b in dat["b1"] if b["high"]-b["low"] < 100 and b["low"] > 10000]
b1_ns = [b["time_ns"] for b in b1]

def tday(ns):
    dt = datetime.fromtimestamp(ns/1e9, tz=CT)
    return (dt+timedelta(days=1)).date() if dt.hour >= 17 else dt.date()

day_r = {}
for i, bar in enumerate(b5):
    d = tday(bar["time_ns"])
    if d not in day_r: day_r[d] = [i, i+1]
    else: day_r[d][1] = i+1
ad = sorted(day_r.keys())
print(f"5m={len(b5):,}  1m={len(b1):,}  Days={len(ad)}", flush=True)

zone_age_ns = ZONE_MAX_AGE * NS_MIN
entries = []

for di, dd in enumerate(ad):
    if di < 2: continue
    ds, de = day_r[dd]
    de2 = b5[min(de-1, len(b5)-1)]["time_ns"] + 5*NS_MIN
    shs = []; sls = []; used = set(); active = []; last_ens = 0; day_n = 0
    filled = set()

    for cursor in range(ds+3, min(de, len(b5))):
        bar = b5[cursor]
        t = bar["hour"]*60 + bar["minute"]
        p1, p2 = b5[cursor-1], b5[cursor-2]

        # Swing detection (same as BOS+OB)
        if p1["high"] > p2["high"] and p1["high"] > bar["high"]:
            depth = p1["high"] - sls[-1][0] if sls else 999
            if depth >= 5: shs.append((p1["high"], cursor-1))
        if p1["low"] < p2["low"] and p1["low"] < bar["low"]:
            depth = shs[-1][0] - p1["low"] if shs else 999
            if depth >= 5: sls.append((p1["low"], cursor-1))

        if not (SESSION_START <= t < SESSION_END): continue
        if bar["hour"] in SKIP_HOURS: continue
        if len(shs) < 2 or len(sls) < 2: continue
        if day_n >= MAX_DAILY: continue

        # Displacement body filter
        body = abs(bar["close"] - bar["open"])
        rng = bar["high"] - bar["low"]
        if rng <= 0 or body/rng < MIN_BODY_PCT: continue

        # ============================================================
        # CISD DETECTION (instead of BOS)
        # ============================================================
        # Check previous structure and current break
        hh = shs[-1][0] > shs[-2][0]  # Last SH higher than previous
        hl = sls[-1][0] > sls[-2][0]  # Last SL higher than previous
        ll = sls[-1][0] < sls[-2][0]  # Last SL lower than previous
        lh = shs[-1][0] < shs[-2][0]  # Last SH lower than previous

        direction = None

        # BULL CISD: previous structure was bearish (LL or LH), now closes above last swing high
        # This is the FIRST break upward after bearish structure
        if (ll or lh) and bar["close"] > shs[-1][0]:
            # Make sure this is NOT a BOS (which requires HH+HL)
            # CISD = the structure was bearish, now breaking bullish
            if not (hh and hl):  # NOT already in confirmed uptrend
                direction = "bull"

        # BEAR CISD: previous structure was bullish (HH or HL), now closes below last swing low
        elif (hh or hl) and bar["close"] < sls[-1][0]:
            if not (ll and lh):  # NOT already in confirmed downtrend
                direction = "bear"

        if direction is None or cursor in used: continue
        used.add(cursor)
        created_ns = bar["time_ns"] + 5*NS_MIN

        # ============================================================
        # ZONE CREATION (identical to BOS+OB v3)
        # ============================================================
        # BODY zone
        if direction == "bull":
            bzt, bzb, bsp = bar["close"], bar["open"], bar["low"]-1
        else:
            bzt, bzb, bsp = bar["open"], bar["close"], bar["high"]+1
        if bzt > bzb + 1:
            zk = ("BODY", direction, round(bzt,1), round(bzb,1))
            if zk not in filled:
                active.append({"side":direction, "zt":bzt, "zb":bzb, "sp":bsp,
                    "created_ns":created_ns, "cursor":cursor, "date":dd,
                    "type":"BODY", "zk":zk})

        # 1mOB zone
        disp_start = b5[max(cursor-1, ds)]["time_ns"]
        disp_end = bar["time_ns"] + 5*NS_MIN
        si_s = bisect.bisect_left(b1_ns, disp_start)
        si_e = bisect.bisect_left(b1_ns, disp_end)
        for k in range(min(si_e-1, len(b1)-1), max(si_s, 0), -1):
            kb = b1[k]
            if direction == "bull" and kb["close"] < kb["open"] and kb["high"]-kb["low"] > 1:
                zk = ("1mOB", direction, round(kb["open"],1), round(kb["close"],1))
                if zk not in filled:
                    active.append({"side":direction, "zt":kb["open"], "zb":kb["close"],
                        "sp":kb["low"]-1, "created_ns":created_ns, "cursor":cursor,
                        "date":dd, "type":"1mOB", "zk":zk})
                break
            elif direction == "bear" and kb["close"] > kb["open"] and kb["high"]-kb["low"] > 1:
                zk = ("1mOB", direction, round(kb["close"],1), round(kb["open"],1))
                if zk not in filled:
                    active.append({"side":direction, "zt":kb["close"], "zb":kb["open"],
                        "sp":kb["high"]+1, "created_ns":created_ns, "cursor":cursor,
                        "date":dd, "type":"1mOB", "zk":zk})
                break

        # ============================================================
        # 1m RETRACE TO ZONES (identical to BOS+OB v3)
        # ============================================================
        cursor_end = bar["time_ns"] + 5*NS_MIN
        prev_end = b5[cursor-1]["time_ns"] + 5*NS_MIN if cursor > ds else 0
        si = bisect.bisect_left(b1_ns, prev_end)
        new_active = []
        for z in active:
            if cursor_end - z["created_ns"] > zone_age_ns: continue
            if z["zk"] in filled: continue
            found = False
            for bi in range(si, len(b1)):
                c1 = b1[bi]
                if c1["time_ns"] >= cursor_end: break
                if c1["time_ns"] < z["created_ns"]: continue
                if c1["time_ns"] - last_ens < COOLDOWN_S*10**9: continue
                touched = False
                if z["side"] == "bull" and c1["low"] <= z["zt"] and c1["close"] >= z["zb"]:
                    if abs(c1["close"] - z["zt"]) > MAX_ED: continue
                    ep = c1["close"] + SLIP; risk = ep - z["sp"]
                    if risk >= MIN_RISK and risk <= MAX_RISK:
                        tp = ep + risk*RR; touched = True
                elif z["side"] == "bear" and c1["high"] >= z["zb"] and c1["close"] <= z["zt"]:
                    if abs(c1["close"] - z["zb"]) > MAX_ED: continue
                    ep = c1["close"] - SLIP; risk = z["sp"] - ep
                    if risk >= MIN_RISK and risk <= MAX_RISK:
                        tp = ep - risk*RR; touched = True
                if touched:
                    filled.add(z["zk"]); found = True; last_ens = c1["time_ns"]; day_n += 1
                    entries.append({"date":z["date"], "side":z["side"], "ep":ep,
                        "sp":z["sp"], "tp":tp, "risk":risk, "ens":c1["time_ns"],
                        "de2":de2, "type":z["type"]})
                    break
            if not found: new_active.append(z)
        active = new_active

# Simulate (identical to BOS+OB v3)
entries.sort(key=lambda x: x["ens"])
results = []; cur_day = None; in_pos = False; pos_exit = 0; cooldown = 0
for e in entries:
    if e["date"] != cur_day:
        cur_day = e["date"]; in_pos = False; pos_exit = 0; cooldown = 0
    if in_pos and e["ens"] < pos_exit: continue
    if e["ens"] < cooldown: continue
    si = bisect.bisect_right(b1_ns, e["ens"])
    res = "OPEN"; pnl = 0
    for bi in range(si, min(si+500, len(b1))):
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
        cooldown = pos_exit + COOLDOWN_S*10**9
    results.append({**e, "result":res, "pnl":pnl})

# Results
w = sum(1 for x in results if x["result"]=="WIN")
l = sum(1 for x in results if x["result"]=="LOSS")
eod = sum(1 for x in results if x["result"]=="EOD")
tot = sum(x["pnl"] for x in results)
wp = sum(x["pnl"] for x in results if x["result"]=="WIN")
lp = sum(x["pnl"] for x in results if x["result"]=="LOSS")
tr = w+l; wr = 100*w/tr if tr else 0
pf = abs(wp/lp) if lp else 0
mo = len(set(str(x["date"])[:7] for x in results)) or 1
ar = sum(x["risk"] for x in results)/len(results) if results else 0

print(f"\n{'='*60}")
print(f"CISD + OB REVERSAL | {len(ad)} days | {CTS}ct | RR {RR}")
print(f"{'='*60}")
print(f"Trades: {len(results)} | W{w} L{l} EOD{eod}")
print(f"Win Rate: {wr:.0f}% | Profit Factor: {pf:.2f}")
print(f"Avg Win: ${wp/w if w else 0:+,.0f} | Avg Loss: ${lp/l if l else 0:+,.0f} | Avg Risk: {ar:.0f}pts")
print(f"Total PnL: ${tot:+,.0f} | Monthly: ${tot/mo:+,.0f}")
print(f"Trades/Day: {len(results)/len(ad):.1f}")

print(f"\nBy zone type:")
for zt in sorted(set(x["type"] for x in results)):
    zw = sum(1 for x in results if x["result"]=="WIN" and x["type"]==zt)
    zl = sum(1 for x in results if x["result"]=="LOSS" and x["type"]==zt)
    zpnl = sum(x["pnl"] for x in results if x["type"]==zt)
    print(f"  {zt:<6}: {zw}W/{zl}L = {100*zw/(zw+zl):.0f}% | ${zpnl/mo:+,.0f}/mo")

print(f"\nMonthly:")
mp = defaultdict(float); mw = defaultdict(int); ml = defaultdict(int); mc = defaultdict(int)
for x in results:
    k = str(x["date"])[:7]; mp[k] += x["pnl"]; mc[k] += 1
    if x["result"]=="WIN": mw[k] += 1
    elif x["result"]=="LOSS": ml[k] += 1
for k in sorted(mp.keys()):
    flag = " <<<" if mp[k] < 0 else ""
    print(f"  {k}: {mw[k]}W/{ml[k]}L ${mp[k]:>+9,.0f} ({mc[k]}t){flag}")
pos = sum(1 for v in mp.values() if v > 0)
print(f"Positive months: {pos}/{len(mp)}")
