# KCOMS V9.6 - FULL - FVG + REVERSAL + IFVG + TRAILING + 2ND POSITION
# Paper Only. All videos: FVG + Golden + Homma + Killzone + Protected Low + Valid CHoCH + IFVG + Trailing + Pyramid
# HTF: 1h Supply/Demand + Protected | LTF: 5m CHoCH + Sweep + FVG/IFVG | 1-7 entries/day, max 2 open, 2nd at half risk
import threading, time, requests, os, json, math
from flask import Flask, jsonify
from datetime import datetime, timezone, timedelta

app = Flask(__name__)
SYMBOL = os.getenv("SYMBOL", "BTCUSDT").upper()
TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT = os.getenv("TELEGRAM_CHAT_ID", "")

MAX_TRADES_PER_DAY = int(os.getenv("MAX_TRADES_PER_DAY", "7"))
MAX_OPEN_POSITIONS = int(os.getenv("MAX_OPEN_POSITIONS", "2"))
COOLDOWN_MINUTES = int(os.getenv("COOLDOWN_MINUTES", "5"))
RISK_PERCENT = float(os.getenv("RISK_PERCENT", "0.1"))
ADDON_RISK_PERCENT = float(os.getenv("ADDON_RISK_PERCENT", "0.05"))
MAX_NOTIONAL = float(os.getenv("MAX_NOTIONAL_USDT", os.getenv("TRADE_SIZE_USDT", "5")))
MAX_DAILY_LOSS = float(os.getenv("MAX_DAILY_LOSS_USDT", "3.0"))
MAX_CONSEC_LOSS = int(os.getenv("MAX_CONSECUTIVE_LOSSES", "3"))
PAPER_FILE = os.getenv("PAPER_BALANCE_FILE", "/tmp/kcoms_v9_balance.json")
START_BAL = float(os.getenv("PAPER_BALANCE", "100"))
SCAN_INTERVAL = 1
FEE = 0.001

EAT = timezone(timedelta(hours=3), "EAT")
def now_eat(): return datetime.now(EAT)

state = {"balance": START_BAL, "wins":0, "losses":0, "trades_today":0, "day": now_eat().date().isoformat(),
         "day_start": START_BAL, "consec_loss":0, "cooldown_until":0, "positions":[], "trade_history":[],
         "tier_a_count":0, "tier_b_count":0}
latest = {"bot":"KCOMS V9.6", "status":"BOOTING", "price":0, "bias":"NEUTRAL", "fvg":None, "ce":None, "tier":"NONE",
          "killzone":"NONE", "golden":False, "homma":"NONE", "protected":None, "choch_valid":False, "ifvg":None, "in_zone":False, "open_pos":0,
          "balance":START_BAL, "wins":0, "losses":0, "trades_today":0, "last":"Booting V9.6", "rejection":""}

SESSION = requests.Session()
SESSION.headers.update({"User-Agent":"Mozilla/5.0"})
BASES = ["https://data-api.binance.vision","https://api.binance.com","https://api1.binance.com","https://api2.binance.com","https://api3.binance.com"]

def send_tg(msg):
    try:
        if TG_TOKEN and TG_CHAT:
            SESSION.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage", json={"chat_id":TG_CHAT,"text":msg}, timeout=5)
    except: pass

def get_klines(tf, limit=250):
    for base in BASES:
        try:
            r = SESSION.get(f"{base}/api/v3/klines", params={"symbol":SYMBOL,"interval":tf,"limit":limit}, timeout=(2,4))
            d = r.json()
            if isinstance(d, list) and len(d)>30:
                return [{"t":int(k[0]),"o":float(k[1]),"h":float(k[2]),"l":float(k[3]),"c":float(k[4]),"v":float(k[5]),"ct":int(k[6])} for k in d]
        except: pass
    return None

def closed(rows):
    now_ms = int(time.time()*1000)
    return [c for c in rows if c["ct"] < now_ms]

def ema(vals, p):
    if len(vals)<p: return None
    k=2/(p+1); e=sum(vals[:p])/p
    for v in vals[p:]: e=v*k+e*(1-k)
    return e

def atr(rows, p=14):
    if len(rows)<p+1: return None
    tr=[max(rows[i]["h"]-rows[i]["l"], abs(rows[i]["h"]-rows[i-1]["c"]), abs(rows[i]["l"]-rows[i-1]["c"])) for i in range(1,len(rows))]
    return sum(tr[-p:])/p

def get_killzone():
    h = now_eat().hour + now_eat().minute/60.0
    if 9.0 <= h < 11.0: return ("LONDON_SILVER_BULLET", 20)
    if 17.0 <= h < 19.0: return ("NY_SILVER_BULLET", 20)
    if 10.0 <= h < 12.0: return ("LONDON_OPEN", 15)
    if 15.5 <= h < 17.0: return ("NY_OPEN", 15)
    if 20.0 <= h < 22.0: return ("NY_PM", 5)
    if 22.0 <= h or h < 8.0: return ("DEAD_ZONE", -10)
    return ("NORMAL", 0)

def find_fvgs(rows):
    fvgs=[]
    for i in range(1, len(rows)-1):
        c1, c2, c3 = rows[i-1], rows[i], rows[i+1]
        if c1["h"] < c3["l"]:
            top, bot = c3["l"], c1["h"]
            fvgs.append({"dir":"LONG","top":top,"bot":bot,"ce":(top+bot)/2,"size":top-bot,"idx":i,"time":c2["t"]})
        if c1["l"] > c3["h"]:
            top, bot = c1["l"], c3["h"]
            fvgs.append({"dir":"SHORT","top":top,"bot":bot,"ce":(top+bot)/2,"size":top-bot,"idx":i,"time":c2["t"]})
    return fvgs

def get_protected_level(rows, bias):
    if len(rows)<30: return None
    if bias=="BULLISH":
        return min(c["l"] for c in rows[-25:-5])
    elif bias=="BEARISH":
        return max(c["h"] for c in rows[-25:-5])
    return None

def get_htf_zone(rows1h, bias):
    fvgs = find_fvgs(rows1h)
    if not fvgs: return None, False
    if bias=="BULLISH":
        cands = [f for f in fvgs if f["dir"]=="LONG"]
    else:
        cands = [f for f in fvgs if f["dir"]=="SHORT"]
    if not cands: return None, False
    z = cands[-1]
    return z, True

def is_valid_choch(rows5, protected, direction, atr5):
    if protected is None: return False, "NO_PROTECTED"
    last3 = rows5[-3:]
    last = rows5[-1]
    if direction=="SHORT":
        continued = min(c["l"] for c in last3) < protected - 0.5*atr5
        if not continued: return False, "INVALID: Failure to Continue"
        prior_high = max(c["h"] for c in rows5[-10:-3])
        if max(c["c"] for c in last3) > prior_high: return False, "INVALID: Broke back above High"
        if last["l"] < protected and last["c"] > protected: return False, "INVALID: Liquidity Grab wick only"
    if direction=="LONG":
        continued = max(c["h"] for c in last3) > protected + 0.5*atr5
        if not continued: return False, "INVALID: Failure to Continue"
        prior_low = min(c["l"] for c in rows5[-10:-3])
        if min(c["c"] for c in last3) < prior_low: return False, "INVALID: Broke back below Low"
        if last["h"] > protected and last["c"] < protected: return False, "INVALID: Liquidity Grab wick only"
    return True, "VALID CHoCH"

def find_ifvg(rows):
    ifvgs=[]
    fvgs = find_fvgs(rows)
    for f in fvgs:
        for c in rows[-5:]:
            if f["dir"]=="SHORT" and c["c"] > f["top"]:
                ifvgs.append({"dir":"LONG","top":f["top"],"bot":f["bot"],"ce":f["ce"],"type":"IFVG"})
                break
            if f["dir"]=="LONG" and c["c"] < f["bot"]:
                ifvgs.append({"dir":"SHORT","top":f["top"],"bot":f["bot"],"ce":f["ce"],"type":"IFVG"})
                break
    return ifvgs

def find_sweep_and_bos(rows5, direction):
    seg = rows5[-30:]
    if len(seg)<20: return False, False, None, None, None
    if direction=="LONG":
        lows = [c["l"] for c in seg[-15:-5]]
        eq_low = min(lows)
        sweep=False; sweep_low=None
        for c in seg[-10:]:
            if c["l"] < eq_low and c["c"] > eq_low:
                sweep=True; sweep_low=c["l"]; break
        last_high = max(c["h"] for c in seg[-20:-5])
        bos = rows5[-1]["c"] > last_high
        move_start = sweep_low if sweep_low else min(c["l"] for c in seg[-20:])
        move_end = max(c["h"] for c in seg[-5:])
        return sweep, bos, sweep_low, move_start, move_end
    else:
        highs = [c["h"] for c in seg[-15:-5]]
        eq_high = max(highs)
        sweep=False; sweep_high=None
        for c in seg[-10:]:
            if c["h"] > eq_high and c["c"] < eq_high:
                sweep=True; sweep_high=c["h"]; break
        last_low = min(c["l"] for c in seg[-20:-5])
        bos = rows5[-1]["c"] < last_low
        move_start = sweep_high if sweep_high else max(c["h"] for c in seg[-20:])
        move_end = min(c["l"] for c in seg[-5:])
        return sweep, bos, sweep_high, move_start, move_end

def is_golden_fvg(fvg, move_start, move_end, direction):
    rng = abs(move_end - move_start)
    if rng == 0: return False, False
    if direction=="LONG":
        fib50 = move_end - 0.50*rng; fib618 = move_end - 0.618*rng
        fib382 = move_end - 0.382*rng; fib786 = move_end - 0.786*rng
        golden = not (fvg["top"] < fib618 or fvg["bot"] > fib50)
        standard = not (fvg["top"] < fib786 or fvg["bot"] > fib382)
        return golden, standard
    else:
        fib50 = move_end + 0.50*rng; fib618 = move_end + 0.618*rng
        fib382 = move_end + 0.382*rng; fib786 = move_end + 0.786*rng
        golden = not (fvg["bot"] > fib618 or fvg["top"] < fib50)
        standard = not (fvg["bot"] > fib786 or fvg["top"] < fib382)
        return golden, standard

def homma_confirmation(candles, direction):
    if len(candles)<2: return False, "NONE"
    c = candles[-1]; prev = candles[-2]
    body = abs(c["c"] - c["o"]); rng = c["h"] - c["l"]
    if rng==0: return False, "NONE"
    if direction=="LONG":
        lower_wick = min(c["o"], c["c"]) - c["l"]
        if lower_wick > 2*body and c["c"] > c["o"]: return True, "BULLISH_PIN_BAR"
        if c["c"] > prev["o"] and c["o"] < prev["c"] and c["c"] > c["o"]: return True, "BULLISH_ENGULFING"
        if body < rng*0.2: return True, "DOJI"
    else:
        upper_wick = c["h"] - max(c["o"], c["c"])
        if upper_wick > 2*body and c["c"] < c["o"]: return True, "BEARISH_PIN_BAR"
        if c["c"] < prev["o"] and c["o"] > prev["c"] and c["c"] < c["o"]: return True, "BEARISH_ENGULFING"
        if body < rng*0.2: return True, "DOJI"
    return False, "NONE"

def filter_fvgs(fvgs, rows1h, atr1h, direction):
    cands = [f for f in fvgs if f["dir"]==direction]
    if not cands: return None
    cands = [f for f in cands if f["size"] >= 0.5*atr1h]
    if not cands: return None
    move_low = min(c["l"] for c in rows1h[-20:])
    move_high = max(c["h"] for c in rows1h[-20:])
    mid = (move_low+move_high)/2
    if direction=="LONG":
        cands = [f for f in cands if f["ce"] < mid]
        cands.sort(key=lambda x: x["ce"])
    else:
        cands = [f for f in cands if f["ce"] > mid]
        cands.sort(key=lambda x: -x["ce"])
    if not cands: return None
    best = cands[0]
    for lvl in [move_high, move_low]:
        if abs(best["ce"]-lvl) < 0.5*atr1h: return None
    nearest_round = round(best["ce"]/1000)*1000
    if abs(best["ce"]-nearest_round) < 0.3*atr1h: return None
    return best

def analyse():
    d1h = get_klines("1h", 250); d15 = get_klines("15m", 250); d5 = get_klines("5m", 250); d1 = get_klines("1m", 250)
    if not all([d1h,d15,d5,d1]): return None
    c1h, c15, c5, c1 = closed(d1h), closed(d15), closed(d5), closed(d1)
    if len(c1h)<50 or len(c5)<50: return None
    price = d1[-1]["c"]
    closes1h = [c["c"] for c in c1h]
    e200 = ema(closes1h,200)
    atr1h_val = atr(c1h) or 100
    atr5 = atr(c5) or 50
    bias = "NEUTRAL"
    if e200 and price>e200: bias="BULLISH"
    if e200 and price<e200: bias="BEARISH"
    kz_name, kz_bonus = get_killzone()
    fvgs1h = find_fvgs(c1h)
    fvgs15 = find_fvgs(c15)
    htf_zone, has_zone = get_htf_zone(c1h, bias)
    protected = get_protected_level(c1h, bias)
    ifvgs = find_ifvg(c5)
    results={}
    for direction in ["LONG","SHORT"]:
        is_reversal = (bias=="BULLISH" and direction=="SHORT") or (bias=="BEARISH" and direction=="LONG")
        sweep, bos, sweep_extreme, move_start, move_end = find_sweep_and_bos(c5, direction)
        disp=False
        if len(c5)>=3:
            last3=c5[-3:]
            avg_range=sum(c["h"]-c["l"] for c in c5[-20:-3])/17
            disp=(abs(last3[-1]["c"]-last3[0]["o"]) > 1.5*avg_range)
        fvg = filter_fvgs(fvgs1h, c1h, atr1h_val, direction)
        if not fvg:
            fvg = filter_fvgs(fvgs15, c15, atr(c15) or 50, direction)
        ifvg = None
        for iv in ifvgs:
            if iv["dir"]==direction: ifvg=iv; break
        golden=False; standard=False
        if fvg and move_start is not None and move_end is not None:
            golden, standard = is_golden_fvg(fvg, move_start, move_end, direction)
        in_fvg=False; near_ce=False; invalid=False
        if fvg:
            in_fvg = fvg["bot"] <= price <= fvg["top"]
            near_ce = abs(price - fvg["ce"]) <= 0.25*fvg["size"] + 0.2*atr5
            for c in c5[-5:]:
                if direction=="LONG" and c["c"] < fvg["bot"]: invalid=True
                if direction=="SHORT" and c["c"] > fvg["top"]: invalid=True
            if invalid: fvg=None; in_fvg=False; near_ce=False; golden=False; standard=False
        homma_ok, homma_name = homma_confirmation(c5[-3:], direction)
        choch=False
        if len(c5)>=6:
            micro=c5[-6:-1]
            if direction=="LONG" and c5[-1]["c"] > max(x["h"] for x in micro): choch=True
            if direction=="SHORT" and c5[-1]["c"] < min(x["l"] for x in micro): choch=True
        old_bias = "BEARISH" if direction=="LONG" else "BULLISH"
        prot_for_check = get_protected_level(c1h, old_bias)
        valid_choch, choch_reason = is_valid_choch(c5, prot_for_check, direction, atr5)
        if not choch:
            valid_choch=False; choch_reason="NO CHoCH"
        in_zone=False
        if htf_zone:
            if htf_zone["bot"] - 0.5*atr1h_val <= price <= htf_zone["top"] + 0.5*atr1h_val:
                in_zone=True
        if is_reversal and not in_zone:
            results[direction]={"score":0,"reasons":["REVERSAL outside HTF Supply/Demand - No trade"],"confirmed":False,"fvg":None,"sl":0,"tp":0,"entry":price,"tier":None,"golden":False,"homma":False,"homma_name":"NONE","near_ce":False,"sweep":False,"bos":False,"protected":protected,"valid_choch":False,"ifvg":None,"in_zone":False}
            continue
        score=0; reasons=[f"KZ:{kz_name} {kz_bonus:+d}"]
        if sweep: score+=25; reasons.append("Sweep")
        if disp: score+=15; reasons.append("Displacement")
        if bos: score+=20; reasons.append("BOS")
        if fvg: score+=20; reasons.append(f"FVG {fvg['bot']:.1f}-{fvg['top']:.1f} CE {fvg['ce']:.1f}")
        if golden: score+=25; reasons.append("GOLDEN 50-61.8%")
        elif standard: score+=10; reasons.append("Standard 38.2-78.6%")
        if ifvg: score+=25; reasons.append(f"IFVG {ifvg['bot']:.1f}-{ifvg['top']:.1f}")
        if in_fvg: score+=10; reasons.append("Price in FVG")
        if near_ce: score+=15; reasons.append("At CE 50%")
        if choch: score+=10; reasons.append("5m CHoCH")
        if valid_choch: score+=20; reasons.append(f"VALID CHoCH {choch_reason}")
        else: reasons.append(f"{choch_reason}")
        if homma_ok: score+=20; reasons.append(f"Homma {homma_name}")
        if in_zone: score+=15; reasons.append("In HTF Supply/Demand")
        if bias==("BULLISH" if direction=="LONG" else "BEARISH"): score+=5; reasons.append("HTF aligned")
        score+=kz_bonus
        tier=None
        trades_today = state["trades_today"]
        if trades_today >= 7:
            tier=None
        elif (golden or ifvg) and homma_ok and valid_choch and score>=95:
            tier="A_GOLDEN"
        elif trades_today < 3:
            if score>=105 and homma_ok and valid_choch and kz_name in ("LONDON_SILVER_BULLET","NY_SILVER_BULLET"):
                tier="A_GOLDEN"
            else:
                tier=None
        elif trades_today >=3 and trades_today <7:
            if (standard or ifvg) and score>=85 and valid_choch and (homma_ok or choch):
                tier="B_STANDARD"
        if ifvg:
            entry = ifvg["bot"] if direction=="LONG" else ifvg["top"]
        elif homma_ok:
            entry = c5[-1]["c"]
        elif fvg:
            entry = fvg["ce"]
        else:
            entry = price
        if direction=="LONG":
            sl = (c5[-1]["l"] - 0.2*atr5) if homma_ok else ((sweep_extreme - 0.3*atr5) if sweep_extreme else (fvg["bot"]-0.5*atr5 if fvg else price-atr5))
            if ifvg: sl = ifvg["bot"] - 0.3*atr5
            tp = max(c["h"] for c in c1h[-20:])
            if tp - entry < 2*abs(entry-sl):
                tp = entry + 2*abs(entry-sl)
        else:
            sl = (c5[-1]["h"] + 0.2*atr5) if homma_ok else ((sweep_extreme + 0.3*atr5) if sweep_extreme else (fvg["top"]+0.5*atr5 if fvg else price+atr5))
            if ifvg: sl = ifvg["top"] + 0.3*atr5
            tp = min(c["l"] for c in c1h[-20:])
            if entry - tp < 2*abs(entry-sl):
                tp = entry - 2*abs(entry-sl)
        confirmed = tier is not None and sweep and bos and disp and (fvg or ifvg) and valid_choch and (near_ce or ifvg) and (homma_ok or choch)
        if not confirmed: tier=None
        results[direction]={"score":score,"reasons":reasons,"confirmed":confirmed,"fvg":fvg,"sl":sl,"tp":tp,"entry":entry,"tier":tier,"golden":golden,"homma":homma_ok,"homma_name":homma_name,"near_ce":near_ce,"sweep":sweep,"bos":bos,"disp":disp,"choch":choch,"protected":protected,"valid_choch":valid_choch,"ifvg":ifvg,"in_zone":in_zone}
    best_dir = max(results, key=lambda k: results[k]["score"])
    return {"price":price,"bias":bias,"best":best_dir,"results":results,"ema":e200,"atr1h":atr1h_val,"kz":kz_name,"protected":protected,"htf_zone":htf_zone}

def load():
    try:
        with open(PAPER_FILE) as f: s=json.load(f)
        # Migrate old V9.5 single position to list
        if "position" in s and s["position"]:
            s["positions"]=[s["position"]]
        if "positions" not in s: s["positions"]=[]
        state.update(s)
    except: pass
def save():
    try:
        with open(PAPER_FILE,"w") as f: json.dump({k:state[k] for k in ["balance","wins","losses","trades_today","day","day_start","consec_loss","cooldown_until","positions","trade_history","tier_a_count","tier_b_count"]}, f)
    except: pass

def open_pos(a, direction, is_addon=False):
    r = a["results"][direction]; fvg=r["fvg"]
    entry = r["entry"]; sl, tp = r["sl"], r["tp"]
    risk_dist = abs(entry-sl)
    if risk_dist<=0: return False
    risk_pct = ADDON_RISK_PERCENT if is_addon else RISK_PERCENT
    risk_cash = state["balance"]*(risk_pct/100)
    qty = min(risk_cash/risk_dist, MAX_NOTIONAL/entry)
    if qty<=0: return False
    # V9.6 ADD-ON CHECKS: New FVG + First in +1R profit
    if is_addon:
        first = state["positions"][0] if state["positions"] else None
        if first and first.get("fvg") and fvg:
            # Must be NEW FVG, not same CE
            if abs(first["fvg"]["ce"] - fvg["ce"]) < 0.2*abs(first["fvg"]["top"]-first["fvg"]["bot"]):
                return False
        if first:
            profit = (a["price"] - first["entry"]) if direction=="LONG" else (first["entry"] - a["price"])
            first_risk = abs(first["entry"] - first["orig_sl"])
            if first_risk==0 or profit < 1.0*first_risk:
                return False
    pos = {"side":direction,"entry":entry,"sl":sl,"tp":tp,"qty":qty,"notional":qty*entry,"fvg":fvg,"tier":r["tier"],"orig_sl":sl,"orig_tp":tp,"is_addon":is_addon}
    state["positions"].append(pos)
    state["trades_today"]+=1
    if r["tier"]=="A_GOLDEN": state["tier_a_count"]+=1
    else: state["tier_b_count"]+=1
    save()
    tag = "2ND ADD-ON (50% risk)" if is_addon else "1ST ENTRY"
    ifvg_txt = f" IFVG {r['ifvg']['bot']:.2f}-{r['ifvg']['top']:.2f}" if r["ifvg"] else ""
    msg=f"🟢 V9.6 {tag} {r['tier']} {direction} {state['trades_today']}/7 | Open {len(state['positions'])}/2\nEntry {entry:.2f} {'(IFVG Start)' if r['ifvg'] else '(Homma Close / CE)'}\nFVG {fvg['bot']:.2f}-{fvg['top']:.2f} CE {fvg['ce']:.2f} {ifvg_txt}\nGolden: {r['golden']} | Homma: {r['homma_name']} | Valid CHoCH: {r['valid_choch']}\nProtected: {r['protected']:.2f} | In HTF Zone: {r['in_zone']}\nSL {sl:.2f} | TP {tp:.2f} (1:2+ / Liquidity)\nScore {r['score']} | KZ {a['kz']}\nReasons: {', '.join(r['reasons'])}"
    print(msg, flush=True); send_tg(msg); latest["last"]=msg
    return True

def monitor(price, c5):
    for pos in list(state["positions"]):
        side=pos["side"]
        risk = abs(pos["entry"] - pos["orig_sl"])
        profit = (price - pos["entry"]) if side=="LONG" else (pos["entry"] - price)
        if risk>0 and profit > 1.0*risk:
            if side=="LONG":
                new_sl = min(c["l"] for c in c5[-5:]) - 0.2*atr(c5)
                if new_sl > pos["sl"]: pos["sl"]=new_sl
                if pos["sl"] < pos["entry"]: pos["sl"]=pos["entry"]
            else:
                new_sl = max(c["h"] for c in c5[-5:]) + 0.2*atr(c5)
                if new_sl < pos["sl"]: pos["sl"]=new_sl
                if pos["sl"] > pos["entry"]: pos["sl"]=pos["entry"]
        hit_tp = price>=pos["tp"] if side=="LONG" else price<=pos["tp"]
        hit_sl = price<=pos["sl"] if side=="LONG" else price>=pos["sl"]
        if not (hit_tp or hit_sl): continue
        exit_p = pos["tp"] if hit_tp else pos["sl"]
        gross = (exit_p-pos["entry"])*pos["qty"]*(1 if side=="LONG" else -1)
        fees = (pos["notional"]+pos["qty"]*exit_p)*FEE
        pnl = gross-fees
        state["balance"]+=pnl
        if pnl>0: state["wins"]+=1; state["consec_loss"]=0
        else: state["losses"]+=1; state["consec_loss"]+=1; state["cooldown_until"]=time.time()+COOLDOWN_MINUTES*60
        state["trade_history"].append({"side":side,"entry":pos["entry"],"exit":exit_p,"pnl":round(pnl,4),"result":"WIN" if pnl>0 else "LOSS","tier":pos.get("tier"),"addon":pos.get("is_addon")})
        state["positions"].remove(pos); save()
        msg=f"{'✅' if pnl>0 else '❌'} V9.6 {'TP' if hit_tp else 'SL/Trail'} {'ADD-ON' if pos.get('is_addon') else '1ST'} {side} Exit {exit_p:.2f} PnL ${pnl:.4f} Bal ${state['balance']:.2f} W:{state['wins']} L:{state['losses']} D:{state['trades_today']}/7 Open:{len(state['positions'])}/2"
        print(msg, flush=True); send_tg(msg); latest["last"]=msg

def run():
    load()
    print(f"START V9.6 FVG + REVERSAL + IFVG + TRAILING + 2ND POSITION - 1-7/Day", flush=True)
    send_tg("🚀 KCOMS V9.6 Online\nProtected + Valid CHoCH + IFVG + Trailing + 2nd Position\nTier A: Golden/IFVG | Tier B: Standard\nMax 7 entries/day | Max 2 open | 2nd at half risk")
    while True:
        try:
            today = now_eat().date().isoformat()
            if today!=state["day"]:
                state["day"]=today; state["trades_today"]=0; state["day_start"]=state["balance"]; state["consec_loss"]=0; state["tier_a_count"]=0; state["tier_b_count"]=0; save()
            if time.time()<state["cooldown_until"]:
                time.sleep(2); continue
            if state["day_start"]-state["balance"] >= MAX_DAILY_LOSS:
                latest["status"]="DAILY_LOSS_LIMIT"; time.sleep(5); continue
            if state["consec_loss"]>=MAX_CONSEC_LOSS:
                latest["status"]="LOSS_STREAK_STOP"; time.sleep(5); continue
            if state["trades_today"]>=MAX_TRADES_PER_DAY:
                latest["status"]="DAILY_LIMIT_7_REACHED"; time.sleep(10); continue
            a = analyse()
            if not a:
                time.sleep(SCAN_INTERVAL); continue
            price=a["price"]
            d5 = get_klines("5m", 250)
            c5 = closed(d5) if d5 else []
            monitor(price, c5)
            best=a["best"]; res=a["results"][best]
            latest.update({"price":round(price,2),"bias":a["bias"],"fvg":res["fvg"],"ce":res["fvg"]["ce"] if res["fvg"] else None,
                           "tier":res["tier"],"killzone":a["kz"],"golden":res["golden"],"homma":res["homma_name"],
                           "protected":res["protected"],"choch_valid":res["valid_choch"],"ifvg":res["ifvg"],"in_zone":res["in_zone"],"open_pos":len(state["positions"]),
                           "balance":round(state["balance"],2),"wins":state["wins"],"losses":state["losses"],"trades_today":state["trades_today"],
                           "status":"IN_POSITION" if state["positions"] else "SCANNING","rejection":", ".join(res["reasons"])})
            print(f"[{now_eat().strftime('%H:%M:%S')} EAT] Price {price:.2f} | Bias {a['bias']} | KZ {a['kz']} | Best {best} Score {res['score']} Tier {res['tier']} Golden {res['golden']} IFVG {res['ifvg']} ValidCHoCH {res['valid_choch']} InZone {res['in_zone']} | Bal ${state['balance']:.2f} D:{state['trades_today']}/7 Open:{len(state['positions'])}/2 A:{state['tier_a_count']} B:{state['tier_b_count']}", flush=True)
            if res["confirmed"]:
                if len(state["positions"]) == 0:
                    open_pos(a, best, is_addon=False)
                elif len(state["positions"]) == 1:
                    open_side = state["positions"][0]["side"]
                    if open_side == best:
                        open_pos(a, best, is_addon=True)
            time.sleep(SCAN_INTERVAL)
        except Exception as e:
            print(f"LOOP ERR {e}", flush=True); time.sleep(2)

@app.route("/")
def home():
    return f"<h1>KCOMS V9.6 ✅</h1><p>Price {latest['price']} | Bias {latest['bias']} | {latest['status']} | KZ {latest['killzone']} | Open {latest['open_pos']}/2</p><p>FVG {latest['fvg']} | CE {latest['ce']} | Tier {latest['tier']} | Golden {latest['golden']} | Homma {latest['homma']}</p><p>Protected {latest['protected']} | Valid CHoCH {latest['choch_valid']} | IFVG {latest['ifvg']} | In HTF Zone {latest['in_zone']}</p><p>Bal ${latest['balance']} W:{latest['wins']} L:{latest['losses']} D:{latest['trades_today']}/7 A:{state['tier_a_count']} B:{state['tier_b_count']}</p><p>{latest['last']}</p><p>Reasons: {latest['rejection']}</p>"

@app.route("/health")
def health(): return "ok",200
@app.route("/status")
def status(): return jsonify({**latest, "positions":state["positions"], "paper_only":True, "tier_a":state["tier_a_count"], "tier_b":state["tier_b_count"]})

threading.Thread(target=run, daemon=True).start()
if __name__=="__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT",10000)), threaded=True)
