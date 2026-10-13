"""
NIFTY option-buying bot (Angel One SmartAPI) - LIVE TRADING - "SMART MONEY" VERSION

Idea: follow the footprints large players leave, not just raw OI numbers.

BASE SIGNAL (mandatory) - option WRITER FLOW (writers are mostly institutions)
  Bullish: put WRITING at/below spot (PE OI up while PE price flat/down) + call UNWINDING/short-covering at/above spot
  Bearish: call WRITING at/above spot (CE OI up while CE price flat/down) + put UNWINDING at/below spot
  Needs flow >= MIN_FLOW_OI and bull/bear flow ratio >= WRITER_RATIO over the last LOOKBACK_MIN minutes.

SMART-MONEY CONFIRMATIONS (need >= MIN_EXTRA_SCORE points)
  +2  Futures build-up: price up + futures OI up (long build-up) for bulls; price down + OI up (short build-up) for bears
  +2  Liquidity sweep + reclaim on 5-min futures candles (stop-hunt below swing low then close back above = bull;
      sweep above swing high then close back below = bear)
  +1  Volume spike in the option we want to buy (latest interval volume >= VOL_SPIKE_X x recent average)

REQUIRED FILTERS
  - VWAP side (NIFTY futures VWAP) held for VWAP_HOLD snapshots, and price not stretched from VWAP
  - OI walls: enough room (MIN_ROOM_PTS) to the opposing max-OI wall (call wall above for bulls, put wall below for bears)
  - Chosen option (highest volume within ATM +/- CANDIDATE_RANGE) trading above its own VWAP
  - Signal confirmed on CONFIRM_COUNT consecutive snapshots

TRADE MANAGEMENT
  BUY limit -> confirm fill -> exchange-side STOPLOSS_LIMIT -> software target at fixed 1:2 risk-reward.
  Market data keeps updating while a trade is open. Safety: max trades, max daily loss, cooldown after loss,
  time exit, STOP-file kill switch, refuses to start with an open NIFTY position, logs out on exit.

Install:  pip install smartapi-python pyotp requests logzero websocket-client
Secrets via env vars: ANGEL_API_KEY, ANGEL_CLIENT_ID, ANGEL_PIN, ANGEL_TOTP_SECRET  (optional: TG_BOT_TOKEN, TG_CHAT_ID)
Kill switch: create an empty file named STOP next to this script.
All thresholds below are STARTING GUESSES - tune them using the data saved in nifty_chain.db.
"""
import os, time, atexit, sqlite3, datetime as dt
from collections import deque
import requests, pyotp
from SmartApi import SmartConnect

# ---------------- CONFIG ----------------
API_KEY     = os.getenv("ANGEL_API_KEY", "29TdHjRW")
CLIENT_ID   = os.getenv("ANGEL_CLIENT_ID", "P134100")
PIN         = os.getenv("ANGEL_PIN", "7343")
TOTP_SECRET = os.getenv("ANGEL_TOTP_SECRET", "HRC7DYBVYPQ6WL7KU4V2RBP45U")
TG_TOKEN, TG_CHAT = os.getenv("TG_BOT_TOKEN", ""), os.getenv("TG_CHAT_ID", "")

SCRIP_URL = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
NIFTY_SPOT_TOKEN = "99926000"
STRIKE_STEP, N_STRIKES, CANDIDATE_RANGE = 50, 8, 1

INTERVAL_SEC, LOOKBACK_MIN, CONFIRM_COUNT = 60, 15, 2
MIN_VOLUME = 50_000                 # liquidity filter for the option we buy

# -- writer flow (base signal)
MIN_FLOW_OI  = 100_000              # min OI quantity (contracts x lot size) of flow in the signal direction
WRITER_RATIO = 1.5                  # bull flow / bear flow (or reverse) must be at least this

# -- smart-money confirmations
FUT_MIN_MOVE, FUT_OI_MIN_PCT = 8, 0.10     # futures: >= 8 pts move and >= 0.10% OI change over lookback
SWEEP_LOOKBACK = 12                         # 5-min candles used as the swing reference (~1 hour; shrinks to min 6 early in the day)
USE_CANDLE_API = False                      # Angel's getCandleData often returns false "rate limit" errors (known issue);
                                            # False = build 5-min candles from our own futures price samples instead
VOL_SPIKE_X = 2.0
MIN_EXTRA_SCORE = 2

# -- required filters
VWAP_HOLD = 3
MAX_VWAP_DIST_PCT = 0.30
MIN_ROOM_PTS = 60                   # distance to opposing OI wall needed (1:2 needs room to run)

# -- orders / risk (fixed 1:2)
SL_PCT, RR = 0.20, 2.0
SL_LIMIT_BUFFER_PCT = 0.03
ENTRY_BUFFER_PCT = 0.005
EXIT_BUFFER_PCT = 0.01
FILL_WAIT_SEC, MANAGE_SEC = 10, 2

LOTS, LOT_SIZE = 1, 65              # VERIFY current NIFTY lot size before running
DATA_START, TRADE_FROM = dt.time(9, 16), dt.time(9, 30)
NO_NEW_ENTRY_AFTER, END = dt.time(14, 45), dt.time(15, 15)
MAX_TRADES_PER_DAY, MAX_DAILY_LOSS, COOLDOWN_MIN = 3, 3000, 10   # INR

FUT_KEY = ("FUT", "FUT")

# ---------------- HELPERS ----------------
def notify(msg):
    print(f"[{dt.datetime.now():%H:%M:%S}] {msg}")
    if TG_TOKEN and TG_CHAT:
        try:
            requests.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                          data={"chat_id": TG_CHAT, "text": msg}, timeout=5)
        except Exception:
            pass

def tick(p):
    return max(0.05, round(round(p / 0.05) * 0.05, 2))

def with_retry(fn, *args, tries=3, **kw):
    """Retry READ-ONLY data calls on timeouts / rate-limit errors. Never use this for placeOrder."""
    for i in range(tries):
        try:
            return fn(*args, **kw)
        except Exception as e:
            if i == tries - 1:
                raise
            time.sleep((4 if "rate" in str(e).lower() else 2) * (i + 1))

def login():
    s = SmartConnect(api_key=API_KEY)
    r = s.generateSession(CLIENT_ID, PIN, pyotp.TOTP(TOTP_SECRET).now())
    if not r.get("status"):
        raise SystemExit(f"Login failed: {r}")
    return s

def fetch_scrip_master():
    """Download Angel's ~35 MB instrument file with retries and cache it per day, so a flaky connection
    or a restart doesn't break startup. Falls back to the newest cached copy if downloads keep failing."""
    import json, glob
    today_file = f"scrip_master_{dt.date.today():%Y%m%d}.json"
    if os.path.exists(today_file):
        try:
            with open(today_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            os.remove(today_file)                       # corrupt cache -> download again
    last_err = None
    for attempt in range(1, 6):
        try:
            tmp = today_file + ".part"
            with requests.get(SCRIP_URL, stream=True, timeout=(10, 60)) as r:
                r.raise_for_status()
                with open(tmp, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1 << 16):
                        if chunk: f.write(chunk)
            with open(tmp, "r", encoding="utf-8") as f:
                data = json.load(f)                     # fails if the file is incomplete
            os.replace(tmp, today_file)
            return data
        except Exception as e:
            last_err = e
            notify(f"Instrument file download failed (attempt {attempt}/5): {str(e)[:100]}")
            time.sleep(3 * attempt)
    for old in sorted(glob.glob("scrip_master_*.json"), reverse=True):
        try:
            with open(old, "r", encoding="utf-8") as f:
                data = json.load(f)
            notify(f"Using older cached instrument file {old} (may miss the newest contracts).")
            return data
        except Exception:
            continue
    raise SystemExit(f"Could not download the instrument file: {last_err}")

def load_instruments():
    data = fetch_scrip_master()
    today = dt.date.today()
    exp = lambda d: dt.datetime.strptime(d["expiry"], "%d%b%Y").date()
    nifty = [d for d in data if d["exch_seg"] == "NFO" and d["name"] == "NIFTY"]
    opts = [d for d in nifty if d["instrumenttype"] == "OPTIDX" and exp(d) >= today]
    futs = [d for d in nifty if d["instrumenttype"] == "FUTIDX" and exp(d) >= today]
    opt_exp = min(exp(d) for d in opts)
    fut = min(futs, key=exp)
    chain = {}
    for d in opts:
        if exp(d) == opt_exp:
            chain[(int(float(d["strike"]) / 100), d["symbol"][-2:])] = (d["token"], d["symbol"])
    notify(f"Option expiry {opt_exp} | futures {fut['symbol']} | contracts {len(chain)}")
    return chain, (fut["token"], fut["symbol"])

def get_spot(s):
    return float(with_retry(s.ltpData, "NSE", "Nifty 50", NIFTY_SPOT_TOKEN)["data"]["ltp"])

def snapshot(s, chain, fut, atm):
    tok_map = {fut[0]: FUT_KEY}
    for i in range(-N_STRIKES, N_STRIKES + 1):
        k = atm + i * STRIKE_STEP
        for t in ("CE", "PE"):
            if (k, t) in chain:
                tok_map[chain[(k, t)][0]] = (k, t)
    r = with_retry(s.getMarketData, "FULL", {"NFO": list(tok_map)})
    out = {}
    for q in r["data"]["fetched"]:
        out[tok_map[q["symbolToken"]]] = dict(
            ltp=float(q["ltp"]), oi=float(q.get("opnInterest", 0)),
            vol=float(q.get("tradeVolume", 0)), avg=float(q.get("avgPrice", 0)),
            sym=q["tradingSymbol"])
    return out

def get_candles(s, fut_token):
    """Completed 5-min futures candles: [time, o, h, l, c, v]. Includes previous days for swing references."""
    now = dt.datetime.now()
    r = s.getCandleData({"exchange": "NFO", "symboltoken": fut_token, "interval": "FIVE_MINUTE",
                         "fromdate": (now - dt.timedelta(days=3)).replace(hour=9, minute=15).strftime("%Y-%m-%d %H:%M"),
                         "todate": now.strftime("%Y-%m-%d %H:%M")})
    data = (r or {}).get("data") or []
    return [[c[0]] + [float(x) for x in c[1:]] for c in data[:-1]]    # drop the in-progress candle

_candle_cache = {"ts": 0.0, "data": []}

def get_candles_cached(s, fut_token):
    """A new 5-min candle only completes every 5 minutes, so refresh at most that often.
    On failure (e.g. rate limit) keep the old candles and retry in ~60 seconds."""
    if _candle_cache["data"] and time.time() - _candle_cache["ts"] < 300:
        return _candle_cache["data"]
    try:
        data = get_candles(s, fut_token)
        _candle_cache.update(ts=time.time(), data=data)
    except Exception as e:
        _candle_cache["ts"] = time.time() - 240
        print("candle fetch failed (using previous candles):", str(e)[:120])
    return _candle_cache["data"]

def _floor5(d):
    return d.replace(minute=d.minute - d.minute % 5, second=0, microsecond=0)

def build_candles(ticks):
    """Completed 5-min candles built from our own futures price samples (one per snapshot): [label, o, h, l, c, 0].
    Coarser than exchange candles (wicks between samples are missed), but needs no extra API calls."""
    buckets = {}
    for ts, px in ticks:
        buckets.setdefault(_floor5(dt.datetime.fromtimestamp(ts)), []).append(px)
    current = _floor5(dt.datetime.now())
    return [[b.isoformat(), p[0], max(p), min(p), p[-1], 0.0]
            for b, p in sorted(buckets.items()) if b < current]

# ---------------- SMART-MONEY ANALYTICS ----------------
def writer_flow(now, past, atm):
    """Bull flow = put writing (<= ATM) + call unwinding (>= ATM). Bear flow = call writing (>= ATM) + put unwinding (<= ATM)."""
    bull = bear = 0.0
    for (k, t), v in now.items():
        if t not in ("CE", "PE") or (k, t) not in past:
            continue
        d_oi = v["oi"] - past[(k, t)]["oi"]
        d_px = v["ltp"] - past[(k, t)]["ltp"]
        if t == "PE" and k <= atm:
            if d_oi > 0 and d_px <= 0: bull += d_oi        # put writing
            elif d_oi < 0:             bear += -d_oi       # put unwinding (support removed)
        elif t == "CE" and k >= atm:
            if d_oi > 0 and d_px <= 0: bear += d_oi        # call writing
            elif d_oi < 0:             bull += -d_oi       # call unwinding (resistance removed)
    return bull, bear

def fut_buildup(now_f, past_f):
    """+1 long build-up, -1 short build-up, 0 none."""
    if not past_f or past_f["oi"] <= 0: return 0
    dp = now_f["ltp"] - past_f["ltp"]
    d_oi_pct = (now_f["oi"] - past_f["oi"]) / past_f["oi"] * 100
    if d_oi_pct >= FUT_OI_MIN_PCT:
        if dp >= FUT_MIN_MOVE:  return 1
        if dp <= -FUT_MIN_MOVE: return -1
    return 0

def sweep_signal(candles):
    """+1 sell-side liquidity swept then reclaimed (bullish), -1 buy-side swept then rejected (bearish)."""
    n = min(SWEEP_LOOKBACK, len(candles) - 3)      # reference candles before the last 3
    if n < 6: return 0
    ref, recent = candles[-(n + 3):-3], candles[-3:]
    swing_low, swing_high = min(c[3] for c in ref), max(c[2] for c in ref)
    last_close = recent[-1][4]
    if any(c[3] < swing_low for c in recent) and last_close > swing_low: return 1
    if any(c[2] > swing_high for c in recent) and last_close < swing_high: return -1
    return 0

def oi_walls(snap, atm):
    ce = {k: v["oi"] for (k, t), v in snap.items() if t == "CE" and k >= atm}
    pe = {k: v["oi"] for (k, t), v in snap.items() if t == "PE" and k <= atm}
    res = max(ce, key=ce.get) if ce else None     # call wall = resistance
    sup = max(pe, key=pe.get) if pe else None     # put wall = support
    return sup, res

def pick_strike(now, atm, side):
    c = [(k, side) for k in range(atm - CANDIDATE_RANGE * STRIKE_STEP,
                                  atm + CANDIDATE_RANGE * STRIKE_STEP + 1, STRIKE_STEP)]
    c = [x for x in c if x in now and now[x]["vol"] >= MIN_VOLUME and now[x]["ltp"] > 0]
    return max(c, key=lambda x: now[x]["vol"]) if c else None

def volume_spike(history, key):
    vols = [h[1][key]["vol"] for h in history if key in h[1]]
    if len(vols) < 6: return False
    deltas = [b - a for a, b in zip(vols, vols[1:])]
    prev = deltas[:-1]
    avg = sum(prev) / len(prev)
    return avg > 0 and deltas[-1] >= VOL_SPIKE_X * avg

def vwap_state(fut_q):
    if not fut_q or fut_q["avg"] <= 0: return 0, 0.0
    dist = (fut_q["ltp"] - fut_q["avg"]) / fut_q["avg"] * 100
    return (1 if dist > 0 else -1), abs(dist)

def evaluate(snap, history, atm, spot, candles, vw_streak, vdist):
    """Returns None while warming up, else a dict describing the current setup."""
    age = history[-1][0] - history[0][0]
    if age < LOOKBACK_MIN * 60 - 30:
        return None
    past = history[0][1]
    bull_f, bear_f = writer_flow(snap, past, atm)
    bull_ratio = bull_f / bear_f if bear_f > 0 else (float("inf") if bull_f > 0 else 0)
    bear_ratio = bear_f / bull_f if bull_f > 0 else (float("inf") if bear_f > 0 else 0)
    direction = None
    if bull_f >= MIN_FLOW_OI and bull_ratio >= WRITER_RATIO: direction = "BULL"
    elif bear_f >= MIN_FLOW_OI and bear_ratio >= WRITER_RATIO: direction = "BEAR"
    sup, res = oi_walls(snap, atm)
    r = dict(dir=direction, ready=False, extra=0, key=None, bf=bull_f, sf=bear_f, sup=sup, res=res)
    if not direction:
        return r
    sign = 1 if direction == "BULL" else -1
    extra = 0
    if fut_buildup(snap.get(FUT_KEY), past.get(FUT_KEY)) == sign: extra += 2
    if sweep_signal(candles) == sign: extra += 2
    key = pick_strike(snap, atm, "CE" if sign == 1 else "PE")
    if key and volume_spike(history, key): extra += 1
    vwap_ok = (vw_streak >= VWAP_HOLD) if sign == 1 else (vw_streak <= -VWAP_HOLD)
    room_ok = (res is None or res - spot >= MIN_ROOM_PTS) if sign == 1 else (sup is None or spot - sup >= MIN_ROOM_PTS)
    opt_ok = bool(key) and snap[key]["avg"] > 0 and snap[key]["ltp"] >= snap[key]["avg"]
    r.update(extra=extra, key=key,
             ready=(vwap_ok and vdist <= MAX_VWAP_DIST_PCT and room_ok and opt_ok and extra >= MIN_EXTRA_SCORE))
    return r

# ---------------- ORDERS ----------------
def place(s, sym, token, side, qty, price, otype="LIMIT", trigger=None):
    p = dict(variety="NORMAL", tradingsymbol=sym, symboltoken=token, transactiontype=side,
             exchange="NFO", ordertype=otype, producttype="INTRADAY", duration="DAY",
             price=str(price), squareoff="0", stoploss="0", quantity=str(qty))
    if trigger: p["triggerprice"] = str(trigger)
    oid = s.placeOrder(p)
    if not oid: raise RuntimeError(f"placeOrder returned nothing for {side} {sym}")
    return oid

def order_info(s, oid):
    for o in (s.orderBook().get("data") or []):
        if o.get("orderid") == oid:
            return o
    return None

def wait_terminal(s, oid, secs):
    end = time.time() + secs
    o = None
    while time.time() < end:
        o = order_info(s, oid)
        if o and o["status"].lower() in ("complete", "rejected", "cancelled"):
            return o
        time.sleep(1)
    return order_info(s, oid)

def cancel(s, oid):
    try: s.cancelOrder(oid, "NORMAL")
    except Exception as e: notify(f"cancel error {oid}: {e}")

def open_nifty_position(s):
    for p in (s.position().get("data") or []):
        if str(p.get("tradingsymbol", "")).startswith("NIFTY") and int(float(p.get("netqty", 0))) != 0:
            return p
    return None

def enter(s, chain, snap, key):
    token, sym = chain[key]
    qty = LOTS * LOT_SIZE
    limit = tick(snap[key]["ltp"] * (1 + ENTRY_BUFFER_PCT))
    oid = place(s, sym, token, "BUY", qty, limit)
    o = wait_terminal(s, oid, FILL_WAIT_SEC)
    if not o or o["status"].lower() != "complete":
        cancel(s, oid); time.sleep(1)
        o = order_info(s, oid)
    filled = int(float(o.get("filledshares", 0))) if o else 0
    if filled <= 0:
        notify(f"Entry not filled for {sym} (status {o and o['status']}). Skipped.")
        return None
    entry = float(o["averageprice"])
    sl = tick(entry * (1 - SL_PCT))
    risk = entry - sl
    target = tick(entry + RR * risk)
    sl_limit = tick(sl - entry * SL_LIMIT_BUFFER_PCT)
    sl_oid = place(s, sym, token, "SELL", filled, sl_limit, "STOPLOSS_LIMIT", trigger=sl)
    t = dict(key=key, sym=sym, token=token, qty=filled, entry=entry, sl=sl, target=target,
             sl_oid=sl_oid, time=dt.datetime.now().isoformat(timespec="seconds"))
    notify(f"ENTRY {sym} x{filled} @ {entry} | SL {sl} | TARGET {target} (1:{RR:g})")
    return t

def square_off(s, t, reason):
    sl_o = order_info(s, t["sl_oid"])
    if sl_o and sl_o["status"].lower() == "complete":
        return float(sl_o["averageprice"]), "SL"
    cancel(s, t["sl_oid"]); time.sleep(1)
    sl_o = order_info(s, t["sl_oid"])
    if sl_o and sl_o["status"].lower() == "complete":
        return float(sl_o["averageprice"]), "SL"
    ltp = float(s.ltpData("NFO", t["sym"], t["token"])["data"]["ltp"])
    oid = place(s, t["sym"], t["token"], "SELL", t["qty"], tick(ltp * (1 - EXIT_BUFFER_PCT)))
    o = wait_terminal(s, oid, FILL_WAIT_SEC)
    tries = 0
    while (not o or o["status"].lower() != "complete") and tries < 5:
        cancel(s, oid); time.sleep(1)
        ltp = float(s.ltpData("NFO", t["sym"], t["token"])["data"]["ltp"])
        oid = place(s, t["sym"], t["token"], "SELL", t["qty"], tick(ltp * (1 - EXIT_BUFFER_PCT * (tries + 2))))
        o = wait_terminal(s, oid, FILL_WAIT_SEC); tries += 1
    if not o or o["status"].lower() != "complete":
        notify(f"!!! COULD NOT EXIT {t['sym']} - CLOSE MANUALLY NOW !!!")
        raise SystemExit("Manual intervention required")
    return float(o["averageprice"]), reason

# ---------------- STORAGE ----------------
def init_db():
    db = sqlite3.connect("nifty_chain.db")
    db.execute("CREATE TABLE IF NOT EXISTS snap(ts TEXT, spot REAL, strike TEXT, type TEXT, ltp REAL, oi REAL, vol REAL, avg REAL)")
    db.execute("CREATE TABLE IF NOT EXISTS trades(ts TEXT, sym TEXT, qty INT, entry REAL, sl REAL, target REAL, exit REAL, pnl REAL, reason TEXT)")
    return db

def save_snap(db, ts, spot, snap):
    db.executemany("INSERT INTO snap VALUES (?,?,?,?,?,?,?,?)",
                   [(ts, spot, str(k), t, v["ltp"], v["oi"], v["vol"], v["avg"]) for (k, t), v in snap.items()])
    db.commit()

# ---------------- MAIN ----------------
def main():
    s = login()
    def _logout():
        try: s.terminateSession(CLIENT_ID)
        except Exception: pass
    atexit.register(_logout)

    if open_nifty_position(s):
        raise SystemExit("A NIFTY position is already open. Close it manually, then restart the bot.")
    chain, fut = load_instruments()
    db = init_db()
    history = deque(maxlen=LOOKBACK_MIN * 60 // INTERVAL_SEC + 1)     # (timestamp, snapshot)
    fut_ticks = []                                                    # (timestamp, futures LTP) per snapshot
    streak, last_dir, vw_streak, last_snap = 0, None, 0, 0.0
    trade, trades_today, day_pnl, cooldown_until = None, 0, 0.0, dt.datetime.min
    notify("Smart-money bot started (LIVE).")

    def close_trade(reason):
        nonlocal trade, day_pnl, cooldown_until
        px, why = square_off(s, trade, reason)
        pnl = (px - trade["entry"]) * trade["qty"]
        day_pnl += pnl
        db.execute("INSERT INTO trades VALUES (?,?,?,?,?,?,?,?,?)",
                   (trade["time"], trade["sym"], trade["qty"], trade["entry"], trade["sl"],
                    trade["target"], px, pnl, why))
        db.commit()
        notify(f"EXIT {trade['sym']} @ {px} ({why}) | PnL {pnl:.0f} | Day {day_pnl:.0f}")
        if pnl < 0:
            cooldown_until = dt.datetime.now() + dt.timedelta(minutes=COOLDOWN_MIN)
        trade = None

    while True:
        now = dt.datetime.now(); now_t = now.time()
        try:
            if os.path.exists("STOP"):
                if trade: close_trade("KILL")
                notify("STOP file found. Exiting."); break
            if now_t >= END:
                if trade: close_trade("TIME")
                notify("Trading window over."); break
            if now_t < DATA_START:
                time.sleep(10); continue

            # ---- manage open position ----
            if trade:
                sl_o = order_info(s, trade["sl_oid"])
                if sl_o and sl_o["status"].lower() == "complete":
                    close_trade("SL")
                else:
                    ltp = float(s.ltpData("NFO", trade["sym"], trade["token"])["data"]["ltp"])
                    if ltp >= trade["target"]:
                        close_trade("TARGET")
                    elif sl_o and sl_o["status"].lower() in ("rejected", "cancelled"):
                        notify("SL order missing/rejected - exiting.")
                        close_trade("SL_ORDER_FAIL")
            if not trade and day_pnl <= -MAX_DAILY_LOSS:
                notify("Max daily loss hit. Stopping for the day."); break

            # ---- market data keeps updating every INTERVAL_SEC (also while in a trade) ----
            if time.time() - last_snap >= INTERVAL_SEC:
                last_snap = time.time()
                spot = get_spot(s)
                atm = round(spot / STRIKE_STEP) * STRIKE_STEP
                snap = snapshot(s, chain, fut, atm)
                save_snap(db, now.isoformat(timespec="seconds"), spot, snap)
                history.append((time.time(), snap))
                if FUT_KEY in snap:
                    fut_ticks.append((time.time(), snap[FUT_KEY]["ltp"]))

                vstate, vdist = vwap_state(snap.get(FUT_KEY))
                vw_streak = vw_streak + vstate if (vstate and vw_streak * vstate >= 0) else vstate

                if not trade and now_t >= TRADE_FROM:
                    candles = (get_candles_cached(s, fut[0]) if USE_CANDLE_API else []) or build_candles(fut_ticks)
                    r = evaluate(snap, history, atm, spot, candles, vw_streak, vdist)
                    if r is None:
                        print(f"{now:%H:%M:%S} warming up ({len(history)} snapshots)")
                    else:
                        if r["ready"] and r["dir"] == last_dir: streak += 1
                        elif r["ready"]: streak = 1
                        else: streak = 0
                        last_dir = r["dir"] if r["ready"] else None
                        print(f"{now:%H:%M:%S} spot {spot:.1f} | flow bull {r['bf']:,.0f} bear {r['sf']:,.0f} -> {r['dir']} "
                              f"| extra {r['extra']} | VWAP {vw_streak:+d} dist {vdist:.2f}% | sup {r['sup']} res {r['res']} "
                              f"| ready {r['ready']} x{streak}")
                        can_trade = (trades_today < MAX_TRADES_PER_DAY and day_pnl > -MAX_DAILY_LOSS
                                     and now >= cooldown_until and now_t < NO_NEW_ENTRY_AFTER)
                        if streak >= CONFIRM_COUNT and can_trade and r["key"]:
                            trade = enter(s, chain, snap, r["key"])
                            if trade:
                                trades_today += 1; streak = 0
        except SystemExit:
            raise
        except Exception as e:
            notify(f"Error: {e}")
        time.sleep(MANAGE_SEC)

if __name__ == "__main__":
    main()
