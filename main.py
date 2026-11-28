"""FVG 1.0 - quality-filtered multi-timeframe BTCUSDT signal engine with V6.7-compatible Binance price fallback.

Paper-trading by default. Live execution is intentionally not implemented in this
release; do not enable real orders until a separately tested Binance USD-M Futures
execution/risk module has been added.

Dependencies: pip install -r requirements.txt
Environment:
  TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID (optional)
  SYMBOL=BTCUSDT
  MIN_SCORE=60
  TRADE_SIZE_USDT=5 (legacy alias for MAX_NOTIONAL_USDT)
  MAX_NOTIONAL_USDT=5
  MIN_NET_RR_AFTER_FEES=1.0
  PAPER_BALANCE=100
  PAPER_BALANCE_FILE=/tmp/fvg1_paper_balance.json
  SCAN_INTERVAL=1
  COOLDOWN_MINUTES=2
  PRICE_STALE_SECONDS=5
  TRADE_ENABLED=false (true is rejected intentionally in this build)
"""

import json
import logging
import math
import os
import threading
import time
import queue
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple

import requests
from flask import Flask, jsonify

# ----------------------------- Configuration -----------------------------
app = Flask(__name__)
SYMBOL = os.getenv("SYMBOL", "BTCUSDT").upper()
MIN_SCORE = int(os.getenv("MIN_SCORE", "60"))
SCAN_INTERVAL = max(1, int(os.getenv("SCAN_INTERVAL", "1")))
LOG_INTERVAL_SECONDS = max(0.5, float(os.getenv("LOG_INTERVAL_SECONDS", "0.5")))
MAX_TRADES_PER_DAY = int(os.getenv("MAX_TRADES_PER_DAY", "15"))
COOLDOWN_MINUTES = int(os.getenv("COOLDOWN_MINUTES", "2"))
RISK_PERCENT = float(os.getenv("RISK_PERCENT", "0.1"))  # percent of paper equity
RR_TARGET = float(os.getenv("RR_TARGET", "2.0"))
# Honour the older Render variable if MAX_NOTIONAL_USDT is not explicitly set.
# This prevents TRADE_SIZE_USDT=5 from being silently ignored.
MAX_NOTIONAL_USDT = float(os.getenv("MAX_NOTIONAL_USDT", os.getenv("TRADE_SIZE_USDT", "5")))
FVG_MAX_AGE_CANDLES = int(os.getenv("FVG_MAX_AGE_CANDLES", "80"))
FVG_MIN_DISPLACEMENT_ATR = float(os.getenv("FVG_MIN_DISPLACEMENT_ATR", "0.8"))
FVG_STOP_ATR_BUFFER = float(os.getenv("FVG_STOP_ATR_BUFFER", "0.25"))
FVG_ENTRY_TOLERANCE_ATR = float(os.getenv("FVG_ENTRY_TOLERANCE_ATR", "0.15"))
PAPER_BALANCE_FILE = os.getenv("PAPER_BALANCE_FILE", "/tmp/fvg1_paper_balance.json")
PAPER_START_BALANCE = float(os.getenv("PAPER_BALANCE", "100"))
TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT = os.getenv("TELEGRAM_CHAT_ID", "")
TRADE_ENABLED = os.getenv("TRADE_ENABLED", "false").lower() == "true"
PAPER_FEE_RATE = float(os.getenv("PAPER_FEE_RATE", "0.001"))  # estimated fee per side
MIN_NET_RR_AFTER_FEES = float(os.getenv("MIN_NET_RR_AFTER_FEES", "1.0"))
PRICE_STALE_SECONDS = max(2, int(os.getenv("PRICE_STALE_SECONDS", "5")))
REST_CONNECT_TIMEOUT = max(0.5, float(os.getenv("REST_CONNECT_TIMEOUT", "1.5")))
REST_READ_TIMEOUT = max(1.0, float(os.getenv("REST_READ_TIMEOUT", "2.5")))
# Signal freshness windows: lower-timeframe confirmation may arrive after structure forms.
CHOCH_VALID_30S_CANDLES = max(1, int(os.getenv("CHOCH_VALID_30S_CANDLES", "5")))
STRUCTURE_VALID_5M_CANDLES = max(1, int(os.getenv("STRUCTURE_VALID_5M_CANDLES", "2")))

if TRADE_ENABLED:
    raise RuntimeError(
        "KCOMS V7 release is paper-only. Live trading is blocked until a tested "
        "Binance USD-M Futures execution module is installed. Set TRADE_ENABLED=false."
    )
if not (1 <= MIN_SCORE <= 100):
    raise ValueError("MIN_SCORE must be between 1 and 100")
if not (0 < RISK_PERCENT <= 2):
    raise ValueError("RISK_PERCENT must be >0 and <=2")
if MAX_NOTIONAL_USDT <= 0:
    raise ValueError("MAX_NOTIONAL_USDT / TRADE_SIZE_USDT must be positive")
if not (0 <= PAPER_FEE_RATE < 0.02):
    raise ValueError("PAPER_FEE_RATE must be between 0 and 0.02")
if MIN_NET_RR_AFTER_FEES < 0:
    raise ValueError("MIN_NET_RR_AFTER_FEES cannot be negative")

# Binance Spot public market-data only. No Futures endpoints are used for V7 paper analysis.
# data-api.binance.vision is the public market-data host; official Spot API hosts are fallbacks.
SPOT_REST_ENDPOINTS = [
    # V6.7 successfully used this public market-data host for /api/v3/klines.
    "https://data-api.binance.vision",
    "https://api.binance.com",
    "https://api1.binance.com",
    "https://api2.binance.com",
    "https://api3.binance.com",
    "https://api-gcp.binance.com",
]
PRICE_WS_URLS = [
    f"wss://data-stream.binance.vision/ws/{SYMBOL.lower()}@trade",
    f"wss://stream.binance.com:9443/ws/{SYMBOL.lower()}@trade",
]
EAT = timezone(timedelta(hours=3), "EAT")  # East Africa Time (UTC+3)

def current_clock() -> dict:
    now = datetime.now(EAT)
    return {"date": now.strftime("%Y-%m-%d"), "time": now.strftime("%H:%M:%S"),
            "timezone": "EAT", "timestamp": now.isoformat()}

price_lock = threading.RLock()
price_feed = {
    "price": None, "received_at": 0.0, "source": "WAITING_FOR_BINANCE_SPOT",
    "exchange_event_ms": None, "exchange_trade_ms": None,
    "latency_ms": None, "ws_connected": False, "last_ws_message_at": 0.0,
}
telegram_queue = queue.Queue(maxsize=500)
telegram_worker_started = False
rest_endpoint_retry_after = {}
ws_thread = None
ws_stop = threading.Event()

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Mozilla/5.0 (compatible; FVG-1.0/1.0)"})

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("FVG-1.0")

# ----------------------------- Runtime state -----------------------------
lock = threading.RLock()
latest = {
    "bot": "FVG 1.0", "status": "BOOTING", "symbol": SYMBOL,
    **current_clock(),
    "price": 0.0, "ema": 0.0, "price_source": "NOT_CONNECTED", "price_timestamp": None,
    "price_age_seconds": None, "price_latency_ms": None, "price_feed_status": "CONNECTING",
    "bias": "NEUTRAL", "signal": "NONE", "score": 0,
    "wins": 0, "losses": 0, "completed_trades": 0, "trades_today": 0,
    "balance": PAPER_START_BALANCE, "last_signal": "Starting",
    "market_regime": "UNKNOWN", "entry_status": "WAITING", "rejection_reasons": [],
    "side_diagnostics": {}, "fvg_stage_diagnostics": {}, "long_score": 0, "short_score": 0,
    "entry_trigger": "NONE", "fvg_zones": {}, "fvg_diagnostics": {}, "last_error": "",
}
state = {
    "balance": PAPER_START_BALANCE,
    "wins": 0,
    "losses": 0,
    "trades_today": 0,
    "day": datetime.now(EAT).date().isoformat(),
    "cooldown_until": 0.0,
    "position": None,
    "last_processed_1m": None,
    "last_alert_key": None,
    "last_trade_key": None,
    "last_status_log": 0.0,
    "last_diagnostic_key": None,
    "rejection_counts": {},
    "recent_rejections": [],
    "trade_history": [],
}

# Cached candles avoid requesting five full histories every second.
candle_cache: Dict[str, Dict[str, object]] = {}
CACHE_TTL = {"1m": 2, "5m": 15, "15m": 30, "30m": 45, "1h": 60}

# ----------------------------- Notifications -----------------------------
def _telegram_sender_worker() -> None:
    """Deliver queued Telegram text alerts off the market-scanning thread."""
    while True:
        message = telegram_queue.get()
        try:
            if TG_TOKEN and TG_CHAT:
                response = SESSION.post(
                    f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                    json={"chat_id": TG_CHAT, "text": message},
                    timeout=(2, 4),
                )
                response.raise_for_status()
        except requests.RequestException as exc:
            log.warning("Telegram delivery failed: %s", exc)
        finally:
            telegram_queue.task_done()


def start_telegram_worker_once() -> None:
    global telegram_worker_started
    if telegram_worker_started:
        return
    telegram_worker_started = True
    threading.Thread(target=_telegram_sender_worker, daemon=True, name="telegram-outbox").start()


def send_tg(message: str) -> None:
    """Queue Telegram alerts so network delays never block price scanning."""
    if not TG_TOKEN or not TG_CHAT:
        return
    start_telegram_worker_once()
    try:
        telegram_queue.put_nowait(message)
    except queue.Full:
        log.error("Telegram outbox full; dropping oldest pending message")
        try:
            telegram_queue.get_nowait()
            telegram_queue.task_done()
            telegram_queue.put_nowait(message)
        except queue.Empty:
            pass


# ----------------------------- Market data -----------------------------
def _record_price(value: float, source: str) -> None:
    if not math.isfinite(value) or value <= 0:
        raise ValueError("Invalid Binance Spot price")
    received = time.time()
    with price_lock:
        price_feed.update({"price": value, "received_at": received, "source": source})
        if source != "BINANCE_SPOT_WS":
            # A REST snapshot has no exchange event timestamp; don't reuse old WS latency.
            price_feed["exchange_event_ms"] = None
            price_feed["exchange_trade_ms"] = None
            price_feed["latency_ms"] = None
        age_ms = None
        if price_feed.get("exchange_event_ms") is not None:
            age_ms = max(0.0, received * 1000.0 - float(price_feed["exchange_event_ms"]))
        price_feed["latency_ms"] = round(age_ms, 1) if age_ms is not None else None
    latest["price_source"] = source
    latest["price_timestamp"] = datetime.now(timezone.utc).isoformat()
    latest["price_age_seconds"] = 0.0
    latest["price_latency_ms"] = price_feed.get("latency_ms")
    latest["last_error"] = ""


def _spot_get(path: str, params: dict) -> Optional[object]:
    failures = []
    now = time.time()
    for base in SPOT_REST_ENDPOINTS:
        if rest_endpoint_retry_after.get(base, 0.0) > now:
            continue
        try:
            response = SESSION.get(
                base + path, params=params,
                timeout=(REST_CONNECT_TIMEOUT, REST_READ_TIMEOUT),
            )
            response.raise_for_status()
            payload = response.json()
            if isinstance(payload, dict) and payload.get("code") not in (None, 0):
                raise ValueError(f"Binance API {payload.get('code')}: {payload.get('msg')}")
            rest_endpoint_retry_after.pop(base, None)
            return payload
        except (requests.RequestException, ValueError) as exc:
            failures.append(f"{base}: {exc}")
            # Temporarily skip a failing host rather than paying its timeout on every scan.
            rest_endpoint_retry_after[base] = time.time() + 30
    latest["last_error"] = " | ".join(failures[-3:]) or "No Binance Spot REST endpoint responded"
    return None


def _websocket_price_worker() -> None:
    """Reconnect to Binance public Spot trade stream; REST remains the fallback."""
    try:
        import websocket
    except ImportError:
        log.warning("websocket-client is not installed; Binance Spot REST price polling will be used")
        return
    attempt = 0
    while not ws_stop.is_set():
        url = PRICE_WS_URLS[attempt % len(PRICE_WS_URLS)]
        try:
            def on_open(_ws):
                with price_lock:
                    price_feed["ws_connected"] = True
                latest["price_feed_status"] = "WEBSOCKET_CONNECTED"
                log.info("Binance Spot WebSocket connected: %s", url)

            def on_message(_ws, message):
                try:
                    payload = json.loads(message)
                    price = float(payload["p"])
                    event_ms = payload.get("E")
                    trade_ms = payload.get("T")
                    with price_lock:
                        price_feed["exchange_event_ms"] = int(event_ms) if event_ms else None
                        price_feed["exchange_trade_ms"] = int(trade_ms) if trade_ms else None
                        price_feed["last_ws_message_at"] = time.time()
                    _record_price(price, "BINANCE_SPOT_WS")
                except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                    log.warning("Ignored malformed Binance Spot WebSocket message")

            def on_error(_ws, error):
                latest["price_feed_status"] = "WEBSOCKET_ERROR"
                log.warning("Binance Spot WebSocket error: %s", error)

            def on_close(_ws, _code, _message):
                with price_lock:
                    price_feed["ws_connected"] = False
                latest["price_feed_status"] = "WEBSOCKET_RECONNECTING"
                log.warning("Binance Spot WebSocket disconnected")

            app_ws = websocket.WebSocketApp(
                url, on_open=on_open, on_message=on_message,
                on_error=on_error, on_close=on_close,
            )
            app_ws.run_forever(ping_interval=15, ping_timeout=8)
        except Exception as exc:
            log.warning("Binance Spot WebSocket connection failed: %s", exc)
        attempt += 1
        ws_stop.wait(min(30, 2 ** min(attempt, 5)))


def start_price_websocket_once() -> None:
    global ws_thread
    if ws_thread is not None and ws_thread.is_alive():
        return
    ws_thread = threading.Thread(target=_websocket_price_worker, daemon=True, name="binance-spot-ws-v79")
    ws_thread.start()


def _price_from_kline(interval: str) -> Optional[float]:
    """Read the live close of Binance's currently-forming Spot candle.

    This deliberately mirrors the V6.7 method (reading kline[4]) that the
    operator confirmed worked on Render. It is a live-updating candle close,
    not the last completed candle close.
    """
    raw = _spot_get("/api/v3/klines", {"symbol": SYMBOL, "interval": interval, "limit": 2})
    if not isinstance(raw, list) or not raw:
        return None
    try:
        value = float(raw[-1][4])
        if math.isfinite(value) and value > 0:
            return value
    except (IndexError, TypeError, ValueError):
        return None
    return None


def get_price() -> Optional[float]:
    """Get fresh Binance Spot BTCUSDT price, preserving V6.7's working route.

    Priority: fresh Binance trade WebSocket, Spot ticker REST, then the
    V6.7-compatible live kline-close fallback (1m, then 15m).
    """
    with price_lock:
        cached_price = price_feed["price"]
        age = time.time() - float(price_feed["received_at"])
        source = str(price_feed["source"])
    if cached_price is not None and age <= PRICE_STALE_SECONDS:
        latest["price_source"] = source
        latest["price_age_seconds"] = round(age, 2)
        with price_lock:
            latest["price_latency_ms"] = price_feed.get("latency_ms")
            latest["price_feed_status"] = (
                "WEBSOCKET_CONNECTED" if price_feed.get("ws_connected")
                else "REST_OR_CACHED"
            )
        return float(cached_price)

    # First try the direct last-traded-price endpoint.
    data = _spot_get("/api/v3/ticker/price", {"symbol": SYMBOL})
    try:
        value = float(data["price"]) if isinstance(data, dict) else None
        if value is not None and math.isfinite(value) and value > 0:
            _record_price(value, "BINANCE_SPOT_REST_TICKER")
            return value
    except (KeyError, TypeError, ValueError):
        pass

    # Important: V6.7 fetched its working price from the latest kline close.
    # Keep this exact data path as the primary fallback instead of depending
    # solely on /ticker/price, which may be blocked while /klines still works.
    for interval, source_name in (("1m", "BINANCE_SPOT_1M_KLINE"),
                                  ("15m", "BINANCE_SPOT_15M_KLINE_FALLBACK")):
        value = _price_from_kline(interval)
        if value is not None:
            _record_price(value, source_name)
            return value

    latest["price_source"] = "UNAVAILABLE"
    latest["price_feed_status"] = "STALE_OR_DISCONNECTED"
    with price_lock:
        latest["price_age_seconds"] = (round(time.time() - float(price_feed["received_at"]), 2)
                                        if price_feed["received_at"] else None)
        latest["price_latency_ms"] = price_feed.get("latency_ms")
    return None


def get_klines(interval: str, limit: int = 250) -> Optional[List[dict]]:
    now = time.time()
    cached = candle_cache.get(interval)
    if cached and now - float(cached["time"]) < CACHE_TTL[interval]:
        return cached["data"]  # type: ignore[return-value]
    raw = _spot_get("/api/v3/klines", {"symbol": SYMBOL, "interval": interval, "limit": limit})
    if not isinstance(raw, list) or len(raw) < 30:
        return None
    rows = []
    previous_open = -1
    for k in raw:
        try:
            row = {"open_time": int(k[0]), "o": float(k[1]), "h": float(k[2]),
                   "l": float(k[3]), "c": float(k[4]), "v": float(k[5]),
                   "close_time": int(k[6])}
            if (row["open_time"] <= previous_open or
                not all(math.isfinite(row[x]) for x in ("o", "h", "l", "c", "v")) or
                min(row["o"], row["h"], row["l"], row["c"]) <= 0 or row["v"] < 0 or
                row["h"] < max(row["o"], row["c"], row["l"]) or
                row["l"] > min(row["o"], row["c"], row["h"])):
                continue
            rows.append(row)
            previous_open = row["open_time"]
        except (IndexError, TypeError, ValueError):
            continue
    if len(rows) < 30:
        return None
    candle_cache[interval] = {"time": now, "data": rows}
    return rows

def closed_candles(rows: List[dict]) -> List[dict]:
    """Exclude the still-forming final candle using exchange close timestamp."""
    now_ms = int(time.time() * 1000)
    return [c for c in rows if c["close_time"] < now_ms]

# ----------------------------- Indicators -----------------------------
def ema(values: List[float], period: int) -> Optional[float]:
    if len(values) < period:
        return None
    alpha = 2.0 / (period + 1.0)
    result = sum(values[:period]) / period
    for value in values[period:]:
        result = alpha * value + (1.0 - alpha) * result
    return result


def atr(candles: List[dict], period: int = 14) -> Optional[float]:
    if len(candles) < period + 1:
        return None
    tr = []
    for i in range(1, len(candles)):
        cur, prev = candles[i], candles[i - 1]
        tr.append(max(cur["h"] - cur["l"], abs(cur["h"] - prev["c"]), abs(cur["l"] - prev["c"])))
    return sum(tr[-period:]) / period


def rsi(values: List[float], period: int = 14) -> Optional[float]:
    if len(values) < period + 1:
        return None
    changes = [values[i] - values[i - 1] for i in range(1, len(values))]
    gains = [max(x, 0.0) for x in changes[-period:]]
    losses = [max(-x, 0.0) for x in changes[-period:]]
    avg_gain, avg_loss = sum(gains) / period, sum(losses) / period
    if avg_loss == 0:
        return 100.0
    return 100.0 - (100.0 / (1.0 + avg_gain / avg_loss))


def trend_bias(candles: List[dict]) -> str:
    closes = [c["c"] for c in candles]
    e20, e50, e200 = ema(closes, 20), ema(closes, 50), ema(closes, 200)
    if None in (e20, e50, e200):
        return "NEUTRAL"
    last = closes[-1]
    if last > e200 and e20 > e50 > e200:
        return "BULLISH"
    if last < e200 and e20 < e50 < e200:
        return "BEARISH"
    return "NEUTRAL"

# ----------------------------- Signal analysis -----------------------------

def net_reward_risk_after_fees(direction: str, entry: float, stop: float,
                               target: float, fee_rate: float) -> float:
    """Estimated net reward/risk after entry and exit fees."""
    if direction == "LONG":
        reward, loss = target - entry, entry - stop
    else:
        reward, loss = entry - target, stop - entry
    net_reward = reward - fee_rate * (entry + target)
    net_loss = loss + fee_rate * (entry + stop)
    return net_reward / net_loss if net_reward > 0 and net_loss > 0 else 0.0


def detect_fvgs(candles: List[dict], timeframe: str) -> List[dict]:
    """Detect three-candle FVGs; require a meaningful middle-candle displacement."""
    found = []
    if len(candles) < 20:
        return found
    atr_value = atr(candles, 14)
    if not atr_value or atr_value <= 0:
        return found
    first = max(2, len(candles) - FVG_MAX_AGE_CANDLES)
    for i in range(first, len(candles)):
        a, b, c = candles[i - 2], candles[i - 1], candles[i]
        displacement = abs(b["c"] - b["o"]) / atr_value
        if displacement < FVG_MIN_DISPLACEMENT_ATR:
            continue
        if c["l"] > a["h"] and b["c"] > b["o"]:
            low, high = a["h"], c["l"]
            found.append({
                "direction": "LONG", "timeframe": timeframe, "low": low, "high": high,
                "ce": (low + high) / 2.0, "formed_at": c["open_time"],
                "formed_index": i, "displacement_atr": round(displacement, 2),
            })
        if c["h"] < a["l"] and b["c"] < b["o"]:
            low, high = c["h"], a["l"]
            found.append({
                "direction": "SHORT", "timeframe": timeframe, "low": low, "high": high,
                "ce": (low + high) / 2.0, "formed_at": c["open_time"],
                "formed_index": i, "displacement_atr": round(displacement, 2),
            })
    return found


def fvg_is_invalidated(fvg: dict, candles: List[dict]) -> bool:
    """An FVG is invalidated when a later candle closes beyond its far edge."""
    later = candles[fvg["formed_index"] + 1:]
    if fvg["direction"] == "LONG":
        return any(c["c"] < fvg["low"] for c in later)
    return any(c["c"] > fvg["high"] for c in later)


def fvg_is_fully_mitigated(fvg: dict, candles: List[dict]) -> bool:
    """Reject zones already fully traversed after formation."""
    later = candles[fvg["formed_index"] + 1:]
    if fvg["direction"] == "LONG":
        return any(c["l"] <= fvg["low"] for c in later)
    return any(c["h"] >= fvg["high"] for c in later)


def recent_liquidity_sweep(candles: List[dict], direction: str,
                           lookback: int = 12) -> bool:
    """Recent sweep/reclaim used as FVG confluence, not as a standalone entry."""
    if len(candles) < lookback + 2:
        return False
    for idx in range(max(lookback, len(candles) - 3), len(candles)):
        prior = candles[idx - lookback:idx]
        candle = candles[idx]
        if direction == "LONG":
            level = min(x["l"] for x in prior)
            if candle["l"] < level and candle["c"] > level:
                return True
        else:
            level = max(x["h"] for x in prior)
            if candle["h"] > level and candle["c"] < level:
                return True
    return False


def structure_break(candles: List[dict], direction: str, lookback: int = 10) -> bool:
    """Recent completed 5m BOS used only as FVG quality confluence."""
    if len(candles) < lookback + 2:
        return False
    prior, last = candles[-lookback-1:-1], candles[-1]
    return (last["c"] > max(x["h"] for x in prior) if direction == "LONG"
            else last["c"] < min(x["l"] for x in prior))


def analyse_market(current_price: Optional[float] = None) -> Optional[dict]:
    """FVG-only analysis with stage-by-stage diagnostics.

    IMPORTANT: diagnostics are observational only. The existing entry gates,
    thresholds, scoring, stop/target calculations and paper execution rules
    below are intentionally unchanged.
    """
    raw = {tf: get_klines(tf, 250) for tf in ("5m", "15m", "1h")}
    if any(v is None for v in raw.values()):
        return None
    candles = {tf: closed_candles(v) for tf, v in raw.items()}
    if any(len(v) < 30 for v in candles.values()):
        return None
    c5, c15, c1h = candles["5m"], candles["15m"], candles["1h"]
    price = current_price if current_price is not None else get_price()
    if price is None or price <= 0:
        return None
    atr5, atr15 = atr(c5, 14), atr(c15, 14)
    if not atr5 or not atr15:
        return None

    # EMA is retained solely as a legacy dashboard reference, never as a filter.
    e200 = ema([x["c"] for x in c15], 200)
    if e200 is not None:
        latest["ema"] = round(e200, 2)

    zones = {
        "1H": detect_fvgs(c1h, "1H"),
        "15M": detect_fvgs(c15, "15M"),
        "5M": detect_fvgs(c5, "5M"),
    }
    candidates = []
    diagnostics = {"LONG": [], "SHORT": []}
    stage_diagnostics = {}

    def bump(counts: dict, key: str) -> None:
        counts[key] = int(counts.get(key, 0)) + 1

    for side in ("LONG", "SHORT"):
        counts = {
            "raw_5m_fvgs": 0, "valid_5m_fvgs": 0,
            "raw_1h_fvgs": 0, "valid_1h_fvgs": 0,
            "invalidated_1h_fvgs": 0, "mitigated_1h_fvgs": 0,
            "raw_15m_fvgs": 0, "valid_15m_fvgs": 0,
            "invalidated_15m_fvgs": 0, "mitigated_15m_fvgs": 0,
            "aligned_5m_fvgs": 0, "candle_touched": 0,
            "near_ce": 0, "sweep_or_bos": 0,
            "risk_valid": 0, "net_rr_pass": 0, "final_candidates": 0,
        }
        rejected = {}
        zone_details = []
        htf_zone_details = []
        htf = []

        for timeframe, series in (("1H", c1h), ("15M", c15)):
            directional = [z for z in zones[timeframe] if z["direction"] == side]
            counts[f"raw_{timeframe.lower()}_fvgs"] = len(directional)
            for z in directional:
                invalidated = fvg_is_invalidated(z, series)
                mitigated = fvg_is_fully_mitigated(z, series)
                htf_zone_details.append({
                    "timeframe": timeframe, "direction": side,
                    "low": round(z["low"], 2), "high": round(z["high"], 2),
                    "ce": round(z["ce"], 2),
                    "displacement_atr": z["displacement_atr"],
                    "formed_at": z["formed_at"],
                    "invalidated": invalidated,
                    "fully_mitigated": mitigated,
                    "valid": not invalidated and not mitigated,
                })
                if invalidated:
                    bump(counts, f"invalidated_{timeframe.lower()}_fvgs")
                if mitigated:
                    bump(counts, f"mitigated_{timeframe.lower()}_fvgs")
                if not invalidated and not mitigated:
                    htf.append(z)
                    bump(counts, f"valid_{timeframe.lower()}_fvgs")

        raw_exec_zones = [z for z in zones["5M"] if z["direction"] == side]
        counts["raw_5m_fvgs"] = len(raw_exec_zones)
        for zone in raw_exec_zones:
            detail = {
                "timeframe": "5M", "direction": side,
                "low": round(zone["low"], 2), "high": round(zone["high"], 2),
                "ce": round(zone["ce"], 2),
                "displacement_atr": zone["displacement_atr"],
                "formed_at": zone["formed_at"],
                "age_5m": len(c5) - 1 - zone["formed_index"],
            }
            invalidated = fvg_is_invalidated(zone, c5)
            mitigated = fvg_is_fully_mitigated(zone, c5)
            detail["invalidated"] = invalidated
            detail["fully_mitigated"] = mitigated
            if invalidated or mitigated:
                why = "FVG invalidated" if invalidated else "FVG fully mitigated"
                detail["rejection"] = why
                bump(rejected, why)
                zone_details.append(detail)
                continue

            bump(counts, "valid_5m_fvgs")
            aligned = [
                h for h in htf
                if (max(h["low"], zone["low"]) <= min(h["high"], zone["high"])
                    or abs(h["ce"] - zone["ce"]) <= 0.75 * atr15)
            ]
            detail["aligned_htf_count"] = len(aligned)
            if not aligned:
                why = "No aligned valid 1H/15M FVG"
                detail["rejection"] = why
                bump(rejected, why)
                zone_details.append(detail)
                continue

            bump(counts, "aligned_5m_fvgs")
            higher = min(aligned, key=lambda h: abs(h["ce"] - zone["ce"]))
            detail["higher_timeframe"] = higher["timeframe"]
            tolerance = FVG_ENTRY_TOLERANCE_ATR * atr5
            touched = (c5[-1]["l"] <= zone["high"] + tolerance
                       and c5[-1]["h"] >= zone["low"] - tolerance)
            near_ce_distance = abs(price - zone["ce"])
            near_ce_limit = max(tolerance, 0.20 * atr5)
            near_ce = near_ce_distance <= near_ce_limit
            detail.update({
                "candle_touched": touched,
                "near_ce": near_ce,
                "live_price": round(price, 2),
                "ce_distance": round(near_ce_distance, 2),
                "ce_distance_atr": round(near_ce_distance / atr5, 3),
                "ce_tolerance": round(near_ce_limit, 2),
            })
            if touched:
                bump(counts, "candle_touched")
            if near_ce:
                bump(counts, "near_ce")
            if not touched:
                why = "Latest completed 5M candle did not touch FVG zone"
                detail["rejection"] = why
                bump(rejected, why)
                zone_details.append(detail)
                continue
            if not near_ce:
                why = "Live price is outside 5M FVG CE tolerance"
                detail["rejection"] = why
                bump(rejected, why)
                zone_details.append(detail)
                continue

            sweep = recent_liquidity_sweep(c5, side)
            bos = structure_break(c5, side)
            detail["liquidity_sweep"] = sweep
            detail["bos"] = bos
            if sweep or bos:
                bump(counts, "sweep_or_bos")
            else:
                why = "No 5M liquidity sweep or BOS confluence"
                detail["rejection"] = why
                bump(rejected, why)
                zone_details.append(detail)
                continue

            entry = zone["ce"]
            if side == "LONG":
                stop = min(zone["low"], higher["low"]) - FVG_STOP_ATR_BUFFER * atr5
                risk = entry - stop
                target = entry + RR_TARGET * risk
                if price > entry + tolerance:
                    why = "LONG price moved above permitted CE entry tolerance"
                    detail["rejection"] = why
                    bump(rejected, why)
                    zone_details.append(detail)
                    continue
            else:
                stop = max(zone["high"], higher["high"]) + FVG_STOP_ATR_BUFFER * atr5
                risk = stop - entry
                target = entry - RR_TARGET * risk
                if price < entry - tolerance:
                    why = "SHORT price moved below permitted CE entry tolerance"
                    detail["rejection"] = why
                    bump(rejected, why)
                    zone_details.append(detail)
                    continue
            if risk <= 0:
                why = "Calculated stop risk is not positive"
                detail["rejection"] = why
                bump(rejected, why)
                zone_details.append(detail)
                continue
            bump(counts, "risk_valid")

            net_rr = net_reward_risk_after_fees(side, entry, stop, target, PAPER_FEE_RATE)
            detail["net_rr_after_fees"] = round(net_rr, 4)
            if net_rr < MIN_NET_RR_AFTER_FEES:
                why = "Net reward/risk after fees below minimum"
                detail["rejection"] = why
                bump(rejected, why)
                zone_details.append(detail)
                continue
            bump(counts, "net_rr_pass")

            age = len(c5) - 1 - zone["formed_index"]
            score = min(100, 60 + min(15, int(zone["displacement_atr"] * 5))
                        + (10 if sweep else 0) + (10 if bos else 0)
                        + (5 if higher["timeframe"] == "1H" else 0))
            reasons = [
                f"{zone['timeframe']} {side} FVG retracement at 50% CE",
                f"Aligned {higher['timeframe']} {side} FVG",
                f"Displacement {zone['displacement_atr']:.2f} ATR",
                "Liquidity sweep" if sweep else "BOS structure break",
                "5M execution zone active",
            ]
            candidates.append({
                "direction": side, "score": score, "entry": entry, "price": price,
                "stop": stop, "target": target, "risk_distance": risk,
                "net_rr_after_fees": net_rr, "zone": zone, "higher_zone": higher,
                "sweep": sweep, "bos": bos, "age_5m": age, "reasons": reasons,
            })
            bump(counts, "final_candidates")
            detail.update({"rejection": None, "score": score,
                           "entry": round(entry, 2), "stop": round(stop, 2),
                           "target": round(target, 2)})
            diagnostics[side].append({
                "timeframe": zone["timeframe"], "low": zone["low"],
                "high": zone["high"], "ce": zone["ce"],
                "higher_timeframe": higher["timeframe"],
                "sweep": sweep, "bos": bos, "score": score,
            })
            zone_details.append(detail)

        stage_diagnostics[side] = {
            "counts": counts,
            "rejection_counts": rejected,
            "higher_timeframe_zones": htf_zone_details[-40:],
            "execution_zones": zone_details[-30:],
        }

    latest["fvg_zones"] = {tf: len(items) for tf, items in zones.items()}
    latest["fvg_diagnostics"] = diagnostics
    latest["fvg_stage_diagnostics"] = stage_diagnostics

    all_rejections = {}
    for side_data in stage_diagnostics.values():
        for reason, count in side_data["rejection_counts"].items():
            all_rejections[reason] = all_rejections.get(reason, 0) + count
    if not candidates:
        if all_rejections:
            top_reasons = sorted(all_rejections.items(), key=lambda item: item[1], reverse=True)[:5]
            rejection_reasons = [f"{reason} ({count} zone(s))" for reason, count in top_reasons]
        else:
            rejection_reasons = ["No directional 5M FVGs detected with required displacement"]
        return {
            "price": price, "direction": "NONE", "score": 0,
            "long_score": 0, "short_score": 0, "reasons": [],
            "rejection_reasons": rejection_reasons,
            "side_diagnostics": diagnostics, "fvg_stage_diagnostics": stage_diagnostics,
            "entry_trigger": "FVG_5M_CE",
            "bias_30m": "FVG_CONTEXT", "bias_1h": "NEUTRAL",
            "bias_15m": "NEUTRAL", "regime": "FVG", "atr_1m": atr5,
            "confirmed": False, "candle_key": c5[-1]["open_time"],
            "chart_candles": c5[-70:],
        }

    selected = max(candidates, key=lambda x: x["score"])
    return {
        "price": price, "direction": selected["direction"], "score": selected["score"],
        "long_score": max((x["score"] for x in candidates if x["direction"] == "LONG"), default=0),
        "short_score": max((x["score"] for x in candidates if x["direction"] == "SHORT"), default=0),
        "reasons": selected["reasons"], "rejection_reasons": [],
        "side_diagnostics": diagnostics, "fvg_stage_diagnostics": stage_diagnostics,
        "entry_trigger": "FVG_5M_CE",
        "entry": selected["entry"], "stop": selected["stop"],
        "target": selected["target"], "risk_distance": selected["risk_distance"],
        "net_rr_after_fees": selected["net_rr_after_fees"],
        "confirmed": selected["score"] >= MIN_SCORE,
        "bias_30m": "FVG_CONTEXT", "bias_1h": selected["higher_zone"]["direction"],
        "bias_15m": selected["higher_zone"]["direction"], "regime": "FVG",
        "atr_1m": atr5, "fvg_zone": selected["zone"],
        "higher_fvg_zone": selected["higher_zone"],
        "candle_key": c5[-1]["open_time"], "chart_candles": c5[-70:],
    }


# ----------------------------- Paper trading -----------------------------
def load_state() -> None:
    try:
        with open(PAPER_BALANCE_FILE, "r", encoding="utf-8") as f:
            saved = json.load(f)
        state["balance"] = max(0.0, float(saved.get("balance", PAPER_START_BALANCE)))
        state["wins"] = max(0, int(saved.get("wins", 0)))
        state["losses"] = max(0, int(saved.get("losses", 0)))
        state["trades_today"] = max(0, int(saved.get("trades_today", 0)))
        state["day"] = str(saved.get("day", state["day"]))
        state["cooldown_until"] = float(saved.get("cooldown_until", 0.0))
        state["position"] = saved.get("position")
        state["last_trade_key"] = saved.get("last_trade_key")
        state["last_alert_key"] = saved.get("last_alert_key")
        state["trade_history"] = saved.get("trade_history", [])[-100:]
        if state["position"] is not None:
            required = {"side", "entry", "stop", "target", "qty", "notional"}
            if not required.issubset(state["position"]) or state["position"]["side"] not in ("LONG", "SHORT"):
                log.warning("Discarding invalid saved paper position")
                state["position"] = None
        reset_daily_counter()
    except FileNotFoundError:
        pass
    except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
        log.warning("Could not restore paper state; starting from configured balance: %s", exc)
        state.update({"balance": PAPER_START_BALANCE, "wins": 0, "losses": 0,
                      "trades_today": 0, "position": None, "cooldown_until": 0.0})


def save_state() -> None:
    tmp = PAPER_BALANCE_FILE + ".tmp"
    payload = {key: state[key] for key in ("balance", "wins", "losses", "trades_today", "day",
               "cooldown_until", "position", "last_trade_key", "last_alert_key", "trade_history")}
    try:
        os.makedirs(os.path.dirname(PAPER_BALANCE_FILE) or ".", exist_ok=True)
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, allow_nan=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, PAPER_BALANCE_FILE)
    except (OSError, TypeError, ValueError) as exc:
        log.warning("Could not persist paper account: %s", exc)


def reset_daily_counter() -> None:
    today = datetime.now(EAT).date().isoformat()
    if today != state["day"]:
        state["day"] = today
        state["trades_today"] = 0
        save_state()


def open_paper_position(signal: dict) -> None:
    equity = float(state["balance"])
    risk_cash = equity * (RISK_PERCENT / 100.0)
    distance = float(signal["risk_distance"])
    if distance <= 0 or equity <= 0:
        return
    entry_price = float(signal.get("entry", signal["price"]))
    qty = risk_cash / distance
    qty = min(qty, MAX_NOTIONAL_USDT / entry_price)
    if qty <= 0:
        return
    notional = qty * entry_price
    state["position"] = {
        "side": signal["direction"], "entry": entry_price,
        "stop": float(signal["stop"]), "target": float(signal["target"]),
        "qty": qty, "notional": notional, "opened_at": time.time(),
        "score": int(signal["score"]), "reasons": signal["reasons"],
        "net_rr_after_fees": float(signal.get("net_rr_after_fees", 0.0)),
    }
    state["trades_today"] += 1
    save_state()
    msg = (
        f"🟢 FVG 1.0 PAPER TRADE ENTERED — {signal['direction']} {SYMBOL}\nDate: {current_clock()['date']} EAT\n"
        f"Entry: {float(signal.get('entry', signal['price'])):.2f}\nSL: {signal['stop']:.2f}\n"
        f"TP: {signal['target']:.2f}\nScore: {signal['score']}/100\n"
        f"Risk: ${risk_cash:.2f} | Notional: ${notional:.2f}\n"
        f"Evidence: {', '.join(signal['reasons'])}"
    )
    log.info(msg.replace("\n", " | "))
    send_tg(msg)
    latest["last_signal"] = msg


def monitor_paper_position(price: float) -> None:
    pos = state.get("position")
    if not pos:
        return
    side = pos["side"]
    stop_hit = price <= pos["stop"] if side == "LONG" else price >= pos["stop"]
    target_hit = price >= pos["target"] if side == "LONG" else price <= pos["target"]
    if not (stop_hit or target_hit):
        return
    # If one sampled price crosses both levels (gap), assume the adverse stop fills first.
    result = "LOSS" if stop_hit else "WIN"
    exit_price = float(pos["stop"] if stop_hit else pos["target"])
    direction = 1 if side == "LONG" else -1
    gross = (exit_price - pos["entry"]) * float(pos["qty"]) * direction
    # Estimated taker fee on both entry and exit notionals; actual rates vary by account/tier.
    exit_notional = float(pos["qty"]) * exit_price
    fees = (float(pos["notional"]) + exit_notional) * PAPER_FEE_RATE
    pnl = gross - fees
    result = "WIN" if pnl > 0 else "LOSS"
    state["balance"] += pnl
    if result == "WIN":
        state["wins"] += 1
    else:
        state["losses"] += 1
        state["cooldown_until"] = time.time() + COOLDOWN_MINUTES * 60
    state.setdefault("trade_history", []).append({
        "closed_at": current_clock()["timestamp"],
        "side": side,
        "entry": float(pos["entry"]),
        "exit": exit_price,
        "stop": float(pos["stop"]),
        "target": float(pos["target"]),
        "gross_pnl": round(gross, 6),
        "fees": round(fees, 6),
        "net_pnl": round(pnl, 6),
        "result": result,
        "score": int(pos.get("score", 0)),
        "net_rr_at_entry": float(pos.get("net_rr_after_fees", 0.0)),
        "duration_seconds": round(max(0.0, time.time() - float(pos.get("opened_at", time.time()))), 1),
    })
    state["trade_history"] = state["trade_history"][-100:]
    msg = (
        f"{'✅' if pnl > 0 else '❌'} FVG 1.0 PAPER {result} — {side} {SYMBOL}\n"
        f"Exit reason: {'TAKE PROFIT' if target_hit else 'STOP LOSS'}\nDate: {current_clock()['date']} EAT\n"
        f"Entry: {float(pos['entry']):.2f} | Exit: {exit_price:.2f}\nNet P/L: ${pnl:.4f} (estimated fees included)\n"
        f"Paper balance: ${state['balance']:.2f}\n"
        f"W:{state['wins']} L:{state['losses']}"
    )
    log.info(msg.replace("\n", " | "))
    send_tg(msg)
    latest["last_signal"] = msg
    state["position"] = None
    save_state()
    latest.update({"wins": state["wins"], "losses": state["losses"],
                   "completed_trades": state["wins"] + state["losses"],
                   "balance": round(state["balance"], 2)})

# ----------------------------- Console monitoring -----------------------------
def log_live_status(price: Optional[float], force: bool = False, refresh_ema: bool = True) -> None:
    """Print the compact V6.7-style status line requested for Render logs."""
    now = time.time()
    if not force and now - float(state.get("last_status_log", 0.0)) < LOG_INTERVAL_SECONDS:
        return
    state["last_status_log"] = now
    latest.update(current_clock())

    # Use the 200-period EMA from completed Binance Spot 15m candles.
    ema_value = float(latest.get("ema") or 0.0)
    if ema_value <= 0 and refresh_ema:
        candles = get_klines("15m", 250)
        if candles:
            closed = closed_candles(candles)
            calculated = ema([c["c"] for c in closed], 200)
            if calculated is not None:
                ema_value = calculated
                latest["ema"] = round(calculated, 2)

    if price is not None and math.isfinite(float(price)) and float(price) > 0:
        price_text = f"{float(price):.2f}"
    else:
        price_text = "UNAVAILABLE"

    position = state.get("position")
    cooldown_remaining = max(0, int(float(state.get("cooldown_until", 0.0)) - now))
    if position:
        status = f"IN_TRADE_{position['side']}"
        trade_detail = (f"ENTRY {float(position['entry']):.2f} | "
                        f"SL {float(position['stop']):.2f} | TP {float(position['target']):.2f}")
    elif cooldown_remaining > 0:
        status = "COOLING"
        trade_detail = f"COOLDOWN {cooldown_remaining}s remaining"
    else:
        status = latest.get("status", "SCANNING")
        if status in ("IN_POSITION", "SCANNING", "BOOTING") and price is not None:
            status = "SCANNING"
        trade_detail = ""
    balance = float(state.get("balance", PAPER_START_BALANCE))
    wins = int(state.get("wins", 0))
    losses = int(state.get("losses", 0))
    trades = int(state.get("trades_today", 0))
    print(
        f"[{latest['date']} {latest['time']} EAT] Price {price_text} | "
        f"EMA {ema_value:.2f} | {status} | Bal ${balance:.2f} | "
        f"W:{wins} L:{losses} D:{trades}/{MAX_TRADES_PER_DAY}"
        f"{' | ' + trade_detail if trade_detail else ''}",
        flush=True,
    )


# ----------------------------- Price display heartbeat -----------------------------
_price_heartbeat_thread = None
_price_heartbeat_start_lock = threading.Lock()
_price_heartbeat_stop = threading.Event()


def _price_display_heartbeat() -> None:
    """Refresh the Render price display every 0.5s from cached WebSocket data.

    This thread never calls Binance REST and does not run market analysis or
    alter trade decisions. It only prints the most recently received price.
    """
    while not _price_heartbeat_stop.wait(0.5):
        try:
            with price_lock:
                price = price_feed.get("price")
            if price is None:
                price = latest.get("price")
            log_live_status(price, refresh_ema=False)
        except Exception:
            log.exception("Price display heartbeat error")


def start_price_display_heartbeat_once() -> None:
    global _price_heartbeat_thread
    with _price_heartbeat_start_lock:
        if _price_heartbeat_thread is not None and _price_heartbeat_thread.is_alive():
            return
        _price_heartbeat_stop.clear()
        _price_heartbeat_thread = threading.Thread(
            target=_price_display_heartbeat,
            daemon=True,
            name="fvg1-price-display-500ms",
        )
        _price_heartbeat_thread.start()
        log.info("Price display heartbeat started: 0.5s (cached WebSocket price; no extra REST polling)")


# ----------------------------- Main loop -----------------------------
def run_bot() -> None:
    load_state()
    start_telegram_worker_once()
    if not TG_TOKEN or not TG_CHAT:
        log.warning("Telegram alerts disabled: TELEGRAM_BOT_TOKEN and/or TELEGRAM_CHAT_ID missing")
    start_price_websocket_once()
    start_price_display_heartbeat_once()
    latest.update({"status": "SCANNING", "balance": round(state["balance"], 2)})
    log.info("FVG 1.0 AUTO PAPER MODE started: %s; LONG+SHORT; minimum score %s; risk %.3f%%; gross RR 1:%.2f; max notional $%.2f; fee/side %.3f%%; minimum net RR %.2f; daily trade limit %s; cooldown %sm; FVG max age %s candles; Telegram trade-event alerts only",
             SYMBOL, MIN_SCORE, RISK_PERCENT, RR_TARGET, MAX_NOTIONAL_USDT,
             PAPER_FEE_RATE * 100, MIN_NET_RR_AFTER_FEES,
             MAX_TRADES_PER_DAY, COOLDOWN_MINUTES, FVG_MAX_AGE_CANDLES)
    log_live_status(None, force=True)

    while True:
        try:
            with lock:
                reset_daily_counter()
                price = get_price()
                if price is None:
                    latest["status"] = "WAITING_FOR_PRICE"
                    latest["last_error"] = latest.get("last_error") or "Binance Spot price unavailable"
                    log.warning("BINANCE SPOT PRICE UNAVAILABLE | REST and WebSocket feed unavailable | %s", latest["last_error"])
                    log_live_status(None)
                    time.sleep(SCAN_INTERVAL)
                    continue
                monitor_paper_position(price)
                analysis = analyse_market(price)
                if analysis is None:
                    latest.update({
                        "status": "IN_TRADE" if state["position"] else "WAITING_FOR_DATA",
                        "price": round(price, 2), "wins": state["wins"],
                        "losses": state["losses"],
                        "completed_trades": state["wins"] + state["losses"],
                        "trades_today": state["trades_today"],
                        "balance": round(state["balance"], 2),
                    })
                    log_live_status(price)
                    time.sleep(SCAN_INTERVAL)
                    continue

                # Record one diagnostic sample per completed 5m candle (not every scan).
                if state.get("last_diagnostic_key") != analysis["candle_key"]:
                    state["last_diagnostic_key"] = analysis["candle_key"]
                    if not analysis["confirmed"]:
                        for reason in analysis["rejection_reasons"]:
                            counts = state.setdefault("rejection_counts", {})
                            counts[reason] = int(counts.get(reason, 0)) + 1
                    recent = state.setdefault("recent_rejections", [])
                    recent.append({
                        "time": current_clock()["timestamp"],
                        "direction": analysis["direction"],
                        "score": analysis["score"],
                        "confirmed": analysis["confirmed"],
                        "reasons": analysis["rejection_reasons"],
                        "side_diagnostics": analysis.get("side_diagnostics", {}),
                    "fvg_stage_diagnostics": analysis.get("fvg_stage_diagnostics", {}),
                    })
                    state["recent_rejections"] = recent[-20:]

                latest.update({
                    "status": "IN_POSITION" if state["position"] else "SCANNING",
                    "price": round(price, 2), "bias": analysis["bias_30m"],
                    "signal": analysis["direction"], "score": analysis["score"],
                    "market_regime": analysis["regime"],
                    "entry_status": "CONFIRMED" if analysis["confirmed"] else "REJECTED",
                    "rejection_reasons": analysis["rejection_reasons"],
                    "side_diagnostics": analysis.get("side_diagnostics", {}),
                    "fvg_stage_diagnostics": analysis.get("fvg_stage_diagnostics", {}),
                    "long_score": analysis["long_score"], "short_score": analysis["short_score"],
                    "entry_trigger": analysis["entry_trigger"],
                    "rejection_counts": dict(state.get("rejection_counts", {})),
                    "recent_rejections": list(state.get("recent_rejections", [])),
                    "wins": state["wins"], "losses": state["losses"],
                    "completed_trades": state["wins"] + state["losses"],
                    "trades_today": state["trades_today"],
                    "balance": round(state["balance"], 2), "last_error": "",
                })

                # Automatically open paper trades when the existing strategy confirms.
                can_enter = (
                    analysis["confirmed"] and state["position"] is None
                    and state["trades_today"] < MAX_TRADES_PER_DAY
                    and time.time() >= float(state["cooldown_until"])
                    and state["last_trade_key"] != analysis["candle_key"]
                )
                if can_enter:
                    state["last_trade_key"] = analysis["candle_key"]
                    open_paper_position(analysis)
                latest["status"] = "IN_POSITION" if state["position"] else "SCANNING"
                latest["wins"] = state["wins"]
                latest["losses"] = state["losses"]
                latest["trades_today"] = state["trades_today"]
                latest["balance"] = round(state["balance"], 2)
                log_live_status(price)
            time.sleep(SCAN_INTERVAL)
        except Exception as exc:
            log.exception("Main loop error")
            latest["last_error"] = str(exc)
            latest["status"] = "ERROR_RECOVERING"
            time.sleep(max(2, SCAN_INTERVAL))

# ----------------------------- Status endpoints -----------------------------
@app.route("/")
def home():
    with lock:
        latest.update(current_clock())
        data = dict(latest)
    return (
        "<h1>FVG 1.0</h1>"
        f"<p>Date: {data['date']} | Time: {data['time']} {data['timezone']}</p>"
        f"<p>{data['symbol']} | BINANCE SPOT DATA | PAPER ONLY | Minimum score {MIN_SCORE}/100</p>"
        f"<p>Status: {data['status']} | Price: {data['price']} | Source: {data.get('price_source', 'UNKNOWN')} | Bias: {data['bias']}</p>"
        f"<p>Feed: {data.get('price_feed_status', 'UNKNOWN')} | Price age: {data.get('price_age_seconds')}s | Exchange latency: {data.get('price_latency_ms')}ms</p>"
        f"<p>Signal: {data['signal']} ({data['score']}/100) | Regime: {data['market_regime']} | Entry: {data.get('entry_status', 'WAITING')} | Trigger: {data.get('entry_trigger', 'NONE')}</p>"
        f"<p>LONG score: {data.get('long_score', 0)} | SHORT score: {data.get('short_score', 0)}</p>"
        f"<p>Rejection reasons: {', '.join(data.get('rejection_reasons', [])) or 'None'}</p>"
        f"<p>Side diagnostics: {data.get('side_diagnostics', {})}</p>"
        f"<p>Rejection counts: {data.get('rejection_counts', {})}</p>"
        f"<p>Paper balance: ${data['balance']} | Completed: {data.get('completed_trades', data['wins'] + data['losses'])} "
        f"| Wins: {data['wins']} | Losses: {data['losses']} "
        f"| Today: {data['trades_today']}/{MAX_TRADES_PER_DAY}</p>"
        "<p>Execution: AUTOMATIC PAPER</p>"
        f"<p>Trade details: {('SIDE ' + str(state['position']['side']) + ' | ENTRY ' + format(float(state['position']['entry']), '.2f') + ' | SL ' + format(float(state['position']['stop']), '.2f') + ' | TP ' + format(float(state['position']['target']), '.2f')) if state['position'] else ('COOLING: ' + str(max(0, int(float(state['cooldown_until']) - time.time()))) + ' seconds remaining' if float(state['cooldown_until']) > time.time() else 'No open trade / not cooling')}</p>"
        f"<p>Last: {data['last_signal']}</p>"
    )


@app.route("/health")
def health():
    latest.update(current_clock())
    engine_alive = bool(_engine_thread and _engine_thread.is_alive())
    return jsonify({
        "ok": True, "bot": "FVG 1.0", "status": latest["status"],
        "engine_thread_alive": engine_alive, **current_clock()
    }), 200


@app.route("/status")
def status():
    with lock:
        latest.update(current_clock())
        completed = state["wins"] + state["losses"]
        return jsonify({**latest, "completed_trades": completed,
                        "win_rate_percent": round(100.0 * state["wins"] / completed, 2) if completed else 0.0,
                        "open_position": state["position"] is not None,
                        "position": state["position"],
                        "trades_today_limit": MAX_TRADES_PER_DAY,
                        "cooldown_active": time.time() < float(state["cooldown_until"]),
                        "cooldown_remaining_seconds": max(0, int(float(state["cooldown_until"]) - time.time())),
                        "trade_levels": ({"side": state["position"]["side"],
                                         "entry": state["position"]["entry"],
                                         "stop_loss": state["position"]["stop"],
                                         "take_profit": state["position"]["target"]}
                                        if state["position"] else None),
                        "paper_only": True, "minimum_score": MIN_SCORE, "strategy": "FVG_ONLY", "fvg_zones": latest.get("fvg_zones", {}), "fvg_diagnostics": latest.get("fvg_diagnostics", {}),
                        "fvg_stage_diagnostics": latest.get("fvg_stage_diagnostics", {}),
                        "diagnostic_candle_interval": "5m",
                        "max_notional_usdt": MAX_NOTIONAL_USDT,
                        "paper_fee_rate_per_side": PAPER_FEE_RATE,
                        "minimum_net_rr_after_fees": MIN_NET_RR_AFTER_FEES,
                        "recent_trade_history": list(state.get("trade_history", [])[-20:]),
                        "rejection_counts": dict(state.get("rejection_counts", {})),
                        "recent_rejections": list(state.get("recent_rejections", [])),
                        "side_diagnostics": latest.get("side_diagnostics", {})})


# ----------------------- Single-service Render Free mode -----------------------
# One Render Web Service hosts BOTH Flask and the trading engine.
# Use exactly ONE Gunicorn worker: multiple workers would duplicate the bot.
# FVG_ROLE=web is accepted for compatibility with the previous V8.1 service.
KCOMS_ROLE = os.getenv("FVG_ROLE", os.getenv("KCOMS_ROLE", "combined")).strip().lower()
_engine_start_lock = threading.Lock()
_engine_thread = None

def start_embedded_engine_once() -> None:
    """Start one scanner thread inside the single Gunicorn web worker."""
    global _engine_thread
    with _engine_start_lock:
        if _engine_thread is not None and _engine_thread.is_alive():
            return
        _engine_thread = threading.Thread(
            target=run_bot, daemon=True, name="fvg1-trading-engine"
        )
        _engine_thread.start()
        log.info("FVG 1.0 embedded trading engine thread started")

if KCOMS_ROLE in ("combined", "web"):
    # Gunicorn imports this module in its sole worker process. The engine and
    # Flask therefore share the same process and in-memory status.
    start_embedded_engine_once()
elif KCOMS_ROLE == "worker":
    # Retained only for manual/local use; do not create a Render Background Worker
    # for the free single-service deployment.
    if __name__ == "__main__":
        log.info("Starting FVG 1.0 standalone trading worker")
        run_bot()
else:
    raise RuntimeError("KCOMS_ROLE must be 'combined', 'web', or 'worker'")

if __name__ == "__main__" and KCOMS_ROLE in ("combined", "web"):
    # Local fallback. Render should use the Gunicorn Start Command in README.txt.
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port, threaded=True)
