import asyncio
import hmac
import json
import os
import time

import httpx
import websockets
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware


app = FastAPI(title="Klinger BTC Server")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

FAST, SLOW, SIG = 34, 55, 13
SECONDS_BEFORE_CLOSE = 30
MODE = "Cruzamento filtrado"
MAX_BODY_HIGH_PRECISION = 59.0
EXHAUSTION_BODY_MIN = 83.0
EXHAUSTION_BODY_MAX = 99.9
MAX_KVO_STRETCH = 51.7
USE_MODERATE_CROSS = True
MIN_POST_CROSS = 10.0
MAX_POST_CROSS = 14.0
REST_URL = "https://data-api.binance.vision/api/v3/klines"
WS_URL = "wss://data-stream.binance.vision/ws/btcusdt@kline_1m"

state = {
    "running": False,
    "mode": MODE,
    "indicator": "Klinger Fabio V4 - Alta Precisao",
    "last_signal": None,
    "last_webhook": None,
    "pending": None,
    "kvo": None,
    "signal": None,
    "price": None,
    "last_closed_bar": None,
    "last_error": None,
    "telegram_status": "checking",
    "profit_count": 0,
    "loss_count": 0,
    "total_count": 0,
    "accuracy": 0.0,
}

task = None
bars = []


def ema(values, length):
    """Equivalente ao ta.ema do Pine: inicia no primeiro valor nao-na."""
    out = []
    value = None
    alpha = 2 / (length + 1)
    for source in values:
        if source is None:
            out.append(None)
            continue
        value = source if value is None else alpha * source + (1 - alpha) * value
        out.append(value)
    return out


def klinger_full(candles):
    """Replica TradingView/ta/12 kvo(fastLen, slowLen, trigLen)."""
    if len(candles) < 2:
        return None
    trend_volume = [None]
    previous_hlc3 = (
        candles[0]["h"] + candles[0]["l"] + candles[0]["c"]
    ) / 3.0
    for candle in candles[1:]:
        hlc3 = (candle["h"] + candle["l"] + candle["c"]) / 3.0
        change = hlc3 - previous_hlc3
        direction = 1.0 if change > 0 else -1.0 if change < 0 else 0.0
        trend_volume.append(direction * candle["v"] * 100.0)
        previous_hlc3 = hlc3

    fast_ema = ema(trend_volume, FAST)
    slow_ema = ema(trend_volume, SLOW)
    kvo = [
        None if fast is None or slow is None else fast - slow
        for fast, slow in zip(fast_ema, slow_ema)
    ]
    return kvo, ema(kvo, SIG)


def evaluate_v4(candles):
    """Aplica literalmente os filtros padrao do Pine Klinger Fabio V4."""
    calculated = klinger_full(candles)
    if not calculated or len(candles) < 3:
        return None
    kvo, signal = calculated
    current = len(candles) - 1
    previous = current - 1
    values = (kvo[previous], signal[previous], kvo[current], signal[current])
    if any(value is None for value in values):
        return None

    candle = candles[current]
    candle_range = max(candle["h"] - candle["l"], 1e-12)
    body_percent = abs(candle["c"] - candle["o"]) / candle_range * 100.0
    buy_cross = kvo[previous] <= signal[previous] and kvo[current] > signal[current]
    sell_cross = kvo[previous] >= signal[previous] and kvo[current] < signal[current]
    buy_stretch_blocked = kvo[current] > MAX_KVO_STRETCH
    sell_stretch_blocked = kvo[current] < -MAX_KVO_STRETCH
    exhaustion_blocked = EXHAUSTION_BODY_MIN < body_percent <= EXHAUSTION_BODY_MAX
    high_precision_body = body_percent <= MAX_BODY_HIGH_PRECISION

    if MODE == "Alta precisao":
        buy_mode = high_precision_body and not buy_stretch_blocked
        sell_mode = high_precision_body and not sell_stretch_blocked
    elif MODE == "Equilibrado":
        buy_mode = not buy_stretch_blocked and not exhaustion_blocked
        sell_mode = not sell_stretch_blocked and not exhaustion_blocked
    else:
        buy_mode = not buy_stretch_blocked
        sell_mode = not sell_stretch_blocked

    buy_separation = kvo[current] - signal[current]
    sell_separation = signal[current] - kvo[current]
    buy_positive = not USE_MODERATE_CROSS or MIN_POST_CROSS <= buy_separation <= MAX_POST_CROSS
    sell_positive = not USE_MODERATE_CROSS or MIN_POST_CROSS <= sell_separation <= MAX_POST_CROSS
    buy = buy_cross and candle["c"] > candle["o"] and buy_mode and buy_positive
    sell = sell_cross and candle["c"] < candle["o"] and sell_mode and sell_positive
    return {
        "signal_type": "COMPRA" if buy else "VENDA" if sell else None,
        "kvo": kvo[current],
        "signal": signal[current],
        "body_percent": body_percent,
        "separation": buy_separation if buy else sell_separation if sell else abs(kvo[current] - signal[current]),
    }


def candle_color(candle):
    if candle["c"] > candle["o"]:
        return "green"
    if candle["c"] < candle["o"]:
        return "red"
    return "doji"


async def telegram(text):
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat:
        raise RuntimeError("Telegram variables are not configured")
    async with httpx.AsyncClient(timeout=10) as client:
        response = await client.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data={"chat_id": chat, "text": text},
        )
        response.raise_for_status()


async def telegram_safe(text):
    try:
        await telegram(text)
    except Exception as exc:
        state["last_error"] = f"Telegram: {type(exc).__name__}"


async def telegram_diagnostics():
    token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    chat = os.getenv("TELEGRAM_CHAT_ID", "")
    if not token or not chat:
        state["telegram_status"] = "missing_environment_variable"
        return

    async with httpx.AsyncClient(timeout=10) as client:
        token_check = await client.get(
            f"https://api.telegram.org/bot{token}/getMe"
        )
        if token_check.status_code != 200:
            state["telegram_status"] = "invalid_token"
            return

        chat_check = await client.get(
            f"https://api.telegram.org/bot{token}/getChat",
            params={"chat_id": chat},
        )
        if chat_check.status_code != 200:
            state["telegram_status"] = "invalid_chat_or_bot_not_started"
            return

    state["telegram_status"] = "ready"


async def seed():
    global bars
    now_ms = int(time.time() * 1000)
    async with httpx.AsyncClient(timeout=15) as client:
        response = await client.get(
            REST_URL,
            params={"symbol": "BTCUSDT", "interval": "1m", "limit": 1000},
        )
        response.raise_for_status()

    # A Binance inclui a vela atual no REST. Mantemos apenas velas já fechadas.
    bars = [
        {
            "t": int(row[0]),
            "o": float(row[1]),
            "h": float(row[2]),
            "l": float(row[3]),
            "c": float(row[4]),
            "v": float(row[5]),
        }
        for row in response.json()
        if int(row[6]) < now_ms
    ]


async def monitor():
    """Replica o Pine V4 nos updates da vela Binance BTCUSDT de 1 minuto."""
    global bars
    pending = None
    last_processed_bar = None
    active_bar = None
    latched_signal = None
    alerted_bar = None
    state["running"] = True

    while state["running"]:
        try:
            await seed()
            last_processed_bar = bars[-1]["t"] if bars else last_processed_bar
            state["last_error"] = None

            async with websockets.connect(
                WS_URL,
                ping_interval=20,
                ping_timeout=20,
            ) as websocket:
                while state["running"]:
                    raw = await asyncio.wait_for(websocket.recv(), timeout=45)
                    message = json.loads(raw)
                    kline = message["k"]
                    candle = {
                        "t": int(kline["t"]),
                        "o": float(kline["o"]),
                        "h": float(kline["h"]),
                        "l": float(kline["l"]),
                        "c": float(kline["c"]),
                        "v": float(kline["v"]),
                    }
                    state["price"] = candle["c"]
                    if active_bar != candle["t"]:
                        active_bar = candle["t"]
                        latched_signal = None

                    closes_at = int(kline["T"])
                    seconds_remaining = max(0.0, (closes_at - int(time.time() * 1000)) / 1000.0)
                    time_window = bool(kline["x"]) or seconds_remaining <= SECONDS_BEFORE_CLOSE
                    evaluation = evaluate_v4(bars + [candle])
                    if evaluation:
                        state["kvo"] = evaluation["kvo"]
                        state["signal"] = evaluation["signal"]

                    # Pine trava o sinal ate o inicio da proxima vela.
                    if time_window and evaluation and evaluation["signal_type"]:
                        latched_signal = latched_signal or evaluation["signal_type"]

                    if latched_signal and alerted_bar != candle["t"]:
                        alerted_bar = candle["t"]
                        event = {
                            "type": latched_signal,
                            "time": int(time.time()),
                            "price": candle["c"],
                            "bar": candle["t"],
                            "seconds_remaining": round(seconds_remaining, 1),
                            "kvo": evaluation["kvo"],
                            "signal": evaluation["signal"],
                            "body_percent": evaluation["body_percent"],
                            "separation": evaluation["separation"],
                        }
                        pending = {"type": latched_signal, "bar": candle["t"]}
                        state["last_signal"] = event
                        state["pending"] = pending
                        icon = "🟢" if latched_signal == "COMPRA" else "🔴"
                        await telegram_safe(
                            f"{icon} Klinger V4 — {latched_signal}\n"
                            f"BTCUSDT 1m | Preço: {candle['c']:.2f}\n"
                            f"Sinal detectado nos {SECONDS_BEFORE_CLOSE}s finais da vela."
                        )

                    if not bool(kline["x"]) or candle["t"] == last_processed_bar:
                        continue

                    last_processed_bar = candle["t"]
                    bars.append(candle)
                    bars = bars[-1000:]
                    state["last_closed_bar"] = candle["t"]

                    # A vela seguinte classifica LUCRO/LOS, como no Pine.
                    if pending and candle["t"] > pending["bar"]:
                        color = candle_color(candle)
                        profit = (
                            pending["type"] == "COMPRA" and color == "green"
                        ) or (
                            pending["type"] == "VENDA" and color == "red"
                        )
                        loss = color != "doji" and not profit
                        if profit:
                            state["profit_count"] += 1
                        elif loss:
                            state["loss_count"] += 1
                        state["total_count"] = state["profit_count"] + state["loss_count"]
                        state["accuracy"] = (
                            state["profit_count"] * 100.0 / state["total_count"]
                            if state["total_count"] else 0.0
                        )
                        pending = None
                        state["pending"] = None

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            state["last_error"] = str(exc)
            if state["running"]:
                await asyncio.sleep(5)


def ensure_monitor():
    global task
    state["running"] = True
    if task is None or task.done():
        task = asyncio.create_task(monitor())


@app.on_event("startup")
async def startup_event():
    # Inicia automaticamente após cada deploy ou despertar da instância.
    ensure_monitor()
    asyncio.create_task(telegram_diagnostics())


@app.get("/")
def root():
    return {
        "ok": True,
        "service": "Klinger BTC",
        "running": state["running"],
        "mode": state["mode"],
    }


@app.get("/status")
def status():
    return state


@app.post("/start")
async def start():
    state["last_error"] = None
    ensure_monitor()
    return state


@app.post("/stop")
async def stop():
    state["running"] = False
    return state


@app.post("/tradingview")
async def tradingview(request: Request):
    # Mantido para compatibilidade caso o plano do TradingView seja atualizado.
    expected = os.getenv("TRADINGVIEW_WEBHOOK_SECRET")
    if not expected:
        raise HTTPException(status_code=503, detail="Webhook secret not configured")
    try:
        payload = await request.json()
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid JSON") from exc
    supplied = str(payload.get("secret", ""))
    if not hmac.compare_digest(supplied, expected):
        raise HTTPException(status_code=401, detail="Invalid secret")
    signal_name = str(payload.get("signal", "")).strip().upper()
    if signal_name not in ("COMPRA", "VENDA"):
        raise HTTPException(status_code=400, detail="Invalid signal")
    ticker = str(payload.get("ticker", "BTCUSDT"))
    interval = str(payload.get("interval", "1"))
    price = payload.get("price")
    event_time = payload.get("time", int(time.time() * 1000))
    event = {
        "type": signal_name,
        "ticker": ticker,
        "interval": interval,
        "price": price,
        "time": event_time,
        "received_at": int(time.time()),
    }
    state["last_webhook"] = event
    state["last_signal"] = event
    asyncio.create_task(
        telegram_safe(
            f"🔔 TradingView — {signal_name}\n"
            f"Ativo: {ticker}\nTempo: {interval}\nPreço: {price}"
        )
    )
    return {"ok": True, "signal": signal_name}
