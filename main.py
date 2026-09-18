import asyncio
import hashlib
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
REST_URL = "https://data-api.binance.vision/api/v3/klines"
WS_URL = "wss://data-stream.binance.vision/ws/btcusdt@kline_1m"

state = {
    "running": False,
    "mode": "binance_direct",
    "last_signal": None,
    "last_webhook": None,
    "pending": None,
    "kvo": None,
    "signal": None,
    "price": None,
    "last_closed_bar": None,
    "last_error": None,
    "telegram_status": "checking",
    "telegram_test": "not_sent",
}

task = None
bars = []
telegram_test_used = False
TELEGRAM_TEST_TOKEN_HASH = "5b58b6730cfab0b19ba0155c7cee0c3d3493269284afbca0cb0ffa028e78c078"


def ema(values, length):
    out = [None] * len(values)
    if len(values) < length:
        return out
    value = sum(values[:length]) / length
    out[length - 1] = value
    alpha = 2 / (length + 1)
    for i in range(length, len(values)):
        value = alpha * values[i] + (1 - alpha) * value
        out[i] = value
    return out


def klinger_full(candles):
    """Klinger clássico usado pela regra 34/55/13."""
    if len(candles) < 100:
        return None

    volume_force = []
    previous_trend = 1
    previous_dm = 0.0
    previous_cm = 0.0
    previous_hlc = None

    for candle in candles:
        hlc = candle["h"] + candle["l"] + candle["c"]
        dm = candle["h"] - candle["l"]
        if previous_hlc is None:
            trend = 1
        elif hlc > previous_hlc:
            trend = 1
        elif hlc < previous_hlc:
            trend = -1
        else:
            trend = previous_trend

        cm = (
            previous_cm + dm
            if trend == previous_trend
            else previous_dm + dm
        )
        vf = (
            candle["v"] * abs(2 * ((dm / cm) - 1)) * trend * 100
            if cm
            else 0.0
        )
        volume_force.append(vf)
        previous_trend = trend
        previous_dm = dm
        previous_cm = cm
        previous_hlc = hlc

    fast_ema = ema(volume_force, FAST)
    slow_ema = ema(volume_force, SLOW)
    kvo = [None] * len(candles)
    for i in range(len(candles)):
        if fast_ema[i] is not None and slow_ema[i] is not None:
            kvo[i] = fast_ema[i] - slow_ema[i]

    first = next((i for i, value in enumerate(kvo) if value is not None), None)
    if first is None:
        return None

    signal_compact = ema([value for value in kvo[first:] if value is not None], SIG)
    signal = [None] * len(candles)
    for offset, value in enumerate(signal_compact):
        signal[first + offset] = value
    return kvo, signal


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
    """Monitora BTCUSDT continuamente enquanto a instância do Render estiver ativa."""
    global bars
    pending = None
    last_processed_bar = None
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

                    # A regra só é avaliada no fechamento da vela de 1 minuto.
                    if not bool(kline["x"]):
                        continue
                    if candle["t"] == last_processed_bar:
                        continue

                    last_processed_bar = candle["t"]
                    if bars and bars[-1]["t"] == candle["t"]:
                        bars[-1] = candle
                    else:
                        bars.append(candle)
                        bars = bars[-1000:]

                    state["last_closed_bar"] = candle["t"]
                    calculated = klinger_full(bars)
                    if not calculated:
                        continue

                    kvo, signal = calculated
                    current = len(bars) - 1
                    previous = current - 1
                    state["kvo"] = kvo[current]
                    state["signal"] = signal[current]

                    # A vela imediatamente posterior confirma ou cancela o sinal.
                    if pending and candle["t"] > pending["bar"]:
                        color = candle_color(candle)
                        confirmed = (
                            pending["type"] == "COMPRA" and color == "green"
                        ) or (
                            pending["type"] == "VENDA" and color == "red"
                        )
                        if confirmed:
                            event = {
                                "type": pending["type"],
                                "time": int(time.time()),
                                "price": candle["c"],
                                "cross_bar": pending["bar"],
                                "confirmation_bar": candle["t"],
                            }
                            state["last_signal"] = event
                            icon = "🟢" if pending["type"] == "COMPRA" else "🔴"
                            await telegram_safe(
                                f"{icon} {pending['type']} CONFIRMADA — BTCUSDT 1m\n"
                                f"Preço no fechamento: {candle['c']:.2f}\n"
                                "Regra: cruzamento do Klinger + próxima vela da mesma cor."
                            )
                        pending = None
                        state["pending"] = None

                    if all(
                        value is not None
                        for value in (
                            kvo[previous],
                            signal[previous],
                            kvo[current],
                            signal[current],
                        )
                    ):
                        crossed_up = (
                            kvo[previous] <= signal[previous]
                            and kvo[current] > signal[current]
                        )
                        crossed_down = (
                            kvo[previous] >= signal[previous]
                            and kvo[current] < signal[current]
                        )
                        color = candle_color(candle)
                        signal_type = (
                            "COMPRA"
                            if crossed_up and color == "green"
                            else "VENDA"
                            if crossed_down and color == "red"
                            else None
                        )
                        if signal_type:
                            pending = {
                                "type": signal_type,
                                "bar": candle["t"],
                                "color": color,
                            }
                            state["pending"] = pending

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


@app.post("/test-telegram")
async def test_telegram(request: Request):
    global telegram_test_used
    supplied = request.headers.get("X-Test-Token", "")
    supplied_hash = hashlib.sha256(supplied.encode()).hexdigest()
    if telegram_test_used or not hmac.compare_digest(
        supplied_hash, TELEGRAM_TEST_TOKEN_HASH
    ):
        raise HTTPException(status_code=404, detail="Not found")

    telegram_test_used = True
    try:
        token = os.getenv("TELEGRAM_BOT_TOKEN", "")
        chat = os.getenv("TELEGRAM_CHAT_ID", "")
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                data={
                    "chat_id": chat,
                    "text": (
                        "🧪 TESTE — NÃO É SINAL REAL\n\n"
                        "🟢 COMPRA CONFIRMADA — BTCUSDT 1m\n"
                        "Preço de exemplo: 78.000,00\n"
                        "Regra: cruzamento do Klinger + próxima vela da mesma cor."
                    ),
                },
            )
        if response.status_code != 200:
            try:
                description = response.json().get("description", "unknown")
            except Exception:
                description = "unknown"
            state["telegram_test"] = "failed"
            state["telegram_test_detail"] = {
                "status": response.status_code,
                "description": description,
            }
            raise HTTPException(status_code=502, detail="Telegram failed")
        state["telegram_test"] = "sent"
        return {"ok": True}
    except HTTPException:
        raise
    except Exception as exc:
        state["telegram_test"] = "failed"
        state["last_error"] = f"Telegram: {type(exc).__name__}"
        raise HTTPException(status_code=502, detail="Telegram failed") from exc


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
