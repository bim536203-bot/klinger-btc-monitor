import asyncio
import hashlib
import hmac
import json
import math
import os
import time
from urllib.parse import urlencode

import httpx
import websockets
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse


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
# Endpoint oficial do USD-M Futures Testnet. O host demo-fapi pode bloquear
# datacenters do Render por localizacao, mesmo para consultas somente leitura.
FUTURES_TESTNET_URL = os.getenv(
    "BINANCE_TESTNET_BASE_URL", "https://testnet.binancefuture.com"
).rstrip("/")
TRADE_SYMBOL = "BTCUSDT"
TRADE_LEVERAGE = 10
TARGET_NOTIONAL_USDT = 50.0
MAX_CONSECUTIVE_LOSSES = 5
QUANTITY_STEP = 0.0001

state = {
    "running": False,
    "mode": MODE,
    "indicator": "Klinger Fabio V4 - Alta Precisao",
    "last_signal": None,
    "signal_history": [],
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
    "trading_environment": "binance_futures_testnet",
    "trading_enabled": False,
    "trading_status": "disabled",
    "trade_leverage": TRADE_LEVERAGE,
    "target_notional_usdt": TARGET_NOTIONAL_USDT,
    "estimated_margin_usdt": TARGET_NOTIONAL_USDT / TRADE_LEVERAGE,
    "open_position": None,
    "last_order": None,
    "last_trade_result": None,
    "consecutive_losses": 0,
    "max_consecutive_losses": MAX_CONSECUTIVE_LOSSES,
    "trading_error": None,
}

task = None
bars = []
dashboard_cache = {"updated_monotonic": 0.0, "data": None}
dashboard_lock = asyncio.Lock()


def env_true(name):
    return os.getenv(name, "").strip().lower() in {"1", "true", "yes", "on"}


def futures_credentials():
    return (
        os.getenv("BINANCE_TESTNET_API_KEY", "").strip(),
        os.getenv("BINANCE_TESTNET_SECRET_KEY", "").strip(),
    )


def order_quantity(price):
    """Arredonda para cima para respeitar notional minimo e step do BTCUSDT."""
    steps = math.ceil((TARGET_NOTIONAL_USDT / price) / QUANTITY_STEP)
    return round(steps * QUANTITY_STEP, 4)


async def futures_request(method, path, params=None, signed=False):
    params = dict(params or {})
    api_key, secret = futures_credentials()
    headers = {}
    if signed:
        if not api_key or not secret:
            raise RuntimeError("Binance Testnet credentials are not configured")
        params["timestamp"] = int(time.time() * 1000)
        params["recvWindow"] = 5000
        query = urlencode(params)
        params["signature"] = hmac.new(
            secret.encode(), query.encode(), hashlib.sha256
        ).hexdigest()
        headers["X-MBX-APIKEY"] = api_key
    async with httpx.AsyncClient(timeout=15) as client:
        response = await client.request(
            method,
            f"{FUTURES_TESTNET_URL}{path}",
            params=params,
            headers=headers,
        )
    if response.status_code >= 400:
        try:
            error = response.json()
            message = error.get("msg", "Binance API error")
            code = error.get("code", response.status_code)
        except Exception:
            message = "Binance API error"
            code = response.status_code
        raise RuntimeError(f"Binance {code}: {message}")
    return response.json()


def safe_float(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def executed_operations(user_trades):
    """Agrupa os fills de fechamento por orderId e calcula o PnL liquido."""
    grouped = {}
    for fill in user_trades:
        realized = safe_float(fill.get("realizedPnl"))
        if abs(realized) < 1e-12:
            continue
        order_id = str(fill.get("orderId", fill.get("id", "unknown")))
        operation = grouped.setdefault(
            order_id,
            {
                "order_id": order_id,
                "closed_at": 0,
                "side": fill.get("side"),
                "quantity": 0.0,
                "quote_value": 0.0,
                "realized_pnl_usdt": 0.0,
                "commission_usdt": 0.0,
            },
        )
        quantity = safe_float(fill.get("qty"))
        price = safe_float(fill.get("price"))
        operation["quantity"] += quantity
        operation["quote_value"] += price * quantity
        operation["realized_pnl_usdt"] += realized
        if str(fill.get("commissionAsset", "")).upper() == "USDT":
            operation["commission_usdt"] += safe_float(fill.get("commission"))
        operation["closed_at"] = max(
            operation["closed_at"], int(fill.get("time", 0) or 0)
        )

    operations = []
    for operation in grouped.values():
        quantity = operation.pop("quantity")
        quote_value = operation.pop("quote_value")
        realized = operation["realized_pnl_usdt"]
        commission = operation["commission_usdt"]
        net = realized - commission
        operation.update(
            {
                "quantity": round(quantity, 8),
                "exit_price": round(quote_value / quantity, 2) if quantity else None,
                "realized_pnl_usdt": round(realized, 6),
                "commission_usdt": round(commission, 6),
                "net_pnl_usdt": round(net, 6),
                "result": "LUCRO" if net > 0 else "LOS" if net < 0 else "ZERO",
            }
        )
        operations.append(operation)
    return sorted(operations, key=lambda item: item["closed_at"], reverse=True)


async def dashboard_snapshot():
    """Resumo publico somente leitura, sem incluir credenciais da Binance."""
    now = time.monotonic()
    cached = dashboard_cache["data"]
    if cached and now - dashboard_cache["updated_monotonic"] < 5:
        return cached

    async with dashboard_lock:
        now = time.monotonic()
        cached = dashboard_cache["data"]
        if cached and now - dashboard_cache["updated_monotonic"] < 5:
            return cached

        api_key, secret = futures_credentials()
        base = {
            "updated_at": int(time.time() * 1000),
            "connected": False,
            "environment": state["trading_environment"],
            "bot": {
                "monitor_running": state["running"],
                "trading_enabled": state["trading_enabled"],
                "trading_status": state["trading_status"],
                "telegram_status": state["telegram_status"],
                "price": state["price"],
                "kvo": state["kvo"],
                "signal_line": state["signal"],
                "last_signal": state["last_signal"],
                "consecutive_losses": state["consecutive_losses"],
                "max_consecutive_losses": state["max_consecutive_losses"],
                "leverage": state["trade_leverage"],
                "estimated_margin_usdt": state["estimated_margin_usdt"],
            },
            "balance": None,
            "position": None,
            "summary": {
                "operations": 0,
                "profits": 0,
                "losses": 0,
                "zero": 0,
                "accuracy": 0.0,
                "net_pnl_usdt": 0.0,
            },
            "operations": [],
            "signal_history": list(state["signal_history"][:100]),
            "error": None,
        }
        if not api_key or not secret:
            base["error"] = "Chaves do Binance Demo ainda nao configuradas"
        else:
            try:
                account, positions, user_trades = await asyncio.gather(
                    futures_request("GET", "/fapi/v2/account", signed=True),
                    futures_request(
                        "GET",
                        "/fapi/v3/positionRisk",
                        {"symbol": TRADE_SYMBOL},
                        signed=True,
                    ),
                    futures_request(
                        "GET",
                        "/fapi/v1/userTrades",
                        {"symbol": TRADE_SYMBOL, "limit": 1000},
                        signed=True,
                    ),
                )
                usdt = next(
                    (
                        asset
                        for asset in account.get("assets", [])
                        if asset.get("asset") == "USDT"
                    ),
                    {},
                )
                base["balance"] = {
                    "wallet_usdt": round(safe_float(usdt.get("walletBalance")), 4),
                    "available_usdt": round(
                        safe_float(usdt.get("availableBalance")), 4
                    ),
                    "margin_usdt": round(safe_float(usdt.get("marginBalance")), 4),
                    "unrealized_pnl_usdt": round(
                        safe_float(usdt.get("unrealizedProfit")), 6
                    ),
                }
                current_position = next(
                    (
                        position
                        for position in positions
                        if abs(safe_float(position.get("positionAmt"))) > 0
                    ),
                    None,
                )
                if current_position:
                    amount = safe_float(current_position.get("positionAmt"))
                    base["position"] = {
                        "direction": "LONG" if amount > 0 else "SHORT",
                        "quantity": abs(amount),
                        "entry_price": safe_float(current_position.get("entryPrice")),
                        "mark_price": safe_float(current_position.get("markPrice")),
                        "unrealized_pnl_usdt": safe_float(
                            current_position.get("unRealizedProfit")
                        ),
                        "leverage": int(safe_float(current_position.get("leverage"))),
                        "margin_type": current_position.get("marginType"),
                    }
                operations = executed_operations(user_trades)
                profits = sum(item["result"] == "LUCRO" for item in operations)
                losses = sum(item["result"] == "LOS" for item in operations)
                zeros = sum(item["result"] == "ZERO" for item in operations)
                decided = profits + losses
                base["operations"] = operations[:100]
                base["summary"] = {
                    "operations": len(operations),
                    "profits": profits,
                    "losses": losses,
                    "zero": zeros,
                    "accuracy": round(profits * 100.0 / decided, 2) if decided else 0.0,
                    "net_pnl_usdt": round(
                        sum(item["net_pnl_usdt"] for item in operations), 6
                    ),
                }
                base["connected"] = True
                state["trading_error"] = None
            except Exception as exc:
                base["error"] = str(exc)

        dashboard_cache["updated_monotonic"] = time.monotonic()
        dashboard_cache["data"] = base
        return base


async def trading_diagnostics():
    state["trading_enabled"] = env_true("BINANCE_TRADING_ENABLED")
    api_key, secret = futures_credentials()
    if not state["trading_enabled"]:
        state["trading_status"] = "disabled"
        return
    if not api_key or not secret:
        state["trading_status"] = "missing_testnet_credentials"
        state["trading_enabled"] = False
        return
    try:
        await futures_request("GET", "/fapi/v2/account", signed=True)
        positions = await futures_request(
            "GET",
            "/fapi/v3/positionRisk",
            {"symbol": TRADE_SYMBOL},
            signed=True,
        )
        if any(float(position.get("positionAmt", 0)) != 0 for position in positions):
            state["trading_status"] = "blocked_existing_position"
            state["trading_error"] = (
                "Existing Testnet position detected; close it before enabling the bot"
            )
            state["trading_enabled"] = False
            return
        try:
            await futures_request(
                "POST",
                "/fapi/v1/marginType",
                {"symbol": TRADE_SYMBOL, "marginType": "ISOLATED"},
                signed=True,
            )
        except RuntimeError as exc:
            if "-4046" not in str(exc):
                raise
        await futures_request(
            "POST",
            "/fapi/v1/leverage",
            {"symbol": TRADE_SYMBOL, "leverage": TRADE_LEVERAGE},
            signed=True,
        )
        state["trading_status"] = "ready"
        state["trading_error"] = None
    except Exception as exc:
        state["trading_status"] = "error"
        state["trading_error"] = str(exc)
        state["trading_enabled"] = False


async def open_testnet_position(signal_type, candle, seconds_remaining):
    if not state["trading_enabled"] or state["trading_status"] != "ready":
        return None
    if state["open_position"] is not None:
        return None
    side = "BUY" if signal_type == "COMPRA" else "SELL"
    quantity = order_quantity(candle["c"])
    response = await futures_request(
        "POST",
        "/fapi/v1/order",
        {
            "symbol": TRADE_SYMBOL,
            "side": side,
            "type": "MARKET",
            "quantity": f"{quantity:.4f}",
            "newOrderRespType": "RESULT",
        },
        signed=True,
    )
    entry_price = float(response.get("avgPrice") or candle["c"])
    position = {
        "signal_type": signal_type,
        "side": side,
        "quantity": quantity,
        "entry_price": entry_price,
        "signal_bar": candle["t"],
        "opened_at": int(time.time()),
        "seconds_remaining": round(seconds_remaining, 1),
        "order_id": response.get("orderId"),
    }
    state["open_position"] = position
    state["last_order"] = {
        "action": "OPEN",
        "side": side,
        "quantity": quantity,
        "price": entry_price,
        "order_id": response.get("orderId"),
    }
    await telegram_safe(
        f"🧪 TESTNET — POSIÇÃO ABERTA\n"
        f"{signal_type} | BTCUSDT Futures {TRADE_LEVERAGE}x isolado\n"
        f"Quantidade: {quantity:.4f} BTC | Entrada: {entry_price:.2f}"
    )
    return position


async def close_testnet_position(candle):
    position = state["open_position"]
    if not position:
        return None
    close_side = "SELL" if position["side"] == "BUY" else "BUY"
    response = await futures_request(
        "POST",
        "/fapi/v1/order",
        {
            "symbol": TRADE_SYMBOL,
            "side": close_side,
            "type": "MARKET",
            "quantity": f"{position['quantity']:.4f}",
            "reduceOnly": "true",
            "newOrderRespType": "RESULT",
        },
        signed=True,
    )
    exit_price = float(response.get("avgPrice") or candle["c"])
    direction = 1.0 if position["side"] == "BUY" else -1.0
    gross_pnl = (
        (exit_price - position["entry_price"])
        * position["quantity"]
        * direction
    )
    won = gross_pnl > 0
    if won:
        state["consecutive_losses"] = 0
    else:
        state["consecutive_losses"] += 1
    result = {
        "result": "LUCRO" if won else "LOS",
        "gross_pnl_usdt": round(gross_pnl, 6),
        "entry_price": position["entry_price"],
        "exit_price": exit_price,
        "quantity": position["quantity"],
        "closed_at": int(time.time()),
        "order_id": response.get("orderId"),
    }
    state["last_trade_result"] = result
    state["last_order"] = {
        "action": "CLOSE",
        "side": close_side,
        "quantity": position["quantity"],
        "price": exit_price,
        "order_id": response.get("orderId"),
    }
    state["open_position"] = None
    if state["consecutive_losses"] >= MAX_CONSECUTIVE_LOSSES:
        state["trading_enabled"] = False
        state["trading_status"] = "paused_loss_limit"
    elif state["trading_enabled"]:
        state["trading_status"] = "ready"
        state["trading_error"] = None
    await telegram_safe(
        f"🧪 TESTNET — {result['result']}\n"
        f"Entrada: {position['entry_price']:.2f} | Saída: {exit_price:.2f}\n"
        f"Resultado bruto: {gross_pnl:+.6f} USDT\n"
        f"Perdas consecutivas: "
        f"{state['consecutive_losses']}/{MAX_CONSECUTIVE_LOSSES}"
    )
    return result


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
            data={
                "chat_id": chat,
                "text": text,
                "parse_mode": "HTML",
                "disable_notification": "false",
            },
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
                        event_id = f"{candle['t']}-{latched_signal}"
                        event = {
                            "id": event_id,
                            "type": latched_signal,
                            "time": int(time.time()),
                            "price": candle["c"],
                            "bar": candle["t"],
                            "seconds_remaining": round(seconds_remaining, 1),
                            "kvo": evaluation["kvo"],
                            "signal": evaluation["signal"],
                            "body_percent": evaluation["body_percent"],
                            "separation": evaluation["separation"],
                            "result": "PENDENTE",
                            "result_time": None,
                            "result_price": None,
                        }
                        pending = {
                            "type": latched_signal,
                            "bar": candle["t"],
                            "event_id": event_id,
                        }
                        state["last_signal"] = event
                        state["pending"] = pending
                        state["signal_history"].insert(0, event.copy())
                        del state["signal_history"][200:]
                        icon = "🟢" if latched_signal == "COMPRA" else "🔴"
                        await telegram_safe(
                            f"🚨🚨 <b>ALERTA DE OPERAÇÃO — {latched_signal}</b> 🚨🚨\n\n"
                            f"{icon} <b>BTCUSDT • 1 minuto</b>\n"
                            f"💰 Preço: <b>{candle['c']:.2f}</b>\n"
                            f"⏱️ Sinal confirmado nos {SECONDS_BEFORE_CLOSE}s finais da vela.\n\n"
                            f"👉 <b>Confira o gráfico agora.</b>\n"
                            f"⚠️ Operação manual: nenhuma ordem foi enviada."
                        )
                        # Modo somente alerta: nenhuma ordem, inclusive Demo/Testnet,
                        # e enviada automaticamente quando surge um sinal.

                    if not bool(kline["x"]) or candle["t"] == last_processed_bar:
                        continue

                    last_processed_bar = candle["t"]
                    bars.append(candle)
                    bars = bars[-1000:]
                    state["last_closed_bar"] = candle["t"]

                    # Tenta fechar novamente em fechamentos posteriores caso a
                    # primeira tentativa falhe por um erro temporario da API.
                    if (
                        state["open_position"]
                        and candle["t"] > state["open_position"]["signal_bar"]
                    ):
                        try:
                            await close_testnet_position(candle)
                        except Exception as exc:
                            state["trading_error"] = str(exc)
                            state["trading_status"] = "close_retry_pending"
                            await telegram_safe(
                                "⚠️ Falha ao fechar posição TESTNET; "
                                "nova tentativa no próximo fechamento."
                            )

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
                        result_name = (
                            "LUCRO" if profit else "LOS" if loss else "DOJI"
                        )
                        for saved_event in state["signal_history"]:
                            if saved_event.get("id") == pending["event_id"]:
                                saved_event["result"] = result_name
                                saved_event["result_time"] = int(time.time())
                                saved_event["result_price"] = candle["c"]
                                break
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


DASHBOARD_HTML = """<!doctype html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
  <meta name="theme-color" content="#090d16">
  <title>Klinger V4 — Binance Demo</title>
  <style>
    :root{--bg:#090d16;--card:#121a29;--line:#243047;--text:#f4f7fb;--muted:#95a3b8;--green:#22c98b;--red:#ff5c70;--yellow:#f5bd36;--blue:#5ea7ff}
    *{box-sizing:border-box}body{margin:0;background:linear-gradient(180deg,#0d1422,#070a11);color:var(--text);font:15px -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;min-height:100vh}
    .wrap{max-width:1120px;margin:auto;padding:18px 14px 40px}.top{display:flex;align-items:center;justify-content:space-between;gap:12px;margin-bottom:15px}
    h1{font-size:21px;margin:0}.sub{color:var(--muted);font-size:12px;margin-top:4px}.live{display:flex;align-items:center;gap:7px;color:var(--green);font-weight:700;font-size:13px}.dot{width:9px;height:9px;border-radius:50%;background:var(--green);box-shadow:0 0 12px var(--green)}
    .grid{display:grid;grid-template-columns:repeat(4,1fr);gap:10px}.card{background:rgba(18,26,41,.94);border:1px solid var(--line);border-radius:16px;padding:15px;box-shadow:0 10px 30px rgba(0,0,0,.18)}
    .label{color:var(--muted);font-size:12px}.value{font-size:24px;font-weight:760;margin-top:6px;letter-spacing:-.4px}.small{font-size:16px}.green{color:var(--green)}.red{color:var(--red)}.yellow{color:var(--yellow)}
    .section{margin-top:12px}.section-title{font-size:15px;margin:0 0 10px}.status-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:9px}.status{background:#0d1422;border:1px solid var(--line);border-radius:12px;padding:11px}.status b{display:block;margin-top:5px}
    .position{display:grid;grid-template-columns:repeat(6,1fr);gap:8px}.empty{color:var(--muted);padding:9px 0}.pill{display:inline-flex;padding:4px 9px;border-radius:999px;font-size:11px;font-weight:750;background:#1a2639}.pill.ok{color:var(--green)}.pill.off{color:var(--yellow)}.pill.bad{color:var(--red)}
    .table-wrap{overflow:auto;border:1px solid var(--line);border-radius:13px}table{width:100%;border-collapse:collapse;min-width:720px;background:#0d1422}th,td{text-align:left;padding:11px;border-bottom:1px solid #202a3d;font-size:13px}th{color:var(--muted);font-size:11px;text-transform:uppercase;letter-spacing:.5px}tr:last-child td{border-bottom:0}
    .error{display:none;background:#3b1720;color:#ffb4bf;border:1px solid #7a2938;padding:11px;border-radius:12px;margin-bottom:12px}.foot{text-align:center;color:var(--muted);font-size:11px;margin-top:15px}
    @media(max-width:800px){.grid{grid-template-columns:repeat(2,1fr)}.status-grid{grid-template-columns:repeat(2,1fr)}.position{grid-template-columns:repeat(2,1fr)}.value{font-size:21px}}
  </style>
</head>
<body><main class="wrap">
  <div class="top"><div><h1>Klinger V4 • BTCUSDT</h1><div class="sub">Binance Futures Demo • atualização automática</div></div><div class="live"><span class="dot"></span><span id="connection">CONECTANDO</span></div></div>
  <div id="error" class="error"></div>
  <section class="grid">
    <div class="card"><div class="label">Saldo Demo</div><div class="value" id="wallet">—</div></div>
    <div class="card"><div class="label">Disponível</div><div class="value" id="available">—</div></div>
    <div class="card"><div class="label">PnL realizado</div><div class="value" id="netPnl">—</div></div>
    <div class="card"><div class="label">PnL posição aberta</div><div class="value" id="openPnl">—</div></div>
  </section>
  <section class="card section"><h2 class="section-title">Resultados executados</h2><div class="grid">
    <div><div class="label">Operações</div><div class="value small" id="operations">0</div></div>
    <div><div class="label">LUCRO</div><div class="value small green" id="profits">0</div></div>
    <div><div class="label">LOS</div><div class="value small red" id="losses">0</div></div>
    <div><div class="label">Precisão</div><div class="value small" id="accuracy">0%</div></div>
  </div></section>
  <section class="card section"><h2 class="section-title">Execução em tempo real</h2><div class="status-grid">
    <div class="status"><span class="label">Monitor</span><b id="monitor">—</b></div>
    <div class="status"><span class="label">Executor</span><b id="executor">—</b></div>
    <div class="status"><span class="label">Telegram</span><b id="telegram">—</b></div>
    <div class="status"><span class="label">Preço BTC</span><b id="price">—</b></div>
    <div class="status"><span class="label">Último sinal</span><b id="lastSignal">Nenhum</b></div>
    <div class="status"><span class="label">KVO / Sinal</span><b id="kvo">—</b></div>
    <div class="status"><span class="label">Perdas seguidas</span><b id="streak">0 / 5</b></div>
    <div class="status"><span class="label">Configuração</span><b id="config">10x • ~5 USDT</b></div>
  </div></section>
  <section class="card section"><h2 class="section-title">Sinais enviados pelo Telegram</h2><div class="table-wrap"><table><thead><tr><th>Horário UTC-4</th><th>Sinal</th><th>Preço</th><th>Resultado</th></tr></thead><tbody id="signalHistory"><tr><td colspan="4" class="empty">Aguardando o próximo sinal.</td></tr></tbody></table></div></section>
  <section class="card section"><h2 class="section-title">Posição aberta</h2><div id="position" class="empty">Nenhuma posição aberta.</div></section>
  <section class="card section"><h2 class="section-title">Histórico de operações</h2><div class="table-wrap"><table><thead><tr><th>Data</th><th>Resultado</th><th>Lado de fechamento</th><th>Quantidade</th><th>Saída</th><th>PnL líquido</th></tr></thead><tbody id="history"><tr><td colspan="6" class="empty">Nenhuma operação executada.</td></tr></tbody></table></div></section>
  <div class="foot">Dados somente leitura • nenhuma chave é enviada ao navegador • <span id="updated">—</span></div>
</main>
<script>
const el=id=>document.getElementById(id);const money=(v,d=2)=>v==null?'—':Number(v).toLocaleString('pt-BR',{minimumFractionDigits:d,maximumFractionDigits:d})+' USDT';
const num=(v,d=2)=>v==null?'—':Number(v).toLocaleString('pt-BR',{minimumFractionDigits:d,maximumFractionDigits:d});
const color=(node,value)=>{node.classList.remove('green','red');if(Number(value)>0)node.classList.add('green');if(Number(value)<0)node.classList.add('red')};
const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
function render(d){el('connection').textContent=d.connected?'AO VIVO':'SEM CONEXÃO';el('error').style.display=d.error?'block':'none';el('error').textContent=d.error||'';
 const b=d.balance||{};el('wallet').textContent=money(b.wallet_usdt,2);el('available').textContent=money(b.available_usdt,2);el('openPnl').textContent=money(b.unrealized_pnl_usdt,4);color(el('openPnl'),b.unrealized_pnl_usdt);
 el('netPnl').textContent=money(d.summary.net_pnl_usdt,4);color(el('netPnl'),d.summary.net_pnl_usdt);el('operations').textContent=d.summary.operations;el('profits').textContent=d.summary.profits;el('losses').textContent=d.summary.losses;el('accuracy').textContent=num(d.summary.accuracy,1)+'%';
 const bot=d.bot;el('monitor').innerHTML=bot.monitor_running?'<span class="pill ok">ATIVO</span>':'<span class="pill bad">PARADO</span>';el('executor').innerHTML=bot.trading_enabled?'<span class="pill ok">'+esc(bot.trading_status)+'</span>':'<span class="pill off">'+esc(bot.trading_status)+'</span>';el('telegram').textContent=bot.telegram_status;el('price').textContent=bot.price?num(bot.price,2)+' USDT':'—';el('lastSignal').textContent=bot.last_signal?bot.last_signal.type+' • '+num(bot.last_signal.price,2):'Nenhum';el('kvo').textContent=(bot.kvo==null?'—':num(bot.kvo,2))+' / '+(bot.signal_line==null?'—':num(bot.signal_line,2));el('streak').textContent=bot.consecutive_losses+' / '+bot.max_consecutive_losses;el('config').textContent=bot.leverage+'x • ~'+num(bot.estimated_margin_usdt,2)+' USDT';
 const signals=d.signal_history||[];el('signalHistory').innerHTML=signals.length?signals.map(s=>{const when=new Date(Number(s.time)*1000).toLocaleString('pt-BR',{timeZone:'America/Manaus',day:'2-digit',month:'2-digit',hour:'2-digit',minute:'2-digit',second:'2-digit'});const result=s.result||'PENDENTE';const resultClass=result==='LUCRO'?'green':result==='LOS'?'red':'yellow';const signalClass=s.type==='COMPRA'?'green':'red';return `<tr><td>${when}</td><td class="${signalClass}"><b>${esc(s.type)}</b></td><td>${num(s.price,2)} USDT</td><td class="${resultClass}"><b>${esc(result)}</b></td></tr>`}).join(''):'<tr><td colspan="4" class="empty">Aguardando o próximo sinal.</td></tr>';
 if(d.position){const p=d.position;el('position').className='position';el('position').innerHTML=`<div><span class="label">Direção</span><div class="value small ${p.direction==='LONG'?'green':'red'}">${esc(p.direction)}</div></div><div><span class="label">Quantidade</span><div class="value small">${num(p.quantity,4)} BTC</div></div><div><span class="label">Entrada</span><div class="value small">${num(p.entry_price,2)}</div></div><div><span class="label">Preço atual</span><div class="value small">${num(p.mark_price,2)}</div></div><div><span class="label">PnL</span><div class="value small ${p.unrealized_pnl_usdt>=0?'green':'red'}">${money(p.unrealized_pnl_usdt,4)}</div></div><div><span class="label">Margem</span><div class="value small">${esc(p.margin_type)} • ${p.leverage}x</div></div>`}else{el('position').className='empty';el('position').textContent='Nenhuma posição aberta.'}
 el('history').innerHTML=d.operations.length?d.operations.map(o=>`<tr><td>${new Date(o.closed_at).toLocaleString('pt-BR')}</td><td class="${o.result==='LUCRO'?'green':o.result==='LOS'?'red':''}"><b>${esc(o.result)}</b></td><td>${esc(o.side||'—')}</td><td>${num(o.quantity,4)} BTC</td><td>${num(o.exit_price,2)}</td><td class="${o.net_pnl_usdt>=0?'green':'red'}">${money(o.net_pnl_usdt,6)}</td></tr>`).join(''):'<tr><td colspan="6" class="empty">Nenhuma operação executada.</td></tr>';el('updated').textContent='Atualizado '+new Date(d.updated_at).toLocaleTimeString('pt-BR')}
async function refresh(){try{const r=await fetch('/dashboard-data',{cache:'no-store'});render(await r.json())}catch(e){el('connection').textContent='ERRO';el('error').style.display='block';el('error').textContent='Não foi possível atualizar o painel.'}}
refresh();setInterval(refresh,3000);
</script></body></html>"""


@app.on_event("startup")
async def startup_event():
    # Inicia automaticamente após cada deploy ou despertar da instância.
    ensure_monitor()
    asyncio.create_task(telegram_diagnostics())
    asyncio.create_task(trading_diagnostics())


@app.get("/")
def root():
    return {
        "ok": True,
        "service": "Klinger BTC",
        "running": state["running"],
        "mode": state["mode"],
    }


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    return DASHBOARD_HTML


@app.get("/dashboard-data")
async def dashboard_data():
    return await dashboard_snapshot()


@app.get("/status")
def status():
    return state


@app.get("/trading-status")
def trading_status():
    return {
        "environment": state["trading_environment"],
        "enabled": state["trading_enabled"],
        "status": state["trading_status"],
        "leverage": state["trade_leverage"],
        "target_notional_usdt": state["target_notional_usdt"],
        "estimated_margin_usdt": state["estimated_margin_usdt"],
        "open_position": state["open_position"],
        "last_order": state["last_order"],
        "last_trade_result": state["last_trade_result"],
        "consecutive_losses": state["consecutive_losses"],
        "max_consecutive_losses": state["max_consecutive_losses"],
        "error": state["trading_error"],
    }


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
