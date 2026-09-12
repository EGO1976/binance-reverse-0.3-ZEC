import asyncio
import hashlib
import hmac
import logging
import math
import os
import time
import urllib.parse
import uuid
import json
from decimal import Decimal, ROUND_HALF_UP
import aiohttp
import ujson
import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI

try:
    import websockets
except ImportError:
    websockets = None

# =========================================================================
# ⚙️ ОСНОВНЫЕ НАСТРОЙКИ БОТА
# =========================================================================
SYMBOL = os.getenv("SYMBOL", "ZECUSDC").strip()
LEVERAGE = int(os.getenv("LEVERAGE", 20))
USDT_AMOUNT = float(os.getenv("USDT_AMOUNT", 1000))
MARTINGALE_MULTIPLIER = float(os.getenv("MARTINGALE", 2.35))
MAX_STREAK = int(os.getenv("MAX_STREAK", 3))
TARGET_PERCENT = float(os.getenv("TARGET_PERCENT", 0.3))

BINANCE_API_KEY = (
    os.getenv("BINANCE_API_KEY") or os.getenv("BINANCE_KEY") or ""
).strip().strip("'\"")

BINANCE_API_SECRET = (
    os.getenv("BINANCE_SECRET_KEY") or os.getenv("BINANCE_API_SECRET") or os.getenv("BINANCE_SECRET") or ""
).strip().strip("'\"")

TG_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip().strip("'\"")
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip().strip("'\"")
STATE_FILE = os.getenv("STATE_FILE", "bot_state.json")
# =========================================================================

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(message)s')
logger = logging.getLogger("TRADING_BOT")

LAST_ERROR_STATE = {"key": None, "sent": False}

async def send_tg_async(session: aiohttp.ClientSession, text: str, is_error: bool = False, error_key: str = None):
    global LAST_ERROR_STATE
    
    if is_error and error_key:
        if LAST_ERROR_STATE["key"] == error_key and LAST_ERROR_STATE["sent"]:
            return
        LAST_ERROR_STATE["key"] = error_key
        LAST_ERROR_STATE["sent"] = True
    elif not is_error:
        LAST_ERROR_STATE["key"] = None
        LAST_ERROR_STATE["sent"] = False

    if TG_TOKEN and TG_CHAT_ID and session and not session.closed:
        try:
            url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
            payload = {"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML"}
            async with session.post(url, json=payload, timeout=3) as resp:
                await resp.read()
        except Exception as e:
            logger.error(f"Ошибка отправки Telegram: {e}")

def format_price(price, tick_size=0.1):
    if tick_size <= 0:
        tick_size = 0.1
    precision = max(0, int(round(-math.log10(tick_size))))
    if precision == 0:
        return str(int(round(price)))
    return f"{price:.{precision}f}"

def format_qty(qty, step_size=0.001):
    if step_size <= 0:
        step_size = 0.001
    precision = max(0, int(round(-math.log10(step_size))))
    return round(float(qty), precision)

def round_to_tick(price: float, tick_size: float) -> float:
    if tick_size <= 0:
        return float(price)
    p = Decimal(str(price))
    tick = Decimal(str(tick_size))
    steps = (p / tick).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    result = steps * tick
    return float(result)

def calculate_tp_sl(entry_price: float, side: str, target_percent: float, tick_size: float):
    if entry_price <= 0:
        raise ValueError("Некорректный entry_price")
    distance = target_percent / 100.0
    if side == "BUY":
        raw_tp = entry_price * (1.0 + distance)
        raw_sl = entry_price * (1.0 - distance)
    else:
        raw_tp = entry_price * (1.0 - distance)
        raw_sl = entry_price * (1.0 + distance)

    tp = round_to_tick(raw_tp, tick_size)
    sl = round_to_tick(raw_sl, tick_size)
    return format_price(tp, tick_size), format_price(sl, tick_size)

class BinanceRateLimiter:
    def __init__(self, max_per_minute=1200):
        self.max_per_minute = max_per_minute
        self.tokens = max_per_minute
        self.last_update = time.time()
        self.lock = asyncio.Lock()

    async def wait(self, weight=1):
        async with self.lock:
            now = time.time()
            elapsed = now - self.last_update
            self.last_update = now
            self.tokens = min(self.max_per_minute, self.tokens + elapsed * (self.max_per_minute / 60.0))
            if self.tokens < weight:
                sleep_time = (weight - self.tokens) * (60.0 / self.max_per_minute)
                await asyncio.sleep(sleep_time)
                self.tokens = 0
            else:
                self.tokens -= weight

class StateManager:
    def __init__(self, file_path):
        self.file_path = file_path
        self.state = {
            "usdt": USDT_AMOUNT,
            "side": "BUY",
            "loss_streak": 0,
            "qty": 0.0,
            "entry_price": 0.0,
            "state": "ENTRY",
            "entry_time": 0.0,
            "entry_order_id": 0,
            "tp_order_id": 0,
            "tp_client_id": "",
            "sl_algo_id": 0,
            "sl_client_algo_id": "",
            "tp_price": 0.0,
            "sl_price": 0.0
        }
        self.load()

    def load(self):
        try:
            with open(self.file_path, 'r') as f:
                saved = json.load(f)
                self.state.update(saved)
                logger.info(f"Состояние загружено из {self.file_path}")
        except FileNotFoundError:
            logger.info("Файл состояния не найден, используем начальные значения")
        except Exception as e:
            logger.error(f"Ошибка загрузки состояния: {e}")

    def save(self):
        try:
            with open(self.file_path, 'w') as f:
                json.dump(self.state, f, indent=2)
        except Exception as e:
            logger.error(f"Ошибка сохранения состояния: {e}")

    def get(self, key, default=None):
        return self.state.get(key, default)

    def set(self, key, value):
        self.state[key] = value
        self.save()

    def update(self, data):
        self.state.update(data)
        self.save()

class BinanceMartingaleBot:
    def __init__(self):
        self.base_url = "https://fapi.binance.com"
        self.ws_base_url = "wss://fstream.binance.com/ws"
        self.session = None
        self.rate_limiter = BinanceRateLimiter()
        
        self.active_symbol = SYMBOL
        self.latest_price = 0.0
        self.last_price_time = 0.0
        self.is_running = True
        self.step_size = 0.001
        self.tick_size = 0.1
        self.listen_key = None
        self.last_pos_check_time = 0.0
        self.is_processing_close = False
        
        self.state_manager = StateManager(STATE_FILE)
        self.strategy = self.state_manager.state
        self.startup_sync_complete = False
        self.adopted_existing_position = False

    @property
    def base_asset(self):
        for quote in ["USDC", "USDT", "BUSD", "FDUSD"]:
            if self.active_symbol.endswith(quote):
                return self.active_symbol[:-len(quote)]
        return self.active_symbol

    async def _request(self, method, endpoint, params=None, signed=False, weight=1, suppress_error_codes=None):
        if suppress_error_codes is None:
            suppress_error_codes = []

        await self.rate_limiter.wait(weight=weight)
        
        if not BINANCE_API_KEY or not BINANCE_API_SECRET:
            logger.error("❌ BINANCE_API_KEY или BINANCE_SECRET_KEY не заданы в переменной окружения!")
            return {"code": -2014, "msg": "API Key or Secret missing"}

        headers = {"X-MBX-APIKEY": BINANCE_API_KEY}
        payload = {}
        if params:
            for k, v in params.items():
                payload[k] = "true" if v is True else ("false" if v is False else str(v))

        if signed:
            payload['timestamp'] = str(int(time.time() * 1000))
            payload['recvWindow'] = "5000"
            query_string = urllib.parse.urlencode(payload)
            signature = hmac.new(
                BINANCE_API_SECRET.encode('utf-8'),
                query_string.encode('utf-8'),
                hashlib.sha256
            ).hexdigest()
            full_query = f"{query_string}&signature={signature}"
        else:
            full_query = urllib.parse.urlencode(payload) if payload else ""

        url = f"{self.base_url}{endpoint}"

        try:
            if method.upper() in ["POST", "PUT", "DELETE"]:
                headers["Content-Type"] = "application/x-www-form-urlencoded"
                async with self.session.request(method, url, data=full_query, headers=headers, timeout=5) as response:
                    if response.status == 404:
                        return {"code": 404, "msg": "Endpoint Not Found"}
                    try:
                        res = await response.json()
                    except Exception:
                        text_body = await response.text()
                        return {"code": response.status, "msg": text_body[:100]}
            else:
                final_url = f"{url}?{full_query}" if full_query else url
                async with self.session.request(method, final_url, headers=headers, timeout=5) as response:
                    if response.status == 404:
                        return {"code": 404, "msg": "Endpoint Not Found"}
                    try:
                        res = await response.json()
                    except Exception:
                        text_body = await response.text()
                        return {"code": response.status, "msg": text_body[:100]}

            if isinstance(res, dict) and "code" in res and res["code"] != 200:
                code = res.get("code")
                msg = res.get("msg", "")
                if code not in suppress_error_codes and code != 404:
                    logger.error(f"Binance API Error [{endpoint}]: Code {code}, Msg: {msg}")
                    if code == -1003:
                        await send_tg_async(self.session, f"⚠️ <b>ПРЕВЫШЕН ЛИМИТ ЗАПРОСОВ [{self.active_symbol}]</b>\n{msg}", is_error=True, error_key="rate_limit")
                    elif code in [-2008, -2014, -2015, -1022]:
                        await send_tg_async(self.session, f"⚠️ <b>ОШИБКА BINANCE API KEY [{self.active_symbol}]</b>\n{msg} (Код {code})", is_error=True, error_key=f"api_{code}")
            return res
        except Exception as e:
            logger.error(f"Ошибка REST API [{endpoint}]: {e}")
            await send_tg_async(self.session, f"⚠️ <b>СБОЙ СЕТЕВОГО СОЕДИНЕНИЯ [{self.active_symbol}]</b>\n{e}", is_error=True, error_key="network_error")
            return {"code": -1, "msg": str(e)}

    async def fetch_symbol_info(self):
        res = await self._request("GET", "/fapi/v1/exchangeInfo")
        if isinstance(res, dict) and "symbols" in res:
            for s in res["symbols"]:
                if s.get("symbol") == self.active_symbol:
                    for f in s.get("filters", []):
                        if f.get("filterType") == "LOT_SIZE":
                            self.step_size = float(f.get("stepSize", 0.001))
                        elif f.get("filterType") == "PRICE_FILTER":
                            self.tick_size = float(f.get("tickSize", 0.1))
                    logger.info(f"Параметры {self.active_symbol}: stepSize={self.step_size}, tickSize={self.tick_size}")
                    break

    async def get_market_data(self):
        if self.latest_price > 0 and (time.time() - self.last_price_time) < 2.0:
            return self.latest_price
        res = await self._request("GET", "/fapi/v1/ticker/price", {"symbol": self.active_symbol})
        if isinstance(res, dict) and "price" in res:
            self.latest_price = float(res["price"])
            self.last_price_time = time.time()
            return self.latest_price
        return 0.0

    async def get_raw_position(self, retries=2):
        for attempt in range(retries):
            res = await self._request("GET", "/fapi/v2/positionRisk", {"symbol": self.active_symbol}, signed=True, weight=5)
            if isinstance(res, list):
                for pos in res:
                    if pos.get("symbol") == self.active_symbol:
                        return pos
            await asyncio.sleep(0.3)
        return None

    async def get_actual_entry_price(self, order_id, side, retries=10):
        expected_sign = 1 if side == "BUY" else -1
        for _ in range(retries):
            try:
                order = await self._request("GET", "/fapi/v1/order", {
                    "symbol": self.active_symbol,
                    "orderId": order_id
                }, signed=True, weight=1)
                if isinstance(order, dict):
                    avg_price = float(order.get("avgPrice", 0) or 0)
                    if order.get("status") == "FILLED" and avg_price > 0:
                        logger.info(f"🎯 Фактический avgPrice MARKET: {avg_price}")
                        return avg_price
            except Exception as e:
                logger.warning(f"Ошибка получения MARKET order: {e}")

            try:
                pos = await self.get_raw_position(retries=1)
                if pos:
                    position_amt = float(pos.get("positionAmt", 0))
                    entry_price = float(pos.get("entryPrice", 0))
                    correct_direction = (expected_sign > 0 and position_amt > 0) or (expected_sign < 0 and position_amt < 0)
                    if correct_direction and abs(position_amt) > 0 and entry_price > 0:
                        logger.info(f"🎯 Фактический entryPrice из positionRisk: {entry_price}")
                        return entry_price
            except Exception as e:
                logger.warning(f"Ошибка получения positionRisk: {e}")

            await asyncio.sleep(0.2)
        return 0.0

    def required_margin_for_notional(self, notional_usdc: float, leverage: float = None) -> float:
        lev = float(leverage or LEVERAGE)
        if lev <= 0:
            lev = float(LEVERAGE) if LEVERAGE > 0 else 1.0
        notional = max(0.0, float(notional_usdc))
        return (notional / lev) * 1.05

    async def get_free_margin(self) -> float:
        res = await self._request("GET", "/fapi/v2/account", signed=True, weight=5)
        if isinstance(res, dict):
            assets = res.get("assets", [])
            target_asset = "USDC" if "USDC" in self.active_symbol else "USDT"
            for asset in assets:
                if asset.get("asset") == target_asset:
                    val = float(asset.get("availableBalance", asset.get("maxWithdrawAmount", 0.0)))
                    if val > 0:
                        return val
            root_avail = float(res.get("availableBalance", 0.0))
            if root_avail > 0:
                return root_avail
            root_max_withdraw = float(res.get("maxWithdrawAmount", 0.0))
            if root_max_withdraw > 0:
                return root_max_withdraw
        bal_res = await self._request("GET", "/fapi/v2/balance", signed=True, weight=1)
        if isinstance(bal_res, list):
            target_asset = "USDC" if "USDC" in self.active_symbol else "USDT"
            for b in bal_res:
                if b.get("asset") == target_asset:
                    return float(b.get("availableBalance", b.get("maxWithdrawAmount", 0.0)))
        return 0.0

    async def get_listen_key(self):
        res = await self._request("POST", "/fapi/v1/listenKey", signed=True, weight=1)
        if isinstance(res, dict) and "listenKey" in res:
            self.listen_key = res["listenKey"]
            return self.listen_key
        return None

    async def keepalive_listen_key(self):
        while self.is_running:
            await asyncio.sleep(1500)
            if self.listen_key:
                await self._request("PUT", "/fapi/v1/listenKey", signed=True, weight=1)
                logger.info("🔑 ListenKey обновлен")

    async def cancel_all_orders(self):
        await self._request("DELETE", "/fapi/v1/allOpenOrders", {"symbol": self.active_symbol}, signed=True, weight=1, suppress_error_codes=[-2011])
        await self._request("DELETE", "/fapi/v1/algoOpenOrders", {"symbol": self.active_symbol}, signed=True, weight=1, suppress_error_codes=[-2011, -1002, 404])

    async def place_tp_sl_orders(self, side, qty, entry_price, is_last_knee=False):
        if entry_price <= 0:
            logger.error("❌ TP/SL НЕ ВЫСТАВЛЕНЫ: entry_price <= 0")
            return None, None

        await self.cancel_all_orders()
        opp_side = "SELL" if side == "BUY" else "BUY"

        formatted_tp_price, formatted_sl_price = calculate_tp_sl(
            entry_price=entry_price,
            side=side,
            target_percent=TARGET_PERCENT,
            tick_size=self.tick_size
        )
        tp_numeric = float(formatted_tp_price)

        current_step = self.strategy.get("loss_streak", 0) + 1
        is_last_knee = is_last_knee or (current_step >= MAX_STREAK)

        if is_last_knee:
            should_skip_sl = True
            formatted_sl_price = "НЕ ВЫСТАВЛЕН (последнее колено)"
            sl_numeric = 0.0
            logger.info(f"🎯 Последнее колено #{current_step}/{MAX_STREAK}: выставляется ТОЛЬКО Тейк-Профит.")
        else:
            next_usdt = round(self.strategy.get("usdt", USDT_AMOUNT) * MARTINGALE_MULTIPLIER, 2)
            required_margin = self.required_margin_for_notional(next_usdt, LEVERAGE)
            free_margin = await self.get_free_margin()

            if free_margin < required_margin:
                should_skip_sl = True
                formatted_sl_price = "НЕ ВЫСТАВЛЕН (нехватка маржи)"
                sl_numeric = 0.0
                logger.warning(
                    f"⚠️ Недостаточно маржи для следующего колена ({current_step + 1}): "
                    f"требуется ≈{required_margin:.2f} USDC, доступно {free_margin:.2f} USDC. SL отменен."
                )
                await send_tg_async(
                    self.session,
                    f"⚠️ <b>НЕХВАТКА МАРЖИ НА СЛЕДУЮЩЕЕ КОЛЕНО [{self.active_symbol}]!</b>\n"
                    f"Колено: #{current_step}/{MAX_STREAK}\n"
                    f"Свободно маржи: {free_margin:.2f} USDC | Требуется: ~{required_margin:.2f} USDC\n"
                    f"❌ Stop-Loss ОТМЕНЕН/НЕ ВЫСТАВЛЕН.\n"
                    f"🎯 Ожидание закрытия по TP ({formatted_tp_price}).",
                    is_error=True,
                    error_key="margin_sl_skipped"
                )
            else:
                should_skip_sl = False
                sl_numeric = float(formatted_sl_price)

        self.strategy.update({
            "tp_order_id": 0,
            "tp_client_id": "",
            "sl_algo_id": 0,
            "sl_client_algo_id": "",
            "tp_price": tp_numeric,
            "sl_price": sl_numeric
        })

        sl_ok = True
        sl_res = None
        sl_client_id = ""
        if not should_skip_sl:
            sl_client_id = f"sl_{uuid.uuid4().hex[:10]}"
            sl_res = await self._request("POST", "/fapi/v1/algoOrder", {
                "symbol": self.active_symbol,
                "side": opp_side,
                "positionSide": "BOTH",
                "algoType": "CONDITIONAL",
                "type": "STOP_MARKET",
                "triggerPrice": formatted_sl_price,
                "closePosition": True,
                "workingType": "CONTRACT_PRICE",
                "clientAlgoId": sl_client_id
            }, signed=True, weight=1)
            sl_ok = isinstance(sl_res, dict) and sl_res.get("algoId") is not None
            if sl_ok:
                self.strategy["sl_algo_id"] = int(sl_res.get("algoId", 0))
                self.strategy["sl_client_algo_id"] = sl_client_id

        tp_client_id = f"tp_{uuid.uuid4().hex[:10]}"
        tp_res = await self._request("POST", "/fapi/v1/order", {
            "symbol": self.active_symbol,
            "side": opp_side,
            "positionSide": "BOTH",
            "type": "LIMIT",
            "timeInForce": "GTC",
            "quantity": qty,
            "price": formatted_tp_price,
            "reduceOnly": True,
            "newClientOrderId": tp_client_id
        }, signed=True, weight=1)
        tp_ok = isinstance(tp_res, dict) and tp_res.get("orderId") is not None
        if tp_ok:
            self.strategy["tp_order_id"] = int(tp_res.get("orderId", 0))
            self.strategy["tp_client_id"] = tp_client_id

        self.state_manager.save()

        if tp_ok and sl_ok:
            logger.info(f"✅ TP: {formatted_tp_price}, SL: {formatted_sl_price if not should_skip_sl else 'НЕТ'}")
        else:
            logger.warning(f"⚠️ TP: {'✅' if tp_ok else '❌'}, SL: {'✅' if sl_ok else '❌'}")

        return formatted_tp_price, formatted_sl_price

    async def sync_existing_position(self):
        pos = await self.get_raw_position(retries=5)
        if pos:
            amt = float(pos.get("positionAmt", 0.0))
            entry_price = float(pos.get("entryPrice", 0.0))
            if abs(amt) > 0 and entry_price > 0:
                self.adopted_existing_position = True
                side = "BUY" if amt > 0 else "SELL"
                qty = abs(amt)
                approx_usdc = round(qty * entry_price, 2)
                saved_streak = self.state_manager.get("loss_streak", 0)
                if saved_streak > 0:
                    loss_streak = saved_streak
                else:
                    loss_streak = max(0, round(math.log(max(approx_usdc, USDT_AMOUNT) / USDT_AMOUNT, MARTINGALE_MULTIPLIER))) if approx_usdc > USDT_AMOUNT else 0
                loss_streak = min(loss_streak, MAX_STREAK - 1)
                is_last_knee = (loss_streak + 1) >= MAX_STREAK
                
                self.strategy.update({
                    "usdt": max(approx_usdc, USDT_AMOUNT),
                    "side": side,
                    "entry_price": entry_price,
                    "qty": qty,
                    "loss_streak": loss_streak,
                    "state": "MONITOR",
                    "entry_time": time.time()
                })
                self.state_manager.save()

                tp_price, sl_price = await self.place_tp_sl_orders(side, qty, entry_price, is_last_knee)
                formatted_entry = format_price(entry_price, self.tick_size)
                await send_tg_async(
                    self.session,
                    f"🔄 <b>СИНХРОНИЗАЦИЯ [{self.active_symbol}]</b>\n"
                    f"Позиция: <b>{side} {qty} {self.base_asset}</b> (~{approx_usdc} USDC)\n"
                    f"Вход: {formatted_entry}\n"
                    f"🎯 TP: {tp_price}\n"
                    f"🛑 SL: {sl_price}"
                )
                return True
        return False

    async def setup_market(self):
        await self._request("POST", "/fapi/v1/positionSide/dual", {"dualSidePosition": "false"}, signed=True, suppress_error_codes=[-4059, -4067])
        await self._request("POST", "/fapi/v1/leverage", {"symbol": self.active_symbol, "leverage": LEVERAGE}, signed=True)
        await self._request("POST", "/fapi/v1/marginType", {"symbol": self.active_symbol, "marginType": "CROSSED"}, signed=True, suppress_error_codes=[-4046, -4067])

    async def ws_ticker_loop(self):
        if not websockets:
            return
        stream_name = f"{self.active_symbol.lower()}@ticker"
        url = f"{self.ws_base_url}/{stream_name}"
        while self.is_running:
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                    logger.info(f"⚡ Market WebSocket подключен ({self.active_symbol})")
                    async for msg in ws:
                        if not self.is_running:
                            break
                        data = ujson.loads(msg)
                        price = float(data.get("c", 0))
                        if price > 0:
                            self.latest_price = price
                            self.last_price_time = time.time()
            except Exception as e:
                logger.warning(f"Ошибка WebSocket Ticker: {e}. Переподключение...")
                await asyncio.sleep(3)

    async def ws_user_data_loop(self):
        if not websockets:
            return
        while self.is_running:
            key = await self.get_listen_key()
            if not key:
                await asyncio.sleep(5)
                continue
            url = f"{self.ws_base_url}/{key}"
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                    logger.info("⚡ User Data Stream WS подключен")
                    async for msg in ws:
                        if not self.is_running:
                            break
                        try:
                            data = ujson.loads(msg)
                            event_type = data.get("e")
                            
                            if event_type == "ORDER_TRADE_UPDATE":
                                order_data = data.get("o", {})
                                status = order_data.get("X")
                                client_id = str(order_data.get("c", ""))
                                if status in ["FILLED", "EXECUTED"]:
                                    saved_tp_id = int(self.strategy.get("tp_order_id", 0) or 0)
                                    order_id = int(order_data.get("i", 0) or 0)
                                    saved_tp_client = self.strategy.get("tp_client_id", "")
                                    if ((saved_tp_client and client_id == saved_tp_client) or
                                            (saved_tp_id and order_id == saved_tp_id)):
                                        logger.info("🎯 WS: ТОЧНО ИСПОЛНЕН TP")
                                        asyncio.create_task(self.handle_position_closed(close_type="TP"))
                            
                            elif event_type in ["ALGO_UPDATE", "ALGO_ORDER_UPDATE"]:
                                algo_data = data.get("o", {})
                                status = algo_data.get("X", algo_data.get("s"))
                                client_algo_id = str(algo_data.get("ca", algo_data.get("clientAlgoId", "")))
                                algo_id = int(algo_data.get("i", algo_data.get("aid", algo_data.get("algoId", 0))) or 0)
                                saved_sl_id = int(self.strategy.get("sl_algo_id", 0) or 0)
                                saved_sl_client = self.strategy.get("sl_client_algo_id", "")
                                if status in ["FILLED", "EXECUTED", "FINISHED"]:
                                    if ((saved_sl_client and client_algo_id == saved_sl_client) or
                                            (saved_sl_id and algo_id == saved_sl_id)):
                                        logger.info("🛑 WS ALGO: ТОЧНО ИСПОЛНЕН SL")
                                        asyncio.create_task(self.handle_position_closed(close_type="SL"))
                            
                            elif event_type == "ACCOUNT_UPDATE":
                                acc_data = data.get("a", {})
                                positions = acc_data.get("P", [])
                                for p in positions:
                                    if p.get("s") == self.active_symbol:
                                        pa = float(p.get("pa", 0))
                                        if pa == 0 and self.strategy["state"] == "MONITOR":
                                            logger.info("📊 ACCOUNT_UPDATE: позиция закрыта")
                                            asyncio.create_task(self.handle_position_closed())
                        except Exception as e:
                            logger.error(f"Ошибка обработки WS: {e}")
            except Exception as e:
                logger.warning(f"Ошибка UserStream WS: {e}. Переподключение...")
                await asyncio.sleep(5)

    async def get_last_executed_order(self):
        entry_time_ms = int(float(self.strategy.get("entry_time", 0) or 0) * 1000)
        saved_tp_id = int(self.strategy.get("tp_order_id", 0) or 0)
        saved_tp_client = self.strategy.get("tp_client_id", "")
        saved_sl_id = int(self.strategy.get("sl_algo_id", 0) or 0)
        saved_sl_client = self.strategy.get("sl_client_algo_id", "")
        best = None

        try:
            res = await self._request("GET", "/fapi/v1/allOrders", {
                "symbol": self.active_symbol,
                "limit": 50
            }, signed=True)
            if isinstance(res, list):
                for order in res:
                    if order.get("status") != "FILLED":
                        continue
                    oid = int(order.get("orderId", order.get("i", 0)) or 0)
                    cid = str(order.get("clientOrderId", ""))
                    update_time = int(order.get("updateTime", 0) or 0)
                    if update_time <= max(0, entry_time_ms - 5000):
                        continue
                    if (saved_tp_id and oid == saved_tp_id) or (saved_tp_client and cid == saved_tp_client):
                        best = ("TP", update_time)
                        break
        except Exception as e:
            logger.error(f"Ошибка проверки TP ордера: {e}")

        if best is None:
            try:
                res = await self._request("GET", "/fapi/v1/algoOpenOrders", {
                    "symbol": self.active_symbol
                }, signed=True, suppress_error_codes=[404, -2011])
                if isinstance(res, list):
                    for order in res:
                        if order.get("status") not in ["FILLED", "FINISHED"]:
                            continue
                        aid = int(order.get("algoId", order.get("i", 0)) or 0)
                        cid = str(order.get("clientAlgoId", order.get("ca", "")))
                        update_time = int(order.get("updateTime", order.get("T", 0)) or 0)
                        if update_time <= max(0, entry_time_ms - 5000):
                            continue
                        if (saved_sl_id and aid == saved_sl_id) or (saved_sl_client and cid == saved_sl_client):
                            best = ("SL", update_time)
                            break
            except Exception as e:
                logger.error(f"Ошибка проверки SL algo history: {e}")

        if best:
            logger.info(f"🔎 Закрытие определено по идентификатору ордера: {best[0]}")
            return best[0], None

        trade = await self.get_last_closing_trade()
        if trade:
            exit_price = float(trade.get("price", 0) or 0)
            tp_price = float(self.strategy.get("tp_price", 0) or 0)
            sl_price = float(self.strategy.get("sl_price", 0) or 0)

            candidates = []
            if tp_price > 0:
                candidates.append((abs(exit_price - tp_price), "TP"))
            if sl_price > 0:
                candidates.append((abs(exit_price - sl_price), "SL"))

            if candidates:
                candidates.sort(key=lambda x: x[0])
                result = candidates[0][1]
                logger.warning(
                    f"⚠️ ID ордера не найден. Определение по РЕАЛЬНОЙ цене исполнения: "
                    f"exit={exit_price:.8f}, TP={tp_price:.8f}, SL={sl_price:.8f} => {result}"
                )
                return result, trade

        logger.error("❌ Не удалось детерминированно определить TP/SL: нет подтвержденного ордера и closing trade")
        return "UNKNOWN", None

    async def get_last_closing_trade(self):
        entry_time_ms = int(float(self.strategy.get("entry_time", 0) or 0) * 1000)
        side = self.strategy.get("side", "BUY")
        closing_side = "SELL" if side == "BUY" else "BUY"

        try:
            res = await self._request("GET", "/fapi/v1/userTrades", {
                "symbol": self.active_symbol,
                "startTime": max(0, entry_time_ms - 5000),
                "limit": 100
            }, signed=True, weight=5)
            if not isinstance(res, list):
                return None

            candidates = []
            for trade in res:
                t = int(trade.get("time", 0) or 0)
                if t < max(0, entry_time_ms - 5000):
                    continue
                if str(trade.get("side", "")).upper() != closing_side:
                    continue
                qty = abs(float(trade.get("qty", trade.get("baseQty", 0)) or 0))
                if qty <= 0:
                    continue
                candidates.append(trade)

            if not candidates:
                return None
            candidates.sort(key=lambda x: int(x.get("time", 0) or 0), reverse=True)
            return candidates[0]
        except Exception as e:
            logger.error(f"Ошибка получения реального closing trade: {e}")
            return None

    async def calculate_realized_pnl(self, entry_price, exit_price, qty, side):
        entry_time_ms = int(float(self.strategy.get("entry_time", 0) or 0) * 1000)
        closing_side = "SELL" if side == "BUY" else "BUY"

        try:
            res = await self._request("GET", "/fapi/v1/userTrades", {
                "symbol": self.active_symbol,
                "startTime": max(0, entry_time_ms - 5000),
                "limit": 100
            }, signed=True, weight=5)
            if isinstance(res, list):
                realized = 0.0
                commission = 0.0
                exit_qty = 0.0
                exit_notional = 0.0
                last_exit_price = exit_price
                has_exit = False

                for trade in res:
                    t = int(trade.get("time", 0) or 0)
                    if t < max(0, entry_time_ms - 5000):
                        continue
                    trade_side = str(trade.get("side", "")).upper()
                    rp = float(trade.get("realizedPnl", 0) or 0)
                    comm = float(trade.get("commission", 0) or 0)
                    commission += abs(comm)
                    realized += rp

                    if trade_side == closing_side:
                        q = abs(float(trade.get("qty", trade.get("baseQty", 0)) or 0))
                        px = float(trade.get("price", 0) or 0)
                        if q > 0 and px > 0:
                            exit_qty += q
                            exit_notional += q * px
                            last_exit_price = px
                            has_exit = True

                if (has_exit and abs(realized) > 0) or (has_exit and commission > 0):
                    avg_exit = exit_notional / exit_qty if exit_qty > 0 else last_exit_price
                    net = realized - commission
                    notional = abs(entry_price * qty)
                    return {
                        "gross": realized,
                        "fee": commission,
                        "net": net,
                        "percent": (net / notional) * 100 if notional > 0 else 0.0,
                        "exit_price": avg_exit,
                        "exit_qty": exit_qty
                    }
        except Exception as e:
            logger.error(f"Ошибка расчета реального PnL: {e}")

        return None

    async def handle_position_closed(self, close_type=None):
        if self.is_processing_close:
            logger.info("Уже обрабатываем закрытие, пропускаем")
            return
        if self.strategy["state"] != "MONITOR":
            logger.info(f"Стратегия в состоянии {self.strategy['state']}, пропускаем")
            return
        
        self.is_processing_close = True
        try:
            if close_type is None:
                close_type, close_trade = await self.get_last_executed_order()
            else:
                close_trade = None

            if close_type == "UNKNOWN" or close_type is None:
                logger.warning("⏳ TP/SL пока не подтвержден Binance. Повторная проверка через 0.5 сек...")
                await asyncio.sleep(0.5)
                close_type, close_trade = await self.get_last_executed_order()
                if close_type == "UNKNOWN" or close_type is None:
                    logger.error("❌ TP/SL не подтвержден. Состояние стратегии НЕ изменяем.")
                    await send_tg_async(
                        self.session,
                        f"⚠️ <b>ЗАКРЫТИЕ ПОДТВЕРЖДЕНО, НО TP/SL ЕЩЕ НЕ ОПРЕДЕЛЕН [{self.active_symbol}]</b>\n"
                        f"Стратегия ждет подтверждение Binance и не делает ошибочный переворот.",
                        is_error=True,
                        error_key="unknown_close"
                    )
                    return

            logger.info(f"Обработка закрытия позиции: {close_type}")

            side = self.strategy.get("side", "BUY")
            entry_price = float(self.strategy.get("entry_price", 0) or 0)
            qty = float(self.strategy.get("qty", 0) or 0)
            loss_streak = self.strategy.get("loss_streak", 0)

            pnl_data = await self.calculate_realized_pnl(
                entry_price,
                float(close_trade.get("price", 0)) if close_trade else 0.0,
                qty,
                side
            )
            
            if close_type == "TP":
                current_step = loss_streak + 1
                msg = f"✅ <b>ТЕЙК-ПРОФИТ [{self.active_symbol}]!</b>\n"
                msg += f"Колено: #{current_step}/{MAX_STREAK}\n"
                msg += f"Направление: {side}\n"
                if pnl_data:
                    sign = "+" if pnl_data["net"] >= 0 else ""
                    msg += f"💰 PnL: {sign}{pnl_data['net']:.2f} USDC ({sign}{pnl_data['percent']:.2f}%)\n"
                    msg += f"📈 Выход: {pnl_data['exit_price']:.2f} | Gross: {pnl_data['gross']:.2f} | Комиссия: {pnl_data['fee']:.2f} USDC\n"
                    msg += f"💳 Баланс: {await self.get_free_margin():.2f} USDC"
                else:
                    msg += "📊 PnL: данные Binance еще не получены"
                await send_tg_async(self.session, msg)
                
                self.strategy.update({
                    "usdt": USDT_AMOUNT,
                    "loss_streak": 0,
                    "qty": 0.0,
                    "entry_price": 0.0,
                    "tp_order_id": 0,
                    "tp_client_id": "",
                    "sl_algo_id": 0,
                    "sl_client_algo_id": "",
                    "tp_price": 0.0,
                    "sl_price": 0.0,
                    "state": "ENTRY"
                })
                self.state_manager.save()
                
            elif close_type == "SL":
                if loss_streak + 1 >= MAX_STREAK:
                    msg = f"ℹ️ <b>ПОЗИЦИЯ НА ПОСЛЕДНЕМ КОЛЕНЕ ЗАКРЫТА СТОРОННИМ ОБРАЗОМ [{self.active_symbol}]</b>\n"
                    msg += f"Сброс серии и запуск с 1-го колена ({USDT_AMOUNT} USDC)."
                    await send_tg_async(self.session, msg)
                    self.strategy.update({
                        "state": "ENTRY",
                        "loss_streak": 0,
                        "usdt": USDT_AMOUNT,
                        "side": "BUY",
                        "qty": 0.0,
                        "entry_price": 0.0
                    })
                    self.state_manager.save()
                    return
                
                next_usdt = round(self.strategy["usdt"] * MARTINGALE_MULTIPLIER, 2)
                required_margin = self.required_margin_for_notional(next_usdt, LEVERAGE)
                free_margin = await self.get_free_margin()

                logger.info(
                    f"💳 Маржа для переворота: номинал={next_usdt:.2f} USDC / "
                    f"плечо={LEVERAGE}x => требуется ≈{required_margin:.2f} USDC, "
                    f"доступно={free_margin:.2f} USDC"
                )
                
                if free_margin < required_margin:
                    await send_tg_async(
                        self.session,
                        f"⚠️ <b>НЕ ХВАТАЕТ МАРЖИ ДЛЯ ПЕРЕВОРОТА [{self.active_symbol}]</b>\n"
                        f"Требуется: {required_margin:.2f} USDC | Доступно: {free_margin:.2f} USDC\n"
                        f"🔄 Сброс к начальной позиции и ожидание пополнения",
                        is_error=True,
                        error_key="margin_flip_blocked"
                    )
                    self.strategy.update({
                        "state": "ENTRY",
                        "usdt": USDT_AMOUNT,
                        "loss_streak": 0,
                        "side": "BUY",
                        "qty": 0.0,
                        "entry_price": 0.0
                    })
                    self.state_manager.save()
                    return
                
                self.strategy["state"] = "PROCESSING"
                self.strategy["loss_streak"] = loss_streak + 1
                self.state_manager.save()
                
                pos = await self.get_raw_position(retries=5)
                while pos and abs(float(pos.get("positionAmt", 0.0))) != 0:
                    logger.info("Ожидание полного закрытия позиции перед переворотом...")
                    await asyncio.sleep(0.5)
                    pos = await self.get_raw_position(retries=2)
                
                success = await self.execute_flip(side, next_usdt)
                if success:
                    self.strategy["state"] = "MONITOR"
                else:
                    logger.error("Сбой переворота, откат состояния к MONITOR")
                    self.strategy["state"] = "MONITOR"
                self.state_manager.save()
                
            else:
                logger.warning(f"Неизвестный тип закрытия: {close_type}")
                self.strategy.update({
                    "state": "ENTRY",
                    "usdt": USDT_AMOUNT,
                    "loss_streak": 0,
                    "side": "BUY",
                    "qty": 0.0,
                    "entry_price": 0.0
                })
                self.state_manager.save()
        finally:
            self.is_processing_close = False

    async def execute_flip(self, current_side, next_usdt):
        new_side = "SELL" if current_side == "BUY" else "BUY"
        last_price = await self.get_market_data()
        if last_price <= 0:
            logger.error("Невозможно выполнить переворот: цена равна 0")
            return False

        pos = await self.get_raw_position(retries=5)
        if pos is not None and abs(float(pos.get("positionAmt", 0.0))) > 0:
            logger.error("🛡️ ПЕРЕВОРОТ ЗАБЛОКИРОВАН: старая позиция еще не закрыта полностью.")
            return False

        await self.cancel_all_orders()
        new_raw_qty = next_usdt / last_price
        new_qty = format_qty(new_raw_qty, self.step_size)

        res = await self._request("POST", "/fapi/v1/order", {
            "symbol": self.active_symbol,
            "side": new_side,
            "positionSide": "BOTH",
            "type": "MARKET",
            "quantity": new_qty,
            "newClientOrderId": f"flip_{uuid.uuid4().hex[:10]}"
        }, signed=True, weight=1)

        if isinstance(res, dict) and res.get("orderId"):
            order_id = res.get("orderId")
            entry_price = await self.get_actual_entry_price(order_id, side=new_side)
            if entry_price <= 0:
                entry_price = float(res.get("avgPrice", 0))
                if entry_price == 0:
                    entry_price = last_price

            self.strategy.update({
                "side": new_side,
                "usdt": next_usdt,
                "qty": new_qty,
                "entry_price": entry_price,
                "entry_time": time.time()
            })
            self.state_manager.save()

            current_step = self.strategy["loss_streak"] + 1
            is_last_knee = current_step >= MAX_STREAK
            tp_price, sl_price = await self.place_tp_sl_orders(new_side, new_qty, entry_price, is_last_knee)
            formatted_entry = format_price(entry_price, self.tick_size)

            await send_tg_async(
                self.session,
                f"🛑 <b>СТОП-ЛОСС ➔ ПЕРЕВОРОТ [{self.active_symbol}]!</b>\n"
                f"{current_side} → {new_side}\n"
                f"Вход: {formatted_entry} | Объем: {new_qty} {self.base_asset} (~{next_usdt} USDC)\n"
                f"🎯 TP: {tp_price} | 🛑 SL: {sl_price} | Колено: #{current_step}/{MAX_STREAK}"
            )
            return True
        else:
            logger.error(f"Ошибка исполнения переворота: {res}")
            return False

    async def start(self):
        self.session = aiohttp.ClientSession()
        self.active_symbol = SYMBOL
        
        await self.fetch_symbol_info()
        await self.setup_market()
        
        asyncio.create_task(self.ws_ticker_loop())
        asyncio.create_task(self.ws_user_data_loop())
        asyncio.create_task(self.keepalive_listen_key())
        
        has_pos = await self.sync_existing_position()
        self.startup_sync_complete = True

        if has_pos:
            self.adopted_existing_position = True
            self.strategy["state"] = "MONITOR"
            self.state_manager.save()
            logger.info("🛡️ СУЩЕСТВУЮЩАЯ ПОЗИЦИЯ ПОДХВАЧЕНА. ДОПОЛНИТЕЛЬНЫЙ ВХОД ЗАПРЕЩЕН.")
        else:
            if self.strategy["state"] == "MONITOR":
                self.strategy.update({
                    "state": "ENTRY",
                    "usdt": USDT_AMOUNT,
                    "loss_streak": 0,
                    "side": "BUY",
                    "qty": 0.0,
                    "entry_price": 0.0
                })
                self.state_manager.save()
            await send_tg_async(
                self.session,
                f"🤖 <b>БОТ ЗАПУЩЕН [{self.active_symbol}]</b>\n"
                f"{self.active_symbol} | Плечо: {LEVERAGE}x\n"
                f"Старт: {USDT_AMOUNT} USDC | Множитель: {MARTINGALE_MULTIPLIER}x\n"
                f"TP/SL: {TARGET_PERCENT}% | MAX_STREAK: {MAX_STREAK}"
            )

        await self.strategy_loop()

    async def strategy_loop(self):
        while self.is_running:
            await asyncio.sleep(0.1)

            try:
                if not self.startup_sync_complete:
                    continue

                if self.strategy["state"] == "ENTRY":
                    pos = await self.get_raw_position(retries=3)
                    if pos is not None and abs(float(pos.get("positionAmt", 0.0))) > 0:
                        logger.warning("🛡️ В ENTRY обнаружена существующая позиция. НОВЫЙ MARKET-ОРДЕР НЕ ОТПРАВЛЯЕМ. Подхватываем позицию...")
                        self.adopted_existing_position = True
                        await self.sync_existing_position()
                        continue

                    if self.adopted_existing_position:
                        pos = await self.get_raw_position(retries=2)
                        if pos is not None and abs(float(pos.get("positionAmt", 0.0))) > 0:
                            logger.warning("🛡️ Подхваченная позиция все еще существует. Вход заблокирован.")
                            await asyncio.sleep(0.5)
                            continue
                        self.adopted_existing_position = False

                    last_price = await self.get_market_data()
                    if last_price <= 0:
                        continue

                    required_margin = self.required_margin_for_notional(self.strategy["usdt"], LEVERAGE)
                    free_margin = await self.get_free_margin()

                    logger.info(
                        f"💳 Проверка маржи: номинал={self.strategy['usdt']:.2f} USDC / "
                        f"плечо={LEVERAGE}x => требуется ≈{required_margin:.2f} USDC, "
                        f"доступно={free_margin:.2f} USDC"
                    )

                    if free_margin < required_margin:
                        await send_tg_async(
                            self.session,
                            f"⚠️ <b>НЕХВАТКА МАРЖИ ДЛЯ ВХОДА [{self.active_symbol}]</b>\n"
                            f"Требуется: ~{required_margin:.2f} USDC | Доступно: {free_margin:.2f} USDC\n"
                            f"Ожидание пополнения баланса...",
                            is_error=True,
                            error_key="low_margin_entry"
                        )
                        await asyncio.sleep(10)
                        continue

                    pos = await self.get_raw_position(retries=2)
                    if pos and abs(float(pos.get("positionAmt", 0))) > 0:
                        logger.warning("Позиция уже существует. Синхронизация...")
                        await self.sync_existing_position()
                        continue

                    self.strategy["state"] = "PROCESSING"
                    raw_qty = self.strategy["usdt"] / last_price
                    qty = format_qty(raw_qty, self.step_size)
                    side = self.strategy["side"]

                    await self.cancel_all_orders()

                    res = await self._request("POST", "/fapi/v1/order", {
                        "symbol": self.active_symbol,
                        "side": side,
                        "positionSide": "BOTH",
                        "type": "MARKET",
                        "quantity": qty,
                        "newClientOrderId": f"e_{uuid.uuid4().hex[:10]}"
                    }, signed=True, weight=1)

                    if isinstance(res, dict) and res.get("orderId"):
                        order_id = res.get("orderId")
                        entry_price = await self.get_actual_entry_price(order_id, side=side)
                        if entry_price <= 0:
                            entry_price = float(res.get("avgPrice", 0))
                            if entry_price == 0:
                                entry_price = last_price

                        self.strategy.update({
                            "entry_price": entry_price,
                            "qty": qty,
                            "entry_time": time.time()
                        })
                        self.state_manager.save()

                        current_step = self.strategy['loss_streak'] + 1
                        is_last_knee = current_step >= MAX_STREAK
                        tp_price, sl_price = await self.place_tp_sl_orders(side, qty, entry_price, is_last_knee)

                        self.strategy["state"] = "MONITOR"
                        self.state_manager.save()
                        icon = "🟢" if side == "BUY" else "🔻"
                        
                        await send_tg_async(
                            self.session,
                            f"{icon} <b>ОТКРЫТА ПОЗИЦИЯ [{self.active_symbol}] ({side})</b>\n"
                            f"Цена: {entry_price} | Объем: {qty} {self.base_asset} (~{self.strategy['usdt']} USDC)\n"
                            f"🎯 TP: {tp_price} | 🛑 SL: {sl_price} | Колено: #{current_step}/{MAX_STREAK}"
                        )
                    else:
                        logger.error(f"Ошибка открытия позиции: {res}")
                        self.strategy["state"] = "ENTRY"
                        await asyncio.sleep(0.5)

                elif self.strategy["state"] == "MONITOR":
                    now = time.time()
                    if now - self.last_pos_check_time > 2.0:
                        self.last_pos_check_time = now
                        pos = await self.get_raw_position(retries=2)
                        if pos is not None:
                            pos_amt = abs(float(pos.get("positionAmt", 0.0)))
                            if pos_amt == 0:
                                logger.info("🔍 Позиция закрыта, запускаем обработку")
                                if not self.is_processing_close:
                                    asyncio.create_task(self.handle_position_closed())
                            else:
                                self.strategy.update({
                                    "entry_price": float(pos.get("entryPrice", self.strategy.get("entry_price", 0.0))),
                                    "qty": abs(float(pos.get("positionAmt", 0.0)))
                                })
                                self.state_manager.save()

            except Exception as e:
                logger.error(f"Ошибка в цикле strategy_loop: {e}")
                await asyncio.sleep(1)

bot = BinanceMartingaleBot()

@asynccontextmanager
async def lifespan(app: FastAPI):
    loop = asyncio.get_running_loop()
    task = loop.create_task(bot.start())
    yield
    bot.is_running = False
    task.cancel()
    if bot.session:
        await bot.session.close()

app = FastAPI(lifespan=lifespan)

@app.get("/")
@app.get("/health")
async def root():
    return {
        "status": "ok",
        "symbol": bot.active_symbol,
        "latest_price": bot.latest_price,
        "strategy": bot.strategy
    }

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)


























