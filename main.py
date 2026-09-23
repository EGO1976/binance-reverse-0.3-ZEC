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
# ⚙️ ОСНОВНЫЕ НАСТРОЙКИ БОТА (8 КОЛЕН ПО 75 USDT, ДВУСТОРОННИЙ HEDGE MODE)
# =========================================================================
SYMBOL = os.getenv("SYMBOL", "BTCUSDT").strip()
LEVERAGE = int(os.getenv("LEVERAGE", 20))
ORDER_USDT = float(os.getenv("ORDER_USDT", 85.0))  # 8 ордеров * 75 USDT = 600 USDT макс. позиция

BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "").strip().strip("'\"")
BINANCE_API_SECRET = os.getenv("BINANCE_SECRET_KEY", "").strip().strip("'\"")
TG_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip().strip("'\"")
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip().strip("'\"")
STATE_FILE = os.getenv("STATE_FILE", "grid_bot_state.json")

# Процентные шаги сетки от цены Anchor (8 колен)
GRID_OFFSETS = [0.0, 0.8, 2.0, 3.6, 5.8, 8.8, 12.8, 15.0]
PROFIT_TARGET_PCT = 0.5  # Скальп-профит +0.5%
# =========================================================================

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(message)s')
logger = logging.getLogger("GRID_SCALPER")

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

def format_price(price: float, tick_size: float = 0.1) -> str:
    if tick_size <= 0:
        tick_size = 0.1
    precision = max(0, int(round(-math.log10(tick_size))))
    if precision == 0:
        return str(int(round(price)))
    return f"{price:.{precision}f}"

def format_qty(qty: float, step_size: float = 0.001) -> float:
    if step_size <= 0:
        step_size = 0.001
    precision = max(0, int(round(-math.log10(step_size))))
    return round(float(qty), precision)

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
            "LONG": {
                "active": False,
                "anchor_price": 0.0,
                "filled_knee_max": 0,
                "global_tp_order_id": 0,
                "knee_tp_orders": {}  # {knee_idx: order_id}
            },
            "SHORT": {
                "active": False,
                "anchor_price": 0.0,
                "filled_knee_max": 0,
                "global_tp_order_id": 0,
                "knee_tp_orders": {}
            }
        }
        self.load()

    def load(self):
        try:
            with open(self.file_path, 'r') as f:
                saved = json.load(f)
                self.state.update(saved)
                logger.info(f"Состояние загружено из {self.file_path}")
        except FileNotFoundError:
            logger.info("Файл состояния не найден, запуск с начальными настройками")
        except Exception as e:
            logger.error(f"Ошибка загрузки состояния: {e}")

    def save(self):
        try:
            with open(self.file_path, 'w') as f:
                json.dump(self.state, f, indent=2)
        except Exception as e:
            logger.error(f"Ошибка сохранения состояния: {e}")

class GridHedgeBot:
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
        
        self.state_manager = StateManager(STATE_FILE)
        self.state = self.state_manager.state

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
            logger.error("❌ API Ключи не найдены!")
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
                    res = await response.json()
            else:
                final_url = f"{url}?{full_query}" if full_query else url
                async with self.session.request(method, final_url, headers=headers, timeout=5) as response:
                    res = await response.json()

            if isinstance(res, dict) and "code" in res and res["code"] != 200:
                code = res.get("code")
                msg = res.get("msg", "")
                if code not in suppress_error_codes:
                    logger.error(f"Binance API Error [{endpoint}]: Code {code}, Msg: {msg}")
            return res
        except Exception as e:
            logger.error(f"Ошибка REST API [{endpoint}]: {e}")
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

    async def setup_market(self):
        # 1. Включаем Hedge Mode (dualSidePosition = true) для параллельного LONG и SHORT
        await self._request("POST", "/fapi/v1/positionSide/dual", {"dualSidePosition": "true"}, signed=True, suppress_error_codes=[-4059, -4067])
        # 2. Устанавливаем плечо
        await self._request("POST", "/fapi/v1/leverage", {"symbol": self.active_symbol, "leverage": LEVERAGE}, signed=True)
        # 3. Маржа Crossed (Кросс)
        await self._request("POST", "/fapi/v1/marginType", {"symbol": self.active_symbol, "marginType": "CROSSED"}, signed=True, suppress_error_codes=[-4046, -4067])

    async def get_market_data(self):
        if self.latest_price > 0 and (time.time() - self.last_price_time) < 2.0:
            return self.latest_price
        res = await self._request("GET", "/fapi/v1/ticker/price", {"symbol": self.active_symbol})
        if isinstance(res, dict) and "price" in res:
            self.latest_price = float(res["price"])
            self.last_price_time = time.time()
            return self.latest_price
        return 0.0

    async def cancel_side_orders(self, pos_side: str):
        """Отменяет открытые ордера конкретной стороны (LONG или SHORT)"""
        open_orders = await self._request("GET", "/fapi/v1/openOrders", {"symbol": self.active_symbol}, signed=True)
        if isinstance(open_orders, list):
            for o in open_orders:
                if o.get("positionSide") == pos_side:
                    await self._request("DELETE", "/fapi/v1/order", {
                        "symbol": self.active_symbol,
                        "orderId": o.get("orderId")
                    }, signed=True)

    # =========================================================================
    # 🎯 ЛОГИКА ТЕЙК-ПРОФИТОВ И РАСЧЕТА СЕТКИ
    # =========================================================================

    def get_grid_levels(self, anchor_price: float, pos_side: str):
        """Рассчитывает 8 цен входа и скальп-ТейкПрофиты для каждого колена"""
        levels = []
        is_long = (pos_side == "LONG")
        
        for idx, pct in enumerate(GRID_OFFSETS, start=1):
            if is_long:
                entry_p = anchor_price * (1.0 - pct / 100.0)
                scalp_tp_p = entry_p * (1.0 + PROFIT_TARGET_PCT / 100.0)
            else:
                entry_p = anchor_price * (1.0 + pct / 100.0)
                scalp_tp_p = entry_p * (1.0 - PROFIT_TARGET_PCT / 100.0)
                
            qty = format_qty(ORDER_USDT / entry_p, self.step_size)
            levels.append({
                "knee": idx,
                "entry_price": float(format_price(entry_p, self.tick_size)),
                "scalp_tp_price": float(format_price(scalp_tp_p, self.tick_size)),
                "qty": qty
            })
        return levels

    async def update_global_tp(self, pos_side: str):
        """Перерасчитывает и перевыставляет Единый Тейк-Профит пачки (+0.5% от средневзвешенной цены P_avg)"""
        positions = await self._request("GET", "/fapi/v2/positionRisk", {"symbol": self.active_symbol}, signed=True)
        if not isinstance(positions, list):
            return
        
        pos = next((p for p in positions if p.get("positionSide") == pos_side), None)
        if not pos:
            return

        pos_amt = abs(float(pos.get("positionAmt", 0)))
        entry_price = float(pos.get("entryPrice", 0))

        if pos_amt == 0 or entry_price == 0:
            return

        is_long = (pos_side == "LONG")
        if is_long:
            global_tp_p = entry_price * (1.0 + PROFIT_TARGET_PCT / 100.0)
            close_side = "SELL"
        else:
            global_tp_p = entry_price * (1.0 - PROFIT_TARGET_PCT / 100.0)
            close_side = "BUY"

        formatted_tp = format_price(global_tp_p, self.tick_size)

        # 1. Отменяем прошлый Global TP ордер, если он существовал
        old_tp_id = self.state[pos_side].get("global_tp_order_id", 0)
        if old_tp_id:
            await self._request("DELETE", "/fapi/v1/order", {
                "symbol": self.active_symbol,
                "orderId": old_tp_id
            }, signed=True, suppress_error_codes=[-2011])

        # 2. Выставляем новый Лимитный Тейк-Профит на весь объём текущей позиции
        res = await self._request("POST", "/fapi/v1/order", {
            "symbol": self.active_symbol,
            "side": close_side,
            "positionSide": pos_side,
            "type": "LIMIT",
            "timeInForce": "GTC",
            "quantity": pos_amt,
            "price": formatted_tp,
            "reduceOnly": "true",
            "newClientOrderId": f"gtp_{pos_side[:1]}_{uuid.uuid4().hex[:8]}"
        }, signed=True)

        if isinstance(res, dict) and res.get("orderId"):
            self.state[pos_side]["global_tp_order_id"] = res["orderId"]
            self.state_manager.save()
            logger.info(f"🎯 [{pos_side}] Обновлен Global TP: {formatted_tp} на объем {pos_amt}")

    async def place_grid_orders(self, pos_side: str, anchor_price: float):
        """Размещает первичную сетку лимитных ордеров (1-й сразу по маркету, 2-8 лимитками)"""
        await self.cancel_side_orders(pos_side)
        levels = self.get_grid_levels(anchor_price, pos_side)
        
        is_long = (pos_side == "LONG")
        entry_side = "BUY" if is_long else "SELL"

        # 1-й ордер исполняем по MARKET
        k1 = levels[0]
        res_m = await self._request("POST", "/fapi/v1/order", {
            "symbol": self.active_symbol,
            "side": entry_side,
            "positionSide": pos_side,
            "type": "MARKET",
            "quantity": k1["qty"],
            "newClientOrderId": f"m1_{pos_side[:1]}_{uuid.uuid4().hex[:8]}"
        }, signed=True)

        if not (isinstance(res_m, dict) and res_m.get("orderId")):
            logger.error(f"❌ Не удалось открыть маркет-ордер 1-го колена для {pos_side}: {res_m}")
            return False

        # Выставляем оставшиеся 7 колен лимитками
        for lvl in levels[1:]:
            await self._request("POST", "/fapi/v1/order", {
                "symbol": self.active_symbol,
                "side": entry_side,
                "positionSide": pos_side,
                "type": "LIMIT",
                "timeInForce": "GTC",
                "quantity": lvl["qty"],
                "price": str(lvl["entry_price"]),
                "newClientOrderId": f"k{lvl['knee']}_{pos_side[:1]}_{uuid.uuid4().hex[:8]}"
            }, signed=True)

        self.state[pos_side].update({
            "active": True,
            "anchor_price": anchor_price,
            "filled_knee_max": 1,
            "knee_tp_orders": {}
        })
        self.state_manager.save()

        # Выставляем Тейк-Профит на 1-е колено
        await self.update_global_tp(pos_side)
        
        icon = "🟢" if is_long else "🔻"
        await send_tg_async(
            self.session,
            f"{icon} <b>ЗАПУЩЕНА СЕТКА {pos_side} [{self.active_symbol}]</b>\n"
            f"Anchor-цена: {anchor_price}\n"
            f"Колен: 8 по {ORDER_USDT} USDT (Макс: 600 USDT)\n"
            f"Плечо: {LEVERAGE}x | Скальп-Цель: +{PROFIT_TARGET_PCT}%"
        )
        return True

    # =========================================================================
    # ⚡ ОБРАБОТКА WEBSOCKET СОБЫТИЙ СЕТКИ И ОРДЕРОВ
    # =========================================================================

    async def handle_order_update(self, order_data: dict):
        status = order_data.get("X")
        if status != "FILLED":
            return

        pos_side = order_data.get("ps")  # LONG или SHORT
        if pos_side not in ["LONG", "SHORT"]:
            return

        client_id = str(order_data.get("c", ""))
        price = float(order_data.get("L", 0) or order_data.get("p", 0))
        qty = float(order_data.get("l", 0) or order_data.get("q", 0))

        # 1. Сработало одно из колен сетки (покупка в LONG или продажа в SHORT)
        if client_id.startswith("k") or client_id.startswith("m1"):
            knee_idx = int(client_id[1]) if client_id.startswith("k") else 1
            logger.info(f"📥 [{pos_side}] Сработало колено #{knee_idx} по цене {price}")

            # Пересчитываем Global TP позиции
            await self.update_global_tp(pos_side)

            # Выставляем индивидуальный Скальп-TP для этого колена
            is_long = (pos_side == "LONG")
            close_side = "SELL" if is_long else "BUY"
            scalp_tp_price = price * (1.0 + PROFIT_TARGET_PCT / 100.0) if is_long else price * (1.0 - PROFIT_TARGET_PCT / 100.0)
            formatted_scalp_tp = format_price(scalp_tp_price, self.tick_size)

            tp_res = await self._request("POST", "/fapi/v1/order", {
                "symbol": self.active_symbol,
                "side": close_side,
                "positionSide": pos_side,
                "type": "LIMIT",
                "timeInForce": "GTC",
                "quantity": qty,
                "price": formatted_scalp_tp,
                "reduceOnly": "true",
                "newClientOrderId": f"stp_k{knee_idx}_{uuid.uuid4().hex[:6]}"
            }, signed=True)

            if isinstance(tp_res, dict) and tp_res.get("orderId"):
                self.state[pos_side]["knee_tp_orders"][str(knee_idx)] = tp_res["orderId"]
                self.state_manager.save()

            await send_tg_async(
                self.session,
                f"📥 <b>[{pos_side}] НАБОР КОЛЕНА #{knee_idx}</b>\n"
                f"Цена входа: {price}\n"
                f"Скальп-TP колена: {formatted_scalp_tp}"
            )

        # 2. Сработал Индивидуальный Скальп-TP колена
        elif client_id.startswith("stp_k"):
            knee_idx = int(client_id[5])
            logger.info(f"💰 [{pos_side}] Закрыто по Скальп-TP колено #{knee_idx}")

            # Пересчитываем Единый Тейк
            await self.update_global_tp(pos_side)

            # Перевыставляем лимитку этого колена обратно в стакан
            anchor_p = self.state[pos_side]["anchor_price"]
            levels = self.get_grid_levels(anchor_p, pos_side)
            target_lvl = next((l for l in levels if l["knee"] == knee_idx), None)

            if target_lvl:
                is_long = (pos_side == "LONG")
                entry_side = "BUY" if is_long else "SELL"
                await self._request("POST", "/fapi/v1/order", {
                    "symbol": self.active_symbol,
                    "side": entry_side,
                    "positionSide": pos_side,
                    "type": "LIMIT",
                    "timeInForce": "GTC",
                    "quantity": target_lvl["qty"],
                    "price": str(target_lvl["entry_price"]),
                    "newClientOrderId": f"k{knee_idx}_{pos_side[:1]}_{uuid.uuid4().hex[:8]}"
                }, signed=True)

            await send_tg_async(
                self.session,
                f"💰 <b>[{pos_side}] СКАЛЬП-ПРОФИТ КОЛЕНА #{knee_idx}!</b>\n"
                f"Цена закрытия: {price} (+{PROFIT_TARGET_PCT}%)\n"
                f"🔄 Лимитка колена #{knee_idx} возвращена в стакан."
            )

        # 3. Сработал Единый Global TP (вся позиция закрыта полностью)
        elif client_id.startswith("gtp"):
            logger.info(f"🎉 [{pos_side}] СРАБОТАЛ ЕДИНЫЙ ТЕЙК-ПРОФИТ ВСЕЙ СЕТКИ!")
            await self.cancel_side_orders(pos_side)
            
            self.state[pos_side].update({
                "active": False,
                "anchor_price": 0.0,
                "filled_knee_max": 0,
                "global_tp_order_id": 0,
                "knee_tp_orders": {}
            })
            self.state_manager.save()

            await send_tg_async(
                self.session,
                f"🎉 <b>[{pos_side}] ВСЯ СЕТКА ЗАКРЫТА ПО ЕДИНОМУ TP!</b>\n"
                f"Цена: {price}\n"
                f"🔄 Сетка полностью перезапускается от новой текущей цены..."
            )

            # Перезапускаем сетку от новой текущей цены
            new_anchor = await self.get_market_data()
            if new_anchor > 0:
                await self.place_grid_orders(pos_side, new_anchor)

    # =========================================================================
    # 🔄 ФОНОВЫЕ ПОТОКИ И WEBSOCKETS
    # =========================================================================

    async def get_listen_key(self):
        res = await self._request("POST", "/fapi/v1/listenKey", signed=True)
        if isinstance(res, dict) and "listenKey" in res:
            self.listen_key = res["listenKey"]
            return self.listen_key
        return None

    async def keepalive_listen_key(self):
        while self.is_running:
            await asyncio.sleep(1500)
            if self.listen_key:
                await self._request("PUT", "/fapi/v1/listenKey", signed=True)

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
                        data = ujson.loads(msg)
                        if data.get("e") == "ORDER_TRADE_UPDATE":
                            await self.handle_order_update(data.get("o", {}))
            except Exception as e:
                logger.warning(f"Ошибка UserStream WS: {e}. Переподключение...")
                await asyncio.sleep(5)

    async def start(self):
        self.session = aiohttp.ClientSession()
        await self.fetch_symbol_info()
        await self.setup_market()

        asyncio.create_task(self.ws_user_data_loop())
        asyncio.create_task(self.keepalive_listen_key())

        curr_price = await self.get_market_data()
        if curr_price <= 0:
            logger.error("❌ Не удалось получить цену с биржи для старта")
            return

        # Запуск сетки LONG, если еще не активна
        if not self.state["LONG"]["active"]:
            await self.place_grid_orders("LONG", curr_price)

        # Запуск сетки SHORT, если еще не активна
        if not self.state["SHORT"]["active"]:
            await self.place_grid_orders("SHORT", curr_price)

        await send_tg_async(
            self.session,
            f"🤖 <b>ДВУСТОРОННИЙ СЕТОЧНЫЙ БОТ УСПЕШНО ЗАПУЩЕН [{self.active_symbol}]</b>\n"
            f"Плечо: {LEVERAGE}x | Режим: Hedge Mode (LONG + SHORT)\n"
            f"Объем колена: {ORDER_USDT} USDT | Шагов: 8 (до 15%)"
        )

bot = GridHedgeBot()

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
        "state": bot.state
    }

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)


























