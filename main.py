import asyncio
import os
import time
import math
import json
import aiohttp
import ccxt.async_support as ccxt
import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI

# ==============================================================================
# 🎛️ ПАНЕЛЬ УПРАВЛЕНИЯ ПАРАМЕТРАМИ БОТА (HEDGE MODE / USDC)
# ==============================================================================

SYMBOL_RAW = os.getenv('SYMBOL', 'ZEC/USDC:USDC').strip()
SYMBOL = SYMBOL_RAW
SYMBOL_DISPLAY = SYMBOL_RAW.split(':')[0] if ':' in SYMBOL_RAW else SYMBOL_RAW

LEVERAGE = int(os.getenv('LEVERAGE', 50))
BASE_ORDER_USDC = float(os.getenv('LONG_USDT_AMOUNT', 100.0))

GRID_LEVELS = 3
VISIBLE_GRID_LEVELS = 1

STATE_FILE = os.getenv('STATE_FILE', 'bot_state.json')


def get_grid_step_percent(step: int) -> float:
    if step == 1:
        return 5.0
    elif step == 2:
        return 10.0
    elif step == 3:
        return 15.0
    return 15.0


def get_step_usdc_volume(step: int) -> float:
    if step <= 1:
        return float(BASE_ORDER_USDC)
    elif step == 2:
        return float(BASE_ORDER_USDC * 2.0)
    elif step == 3:
        return float(BASE_ORDER_USDC * 4.0)
    return float(BASE_ORDER_USDC * 4.0)


WINNER_TAKE_PROFIT_PCT = 0.5

TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN', '').strip()
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', '').strip()

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(message)s')
logger = logging.getLogger("CCXT_USDC_HEDGE_BOT")


async def send_tg_message(session: aiohttp.ClientSession, text: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID or session is None or session.closed:
        print(f"💬 [TG Log]: {text}")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        'chat_id': TELEGRAM_CHAT_ID,
        'text': text,
        'parse_mode': 'HTML',
        'disable_web_page_preview': True
    }
    try:
        async with session.post(url, json=payload, timeout=5) as resp:
            pass
    except Exception as e:
        print(f"❌ Ошибка Telegram: {e}")


class CCXTUsdcHedgeBot:
    def __init__(self):
        self.exchange = None
        self.market_info = None
        self.http_session = None
        self.is_running = True

        self.long_entry_price = 0.0
        self.short_entry_price = 0.0

        self.initial_long_entry = 0.0
        self.initial_short_entry = 0.0

        self.long_contracts_prev = 0.0
        self.short_contracts_prev = 0.0

        self.long_loser_step = 0
        self.short_loser_step = 0

        self.active_limit_orders = {}
        self.long_tp_order_id = None
        self.short_tp_order_id = None

        self.last_long_tp_time = 0.0
        self.last_short_tp_time = 0.0

        self.exec_lock = asyncio.Lock()

    # ============================ STATE ============================

    def save_state(self):
        try:
            with open(STATE_FILE, 'w') as f:
                json.dump({
                    'long_entry_price': self.long_entry_price,
                    'short_entry_price': self.short_entry_price,
                    'initial_long_entry': self.initial_long_entry,
                    'initial_short_entry': self.initial_short_entry,
                    'long_loser_step': self.long_loser_step,
                    'short_loser_step': self.short_loser_step,
                }, f)
        except Exception as e:
            logger.error(f"⚠️ save_state: {e}")

    def load_state(self):
        try:
            if os.path.exists(STATE_FILE):
                with open(STATE_FILE) as f:
                    data = json.load(f)
                self.initial_long_entry = float(data.get('initial_long_entry', 0.0) or 0.0)
                self.initial_short_entry = float(data.get('initial_short_entry', 0.0) or 0.0)
                self.long_loser_step = int(data.get('long_loser_step', 0) or 0)
                self.short_loser_step = int(data.get('short_loser_step', 0) or 0)
                logger.info(f"📂 state загружен: L step={self.long_loser_step}, S step={self.short_loser_step}")
        except Exception as e:
            logger.error(f"⚠️ load_state: {e}")

    def _infer_step_from_position(self, contracts: float, avg_entry: float) -> int:
        """Определяет шаг усреднения по объёму позиции.
        Теоретически contracts / (U/avg) = 2^(step+1) - 1 → 1, 3, 7, 15."""
        if avg_entry <= 0 or contracts <= 0:
            return 0
        base_qty = BASE_ORDER_USDC / avg_entry
        if base_qty <= 0:
            return 0
        ratio = contracts / base_qty
        if ratio < 2:   return 0
        if ratio < 5:   return 1
        if ratio < 11:  return 2
        return 3

    def _infer_initial_entry(self, avg_entry: float, step: int) -> float:
        """Восстанавливает первую цену входа из средней и шага усреднения.
        Коэффициенты получены из формулы средневзвешенной цены."""
        coef = {0: 1.0, 1: 0.9661, 2: 0.9273, 3: 0.8845}.get(step, 1.0)
        return avg_entry / coef if coef > 0 else avg_entry

    # ============================ INIT ============================

    def init_exchange(self):
        raw_key = os.getenv('BINANCE_API_KEY', '').strip().strip("'").strip('"')
        raw_secret = os.getenv('BINANCE_SECRET_KEY', '').strip().strip('"').strip("'")

        return ccxt.binanceusdm({
            'apiKey': raw_key,
            'secret': raw_secret,
            'enableRateLimit': True,
            'rateLimit': 300,
            'timeout': 10000,
            'options': {
                'defaultType': 'future',
                'adjustForTimeDifference': True
            }
        })

    async def safe_close_exchange(self):
        if self.exchange:
            try:
                await self.exchange.close()
            except Exception:
                pass
            self.exchange = None
        if self.http_session and not self.http_session.closed:
            await self.http_session.close()

    async def initialize(self):
        if self.exchange is None:
            self.exchange = self.init_exchange()
        if self.http_session is None or self.http_session.closed:
            self.http_session = aiohttp.ClientSession()

        if self.market_info is None:
            markets = await self.exchange.load_markets()
            if SYMBOL in markets:
                self.market_info = markets[SYMBOL]
            else:
                raise KeyError(f"Символ {SYMBOL} не найден на бирже.")

        try:
            await self.exchange.set_leverage(LEVERAGE, SYMBOL)
            try:
                await self.exchange.set_margin_mode('CROSSED', SYMBOL)
            except Exception:
                pass
            await self.exchange.set_position_mode(hedged=True, symbol=SYMBOL)
        except Exception:
            pass

        logger.info(f"✅ Инициализация выполнена для {SYMBOL_DISPLAY}. Плечо {LEVERAGE}x CROSSED (Hedge Mode, USDC).")

    # ============================ HELPERS ============================

    def round_amount(self, amount):
        try:
            precision = self.market_info.get('precision', {}).get('amount')
            if precision is not None:
                if isinstance(precision, int):
                    return round(amount, precision)
                elif isinstance(precision, float) and precision > 0:
                    precision_digits = abs(int(math.log10(precision)))
                    return round(amount, precision_digits)
            return float(self.exchange.amount_to_precision(SYMBOL, amount))
        except Exception:
            return round(amount, 3)

    def round_price(self, price):
        try:
            return float(self.exchange.price_to_precision(SYMBOL, price))
        except Exception:
            return round(price, 2)

    def calculate_qty_for_usdc(self, usdc_volume, price):
        if price <= 0:
            return 0.0

        raw_qty = usdc_volume / price
        precision = self.market_info.get('precision', {}).get('amount')
        digits = 3
        if isinstance(precision, int):
            digits = precision
        elif isinstance(precision, float) and precision > 0:
            digits = abs(int(math.log10(precision)))

        qty = round(raw_qty, digits)
        limits = self.market_info.get('limits', {})
        min_qty = limits.get('amount', {}).get('min', 0.001)
        if qty < min_qty:
            qty = round(min_qty, digits)

        return qty

    async def get_actual_position_entry_price(self, position_side):
        for _ in range(5):
            try:
                positions = await self.exchange.fetch_positions([SYMBOL])
                pos = next((p for p in positions if p['side'] == position_side.lower() and float(p['contracts']) > 0), None)
                if pos:
                    return float(pos['entryPrice']), float(pos['contracts'])
            except Exception as e:
                logger.error(f"Ошибка получения позиции {position_side}: {e}")
            await asyncio.sleep(0.5)
        return 0.0, 0.0

    # ============================ PnL ============================

    async def fetch_tp_pnl(self, tp_order_id: str = None):
        """Возвращает PnL и комиссию по конкретному TP-ордеру (или последнему закрытому).
        Primary: fetch_my_trades отфильтрованные по orderId.
        Fallback: fetch_closed_orders с cumRealizedPnl."""
        result = {'gross': 0.0, 'fee': 0.0, 'net': 0.0, 'balance': 0.0}

        # Баланс
        try:
            bal = await self.exchange.fetch_balance()
            usdc_total = float(bal.get('USDC', {}).get('total', 0.0) or 0.0)
            usdc_free = float(bal.get('USDC', {}).get('free', 0.0) or 0.0)
            result['balance'] = usdc_total if usdc_total > 0 else usdc_free
        except Exception:
            pass

        for _ in range(6):
            await asyncio.sleep(1.5)
            try:
                gross, fee = 0.0, 0.0
                found = False

                if tp_order_id:
                    try:
                        trades = await self.exchange.fetch_my_trades(SYMBOL, limit=100)
                    except Exception:
                        trades = []
                    matched = [t for t in trades if str(t.get('order')) == str(tp_order_id)]
                    if matched:
                        for t in matched:
                            info = t.get('info', {})
                            gross += float(info.get('realizedPnl', 0.0) or 0.0)
                            fee_cost = t.get('fee', {}).get('cost')
                            if fee_cost is None:
                                fee_cost = info.get('commission', 0.0)
                            fee += abs(float(fee_cost or 0.0))
                        found = True

                if not found:
                    try:
                        orders = await self.exchange.fetch_closed_orders(SYMBOL, limit=10)
                    except Exception:
                        orders = []
                    for o in orders:
                        info = o.get('info', {})
                        pnl = float(info.get('cumRealizedPnl', 0.0) or 0.0)
                        if pnl != 0.0:
                            gross = pnl
                            fee = abs(float(info.get('cumCommission', 0.0) or 0.0))
                            found = True
                            break

                if found:
                    result.update(gross=gross, fee=fee, net=gross - fee)
                    return result
            except Exception as e:
                logger.error(f"⚠️ fetch_tp_pnl: {e}")
        return result

    # ============================ AUDIT ============================

    async def audit_and_sync_all_orders(self):
        try:
            open_orders = await self.exchange.fetch_open_orders(SYMBOL)
            positions = await self.exchange.fetch_positions([SYMBOL])

            long_pos = next((p for p in positions if p['side'] == 'long' and float(p['contracts']) > 0), None)
            short_pos = next((p for p in positions if p['side'] == 'short' and float(p['contracts']) > 0), None)

            tp_long, tp_short = None, None
            grids_long, grids_short = {}, {}

            for o in open_orders:
                ps = str(o.get('info', {}).get('positionSide', '')).upper()
                sd = str(o['side']).upper()
                if ps == 'LONG' and sd == 'SELL':
                    tp_long = o
                elif ps == 'SHORT' and sd == 'BUY':
                    tp_short = o
                elif ps == 'LONG' and sd == 'BUY':
                    grids_long[str(o['id'])] = o
                elif ps == 'SHORT' and sd == 'SELL':
                    grids_short[str(o['id'])] = o

            SIZE_TOL = 0.03  # 3%

            def size_mismatch(order, expected_qty):
                try:
                    actual = float(order.get('amount') or 0)
                    if expected_qty <= 0 or actual <= 0:
                        return True
                    return abs(actual - expected_qty) / expected_qty > SIZE_TOL
                except Exception:
                    return True

            # --- ORPHAN: нет позиции, но есть ордера этой стороны → отменить ---
            if not long_pos:
                for oid in list(grids_long.keys()) + ([str(tp_long['id'])] if tp_long else []):
                    try:
                        await self.exchange.cancel_order(oid, SYMBOL)
                    except Exception:
                        pass
                self.long_tp_order_id = None
                grids_long.clear()
            if not short_pos:
                for oid in list(grids_short.keys()) + ([str(tp_short['id'])] if tp_short else []):
                    try:
                        await self.exchange.cancel_order(oid, SYMBOL)
                    except Exception:
                        pass
                self.short_tp_order_id = None
                grids_short.clear()

            self.active_limit_orders.clear()

            # ===================== LONG =====================
            if long_pos:
                l_contracts = float(long_pos['contracts'])
                l_entry = float(long_pos['entryPrice'])

                # 1) TP LONG
                if tp_long and size_mismatch(tp_long, l_contracts):
                    try:
                        await self.exchange.cancel_order(str(tp_long['id']), SYMBOL)
                        logger.info(f"🔁 TP_LONG отменён: размер {tp_long.get('amount')} ≠ позиции {l_contracts}")
                        tp_long = None
                        self.long_tp_order_id = None
                    except Exception as e:
                        logger.error(f"❌ cancel TP_LONG: {e}")

                if tp_long:
                    self.long_tp_order_id = str(tp_long['id'])
                    logger.info(f"🟢 TP_LONG подхвачен (size={tp_long.get('amount')} @ ${float(tp_long['price']):,.2f})")
                elif not self.long_tp_order_id:
                    tp_price = self.round_price(l_entry * (1.0 + (WINNER_TAKE_PROFIT_PCT / 100.0)))
                    try:
                        n = await self.exchange.create_order(
                            SYMBOL, 'LIMIT', 'sell', self.round_amount(l_contracts), tp_price,
                            params={'positionSide': 'LONG', 'timeInForce': 'GTC'})
                        self.long_tp_order_id = str(n['id'])
                        logger.info(f"📥 TP_LONG создан @ ${tp_price:,.2f} size={self.round_amount(l_contracts)}")
                    except Exception as e:
                        logger.error(f"❌ create TP_LONG: {e}")

                # 2) Grid LONG
                next_step = self.long_loser_step + 1
                if next_step <= GRID_LEVELS:
                    if grids_long:
                        gid, gobj = list(grids_long.items())[0]
                        expected_qty = self.calculate_qty_for_usdc(
                            get_step_usdc_volume(next_step), float(gobj['price']) or l_entry)
                        if size_mismatch(gobj, expected_qty):
                            try:
                                await self.exchange.cancel_order(gid, SYMBOL)
                                logger.info(f"🔁 Grid_LONG#{next_step} отменён: размер {gobj.get('amount')} ≠ ожидаемого {expected_qty}")
                                grids_long.clear()
                            except Exception as e:
                                logger.error(f"❌ cancel grid_LONG: {e}")
                        else:
                            self.active_limit_orders[f"LONG_{next_step}"] = gid
                            logger.info(f"🟢 Grid_LONG#{next_step} подхвачен (size={gobj.get('amount')})")

                    if not grids_long:
                        dist = get_grid_step_percent(next_step)
                        gprice = self.round_price(self.initial_long_entry * (1.0 - (dist / 100.0)))
                        gqty = self.calculate_qty_for_usdc(get_step_usdc_volume(next_step), gprice)
                        try:
                            n = await self.exchange.create_order(
                                SYMBOL, 'LIMIT', 'buy', gqty, gprice,
                                params={'positionSide': 'LONG', 'timeInForce': 'GTC'})
                            self.active_limit_orders[f"LONG_{next_step}"] = str(n['id'])
                            logger.info(f"📥 Grid_LONG#{next_step} создан @ ${gprice:,.2f} size={gqty}")
                        except Exception as e:
                            logger.error(f"❌ create grid_LONG: {e}")
                else:
                    logger.info(f"ℹ️ LONG: усреднение достигло предела ({GRID_LEVELS}), сетку не ставим")

            # ===================== SHORT =====================
            if short_pos:
                s_contracts = float(short_pos['contracts'])
                s_entry = float(short_pos['entryPrice'])

                if tp_short and size_mismatch(tp_short, s_contracts):
                    try:
                        await self.exchange.cancel_order(str(tp_short['id']), SYMBOL)
                        logger.info(f"🔁 TP_SHORT отменён: размер {tp_short.get('amount')} ≠ позиции {s_contracts}")
                        tp_short = None
                        self.short_tp_order_id = None
                    except Exception as e:
                        logger.error(f"❌ cancel TP_SHORT: {e}")

                if tp_short:
                    self.short_tp_order_id = str(tp_short['id'])
                    logger.info(f"🔻 TP_SHORT подхвачен (size={tp_short.get('amount')} @ ${float(tp_short['price']):,.2f})")
                elif not self.short_tp_order_id:
                    tp_price = self.round_price(s_entry * (1.0 - (WINNER_TAKE_PROFIT_PCT / 100.0)))
                    try:
                        n = await self.exchange.create_order(
                            SYMBOL, 'LIMIT', 'buy', self.round_amount(s_contracts), tp_price,
                            params={'positionSide': 'SHORT', 'timeInForce': 'GTC'})
                        self.short_tp_order_id = str(n['id'])
                        logger.info(f"📥 TP_SHORT создан @ ${tp_price:,.2f} size={self.round_amount(s_contracts)}")
                    except Exception as e:
                        logger.error(f"❌ create TP_SHORT: {e}")

                next_step = self.short_loser_step + 1
                if next_step <= GRID_LEVELS:
                    if grids_short:
                        gid, gobj = list(grids_short.items())[0]
                        expected_qty = self.calculate_qty_for_usdc(
                            get_step_usdc_volume(next_step), float(gobj['price']) or s_entry)
                        if size_mismatch(gobj, expected_qty):
                            try:
                                await self.exchange.cancel_order(gid, SYMBOL)
                                logger.info(f"🔁 Grid_SHORT#{next_step} отменён: размер не совпадает")
                                grids_short.clear()
                            except Exception as e:
                                logger.error(f"❌ cancel grid_SHORT: {e}")
                        else:
                            self.active_limit_orders[f"SHORT_{next_step}"] = gid
                            logger.info(f"🔻 Grid_SHORT#{next_step} подхвачен")
                    if not grids_short:
                        dist = get_grid_step_percent(next_step)
                        gprice = self.round_price(self.initial_short_entry * (1.0 + (dist / 100.0)))
                        gqty = self.calculate_qty_for_usdc(get_step_usdc_volume(next_step), gprice)
                        try:
                            n = await self.exchange.create_order(
                                SYMBOL, 'LIMIT', 'sell', gqty, gprice,
                                params={'positionSide': 'SHORT', 'timeInForce': 'GTC'})
                            self.active_limit_orders[f"SHORT_{next_step}"] = str(n['id'])
                            logger.info(f"📥 Grid_SHORT#{next_step} создан @ ${gprice:,.2f} size={gqty}")
                        except Exception as e:
                            logger.error(f"❌ create grid_SHORT: {e}")
                else:
                    logger.info(f"ℹ️ SHORT: усреднение достигло предела ({GRID_LEVELS}), сетку не ставим")

        except Exception as e:
            logger.error(f"❌ audit_and_sync_all_orders: {e}")

    # ============================ RECOVER ============================

    async def recover_state_or_init(self):
        async with self.exec_lock:
            logger.info(f"🔄 Сканирование биржи и подхват позиций для {SYMBOL_DISPLAY}...")
            self.load_state()

            positions = []
            for _ in range(3):
                try:
                    positions = await self.exchange.fetch_positions([SYMBOL])
                    break
                except Exception as e:
                    logger.warning(f"⚠️ Ошибка запроса позиций: {e}")
                    await asyncio.sleep(1)

            ticker = await self.exchange.fetch_ticker(SYMBOL)
            current_price = float(ticker['last'])

            long_pos = next((p for p in positions if p['side'] == 'long' and float(p['contracts']) > 0), None)
            short_pos = next((p for p in positions if p['side'] == 'short' and float(p['contracts']) > 0), None)

            # --- LONG ---
            if long_pos:
                contracts = float(long_pos['contracts'])
                avg = float(long_pos['entryPrice'])
                inferred_step = self._infer_step_from_position(contracts, avg)

                if self.initial_long_entry > 0 and self.long_loser_step == inferred_step:
                    logger.info(f"📂 LONG state OK: step={inferred_step}, init=${self.initial_long_entry:,.2f}")
                else:
                    self.long_loser_step = inferred_step
                    self.initial_long_entry = self._infer_initial_entry(avg, inferred_step)
                    logger.warning(f"⚠️ LONG state скорректирован → step={inferred_step}, init≈${self.initial_long_entry:,.2f}")

                self.long_entry_price = avg
                self.long_contracts_prev = contracts
                logger.info(f"🟢 [{SYMBOL_DISPLAY}] LONG подхвачен: {contracts} | Avg: ${avg:,.2f} | Step: {inferred_step}")
                await send_tg_message(self.http_session,
                    f"🟢 <b>[{SYMBOL_DISPLAY}] LONG подхвачен</b>\n"
                    f"Avg: ${avg:,.2f} | Step: {inferred_step}/{GRID_LEVELS} | Объём: {contracts}")
            else:
                self.long_loser_step = 0
                self.initial_long_entry = 0.0
                logger.info(f"🟢 [{SYMBOL_DISPLAY}] LONG не найден. Открываем базу...")
                base_qty = self.calculate_qty_for_usdc(BASE_ORDER_USDC, current_price)
                await self.exchange.create_order(SYMBOL, 'MARKET', 'buy', base_qty, params={'positionSide': 'LONG'})
                l_entry, l_ct = await self.get_actual_position_entry_price('LONG')
                self.long_entry_price = l_entry if l_entry > 0 else current_price
                self.initial_long_entry = self.long_entry_price
                self.long_contracts_prev = l_ct if l_ct > 0 else base_qty
                await send_tg_message(self.http_session,
                    f"🟢 <b>[{SYMBOL_DISPLAY}] ОТКРЫТА НОВАЯ ПОЗИЦИЯ (LONG)</b>\nЦена: ${self.long_entry_price:,.2f}")

            # --- SHORT ---
            if short_pos:
                contracts = float(short_pos['contracts'])
                avg = float(short_pos['entryPrice'])
                inferred_step = self._infer_step_from_position(contracts, avg)

                if self.initial_short_entry > 0 and self.short_loser_step == inferred_step:
                    logger.info(f"📂 SHORT state OK: step={inferred_step}")
                else:
                    self.short_loser_step = inferred_step
                    self.initial_short_entry = self._infer_initial_entry(avg, inferred_step)
                    logger.warning(f"⚠️ SHORT state скорректирован → step={inferred_step}, init≈${self.initial_short_entry:,.2f}")

                self.short_entry_price = avg
                self.short_contracts_prev = contracts
                logger.info(f"🔻 [{SYMBOL_DISPLAY}] SHORT подхвачен: {contracts} | Avg: ${avg:,.2f} | Step: {inferred_step}")
                await send_tg_message(self.http_session,
                    f"🔻 <b>[{SYMBOL_DISPLAY}] SHORT подхвачен</b>\n"
                    f"Avg: ${avg:,.2f} | Step: {inferred_step}/{GRID_LEVELS} | Объём: {contracts}")
            else:
                self.short_loser_step = 0
                self.initial_short_entry = 0.0
                logger.info(f"🔻 [{SYMBOL_DISPLAY}] SHORT не найден. Открываем базу...")
                base_qty = self.calculate_qty_for_usdc(BASE_ORDER_USDC, current_price)
                await self.exchange.create_order(SYMBOL, 'MARKET', 'sell', base_qty, params={'positionSide': 'SHORT'})
                s_entry, s_ct = await self.get_actual_position_entry_price('SHORT')
                self.short_entry_price = s_entry if s_entry > 0 else current_price
                self.initial_short_entry = self.short_entry_price
                self.short_contracts_prev = s_ct if s_ct > 0 else base_qty
                await send_tg_message(self.http_session,
                    f"🔻 <b>[{SYMBOL_DISPLAY}] ОТКРЫТА НОВАЯ ПОЗИЦИЯ (SHORT)</b>\nЦена: ${self.short_entry_price:,.2f}")

            self.save_state()
            await self.audit_and_sync_all_orders()

    # ============================ REOPEN ============================

    async def reopen_winner_position(self, current_price, winner_side):
        async with self.exec_lock:
            triggered_step = self.long_loser_step if winner_side == 'LONG' else self.short_loser_step
            step_desc = "Базовый ордер" if triggered_step == 0 else f"Усреднение №{triggered_step}"

            # 💡 Захватываем ID TP ДО сброса (для корректного PnL)
            captured_tp_id = self.long_tp_order_id if winner_side == 'LONG' else self.short_tp_order_id

            # 🔒 Сброс state ДО отправки маркета (защита от двойного срабатывания)
            if winner_side == 'LONG':
                self.long_tp_order_id = None
                self.long_contracts_prev = 0
                self.long_loser_step = 0
                self.initial_long_entry = 0
            else:
                self.short_tp_order_id = None
                self.short_contracts_prev = 0
                self.short_loser_step = 0
                self.initial_short_entry = 0
            self.save_state()

            logger.info(f"🎉 [{SYMBOL_DISPLAY}] TP {winner_side} ({step_desc}) исполнен. Переоткрытие...")
            qty = self.calculate_qty_for_usdc(BASE_ORDER_USDC, current_price)
            side = 'buy' if winner_side == 'LONG' else 'sell'

            success = False
            for attempt in range(5):
                try:
                    await self.exchange.create_order(SYMBOL, 'MARKET', side, qty,
                                                     params={'positionSide': winner_side})
                    await asyncio.sleep(0.8)
                    entry, contracts = await self.get_actual_position_entry_price(winner_side)
                    if entry > 0 and contracts > 0:
                        if winner_side == 'LONG':
                            self.long_entry_price = entry
                            self.initial_long_entry = entry
                            self.long_contracts_prev = contracts
                        else:
                            self.short_entry_price = entry
                            self.initial_short_entry = entry
                            self.short_contracts_prev = contracts
                        self.save_state()
                        success = True

                        details = await self.fetch_tp_pnl(captured_tp_id)
                        pnl_pct = (details['net'] / BASE_ORDER_USDC * 100.0) if BASE_ORDER_USDC > 0 else 0.0
                        await send_tg_message(self.http_session,
                            f"✅ <b>ТЕЙК-ПРОФИТ ({SYMBOL_DISPLAY})</b> [{step_desc}]\n"
                            f"Направление: {winner_side}\n"
                            f"💰 Net PnL: <b>{details['net']:+.2f} USDC ({pnl_pct:+.2f}%)</b>\n"
                            f"📊 Gross: {details['gross']:+.2f} | Fee: {details['fee']:.4f}\n"
                            f"💳 Баланс: {details['balance']:.2f} USDC\n"
                            f"🔄 Новый вход: ${entry:,.2f} | Объём: {contracts}")
                        break
                    await asyncio.sleep(1.0)
                except Exception as e:
                    logger.warning(f"⚠️ reopen {winner_side} try#{attempt+1}: {e}")
                    await asyncio.sleep(1.5 + attempt * 0.5)

            if not success:
                logger.error(f"❌ Не удалось переоткрыть {winner_side} после 5 попыток! Синхронизация с биржей...")
                try:
                    positions = await self.exchange.fetch_positions([SYMBOL])
                    pos = next((p for p in positions if p['side'] == winner_side.lower()
                                and float(p['contracts']) > 0), None)
                    if pos:
                        contracts = float(pos['contracts'])
                        avg = float(pos['entryPrice'])
                        inferred = self._infer_step_from_position(contracts, avg)
                        if winner_side == 'LONG':
                            self.long_contracts_prev = contracts
                            self.long_entry_price = avg
                            self.long_loser_step = inferred
                            self.initial_long_entry = self._infer_initial_entry(avg, inferred)
                        else:
                            self.short_contracts_prev = contracts
                            self.short_entry_price = avg
                            self.short_loser_step = inferred
                            self.initial_short_entry = self._infer_initial_entry(avg, inferred)
                        logger.warning(f"🔁 {winner_side} позиция восстановлена: {contracts} @ ${avg:,.2f}")
                except Exception as e:
                    logger.error(f"⚠️ sync after failed reopen: {e}")
                self.save_state()

                await send_tg_message(self.http_session,
                    f"⚠️ <b>[{SYMBOL_DISPLAY}] Не удалось переоткрыть {winner_side}</b>\n"
                    f"Синхронизация выполнена. Проверьте позицию вручную.")

            await self.audit_and_sync_all_orders()

    # ============================ MAIN LOOP ============================

    async def main_loop(self):
        logger.info(f"✅ Главный цикл мониторинга для {SYMBOL_DISPLAY} запущен.")
        while self.is_running:
            try:
                current_time = time.time()
                open_orders = await self.exchange.fetch_open_orders(SYMBOL)
                positions = await self.exchange.fetch_positions([SYMBOL])

                long_pos = next((p for p in positions if p['side'] == 'long' and float(p['contracts']) > 0), None)
                short_pos = next((p for p in positions if p['side'] == 'short' and float(p['contracts']) > 0), None)

                long_contracts = float(long_pos['contracts']) if long_pos else 0.0
                short_contracts = float(short_pos['contracts']) if short_pos else 0.0

                # --- 1. TP LONG ---
                if self.long_contracts_prev > 0 and long_contracts < self.long_contracts_prev - 1e-9:
                    drop_diff = self.long_contracts_prev - long_contracts
                    is_full_close = (long_contracts == 0) or (long_contracts / self.long_contracts_prev < 0.05)

                    if is_full_close and (current_time - self.last_long_tp_time > 5.0):
                        self.last_long_tp_time = current_time
                        logger.info(f"✅ LONG TP (полное закрытие, drop={drop_diff:.6f})")
                        for o in open_orders:
                            if str(o.get('info', {}).get('positionSide', '')).upper() == 'LONG':
                                try:
                                    await self.exchange.cancel_order(str(o['id']), SYMBOL)
                                except Exception:
                                    pass
                        ticker = await self.exchange.fetch_ticker(SYMBOL)
                        await self.reopen_winner_position(float(ticker['last']), 'LONG')
                        await asyncio.sleep(1)
                        continue
                    else:
                        logger.info(f"ℹ️ LONG частичный TP: {self.long_contracts_prev:.6f} → {long_contracts:.6f}")
                        self.long_contracts_prev = long_contracts

                # --- 2. TP SHORT ---
                if self.short_contracts_prev > 0 and short_contracts < self.short_contracts_prev - 1e-9:
                    drop_diff = self.short_contracts_prev - short_contracts
                    is_full_close = (short_contracts == 0) or (short_contracts / self.short_contracts_prev < 0.05)

                    if is_full_close and (current_time - self.last_short_tp_time > 5.0):
                        self.last_short_tp_time = current_time
                        logger.info(f"✅ SHORT TP (полное закрытие, drop={drop_diff:.6f})")
                        for o in open_orders:
                            if str(o.get('info', {}).get('positionSide', '')).upper() == 'SHORT':
                                try:
                                    await self.exchange.cancel_order(str(o['id']), SYMBOL)
                                except Exception:
                                    pass
                        ticker = await self.exchange.fetch_ticker(SYMBOL)
                        await self.reopen_winner_position(float(ticker['last']), 'SHORT')
                        await asyncio.sleep(1)
                        continue
                    else:
                        logger.info(f"ℹ️ SHORT частичный TP: {self.short_contracts_prev:.6f} → {short_contracts:.6f}")
                        self.short_contracts_prev = short_contracts

                # --- 3. Усреднение LONG ---
                if long_contracts > self.long_contracts_prev + 1e-9 and self.long_contracts_prev > 0:
                    async with self.exec_lock:
                        self.long_loser_step = min(GRID_LEVELS, self.long_loser_step + 1)
                        self.long_contracts_prev = long_contracts
                        if long_pos:
                            self.long_entry_price = float(long_pos['entryPrice'])
                        self.save_state()
                        msg = f"🎯 🟢 [{SYMBOL_DISPLAY}] УСРЕДНЕНИЕ УРОВНЯ {self.long_loser_step}/{GRID_LEVELS} ВЫПОЛНЕНО!"
                        logger.info(msg)
                        await send_tg_message(self.http_session, msg)
                        await self.audit_and_sync_all_orders()
                        continue

                # --- 4. Усреднение SHORT ---
                if short_contracts > self.short_contracts_prev + 1e-9 and self.short_contracts_prev > 0:
                    async with self.exec_lock:
                        self.short_loser_step = min(GRID_LEVELS, self.short_loser_step + 1)
                        self.short_contracts_prev = short_contracts
                        if short_pos:
                            self.short_entry_price = float(short_pos['entryPrice'])
                        self.save_state()
                        msg = f"🎯 🔻 [{SYMBOL_DISPLAY}] УСРЕДНЕНИЕ УРОВНЯ {self.short_loser_step}/{GRID_LEVELS} ВЫПОЛНЕНО!"
                        logger.info(msg)
                        await send_tg_message(self.http_session, msg)
                        await self.audit_and_sync_all_orders()
                        continue

                self.long_contracts_prev = long_contracts
                self.short_contracts_prev = short_contracts

                await self.audit_and_sync_all_orders()

            except Exception as e:
                logger.error(f"⚠️ Ошибка в main_loop: {e}")
                await asyncio.sleep(2)

            await asyncio.sleep(1)

    async def run(self):
        while self.is_running:
            try:
                await self.initialize()
                await self.recover_state_or_init()
                await self.main_loop()
            except Exception as err:
                logger.error(f"❌ Критическая ошибка: {err}")
                await self.safe_close_exchange()
                await asyncio.sleep(5)


bot = CCXTUsdcHedgeBot()


@asynccontextmanager
async def lifespan(app: FastAPI):
    loop = asyncio.get_running_loop()
    task = loop.create_task(bot.run())
    yield
    bot.is_running = False
    task.cancel()
    if bot.exchange:
        await bot.exchange.close()


app = FastAPI(lifespan=lifespan)


@app.get("/")
@app.get("/health")
async def root():
    return {"status": "ok", "symbol": SYMBOL_DISPLAY}


if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)



















































































































