import asyncio
import os
import time
import math
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
# Чистое имя для красивых сообщений в телеграм (например, BTC/USDC)
SYMBOL_DISPLAY = SYMBOL_RAW.split(':')[0] if ':' in SYMBOL_RAW else SYMBOL_RAW

LEVERAGE = int(os.getenv('LEVERAGE', 50))
BASE_ORDER_USDC = float(os.getenv('LONG_USDT_AMOUNT', 100.0))  # Стартовый объем в USDC для каждой ветки

GRID_LEVELS = 3                 # 3 уровня усреднения
VISIBLE_GRID_LEVELS = 1         # По одной лимитке усреднения в стакане

# Точные отступы сетки: -5%, -10%, -15% от ТВХ
def get_grid_step_percent(step: int) -> float:
    if step == 1:
        return 5.0
    elif step == 2:
        return 10.0
    elif step == 3:
        return 15.0
    return 15.0

# Объемы по уровням с мультипликаторами x1, x2, x4
def get_step_usdc_volume(step: int) -> float:
    if step <= 1:
        return float(BASE_ORDER_USDC)
    elif step == 2:
        return float(BASE_ORDER_USDC * 2.0)
    elif step == 3:
        return float(BASE_ORDER_USDC * 4.0)
    return float(BASE_ORDER_USDC * 4.0)

WINNER_TAKE_PROFIT_PCT = 0.5    # Жестко 0.5% TP для всех уровней

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

        self.exec_lock = asyncio.Lock()

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

    async def fetch_trade_and_pnl_details(self):
        """Собирает детальные данные по PnL, комиссиям и текущему балансу USDC"""
        for attempt in range(5):
            await asyncio.sleep(1.0)
            try:
                income_history = await self.exchange.fetch_income(SYMBOL, limit=15)
                realized_pnl = 0.0
                fee_paid = 0.0

                for item in reversed(income_history):
                    income_type = item.get('type')
                    amount = float(item.get('amount', 0.0))

                    if income_type == 'REALIZED_PNL':
                        realized_pnl += amount
                    elif income_type == 'COMMISSION':
                        fee_paid += abs(amount)

                    if income_type == 'REALIZED_PNL':
                        break

                net_profit = realized_pnl - fee_paid
                
                balance_data = await self.exchange.fetch_balance()
                usdc_free = balance_data.get('USDC', {}).get('free', 0.0)
                usdc_total = balance_data.get('USDC', {}).get('total', 0.0)

                return {
                    'gross': realized_pnl,
                    'fee': fee_paid,
                    'net': net_profit,
                    'balance': usdc_total if usdc_total > 0 else usdc_free
                }
            except Exception as e:
                logger.error(f"⚠️ Ошибка получения деталей сделки: {e}")
        return {'gross': 0.0, 'fee': 0.0, 'net': 0.0, 'balance': 0.0}

    async def audit_and_sync_all_orders(self):
        try:
            open_orders = await self.exchange.fetch_open_orders(SYMBOL)
            positions = await self.exchange.fetch_positions([SYMBOL])

            long_pos = next((p for p in positions if p['side'] == 'long' and float(p['contracts']) > 0), None)
            short_pos = next((p for p in positions if p['side'] == 'short' and float(p['contracts']) > 0), None)

            existing_tp_long = None
            existing_tp_short = None
            existing_grid_long = {}
            existing_grid_short = {}

            for o in open_orders:
                o_id = str(o['id'])
                o_pos_side = str(o.get('info', {}).get('positionSide', '')).upper()
                o_side = str(o['side']).upper()
                
                if o_pos_side == 'LONG' and o_side == 'SELL':
                    existing_tp_long = o
                elif o_pos_side == 'SHORT' and o_side == 'BUY':
                    existing_tp_short = o
                elif o_pos_side == 'LONG' and o_side == 'BUY':
                    existing_grid_long[o_id] = o
                elif o_pos_side == 'SHORT' and o_side == 'SELL':
                    existing_grid_short[o_id] = o

            self.active_limit_orders.clear()
            self.long_tp_order_id = None
            self.short_tp_order_id = None

            # --- 1. LONG TP ---
            if long_pos:
                l_contracts = float(long_pos['contracts'])
                if existing_tp_long:
                    self.long_tp_order_id = str(existing_tp_long['id'])
                else:
                    l_entry = float(long_pos['entryPrice'])
                    tp_price = self.round_price(l_entry * (1.0 + (WINNER_TAKE_PROFIT_PCT / 100.0)))
                    tp_qty = self.round_amount(l_contracts)
                    try:
                        new_ord = await self.exchange.create_order(
                            SYMBOL, 'LIMIT', 'sell', tp_qty, tp_price,
                            params={'positionSide': 'LONG', 'timeInForce': 'GTC'}
                        )
                        self.long_tp_order_id = str(new_ord['id'])
                        logger.info(f"📥 🟢 [{SYMBOL_DISPLAY}] Создан TP_LONG по ${tp_price:,.2f}")
                    except Exception as e:
                        logger.error(f"❌ Ошибка создания TP_LONG: {e}")

            # --- 2. SHORT TP ---
            if short_pos:
                s_contracts = float(short_pos['contracts'])
                if existing_tp_short:
                    self.short_tp_order_id = str(existing_tp_short['id'])
                else:
                    s_entry = float(short_pos['entryPrice'])
                    tp_price = self.round_price(s_entry * (1.0 - (WINNER_TAKE_PROFIT_PCT / 100.0)))
                    tp_qty = self.round_amount(s_contracts)
                    try:
                        new_ord = await self.exchange.create_order(
                            SYMBOL, 'LIMIT', 'buy', tp_qty, tp_price,
                            params={'positionSide': 'SHORT', 'timeInForce': 'GTC'}
                        )
                        self.short_tp_order_id = str(new_ord['id'])
                        logger.info(f"📥 🔻 [{SYMBOL_DISPLAY}] Создан TP_SHORT по ${tp_price:,.2f}")
                    except Exception as e:
                        logger.error(f"❌ Ошибка создания TP_SHORT: {e}")

            # --- 3. LONG Grid ---
            if long_pos and self.initial_long_entry > 0:
                if existing_grid_long:
                    grid_id, grid_obj = list(existing_grid_long.items())[0]
                    next_step = self.long_loser_step + 1
                    self.active_limit_orders[f"LONG_{next_step}"] = grid_id
                else:
                    next_step = self.long_loser_step + 1
                    if next_step <= GRID_LEVELS:
                        target_usdc = get_step_usdc_volume(next_step)
                        dist_pct = get_grid_step_percent(next_step)
                        grid_price = self.round_price(self.initial_long_entry * (1.0 - (dist_pct / 100.0)))
                        grid_qty = self.calculate_qty_for_usdc(target_usdc, grid_price)
                        try:
                            new_ord = await self.exchange.create_order(
                                SYMBOL, 'LIMIT', 'buy', grid_qty, grid_price,
                                params={'positionSide': 'LONG', 'timeInForce': 'GTC'}
                            )
                            self.active_limit_orders[f"LONG_{next_step}"] = str(new_ord['id'])
                            logger.info(f"📥 🟢 [{SYMBOL_DISPLAY}] Создан лимитник усреднения LONG #{next_step} по ${grid_price:,.2f}")
                        except Exception as e:
                            logger.error(f"❌ Ошибка создания лимитника LONG: {e}")

            # --- 4. SHORT Grid ---
            if short_pos and self.initial_short_entry > 0:
                if existing_grid_short:
                    grid_id, grid_obj = list(existing_grid_short.items())[0]
                    next_step = self.short_loser_step + 1
                    self.active_limit_orders[f"SHORT_{next_step}"] = grid_id
                else:
                    next_step = self.short_loser_step + 1
                    if next_step <= GRID_LEVELS:
                        target_usdc = get_step_usdc_volume(next_step)
                        dist_pct = get_grid_step_percent(next_step)
                        grid_price = self.round_price(self.initial_short_entry * (1.0 + (dist_pct / 100.0)))
                        grid_qty = self.calculate_qty_for_usdc(target_usdc, grid_price)
                        try:
                            new_ord = await self.exchange.create_order(
                                SYMBOL, 'LIMIT', 'sell', grid_qty, grid_price,
                                params={'positionSide': 'SHORT', 'timeInForce': 'GTC'}
                            )
                            self.active_limit_orders[f"SHORT_{next_step}"] = str(new_ord['id'])
                            logger.info(f"📥 🔻 [{SYMBOL_DISPLAY}] Создан лимитник усреднения SHORT #{next_step} по ${grid_price:,.2f}")
                        except Exception as e:
                            logger.error(f"❌ Ошибка создания лимитника SHORT: {e}")

        except Exception as e:
            logger.error(f"❌ Ошибка в аудите ордеров: {e}")

    async def recover_state_or_init(self):
        async with self.exec_lock:
            logger.info(f"🔄 Сканирование биржи и подхват существующих позиций для {SYMBOL_DISPLAY}...")

            positions = []
            for attempt in range(3):
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
                long_contracts = float(long_pos['contracts'])
                self.long_entry_price = float(long_pos['entryPrice'])
                self.initial_long_entry = self.long_entry_price
                self.long_contracts_prev = long_contracts
                logger.info(f"🟢 [{SYMBOL_DISPLAY}] LONG успешно подхвачен: {long_contracts} | ТВХ: ${self.long_entry_price:,.2f}")
                await send_tg_message(
                    self.http_session,
                    f"🟢 <b>[{SYMBOL_DISPLAY}] LONG ПОДХВАЧЕН С БИРЖИ</b>\nЦена входа: ${self.long_entry_price:,.2f} | Объем: {long_contracts}"
                )
            else:
                logger.info(f"🟢 [{SYMBOL_DISPLAY}] LONG не найден! Открываем базу...")
                base_qty = self.calculate_qty_for_usdc(BASE_ORDER_USDC, current_price)
                await self.exchange.create_order(SYMBOL, 'MARKET', 'buy', base_qty, params={'positionSide': 'LONG'})
                l_entry, l_contracts = await self.get_actual_position_entry_price('LONG')
                self.long_entry_price = l_entry if l_entry > 0 else current_price
                self.initial_long_entry = self.long_entry_price
                self.long_contracts_prev = l_contracts if l_contracts > 0 else base_qty
                self.long_loser_step = 0
                await send_tg_message(
                    self.http_session,
                    f"🟢 <b>[{SYMBOL_DISPLAY}] ОТКРЫТА НОВАЯ ПОЗИЦИЯ (LONG)</b>\nЦена: ${self.long_entry_price:,.2f}"
                )

            # --- SHORT ---
            if short_pos:
                short_contracts = float(short_pos['contracts'])
                self.short_entry_price = float(short_pos['entryPrice'])
                self.initial_short_entry = self.short_entry_price
                self.short_contracts_prev = short_contracts
                logger.info(f"🔻 [{SYMBOL_DISPLAY}] SHORT успешно подхвачен: {short_contracts} | ТВХ: ${self.short_entry_price:,.2f}")
                await send_tg_message(
                    self.http_session,
                    f"🔻 <b>[{SYMBOL_DISPLAY}] SHORT ПОДХВАЧЕН С БИРЖИ</b>\nЦена входа: ${self.short_entry_price:,.2f} | Объем: {short_contracts}"
                )
            else:
                logger.info(f"🔻 [{SYMBOL_DISPLAY}] SHORT не найден! Открываем базу...")
                base_qty = self.calculate_qty_for_usdc(BASE_ORDER_USDC, current_price)
                await self.exchange.create_order(SYMBOL, 'MARKET', 'sell', base_qty, params={'positionSide': 'SHORT'})
                s_entry, s_contracts = await self.get_actual_position_entry_price('SHORT')
                self.short_entry_price = s_entry if s_entry > 0 else current_price
                self.initial_short_entry = self.short_entry_price
                self.short_contracts_prev = s_contracts if s_contracts > 0 else base_qty
                self.short_loser_step = 0
                await send_tg_message(
                    self.http_session,
                    f"🔻 <b>[{SYMBOL_DISPLAY}] ОТКРЫТА НОВАЯ ПОЗИЦИЯ (SHORT)</b>\nЦена: ${self.short_entry_price:,.2f}"
                )

            await self.audit_and_sync_all_orders()

    async def reopen_winner_position(self, current_price, winner_side):
        async with self.exec_lock:
            icon = "🟢" if winner_side == 'LONG' else "🔻"
            direction_text = "BUY" if winner_side == 'LONG' else "SELL"
            
            triggered_step = self.long_loser_step if winner_side == 'LONG' else self.short_loser_step
            step_desc = f"Базовый ордер" if triggered_step == 0 else f"Усреднение №{triggered_step}"

            logger.info(f"🎉 [{SYMBOL_DISPLAY}] Тейк-профит {winner_side} ({step_desc}) исполнен! Переоткрытие...")

            qty = self.calculate_qty_for_usdc(BASE_ORDER_USDC, current_price)
            side = 'buy' if winner_side == 'LONG' else 'sell'

            try:
                await self.exchange.create_order(SYMBOL, 'MARKET', side, qty, params={'positionSide': winner_side})
                actual_entry, actual_contracts = await self.get_actual_position_entry_price(winner_side)
                
                if actual_entry > 0:
                    if winner_side == 'LONG':
                        self.long_entry_price = actual_entry
                        self.initial_long_entry = actual_entry
                        self.long_contracts_prev = actual_contracts
                        self.long_loser_step = 0
                    else:
                        self.short_entry_price = actual_entry
                        self.initial_short_entry = actual_entry
                        self.short_contracts_prev = actual_contracts
                        self.short_loser_step = 0

                details = await self.fetch_trade_and_pnl_details()
                net_pnl = details['net']
                gross_pnl = details['gross']
                fee_total = details['fee']
                wallet_balance = details['balance']
                
                pnl_percent = (net_pnl / BASE_ORDER_USDC) * 100.0 if BASE_ORDER_USDC > 0 else 0.0

                tg_msg = (
                    f"✅ <b>ТЕЙК-ПРОФИТ! ({SYMBOL_DISPLAY})</b> [{step_desc}]\n"
                    f"Направление: {direction_text}\n"
                    f"💰 PnL: <b>{net_pnl:+.2f} USDC ({pnl_percent:+.2f}%)</b>\n"
                    f"📈 Выход: {current_price:,.2f} | Gross: {gross_pnl:+.2f} USDC\n"
                    f"💸 Комиссия всего: {fee_total:.4f} USDC\n"
                    f"💳 Баланс: {wallet_balance:.2f} USDC\n"
                    f"🔄 <b>Новый вход:</b> ${actual_entry:,.2f} | Объем: {actual_contracts}"
                )
                await send_tg_message(self.http_session, tg_msg)
            except Exception as e:
                logger.error(f"❌ Ошибка переоткрытия {winner_side}: {e}")

            await self.audit_and_sync_all_orders()

    async def main_loop(self):
        logger.info(f"✅ Главный цикл мониторинга для {SYMBOL_DISPLAY} запущен.")
        while self.is_running:
            try:
                open_orders = await self.exchange.fetch_open_orders(SYMBOL)
                open_order_ids = set(str(o['id']) for o in open_orders)
                positions = await self.exchange.fetch_positions([SYMBOL])

                long_pos = next((p for p in positions if p['side'] == 'long' and float(p['contracts']) > 0), None)
                short_pos = next((p for p in positions if p['side'] == 'short' and float(p['contracts']) > 0), None)

                long_contracts = float(long_pos['contracts']) if long_pos else 0.0
                short_contracts = float(short_pos['contracts']) if short_pos else 0.0

                # --- 1. TP LONG ---
                if self.long_tp_order_id and self.long_tp_order_id not in open_order_ids:
                    if long_contracts < self.long_contracts_prev:
                        self.long_tp_order_id = None
                        for o in open_orders:
                            if str(o.get('info', {}).get('positionSide', '')).upper() == 'LONG':
                                try:
                                    await self.exchange.cancel_order(str(o['id']), SYMBOL)
                                except Exception:
                                    pass
                        ticker = await self.exchange.fetch_ticker(SYMBOL)
                        await self.reopen_winner_position(float(ticker['last']), 'LONG')
                        await asyncio.sleep(0.5)
                        continue

                # --- 2. TP SHORT ---
                if self.short_tp_order_id and self.short_tp_order_id not in open_order_ids:
                    if short_contracts < self.short_contracts_prev:
                        self.short_tp_order_id = None
                        for o in open_orders:
                            if str(o.get('info', {}).get('positionSide', '')).upper() == 'SHORT':
                                try:
                                    await self.exchange.cancel_order(str(o['id']), SYMBOL)
                                except Exception:
                                    pass
                        ticker = await self.exchange.fetch_ticker(SYMBOL)
                        await self.reopen_winner_position(float(ticker['last']), 'SHORT')
                        await asyncio.sleep(0.5)
                        continue

                # --- 3. Grid Averaging LONG ---
                if long_contracts > self.long_contracts_prev:
                    async with self.exec_lock:
                        self.long_loser_step = min(GRID_LEVELS, self.long_loser_step + 1)
                        self.long_contracts_prev = long_contracts
                        if long_pos:
                            self.long_entry_price = float(long_pos['entryPrice'])

                        msg = f"🎯 🟢 [{SYMBOL_DISPLAY}] УСРЕДНЕНИЕ УРОВНЯ {self.long_loser_step}/{GRID_LEVELS} ВЫПОЛНЕНО!"
                        logger.info(msg)
                        await send_tg_message(self.http_session, msg)
                        await self.audit_and_sync_all_orders()
                        continue

                # --- 4. Grid Averaging SHORT ---
                if short_contracts > self.short_contracts_prev:
                    async with self.exec_lock:
                        self.short_loser_step = min(GRID_LEVELS, self.short_loser_step + 1)
                        self.short_contracts_prev = short_contracts
                        if short_pos:
                            self.short_entry_price = float(short_pos['entryPrice'])

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















































