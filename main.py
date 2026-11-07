import asyncio
import json
import logging
import math
import os
import time
from pathlib import Path
from contextlib import asynccontextmanager
from typing import Any, Optional

import aiohttp
import ccxt.async_support as ccxt
import uvicorn
from fastapi import FastAPI

# =============================================================================
# Binance USD-M / USDC Hedge Mode bot
#
# IMPORTANT:
# - Test on Binance demo/testnet first.
# - Use API key permissions without withdrawals.
# - Existing position recovery is fail-safe: if state file is missing, the bot
#   adopts the exchange position but suspends GRID placement because its original
#   anchor/filled grid level cannot be reconstructed reliably from average entry.
# - PnL and fees are read from actual user trades for the specific TP order.
# =============================================================================

SYMBOL = os.getenv("SYMBOL", "ZEC/USDC:USDC").strip()
SYMBOL_DISPLAY = SYMBOL.split(":")[0]
LEVERAGE = int(os.getenv("LEVERAGE", "50"))
BASE_ORDER_USDC = float(os.getenv("LONG_USDT_AMOUNT", "100"))
GRID_LEVELS = int(os.getenv("GRID_LEVELS", "3"))
VISIBLE_GRID_LEVELS = 1
WINNER_TAKE_PROFIT_PCT = float(os.getenv("TP_PCT", "0.5"))
POLL_SECONDS = float(os.getenv("POLL_SECONDS", "1.0"))
STATE_FILE = Path(os.getenv("STATE_FILE", "bot_state.json"))
DRY_RUN = os.getenv("DRY_RUN", "false").lower() in ("1", "true", "yes")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("USDC_HEDGE_BOT")


def grid_step_percent(step: int) -> float:
    return {1: 5.0, 2: 10.0, 3: 15.0}.get(step, 15.0)


def grid_step_usdc(step: int) -> float:
    return BASE_ORDER_USDC * {1: 1.0, 2: 2.0, 3: 4.0}.get(step, 4.0)


async def telegram(session: Optional[aiohttp.ClientSession], message: str) -> None:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID or session is None or session.closed:
        log.info("TG: %s", message.replace("\n", " | "))
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        async with session.post(
            url,
            json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML",
                  "disable_web_page_preview": True},
            timeout=aiohttp.ClientTimeout(total=8),
        ) as response:
            if response.status >= 300:
                log.warning("Telegram HTTP %s: %s", response.status, await response.text())
    except Exception:
        log.exception("Telegram send failed")


class HedgeBot:
    def __init__(self):
        self.exchange = None
        self.session = None
        self.market = None
        self.lock = asyncio.Lock()
        self.running = True

        self.state = {
            "version": 2,
            "symbol": SYMBOL,
            "long": self._empty_side(),
            "short": self._empty_side(),
            "last_processed_trade_ids": {"LONG": [], "SHORT": []},
        }
        self.last_reconcile = 0.0

    @staticmethod
    def _empty_side():
        return {
            "initial_entry": 0.0,
            "entry_price": 0.0,
            "contracts": 0.0,
            "grid_step": 0,
            "tp_order_id": None,
            "grid_order_id": None,
            "recovery_uncertain": False,
            "tp_created_at": 0.0,
            "pending_reopen": False,
        }

    def _load_state(self):
        if not STATE_FILE.exists():
            log.warning("State file %s not found; existing positions will be adopted conservatively.", STATE_FILE)
            return False
        try:
            data = json.loads(STATE_FILE.read_text(encoding="utf-8"))
            if data.get("symbol") != SYMBOL:
                log.warning("State symbol differs (%s vs %s); ignoring old state.", data.get("symbol"), SYMBOL)
                return False
            for side in ("long", "short"):
                if isinstance(data.get(side), dict):
                    self.state[side].update(data[side])
            self.state["last_processed_trade_ids"] = data.get(
                "last_processed_trade_ids", self.state["last_processed_trade_ids"]
            )
            log.info("State restored from %s", STATE_FILE)
            return True
        except Exception:
            log.exception("Could not read state file; starting with conservative recovery.")
            return False

    def _save_state(self):
        tmp = STATE_FILE.with_suffix(STATE_FILE.suffix + ".tmp")
        try:
            tmp.write_text(json.dumps(self.state, ensure_ascii=False, indent=2), encoding="utf-8")
            tmp.replace(STATE_FILE)
        except Exception:
            log.exception("Could not persist bot state")

    def _new_exchange(self):
        key = os.getenv("BINANCE_API_KEY", "").strip().strip("'").strip('"')
        secret = os.getenv("BINANCE_SECRET_KEY", "").strip().strip("'").strip('"')
        if not key or not secret:
            raise RuntimeError("BINANCE_API_KEY / BINANCE_SECRET_KEY are not configured")
        return ccxt.binanceusdm({
            "apiKey": key,
            "secret": secret,
            "enableRateLimit": True,
            "timeout": 15000,
            "options": {"defaultType": "future", "adjustForTimeDifference": True},
        })

    async def initialize(self):
        if self.exchange is None:
            self.exchange = self._new_exchange()
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        await self.exchange.load_markets()
        if SYMBOL not in self.exchange.markets:
            raise RuntimeError(f"Symbol {SYMBOL} not found in loaded CCXT markets")
        self.market = self.exchange.market(SYMBOL)

        # Validate that the market is a linear contract settled in USDC.
        settle = self.market.get("settle")
        if settle and settle != "USDC":
            raise RuntimeError(f"{SYMBOL} settles in {settle}, not USDC")
        if not self.market.get("contract"):
            raise RuntimeError(f"{SYMBOL} is not recognized as a contract market")

        if not DRY_RUN:
            await self.exchange.set_leverage(LEVERAGE, SYMBOL)
            try:
                await self.exchange.set_margin_mode("cross", SYMBOL)
            except Exception as exc:
                # Binance may report that the requested mode is already active.
                log.info("Margin mode unchanged/not set: %s", exc)
            try:
                await self.exchange.set_position_mode(True, SYMBOL)
            except Exception as exc:
                log.info("Position mode unchanged/not set: %s", exc)

        log.info("Initialized %s | leverage=%sx | contractSize=%s | dry_run=%s",
                 SYMBOL, LEVERAGE, self.market.get("contractSize"), DRY_RUN)

    async def close(self):
        if self.exchange is not None:
            await self.exchange.close()
            self.exchange = None
        if self.session is not None and not self.session.closed:
            await self.session.close()

    def amount_precision(self, amount: float) -> float:
        return float(self.exchange.amount_to_precision(SYMBOL, amount))

    def price_precision(self, price: float) -> float:
        return float(self.exchange.price_to_precision(SYMBOL, price))

    def qty_for_notional(self, notional_usdc: float, price: float) -> float:
        if price <= 0:
            raise ValueError("Price must be positive")
        contract_size = float(self.market.get("contractSize") or 1.0)
        # CCXT contract amount is number of contracts, not necessarily base units.
        qty = self.amount_precision(notional_usdc / (price * contract_size))
        min_amount = ((self.market.get("limits") or {}).get("amount") or {}).get("min")
        min_cost = ((self.market.get("limits") or {}).get("cost") or {}).get("min")
        if min_amount is not None and qty < float(min_amount):
            raise ValueError(f"Calculated quantity {qty} is below min amount {min_amount}")
        estimated_cost = qty * contract_size * price
        if min_cost is not None and estimated_cost < float(min_cost):
            raise ValueError(f"Calculated notional {estimated_cost} is below min cost {min_cost}")
        if qty <= 0:
            raise ValueError("Calculated quantity is zero after precision")
        return qty

    @staticmethod
    def _position_side(position: dict) -> Optional[str]:
        info = position.get("info") or {}
        raw = str(info.get("positionSide") or "").upper()
        if raw in ("LONG", "SHORT"):
            return raw
        side = str(position.get("side") or "").lower()
        if side == "long":
            return "LONG"
        if side == "short":
            return "SHORT"
        return None

    async def fetch_positions(self):
        positions = await self.exchange.fetch_positions([SYMBOL])
        result = {"LONG": None, "SHORT": None}
        for p in positions:
            side = self._position_side(p)
            contracts = abs(float(p.get("contracts") or 0))
            if side in result and contracts > 0:
                result[side] = p
        return result

    async def fetch_open_orders_by_side(self):
        orders = await self.exchange.fetch_open_orders(SYMBOL)
        result = {"LONG_TP": [], "SHORT_TP": [], "LONG_GRID": [], "SHORT_GRID": []}
        for order in orders:
            info = order.get("info") or {}
            pos_side = str(info.get("positionSide") or "").upper()
            side = str(order.get("side") or "").lower()
            reduce_only = order.get("reduceOnly")
            if reduce_only is None:
                reduce_only = str(info.get("reduceOnly", "false")).lower() == "true"
            # Hedge-mode Binance TP closes LONG with SELL, SHORT with BUY.
            if pos_side == "LONG" and side == "sell":
                result["LONG_TP" if reduce_only or order.get("type") == "limit" else "LONG_TP"].append(order)
            elif pos_side == "SHORT" and side == "buy":
                result["SHORT_TP"].append(order)
            elif pos_side == "LONG" and side == "buy":
                result["LONG_GRID"].append(order)
            elif pos_side == "SHORT" and side == "sell":
                result["SHORT_GRID"].append(order)
        return result

    async def create_order(self, order_type, side, amount, price=None, params=None):
        if DRY_RUN:
            log.warning("DRY_RUN: would create %s %s amount=%s price=%s params=%s",
                        order_type, side, amount, price, params)
            return {"id": f"dry-{time.time_ns()}", "status": "open", "amount": amount, "price": price}
        return await self.exchange.create_order(SYMBOL, order_type, side, amount, price, params or {})

    async def cancel_order(self, order_id):
        if DRY_RUN:
            log.warning("DRY_RUN: would cancel order %s", order_id)
            return
        try:
            await self.exchange.cancel_order(str(order_id), SYMBOL)
        except ccxt.OrderNotFound:
            pass

    async def open_base_position(self, side: str, price: float):
        key = side.lower()
        order_side = "buy" if side == "LONG" else "sell"
        qty = self.qty_for_notional(BASE_ORDER_USDC, price)
        order = await self.create_order(
            "market", order_side, qty, None, {"positionSide": side}
        )
        if DRY_RUN:
            entry = price
            contracts = qty
        else:
            # Wait briefly for Binance position endpoint to reflect the fill.
            entry, contracts = 0.0, 0.0
            for _ in range(8):
                await asyncio.sleep(0.4)
                positions = await self.fetch_positions()
                p = positions[side]
                if p and float(p.get("contracts") or 0) > 0:
                    entry = float(p.get("entryPrice") or 0)
                    contracts = abs(float(p.get("contracts") or 0))
                    if entry > 0 and contracts > 0:
                        break
            if entry <= 0 or contracts <= 0:
                raise RuntimeError(f"{side} market order {order.get('id')} submitted, but position fill was not confirmed")
        self.state[key] = self._empty_side()
        self.state[key].update({
            "initial_entry": entry,
            "entry_price": entry,
            "contracts": contracts,
            "grid_step": 0,
            "recovery_uncertain": False,
        })
        self._save_state()
        await telegram(self.session,
                       f"🟢 <b>{SYMBOL_DISPLAY}: открыта базовая {side}</b>\\n"
                       f"Цена: {entry:.8f}\\nКонтракты: {contracts:.8f}")
        return order

    async def adopt_positions(self, state_file_was_loaded: bool):
        positions = await self.fetch_positions()
        for side in ("LONG", "SHORT"):
            p = positions[side]
            key = side.lower()
            s = self.state[key]
            if p:
                contracts = abs(float(p.get("contracts") or 0))
                entry = float(p.get("entryPrice") or 0)
                if entry <= 0:
                    raise RuntimeError(f"Exchange returned invalid {side} entryPrice: {entry}")
                was_known = state_file_was_loaded and float(s.get("initial_entry") or 0) > 0
                s["entry_price"] = entry
                s["contracts"] = contracts
                if not was_known:
                    s["initial_entry"] = entry
                    s["grid_step"] = 0
                    s["recovery_uncertain"] = True
                    log.warning(
                        "%s adopted at avg entry=%s. Original anchor/filled grid level unknown; GRID suspended.",
                        side, entry
                    )
                    await telegram(
                        self.session,
                        f"⚠️ <b>{SYMBOL_DISPLAY} {side}: позиция подхвачена</b>\n"
                        f"Средняя цена: {entry}\nОбъём контрактов: {contracts}\n"
                        "Исходная точка и уровень сетки неизвестны: новые GRID-ордера временно отключены."
                    )
                else:
                    log.info("%s recovered: contracts=%s avg_entry=%s anchor=%s step=%s",
                             side, contracts, entry, s["initial_entry"], s["grid_step"])
            else:
                self.state[key] = self._empty_side()

        # Start a fresh two-sided cycle only when the exchange is flat for that side.
        ticker = await self.exchange.fetch_ticker(SYMBOL)
        current_price = float(ticker.get("last") or ticker.get("close") or 0)
        if current_price <= 0:
            raise RuntimeError("Could not obtain a valid market price for initial orders")
        for side in ("LONG", "SHORT"):
            if positions[side] is None:
                await self.open_base_position(side, current_price)
        self._save_state()

    async def _ensure_tp(self, side: str, position: dict, orders: dict):
        key = side.lower()
        s = self.state[key]
        contracts = abs(float(position.get("contracts") or 0))
        entry = float(position.get("entryPrice") or 0)
        if contracts <= 0 or entry <= 0:
            return

        target = entry * (1 + WINNER_TAKE_PROFIT_PCT / 100) if side == "LONG" else \
                 entry * (1 - WINNER_TAKE_PROFIT_PCT / 100)
        target = self.price_precision(target)
        candidates = orders[f"{side}_TP"]

        # Keep one TP only if its quantity and price still match the live position.
        keep = None
        for order in candidates:
            o_amount = float(order.get("amount") or 0)
            o_price = float(order.get("price") or 0)
            amount_ok = abs(o_amount - contracts) <= max(1e-10, contracts * 0.0005)
            price_ok = o_price > 0 and abs(o_price - target) <= max(1e-10, target * 0.000001)
            if keep is None and amount_ok and price_ok:
                keep = order
            else:
                await self.cancel_order(order["id"])

        if keep:
            s["tp_order_id"] = str(keep["id"])
            return

        if s.get("tp_order_id"):
            # Stale in-memory ID is not evidence that an order is still open.
            s["tp_order_id"] = None

        order_side = "sell" if side == "LONG" else "buy"
        amount = self.amount_precision(contracts)
        if amount <= 0:
            raise RuntimeError(f"Invalid TP quantity for {side}: {amount}")
        created = await self.create_order(
            "limit", order_side, amount, target,
            {"positionSide": side, "timeInForce": "GTC", "reduceOnly": False}
        )
        s["tp_order_id"] = str(created["id"])
        s["tp_created_at"] = time.time()
        log.info("%s TP set: id=%s qty=%s price=%s", side, created["id"], amount, target)
        self._save_state()

    async def _ensure_grid(self, side: str, position: dict, orders: dict):
        key = side.lower()
        s = self.state[key]
        existing = orders[f"{side}_GRID"]
        if s.get("recovery_uncertain"):
            # Do not invent the original grid anchor/level after a cold recovery.
            for order in existing:
                log.warning("%s existing grid order %s left untouched during uncertain recovery",
                            side, order["id"])
            return

        # Only one visible averaging order is intended. Keep one matching the next level.
        next_step = int(s.get("grid_step") or 0) + 1
        if next_step > GRID_LEVELS:
            for order in existing:
                await self.cancel_order(order["id"])
            s["grid_order_id"] = None
            return

        anchor = float(s.get("initial_entry") or 0)
        if anchor <= 0:
            log.error("%s has no valid grid anchor; GRID disabled.", side)
            return

        dist = grid_step_percent(next_step) / 100.0
        target = anchor * (1 - dist) if side == "LONG" else anchor * (1 + dist)
        target = self.price_precision(target)
        qty = self.qty_for_notional(grid_step_usdc(next_step), target)

        keep = None
        for order in existing:
            same_price = abs(float(order.get("price") or 0) - target) <= max(1e-10, target * 0.000001)
            same_amount = abs(float(order.get("amount") or 0) - qty) <= max(1e-10, qty * 0.0005)
            if keep is None and same_price and same_amount:
                keep = order
            else:
                await self.cancel_order(order["id"])

        if keep:
            s["grid_order_id"] = str(keep["id"])
            return

        # Cancel stale grid order before replacing it.
        s["grid_order_id"] = None
        order_side = "buy" if side == "LONG" else "sell"
        created = await self.create_order(
            "limit", order_side, qty, target,
            {"positionSide": side, "timeInForce": "GTC"}
        )
        s["grid_order_id"] = str(created["id"])
        log.info("%s GRID level %s set: id=%s qty=%s price=%s",
                 side, next_step, created["id"], qty, target)
        self._save_state()

    async def reconcile(self):
        positions = await self.fetch_positions()
        orders = await self.fetch_open_orders_by_side()

        for side in ("LONG", "SHORT"):
            p = positions[side]
            key = side.lower()
            if p:
                self.state[key]["entry_price"] = float(p.get("entryPrice") or 0)
                self.state[key]["contracts"] = abs(float(p.get("contracts") or 0))
                await self._ensure_tp(side, p, orders)
                # Refresh open orders after TP/grid changes only on next poll.
                await self._ensure_grid(side, p, orders)
            else:
                # Reopen the winner only after a completed TP and confirmed flat position.
                if self.state[key].get("pending_reopen") and not DRY_RUN:
                    ticker = await self.exchange.fetch_ticker(SYMBOL)
                    price = float(ticker.get("last") or ticker.get("close") or 0)
                    if price <= 0:
                        raise RuntimeError("Invalid price while reopening after TP")
                    await self.open_base_position(side, price)
                elif self.state[key].get("pending_reopen") and DRY_RUN:
                    log.warning("DRY_RUN: pending reopen for %s; no simulated position engine is enabled.", side)
                else:
                    self.state[key] = self._empty_side()
        self._save_state()

    async def fetch_order_trades(self, order_id: str):
        """Return actual fills belonging to one exchange order, not arbitrary last closed order."""
        if not order_id or str(order_id).startswith("dry-"):
            return []
        try:
            # Binance supports orderId in myTrades params; use a bounded time window too.
            since = int((time.time() - 7 * 24 * 3600) * 1000)
            trades = await self.exchange.fetch_my_trades(
                SYMBOL, since=since, limit=1000, params={"orderId": order_id}
            )
            return [t for t in trades if str((t.get("info") or {}).get("orderId", t.get("order"))) == str(order_id)]
        except Exception:
            log.exception("Could not fetch fills for TP order %s", order_id)
            return []

    @staticmethod
    def aggregate_trade_fills(trades):
        gross = 0.0
        fees_by_currency = {}
        filled = 0.0
        for trade in trades:
            info = trade.get("info") or {}
            realized = info.get("realizedPnl")
            if realized is not None:
                gross += float(realized)
            else:
                # Do not fabricate realized PnL from price*amount: use exchange income data.
                pass
            filled += float(trade.get("amount") or 0)
            fee = trade.get("fee") or {}
            if fee.get("cost") is not None:
                currency = fee.get("currency") or "UNKNOWN"
                fees_by_currency[currency] = fees_by_currency.get(currency, 0.0) + abs(float(fee["cost"]))
            elif info.get("commission") is not None:
                currency = info.get("commissionAsset") or "UNKNOWN"
                fees_by_currency[currency] = fees_by_currency.get(currency, 0.0) + abs(float(info["commission"]))
        return gross, fees_by_currency, filled

    async def report_tp(self, side: str, order_id: str, expected_qty: float):
        trades = await self.fetch_order_trades(order_id)
        if not trades:
            await telegram(
                self.session,
                f"⚠️ <b>{SYMBOL_DISPLAY} {side}: TP исполнен</b>\n"
                f"Биржа подтвердила закрытие ордера {order_id}, но история исполнений пока недоступна. "
                "PnL/комиссия не подменяются приблизительными значениями."
            )
            return

        gross, fees, filled = self.aggregate_trade_fills(trades)
        fee_text = ", ".join(f"{v:.6f} {k}" for k, v in fees.items()) or "нет данных"
        fee_usdc = fees.get("USDC")
        net = gross - fee_usdc if fee_usdc is not None else None
        full = filled >= expected_qty * 0.999
        msg = (
            f"✅ <b>TP {side} — {SYMBOL_DISPLAY}</b>\n"
            f"Исполнено: {filled:.8f} / {expected_qty:.8f} контрактов\n"
            f"Gross realized PnL: {gross:+.6f} USDC\n"
            f"Комиссия: {fee_text}\n"
        )
        if net is not None:
            msg += f"Net PnL (Gross − USDC fee): <b>{net:+.6f} USDC</b>\n"
        else:
            msg += "Net PnL: не рассчитан — комиссия списана не в USDC или валюта комиссии неизвестна.\n"
        msg += f"Закрытие полное: {'да' if full else 'нет, частичное'}"
        await telegram(self.session, msg)

    async def process_tp_events(self):
        """
        Detect TP completion from order status, then account only fills belonging
        to that TP order. Position-size changes alone never trigger a reopen.
        """
        for side in ("LONG", "SHORT"):
            s = self.state[side.lower()]
            order_id = s.get("tp_order_id")
            if not order_id or str(order_id).startswith("dry-"):
                continue
            try:
                order = await self.exchange.fetch_order(str(order_id), SYMBOL)
            except ccxt.OrderNotFound:
                # Missing order is not proof of a fill (could have been cancelled).
                s["tp_order_id"] = None
                continue
            except Exception:
                log.exception("fetch_order failed for %s TP %s", side, order_id)
                continue

            status = str(order.get("status") or "").lower()
            filled = float(order.get("filled") or 0)
            amount = float(order.get("amount") or 0)
            if status == "closed" or (amount > 0 and filled >= amount * 0.999):
                await self.report_tp(side, str(order_id), amount)
                s["tp_order_id"] = None
                s["pending_reopen"] = True
                # Reopen only after the exchange confirms that the old side is flat.
                log.info("%s TP order %s closed; waiting for flat-position confirmation.", side, order_id)
                self._save_state()
            elif status in ("canceled", "rejected", "expired"):
                s["tp_order_id"] = None
                self._save_state()

    async def process_grid_events(self):
        for side in ("LONG", "SHORT"):
            s = self.state[side.lower()]
            order_id = s.get("grid_order_id")
            if not order_id or str(order_id).startswith("dry-"):
                continue
            try:
                order = await self.exchange.fetch_order(str(order_id), SYMBOL)
            except ccxt.OrderNotFound:
                s["grid_order_id"] = None
                self._save_state()
                continue
            except Exception:
                log.exception("fetch_order failed for %s GRID %s", side, order_id)
                continue

            status = str(order.get("status") or "").lower()
            amount = float(order.get("amount") or 0)
            filled = float(order.get("filled") or 0)
            if status == "closed" or (amount > 0 and filled >= amount * 0.999):
                s["grid_step"] = min(GRID_LEVELS, int(s.get("grid_step") or 0) + 1)
                s["grid_order_id"] = None
                log.info("%s GRID level %s filled.", side, s["grid_step"])
                await telegram(
                    self.session,
                    f"🎯 <b>{SYMBOL_DISPLAY}: усреднение {side}</b>\n"
                    f"Выполнен уровень {s['grid_step']}/{GRID_LEVELS}"
                )
                self._save_state()
            elif status in ("canceled", "rejected", "expired"):
                s["grid_order_id"] = None
                self._save_state()

    async def main_loop(self):
        log.info("Monitoring loop started.")
        while self.running:
            try:
                await self.process_tp_events()
                await self.process_grid_events()
                await self.reconcile()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Main loop error")
            await asyncio.sleep(POLL_SECONDS)

    async def run(self):
        while self.running:
            try:
                await self.initialize()
                loaded = self._load_state()
                await self.adopt_positions(loaded)
                await self.reconcile()
                await self.main_loop()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Fatal bot error; reconnecting in 5 seconds.")
                await self.close()
                await asyncio.sleep(5)


bot = HedgeBot()


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(bot.run())
    try:
        yield
    finally:
        bot.running = False
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        await bot.close()


app = FastAPI(lifespan=lifespan)


@app.get("/")
@app.get("/health")
async def health():
    return {
        "status": "ok",
        "symbol": SYMBOL_DISPLAY,
        "dry_run": DRY_RUN,
        "state_file": str(STATE_FILE),
    }


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    uvicorn.run(app, host="0.0.0.0", port=port)
















































