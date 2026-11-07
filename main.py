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















































