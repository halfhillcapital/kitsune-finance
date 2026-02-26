from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from fractions import Fraction
from typing import cast

import pandas as pd
import yfinance as yf

from app.storage import read_watchlist, write_stock

log = logging.getLogger(__name__)


def _nan_to_none(val):
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except (ValueError, TypeError):
        pass
    return val


def _ratio_str(value: float) -> str:
    frac = Fraction(value).limit_denominator(1000)
    return f"{frac.numerator}:{frac.denominator}"


def fetch_single_stock(ticker: str) -> dict:
    """Fetch all data for a single ticker and return as dict."""
    t = yf.Ticker(ticker)

    # Calendar
    cal = t.calendar or {}
    calendar_data = {
        "dividend_date": cal.get("Dividend Date"),
        "ex_dividend_date": cal.get("Ex-Dividend Date"),
        "earnings_dates": cal.get("Earnings Date"),
        "earnings_high": _nan_to_none(cal.get("Earnings High")),
        "earnings_low": _nan_to_none(cal.get("Earnings Low")),
        "earnings_average": _nan_to_none(cal.get("Earnings Average")),
        "revenue_high": _nan_to_none(cal.get("Revenue High")),
        "revenue_low": _nan_to_none(cal.get("Revenue Low")),
        "revenue_average": _nan_to_none(cal.get("Revenue Average")),
    }

    # Earnings dates
    earnings: list[dict] = []
    try:
        df = t.get_earnings_dates(limit=100)
        if df is not None and not df.empty:
            for dt_index, row in df.iterrows():
                earnings.append({
                    "date": cast(pd.Timestamp, dt_index).to_pydatetime(),
                    "eps_estimate": _nan_to_none(row.get("EPS Estimate")),
                    "reported_eps": _nan_to_none(row.get("Reported EPS")),
                    "surprise_pct": _nan_to_none(row.get("Surprise(%)")),
                })
    except Exception:
        log.warning("Failed to fetch earnings dates for %s", ticker, exc_info=True)

    # Dividends
    dividends: list[dict] = []
    try:
        s = t.dividends
        if s is not None and not s.empty:
            dividends = [
                {"date": cast(pd.Timestamp, idx).date(), "amount": float(val)}
                for idx, val in s.items()
            ]
    except Exception:
        log.warning("Failed to fetch dividends for %s", ticker, exc_info=True)

    # Splits
    splits: list[dict] = []
    try:
        s = t.splits
        if s is not None and not s.empty:
            splits = [
                {"date": cast(pd.Timestamp, idx).date(), "ratio": _ratio_str(float(val))}
                for idx, val in s.items()
            ]
    except Exception:
        log.warning("Failed to fetch splits for %s", ticker, exc_info=True)

    return {
        "calendar": calendar_data,
        "earnings": earnings,
        "dividends": dividends,
        "splits": splits,
        "updated_at": datetime.now().isoformat(),
    }


async def sync_single_stock(ticker: str) -> None:
    """Fetch and persist data for a single ticker."""
    log.info("Syncing stock data for %s", ticker)
    try:
        data = await asyncio.to_thread(fetch_single_stock, ticker)
        await write_stock(ticker, data)
        log.info("Synced %s successfully", ticker)
    except Exception:
        log.error("Failed to sync %s", ticker, exc_info=True)


async def sync_all_stocks() -> None:
    """Sync all tickers in the watchlist."""
    tickers = await read_watchlist()
    log.info("Starting stock sync for %d tickers", len(tickers))
    for ticker in tickers:
        await sync_single_stock(ticker)
        await asyncio.sleep(2)
    log.info("Stock sync complete")
