from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from app.jobs.fetch_stock import sync_single_stock
from app.models import DividendRecord, EarningsDate, SplitRecord, StockCalendar
from app.storage import add_to_watchlist, read_stock

router = APIRouter(prefix="/stocks")


async def _load_stock(ticker: str) -> dict:
    """Load stock data from DB, auto-fetching if not yet cached."""
    data = await read_stock(ticker)
    if data is None:
        await add_to_watchlist(ticker)
        await sync_single_stock(ticker)
        data = await read_stock(ticker)
    if not isinstance(data, dict):
        raise HTTPException(status_code=502, detail=f"Failed to fetch data for {ticker}")
    return data


@router.get("/{ticker}/calendar", response_model=StockCalendar)
async def get_stock_calendar(ticker: str):
    data = await _load_stock(ticker)
    cal = data.get("calendar")
    if not cal:
        raise HTTPException(status_code=404, detail=f"No calendar data for {ticker}")
    return StockCalendar(**cal)


@router.get("/{ticker}/earnings", response_model=list[EarningsDate])
async def get_stock_earnings(
    ticker: str,
    limit: int = Query(12, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    data = await _load_stock(ticker)
    earnings = data.get("earnings", [])
    return [EarningsDate(**e) for e in earnings[offset : offset + limit]]


@router.get("/{ticker}/dividends", response_model=list[DividendRecord])
async def get_stock_dividends(ticker: str):
    data = await _load_stock(ticker)
    return [DividendRecord(**d) for d in data.get("dividends", [])]


@router.get("/{ticker}/splits", response_model=list[SplitRecord])
async def get_stock_splits(ticker: str):
    data = await _load_stock(ticker)
    return [SplitRecord(**s) for s in data.get("splits", [])]
