from __future__ import annotations

from fastapi import APIRouter
from pydantic import BaseModel

from jobs.fetch_calendars import sync_all_calendars
from jobs.fetch_stock import sync_all_stocks, sync_single_stock
from storage import add_to_watchlist, read_watchlist, remove_from_watchlist

router = APIRouter(prefix="/admin")


class AddTickersRequest(BaseModel):
    tickers: list[str]


@router.get("/watchlist")
async def get_watchlist() -> list[str]:
    return await read_watchlist()


@router.post("/watchlist")
async def add_tickers(req: AddTickersRequest) -> list[str]:
    for ticker in req.tickers:
        await add_to_watchlist(ticker)
        await sync_single_stock(ticker.upper())
    return await read_watchlist()


@router.delete("/watchlist/{ticker}")
async def remove_ticker(ticker: str) -> list[str]:
    await remove_from_watchlist(ticker)
    return await read_watchlist()


@router.post("/sync")
async def trigger_sync() -> dict:
    await sync_all_stocks()
    await sync_all_calendars()
    return {"status": "ok"}
