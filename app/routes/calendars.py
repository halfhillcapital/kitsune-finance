from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Query

from app.models import EarningsCalendarItem, EconomicsCalendarItem
from app.storage import read_earnings_calendar, read_economics_calendar

router = APIRouter(prefix="/calendar")

_DAY_FMT = "%A, %m/%d/%Y"


@router.get("/earnings", response_model=dict[str, dict[str, list[EarningsCalendarItem]]])
async def get_earnings_calendar(
    start: date | None = Query(None, description="Start date YYYY-MM-DD"),
    end: date | None = Query(None, description="End date YYYY-MM-DD"),
):
    data = await read_earnings_calendar(start=start, end=end)
    return {
        day.strftime(_DAY_FMT): {
            company: [EarningsCalendarItem(**i) for i in items]
            for company, items in companies.items()
        }
        for day, companies in data.items()
    }


@router.get("/economics", response_model=dict[str, list[EconomicsCalendarItem]])
async def get_economics_calendar(
    start: date | None = Query(None, description="Start date YYYY-MM-DD"),
    end: date | None = Query(None, description="End date YYYY-MM-DD"),
):
    data = await read_economics_calendar(start=start, end=end)
    return {
        day.strftime(_DAY_FMT): [EconomicsCalendarItem(**e) for e in events]
        for day, events in data.items()
    }
