from __future__ import annotations

from datetime import date, datetime

from pydantic import BaseModel


# --- Stock-specific models (from yf.Ticker) ---


class StockCalendar(BaseModel):
    dividend_date: date | None = None
    ex_dividend_date: date | None = None
    earnings_dates: list[datetime] | None = None
    earnings_high: float | None = None
    earnings_low: float | None = None
    earnings_average: float | None = None
    revenue_high: float | None = None
    revenue_low: float | None = None
    revenue_average: float | None = None


class EarningsDate(BaseModel):
    date: datetime
    eps_estimate: float | None = None
    reported_eps: float | None = None
    surprise_pct: float | None = None


class DividendRecord(BaseModel):
    date: date
    amount: float


class SplitRecord(BaseModel):
    date: date
    ratio: str  # e.g. "4:1"


# --- Market-wide calendar models (from yf.Calendars) ---


class EarningsCalendarItem(BaseModel):
    symbol: str
    marketcap: float | None = None
    event_name: str | None = None
    date: datetime | None = None
    timing: str | None = None
    eps_estimate: float | None = None
    reported_eps: float | None = None
    surprise_pct: float | None = None


class EconomicsCalendarItem(BaseModel):
    date: datetime | None = None
    is_all_day: bool = False
    currency: str | None = None
    impact: str | None = None
    event: str | None = None
    actual: str | None = None
    forecast: str | None = None
    previous: str | None = None


