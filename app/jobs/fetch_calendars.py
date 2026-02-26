from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, timedelta

import pandas as pd
import yfinance as yf
import curl_cffi as curl

from app.storage import write_earnings_calendar, write_economics_calendar
from app.jobs.parsers.forexfactory import extract_calendar_table, parse_economic_calendar

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


def _df_to_records(df: pd.DataFrame) -> list[dict]:
    df = df.reset_index()
    records = df.to_dict(orient="records")
    return [
        {k: _nan_to_none(v) for k, v in row.items()}
        for row in records
    ]


def _to_date(val) -> date | None:
    """Extract a date from a datetime-like value."""
    if val is None:
        return None
    try:
        if isinstance(val, datetime):
            return val.date()
        if isinstance(val, date):
            return val
        return date.fromisoformat(str(val)[:10])
    except (ValueError, TypeError, AttributeError):
        return None


def _fetch_earnings_raw() -> dict[date, dict[str, list[dict]]]:
    """Synchronous yfinance fetch — returns dict[day, dict[company, list[item]]]."""
    yesterday = datetime.now() - timedelta(days=1)
    cal = yf.Calendars(start=yesterday)

    result: dict[date, dict[str, list[dict]]] = {}
    offset = 0
    while True:
        df = cal.get_earnings_calendar(
            limit=100, offset=offset,
            market_cap=1_000_000_000, filter_most_active=False,
        )
        if df is None or df.empty:
            break
        records = _df_to_records(df)
        for r in records:
            sym = r.get("Symbol") or r.get("index", "")
            company = r.get("Company") or sym
            item = {
                "symbol": sym,
                "marketcap": r.get("Marketcap"),
                "event_name": r.get("Event Name"),
                "date": r.get("Event Start Date"),
                "timing": r.get("Timing"),
                "eps_estimate": r.get("EPS Estimate"),
                "reported_eps": r.get("Reported EPS"),
                "surprise_pct": r.get("Surprise(%)"),
            }
            key = _to_date(item["date"])
            if key is None:
                continue
            result.setdefault(key, {}).setdefault(company, []).append(item)
        if len(records) < 100:
            break
        offset += 100
    return result


async def sync_earnings_calendar() -> None:
    log.info("Syncing market earnings calendar")
    try:
        data = await asyncio.to_thread(_fetch_earnings_raw)
        if not data:
            log.warning("No earnings calendar data returned")
            return

        await write_earnings_calendar(data)
        total = sum(len(e) for day in data.values() for e in day.values())
        log.info("Synced %d earnings calendar items across %d days", total, len(data))
    except Exception:
        log.error("Failed to sync earnings calendar", exc_info=True)


def _parse_day(val) -> date | None:
    """Coerce a value to a date object. Accepts date, datetime, or ISO string."""
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    try:
        return date.fromisoformat(str(val)[:10])
    except (ValueError, TypeError):
        return None


def _fetch_economics_raw() -> list[dict]:
    """Synchronous HTTP fetch + parse."""
    url = "https://www.forexfactory.com/calendar"
    response = curl.get(url, impersonate="chrome")
    if response.status_code != 200:
        log.warning("Failed to fetch economics calendar: HTTP %d", response.status_code)
        return []
    table = extract_calendar_table(response.text)
    return parse_economic_calendar(table)


async def sync_economics_calendar() -> None:
    try:
        log.info("Syncing economic events calendar")
        events = await asyncio.to_thread(_fetch_economics_raw)
        if not events:
            log.warning("No economic events found in calendar data")
            return

        by_day: dict[date, list[dict]] = {}
        for ev in events:
            day = _parse_day(ev.get("date"))
            if day is None:
                continue
            by_day.setdefault(day, []).append(ev)

        await write_economics_calendar(by_day)
        log.info("Synced %d economic events across %d days", len(events), len(by_day))
    except Exception:
        log.error("Failed to sync economic events calendar", exc_info=True)


async def sync_all_calendars() -> None:
    await sync_earnings_calendar()
    await sync_economics_calendar()
