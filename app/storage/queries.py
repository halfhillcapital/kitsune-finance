from __future__ import annotations

import json
from datetime import date, datetime

import app.database as db


async def read_watchlist() -> list[str]:
    async with db.get_pool().acquire() as conn:
        rows = await conn.fetch("SELECT ticker FROM watchlist ORDER BY ticker")
    return [r["ticker"] for r in rows]


async def add_to_watchlist(ticker: str) -> None:
    upper = ticker.upper()
    async with db.get_pool().acquire() as conn:
        await conn.execute(
            "INSERT INTO watchlist (ticker) VALUES ($1) ON CONFLICT DO NOTHING",
            upper,
        )


async def remove_from_watchlist(ticker: str) -> None:
    upper = ticker.upper()
    async with db.get_pool().acquire() as conn:
        await conn.execute("DELETE FROM watchlist WHERE ticker = $1", upper)


async def read_stock(ticker: str) -> dict | None:
    upper = ticker.upper()
    async with db.get_pool().acquire() as conn:
        cal = await conn.fetchrow(
            "SELECT * FROM stock_calendar WHERE ticker = $1", upper
        )
        earnings = await conn.fetch(
            "SELECT date, eps_estimate, reported_eps, surprise_pct "
            "FROM stock_earnings WHERE ticker = $1 ORDER BY date DESC",
            upper,
        )
        dividends = await conn.fetch(
            "SELECT date, amount FROM stock_dividends WHERE ticker = $1 ORDER BY date DESC",
            upper,
        )
        splits = await conn.fetch(
            "SELECT date, ratio FROM stock_splits WHERE ticker = $1 ORDER BY date DESC",
            upper,
        )

    if not cal and not earnings and not dividends and not splits:
        return None

    calendar_data = None
    if cal:
        earnings_dates = cal["earnings_dates"]
        if isinstance(earnings_dates, str):
            earnings_dates = json.loads(earnings_dates)
        calendar_data = {
            "dividend_date": _date_str(cal["dividend_date"]),
            "ex_dividend_date": _date_str(cal["ex_dividend_date"]),
            "earnings_dates": earnings_dates,
            "earnings_high": cal["earnings_high"],
            "earnings_low": cal["earnings_low"],
            "earnings_average": cal["earnings_average"],
            "revenue_high": cal["revenue_high"],
            "revenue_low": cal["revenue_low"],
            "revenue_average": cal["revenue_average"],
        }

    return {
        "calendar": calendar_data,
        "earnings": [
            {
                "date": r["date"].isoformat() if r["date"] else None,
                "eps_estimate": r["eps_estimate"],
                "reported_eps": r["reported_eps"],
                "surprise_pct": r["surprise_pct"],
            }
            for r in earnings
        ],
        "dividends": [
            {"date": _date_str(r["date"]), "amount": r["amount"]}
            for r in dividends
        ],
        "splits": [
            {"date": _date_str(r["date"]), "ratio": r["ratio"]}
            for r in splits
        ],
    }


async def write_stock(ticker: str, data: dict) -> None:
    upper = ticker.upper()
    cal = data.get("calendar") or {}
    earnings_dates = cal.get("earnings_dates")
    if earnings_dates is not None:
        earnings_dates = json.dumps(earnings_dates, default=str)

    async with db.get_pool().acquire() as conn:
        async with conn.transaction():
            # Upsert calendar
            await conn.execute(
                """
                INSERT INTO stock_calendar
                    (ticker, dividend_date, ex_dividend_date, earnings_dates,
                     earnings_high, earnings_low, earnings_average,
                     revenue_high, revenue_low, revenue_average, updated_at)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10, now())
                ON CONFLICT (ticker) DO UPDATE SET
                    dividend_date = EXCLUDED.dividend_date,
                    ex_dividend_date = EXCLUDED.ex_dividend_date,
                    earnings_dates = EXCLUDED.earnings_dates,
                    earnings_high = EXCLUDED.earnings_high,
                    earnings_low = EXCLUDED.earnings_low,
                    earnings_average = EXCLUDED.earnings_average,
                    revenue_high = EXCLUDED.revenue_high,
                    revenue_low = EXCLUDED.revenue_low,
                    revenue_average = EXCLUDED.revenue_average,
                    updated_at = now()
                """,
                upper,
                _to_date(cal.get("dividend_date")),
                _to_date(cal.get("ex_dividend_date")),
                earnings_dates,
                _to_float(cal.get("earnings_high")),
                _to_float(cal.get("earnings_low")),
                _to_float(cal.get("earnings_average")),
                _to_float(cal.get("revenue_high")),
                _to_float(cal.get("revenue_low")),
                _to_float(cal.get("revenue_average")),
            )

            # Upsert earnings
            for e in data.get("earnings", []):
                dt = _to_datetime(e.get("date"))
                if dt is None:
                    continue
                await conn.execute(
                    """
                    INSERT INTO stock_earnings (ticker, date, eps_estimate, reported_eps, surprise_pct)
                    VALUES ($1,$2,$3,$4,$5)
                    ON CONFLICT (ticker, date) DO UPDATE SET
                        eps_estimate = COALESCE(EXCLUDED.eps_estimate, stock_earnings.eps_estimate),
                        reported_eps = COALESCE(EXCLUDED.reported_eps, stock_earnings.reported_eps),
                        surprise_pct = COALESCE(EXCLUDED.surprise_pct, stock_earnings.surprise_pct)
                    """,
                    upper, dt,
                    _to_float(e.get("eps_estimate")),
                    _to_float(e.get("reported_eps")),
                    _to_float(e.get("surprise_pct")),
                )

            # Upsert dividends
            for d in data.get("dividends", []):
                dt = _to_date(d.get("date"))
                if dt is None:
                    continue
                await conn.execute(
                    """
                    INSERT INTO stock_dividends (ticker, date, amount)
                    VALUES ($1,$2,$3)
                    ON CONFLICT (ticker, date) DO UPDATE SET amount = EXCLUDED.amount
                    """,
                    upper, dt, _to_float(d.get("amount")),
                )

            # Upsert splits
            for s in data.get("splits", []):
                dt = _to_date(s.get("date"))
                if dt is None:
                    continue
                await conn.execute(
                    """
                    INSERT INTO stock_splits (ticker, date, ratio)
                    VALUES ($1,$2,$3)
                    ON CONFLICT (ticker, date) DO UPDATE SET ratio = EXCLUDED.ratio
                    """,
                    upper, dt, s.get("ratio"),
                )


async def read_earnings_calendar(
    start: date | None = None, end: date | None = None,
) -> dict[date, dict[str, list[dict]]]:
    clauses = ["1=1"]
    args: list = []
    if start:
        args.append(start)
        clauses.append(f"day >= ${len(args)}")
    if end:
        args.append(end)
        clauses.append(f"day <= ${len(args)}")
    sql = (
        "SELECT * FROM earnings_calendar WHERE "
        + " AND ".join(clauses)
        + " ORDER BY day DESC, id"
    )
    async with db.get_pool().acquire() as conn:
        rows = await conn.fetch(sql, *args)
    result: dict[date, dict[str, list[dict]]] = {}
    for r in rows:
        day = r["day"]
        company = r["company"] or r["symbol"]
        item = {
            "symbol": r["symbol"],
            "marketcap": r["marketcap"],
            "event_name": r["event_name"],
            "date": r["date"].isoformat() if r["date"] else None,
            "timing": r["timing"],
            "eps_estimate": r["eps_estimate"],
            "reported_eps": r["reported_eps"],
            "surprise_pct": r["surprise_pct"],
        }
        result.setdefault(day, {}).setdefault(company, []).append(item)
    return result


async def write_earnings_calendar(data: dict[date, dict[str, list[dict]]]) -> None:
    async with db.get_pool().acquire() as conn:
        async with conn.transaction():
            for day, companies in data.items():
                for company, items in companies.items():
                    for item in items:
                        dt = _to_datetime(item.get("date"))
                        await conn.execute(
                            """
                            INSERT INTO earnings_calendar
                                (day, company, symbol, marketcap, event_name, date, timing,
                                 eps_estimate, reported_eps, surprise_pct)
                            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                            ON CONFLICT (symbol, date) DO UPDATE SET
                                day = EXCLUDED.day,
                                company = EXCLUDED.company,
                                marketcap = COALESCE(EXCLUDED.marketcap, earnings_calendar.marketcap),
                                event_name = COALESCE(EXCLUDED.event_name, earnings_calendar.event_name),
                                timing = COALESCE(EXCLUDED.timing, earnings_calendar.timing),
                                eps_estimate = COALESCE(EXCLUDED.eps_estimate, earnings_calendar.eps_estimate),
                                reported_eps = COALESCE(EXCLUDED.reported_eps, earnings_calendar.reported_eps),
                                surprise_pct = COALESCE(EXCLUDED.surprise_pct, earnings_calendar.surprise_pct)
                            """,
                            day, company, item.get("symbol", ""),
                            _to_float(item.get("marketcap")),
                            item.get("event_name"),
                            dt,
                            item.get("timing"),
                            _to_float(item.get("eps_estimate")),
                            _to_float(item.get("reported_eps")),
                            _to_float(item.get("surprise_pct")),
                        )


async def read_economics_calendar(
    start: date | None = None, end: date | None = None,
) -> dict[date, list[dict]]:
    clauses = ["1=1"]
    args: list = []
    if start:
        args.append(start)
        clauses.append(f"day >= ${len(args)}")
    if end:
        args.append(end)
        clauses.append(f"day <= ${len(args)}")
    sql = (
        "SELECT * FROM economics_calendar WHERE "
        + " AND ".join(clauses)
        + " ORDER BY day, id"
    )
    async with db.get_pool().acquire() as conn:
        rows = await conn.fetch(sql, *args)
    result: dict[date, list[dict]] = {}
    for r in rows:
        item = {
            "time": r["time"],
            "currency": r["currency"],
            "impact": r["impact"],
            "event": r["event"],
            "actual": r["actual"],
            "forecast": r["forecast"],
            "previous": r["previous"],
        }
        result.setdefault(r["day"], []).append(item)
    return result


async def write_economics_calendar(data: dict[date, list[dict]]) -> None:
    async with db.get_pool().acquire() as conn:
        async with conn.transaction():
            for day, events in data.items():
                for ev in events:
                    event_name = ev.get("event")
                    if not event_name:
                        continue
                    await conn.execute(
                        """
                        INSERT INTO economics_calendar
                            (day, time, currency, impact, event, actual, forecast, previous)
                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                        ON CONFLICT (day, event) DO UPDATE SET
                            time = COALESCE(EXCLUDED.time, economics_calendar.time),
                            currency = COALESCE(EXCLUDED.currency, economics_calendar.currency),
                            impact = COALESCE(EXCLUDED.impact, economics_calendar.impact),
                            actual = COALESCE(EXCLUDED.actual, economics_calendar.actual),
                            forecast = COALESCE(EXCLUDED.forecast, economics_calendar.forecast),
                            previous = COALESCE(EXCLUDED.previous, economics_calendar.previous)
                        """,
                        day,
                        ev.get("time"),
                        ev.get("currency"),
                        ev.get("impact"),
                        event_name,
                        ev.get("actual"),
                        ev.get("forecast"),
                        ev.get("previous"),
                    )


# --- helpers ---

def _date_str(val) -> str | None:
    if val is None:
        return None
    if isinstance(val, (date, datetime)):
        return val.isoformat()
    return str(val)


def _to_date(val) -> date | None:
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


def _to_datetime(val) -> datetime | None:
    if val is None:
        return None
    if isinstance(val, datetime):
        return val
    try:
        return datetime.fromisoformat(str(val))
    except (ValueError, TypeError):
        return None


def _to_float(val) -> float | None:
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None
