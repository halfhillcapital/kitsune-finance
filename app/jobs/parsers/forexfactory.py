from __future__ import annotations

from datetime import date, datetime, time, timezone
from zoneinfo import ZoneInfo

from lxml import html
from lxml.html import tostring

_CET = ZoneInfo("Europe/Berlin")


_IMPACT_MAP = {
    "red": "High",
    "ora": "Medium",
    "yel": "Low",
    "gra": "Non-Economic",
}


def _text(el: html.HtmlElement | None) -> str | None:
    """Return stripped text_content of *el*, or None if blank/missing."""
    if el is None:
        return None
    t = el.text_content().strip()
    return t or None


def _impact_label(td: html.HtmlElement) -> str | None:
    """Extract impact level from the icon class on the <span> inside *td*."""
    for span in td.iter("span"):
        for cls in span.classes:
            if cls.startswith("icon--ff-impact-"):
                suffix = cls.rsplit("-", 1)[-1]
                return _IMPACT_MAP.get(suffix)
    return None


def extract_calendar_table(page_html: str) -> str:
    """Extract the raw ``<table class="calendar__table">`` from a full page."""
    doc = html.fromstring(page_html)
    tables = doc.find_class("calendar__table")
    if not tables:
        raise ValueError("No <table class='calendar__table'> found in HTML")
    return str(tostring(tables[0], encoding="unicode"))


def _resolve_date(raw: str) -> date:
    """Parse a ForexFactory date like 'Thu Feb 26' into a date with correct year.

    ForexFactory doesn't include the year, so we infer it: if the resulting date
    is more than 3 months in the future, it's probably last year; if more than
    9 months in the past, it's probably next year.
    """
    parsed = datetime.strptime(raw, "%a %b %d").date()
    today = date.today()
    candidate = parsed.replace(year=today.year)
    delta = (candidate - today).days
    if delta > 90:
        candidate = candidate.replace(year=today.year - 1)
    elif delta < -270:
        candidate = candidate.replace(year=today.year + 1)
    return candidate


def _parse_time(raw: str | None) -> time | None:
    """Parse a ForexFactory time like '8:30am' into a time object.

    Returns None for non-time values like 'All Day', 'Tentative', or None.
    """
    if not raw:
        return None
    raw = raw.strip().lower()
    for fmt in ("%I:%M%p", "%I:%M %p"):
        try:
            return datetime.strptime(raw, fmt).time()
        except ValueError:
            continue
    return None


def parse_economic_calendar(raw_html: str) -> list[dict]:
    """Parse a ForexFactory economic-calendar HTML table into a flat list.

    Each returned dict contains:
        date, is_all_day, currency, impact, event, actual, forecast, previous
    """
    doc = html.fromstring(raw_html)
    rows = doc.xpath('//tr[@data-event-id]')

    events: list[dict] = []
    current_date: date | None = None
    current_time_str: str | None = None

    for row in rows:
        # --- date (only present on first row of each day) ---
        date_td = row.find_class("calendar__date")
        if date_td:
            raw = _text(date_td[0])
            if raw:
                current_date = _resolve_date(raw)
                current_time_str = None  # reset time on new day

        # --- time (empty means same as previous row) ---
        time_td = row.find_class("calendar__time")
        if time_td:
            t = _text(time_td[0])
            if t:
                current_time_str = t

        # --- currency ---
        cur_td = row.find_class("calendar__currency")
        currency = _text(cur_td[0]) if cur_td else None

        # --- impact ---
        imp_td = row.find_class("calendar__impact")
        impact = _impact_label(imp_td[0]) if imp_td else None

        # --- event title ---
        title_spans = row.find_class("calendar__event-title")
        event_name = _text(title_spans[0]) if title_spans else None

        # --- actual / forecast / previous ---
        actual = _text(row.find_class("calendar__actual")[0]) if row.find_class("calendar__actual") else None
        forecast = _text(row.find_class("calendar__forecast")[0]) if row.find_class("calendar__forecast") else None
        previous = _text(row.find_class("calendar__previous")[0]) if row.find_class("calendar__previous") else None

        # --- build full datetime ---
        is_all_day = False
        event_dt: datetime | None = None
        if current_date is not None:
            parsed_time = _parse_time(current_time_str)
            if parsed_time is None:
                # "All Day", "Tentative", or missing → midnight UTC + all-day flag
                is_all_day = True
                event_dt = datetime.combine(current_date, time.min, tzinfo=timezone.utc)
            else:
                # Times from ForexFactory are in CET — convert to UTC
                event_dt = datetime.combine(current_date, parsed_time, tzinfo=_CET).astimezone(timezone.utc)

        events.append(
            {
                "date": event_dt,
                "is_all_day": is_all_day,
                "currency": currency,
                "impact": impact,
                "event": event_name,
                "actual": actual,
                "forecast": forecast,
                "previous": previous,
            }
        )

    return events
