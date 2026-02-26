CREATE TABLE IF NOT EXISTS watchlist (
    ticker TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS stock_calendar (
    ticker            TEXT PRIMARY KEY,
    dividend_date     DATE,
    ex_dividend_date  DATE,
    earnings_dates    JSONB,
    earnings_high     DOUBLE PRECISION,
    earnings_low      DOUBLE PRECISION,
    earnings_average  DOUBLE PRECISION,
    revenue_high      DOUBLE PRECISION,
    revenue_low       DOUBLE PRECISION,
    revenue_average   DOUBLE PRECISION,
    updated_at        TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS stock_earnings (
    id            SERIAL PRIMARY KEY,
    ticker        TEXT NOT NULL,
    date          TIMESTAMPTZ NOT NULL,
    eps_estimate  DOUBLE PRECISION,
    reported_eps  DOUBLE PRECISION,
    surprise_pct  DOUBLE PRECISION,
    UNIQUE (ticker, date)
);

CREATE TABLE IF NOT EXISTS stock_dividends (
    id      SERIAL PRIMARY KEY,
    ticker  TEXT NOT NULL,
    date    DATE NOT NULL,
    amount  DOUBLE PRECISION,
    UNIQUE (ticker, date)
);

CREATE TABLE IF NOT EXISTS stock_splits (
    id      SERIAL PRIMARY KEY,
    ticker  TEXT NOT NULL,
    date    DATE NOT NULL,
    ratio   TEXT,
    UNIQUE (ticker, date)
);

CREATE TABLE IF NOT EXISTS earnings_calendar (
    id            SERIAL PRIMARY KEY,
    company       TEXT,
    symbol        TEXT NOT NULL,
    marketcap     DOUBLE PRECISION,
    event_name    TEXT,
    date          TIMESTAMPTZ,
    timing        TEXT,
    eps_estimate  DOUBLE PRECISION,
    reported_eps  DOUBLE PRECISION,
    surprise_pct  DOUBLE PRECISION,
    UNIQUE (symbol, date)
);

CREATE TABLE IF NOT EXISTS economics_calendar (
    id          SERIAL PRIMARY KEY,
    date        TIMESTAMPTZ,
    is_all_day  BOOLEAN NOT NULL DEFAULT FALSE,
    currency    TEXT,
    impact      TEXT,
    event       TEXT NOT NULL,
    actual      TEXT,
    forecast    TEXT,
    previous    TEXT,
    UNIQUE (date, event)
);
