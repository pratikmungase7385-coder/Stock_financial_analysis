import os
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


SCHEMA_SQL = """
-- ================= companies =================
CREATE TABLE IF NOT EXISTS companies (
    company_id TEXT PRIMARY KEY,
    company_logo TEXT,
    company_name TEXT,
    chart_link TEXT,
    about_company TEXT,
    website TEXT,
    nse_profile TEXT,
    bse_profile TEXT,
    face_value NUMERIC,
    book_value NUMERIC,
    roce_percentage NUMERIC,
    roe_percentage NUMERIC
);

-- ================= analysis =================
CREATE TABLE IF NOT EXISTS analysis (
    id SERIAL PRIMARY KEY,
    company_id TEXT REFERENCES companies(company_id),
    compounded_sales_growth TEXT,
    compounded_profit_growth TEXT,
    stock_price_cagr TEXT,
    roe TEXT
);

-- ================= pros & cons =================
CREATE TABLE IF NOT EXISTS prosandcons (
    id SERIAL PRIMARY KEY,
    company_id TEXT REFERENCES companies(company_id),
    pros TEXT,
    cons TEXT
);

-- ================= balance sheet =================
CREATE TABLE IF NOT EXISTS balancesheet (
    id SERIAL PRIMARY KEY,
    company_id TEXT REFERENCES companies(company_id),
    year TEXT,
    equity_capital NUMERIC,
    reserves NUMERIC,
    borrowings NUMERIC,
    other_liabilities NUMERIC,
    total_liabilities NUMERIC,
    fixed_assets NUMERIC,
    cwip NUMERIC,
    investments NUMERIC,
    other_asset NUMERIC,
    total_assets NUMERIC
);

-- ================= profit & loss =================
CREATE TABLE IF NOT EXISTS profitandloss (
    id SERIAL PRIMARY KEY,
    company_id TEXT REFERENCES companies(company_id),
    year TEXT,
    sales NUMERIC,
    expenses NUMERIC,
    operating_profit NUMERIC,
    opm_percentage NUMERIC,
    other_income NUMERIC,
    interest NUMERIC,
    depreciation NUMERIC,
    profit_before_tax NUMERIC,
    tax_percentage NUMERIC,
    net_profit NUMERIC,
    eps NUMERIC,
    dividend_payout NUMERIC
);

-- ================= cashflow =================
CREATE TABLE IF NOT EXISTS cashflow (
    id SERIAL PRIMARY KEY,
    company_id TEXT REFERENCES companies(company_id),
    year TEXT,
    operating_activity NUMERIC,
    investing_activity NUMERIC,
    financing_activity NUMERIC,
    net_cash_flow NUMERIC
);

-- ================= documents =================
CREATE TABLE IF NOT EXISTS documents (
    id SERIAL PRIMARY KEY,
    company_id TEXT REFERENCES companies(company_id),
    year TEXT,
    annual_report TEXT
);
"""


def init_db():
    db_url = os.getenv("DATABASE_URL")

    if not db_url:
        raise RuntimeError("❌ DATABASE_URL env variable not set")

    conn = psycopg2.connect(
        db_url,
        sslmode="require"
    )

    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()

    cur.execute(SCHEMA_SQL)

    cur.close()
    conn.close()

    print("✅ Database schema ensured (tables created if missing)")
