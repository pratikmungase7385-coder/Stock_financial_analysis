from dotenv import load_dotenv
load_dotenv()

import os
import psycopg2


def init_db(reset=False):
    conn = psycopg2.connect(
        os.environ["DATABASE_URL"],
        sslmode="require"
    )
    cur = conn.cursor()

    # 🔥 ONLY when you EXPLICITLY want to delete everything
    if reset:
        print("⚠️ RESET MODE: DROPPING ALL TABLES")
        cur.execute("""
            DROP TABLE IF EXISTS documents;
            DROP TABLE IF EXISTS cashflow;
            DROP TABLE IF EXISTS profitandloss;
            DROP TABLE IF EXISTS balancesheet;
            DROP TABLE IF EXISTS prosandcons;
            DROP TABLE IF EXISTS analysis;
            DROP TABLE IF EXISTS companies;
        """)

    # ✅ SAFE CREATE (NO DATA LOSS)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS companies (
            company_id VARCHAR(20) PRIMARY KEY,
            company_logo VARCHAR(255),
            company_name VARCHAR(255),
            chart_link VARCHAR(255),
            about_company TEXT,
            website VARCHAR(255),
            nse_profile VARCHAR(255),
            bse_profile VARCHAR(255),
            face_value INT,
            book_value INT,
            roce_percentage DECIMAL(10,2),
            roe_percentage DECIMAL(10,2)
        );

        CREATE TABLE IF NOT EXISTS analysis (
            id SERIAL PRIMARY KEY,
            company_id VARCHAR(20),
            compounded_sales_growth VARCHAR(50),
            compounded_profit_growth VARCHAR(50),
            stock_price_cagr VARCHAR(50),
            roe VARCHAR(50)
        );

        CREATE TABLE IF NOT EXISTS prosandcons (
            id SERIAL PRIMARY KEY,
            company_id VARCHAR(20),
            pros TEXT,
            cons TEXT
        );

        CREATE TABLE IF NOT EXISTS balancesheet (
            id SERIAL PRIMARY KEY,
            company_id VARCHAR(20),
            year VARCHAR(10),
            equity_capital VARCHAR(20),
            reserves BIGINT,
            borrowings BIGINT,
            other_liabilities BIGINT,
            total_liabilities BIGINT,
            fixed_assets BIGINT,
            cwip BIGINT,
            investments BIGINT,
            other_asset BIGINT,
            total_assets BIGINT
        );

        CREATE TABLE IF NOT EXISTS profitandloss (
            id SERIAL PRIMARY KEY,
            company_id VARCHAR(20),
            year VARCHAR(10),
            sales BIGINT,
            expenses BIGINT,
            operating_profit BIGINT,
            opm_percentage INT,
            other_income BIGINT,
            interest BIGINT,
            depreciation BIGINT,
            profit_before_tax BIGINT,
            tax_percentage VARCHAR(20),
            net_profit BIGINT,
            eps INT,
            dividend_payout VARCHAR(20)
        );

        CREATE TABLE IF NOT EXISTS cashflow (
            id SERIAL PRIMARY KEY,
            company_id VARCHAR(20),
            year VARCHAR(10),
            operating_activity BIGINT,
            investing_activity BIGINT,
            financing_activity BIGINT,
            net_cash_flow BIGINT
        );

        CREATE TABLE IF NOT EXISTS documents (
            id SERIAL PRIMARY KEY,
            company_id VARCHAR(20),
            year INT,
            annual_report TEXT
        );
    """)

    conn.commit()
    cur.close()
    conn.close()

    print("✅ DB schema ensured (no data loss)")


if __name__ == "__main__":
    # ⚠️ CHANGE THIS ONLY WHEN YOU WANT FULL RESET
    init_db(reset=True)   # <-- run ONCE manually
