from dotenv import load_dotenv
load_dotenv()

import os
import psycopg2

def init_db():
    db = psycopg2.connect(
        os.environ["DATABASE_URL"],
        sslmode="require"
    )
    cur = db.cursor()

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
        id INT,
        company_id VARCHAR(20),
        compounded_sales_growth VARCHAR(50),
        compounded_profit_growth VARCHAR(50),
        stock_price_cagr VARCHAR(50),
        roe VARCHAR(50)
    );

    CREATE TABLE IF NOT EXISTS prosandcons (
        id INT,
        company_id VARCHAR(20),
        pros TEXT,
        cons TEXT
    );

    CREATE TABLE IF NOT EXISTS balancesheet (
        id INT,
        company_id VARCHAR(20),
        year VARCHAR(20),
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
        id INT,
        company_id VARCHAR(20),
        year VARCHAR(20),
        sales BIGINT,
        expenses BIGINT,
        operating_profit BIGINT,
        opm_percentage INT,
        other_income BIGINT,
        interest BIGINT,
        depreciation BIGINT,
        profit_before_tax BIGINT,
        tax_percentage VARCHAR(10),
        net_profit BIGINT,
        eps INT,
        dividend_payout VARCHAR(10)
    );

    CREATE TABLE IF NOT EXISTS cashflow (
        id INT,
        company_id VARCHAR(20),
        year VARCHAR(20),
        operating_activity BIGINT,
        investing_activity BIGINT,
        financing_activity BIGINT,
        net_cash_flow BIGINT
    );

    CREATE TABLE IF NOT EXISTS documents (
        id INT,
        company_id VARCHAR(20),
        year INT,
        annual_report VARCHAR(255)
    );
    """)

    db.commit()
    cur.close()
    db.close()
    print("✅ Database schema ensured (tables created if missing)")

if __name__ == "__main__":
    init_db()
