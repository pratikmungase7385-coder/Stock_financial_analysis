from fetch import get_company_ids, fetch_company, save_raw_json
import psycopg2
import time

# ================= DATABASE =================
DB = psycopg2.connect(
    host="localhost",
    user="postgres",        # apna user
    password="postgres",    # apna password
    dbname="ml",
    port=5432
)
cur = DB.cursor()

# ================= GENERIC INSERT =================
def insert_rows(table, rows, cols):
    if not rows:
        return

    for r in rows:
        try:
            values = [r.get(c) for c in cols]
            cur.execute(
                f"""
                INSERT INTO {table} ({','.join(cols)})
                VALUES ({','.join(['%s'] * len(cols))})
                """,
                values
            )
        except Exception as e:
            # 🔥 mandatory rollback in PostgreSQL
            DB.rollback()
            print(f"⚠️ Skipped row in {table}: {e}")

# ================= MAIN PIPELINE =================
def main():
    company_ids = get_company_ids()
    print("Total companies:", len(company_ids))

    for cid in company_ids:
        print(f"\nFetching → {cid}")

        data = fetch_company(cid)
        if not data:
            print("❌ API failed")
            continue

        save_raw_json(cid, data)

        company = data.get("company")
        d = data.get("data")

        if not company or not d:
            print("❌ Invalid API structure")
            continue

        # ---------- companies ----------
        try:
            cur.execute("""
                INSERT INTO companies (
                    company_id, company_logo, company_name, chart_link,
                    about_company, website, nse_profile, bse_profile,
                    face_value, book_value, roce_percentage, roe_percentage
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (company_id) DO NOTHING
            """, (
                company.get("id"),
                company.get("company_logo"),
                company.get("company_name"),
                company.get("chart_link"),
                company.get("about_company"),
                company.get("website"),
                company.get("nse_profile"),
                company.get("bse_profile"),
                company.get("face_value"),
                company.get("book_value"),
                company.get("roce_percentage"),
                company.get("roe_percentage")
            ))
            DB.commit()
        except Exception as e:
            DB.rollback()
            print("❌ Company insert failed:", e)
            continue

        # ---------- analysis ----------
        insert_rows(
            "analysis",
            d.get("analysis", []),
            [
                "id",
                "company_id",
                "compounded_sales_growth",
                "compounded_profit_growth",
                "stock_price_cagr",
                "roe"
            ]
        )

        # ---------- pros & cons ----------
        insert_rows(
            "prosandcons",
            d.get("prosandcons", []),
            ["id", "company_id", "pros", "cons"]
        )

        # ---------- balance sheet ----------
        insert_rows(
            "balancesheet",
            d.get("balancesheet", []),
            [
                "id", "company_id", "year", "equity_capital", "reserves",
                "borrowings", "other_liabilities", "total_liabilities",
                "fixed_assets", "cwip", "investments",
                "other_asset", "total_assets"
            ]
        )

        # ---------- profit & loss ----------
        insert_rows(
            "profitandloss",
            d.get("profitandloss", []),
            [
                "id", "company_id", "year", "sales", "expenses",
                "operating_profit", "opm_percentage", "other_income",
                "interest", "depreciation", "profit_before_tax",
                "tax_percentage", "net_profit", "eps", "dividend_payout"
            ]
        )

        # ---------- cashflow ----------
        insert_rows(
            "cashflow",
            d.get("cashflow", []),
            [
                "id", "company_id", "year", "operating_activity",
                "investing_activity", "financing_activity",
                "net_cash_flow"
            ]
        )

        # ---------- documents ----------
        insert_rows(
            "documents",
            d.get("documents", []),
            ["id", "company_id", "year", "annual_report"]
        )

        DB.commit()
        print("✅ Saved")

        time.sleep(1)  # API safe delay

    cur.close()
    DB.close()
    print("\nALL COMPANIES PROCESSED")

# ================= RUN =================
if __name__ == "__main__":
    main()
