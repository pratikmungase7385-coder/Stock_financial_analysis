from dotenv import load_dotenv
load_dotenv()

import os
import time
import psycopg2

from fetch import get_company_ids, fetch_company, save_raw_json


# ================= DB CONNECT =================
def get_conn():
    return psycopg2.connect(
        os.environ["DATABASE_URL"],
        sslmode="require"
    )


def get_db():
    conn = psycopg2.connect(
        os.environ["DATABASE_URL"],
        sslmode="require",
        connect_timeout=5
    )
    conn.autocommit = True
    return conn


# ================= SAFE INSERT =================
def insert_rows(conn, table, rows, cols):
    if not rows:
        return

    for r in rows:
        cur = conn.cursor()
        try:
            values = [r.get(c) for c in cols]
            cur.execute(
                f"""
                INSERT INTO {table} ({','.join(cols)})
                VALUES ({','.join(['%s'] * len(cols))})
                """,
                values
            )
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(f"⚠️ {table} skipped:", e)
        finally:
            cur.close()


# ================= MAIN PIPELINE =================
def main():
    company_ids = get_company_ids()
    print("Total companies:", len(company_ids))

    conn = get_conn()

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

        try:
            # ---------- COMPANIES ----------
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO companies (
                    company_id, company_logo, company_name, chart_link,
                    about_company, website, nse_profile, bse_profile,
                    face_value, book_value, roce_percentage, roe_percentage
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
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
            conn.commit()
            cur.close()

            # ---------- ANALYSIS ----------
            insert_rows(conn, "analysis", d.get("analysis", []), [
                "company_id",
                "compounded_sales_growth",
                "compounded_profit_growth",
                "stock_price_cagr",
                "roe"
            ])

            # ---------- PROS & CONS ----------
            insert_rows(conn, "prosandcons", d.get("prosandcons", []), [
                "company_id", "pros", "cons"
            ])

            # ---------- BALANCE SHEET ----------
            insert_rows(conn, "balancesheet", d.get("balancesheet", []), [
                "company_id", "year", "equity_capital", "reserves",
                "borrowings", "other_liabilities", "total_liabilities",
                "fixed_assets", "cwip", "investments",
                "other_asset", "total_assets"
            ])

            # ---------- PROFIT & LOSS ----------
            insert_rows(conn, "profitandloss", d.get("profitandloss", []), [
                "company_id", "year", "sales", "expenses",
                "operating_profit", "opm_percentage", "other_income",
                "interest", "depreciation", "profit_before_tax",
                "tax_percentage", "net_profit", "eps", "dividend_payout"
            ])

            # ---------- CASHFLOW ----------
            insert_rows(conn, "cashflow", d.get("cashflow", []), [
                "company_id", "year", "operating_activity",
                "investing_activity", "financing_activity",
                "net_cash_flow"
            ])

            # ---------- DOCUMENTS (FIXED KEYS) ----------
            docs_clean = [{
                "company_id": cid,
                "year": doc.get("Year"),
                "annual_report": doc.get("Annual_Report")
            } for doc in d.get("documents", [])]

            insert_rows(conn, "documents", docs_clean, [
                "company_id", "year", "annual_report"
            ])

            print("✅ Saved", cid)

        except Exception as e:
            conn.rollback()
            print("❌ Failed:", cid, e)

        time.sleep(1)  # API safety

    conn.close()
    print("\nALL COMPANIES PROCESSED")


# ================= RUN =================
if __name__ == "__main__":
    main()
