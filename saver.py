from dotenv import load_dotenv
load_dotenv()

import os
import json
import psycopg2

RAW_DIR = "raw_data"


# ================= DB CONNECTION =================
def get_db():
    return psycopg2.connect(
        os.environ["DATABASE_URL"],
        sslmode="require"
    )


# ================= GENERIC INSERT (NO id) =================
def insert_rows(cur, table, rows, cols):
    if not rows:
        return

    for r in rows:
        values = [r.get(c) for c in cols]
        placeholders = ",".join(["%s"] * len(cols))

        cur.execute(
            f"""
            INSERT INTO {table} ({','.join(cols)})
            VALUES ({placeholders})
            """,
            values
        )


# ================= MAIN SAVER =================
def main():
    if not os.path.exists(RAW_DIR):
        print("❌ raw_data folder not found")
        return

    db = get_db()
    cur = db.cursor()

    files = os.listdir(RAW_DIR)
    print("Total raw files:", len(files))

    for file in files:
        path = os.path.join(RAW_DIR, file)

        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        company = data.get("company")
        d = data.get("data")

        if not company or not d:
            print("❌ Invalid file:", file)
            continue

        cid = company.get("id")
        print("Saving →", cid)

        try:
            # ---------- companies ----------
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

            # ---------- analysis ----------
            insert_rows(cur, "analysis", d.get("analysis", []), [
                "company_id",
                "compounded_sales_growth",
                "compounded_profit_growth",
                "stock_price_cagr",
                "roe"
            ])

            # ---------- pros & cons ----------
            insert_rows(cur, "prosandcons", d.get("prosandcons", []), [
                "company_id",
                "pros",
                "cons"
            ])

            # ---------- balance sheet ----------
            insert_rows(cur, "balancesheet", d.get("balancesheet", []), [
                "company_id", "year", "equity_capital", "reserves",
                "borrowings", "other_liabilities", "total_liabilities",
                "fixed_assets", "cwip", "investments",
                "other_asset", "total_assets"
            ])

            # ---------- profit & loss ----------
            insert_rows(cur, "profitandloss", d.get("profitandloss", []), [
                "company_id", "year", "sales", "expenses",
                "operating_profit", "opm_percentage", "other_income",
                "interest", "depreciation", "profit_before_tax",
                "tax_percentage", "net_profit", "eps", "dividend_payout"
            ])

            # ---------- cashflow ----------
            insert_rows(cur, "cashflow", d.get("cashflow", []), [
                "company_id", "year", "operating_activity",
                "investing_activity", "financing_activity",
                "net_cash_flow"
            ])

            # ---------- documents (FIXED KEYS) ----------
            docs = []
            for doc in d.get("documents", []):
                docs.append({
                    "company_id": cid,
                    "year": doc.get("Year"),
                    "annual_report": doc.get("Annual_Report")
                })

            insert_rows(cur, "documents", docs, [
                "company_id", "year", "annual_report"
            ])

            db.commit()
            print("✅ Saved", cid)

        except Exception as e:
            db.rollback()
            print("❌ Failed for", cid, e)

    cur.close()
    db.close()
    print("\nALL DATA SAVED")


# ================= RUN =================
if __name__ == "__main__":
    main()
