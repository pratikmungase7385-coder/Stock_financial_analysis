# from dotenv import load_dotenv
# load_dotenv()


# import json
# import os
# import psycopg2

# RAW_DIR = "raw_data"

# import os
# import psycopg2

# DB = psycopg2.connect(
#     os.environ["DATABASE_URL"],
#     sslmode="require"
# )
# cur = DB.cursor()


# def insert_rows(table, rows, cols):
#     if not rows:
#         return

#     for r in rows:
#         try:
#             values = [r.get(c) for c in cols]
#             cur.execute(
#                 f"""
#                 INSERT INTO {table} ({','.join(cols)})
#                 VALUES ({','.join(['%s'] * len(cols))})
#                 ON CONFLICT (id) DO NOTHING
#                 """,
#                 values
#             )
#         except Exception as e:
#             DB.rollback()
#             print(f"⚠️ Skipped row in {table}: {e}")


# def main():
#     files = os.listdir(RAW_DIR)
#     print("Total raw files:", len(files))

#     for file in files:
#         path = os.path.join(RAW_DIR, file)

#         with open(path, "r", encoding="utf-8") as f:
#             data = json.load(f)

#         company = data.get("company")
#         d = data.get("data")

#         if not company or not d:
#             print("❌ Invalid file:", file)
#             continue

#         cid = company.get("id")
#         print("Saving →", cid)

       
#         try:
#             cur.execute("""
#                 INSERT INTO companies (
#                     company_id, company_logo, company_name, chart_link,
#                     about_company, website, nse_profile, bse_profile,
#                     face_value, book_value, roce_percentage, roe_percentage
#                 ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
#                 ON CONFLICT (company_id) DO NOTHING
#             """, (
#                 company.get("id"),
#                 company.get("company_logo"),
#                 company.get("company_name"),
#                 company.get("chart_link"),
#                 company.get("about_company"),
#                 company.get("website"),
#                 company.get("nse_profile"),
#                 company.get("bse_profile"),
#                 company.get("face_value"),
#                 company.get("book_value"),
#                 company.get("roce_percentage"),
#                 company.get("roe_percentage")
#             ))
#             DB.commit()
#         except Exception as e:
#             DB.rollback()
#             print("❌ Company insert failed:", e)
#             continue

  
#         insert_rows(
#             "analysis",
#             d.get("analysis", []),
#             [
#                 "id",
#                 "company_id",
#                 "compounded_sales_growth",
#                 "compounded_profit_growth",
#                 "stock_price_cagr",
#                 "roe"
#             ]
#         )

       
#         insert_rows(
#             "prosandcons",
#             d.get("prosandcons", []),
#             ["id", "company_id", "pros", "cons"]
#         )

       
#         insert_rows(
#             "balancesheet",
#             d.get("balancesheet", []),
#             [
#                 "id", "company_id", "year", "equity_capital", "reserves",
#                 "borrowings", "other_liabilities", "total_liabilities",
#                 "fixed_assets", "cwip", "investments",
#                 "other_asset", "total_assets"
#             ]
#         )

      
#         insert_rows(
#             "profitandloss",
#             d.get("profitandloss", []),
#             [
#                 "id", "company_id", "year", "sales", "expenses",
#                 "operating_profit", "opm_percentage", "other_income",
#                 "interest", "depreciation", "profit_before_tax",
#                 "tax_percentage", "net_profit", "eps", "dividend_payout"
#             ]
#         )

  
#         insert_rows(
#             "cashflow",
#             d.get("cashflow", []),
#             [
#                 "id", "company_id", "year", "operating_activity",
#                 "investing_activity", "financing_activity",
#                 "net_cash_flow"
#             ]
#         )

       
#         insert_rows(
#             "documents",
#             d.get("documents", []),
#             ["id", "company_id", "Year", "Annual_Report"]
#         )

#         DB.commit()
#         print("✅ Saved")

#     cur.close()
#     DB.close()
#     print("\nALL DATA SAVED")

# if __name__ == "__main__":
#     main()


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


# ================= GENERIC INSERT =================
def insert_rows(cur, table, rows, cols, conflict_col="id"):
    """
    Generic safe insert with rollback protection
    """
    if not rows:
        return

    for r in rows:
        try:
            values = [r.get(c) for c in cols]
            cur.execute(
                f"""
                INSERT INTO {table} ({','.join(cols)})
                VALUES ({','.join(['%s'] * len(cols))})
                ON CONFLICT ({conflict_col}) DO NOTHING
                """,
                values
            )
        except Exception as e:
            print(f"⚠️ Skipped row in {table}: {e}")


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
            insert_rows(
                cur,
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
                cur,
                "prosandcons",
                d.get("prosandcons", []),
                ["id", "company_id", "pros", "cons"]
            )

            # ---------- balance sheet ----------
            insert_rows(
                cur,
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
                cur,
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
                cur,
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
                cur,
                "documents",
                d.get("documents", []),
                ["id", "company_id", "year", "annual_report"]
            )

            db.commit()
            print("✅ Saved")

        except Exception as e:
            db.rollback()
            print("❌ Failed for", cid, e)

    cur.close()
    db.close()
    print("\nALL DATA SAVED")


# ================= RUN =================
if __name__ == "__main__":
    main()
