
# from dotenv import load_dotenv
# load_dotenv()


# from fetch import get_company_ids, fetch_company, save_raw_json
# import psycopg2
# import time


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
#                 """,
#                 values
#             )
#         except Exception as e:
           
#             DB.rollback()
#             print(f"⚠️ Skipped row in {table}: {e}")


# def main():
#     company_ids = get_company_ids()
#     print("Total companies:", len(company_ids))

#     for cid in company_ids:
#         print(f"\nFetching → {cid}")

#         data = fetch_company(cid)
#         if not data:
#             print("❌ API failed")
#             continue

#         save_raw_json(cid, data)

#         company = data.get("company")
#         d = data.get("data")

#         if not company or not d:
#             print("❌ Invalid API structure")
#             continue

     
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

#         # ---------- pros & cons ----------
#         insert_rows(
#             "prosandcons",
#             d.get("prosandcons", []),
#             ["id", "company_id", "pros", "cons"]
#         )

#         # ---------- balance sheet ----------
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

#         # ---------- profit & loss ----------
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

#         # ---------- cashflow ----------
#         insert_rows(
#             "cashflow",
#             d.get("cashflow", []),
#             [
#                 "id", "company_id", "year", "operating_activity",
#                 "investing_activity", "financing_activity",
#                 "net_cash_flow"
#             ]
#         )

#         # ---------- documents ----------
#         insert_rows(
#             "documents",
#             d.get("documents", []),
#             ["id", "company_id", "year", "annual_report"]
#         )

#         DB.commit()
#         print("✅ Saved")

#         time.sleep(1)  # API safe delay

#     cur.close()
#     DB.close()
#     print("\nALL COMPANIES PROCESSED")

# # ================= RUN =================
# if __name__ == "__main__":
#     main()



from dotenv import load_dotenv
load_dotenv()

import os
import time
import psycopg2

from fetch import get_company_ids, fetch_company, save_raw_json

# ---------------- DB ----------------
def get_conn():
    return psycopg2.connect(
        os.environ["DATABASE_URL"],
        sslmode="require"
    )

# ---------------- UTILS ----------------
def clean(v):
    if v in ("", "--", None):
        return None
    return v

def insert_rows(conn, table, rows, cols):
    if not rows:
        return

    values = []
    for r in rows:
        values.append([clean(r.get(c)) for c in cols])

    placeholders = ",".join(["%s"] * len(cols))
    query = f"""
        INSERT INTO {table} ({','.join(cols)})
        VALUES ({placeholders})
    """

    cur = conn.cursor()
    try:
        cur.executemany(query, values)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"❌ {table} insert error:", e)
    finally:
        cur.close()

# ---------------- PIPELINE ----------------
def main():
    company_ids = get_company_ids()
    print("Total companies:", len(company_ids))

    conn = get_conn()

    for i, cid in enumerate(company_ids, start=1):
        print(f"\n[{i}/{len(company_ids)}] Fetching → {cid}")

        data = fetch_company(cid)
        if not data:
            print("❌ API failed")
            continue

        save_raw_json(cid, data)

        company = data.get("company")
        d = data.get("data")

        if not company or not d:
            print("❌ Invalid structure")
            continue

        # -------- companies --------
        cur = conn.cursor()
        try:
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
                clean(company.get("face_value")),
                clean(company.get("book_value")),
                clean(company.get("roce_percentage")),
                clean(company.get("roe_percentage")),
            ))
            conn.commit()
        except Exception as e:
            conn.rollback()
            print("❌ Company insert failed:", e)
        finally:
            cur.close()

        insert_rows(conn, "analysis", d.get("analysis", []),
            ["company_id","compounded_sales_growth","compounded_profit_growth","stock_price_cagr","roe"]
        )

        insert_rows(conn, "prosandcons", d.get("prosandcons", []),
            ["company_id","pros","cons"]
        )

        insert_rows(conn, "balancesheet", d.get("balancesheet", []),
            ["company_id","year","equity_capital","reserves","borrowings","total_assets"]
        )

        insert_rows(conn, "profitandloss", d.get("profitandloss", []),
            ["company_id","year","sales","expenses","operating_profit","net_profit"]
        )

        insert_rows(conn, "cashflow", d.get("cashflow", []),
            ["company_id","year","operating_activity","investing_activity","financing_activity","net_cash_flow"]
        )

        insert_rows(conn, "documents", d.get("documents", []),
            ["company_id","year","annual_report"]
        )

        print("✅ Saved", cid)
        time.sleep(0.5)  # API safety

    conn.close()
    print("\n🎉 ALL COMPANIES SEEDED")

if __name__ == "__main__":
    main()
