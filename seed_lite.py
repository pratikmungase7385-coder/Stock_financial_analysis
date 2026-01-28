from dotenv import load_dotenv
load_dotenv()

from fetch import fetch_company, save_raw_json
import psycopg2
import os

DB = psycopg2.connect(
    os.environ["DATABASE_URL"],
    sslmode="require"
)

def seed_small():
    SAMPLE_COMPANIES = [
        "TCS",
        "INFY",
        "HDFCBANK"
    ]

    cur = DB.cursor()

    for cid in SAMPLE_COMPANIES:
        print("Seeding →", cid)

        data = fetch_company(cid)
        if not data:
            print("❌ API failed:", cid)
            continue

        save_raw_json(cid, data)

        company = data["company"]
        d = data["data"]

        # companies
        cur.execute("""
            INSERT INTO companies (
                company_id, company_logo, company_name
            ) VALUES (%s,%s,%s)
            ON CONFLICT (company_id) DO NOTHING
        """, (
            company["id"],
            company["company_logo"],
            company["company_name"]
        ))

        DB.commit()

    cur.close()
    DB.close()
    print("✅ seed_little DONE")

if __name__ == "__main__":
    seed_small()
