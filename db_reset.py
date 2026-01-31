import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(
    os.environ["DATABASE_URL"],
    sslmode="require"
)

cur = conn.cursor()

cur.execute("""
DROP TABLE IF EXISTS documents;
DROP TABLE IF EXISTS cashflow;
DROP TABLE IF EXISTS profitandloss;
DROP TABLE IF EXISTS balancesheet;
DROP TABLE IF EXISTS prosandcons;
DROP TABLE IF EXISTS analysis;
DROP TABLE IF EXISTS companies;
""")

conn.commit()
cur.close()
conn.close()

print("🔥 ONE-TIME HARD RESET DONE")
