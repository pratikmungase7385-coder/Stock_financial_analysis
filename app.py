from dotenv import load_dotenv
load_dotenv()

import os
import psycopg2
from psycopg2.extras import RealDictCursor

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

from db_init import init_db

# ================= APP =================
app = FastAPI()

app.add_middleware(
    SessionMiddleware,
    secret_key=os.getenv("SESSION_SECRET", "dev-secret")
)

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# ================= STARTUP =================
@app.on_event("startup")
def startup():
    init_db()   # ONLY schema ensure, NEVER delete data here

# ================= DB (🔥 CORRECT WAY) =================
def get_db():
    conn = psycopg2.connect(
        os.environ["DATABASE_URL"],
        sslmode="require",
        connect_timeout=5,
        cursor_factory=RealDictCursor
    )
    conn.autocommit = True   # 🔥 VERY IMPORTANT on Render
    return conn

# ================= HOME =================
@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    db = get_db()
    cur = db.cursor()

    cur.execute("""
        SELECT DISTINCT c.company_id, c.company_name
        FROM companies c
        WHERE EXISTS (
            SELECT 1 FROM analysis a WHERE a.company_id = c.company_id
        )
        AND EXISTS (
            SELECT 1 FROM prosandcons p
            WHERE p.company_id = c.company_id
              AND (p.pros IS NOT NULL OR p.cons IS NOT NULL)
        )
        ORDER BY c.company_name
        LIMIT 5
    """)

    examples = cur.fetchall()

    cur.close()
    db.close()

    return templates.TemplateResponse(
        "home.html",
        {"request": request, "examples": examples}
    )

# ================= SEARCH =================
@app.get("/search")
def search(q: str, request: Request):
    db = get_db()
    cur = db.cursor()

    cur.execute("""
        SELECT company_id
        FROM companies
        WHERE company_id ILIKE %s OR company_name ILIKE %s
        LIMIT 1
    """, (f"%{q}%", f"%{q}%"))

    row = cur.fetchone()

    cur.close()
    db.close()

    if not row:
        request.session["error"] = f"No company found for '{q}'"
        return RedirectResponse("/", status_code=302)

    return RedirectResponse(f"/company/{row['company_id']}", status_code=302)

# ================= ALL COMPANIES =================
@app.get("/companies", response_class=HTMLResponse)
def companies(request: Request):
    db = get_db()
    cur = db.cursor()

    cur.execute("""
        SELECT company_id, company_name, company_logo
        FROM companies
        ORDER BY company_name
    """)

    companies = cur.fetchall()

    cur.close()
    db.close()

    return templates.TemplateResponse(
        "list.html",
        {"request": request, "companies": companies}
    )

# ================= COMPANY PAGE =================
@app.get("/company/{cid}", response_class=HTMLResponse)
def company(cid: str, request: Request):
    db = get_db()
    cur = db.cursor()

    cur.execute("SELECT * FROM companies WHERE company_id=%s", (cid,))
    company = cur.fetchone()

    if not company:
        cur.close()
        db.close()
        return HTMLResponse("Company not found", status_code=404)

    cur.execute("SELECT * FROM analysis WHERE company_id=%s", (cid,))
    analysis = cur.fetchall()

    cur.execute("SELECT pros FROM prosandcons WHERE company_id=%s AND pros IS NOT NULL", (cid,))
    pros = cur.fetchall()

    cur.execute("SELECT cons FROM prosandcons WHERE company_id=%s AND cons IS NOT NULL", (cid,))
    cons = cur.fetchall()

    cur.execute("SELECT * FROM balancesheet WHERE company_id=%s ORDER BY year", (cid,))
    balancesheet = cur.fetchall()

    cur.execute("SELECT * FROM profitandloss WHERE company_id=%s ORDER BY year", (cid,))
    profitandloss = cur.fetchall()

    cur.execute("SELECT * FROM cashflow WHERE company_id=%s ORDER BY year", (cid,))
    cashflow = cur.fetchall()

    cur.execute("SELECT * FROM documents WHERE company_id=%s ORDER BY year DESC", (cid,))
    documents = cur.fetchall()

    cur.close()
    db.close()

    return templates.TemplateResponse(
        "company.html",
        {
            "request": request,
            "company": company,
            "analysis": analysis,
            "pros": pros,
            "cons": cons,
            "balancesheet": balancesheet,
            "profitandloss": profitandloss,
            "cashflow": cashflow,
            "documents": documents
        }
    )
