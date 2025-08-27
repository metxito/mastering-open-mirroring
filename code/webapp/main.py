from fastapi import FastAPI, Request, Form
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
import sqlalchemy as sa
from sqlalchemy import text

app = FastAPI()
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")

# Database connection config
DATABASE_URL = "mssql+pyodbc://sa:FABcon2025!@fabcon-sqlserver,1433/fabcon_control?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
engine = sa.create_engine(DATABASE_URL)

def get_queries():
    with engine.connect() as conn:
        result = conn.execute(text("SELECT * FROM [control].[queries_control]"))
        return [dict(row._mapping) for row in result]

def get_proyect_names():
    with engine.connect() as conn:
        result = conn.execute(text("SELECT DISTINCT proyect_name FROM [control].[queries_control]"))
        return [row[0] for row in result]

def get_connection_names():
    with engine.connect() as conn:
        result = conn.execute(text("SELECT DISTINCT connection_name FROM [source].[sources]"))
        return [row[0] for row in result]

@app.get("/current_status")
def current_status(request: Request):
    queries = get_queries()
    proyect_names = get_proyect_names()
    connection_names = get_connection_names()
    return templates.TemplateResponse("current_status.html", {
        "request": request,
        "queries": queries,
        "proyect_names": proyect_names,
        "connection_names": connection_names
    })

@app.post("/create_query")
def create_query(
    request: Request,
    proyect_name: str = Form(...),
    connection_name: str = Form(...),
    query_name: str = Form(...),
    base_query: str = Form(...),
    unique_keys: str = Form(...),
    timestamp_keys: str = Form(...),
    change_detection_mode: str = Form(...),
    change_detection_code: str = Form(...)
):
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO [control].[queries_control]
            (proyect_name, connection_name, query_name, base_query, unique_keys, timestamp_keys, change_detection_mode, change_detection_code, active)
            VALUES (:proyect_name, :connection_name, :query_name, :base_query, :unique_keys, :timestamp_keys, :change_detection_mode, :change_detection_code, 1)
        """), {
            "proyect_name": proyect_name,
            "connection_name": connection_name,
            "query_name": query_name,
            "base_query": base_query,
            "unique_keys": unique_keys,
            "timestamp_keys": timestamp_keys,
            "change_detection_mode": change_detection_mode,
            "change_detection_code": change_detection_code
        })
    return RedirectResponse(url="/current_status", status_code=303)

@app.post("/toggle_active/{query_id}")
def toggle_active(query_id: int, active: int = Form(...)):
    with engine.connect() as conn:
        conn.execute(text("UPDATE [control].[queries_control] SET active = :active WHERE id = :id"), {"active": active, "id": query_id})
    return RedirectResponse(url="/current_status", status_code=303)

@app.post("/restart/{query_id}")
def restart(query_id: int):
    with engine.connect() as conn:
        conn.execute(text("UPDATE [control].[queries_control] SET next_file_sequence = NULL, current_timestamp = NULL WHERE id = :id"), {"id": query_id})
    return RedirectResponse(url="/current_status", status_code=303)

# Integration page
def get_sources():
    with engine.connect() as conn:
        result = conn.execute(text("SELECT * FROM [source].[sources]"))
        return [dict(row) for row in result]

@app.get("/integration")
def integration(request: Request):
    sources = get_sources()
    return templates.TemplateResponse("integration.html", {
        "request": request,
        "sources": sources
    })

@app.post("/refresh_sources")
def refresh_sources():
    return RedirectResponse(url="/integration", status_code=303)

@app.post("/include_source/{source_id}")
def include_source(source_id: int):
    with engine.connect() as conn:
        conn.execute(text("EXEC [control].[usp_include_new_source] @source_id=:source_id"), {"source_id": source_id})
    return RedirectResponse(url="/integration", status_code=303)

# Check SQL Server connection status
@app.get("/check_connection")
def check_connection_form(request: Request):
    return templates.TemplateResponse("check_connection.html", {"request": request})

@app.post("/check_connection")
def check_connection(request: Request):
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return templates.TemplateResponse("check_connection.html", {"request": request, "status": "success"})
    except Exception as e:
        return templates.TemplateResponse("check_connection.html", {"request": request, "status": "error", "error": str(e)})
