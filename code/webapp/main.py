from fastapi import FastAPI, Request, Form
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse
from fastapi.responses import HTMLResponse

import sqlalchemy as sa
from sqlalchemy import text



app = FastAPI()



templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")

# Database connection config
DATABASE_CONTROL_URL = "mssql+pyodbc://sa:FABcon2025!@fabcon-sqlserver,1433/fabcon_control?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
engine_control = sa.create_engine(DATABASE_CONTROL_URL)
DATABASE_SOURCE_URL = "mssql+pyodbc://sa:FABcon2025!@fabcon-sqlserver,1433/fabcon_source_rowversion?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
engine_source = sa.create_engine(DATABASE_SOURCE_URL)





def get_queries():
    with engine_control.begin() as conn:
        result = conn.execute(text("SELECT * FROM [control].[v_queries]"))
        return [dict(row._mapping) for row in result]

def get_proyect_names():
    with engine_control.begin() as conn:
        result = conn.execute(text("SELECT DISTINCT proyect_name FROM [control].[queries_control]"))
        return [row[0] for row in result]

def get_connection_names():
    with engine_control.begin() as conn:
        result = conn.execute(text("SELECT DISTINCT connection_name FROM [source].[sources]"))
        return [row[0] for row in result]





@app.get("/")
def root():
    return RedirectResponse(url="/current_status")





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
    with engine_control.connect() as conn:
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
def toggle_active(query_id: int):
    with engine_control.begin() as conn:
        conn.execute(text("UPDATE [control].[queries_control] SET [active] = CASE WHEN [active] = 1 THEN 0 ELSE 1 END WHERE [id] = :id"), {"id": query_id})
        
    return RedirectResponse(url="/current_status", status_code=303)





@app.post("/restart/{query_id}")
def restart(query_id: int):
    with engine_control.begin() as conn:
        conn.execute(text("UPDATE [control].[queries_control] SET [next_file_sequence] = NULL, [current_timestamp] = NULL WHERE [id] = :id"), {"id": query_id})
    return RedirectResponse(url="/current_status", status_code=303)





# Integration page
def get_sources():
    with engine_control.begin() as conn:
        result = conn.execute(text("SELECT * FROM [source].[v_sources]"))
        return [dict(row._mapping) for row in result]





@app.get("/integration")
def integration(request: Request):
    sources = get_sources()
    return templates.TemplateResponse("integration.html", {
        "request": request,
        "sources": sources
    })





@app.post("/refresh_sources")
def refresh_sources():
    with engine_control.begin() as conn:
        conn.execute(text("EXEC [control].[usp_refresh_metadata]"))
    return RedirectResponse(url="/integration", status_code=303)



# Edit query fields
@app.post("/delete_query/{query_id}")
def edit_query(
    query_id: int
):
    with engine_control.begin() as conn:
        conn.execute(text(
            "DELETE FROM [control].[queries_control] WHERE [id] = :id"), 
            {
                "id": query_id
            }
        )
    return RedirectResponse(url="/current_status", status_code=303)


# Edit query fields
@app.post("/edit_query/{query_id}")
def edit_query(
    query_id: int,
    query_new_lsn: str = Form(...),
    query_full: str = Form(...),
    query_incremental: str = Form(...),
    unique_keys: str = Form(...)
):
    
    with engine_control.begin() as conn:
        conn.execute(text(
            "UPDATE [control].[queries_control] SET [query_new_lsn] = :query_new_lsn, [query_full] = :query_full, [query_incremental] = :query_incremental, [unique_keys] = :unique_keys WHERE [id] = :id"), 
            {
                "query_new_lsn": query_new_lsn,
                "query_full": query_full,
                "query_incremental": query_incremental,
                "unique_keys": unique_keys,
                "id": query_id
            }
        )
    return RedirectResponse(url="/current_status", status_code=303)





@app.post("/include_source/{source_id}")
def include_source(source_id: int):
    with engine_control.begin() as conn:
        conn.execute(text("EXEC [control].[usp_add_source_object] @id=:source_id"), {"source_id": source_id})
    return RedirectResponse(url="/current_status", status_code=303)









def get_source_columns(id):
    with engine_control.begin() as conn:
        result = conn.execute(text("SELECT c.* FROM [source].[sources] AS src JOIN [source].[columns] AS c ON src.[connection_name]=c.[connection_name] AND src.[object]=c.[object] WHERE src.[id] = :id"), {"id": id})
        return [dict(row._mapping) for row in result]

@app.get("/update_columns/{id}", response_class=HTMLResponse)
def update_columns_form(request: Request, id: int):
    columns = get_source_columns(id)
    return templates.TemplateResponse("update_columns.html", {
        "request": request,
        "source_id": id,
        "columns": columns
    })



@app.post("/update_columns/{tableid}")
def update_columns(request: Request, tableid: int, unique_key: list[int] = Form([])):
    print (tableid)
    # unique_key and timestamp_key are lists of column ids to set as True
    with engine_control.begin() as conn:
        # First, set all to 0 for this source
        conn.execute(text("UPDATE [source].[columns] SET [unique_key]=0 WHERE [object] IN (SELECT [object] FROM [source].[sources] WHERE [id]=:tableid)"), {"tableid": tableid})
        # Then, set selected unique_key columns to 1
        if unique_key:
            for i in unique_key:
                conn.execute(text("UPDATE [source].[columns] SET [unique_key]=1 WHERE [id] = :i"), {"i": i})
        
    
    return RedirectResponse(url="/integration", status_code=303)




# Check SQL Server connection status
@app.get("/check_connection")
def check_connection_form(request: Request):
    return templates.TemplateResponse("check_connection.html", {"request": request})





@app.post("/check_connection")
def check_connection(request: Request):
    try:
        with engine_control.begin() as conn:
            conn.execute(text("SELECT 1"))
        return templates.TemplateResponse("check_connection.html", {"request": request, "status": "success"})
    except Exception as e:
        return templates.TemplateResponse("check_connection.html", {"request": request, "status": "error", "error": str(e)})
