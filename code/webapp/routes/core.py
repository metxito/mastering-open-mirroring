
from fastapi import APIRouter, Request, Form
from fastapi.responses import RedirectResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from services.data_service import (
    get_queries, get_proyect_names, get_connection_names, get_sources, get_source_columns
)

router = APIRouter()
templates = Jinja2Templates(directory="templates")

@router.get("/")
def root():
    return RedirectResponse(url="/current_status")

@router.get("/current_status")
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

@router.post("/create_query")
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
    from services.data_service import engine_control
    from sqlalchemy import text
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

@router.post("/toggle_active/{query_id}")
def toggle_active(query_id: int):
    from services.data_service import engine_control
    from sqlalchemy import text
    with engine_control.begin() as conn:
        conn.execute(text("UPDATE [control].[queries_control] SET [active] = CASE WHEN [active] = 1 THEN 0 ELSE 1 END WHERE [id] = :id"), {"id": query_id})
    return RedirectResponse(url="/current_status", status_code=303)

@router.post("/restart/{query_id}")
def restart(query_id: int):
    from services.data_service import engine_control
    from sqlalchemy import text
    with engine_control.begin() as conn:
        conn.execute(text("UPDATE [control].[queries_control] SET [next_file_sequence] = NULL, [current_timestamp] = NULL WHERE [id] = :id"), {"id": query_id})
    return RedirectResponse(url="/current_status", status_code=303)

@router.get("/integration")
def integration(request: Request):
    sources = get_sources()
    return templates.TemplateResponse("integration.html", {
        "request": request,
        "sources": sources
    })

@router.post("/refresh_sources")
def refresh_sources():
    from services.data_service import engine_control
    from sqlalchemy import text
    with engine_control.begin() as conn:
        conn.execute(text("EXEC [control].[usp_refresh_metadata]"))
    return RedirectResponse(url="/integration", status_code=303)

@router.post("/delete_query/{query_id}")
def delete_query(query_id: int):
    from services.data_service import engine_control
    from sqlalchemy import text
    with engine_control.begin() as conn:
        conn.execute(text("DELETE FROM [control].[queries_control] WHERE [id] = :id"), {"id": query_id})
    return RedirectResponse(url="/current_status", status_code=303)

@router.post("/edit_query/{query_id}")
def edit_query(
    query_id: int,
    query_new_lsn: str = Form(...),
    query_full: str = Form(...),
    query_incremental: str = Form(...),
    unique_keys: str = Form(...)
):
    from services.data_service import engine_control
    from sqlalchemy import text
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

@router.post("/include_source/{source_id}")
def include_source(source_id: int):
    from services.data_service import engine_control
    from sqlalchemy import text
    with engine_control.begin() as conn:
        conn.execute(text("EXEC [control].[usp_add_source_object] @id=:source_id"), {"source_id": source_id})
    return RedirectResponse(url="/current_status", status_code=303)

@router.get("/update_columns/{id}", response_class=HTMLResponse)
def update_columns_form(request: Request, id: int):
    columns = get_source_columns(id)
    return templates.TemplateResponse("update_columns.html", {
        "request": request,
        "source_id": id,
        "columns": columns
    })

@router.post("/update_columns/{tableid}")
def update_columns(request: Request, tableid: int, unique_key: list[int] = Form([])):
    from services.data_service import engine_control
    from sqlalchemy import text
    # unique_key and timestamp_key are lists of column ids to set as True
    with engine_control.begin() as conn:
        # First, set all to 0 for this source
        conn.execute(text("UPDATE [source].[columns] SET [unique_key]=0 WHERE [object] IN (SELECT [object] FROM [source].[sources] WHERE [id]=:tableid)"), {"tableid": tableid})
        # Then, set selected unique_key columns to 1
        if unique_key:
            for i in unique_key:
                conn.execute(text("UPDATE [source].[columns] SET [unique_key]=1 WHERE [id] = :i"), {"i": i})
    return RedirectResponse(url="/integration", status_code=303)

@router.get("/check_connection")
def check_connection_form(request: Request):
    return templates.TemplateResponse("check_connection.html", {"request": request})

@router.post("/check_connection")
def check_connection(request: Request):
    from services.data_service import engine_control
    from sqlalchemy import text
    try:
        with engine_control.begin() as conn:
            conn.execute(text("SELECT 1"))
        return templates.TemplateResponse("check_connection.html", {"request": request, "status": "success"})
    except Exception as e:
        return templates.TemplateResponse("check_connection.html", {"request": request, "status": "error", "error": str(e)})

# Register router with FastAPI app
def register_routes(app):
    app.include_router(router)
