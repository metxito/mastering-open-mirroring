import sqlalchemy as sa
from sqlalchemy import text
from config import Config

# Database connection config
engine_control = sa.create_engine(Config.DATABASE_CONTROL_URL)
engine_source = sa.create_engine(Config.DATABASE_SOURCE_URL)

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

def get_sources():
    with engine_control.begin() as conn:
        result = conn.execute(text("SELECT * FROM [source].[v_sources]"))
        return [dict(row._mapping) for row in result]

def get_source_columns(id):
    with engine_control.begin() as conn:
        result = conn.execute(text("SELECT c.* FROM [source].[sources] AS src JOIN [source].[columns] AS c ON src.[connection_name]=c.[connection_name] AND src.[object]=c.[object] WHERE src.[id] = :id"), {"id": id})
        return [dict(row._mapping) for row in result]

def run_simulation_procedure():
    with engine_source.begin() as conn:
        result = conn.execute(text("EXEC [dbo].[usp_insert_range_transaction] @days=2"))
        row = result.fetchone()
        if row and ("MaxCreatedOn" in row._mapping):
            return row._mapping["MaxCreatedOn"]
        return None
