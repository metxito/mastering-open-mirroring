
import sqlalchemy as sa
from sqlalchemy import text

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

def get_sources():
    with engine_control.begin() as conn:
        result = conn.execute(text("SELECT * FROM [source].[v_sources]"))
        return [dict(row._mapping) for row in result]

def get_source_columns(id):
    with engine_control.begin() as conn:
        result = conn.execute(text("SELECT c.* FROM [source].[sources] AS src JOIN [source].[columns] AS c ON src.[connection_name]=c.[connection_name] AND src.[object]=c.[object] WHERE src.[id] = :id"), {"id": id})
        return [dict(row._mapping) for row in result]
