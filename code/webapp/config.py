# Configuration settings for the web application.
# Example: Add your configuration variables here.

class Config:
    DEBUG = False
    TESTING = False

    DATABASE_CONTROL_URL = "mssql+pyodbc://sa:FABcon2025!@fabcon-sqlserver,1433/fabcon_control?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
    DATABASE_SOURCE_URL =  "mssql+pyodbc://sa:FABcon2025!@fabcon-sqlserver,1433/fabcon_source_cdc?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
