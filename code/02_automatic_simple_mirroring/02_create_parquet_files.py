import os
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("mssql+pyodbc://sa:FABcon2025!@fabcon-sqlserver,1433/fabcon_source?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes")

tables = [
    "Card",
    "CardAccount",
    "CardType",
    "Currency",
    "Customer",
    "Merchant",
    "MerchantCategory",
    "TransactionStatus",
    "TransactionType"
]


with engine.connect() as conn:
    for t in tables:
        folder = f"../../results/01_simple_mirroring/{t}"
        os.makedirs(folder, exist_ok=True)
        file_name = f"00000000000000000001.parquet"
        full_path = os.path.join(folder, file_name)

        #,[__rowMarker__]=0  is not mandatory in the first insert. Actually is not recommended
        query = f"SELECT * FROM [dbo].[{t}]"

        df = pd.read_sql(query, conn)
        df.to_parquet(full_path, engine="pyarrow", index=False)

        print (f"{full_path} has been created")

