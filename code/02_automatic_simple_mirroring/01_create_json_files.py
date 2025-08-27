import os
import json
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
    "TransactionType",
    "Transactions",
    "Payments"
]


with engine.connect() as conn:
    for t in tables:
        folder = f"../../results/01_simple_mirroring/{t}"
        os.makedirs(folder, exist_ok=True)
        full_path = os.path.join(folder, "_metadata.json")

        query = f"SELECT [keyColumns] = c.[name] FROM [sys].[tables] AS t JOIN [sys].[indexes] AS i ON [t].[object_id] = [i].[object_id] JOIN [sys].[index_columns] AS ic ON [i].[object_id] = [ic].[object_id] AND [i].[index_id] = [ic].[index_id] JOIN [sys].[columns] AS c ON [ic].[object_id] = [c].[object_id] AND [ic].[column_id] = [c].[column_id] WHERE [t].[name] = '{t}' AND [i].[is_primary_key] = 1"

        df = pd.read_sql(query, conn)
        data = {"keyColumns": df["keyColumns"].tolist()}
        
        # Save to file
        with open(full_path, "w") as f:
            json.dump(data, f)
        
        print (f"{full_path} has been created")



