import os
import json
import pandas as pd
from urllib.parse import urlparse
from sqlalchemy import create_engine, text
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient


tables = [
    "Card",
    "CardAccount",
    "Currency",
    "Customer",
    "Merchant",
    "MerchantCategory",
    "TransactionStatus",
    "TransactionType",
    "Transactions",
    "Payments"
]
config = json.load(open("../00_config/config.json"))

client_credential = ClientSecretCredential(
    tenant_id=config["tenant_id"],
    client_id=config["client_id"],
    client_secret=config["client_secret"]
)

onelake_url_parts = urlparse(config["onelake_landing_zone"])
onelake_url_segments = onelake_url_parts.path.strip("/").split("/")
onelake_account_url = f"{onelake_url_parts.scheme}://{onelake_url_parts.netloc}"
onelake_filesystem = onelake_url_segments[0]
onelake_landingzone_path = "/".join(onelake_url_segments[1:])

onelake_service_client = DataLakeServiceClient(account_url=onelake_account_url, credential=client_credential)
onelake_filesystem = onelake_service_client.get_file_system_client(file_system=onelake_filesystem)


sql_source_engine = create_engine(f"mssql+pyodbc://{config["sql_user"]}:{config["sql_password"]}@{config["sql_server"]},{config["sql_port"]}/{config["sql_catalog_source"]}?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes")
sql_control_engine = create_engine(f"mssql+pyodbc://{config["sql_user"]}:{config["sql_password"]}@{config["sql_server"]},{config["sql_port"]}/{config["sql_catalog_control"]}?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes")



#    [id]                    INT                        IDENTITY (1, 1) PRIMARY KEY,
#    [proyect_name]          VARCHAR(128)    NOT NULL,
#    [connection_name]       VARCHAR(128)    NOT NULL,
#    [query_name]            VARCHAR(128)    NOT NULL,
#    [base_query]            NVARCHAR(MAX)   NOT NULL,
#    [unique_keys]           NVARCHAR(MAX)   NOT NULL,
#    [timestamp_keys]        NVARCHAR(MAX)   NOT NULL,
#    [change_detection_mode] VARCHAR(62)     NOT NULL, -- condition, comparation, only_insert
#    [change_detection_code] NVARCHAR(MAX)       NULL,
#    [next_file_sequence]    BIGINT              NULL,
#    [current_timestamp]     NVARCHAR(128)       NULL,
#    [active]                BIT             NOT NULL    DEFAULT 1,
#    [CreatedAt]             DATETIME2       NOT NULL    DEFAULT SYSUTCDATETIME()

query_control_insert = (
        "INSERT INTO [control].[queries_control] "
        "([proyect_name], [connection_name], [query_name],  [base_query],   [unique_keys],  [timestamp_keys], [change_detection_mode], [active]) "
        "VALUES "
        "(:proy_name,     :conn_name,        :query_name,   :query,         :uniquekeys,    :timestamp_keys,  'condition',             1)"
    )

with sql_source_engine.connect() as source_conn, sql_control_engine.connect() as control_conn:
    for t in tables:
        
        # PREPARE folder
        folder = f"../../results/01_simple_mirroring/{t}"
        os.makedirs(folder, exist_ok=True)


        # QUERY Unique keys
        keycolumns_query = f"SELECT [keyColumns] = c.[name] FROM [sys].[tables] AS t JOIN [sys].[indexes] AS i ON [t].[object_id] = [i].[object_id] JOIN [sys].[index_columns] AS ic ON [i].[object_id] = [ic].[object_id] AND [i].[index_id] = [ic].[index_id] JOIN [sys].[columns] AS c ON [ic].[object_id] = [c].[object_id] AND [ic].[column_id] = [c].[column_id] WHERE [t].[name] = '{t}' AND [i].[is_primary_key] = 1"
        keycolumns_df = pd.read_sql(keycolumns_query, source_conn)
        metadata_content = {"keyColumns": keycolumns_df["keyColumns"].tolist()}
        

        # SAVE json file to local
        metadata_file = "_metadata.json"
        metadata_full_path = os.path.join(folder, metadata_file)
        with open(metadata_full_path, "w") as f:
            json.dump(metadata_content, f)
        

        # QUERY data
        #,[__rowMarker__]=0  is not mandatory in the first insert. Actually is not recommended
        data_query = f"SELECT * FROM [dbo].[{t}]"
        data_df = pd.read_sql(data_query, source_conn)


        # SAVE parquet file to local
        parquet_file = f"00000000000000000001.parquet"
        parque_full_path = os.path.join(folder, parquet_file)
        data_df.to_parquet(parque_full_path, engine="pyarrow", index=False)
        

        
        table_path = f"{onelake_landingzone_path}/{t}"
        onelake_table_directory = onelake_filesystem.get_directory_client(table_path)


        if not onelake_table_directory.exists():
            onelake_filesystem.create_directory(table_path)


        with open(metadata_full_path, "rb") as data:
            file_client = onelake_table_directory.get_file_client(metadata_file)
            file_client.upload_data(data, overwrite=True)


        with open(parque_full_path, "rb") as data:
            file_client = onelake_table_directory.get_file_client(parquet_file)
            file_client.upload_data(data, overwrite=True)

        #(':proy_name',   ':conn_name',      ':query_name',   ':query',       ':uniquekeys',  ':timestamp_keys')"
        parameters = {
            "proy_name": "simple_mirroring",
            "conn_name": "simple_mirroring",
            "query_name": f"{t}",
            "query": data_query,
            "uniquekeys": "[" + ", ".join(f'"{k}"' for k in keycolumns_df["keyColumns"].tolist()) + "]", 
            "timestamp_keys": ""
        }
        
        control_conn.execute(
            text(query_control_insert),
            parameters
        )

        print (f"{t} done")
