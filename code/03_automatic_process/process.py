import os
import json
import pandas as pd
from urllib.parse import urlparse
from sqlalchemy import create_engine, text
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient



################################################
#   SETUP
################################################
# Load configuration from JSON file
config = json.load(open("../00_config/config.json"))

# Authenticate to Azure using client credentials
client_credential = ClientSecretCredential(
    tenant_id=config["tenant_id"],
    client_id=config["client_id"],
    client_secret=config["client_secret"]
)

# Parse OneLake URL and extract relevant segments
onelake_url_parts = urlparse(config["onelake_landing_zone"])
onelake_url_segments = onelake_url_parts.path.strip("/").split("/")
onelake_account_url = f"{onelake_url_parts.scheme}://{onelake_url_parts.netloc}"
onelake_filesystem = onelake_url_segments[0]
onelake_landingzone_path = "/".join(onelake_url_segments[1:])



# Create DataLake service client and filesystem client
onelake_service_client = DataLakeServiceClient(account_url=onelake_account_url, credential=client_credential)
onelake_filesystem = onelake_service_client.get_file_system_client(file_system=onelake_filesystem)

# Create SQLAlchemy engines for source and control databases
sql_source_engine = create_engine(f"mssql+pyodbc://{config["sql_user"]}:{config["sql_password"]}@{config["sql_server"]},{config["sql_port"]}/{config["sql_catalog_source"]}?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes")
sql_control_engine = create_engine(f"mssql+pyodbc://{config["sql_user"]}:{config["sql_password"]}@{config["sql_server"]},{config["sql_port"]}/{config["sql_catalog_control"]}?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes")




#######################################################
#   GET objects 
#######################################################
# Read control table metadata for selected tables
proyect_name = "AutomaticMirroring"
keycolumns_df = None
with sql_control_engine.connect() as control_conn:
    keycolumns_df = pd.read_sql(
        f"SELECT [id], [query_name], [base_query], [unique_keys], [timestamp_keys], [change_detection_mode], [change_detection_code] FROM [control].[queries_control] WHERE [proyect_name] = '{proyect_name}'",
        sql_control_engine
    )



#######################################################
#   START process
#######################################################
if keycolumns_df:

    # Main loop: process each table
    with sql_source_engine.connect() as source_conn, sql_control_engine.connect() as control_conn:

        # Iterate over each row and print the base_query column
        for _, row in keycolumns_df.iterrows():
            

            control_id = row["id"]
            control_query_name = row["query_name"]
            control_base_query = row["base_query"]
            control_unique_keys = json.loads(row["unique_keys"])
            control_timestamp_keys = json.loads(row["timestamp_keys"])
            control_change_detection_mode = row["change_detection_mode"]
            control_change_detection_code = row["change_detection_code"]





            current_info_query = (
                "SELECT [next_file_sequence], [current_timestamp] "
                "FROM [control].[queries_control] "
                f"WHERE [id] = {control_id}"
            )
            current_info = control_conn.execute(text(current_info_query)).fetchone()
            if current_info:
                control_current_timestamp = current_info["current_timestamp"]
                if not control_current_timestamp or pd.isna(current_info["next_file_sequence"]):
                    control_next_file_sequece = 1
                else:
                    control_next_file_sequece = current_info["next_file_sequence"]
            else:
                control_next_file_sequece = None
                control_current_timestamp = None




            local_tmp_folder = f"../../results/03_automatic_process/{control_query_name}"


            # Prepare OneLake directory for the table
            onelake_table_path = f"{onelake_landingzone_path}/{control_query_name}"
            onelake_table_directory = onelake_filesystem.get_directory_client(onelake_table_path)


            # CHECK if the table needs to be recreated
            if control_next_file_sequece == 1:
                if onelake_table_directory.exists():
                    onelake_filesystem.delete_directory(onelake_table_path)
            
            
            # Create directory in OneLake if it doesn't exist
            if not onelake_table_directory.exists():
                onelake_filesystem.create_directory(onelake_table_path)

            onelake_metadata_file_path = f"{onelake_table_path}/_metadata.json"
            onelake_metadata_file = onelake_filesystem.get_file_client(onelake_metadata_file_path)
            if not onelake_metadata_file.exists():
                onelake_metadata_file.upload_data('{"keyColumns":' + json.dumps(control_unique_keys) + '}', overwrite=True)


            
            
            # Construct the SQL query
            unpivot_list = ", ".join([f"[{col}]" for col in control_timestamp_keys])
            max_timestamp_query = (
                "SELECT MAX([timestamp_value]) AS max_timestamp "
                "FROM ("
                f"{control_base_query}"
                ") AS sub "
                "UNPIVOT ("
                f"[timestamp_value] FOR [timestamp_column] IN ({unpivot_list})"
                ") AS unpvt"
            )
            result = source_conn.execute(text(max_timestamp_query))
            control_new_timestamp = result.scalar()

            


            # Query all data from the table
            #,[__rowMarker__]=0  is not mandatory in the first insert. Actually is not recommended
            data_query = control_base_query
            if control_next_file_sequece > 1:
                timestamp_filter = " OR ".join([f"[{col}] >= '{control_current_timestamp}'" for col in control_timestamp_keys])

                rowMarker = ""
                if control_change_detection_mode == "only_insert":
                    rowMarker = ", 0 AS [__rowMarker__]"
                elif control_change_detection_mode == "condition":
                    rowMarker = f", IIF({control_change_detection_code}, 1, 0) AS [__rowMarker__]"
                # __rowMarker__
                #    0 : Insert
                #    1 : Update
                #    2 : Delete
                #    4 : Upsert

                data_query = (
                    f"SELECT sub.* {rowMarker} "
                    "FROM ("
                    f"{control_base_query}"
                    ") AS sub "
                    "WHERE "
                    f"{timestamp_filter}"
                )
            data_df = pd.read_sql(data_query, source_conn)


            # Save table data as Parquet locally
            parquet_file = f"{str(control_next_file_sequece).zfill(20)}.parquet"
            parquet_file_path = os.path.join(local_tmp_folder, parquet_file)
            data_df.to_parquet(parquet_file_path, engine="pyarrow", index=False)
            
            
            # Upload Parquet file to OneLake
            with open(parquet_file_path, "rb") as data:
                onelake_parquet_file_path = f"{onelake_table_path}/{parquet_file}"
                file_client = onelake_filesystem.get_file_client(onelake_parquet_file_path)
                file_client.upload_data(data, overwrite=True)

            
            # Insert metadata into control table (parameters incomplete, needs fixing)
            update_query = (
                "UPDATE [control].[queries_control] "
                "SET "
                "[next_file_sequence] = ISNULL([next_file_sequence], 1) + 1, "
                f"[current_timestamp] = '{control_new_timestamp}'"
                f"WHERE [id] = '{control_id}'"
            )
            control_conn.execute(
                update_query
            )

            # Print status for each table
            print (f"{control_query_name} done with {control_new_timestamp}")