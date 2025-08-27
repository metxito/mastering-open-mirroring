# Python 3.8+, azure-identity, azure-storage-file-datalake >=12.21.0
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient
import json
import os

# 1. Leer credenciales
config = json.load(open("../00_config/config.json"))
credential = ClientSecretCredential(
    tenant_id=config["tenant_id"],
    client_id=config["client_id"],
    client_secret=config["client_secret"]
)

# 2. Conectar al servicio OneLake
account_url = "https://onelake.dfs.fabric.microsoft.com"
service_client = DataLakeServiceClient(account_url=account_url, credential=credential)


# 3. Obtener cliente del workspace / filesystem (lakehouse)
filesystem_id = "f513de46-8e4e-45be-ba70-92e9eab33713"
filesystem = service_client.get_file_system_client(file_system=filesystem_id)

# 4. Obtener cliente de directorio
directory = filesystem.get_directory_client(path="Files/LandingZone")  # incluye lakehouse + ruta

# 5. Subir archivo
with open("ruta/local/archivo.ext", "rb") as data:
    file_client = directory.get_file_client("archivo.ext")
    file_client.upload_data(data, overwrite=True)