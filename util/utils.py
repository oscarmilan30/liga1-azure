from databricks.sdk.runtime import *

def obtener_storage(capa: str) -> str:
    """
    Devuelve el prefijo completo de ADLS para la capa indicada.
    Solo se pasa la capa, el storage account se obtiene desde Key Vault.
    """
    storage_account = dbutils.secrets.get(scope="secretliga1", key="storageaccountidt")
    return f"abfss://{capa}@{storage_account}.dfs.core.windows.net"
