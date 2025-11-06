# Databricks notebook source
# ==========================================================
# UTILITARIOS
# Proyecto: Liga 1 Perú
# Autor: Oscar García Del Águila
# ==========================================================

# utils_adls.py
from pyspark.sql import SparkSession

def get_dbutils():
    """
    Obtiene la instancia de dbutils de forma segura
    """
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        dbutils = spark._jvm.com.databricks.backend.daemon.dbutils.DBUtilsHolder.getDBUtils()
        return dbutils
    except:
        try:
            import IPython
            return IPython.get_ipython().user_ns["dbutils"]
        except:
            raise Exception("No se pudo obtener dbutils")

def setup_adls():
    """
    Configuración rápida de ADLS
    """
    dbutils = get_dbutils()
    try:
        # Carga de los secretos
        adls_account_name = dbutils.secrets.get(scope="secretliga1", key="storageaccount")
        client_id = dbutils.secrets.get(scope="secretliga1", key="clientid")
        client_secret = dbutils.secrets.get(scope="secretliga1", key="secretidt")
        tenant_id = dbutils.secrets.get(scope="secretliga1", key="tenantidt")

        spark = SparkSession.builder.getOrCreate()
       
        # Configuración de Spark
        spark.conf.set(f"fs.azure.account.auth.type.{adls_account_name}.dfs.core.windows.net", "OAuth")
        spark.conf.set(f"fs.azure.account.oauth.provider.type.{adls_account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
        spark.conf.set(f"fs.azure.account.oauth2.client.id.{adls_account_name}.dfs.core.windows.net", client_id)
        spark.conf.set(f"fs.azure.account.oauth2.client.secret.{adls_account_name}.dfs.core.windows.net", client_secret)
        spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{adls_account_name}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token")

        print(f"Autenticación ADLS configurada para: {adls_account_name}")
       
    except Exception as e:
        print(f"Error configurando ADLS: {e}")


def get_abfss_path(carpeta=""):
    """
    Retorna ruta ABFSS completa
    """
    dbutils = get_dbutils()
    try:
        container_name = dbutils.secrets.get(scope="secretliga1", key="filesystemname")
        adls_account_name = dbutils.secrets.get(scope="secretliga1", key="storageaccount")
       
        base_path = f"abfss://{container_name}@{adls_account_name}.dfs.core.windows.net/"
        return base_path + f"{carpeta}" if carpeta else base_path
       
    except Exception as e:
        print(f"Error obteniendo ruta ABFSS: {e}")
        return None


def test_conexion_adls():
    """
    Test rápido de conexión a ADLS
    """
    dbutils = get_dbutils()
    try:
        ruta_raiz = get_abfss_path()
        if ruta_raiz:
            archivos = dbutils.fs.ls(ruta_raiz)
            print(f"Conexión exitosa. Carpetas encontradas: {len(archivos)}")
            return True
        else:
            print("No se pudo obtener la ruta")
            return False
    except Exception as e:
        print(f"Error en conexión: {e}")
        return False
