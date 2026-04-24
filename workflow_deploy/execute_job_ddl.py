# Databricks notebook source
# ==========================================================
# EXECUTE JOB DDL - Proyecto Liga 1 Perú
# Recibe nombre de archivo DDL y lo ejecuta
# ==========================================================

from env_setup import *
from utils_liga1 import get_dbutils
import os

spark = SparkSession.builder.getOrCreate()
dbutils = get_dbutils()

dbutils.widgets.text("prm_name_ddl", "")
prm_name_ddl = dbutils.widgets.get("prm_name_ddl")

print(f"Archivo DDL recibido: {prm_name_ddl}")

container_name   = dbutils.secrets.get(scope="secretliga1", key="filesystemname")
adls_account_name = dbutils.secrets.get(scope="secretliga1", key="storageaccount")
catalog_name     = "adbliga1futbol"

# COMMAND ----------

def execute_ddl_file(file_path):
    """Ejecuta un archivo DDL reemplazando las variables y soportando múltiples sentencias."""
    try:
        with open(file_path, 'r') as file:
            ddl_content = file.read()

        # Reemplazar variables
        ddl_content = ddl_content.replace("${catalog_name}", catalog_name)
        ddl_content = ddl_content.replace("${container_name}", container_name)
        ddl_content = ddl_content.replace("${storage_account}", adls_account_name)

        # 🔑 Dividir en sentencias individuales (por ;)
        statements = [s.strip() for s in ddl_content.split(";") if s.strip()]

        print(f"Encontradas {len(statements)} sentencias en {os.path.basename(file_path)}")

        for idx, stmt in enumerate(statements, start=1):
            print(f"\n--- Ejecutando sentencia {idx} ---")
            print(stmt)
            spark.sql(stmt)

        print(f"\nDDL ejecutado exitosamente: {os.path.basename(file_path)}")
        return True

    except Exception as e:
        print(f"Error ejecutando DDL {file_path}: {str(e)}")
        raise


def process_ddl_files(ddl_name):
    """Procesa todos los archivos DDL para un objeto específico"""
    base_path = get_workspace_path(f"ddl_deploy/{ddl_name}")

    if not os.path.exists(base_path):
        print(f"No se encuentra: ddl_deploy/{ddl_name}")
        return False

    # Buscar archivos SQL
    sql_files = [f for f in os.listdir(base_path) if f.endswith('.sql')]

    if not sql_files:
        print(f"No hay archivos SQL en: {ddl_name}")
        return False

    # Identificar archivos
    table_file = f"{ddl_name}.sql"
    view_file  = f"{ddl_name}_view.sql"

    executed_count = 0

    # Ejecutar tabla primero
    if table_file in sql_files:
        file_path = os.path.join(base_path, table_file)
        if execute_ddl_file(file_path):
            executed_count += 1

    # Ejecutar vista después
    if view_file in sql_files:
        file_path = os.path.join(base_path, view_file)
        if execute_ddl_file(file_path):
            executed_count += 1

    # Archivos adicionales
    other_files = [f for f in sql_files if f not in [table_file, view_file]]
    for sql_file in other_files:
        file_path = os.path.join(base_path, sql_file)
        if execute_ddl_file(file_path):
            executed_count += 1

    print(f"Total ejecutados: {executed_count} archivos")
    return executed_count > 0

# COMMAND ----------


# Ejecutar el proceso principal
try:
    print("Iniciando ejecución DDL...")
    success = process_ddl_files(prm_name_ddl)

    if success:
        print(f"DDL completado: {prm_name_ddl}")
    else:
        print(f"Falló DDL: {prm_name_ddl}")

except Exception as e:
    print(f"Error crítico: {str(e)}")
    raise
