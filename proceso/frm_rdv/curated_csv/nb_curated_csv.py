# Databricks notebook source
# ==========================================================
# UNIFICAR ARCHIVOS CSV
# Proyecto: Liga 1 Perú
# Autor: Oscar García Del Águila
# ==========================================================

import traceback
from pyspark.sql import SparkSession
from datetime import datetime
from utils_liga1 import get_dbutils, get_abfss_path, read_parquet_adls, write_parquet_adls,is_dataframe_empty
from curated_csv import procesar_curated_csv

# ----------------------------------------------------------
# PARÁMETROS DESDE ADF
# ----------------------------------------------------------
dbutils.widgets.text("nombre_archivo", "")
dbutils.widgets.text("anio", "")
dbutils.widgets.text("filesystem", "")
dbutils.widgets.text("capa_rdv", "")
dbutils.widgets.text("rutaBase", "")
dbutils.widgets.text("ModoEjecucion", "")
dbutils.widgets.text("FechaCarga", "")

nombre_archivo = dbutils.widgets.get("nombre_archivo")
anio = dbutils.widgets.get("anio")
filesystem = dbutils.widgets.get("filesystem")
capa_rdv = dbutils.widgets.get("capa_rdv").strip("/")
rutaBase = dbutils.widgets.get("rutaBase").strip("/")
ModoEjecucion = dbutils.widgets.get("ModoEjecucion")
FechaCarga = dbutils.widgets.get("FechaCarga")

# ----------------------------------------------------------
# CONFIGURACIÓN DE ADLS Y SPARK
# ----------------------------------------------------------
spark = SparkSession.builder.getOrCreate()
# setup_adls()  # reemplazado por Access Connector (Managed Identity acc-liga1)

print("===============================================")
print("PROCESO CURATED CSV → RDV")
print("===============================================")
print(f"Entidad        : {nombre_archivo}")
print(f"Año            : {anio}")
print(f"Modo ejecución : {ModoEjecucion}")
print("===============================================")

# COMMAND ----------

# ----------------------------------------------------------
# RUTAS ORIGEN (STG) / DESTINO (RDV)
# ----------------------------------------------------------
if ModoEjecucion == "HISTORICO":
    ruta_origen = f"{capa_rdv}/{nombre_archivo}/stg/{anio}/data"
    ruta_destino = f"{capa_rdv}/{nombre_archivo}/stg/{anio}/curated"

elif ModoEjecucion == "REPROCESO":
    ruta_origen = f"{capa_rdv}/{nombre_archivo}/stg/{anio}/01/01/data"
    ruta_destino = f"{capa_rdv}/{nombre_archivo}/{anio}/01/01/data"

else:  # INCREMENTAL
    fecha_fmt = FechaCarga.split(" ")[0].replace("-", "/")
    ruta_origen = f"{capa_rdv}/{nombre_archivo}/stg/{fecha_fmt}/data"
    ruta_destino = f"{capa_rdv}/{nombre_archivo}/{fecha_fmt}/data"


ruta_abfss_origen = get_abfss_path(ruta_origen)
ruta_abfss_destino = get_abfss_path(ruta_destino)

# ----------------------------------------------------------
# EJECUCIÓN SEGURA
# ----------------------------------------------------------
try:
    print("===== LECTURA DESDE STG/DATA =====")
    df = read_parquet_adls(spark, ruta_abfss_origen)

    # Validar si el DataFrame está vacío antes de procesar
    if is_dataframe_empty(df):
        raise Exception(f"No se encontró data en la ruta origen: {ruta_abfss_origen}")

    print("===== APLICANDO CURATED CSV =====")
    df_proc = procesar_curated_csv(df)

    print("===== ESCRITURA EN STG/CURATED =====")
    write_parquet_adls(df_proc, ruta_abfss_destino)
    print("Curated CSV completado correctamente.")

    # ------------------------------------------------------
    # LIMPIEZA DE CARPETA STG/DATA (solo si existe)
    # ------------------------------------------------------
    # try:
    #     print(f"Eliminando carpeta temporal: {ruta_abfss_origen}")
    #     dbutils.fs.rm(ruta_abfss_origen, recurse=True)
    #     print("Carpeta stg/data eliminada correctamente.")
    # except Exception as e:
    #     print(f"No se pudo eliminar carpeta {ruta_abfss_origen}: {str(e)}")

except Exception as e:
    print("Error en Curated CSV:", str(e))
    import traceback
    print(traceback.format_exc())
    raise
