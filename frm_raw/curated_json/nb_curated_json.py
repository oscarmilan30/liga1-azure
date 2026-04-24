# Databricks notebook source
# Databricks notebook source
# ==========================================================
# UNIFICAR ARCHIVOS JSON
# Proyecto: Liga 1 Perú
# Autor: Oscar García Del Águila
# ==========================================================

from env_setup import *
from pyspark.sql import SparkSession
from datetime import datetime
from utils_liga1 import setup_adls,get_dbutils,get_abfss_path,read_json_adls, write_parquet_adls,is_dataframe_empty
from curated_json import procesar_curated_json

# Obtener parámetros de ADF
dbutils.widgets.text("nombre_archivo", "")
dbutils.widgets.text("anio", "")
dbutils.widgets.text("filesystem", "")
dbutils.widgets.text("capa_landing", "")
dbutils.widgets.text("capa_raw", "")
dbutils.widgets.text("rutaBase", "")
dbutils.widgets.text("ModoEjecucion", "")
dbutils.widgets.text("FechaCarga", "")

nombre_archivo = dbutils.widgets.get("nombre_archivo")
anio = dbutils.widgets.get("anio")
filesystem = dbutils.widgets.get("filesystem")
capa_landing = dbutils.widgets.get("capa_landing").strip("/")
capa_raw = dbutils.widgets.get("capa_raw").strip("/")
rutaBase = dbutils.widgets.get("rutaBase").strip("/")
ModoEjecucion = dbutils.widgets.get("ModoEjecucion")
FechaCarga = dbutils.widgets.get("FechaCarga")

# ----------------------------------------------------------
# CONFIGURACIÓN DE ADLS Y SPARK
# ----------------------------------------------------------
spark = SparkSession.builder.getOrCreate()
setup_adls()
dbutils = get_dbutils()

print("===============================================")
print("PROCESO CURATED JSON → RAW")
print("===============================================")
print(f"Entidad        : {nombre_archivo}")
print(f"Año            : {anio}")
print(f"Modo ejecución : {ModoEjecucion}")
print("===============================================")

# COMMAND ----------


# ----------------------------------------------------------
# RUTAS ORIGEN / DESTINO
# ----------------------------------------------------------
ruta_json = get_abfss_path(f"{capa_landing}/{anio}/{nombre_archivo}_{anio}.json")

if ModoEjecucion == "HISTORICO":
    ruta_destino = f"{capa_raw}/{rutaBase}/{nombre_archivo}/stg/{anio}/curated"
elif ModoEjecucion == "REPROCESO":
    ruta_destino = f"{capa_raw}/{rutaBase}/{nombre_archivo}/{anio}/01/01/data"
else:
    fecha_fmt = FechaCarga.split(" ")[0].replace("-", "/")
    ruta_destino = f"{capa_raw}/{rutaBase}/{nombre_archivo}/{fecha_fmt}/data"

ruta_abfss = get_abfss_path(ruta_destino)

print(f"Ruta origen JSON : {ruta_json}")
print(f"Ruta destino RAW : {ruta_abfss}")

# ----------------------------------------------------------
# LECTURA, PROCESO Y ESCRITURA
# ----------------------------------------------------------
try:
    print("===== LECTURA DESDE LANDING =====")
    df = read_json_adls(spark, ruta_json)

     # Validar JSON vacío
    if is_dataframe_empty(df):
        raise Exception(f"No se encontró data en el archivo JSON: {ruta_json}")

    print("===== PROCESANDO CURATED JSON =====")
    df_proc = procesar_curated_json(df, anio)

    print("===== ESCRIBIENDO EN RAW =====")
    write_parquet_adls(df_proc, ruta_abfss)

    print("Curated JSON completado correctamente.")
except Exception as e:
    print("Error en Curated JSON:", str(e))
    import traceback
    print(traceback.format_exc())
    raise