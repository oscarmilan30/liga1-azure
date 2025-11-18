# Databricks notebook source
# Databricks notebook source
# ==========================================================
# CURATED - DATAENTRY EQUIPOS
# Proyecto: Liga 1 Perú
# Autor: Oscar García Del Águila
# ==========================================================

from env_setup import *
from pyspark.sql import SparkSession
from utils_liga1 import setup_adls, get_dbutils, get_abfss_path, read_parquet_adls, write_parquet_adls, is_dataframe_empty,get_pipeline_params,log
from curated_dataentry import procesar_curated_dataentry

# ----------------------------------------------------------
# CONFIGURACIÓN INICIAL
# ----------------------------------------------------------
entity_name = "catalogo_equipos"
log("Inicio de ejecución del pipeline Curated", "INFO", entity_name)

spark = SparkSession.builder.getOrCreate()
setup_adls()
dbutils = get_dbutils()

# COMMAND ----------

# ----------------------------------------------------------
# PARÁMETROS
# ----------------------------------------------------------
try:
    dbutils.widgets.text("prm_pipelineid", "")
    prm_pipelineid = dbutils.widgets.get("prm_pipelineid")

    dict_params = get_pipeline_params(prm_pipelineid)
    prm_nombre_archivo = dict_params["NOMBRE_ARCHIVO"]
    prm_filesystem = dict_params["FILESYSTEM"]
    prm_capa_raw = dict_params["CAPA_RAW"]
    prm_ruta_base = dict_params["RUTA_BASE"]

    log("Parámetros obtenidos correctamente", "INFO", entity_name)

except Exception as e:
    log(f"Error al obtener parámetros: {e}", "ERROR", entity_name)

# COMMAND ----------

# ----------------------------------------------------------
# RUTAS DE LECTURA / ESCRITURA
# ----------------------------------------------------------
ruta_origen = f"{prm_capa_raw}/{prm_ruta_base}/{prm_nombre_archivo}/stg/{prm_nombre_archivo}/data"
ruta_destino = f"{prm_capa_raw}/{prm_ruta_base}/{prm_nombre_archivo}/data"

ruta_abfss_origen = get_abfss_path(ruta_origen)
ruta_abfss_destino = get_abfss_path(ruta_destino)

# ----------------------------------------------------------
# PROCESAMIENTO
# ----------------------------------------------------------
try:
    log("Lectura desde STG/DATA", "INFO", entity_name)
    df = read_parquet_adls(spark, ruta_abfss_origen)

    if is_dataframe_empty(df):
        raise Exception(f"No se encontró data en la ruta origen: {ruta_abfss_origen}")

    log("Limpieza y normalización", "INFO", entity_name)
    df_curated = procesar_curated_dataentry(df)

    log("Escritura a destino final", "INFO", entity_name)
    write_parquet_adls(df_curated, ruta_abfss_destino)

    log("Eliminando carpeta temporal STG", "INFO", entity_name)
    dbutils.fs.rm(ruta_abfss_origen, recurse=True)

    log("Proceso completado correctamente", "SUCCESS", entity_name)

except Exception as e:
    log(f"Error en ejecución: {e}", "ERROR", entity_name)
    print(traceback.format_exc())

log("Finalización del pipeline Curated", "INFO", entity_name)