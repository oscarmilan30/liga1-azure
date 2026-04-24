# Databricks notebook source
# Databricks notebook source
# ==========================================================
# UDV - CATALOGO_EQUIPOS
# Proyecto: Liga 1 Perú
# Autor: Oscar García Del Águila
# ==========================================================

from env_setup import *
from pyspark.sql import SparkSession
from utils_liga1 import setup_adls, get_dbutils, get_abfss_path, read_parquet_adls, is_dataframe_empty,get_predecesor, get_pipeline_params,get_yaml_from_param,write_delta_udv, log, get_pipeline
from md_catalogo_equipos import carga_final
import traceback
import sys

# ----------------------------------------------------------
# CONFIGURACIÓN INICIAL
# ----------------------------------------------------------
log("Inicio de ejecución del pipeline UDV", "INFO", "md_catalogo_equipos")

spark = SparkSession.builder.getOrCreate()
setup_adls()
dbutils = get_dbutils()

# COMMAND ----------

# ----------------------------------------------------------
# PARÁMETROS Y PREDECESORES
# ----------------------------------------------------------
try:
    dbutils.widgets.text("prm_pipelineid", "4")
    prm_pipelineid = dbutils.widgets.get("prm_pipelineid")

    pipeline_name = get_pipeline(prm_pipelineid)
    entity_name = pipeline_name['pipeline']

    dict_predecesores = get_predecesor(prm_pipelineid)
    dict_params = get_pipeline_params(prm_pipelineid)


    prm_ruta_predecesor = dict_predecesores["catalogo_quipos"]["Ruta_Predecesor"]
    ruta_abfss_origen = get_abfss_path(prm_ruta_predecesor)

    prm_capa_udv = dict_params["CAPA_UDV"]
    prm_ruta_base = dict_params["RUTA_BASE"]
    prm_ruta_tabla = dict_params["RUTA_TABLA"]
    prm_formato = dict_params["FORMATO_SALIDA"]
    prm_schema_tb = dict_params["SCHEMA_TABLA"]
    prm_tabla_output = dict_params["NOMBRE_TABLA"]
    prm_ruta_yaml = dict_params["YAML_PATH"]

    prm_ruta_tabla_output = f"{prm_capa_udv}/{prm_ruta_base}/{prm_ruta_tabla}"
    ruta_delta_udv = get_abfss_path(prm_ruta_tabla_output)

    log("Parámetros cargados correctamente", "INFO", entity_name)

except Exception as e:
    log(f"Error al cargar parámetros o predecesores: {e}", "ERROR", entity_name)

# COMMAND ----------

# ----------------------------------------------------------
# LECTURA YAML
# ----------------------------------------------------------
try:
    yaml_conf = get_yaml_from_param(prm_ruta_yaml)
    prm_cols = yaml_conf[entity_name]["cols"]
    prm_schema = yaml_conf[entity_name]["schema"]
    log("YAML cargado correctamente", "INFO", entity_name)
except Exception as e:
    log(f"Error al leer YAML {prm_ruta_yaml}: {e}", "ERROR", entity_name)

# COMMAND ----------

# ----------------------------------------------------------
# EJECUCIÓN PRINCIPAL
# ----------------------------------------------------------
try:
    log("Lectura desde RAW/DATA", "INFO", entity_name)
    df = read_parquet_adls(spark, ruta_abfss_origen)

    if is_dataframe_empty(df):
        raise Exception(f"No se encontró data en la ruta origen: {ruta_abfss_origen}")

    df_final = carga_final(df, prm_cols, prm_schema)

    # 1. MERGE simple (UPSERT)
    write_delta_udv(
        spark, df_final, prm_schema_tb, prm_tabla_output, 
        mode="merge",
        merge_condition="delta.id_equipo = df.id_equipo"
    )

    log("Proceso completado correctamente", "SUCCESS", entity_name)

except Exception as e:
    log(f"Error en ejecución: {e}", "ERROR", entity_name)
    print(traceback.format_exc())

log("Finalización del pipeline UDV", "INFO", entity_name)