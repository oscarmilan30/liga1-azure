# Databricks notebook source
# ==========================================================
# UDV - CATALOGO_EQUIPOS
# Proyecto: Liga 1 Perú
# Autor: Oscar García Del Águila
# ==========================================================

from pyspark.sql import SparkSession
from utils_liga1 import get_dbutils, get_abfss_path, read_parquet_adls, is_dataframe_empty,get_predecesor, get_pipeline_params,get_yaml_from_param,write_delta_udv, log, log_quality, get_pipeline
from md_catalogo_equipos import carga_final
import traceback
import sys

# ----------------------------------------------------------
# CONFIGURACIÓN INICIAL
# ----------------------------------------------------------
log("Inicio de ejecución del pipeline UDV", "INFO", "md_catalogo_equipos")

spark = SparkSession.builder.getOrCreate()
# setup_adls()  # reemplazado por Access Connector (Managed Identity acc-liga1)

# COMMAND ----------

# ----------------------------------------------------------
# PARÁMETROS Y PREDECESORES
# ----------------------------------------------------------
try:
    dbutils.widgets.text("prm_pipelineid", "4")
    dbutils.widgets.text("prm_id_ejecucion", "0")
    dbutils.widgets.text("prm_id_ejecucion_e2e", "0")
    prm_pipelineid   = int(dbutils.widgets.get("prm_pipelineid"))
    prm_id_ejecucion = int(dbutils.widgets.get("prm_id_ejecucion"))
    prm_id_ejecucion_e2e = int(dbutils.widgets.get("prm_id_ejecucion_e2e"))

    pipeline_name = get_pipeline(prm_pipelineid)
    entity_name = pipeline_name['pipeline']

    dict_predecesores = get_predecesor(prm_pipelineid)
    dict_params = get_pipeline_params(prm_pipelineid)


    prm_ruta_predecesor = dict_predecesores["catalogo_equipos"]["Ruta_Predecesor"]
    ruta_abfss_origen = get_abfss_path(prm_ruta_predecesor)

    prm_formato = dict_params["FORMATO_SALIDA"]
    prm_schema_tb = dict_params["SCHEMA_TABLA"]
    prm_tabla_output = dict_params["NOMBRE_TABLA"]
    prm_ruta_yaml = dict_params["YAML_PATH"]

    log("Parámetros cargados correctamente", "INFO", entity_name)

except Exception as e:
    log(f"Error al cargar parámetros o predecesores: {e}", "ERROR", entity_name)
    print(traceback.format_exc())
    raise

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
    print(traceback.format_exc())
    raise

# COMMAND ----------

# ----------------------------------------------------------
# EJECUCIÓN PRINCIPAL
# ----------------------------------------------------------
cnt_entrada = 0
cnt_salida  = 0
cnt_nulos   = 0
try:
    log("Lectura desde RDV/DATA", "INFO", entity_name)
    df = read_parquet_adls(spark, ruta_abfss_origen)

    if is_dataframe_empty(df):
        raise Exception(f"No se encontró data en la ruta origen: {ruta_abfss_origen}")

    cnt_entrada = df.count()
    df_final = carga_final(df, prm_cols, prm_schema)

    # 1. MERGE simple (UPSERT)
    write_delta_udv(
        spark, df_final, prm_schema_tb, prm_tabla_output,
        mode="merge",
        merge_condition="delta.id_equipo = df.id_equipo",
        coalesce_on_match=True,
        coalesce_key_cols=["id_equipo"]
    )

    cnt_salida = df_final.count()
    log("Proceso completado correctamente", "SUCCESS", entity_name)

except Exception as e:
    log(f"Error en ejecución: {e}", "ERROR", entity_name)
    print(traceback.format_exc())
    raise
finally:
    log_quality(int(prm_pipelineid), "UDV", f"{prm_schema_tb}.{prm_tabla_output}", cnt_entrada, cnt_salida, id_ejecucion=prm_id_ejecucion, id_ejecucion_e2e=prm_id_ejecucion_e2e, registros_nulos_clave=cnt_nulos)

log("Finalización del pipeline UDV", "INFO", entity_name)
