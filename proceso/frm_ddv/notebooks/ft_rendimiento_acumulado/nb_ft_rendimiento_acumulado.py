# Databricks notebook source
# ==========================================================
# DDV - FT_RENDIMIENTO_ACUMULADO
# Proyecto: Liga 1 Perú
# Autor: Oscar García Del Águila
# ==========================================================

from utils_liga1 import (
    get_dbutils, get_pipeline, get_pipeline_params, get_predecesor,
    get_yaml_from_param, write_delta_udv, log, log_quality, log_wrong_records, is_dataframe_empty, read_delta_adls, get_abfss_path
)
from ft_rendimiento_acumulado import carga_final_ft_rendimiento_acumulado
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import traceback

spark = SparkSession.builder.getOrCreate()
# setup_adls()  # reemplazado por Access Connector (Managed Identity acc-liga1)

log("Inicio DDV ft_rendimiento_acumulado", "INFO", "ft_rendimiento_acumulado")

# COMMAND ----------

# ----------------------------------------------------------
# PARÁMETROS Y PREDECESORES
# ----------------------------------------------------------
try:
    dbutils.widgets.text("prm_pipelineid",   "40")
    dbutils.widgets.text("prm_periodo",      "0")
    dbutils.widgets.text("prm_id_ejecucion", "0")
    dbutils.widgets.text("prm_id_ejecucion_e2e", "0")
    prm_pipelineid   = int(dbutils.widgets.get("prm_pipelineid"))
    prm_periodo      = int(dbutils.widgets.get("prm_periodo"))  # 0 = carga completa, YYYY = incremental
    prm_id_ejecucion = int(dbutils.widgets.get("prm_id_ejecucion"))
    prm_id_ejecucion_e2e = int(dbutils.widgets.get("prm_id_ejecucion_e2e"))

    pipeline_info     = get_pipeline(prm_pipelineid)
    entity_name       = pipeline_info["pipeline"]
    dict_params       = get_pipeline_params(prm_pipelineid)
    dict_predecesores = get_predecesor(prm_pipelineid)

    prm_ruta_yaml    = dict_params["YAML_PATH"]
    prm_schema_tb    = dict_params["SCHEMA_TABLA"]
    prm_tabla_output = dict_params["NOMBRE_TABLA"]

    prm_ruta_hm_clasificacion = dict_predecesores["hm_tablas_clasificacion"]["Ruta_Predecesor"]
    prm_ruta_hm_valoracion = dict_predecesores["hm_valoracion_equipos"]["Ruta_Predecesor"]
    prm_ruta_dm_equipos = dict_predecesores["dm_equipos"]["Ruta_Predecesor"]

    log("Parámetros cargados", "INFO", entity_name)
except Exception as e:
    log(f"Error parámetros: {e}", "ERROR", "ft_rendimiento_acumulado")
    print(traceback.format_exc())
    raise

# COMMAND ----------

# ----------------------------------------------------------
# YAML
# ----------------------------------------------------------
try:
    yaml_conf = get_yaml_from_param(prm_ruta_yaml)
    cfg       = yaml_conf[entity_name]

    prm_cols_hm_clasificacion = cfg["cols_hm_clasificacion"]
    prm_cols_hm_valoracion    = cfg["cols_hm_valoracion"]
    prm_cols_dm_equipos       = cfg["cols_dm_equipos"]
    prm_schema                = cfg["schema"]

    log("YAML cargado", "INFO", entity_name)
except Exception as e:
    log(f"Error YAML: {e}", "ERROR", entity_name)
    print(traceback.format_exc())
    raise

# COMMAND ----------

# ----------------------------------------------------------
# LECTURA TABLAS
# ----------------------------------------------------------
try:
    df_clasificacion = read_delta_adls(spark, prm_ruta_hm_clasificacion)
    df_valoracion    = read_delta_adls(spark, prm_ruta_hm_valoracion)
    df_dm_equipos    = read_delta_adls(spark, prm_ruta_dm_equipos)

    if prm_periodo > 0:
        df_clasificacion = df_clasificacion.filter(col("periodo") == prm_periodo)

    if is_dataframe_empty(df_clasificacion):
        raise Exception(f"hm_tablas_clasificacion vacía: {prm_ruta_hm_clasificacion}")

    log("Tablas leídas", "INFO", entity_name)
except Exception as e:
    log(f"Error lectura: {e}", "ERROR", entity_name)
    print(traceback.format_exc())
    raise

# COMMAND ----------

# ----------------------------------------------------------
# TRANSFORMACIÓN + ESCRITURA
# ----------------------------------------------------------
cnt_entrada = df_clasificacion.count()
cnt_salida  = 0
try:
    df_final = carga_final_ft_rendimiento_acumulado(
        df_clasificacion, prm_cols_hm_clasificacion,
        df_valoracion,    prm_cols_hm_valoracion,
        df_dm_equipos,    prm_cols_dm_equipos,
        prm_schema
    )
    cnt_salida = df_final.count()
    df_wrong_nulos = df_final.filter(col("id_equipo").isNull())
    cnt_nulos = df_wrong_nulos.count()
    log_wrong_records(spark, df_wrong_nulos, entity_name, "DDV", prm_pipelineid, "NULO_CLAVE")
    if prm_periodo > 0:
        write_delta_udv(
            spark, df_final,
            prm_schema_tb, prm_tabla_output,
            mode="overwrite",
            replace_where=f"periodo = {prm_periodo}"
        )
    else:
        write_delta_udv(
            spark, df_final,
            prm_schema_tb, prm_tabla_output,
            mode="overwrite"
        )
    log("ft_rendimiento_acumulado cargado correctamente", "SUCCESS", entity_name)
except Exception as e:
    log(f"Error transformación/escritura: {e}", "ERROR", entity_name)
    print(traceback.format_exc())
    raise
finally:
    log_quality(prm_pipelineid, "DDV", f"{prm_schema_tb}.{prm_tabla_output}",
                cnt_entrada, cnt_salida, id_ejecucion=prm_id_ejecucion, id_ejecucion_e2e=prm_id_ejecucion_e2e, registros_nulos_clave=cnt_nulos)

log("Fin DDV ft_rendimiento_acumulado", "INFO", entity_name)
