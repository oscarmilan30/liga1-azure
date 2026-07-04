# Databricks notebook source
# ==========================================================
# DDV - FT_ESTADISTICAS_JUGADORES
# Proyecto: Liga 1 Perú
# Autor: Oscar García Del Águila
# ==========================================================

from utils_liga1 import (
    is_dataframe_empty, get_predecesor, get_pipeline_params,
    get_yaml_from_param, write_delta_udv, log, log_quality,
    read_delta_adls, get_pipeline
)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from ft_estadisticas_jugadores import carga_final_ft_estadisticas_jugadores
import traceback

# ----------------------------------------------------------
# CONFIGURACIÓN INICIAL
# ----------------------------------------------------------
entity_name = "ft_estadisticas_jugadores"
log("Inicio de ejecución del pipeline DDV", "INFO", entity_name)

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# ----------------------------------------------------------
# PARÁMETROS Y PREDECESORES
# ----------------------------------------------------------
try:
    dbutils.widgets.text("prm_pipelineid",        "44")
    dbutils.widgets.text("prm_id_ejecucion",      "0")
    dbutils.widgets.text("prm_id_ejecucion_e2e",  "0")
    dbutils.widgets.text("prm_periodo",           "0")  # 0 = carga completa, YYYY = incremental

    prm_pipelineid       = int(dbutils.widgets.get("prm_pipelineid"))
    prm_id_ejecucion     = int(dbutils.widgets.get("prm_id_ejecucion"))
    prm_id_ejecucion_e2e = int(dbutils.widgets.get("prm_id_ejecucion_e2e"))
    prm_periodo          = int(dbutils.widgets.get("prm_periodo"))

    pipeline_info     = get_pipeline(prm_pipelineid)
    entity_name       = pipeline_info["pipeline"]

    dict_params       = get_pipeline_params(prm_pipelineid)
    dict_predecesores = get_predecesor(prm_pipelineid)

    # Predecesores
    prm_ruta_hm_estadisticas    = dict_predecesores["hm_estadisticas_jugadores"]["Ruta_Predecesor"]
    prm_ruta_md_plantillas      = dict_predecesores["md_plantillas"]["Ruta_Predecesor"]
    prm_ruta_dm_equipos         = dict_predecesores["dm_equipos"]["Ruta_Predecesor"]

    prm_capa_ddv     = dict_params["CAPA_DDV"]
    prm_ruta_base    = dict_params.get("RUTA_BASE", "")
    prm_formato      = dict_params["FORMATO_SALIDA"]
    prm_schema_tb    = dict_params["SCHEMA_TABLA"]
    prm_tabla_output = dict_params["NOMBRE_TABLA"]
    prm_ruta_yaml    = dict_params["YAML_PATH"]

    log("Parámetros cargados correctamente", "INFO", entity_name)

except Exception as e:
    log(f"Error al cargar parámetros: {e}", "ERROR", entity_name)
    print(traceback.format_exc())
    raise

# COMMAND ----------

# ----------------------------------------------------------
# LECTURA YAML
# ----------------------------------------------------------
try:
    yaml_conf = get_yaml_from_param(prm_ruta_yaml)

    prm_cols_hm_estadisticas     = yaml_conf[entity_name]["cols_hm_estadisticas"]
    prm_cols_md_plantillas       = yaml_conf[entity_name]["cols_md_plantillas"]
    prm_cols_hm_plantillas_equipo = yaml_conf[entity_name]["cols_hm_plantillas_equipo"]
    prm_cols_dm_equipos          = yaml_conf[entity_name]["cols_dm_equipos"]
    prm_schema                   = yaml_conf[entity_name]["schema"]

    log("YAML cargado correctamente", "INFO", entity_name)

except Exception as e:
    log(f"Error al leer YAML {prm_ruta_yaml}: {e}", "ERROR", entity_name)
    print(traceback.format_exc())
    raise

# COMMAND ----------

# ----------------------------------------------------------
# LECTURA PREDECESORES
# ----------------------------------------------------------
try:
    log("Lectura de predecesores UDV", "INFO", entity_name)

    df_hm_estadisticas     = read_delta_adls(spark, prm_ruta_hm_estadisticas)
    if prm_periodo > 0:
        df_hm_estadisticas = df_hm_estadisticas.filter(col("periodo") == prm_periodo)
    df_md_plantillas       = read_delta_adls(spark, prm_ruta_md_plantillas)
    df_dm_equipos          = read_delta_adls(spark, prm_ruta_dm_equipos)

    # hm_plantillas_equipo: predecesor indirecto a través de md_plantillas
    # Ruta construida igual que las otras tablas UDV
    prm_ruta_hm_plantillas_equipo = dict_predecesores.get(
        "hm_plantillas_equipo", {}
    ).get("Ruta_Predecesor", "primera_division/udv/hm_plantillas_equipo/data")
    df_hm_plantillas_equipo = read_delta_adls(spark, prm_ruta_hm_plantillas_equipo)

    log("Predecesores cargados correctamente", "INFO", entity_name)

except Exception as e:
    log(f"Error en lectura de predecesores: {e}", "ERROR", entity_name)
    print(traceback.format_exc())
    raise

# COMMAND ----------

# ----------------------------------------------------------
# EJECUCIÓN PRINCIPAL
# ----------------------------------------------------------
cnt_entrada = df_hm_estadisticas.count()
cnt_salida  = 0

try:
    log("Inicio de Ejecución Principal DDV", "INFO", entity_name)

    df_final = carga_final_ft_estadisticas_jugadores(
        df_hm_estadisticas,
        prm_cols_hm_estadisticas,
        df_md_plantillas,
        prm_cols_md_plantillas,
        df_hm_plantillas_equipo,
        prm_cols_hm_plantillas_equipo,
        df_dm_equipos,
        prm_cols_dm_equipos,
        prm_schema
    )

    if is_dataframe_empty(df_final):
        log("df_final vacío, no se realizará escritura en DDV", "WARN", entity_name)
    else:
        if prm_periodo > 0:
            log(f"Escribiendo FT incremental para periodo={prm_periodo}", "INFO", entity_name)
            write_delta_udv(
                spark,
                df_final,
                prm_schema_tb,
                prm_tabla_output,
                mode="overwrite",
                replace_where=f"periodo = {prm_periodo}"
            )
        else:
            log("Escribiendo FT completo (carga histórica)", "INFO", entity_name)
            write_delta_udv(
                spark,
                df_final,
                prm_schema_tb,
                prm_tabla_output,
                mode="overwrite",
                partition_by=["periodo"],
                overwrite_dynamic_partition=True
            )

        cnt_salida = df_final.count()
        log("Escritura DDV completada exitosamente", "SUCCESS", entity_name)

except Exception as e:
    log(f"Error en ejecución principal: {e}", "ERROR", entity_name)
    print(traceback.format_exc())
    raise
finally:
    log_quality(
        prm_pipelineid, "DDV", f"{prm_schema_tb}.{prm_tabla_output}",
        cnt_entrada, cnt_salida,
   