# Databricks notebook source
# ==========================================================
# UDV - HM_ESTADISTICAS_JUGADORES
# Proyecto: Liga 1 Perú
# Autor: Oscar García Del Águila
# ==========================================================

from utils_liga1 import (
    get_entity_data, is_dataframe_empty, get_predecesor, get_pipeline_params,
    get_yaml_from_param, write_delta_udv, log, log_quality, log_wrong_records,
    read_delta_adls, get_pipeline
)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from hm_estadisticas_jugadores import carga_final_hm_estadisticas_jugadores
import traceback

# ----------------------------------------------------------
# CONFIGURACIÓN INICIAL
# ----------------------------------------------------------
entity_name = "hm_estadisticas_jugadores"
log("Inicio de ejecución del pipeline UDV", "INFO", entity_name)

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# ----------------------------------------------------------
# PARÁMETROS Y PREDECESORES
# ----------------------------------------------------------
try:
    dbutils.widgets.text("prm_pipelineid",        "43")
    dbutils.widgets.text("prm_id_ejecucion",      "0")
    dbutils.widgets.text("prm_id_ejecucion_e2e",  "0")
    dbutils.widgets.text("prm_modo_ejecucion",    "HISTORICO")

    prm_pipelineid       = int(dbutils.widgets.get("prm_pipelineid"))
    prm_id_ejecucion     = int(dbutils.widgets.get("prm_id_ejecucion"))
    prm_id_ejecucion_e2e = int(dbutils.widgets.get("prm_id_ejecucion_e2e"))
    prm_modo_ejecucion   = dbutils.widgets.get("prm_modo_ejecucion")

    pipeline_info     = get_pipeline(prm_pipelineid)
    entity_name       = pipeline_info["pipeline"]

    dict_params       = get_pipeline_params(prm_pipelineid)
    dict_predecesores = get_predecesor(prm_pipelineid)

    # Rutas predecesores
    entity_raw_stats              = dict_predecesores["estadisticas_jugadores"]["RutaTabla"]
    prm_ruta_md_plantillas        = dict_predecesores["md_plantillas"]["Ruta_Predecesor"]
    prm_ruta_md_catalogo_equipos  = dict_predecesores["md_catalogo_equipos"]["Ruta_Predecesor"]

    prm_capa_udv     = dict_params["CAPA_UDV"]
    prm_ruta_base    = dict_params.get("RUTA_BASE", "")
    prm_ruta_tabla   = dict_params["RUTA_TABLA"]
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

    prm_cols_raw      = yaml_conf[entity_name]["cols_raw_estadisticas"]
    prm_cols_catalogo = yaml_conf[entity_name]["cols_catalogo_equipos"]
    prm_case_rules    = yaml_conf[entity_name]["case_rules"]
    prm_rename_cols   = yaml_conf[entity_name]["rename_columns"]
    prm_schema        = yaml_conf[entity_name]["schema"]

    log("YAML cargado correctamente", "INFO", entity_name)

except Exception as e:
    log(f"Error al leer YAML {prm_ruta_yaml}: {e}", "ERROR", entity_name)
    print(traceback.format_exc())
    raise

# COMMAND ----------

# ----------------------------------------------------------
# LECTURA RDV — estadisticas_jugadores
# ----------------------------------------------------------
_no_raw_data = False
try:
    log("Lectura desde RDV estadisticas_jugadores", "INFO", entity_name)

    df_raw_estadisticas = get_entity_data(
        entity_raw_stats,
        dedup_cols=None,
        modoejecucion=prm_modo_ejecucion
    )

    if df_raw_estadisticas is None or is_dataframe_empty(df_raw_estadisticas):
        log("No hay data pendiente en RDV para estadisticas_jugadores. Fin sin error.", "WARN", entity_name)
        _no_raw_data = True
    else:
        log(f"Registros raw leídos: {df_raw_estadisticas.count()}", "INFO", entity_name)

except Exception as e:
    log(f"Error en lectura de estadisticas_jugadores: {e}", "ERROR", entity_name)
    print(traceback.format_exc())
    raise

if _no_raw_data:
    dbutils.notebook.exit("NO_DATA")

# COMMAND ----------

# ----------------------------------------------------------
# LECTURA PREDECESORES — md_catalogo_equipos y md_plantillas
# ----------------------------------------------------------
try:
    log("Lectura de md_catalogo_equipos", "INFO", entity_name)
    df_md_catalogo_equipos = read_delta_adls(spark, prm_ruta_md_catalogo_equipos)
    log("md_catalogo_equipos cargado correctamente", "INFO", entity_name)

    log("Lectura de md_plantillas", "INFO", entity_name)
    df_md_plantillas = read_delta_adls(spark, prm_ruta_md_plantillas)
    log("md_plantillas cargado correctamente", "INFO", entity_name)

except Exception as e:
    log(f"Error al leer predecesores: {e}", "ERROR", entity_name)
    print(traceback.format_exc())
    raise

# COMMAND ----------

# ----------------------------------------------------------
# EJECUCIÓN PRINCIPAL
# ----------------------------------------------------------
cnt_entrada = df_raw_estadisticas.count()
cnt_salida  = 0
cnt_nulos   = 0

try:
    log("Inicio de Ejecución Principal", "INFO", entity_name)

    df_final, df_sin_match = carga_final_hm_estadisticas_jugadores(
        df_raw_estadisticas,
        prm_cols_raw,
        df_md_catalogo_equipos,
        prm_cols_catalogo,
        df_md_plantillas,
        prm_case_rules,
        prm_rename_cols,
        prm_schema
    )

    log_wrong_records(spark, df_sin_match, entity_name, "UDV", prm_pipelineid, "SIN_MATCH")
    cnt_nulos = df_final.filter(col("id_estadistica_jugador").isNull()).count()

    if is_dataframe_empty(df_final):
        log("df_final vacío, no se realizará escritura en UDV", "WARN", entity_name)
    else:
        log("Escribiendo HM con overwrite dinámico por partición [periodo]", "INFO", entity_name)

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
        log("Escritura completada exitosamente", "SUCCESS", entity_name)

except Exception as e:
    log(f"Error en ejecución principal: {e}", "ERROR", entity_name)
    print(traceback.format_exc())
    raise
finally:
    log_quality(
        prm_pipelineid, "UDV", f"{prm_schema_tb}.{prm_tabla_output}",
        cnt_entrada, cnt_salida,
        id_ejecucion=prm_id_ej