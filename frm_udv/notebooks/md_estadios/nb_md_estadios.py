# Databricks notebook source
# Databricks notebook source
# ==========================================================
# UDV - ESTADIOS
# Proyecto: Liga 1 Perú
# Autor: Oscar García Del Águila
# ==========================================================

from env_setup import *
from utils_liga1 import setup_adls, get_dbutils, get_entity_data, get_abfss_path, is_dataframe_empty, get_predecesor,get_pipeline_params, get_yaml_from_param, write_delta_udv, log, read_udv_table, get_pipeline, extract_entity_name
from pyspark.sql import SparkSession
from md_estadios import carga_final
import traceback
import sys

# ----------------------------------------------------------
# CONFIGURACIÓN INICIAL
# ----------------------------------------------------------

log("Inicio de ejecución del pipeline UDV", "INFO", "md_estadios")

spark = SparkSession.builder.getOrCreate()
setup_adls()
dbutils = get_dbutils()

# COMMAND ----------

# ----------------------------------------------------------
# PARÁMETROS Y PREDECESORES
# ----------------------------------------------------------
try:
    # Para pruebas puedes dejar un valor por defecto; luego ADF lo setea
    dbutils.widgets.text("prm_pipelineid", "7")
    prm_pipelineid = int(dbutils.widgets.get("prm_pipelineid"))

    pipeline_name = get_pipeline(prm_pipelineid)
    entity_name = pipeline_name["pipeline"]               # ej: 'md_estadios'

    dict_predecesores = get_predecesor(prm_pipelineid)
    dict_params       = get_pipeline_params(prm_pipelineid)

    prm_ruta_tabla_pred = dict_predecesores["md_catalogo_equipos"]["RutaTabla"]  # ej: 'tb_udv.md_catalogo_equipos'

    prm_capa_udv     = dict_params["CAPA_UDV"]
    prm_ruta_base    = dict_params["RUTA_BASE"]
    prm_ruta_tabla   = dict_params["RUTA_TABLA"]
    prm_formato      = dict_params["FORMATO_SALIDA"]
    prm_schema_tb    = dict_params["SCHEMA_TABLA"]
    prm_tabla_output = dict_params["NOMBRE_TABLA"]
    prm_ruta_yaml    = dict_params["YAML_PATH"]

    prm_ruta_tabla_output = f"{prm_capa_udv}/{prm_ruta_base}/{prm_ruta_tabla}"
    ruta_delta_udv = get_abfss_path(prm_ruta_tabla_output)

    log("Parámetros cargados correctamente", "INFO", entity_name)

except Exception as e:
    log(f"Error al cargar parámetros o predecesores: {e}", "ERROR", "md_estadios")
    print(traceback.format_exc())
    raise

# COMMAND ----------

# ----------------------------------------------------------
# LECTURA YAML
# ----------------------------------------------------------
try:
    yaml_conf = get_yaml_from_param(prm_ruta_yaml)

    prm_cols_catalogo_equipos    = yaml_conf[entity_name]["cols_catalogo_equipos"]
    prm_cols_estadios            = yaml_conf[entity_name]["cols_estadios"]
    prm_drop_duplicates_estadios = yaml_conf[entity_name]["drop_duplicates_estadios"]
    prm_rename_columns           = yaml_conf[entity_name]["rename_columns"]
    prm_schema                   = yaml_conf[entity_name]["schema"]
    prm_dedup_cols               = yaml_conf[entity_name]["dedup_cols"]
    prm_case_rules               = yaml_conf[entity_name]["case_rules"]

    log("YAML cargado correctamente", "INFO", entity_name)

except Exception as e:
    log(f"Error al leer YAML {prm_ruta_yaml}: {e}", "ERROR", entity_name)
    print(traceback.format_exc())
    raise

# COMMAND ----------

# ----------------------------------------------------------
# LECTURA PREDECESORES (CATÁLOGO + RAW)
# ----------------------------------------------------------
try:
    log("Lectura desde UDV/RAW", "INFO", entity_name)

    # 1) Catálogo de equipos (md_catalogo_equipos) vía RutaTabla
    df_catalogo_equipos = read_udv_table(prm_ruta_tabla_pred)

    if is_dataframe_empty(df_catalogo_equipos):
        raise Exception(f"No se encontró data en la tabla predecesora: {prm_ruta_tabla_pred}")

    # 2) RAW estadios desde tbl_paths (flg_udv = 'N')
    entity_raw = extract_entity_name(entity_name)  # 'md_estadios' -> 'estadios'

    df_raw_estadios = get_entity_data(entity_raw,dedup_cols=prm_dedup_cols)

    if is_dataframe_empty(df_raw_estadios):
        raise Exception(f"No se encontró data en RAW para la entidad: {entity_raw}")

    log("Predecesores completados correctamente", "INFO", entity_name)

except Exception as e:
    log(f"Error en lectura de predecesores/RAW: {e}", "ERROR", entity_name)
    print(traceback.format_exc())
    raise

log("Finalización de la etapa de lectura", "INFO", entity_name)

# COMMAND ----------

# ----------------------------------------------------------
# EJECUCIÓN PRINCIPAL
# ----------------------------------------------------------
try:
    log("Inicio de Ejecución Principal", "INFO", entity_name)

    df_final = carga_final(
        df_catalogo_equipos,
        df_raw_estadios,
        prm_cols_catalogo_equipos,
        prm_cols_estadios,
        prm_drop_duplicates_estadios,
        prm_rename_columns,
        prm_schema,
        prm_case_rules
    )


    # MERGE simple (UPSERT) sobre id_equipo
    write_delta_udv(
        spark,
        df_final,
        prm_schema_tb,
        prm_tabla_output,
        mode="merge",
        merge_condition="delta.id_estadio = df.id_estadio"
    )

    log("Proceso completado correctamente", "SUCCESS", entity_name)

except Exception as e:
    log(f"Error en ejecución principal: {e}", "ERROR", entity_name)
    print(traceback.format_exc())
    raise

log("Finalización del pipeline UDV", "INFO", entity_name)