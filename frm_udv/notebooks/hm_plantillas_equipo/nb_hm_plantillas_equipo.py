# Databricks notebook source
# ==========================================================
# UDV - JUGADORES
# Proyecto: Liga 1 Perú
# Autor: Oscar García Del Águila
# ==========================================================

from env_setup import *
from utils_liga1 import setup_adls, get_dbutils, get_entity_data, is_dataframe_empty, get_predecesor,get_pipeline_params, get_yaml_from_param, write_delta_udv, log, read_udv_table, get_pipeline
from pyspark.sql import SparkSession
#from hm_plantillas_equipo import carga_final
import traceback
import sys

# ----------------------------------------------------------
# CONFIGURACIÓN INICIAL
# ----------------------------------------------------------

log("Inicio de ejecución del pipeline UDV", "INFO", "hm_plantillas_equipo")

spark = SparkSession.builder.getOrCreate()
setup_adls()
dbutils = get_dbutils()

# COMMAND ----------

# ----------------------------------------------------------
# PARÁMETROS Y PREDECESORES
# ----------------------------------------------------------
try:
    # Para pruebas puedes dejar un valor por defecto; luego ADF lo setea
    dbutils.widgets.text("prm_pipelineid", "9")
    prm_pipelineid = int(dbutils.widgets.get("prm_pipelineid"))

    pipeline_name = get_pipeline(prm_pipelineid)
    entity_name = pipeline_name["pipeline"]               # ej: 'md_estadios'
    parent_pipelineid = pipeline_name["parent_pipelineid"]

    dict_params       = get_pipeline_params(prm_pipelineid)
    dict_predecesores = get_predecesor(prm_pipelineid)
    
    dict_params_chd   = get_pipeline_params(parent_pipelineid)
    dict_predecesores_chd = get_predecesor(parent_pipelineid)

    prm_tabla_md_plantillas = dict_predecesores["md_plantillas"]["RutaTabla"]
    prm_tabla_md_catalogo_equipos = dict_predecesores["md_catalogo_equipos"]["RutaTabla"]
    entity_raw = dict_predecesores_chd["plantillas"]["RutaTabla"]

    prm_capa_udv     = dict_params["CAPA_UDV"]
    prm_ruta_base    = dict_params["RUTA_BASE"]
    prm_ruta_tabla   = dict_params["RUTA_TABLA"]
    prm_formato      = dict_params["FORMATO_SALIDA"]
    prm_schema_tb    = dict_params["SCHEMA_TABLA"]
    prm_tabla_output = dict_params["NOMBRE_TABLA"]
    prm_ruta_yaml    = dict_params["YAML_PATH"]

    prm_capa_udv_chd     = dict_params["CAPA_UDV"]
    prm_ruta_base_chd    = dict_params["RUTA_BASE"]
    prm_ruta_tabla_chd   = dict_params["RUTA_TABLA"]
    prm_formato_chd      = dict_params["FORMATO_SALIDA"]
    prm_schema_tb        = dict_params["SCHEMA_TABLA"]
    prm_tabla_output_chd = dict_params["NOMBRE_TABLA"]


    log("Parámetros cargados correctamente", "INFO", entity_name)

except Exception as e:
    log(f"Error al cargar parámetros o predecesores: {e}", "ERROR", entity_name)
    print(traceback.format_exc())
    raise

# COMMAND ----------

# # ----------------------------------------------------------
# # LECTURA YAML
# # ----------------------------------------------------------
# try:
#     yaml_conf = get_yaml_from_param(prm_ruta_yaml)

#     prm_cols_catalogo_equipos    = yaml_conf[entity_name]["cols_catalogo_equipos"]
#     prm_cols_estadios            = yaml_conf[entity_name]["cols_estadios"]
#     prm_drop_duplicates_estadios = yaml_conf[entity_name]["drop_duplicates_estadios"]
#     prm_rename_columns           = yaml_conf[entity_name]["rename_columns"]
#     prm_schema                   = yaml_conf[entity_name]["schema"]
#     prm_dedup_cols               = yaml_conf[entity_name]["dedup_cols"]
#     prm_case_rules               = yaml_conf[entity_name]["case_rules"]

#     log("YAML cargado correctamente", "INFO", entity_name)

# except Exception as e:
#     log(f"Error al leer YAML {prm_ruta_yaml}: {e}", "ERROR", entity_name)
#     print(traceback.format_exc())
#     raise

# COMMAND ----------

# ----------------------------------------------------------
# LECTURA PREDECESORES (CATÁLOGO + RAW)
# ----------------------------------------------------------
try:
    log("Lectura desde UDV/RAW", "INFO", entity_name)

    # 1) Catálogo de plantillas (md_plantillas) vía RutaTabla
    df_md_plantillas = read_udv_table(prm_tabla_md_plantillas)

    # 2) Catálogo de equipos (md_catalogo_equipos) vía RutaTabla
    df_md_catalogo_equipos = read_udv_table(prm_tabla_md_catalogo_equipos)

    # 3) RAW plantillas desde tbl_paths (flg_udv = 'N')

    df_raw_plantillas = get_entity_data(entity_raw,dedup_cols=None)

    if is_dataframe_empty(df_raw_plantillas):
        raise Exception(f"No se encontró data en RAW para la entidad: {entity_raw}")

    log("Predecesores completados correctamente", "INFO", entity_name)

except Exception as e:
    log(f"Error en lectura de predecesores/RAW: {e}", "ERROR", entity_name)
    print(traceback.format_exc())
    raise

log("Finalización de la etapa de lectura", "INFO", entity_name)

# COMMAND ----------

display(df_md_plantillas)
display(df_md_catalogo_equipos)
display(df_raw_plantillas)
