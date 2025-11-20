# Databricks notebook source
# ==========================================================
# UDV - PLANTILLAS
# Proyecto: Liga 1 Perú
# Autor: Oscar García Del Águila
# ==========================================================

from env_setup import *
from utils_liga1 import setup_adls, get_dbutils, get_entity_data, is_dataframe_empty, get_predecesor,get_pipeline_params, get_yaml_from_param, write_delta_udv, log, read_udv_table, get_pipeline
from pyspark.sql import SparkSession
from hm_plantillas_equipo import carga_final_md_plantillas,carga_final_hm_plantillas
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
    entity_name = pipeline_name["pipeline"]          
    parent_pipelineid = pipeline_name["parent_pipelineid"]

    pipeline_name_chd = get_pipeline(parent_pipelineid)
    entity_name_chd = pipeline_name_chd["pipeline"]  

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

    prm_capa_udv_chd     = dict_params_chd["CAPA_UDV"]
    prm_ruta_base_chd    = dict_params_chd["RUTA_BASE"]
    prm_ruta_tabla_chd   = dict_params_chd["RUTA_TABLA"]
    prm_formato_chd      = dict_params_chd["FORMATO_SALIDA"]
    prm_schema_tb_chd    = dict_params_chd["SCHEMA_TABLA"]
    prm_tabla_output_chd = dict_params_chd["NOMBRE_TABLA"]


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

    prm_cols_raw_plantillas_md      = yaml_conf[entity_name]["cols_raw_plantillas_md"]
    prm_case_rules             = yaml_conf[entity_name]["case_rules"]
    prm_rename_columns_md           = yaml_conf[entity_name]["rename_columns_md"]
    prm_schema_md                   = yaml_conf[entity_name]["schema_md"]
    prm_cols_catalogo_equipos_hm      = yaml_conf[entity_name]["cols_catalogo_equipos_hm"]
    prm_cols_raw_plantillas_hm       = yaml_conf[entity_name]["cols_raw_plantillas_hm"]
    prm_dedup_cols_hm      = yaml_conf[entity_name]["dedup_cols_hm"]
    prm_rename_columns_hm      = yaml_conf[entity_name]["rename_columns_hm"]
    prm_schema_hm                    = yaml_conf[entity_name]["schema_hm"]


    log("YAML cargado correctamente", "INFO", entity_name)

except Exception as e:
    log(f"Error al leer YAML {prm_ruta_yaml}: {e}", "ERROR", entity_name)
    print(traceback.format_exc())
    raise

# COMMAND ----------

# ----------------------------------------------------------
# LECTURA PREDECESORES (RAW)
# ----------------------------------------------------------
try:
    log("Lectura desde UDV/RAW", "INFO", entity_name)

    # 1) RAW plantillas desde tbl_paths (flg_udv = 'N')

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

# ----------------------------------------------------------
# EJECUCIÓN PRINCIPAL
# ----------------------------------------------------------
try:
    log("Inicio de Ejecución Principal", "INFO", entity_name_chd)

    df_final_md = carga_final_md_plantillas(
        df_raw_plantillas,
        prm_cols_raw_plantillas_md,
        prm_case_rules,
        prm_rename_columns_md,
        prm_schema_md
            )

    # MERGE simple (UPSERT) sobre id_equipo
    write_delta_udv(
        spark,
        df_final_md,
        prm_schema_tb_chd,
        prm_tabla_output_chd,
        mode="merge",
        merge_condition="delta.id_jugador = df.id_jugador"
    )

    log("Proceso completado correctamente", "SUCCESS", entity_name_chd)

except Exception as e:
    log(f"Error en ejecución principal: {e}", "ERROR", entity_name_chd)
    print(traceback.format_exc())
    raise

log("Finalización del pipeline UDV", "INFO", entity_name_chd)

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

    log("Predecesores completados correctamente", "INFO", entity_name)

except Exception as e:
    log(f"Error en lectura de predecesores/RAW: {e}", "ERROR", entity_name)
    print(traceback.format_exc())
    raise

log("Finalización de la etapa de lectura", "INFO", entity_name)

# COMMAND ----------

try:
    log("Inicio de Ejecución Principal", "INFO", entity_name)

    df_final_hm = carga_final_hm_plantillas(
        df_md_catalogo_equipos,
        prm_cols_catalogo_equipos_hm,
        df_raw_plantillas,
        prm_cols_raw_plantillas_hm,
        df_md_plantillas,
        prm_case_rules,
        prm_rename_columns_hm,
        prm_schema_hm,
        prm_dedup_cols_hm
    )

    if is_dataframe_empty(df_final_hm):
        log("df_final_hm vacío, no se realizará escritura en UDV", "WARN", entity_name)
    else:
        log("Escribiendo HM con overwrite dinámico por partición [periodo]", "INFO", entity_name)

        write_delta_udv(
            spark,
            df_final_hm,
            prm_schema_tb,        
            prm_tabla_output,   
            mode="overwrite",
            partition_by=["periodo"],
            overwrite_dynamic_partition=True
        )

        log("Escritura HM completada con overwrite dinámico", "SUCCESS", entity_name)

except Exception as e:
    log(f"Error en ejecución principal: {e}", "ERROR", entity_name)
    print(traceback.format_exc())
    raise

log("Finalización del pipeline UDV", "INFO", entity_name)

