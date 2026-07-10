# Databricks notebook source
# ==========================================================
# UDV - HM_TABLAS_CLASIFICACION
# Proyecto: Liga 1 Perú
# Autor: Oscar García Del Águila
# ==========================================================
from utils_liga1 import (get_dbutils, get_predecesor, get_pipeline_params, get_yaml_from_param, get_abfss_path, read_delta_adls, get_pipeline, write_delta_udv, log, log_quality, log_wrong_records, is_dataframe_empty, get_entity_data)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from hm_tablas_clasificacion import carga_final_hm_tablas_clasificacion
import traceback

log("Inicio pipeline HM_TABLAS_CLASIFICACION", "INFO", "hm_tablas_clasificacion")
spark = SparkSession.builder.getOrCreate()
# setup_adls()  # reemplazado por Access Connector (Managed Identity acc-liga1)

# COMMAND ----------

# ----------------------------------------------------------
# PARÁMETROS Y PREDECESORES
# ----------------------------------------------------------
try:
    # Para pruebas en notebook; en ADF se setea dinámicamente
    dbutils.widgets.text("prm_pipelineid", "20")
    prm_pipelineid = int(dbutils.widgets.get("prm_pipelineid"))
    dbutils.widgets.text("prm_id_ejecucion", "0")
    dbutils.widgets.text("prm_id_ejecucion_e2e", "0")
    dbutils.widgets.text("prm_modo_ejecucion", "HISTORICO")
    prm_id_ejecucion     = int(dbutils.widgets.get("prm_id_ejecucion"))
    prm_id_ejecucion_e2e = int(dbutils.widgets.get("prm_id_ejecucion_e2e"))
    prm_modo_ejecucion   = dbutils.widgets.get("prm_modo_ejecucion")

    pipeline_info    = get_pipeline(prm_pipelineid)
    entity_name      = pipeline_info["pipeline"]          # 'hm_tablas_clasificacion'

    dict_predecesores = get_predecesor(prm_pipelineid)
    dict_params       = get_pipeline_params(prm_pipelineid)

    # Predecesores:
    #  - md_catalogo_equipos     -> RutaTabla = 'tb_udv.md_catalogo_equipos'
    #  - tablas_clasificacion    -> RutaTabla = 'tablas_clasificacion' (RAW)
    prm_ruta_md_catalogo_equipos = dict_predecesores["md_catalogo_equipos"]["Ruta_Predecesor"]
    entity_raw_clasificacion     = dict_predecesores["tablas_clasificacion"]["RutaTabla"]

    # Parámetros UDV
    prm_capa_udv     = dict_params["CAPA_UDV"]
    prm_ruta_base    = dict_params["RUTA_BASE"]
    prm_ruta_tabla   = dict_params["RUTA_TABLA"]
    prm_formato      = dict_params["FORMATO_SALIDA"]
    prm_schema_tb    = dict_params["SCHEMA_TABLA"]
    prm_tabla_output = dict_params["NOMBRE_TABLA"]
    prm_ruta_yaml    = dict_params["YAML_PATH"]

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

    prm_campo_json                 = yaml_conf[entity_name]["campo_json"]
    prm_cols_raw_clasificacion_hm  = yaml_conf[entity_name]["cols_raw_clasificacion_hm"]
    prm_cols_json_item_hm          = yaml_conf[entity_name]["cols_json_item_hm"]
    prm_rename_columns_hm          = yaml_conf[entity_name]["rename_columns_hm"]
    prm_schema_hm                  = yaml_conf[entity_name]["schema_hm"]

    log("YAML cargado correctamente", "INFO", entity_name)

except Exception as e:
    log(f"Error al leer YAML {prm_ruta_yaml}: {e}", "ERROR", entity_name)
    print(traceback.format_exc())
    raise

# COMMAND ----------

# ----------------------------------------------------------
# LECTURA PREDECESORES (MD_CATALOGO_EQUIPOS + RAW JSON)
# ----------------------------------------------------------
_no_raw_data = False
try:
    log("Lectura de predecesores", "INFO", entity_name)

    df_md_catalogo_equipos = read_delta_adls(spark, prm_ruta_md_catalogo_equipos)
    if is_dataframe_empty(df_md_catalogo_equipos):
        raise Exception(f"No se encontró data en: {prm_ruta_md_catalogo_equipos}")

    df_raw = get_entity_data(entity_raw_clasificacion, modoejecucion=prm_modo_ejecucion)
    if df_raw is None or is_dataframe_empty(df_raw):
        log(f"No hay data pendiente en RAW para '{entity_raw_clasificacion}' (flg_udv=N). Fin sin error.", "WARN", entity_name)
        _no_raw_data = True
    else:
        log("Predecesores completados correctamente", "INFO", entity_name)

except Exception as e:
    log(f"Error en lectura de predecesores/RAW: {e}", "ERROR", entity_name)
    print(traceback.format_exc())
    raise

if _no_raw_data:
    dbutils.notebook.exit(f"NO_DATA: Sin registros pendientes (flg_udv=S) para entidad '{entity_name}'")

# COMMAND ----------

# ----------------------------------------------------------
# EJECUCIÓN PRINCIPAL
# ----------------------------------------------------------
cnt_entrada = df_raw.count()
cnt_salida  = 0
cnt_nulos   = 0
try:
    log("Inicio de Ejecución Principal", "INFO", entity_name)

    df_final_hm, df_discarded_hm = carga_final_hm_tablas_clasificacion(
    df_raw=df_raw,
    df_md_catalogo_equipos=df_md_catalogo_equipos,
    prm_cols_raw_clasificacion_hm=prm_cols_raw_clasificacion_hm,
    prm_cols_json_item_hm=prm_cols_json_item_hm,
    prm_rename_columns_hm=prm_rename_columns_hm,
    prm_schema_hm=prm_schema_hm,
    campo_json=prm_campo_json
    )

    log_wrong_records(spark, df_discarded_hm, entity_name, "UDV", prm_pipelineid, "SIN_MATCH_CATALOGO")
    cnt_nulos = df_final_hm.filter(col("id_equipo").isNull()).count()

    if is_dataframe_empty(df_final_hm):
        log("df_final_hm vacío, no se realizará escritura en UDV", "WARN", entity_name)
    else:
        log("Escribiendo HM_TABLAS_CLASIFICACION con overwrite dinámico por partición [periodo]", "INFO", entity_name)

        write_delta_udv(
            spark,
            df_final_hm,
            prm_schema_tb,          # ej: 'tb_udv'
            prm_tabla_output,       # ej: 'hm_tablas_clasificacion'
            mode="overwrite",
            partition_by=["periodo"],
            overwrite_dynamic_partition=True
        )

        cnt_salida = df_final_hm.count()
        log("Escritura HM_TABLAS_CLASIFICACION completada", "SUCCESS", entity_name)


except Exception as e:
    log(f"Error en ejecución principal: {e}", "ERROR", entity_name)
    print(traceback.format_exc())
    raise
finally:
    log_quality(prm_pipelineid, "UDV", f"{prm_schema_tb}.{prm_tabla_output}",
                cnt_entrada, cnt_salida, id_ejecucion=prm_id_ejecucion, id_ejecucion_e2e=prm_id_ejecucion_e2e, registros_nulos_clave=cnt_nulos)

log("Finalización del pipeline UDV", "INFO", entity_name)
