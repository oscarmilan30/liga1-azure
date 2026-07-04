# Databricks notebook source
# ==========================================================
# UDV - ESTADIOS
# Proyecto: Liga 1 Perú
# Autor: Oscar García Del Águila
# ==========================================================

from utils_liga1 import get_dbutils, get_entity_data, is_dataframe_empty, get_predecesor,get_pipeline_params, get_yaml_from_param, write_delta_udv, log, log_quality, log_wrong_records, read_delta_adls, get_abfss_path, get_pipeline, extract_entity_name
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from md_estadios import carga_final
import traceback
import sys

log("Inicio de ejecución del pipeline UDV", "INFO", "md_estadios")

spark = SparkSession.builder.getOrCreate()
# setup_adls()  # reemplazado por Access Connector (Managed Identity acc-liga1)

# COMMAND ----------

# ----------------------------------------------------------
# PARÁMETROS Y PREDECESORES
# ----------------------------------------------------------
try:
    dbutils.widgets.text("prm_pipelineid", "7")
    dbutils.widgets.text("prm_id_ejecucion", "0")
    dbutils.widgets.text("prm_id_ejecucion_e2e", "0")
    dbutils.widgets.text("prm_modo_ejecucion", "HISTORICO")
    prm_pipelineid      = int(dbutils.widgets.get("prm_pipelineid"))
    prm_id_ejecucion    = int(dbutils.widgets.get("prm_id_ejecucion"))
    prm_id_ejecucion_e2e = int(dbutils.widgets.get("prm_id_ejecucion_e2e"))
    prm_modo_ejecucion  = dbutils.widgets.get("prm_modo_ejecucion")

    pipeline_name = get_pipeline(prm_pipelineid)
    entity_name = pipeline_name["pipeline"]

    dict_predecesores = get_predecesor(prm_pipelineid)
    dict_params       = get_pipeline_params(prm_pipelineid)

    prm_ruta_pred = dict_predecesores["md_catalogo_equipos"]["Ruta_Predecesor"]

    prm_formato      = dict_params["FORMATO_SALIDA"]
    prm_schema_tb    = dict_params["SCHEMA_TABLA"]
    prm_tabla_output = dict_params["NOMBRE_TABLA"]
    prm_ruta_yaml    = dict_params["YAML_PATH"]

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
# LECTURA PREDECESORES
# ----------------------------------------------------------
_no_raw_data = False
try:
    log("Lectura desde UDV/RDV", "INFO", entity_name)

    df_catalogo_equipos = read_delta_adls(spark, prm_ruta_pred)

    if is_dataframe_empty(df_catalogo_equipos):
        raise Exception(f"No se encontró data en: {prm_ruta_pred}")

    entity_raw = extract_entity_name(entity_name)

    df_raw_estadios = get_entity_data(entity_raw, dedup_cols=prm_dedup_cols, modoejecucion=prm_modo_ejecucion)

    if df_raw_estadios is None or is_dataframe_empty(df_raw_estadios):
        log(f"No hay data pendiente en RAW para '{entity_raw}' (flg_udv=N). Fin sin error.", "WARN", entity_name)
        _no_raw_data = True
    else:
        log("Predecesores completados correctamente", "INFO", entity_name)

except Exception as e:
    log(f"Error en lectura de predecesores/RDV: {e}", "ERROR", entity_name)
    print(traceback.format_exc())
    raise

if _no_raw_data:
    dbutils.notebook.exit("NO_DATA")

log("Finalización de la etapa de lectura", "INFO", entity_name)

# COMMAND ----------

# ----------------------------------------------------------
# EJECUCIÓN PRINCIPAL
# ----------------------------------------------------------
cnt_entrada = df_raw_estadios.count()
cnt_salida  = 0
cnt_nulos   = 0
try:
    log("Inicio de Ejecución Principal", "INFO", entity_name)

    df_final, df_discarded = carga_final(
        df_catalogo_equipos,
        df_raw_estadios,
        prm_cols_catalogo_equipos,
        prm_cols_estadios,
        prm_drop_duplicates_estadios,
        prm_rename_columns,
        prm_schema,
        prm_case_rules
    )

    log_wrong_records(spark, df_discarded, entity_name, "UDV", prm_pipelineid, "SIN_MATCH_CATALOGO")
    cnt_nulos = df_final.filter(col("id_estadio").isNull()).count()
    log_wrong_records(spark, df_final.filter(col("id_estadio").isNull()), entity_name, "UDV", prm_pipelineid, "NULO_CLAVE")

    write_delta_udv(
        spark,
        df_final,
        prm_schema_tb,
        prm_tabla_output,
        mode="merge",
        merge_condition="delta.id_estadio = df.id_estadio",
        coalesce_on_match=True,
        coalesce_key_cols=["id_estadio"]
    )

    cnt_salida = df_final.count()
    log("Proceso completado correctamente", "SUCCESS", entity_name)

except Exception as e:
    log(f"Error en ejecución principal: {e}", "ERROR", entity_name)
    print(traceback.format_exc())
    raise
finally:
    log_quality(int(prm_pipelineid), "UDV", f"{prm_schema_tb}.{prm_tabla_output}", cnt_entrada, cnt_salida, id_ejecucion=prm_id_ejecucion, id_ejecucion_e2e=prm_id_ejecucion_e2e, registros_nulos_clave=cnt_nulos)

log("Finalización del pipeline UDV", "INFO", entity_name)
