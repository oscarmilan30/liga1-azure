# Databricks notebook source
# ==========================================================
# UDV - PLANTILLAS
# Proyecto: Liga 1 Perú
# Autor: Oscar García Del Águila
# ==========================================================

from utils_liga1 import get_dbutils, get_entity_data, is_dataframe_empty, get_predecesor,get_pipeline_params, get_yaml_from_param, write_delta_udv, log, log_quality, log_wrong_records, read_delta_adls, get_pipeline, get_abfss_path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from hm_plantillas_equipo import carga_final_md_plantillas, carga_final_hm_plantillas, normalizar_foto_url_batch
import traceback
import sys

# ----------------------------------------------------------
# CONFIGURACIÓN INICIAL
# ----------------------------------------------------------

log("Inicio de ejecución del pipeline UDV", "INFO", "hm_plantillas_equipo")

spark = SparkSession.builder.getOrCreate()
# setup_adls()  # reemplazado por Access Connector (Managed Identity acc-liga1)

# COMMAND ----------

# ----------------------------------------------------------
# PARÁMETROS Y PREDECESORES
# ----------------------------------------------------------
try:
    # Para pruebas puedes dejar un valor por defecto; luego ADF lo setea
    dbutils.widgets.text("prm_pipelineid",   "9")
    dbutils.widgets.text("prm_id_ejecucion", "0")
    dbutils.widgets.text("prm_id_ejecucion_e2e", "0")
    dbutils.widgets.text("prm_modo_ejecucion", "HISTORICO")
    prm_pipelineid      = int(dbutils.widgets.get("prm_pipelineid"))
    prm_id_ejecucion    = int(dbutils.widgets.get("prm_id_ejecucion"))
    prm_id_ejecucion_e2e = int(dbutils.widgets.get("prm_id_ejecucion_e2e"))
    prm_modo_ejecucion  = dbutils.widgets.get("prm_modo_ejecucion")

    pipeline_name = get_pipeline(prm_pipelineid)
    entity_name = pipeline_name["pipeline"]
    parent_pipelineid = pipeline_name["parent_pipelineid"]

    pipeline_name_chd = get_pipeline(parent_pipelineid)
    entity_name_chd = pipeline_name_chd["pipeline"]

    dict_params       = get_pipeline_params(prm_pipelineid)
    dict_predecesores = get_predecesor(prm_pipelineid)

    dict_params_chd   = get_pipeline_params(parent_pipelineid)
    dict_predecesores_chd = get_predecesor(parent_pipelineid)

    prm_ruta_md_plantillas = dict_predecesores["md_plantillas"]["Ruta_Predecesor"]
    prm_ruta_md_catalogo_equipos = dict_predecesores["md_catalogo_equipos"]["Ruta_Predecesor"]
    entity_raw = dict_predecesores_chd["plantillas"]["RutaTabla"]

    prm_capa_udv     = dict_params["CAPA_UDV"]
    prm_ruta_base    = dict_params.get("RUTA_BASE", "")
    prm_ruta_tabla   = dict_params["RUTA_TABLA"]
    prm_formato      = dict_params["FORMATO_SALIDA"]
    prm_schema_tb    = dict_params["SCHEMA_TABLA"]
    prm_tabla_output = dict_params["NOMBRE_TABLA"]
    prm_ruta_yaml    = dict_params["YAML_PATH"]

    prm_capa_udv_chd     = dict_params_chd["CAPA_UDV"]
    prm_ruta_base_chd    = dict_params_chd.get("RUTA_BASE", "")
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

    prm_cols_raw_plantillas_md  = yaml_conf[entity_name]["cols_raw_plantillas_md"]
    prm_case_rules              = yaml_conf[entity_name]["case_rules"]
    prm_rename_columns_md       = yaml_conf[entity_name]["rename_columns_md"]
    prm_schema_md               = yaml_conf[entity_name]["schema_md"]
    prm_cols_catalogo_equipos_hm = yaml_conf[entity_name]["cols_catalogo_equipos_hm"]
    prm_cols_raw_plantillas_hm  = yaml_conf[entity_name]["cols_raw_plantillas_hm"]
    prm_dedup_cols_hm           = yaml_conf[entity_name]["dedup_cols_hm"]
    prm_rename_columns_hm       = yaml_conf[entity_name]["rename_columns_hm"]
    prm_schema_hm               = yaml_conf[entity_name]["schema_hm"]
    log("YAML cargado correctamente", "INFO", entity_name)

except Exception as e:
    log(f"Error al leer YAML {prm_ruta_yaml}: {e}", "ERROR", entity_name)
    print(traceback.format_exc())
    raise

# COMMAND ----------

# ----------------------------------------------------------
# LECTURA PREDECESORES (RDV)
# ----------------------------------------------------------
_no_raw_data = False
try:
    log("Lectura desde UDV/RDV", "INFO", entity_name)

    df_raw_plantillas = get_entity_data(entity_raw, dedup_cols=None, modoejecucion=prm_modo_ejecucion)

    if df_raw_plantillas is None or is_dataframe_empty(df_raw_plantillas):
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
# LECTURA PREDECESORES DE REFERENCIA (posiciones y nacionalidades)
# Debe ejecutarse ANTES de carga_final_md_plantillas y carga_final_hm_plantillas
# posiciones    → predecesor de hm_plantillas_equipo (pipeline 9)
# nacionalidades → predecesor de md_plantillas       (pipeline 8, dict_predecesores_chd)
# ----------------------------------------------------------
df_posiciones     = None
df_nacionalidades = None
try:
    if "posiciones" in dict_predecesores:
        path_pos = get_abfss_path(dict_predecesores["posiciones"]["Ruta_Predecesor"])
        df_posiciones = spark.read.parquet(path_pos)
        log(f"Parquet posiciones cargado: {path_pos}", "INFO", entity_name)
    if "nacionalidades" in dict_predecesores_chd:
        path_nac = get_abfss_path(dict_predecesores_chd["nacionalidades"]["Ruta_Predecesor"])
        df_nacionalidades = spark.read.parquet(path_nac)
        log(f"Parquet nacionalidades cargado: {path_nac}", "INFO", entity_name)
except Exception as e:
    log(f"No se pudieron cargar lookup parquets (no crítico): {e}", "WARN", entity_name)
    df_posiciones     = None
    df_nacionalidades = None

log("Finalización de la etapa de lectura de referencia", "INFO", entity_name)

# COMMAND ----------

# ----------------------------------------------------------
# EJECUCIÓN PRINCIPAL — MD PLANTILLAS
# ----------------------------------------------------------
try:
    log("Inicio de Ejecución Principal", "INFO", entity_name_chd)

    df_final_md = carga_final_md_plantillas(
        df_raw_plantillas,
        prm_cols_raw_plantillas_md,
        prm_case_rules,
        prm_rename_columns_md,
        prm_schema_md,
        df_nacionalidades=df_nacionalidades
    )

    # Validar y normalizar foto_url:
    # HEAD request por URL única → si 404 prueba extensión alternativa (.jpg↔.JPG)
    # → si sigue 404 usa imagen default. Si error de red / 403 conserva URL original.
    log("Normalizando foto_url via HEAD requests (URLs únicas)", "INFO", entity_name_chd)
    df_final_md = normalizar_foto_url_batch(spark, df_final_md, "foto_url")
    log("Normalización foto_url completada", "INFO", entity_name_chd)

    log_wrong_records(spark, df_final_md.filter(col("id_jugador").isNull()), entity_name_chd, "UDV", parent_pipelineid, "NULO_CLAVE")

    # MERGE con coalesce: preserva datos existentes si el nuevo batch trae null/vacío
    write_delta_udv(
        spark,
        df_final_md,
        prm_schema_tb_chd,
        prm_tabla_output_chd,
        mode="merge",
        merge_condition="delta.id_jugador = df.id_jugador",
        coalesce_on_match=True,
        coalesce_key_cols=["id_jugador"]
    )

    log("Proceso completado correctamente", "SUCCESS", entity_name_chd)

except Exception as e:
    log(f"Error en ejecución principal: {e}", "ERROR", entity_name_chd)
    print(traceback.format_exc())
    raise

log("Finalización del pipeline UDV", "INFO", entity_name_chd)

# COMMAND ----------

# ----------------------------------------------------------
# LECTURA PREDECESORES (CATÁLOGO UDV)
# ----------------------------------------------------------
try:
    log("Lectura desde UDV/RDV", "INFO", entity_name)

    df_md_plantillas = read_delta_adls(spark, prm_ruta_md_plantillas)
    df_md_catalogo_equipos = read_delta_adls(spark, prm_ruta_md_catalogo_equipos)

    log("Predecesores completados correctamente", "INFO", entity_name)

except Exception as e:
    log(f"Error en lectura de predecesores/RDV: {e}", "ERROR", entity_name)
    print(traceback.format_exc())
    raise

log("Finalización de la etapa de lectura", "INFO", entity_name)

# COMMAND ----------

cnt_entrada = df_raw_plantillas.count()
cnt_salida  = 0
cnt_nulos   = 0
try:
    log("Inicio de Ejecución Principal", "INFO", entity_name)

    df_final_hm, df_discarded_hm = carga_final_hm_plantillas(
        df_md_catalogo_equipos,
        prm_cols_catalogo_equipos_hm,
        df_raw_plantillas,
        prm_cols_raw_plantillas_hm,
        df_md_plantillas,
        prm_case_rules,
        prm_rename_columns_hm,
        prm_schema_hm,
        prm_dedup_cols_hm,
        df_posiciones=df_posiciones
    )

    log_wrong_records(spark, df_discarded_hm, entity_name, "UDV", prm_pipelineid, "SIN_MATCH_CATALOGO")
    cnt_nulos = df_final_hm.filter(col("id_plantilla_equipo").isNull()).count()

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

        cnt_salida = df_final_hm.count()
        log("Escritura HM completada con overwrite dinámico", "SUCCESS", entity_name)

except Exception as e:
    log(f"Error en ejecución principal: {e}", "ERROR", entity_name)
    print(traceback.format_exc())
    raise
finally:
    log_quality(prm_pipelineid, "UDV", f"{prm_schema_tb}.{prm_tabla_output}",
                cnt_entrada, cnt_salida, id_ejecucion=prm_id_ejecucion, id_ejecucion_e2e=prm_id_ejecucion_e2e, registros_nulos_clave=cnt_nulos)

log("Finalización del pipeline UDV", "INFO", entity_name)
