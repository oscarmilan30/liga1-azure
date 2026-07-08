# Databricks notebook source
# ==========================================================
# UDV - ENTRENADORES
# Proyecto: Liga 1 Perú
# Autor: Oscar García Del Águila
# ==========================================================
from utils_liga1 import get_dbutils, get_entity_data, is_dataframe_empty, get_predecesor, get_pipeline_params, get_yaml_from_param, write_delta_udv, log, log_quality, log_wrong_records, read_delta_adls, get_pipeline, get_abfss_path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from hm_entrenadores_equipo import carga_final_md_entrenadores, carga_final_hm_entrenadores
import traceback
import sys

log("Inicio de ejecución del pipeline UDV", "INFO", "hm_entrenadores_equipo")
spark = SparkSession.builder.getOrCreate()
# setup_adls()  # reemplazado por Access Connector (Managed Identity acc-liga1)

# COMMAND ----------

try:
    dbutils.widgets.text("prm_pipelineid",   "12")
    dbutils.widgets.text("prm_id_ejecucion", "0")
    dbutils.widgets.text("prm_id_ejecucion_e2e", "0")
    prm_pipelineid   = int(dbutils.widgets.get("prm_pipelineid"))
    prm_id_ejecucion = int(dbutils.widgets.get("prm_id_ejecucion"))
    prm_id_ejecucion_e2e = int(dbutils.widgets.get("prm_id_ejecucion_e2e"))
    dbutils.widgets.text("prm_modo_ejecucion", "HISTORICO")
    prm_modo_ejecucion  = dbutils.widgets.get("prm_modo_ejecucion")
    pipeline_name = get_pipeline(prm_pipelineid)
    entity_name = pipeline_name["pipeline"]
    parent_pipelineid = pipeline_name["parent_pipelineid"]
    pipeline_name_chd = get_pipeline(parent_pipelineid)
    entity_name_chd = pipeline_name_chd["pipeline"]
    dict_params       = get_pipeline_params(prm_pipelineid)
    dict_predecesores = get_predecesor(prm_pipelineid)
    dict_params_chd        = get_pipeline_params(parent_pipelineid)
    dict_predecesores_chd  = get_predecesor(parent_pipelineid)
    prm_ruta_md_entrenadores       = dict_predecesores["md_entrenadores"]["Ruta_Predecesor"]
    prm_ruta_md_catalogo_equipos   = dict_predecesores["md_catalogo_equipos"]["Ruta_Predecesor"]
    entity_raw = dict_predecesores_chd["entrenadores"]["RutaTabla"]
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

try:
    yaml_conf = get_yaml_from_param(prm_ruta_yaml)
    prm_cols_raw_entrenadores_md   = yaml_conf[entity_name]["cols_raw_entrenadores_md"]
    prm_case_rules                 = yaml_conf[entity_name]["case_rules"]
    prm_rename_columns_md          = yaml_conf[entity_name]["rename_columns_md"]
    prm_schema_md                  = yaml_conf[entity_name]["schema_md"]
    prm_cols_catalogo_equipos_hm   = yaml_conf[entity_name]["cols_catalogo_equipos_hm"]
    prm_cols_raw_entrenadores_hm   = yaml_conf[entity_name]["cols_raw_entrenadores_hm"]
    prm_dedup_cols_hm              = yaml_conf[entity_name]["dedup_cols_hm"]
    prm_rename_columns_hm          = yaml_conf[entity_name]["rename_columns_hm"]
    prm_schema_hm                  = yaml_conf[entity_name]["schema_hm"]
    log("YAML cargado correctamente", "INFO", entity_name)
except Exception as e:
    log(f"Error al leer YAML {prm_ruta_yaml}: {e}", "ERROR", entity_name)
    print(traceback.format_exc())
    raise

# COMMAND ----------

_no_raw_data = False
try:
    log("Lectura desde RAW (tbl_paths, flg_udv = 'N')", "INFO", entity_name_chd)
    df_raw_entrenadores = get_entity_data(entity_raw, dedup_cols=None, modoejecucion=prm_modo_ejecucion)
    if df_raw_entrenadores is None or is_dataframe_empty(df_raw_entrenadores):
        log(f"No hay data pendiente en RAW para '{entity_raw}' (flg_udv=N). Fin sin error.", "WARN", entity_name_chd)
        _no_raw_data = True
    else:
        log("Lectura de RAW completada correctamente", "INFO", entity_name_chd)
except Exception as e:
    log(f"Error en lectura de RAW: {e}", "ERROR", entity_name_chd)
    print(traceback.format_exc())
    raise
if _no_raw_data:
    dbutils.notebook.exit("NO_DATA")
log("Finalización de la etapa de lectura RAW", "INFO", entity_name_chd)

# COMMAND ----------

# ----------------------------------------------------------
# LECTURA PREDECESORES DE REFERENCIA (nacionalidades)
# ----------------------------------------------------------
# nacionalidades → predecesor de md_entrenadores (pipeline 11, dict_predecesores_chd)
df_nacionalidades = None
try:
    if "nacionalidades" in dict_predecesores_chd:
        path_nac = get_abfss_path(dict_predecesores_chd["nacionalidades"]["Ruta_Predecesor"])
        df_nacionalidades = spark.read.parquet(path_nac)
        log(f"Parquet nacionalidades cargado: {path_nac}", "INFO", entity_name)
except Exception as e:
    log(f"No se pudo cargar lookup nacionalidades (no crítico): {e}", "WARN", entity_name)
    df_nacionalidades = None

# COMMAND ----------

cnt_entrada_md = df_raw_entrenadores.count()
cnt_salida_md  = 0
cnt_nulos_md   = 0
try:
    log("Inicio de Ejecución Principal - MD_ENTRENADORES", "INFO", entity_name_chd)
    df_final_md = carga_final_md_entrenadores(
        df_raw_entrenadores,
        prm_cols_raw_entrenadores_md,
        prm_case_rules,
        prm_rename_columns_md,
        prm_schema_md,
        df_nacionalidades=df_nacionalidades
    )
    cnt_salida_md = df_final_md.count()
    cnt_nulos_md = df_final_md.filter(col("id_entrenador").isNull()).count()
    log_wrong_records(spark, df_final_md.filter(col("id_entrenador").isNull()), entity_name_chd, "UDV", parent_pipelineid, "NULO_CLAVE")

    write_delta_udv(
        spark, df_final_md, prm_schema_tb_chd, prm_tabla_output_chd,
        mode="merge",
        merge_condition="delta.id_entrenador = df.id_entrenador",
        coalesce_on_match=True,
        coalesce_key_cols=["id_entrenador"]
    )
    log("Proceso MD_ENTRENADORES completado correctamente", "SUCCESS", entity_name_chd)
except Exception as e:
    log(f"Error en ejecución principal MD_ENTRENADORES: {e}", "ERROR", entity_name_chd)
    print(traceback.format_exc())
    raise
finally:
    log_quality(parent_pipelineid, "UDV", f"{prm_schema_tb_chd}.{prm_tabla_output_chd}",
                cnt_entrada_md, cnt_salida_md, id_ejecucion=prm_id_ejecucion, id_ejecucion_e2e=prm_id_ejecucion_e2e, registros_nulos_clave=cnt_nulos_md)
log("Finalización del pipeline UDV - MD_ENTRENADORES", "INFO", entity_name_chd)

# COMMAND ----------

try:
    log("Lectura de predecesores UDV (md_entrenadores, md_catalogo_equipos)", "INFO", entity_name)
    df_md_entrenadores = read_delta_adls(spark, prm_ruta_md_entrenadores)
    df_md_catalogo_equipos = read_delta_adls(spark, prm_ruta_md_catalogo_equipos)
    if df_md_entrenadores is None or is_dataframe_empty(df_md_entrenadores):
        raise Exception("md_entrenadores está vacío o no existe.")
    if df_md_catalogo_equipos is None or is_dataframe_empty(df_md_catalogo_equipos):
        raise Exception("md_catalogo_equipos está vacío o no existe.")
    log("Predecesores UDV leídos correctamente", "INFO", entity_name)
except Exception as e:
    log(f"Error en lectura de predecesores UDV: {e}", "ERROR", entity_name)
    print(traceback.format_exc())
    raise
log("Finalización de la etapa de lectura de predecesores UDV", "INFO", entity_name)

# COMMAND ----------

cnt_salida_hm = 0
cnt_nulos_hm  = 0
try:
    log("Inicio de Ejecución Principal - HM_ENTRENADORES_EQUIPO", "INFO", entity_name)
    df_final_hm, df_discarded_hm = carga_final_hm_entrenadores(df_md_catalogo_equipos, prm_cols_catalogo_equipos_hm, df_raw_entrenadores, prm_cols_raw_entrenadores_hm, df_md_entrenadores, prm_case_rules, prm_rename_columns_hm, prm_schema_hm, prm_dedup_cols_hm)
    log_wrong_records(spark, df_discarded_hm, entity_name, "UDV", prm_pipelineid, "SIN_MATCH_CATALOGO")
    cnt_nulos_hm = df_final_hm.filter(col("id_entrenador").isNull()).count()
    if is_dataframe_empty(df_final_hm):
        log("df_final_hm vacío, no se realizará escritura en UDV", "WARN", entity_name)
    else:
        log("Escribiendo HM con overwrite dinámico por partición [periodo]", "INFO", entity_name)
        cnt_salida_hm = df_final_hm.count()
        write_delta_udv(spark, df_final_hm, prm_schema_tb, prm_tabla_output, mode="overwrite", partition_by=["periodo"], overwrite_dynamic_partition=True)
        log("Escritura HM completada con overwrite dinámico", "SUCCESS", entity_name)
except Exception as e:
    log(f"Error en ejecución principal HM_ENTRENADORES_EQUIPO: {e}", "ERROR", entity_name)
    print(traceback.format_exc())
    raise
finally:
    log_quality(prm_pipelineid, "UDV", f"{prm_schema_tb}.{prm_tabla_output}",
                cnt_entrada_md, cnt_salida_hm, id_ejecucion=prm_id_ejecucion, id_ejecucion_e2e=prm_id_ejecucion_e2e, registros_nulos_clave=cnt_nulos_hm)
log("Finalización del pipeline UDV - HM_ENTRENADORES_EQUIPO", "INFO", entity_name)
