# Databricks notebook source
# ==========================================================
# UDV - HM_CAMPEONES
# Proyecto: Liga 1 Perú
# Autor: Oscar García Del Águila
# ==========================================================

from pyspark.sql import SparkSession

from utils_liga1 import (
    get_dbutils, generar_udv_json, is_dataframe_empty,
    get_predecesor, get_pipeline_params, get_yaml_from_param,
    write_delta_udv, log, log_quality, log_wrong_records, get_abfss_path, read_delta_adls, get_pipeline,
)

from hm_campeones import carga_final_hm_campeones
import traceback

log("Inicio de ejecución del pipeline UDV", "INFO", "hm_campeones")

spark = SparkSession.builder.getOrCreate()
# setup_adls()  # reemplazado por Access Connector (Managed Identity acc-liga1)

# COMMAND ----------

# ----------------------------------------------------------
# PARÁMETROS Y PREDECESORES
# ----------------------------------------------------------
try:
    dbutils.widgets.text("prm_pipelineid",   "22")
    dbutils.widgets.text("prm_id_ejecucion", "0")
    dbutils.widgets.text("prm_id_ejecucion_e2e", "0")
    prm_pipelineid   = int(dbutils.widgets.get("prm_pipelineid"))
    prm_id_ejecucion = int(dbutils.widgets.get("prm_id_ejecucion"))
    prm_id_ejecucion_e2e = int(dbutils.widgets.get("prm_id_ejecucion_e2e"))
    dbutils.widgets.text("prm_modo_ejecucion", "HISTORICO")
    prm_modo_ejecucion  = dbutils.widgets.get("prm_modo_ejecucion")

    pipeline_info = get_pipeline(prm_pipelineid)
    entity_name   = pipeline_info["pipeline"]

    dict_predecesores = get_predecesor(prm_pipelineid)
    prm_ruta_md_catalogo_equipos  = dict_predecesores["md_catalogo_equipos"]["Ruta_Predecesor"]
    entity_raw_campeones          = dict_predecesores["campeones"]["RutaTabla"]

    dict_params      = get_pipeline_params(prm_pipelineid)
    prm_capa_udv     = dict_params["CAPA_UDV"]
    prm_ruta_base    = dict_params["RUTA_BASE"]
    prm_ruta_tabla   = dict_params["RUTA_TABLA"]
    prm_formato      = dict_params["FORMATO_SALIDA"]
    prm_schema_tb    = dict_params["SCHEMA_TABLA"]
    prm_tabla_output = dict_params["NOMBRE_TABLA"]
    prm_ruta_yaml    = dict_params["YAML_PATH"]

    log("Parámetros cargados correctamente", "INFO", entity_name)
except Exception as e:
    log(f"Error al cargar parámetros o predecesores: {e}", "ERROR", "hm_campeones")
    print(traceback.format_exc())
    raise

# COMMAND ----------

# ----------------------------------------------------------
# LECTURA YAML
# ----------------------------------------------------------
try:
    yaml_conf = get_yaml_from_param(prm_ruta_yaml)

    # Sección hm_campeones dentro del YAML
    prm_campo_json        = yaml_conf[entity_name]["campo_json"]
    prm_cols_raw_campeones_hm = yaml_conf[entity_name]["cols_raw_campeones_hm"]
    prm_schema_hm         = yaml_conf[entity_name]["schema_hm"]


    log("YAML cargado correctamente", "INFO", entity_name)

except Exception as e:
    log(f"Error al leer YAML {prm_ruta_yaml}: {e}", "ERROR", entity_name)
    print(traceback.format_exc())
    raise

# COMMAND ----------


# ----------------------------------------------------------
# LECTURA PREDECESORES (MD_CATALOGO_EQUIPOS + RAW CAMPEONES)
# ----------------------------------------------------------
_no_raw_data = False
try:
    log("Lectura desde UDV/RAW", "INFO", entity_name)

    # 1) MD_CATALOGO_EQUIPOS (ej: 'tb_udv.md_catalogo_equipos')
    df_md_catalogo_equipos = read_delta_adls(spark, prm_ruta_md_catalogo_equipos)
    if is_dataframe_empty(df_md_catalogo_equipos):
        raise Exception(f"No se encontró data en: {prm_ruta_md_catalogo_equipos}")

    # 2) RAW CAMPEONES (JSON FotMob) desde tbl_paths (flg_udv = 'N')
    df_raw_campeones = generar_udv_json(
        entidad=entity_raw_campeones,  # 'campeones'
        campo_json=prm_campo_json,      # normalmente 'data'
        modoejecucion=prm_modo_ejecucion
    )

    if df_raw_campeones is None or is_dataframe_empty(df_raw_campeones):
        log(f"No hay data pendiente en RAW para '{entity_raw_campeones}' (flg_udv=N). Fin sin error.", "WARN", entity_name)
        _no_raw_data = True
    else:
        log("Predecesores completados correctamente", "INFO", entity_name)

except Exception as e:
    log(f"Error en lectura de predecesores/RAW: {e}", "ERROR", entity_name)
    print(traceback.format_exc())
    raise

if _no_raw_data:
    dbutils.notebook.exit("NO_DATA")

log("Finalización de la etapa de lectura", "INFO", entity_name)

# COMMAND ----------

# ----------------------------------------------------------
# EJECUCIÓN PRINCIPAL
# ----------------------------------------------------------
cnt_entrada = df_raw_campeones.count()
cnt_salida  = 0
cnt_nulos   = 0
try:
    log("Inicio de Ejecución Principal", "INFO", entity_name)
    df_final_hm = carga_final_hm_campeones(
        df_md_catalogo_equipos=df_md_catalogo_equipos,
        df_raw_campeones=df_raw_campeones,
        prm_cols_raw_campeones_hm=prm_cols_raw_campeones_hm,
        prm_schema_hm=prm_schema_hm,
    )
    if is_dataframe_empty(df_final_hm):
        log("df_final_hm vacío, no se realizará escritura en UDV", "WARN", entity_name)
    else:
        log("Escribiendo HM_CAMPEONES con overwrite dinámico por partición [periodo]", "INFO", entity_name)
        cnt_salida = df_final_hm.count()
        write_delta_udv(
            spark,
            df_final_hm,
            prm_schema_tb,
            prm_tabla_output,
            mode="overwrite",
            partition_by=["periodo"],
            overwrite_dynamic_partition=True,
        )
        log("Escritura HM_CAMPEONES completada con overwrite dinámico", "SUCCESS", entity_name)
except Exception as e:
    log(f"Error en ejecución principal: {e}", "ERROR", entity_name)
    print(traceback.format_exc())
    raise
finally:
    log_quality(int(prm_pipelineid), "UDV", f"{prm_schema_tb}.{prm_tabla_output}",
                cnt_entrada, cnt_salida, id_ejecucion=prm_id_ejecucion, id_ejecucion_e2e=prm_id_ejecucion_e2e, registros_nulos_clave=cnt_nulos)
log("Finalización del pipeline UDV", "INFO", entity_name)
