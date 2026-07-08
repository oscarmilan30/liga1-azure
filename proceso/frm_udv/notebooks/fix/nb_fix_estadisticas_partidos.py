# Databricks notebook source
# ==========================================================
# FIX - HM_ESTADISTICAS_PARTIDOS (histórico 2020-2025)
# Lee JSON desde landing, unifica en Parquet (RAW), transforma y hace UPSERT en UDV
# ==========================================================

from utils_liga1 import (
    get_dbutils, log, read_delta_adls, get_pipeline,
    get_predecesor, get_pipeline_params, get_yaml_from_param,
    write_parquet_adls, write_delta_udv, is_dataframe_empty, get_abfss_path,
    log_quality
)
from hm_estadisticas_partidos_fix import carga_final_hm_estadisticas_partidos_fix
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lit
import traceback

spark = SparkSession.builder.getOrCreate()
# setup_adls()  # reemplazado por Access Connector (Managed Identity acc-liga1)

log("Inicio FIX estadísticas partidos", "INFO", "fix_estadisticas_partidos")

# ----------------------------------------------------------
# PARÁMETROS Y PREDECESORES
# ----------------------------------------------------------
try:
    dbutils.widgets.text("prm_pipelineid", "41")
    prm_pipelineid = int(dbutils.widgets.get("prm_pipelineid"))
    dbutils.widgets.text("prm_id_ejecucion", "0")
    prm_id_ejecucion     = int(dbutils.widgets.get("prm_id_ejecucion"))
    dbutils.widgets.text("prm_id_ejecucion_e2e", "0")
    prm_id_ejecucion_e2e = int(dbutils.widgets.get("prm_id_ejecucion_e2e"))
    dbutils.widgets.text("prm_anio_reproceso", "0")
    _anio_str            = dbutils.widgets.get("prm_anio_reproceso")
    prm_anio_reproceso   = int(_anio_str) if _anio_str and _anio_str.strip().lstrip("-").isdigit() else 0

    pipeline_info = get_pipeline(prm_pipelineid)
    entity_name = pipeline_info["pipeline"]

    dict_predecesores = get_predecesor(prm_pipelineid)
    dict_params = get_pipeline_params(prm_pipelineid)

    prm_filesystem = dict_params["FILESYSTEM"]
    prm_yaml_path = dict_params["YAML_PATH"]
    prm_schema_tb = dict_params["SCHEMA_TABLA"]
    prm_tabla_output = dict_params["NOMBRE_TABLA"]
    prm_landing_path = dict_params["LANDING_JSON_PATH"]
    prm_raw_path = dict_params["RDV_FIX_PATH"]
    prm_ruta_hm_partidos = dict_predecesores["hm_partidos"]["Ruta_Predecesor"]

    log("Parámetros cargados correctamente", "INFO", entity_name)

except Exception as e:
    log(f"Error al cargar metadatos: {e}", "ERROR", entity_name)
    print(traceback.format_exc())
    raise

# ----------------------------------------------------------
# LECTURA YAML (del fix)
# ----------------------------------------------------------
try:
    yaml_conf = get_yaml_from_param(prm_yaml_path)
    entity_yaml = "hm_estadisticas_partidos_fix"
    prm_campo_json = yaml_conf[entity_yaml]["campo_json"]
    prm_cols_raw = yaml_conf[entity_yaml]["cols_raw_estadisticas_hm"]
    prm_rename = yaml_conf[entity_yaml]["rename_columns_hm"]
    prm_schema_hm = yaml_conf[entity_yaml]["schema_hm"]
    prm_fecha_ddMMyyyy = yaml_conf[entity_yaml]["fecha_ddMMyyyy"]
    prm_numeric_no_parse = yaml_conf[entity_yaml]["numeric_no_parse"]
    prm_goles_cfg = yaml_conf[entity_yaml]["goles_from_marcador"]
    prm_stats_with_pct = yaml_conf[entity_yaml]["stats_with_pct"]
    prm_en_fallback_hm = yaml_conf[entity_yaml]["en_fallback_hm"]
    log("YAML del fix cargado correctamente", "INFO", entity_name)
except Exception as e:
    log(f"Error al leer YAML: {e}", "ERROR", entity_name)
    raise

# ----------------------------------------------------------
# 1. INGESTA: JSON → PARQUET UNIFICADO EN RAW
# ----------------------------------------------------------
try:
    log("Leyendo JSON desde landing", "INFO", entity_name)
    landing_full = get_abfss_path(prm_landing_path)
    raw_full = get_abfss_path(prm_raw_path)

    files = [f.path for f in dbutils.fs.ls(landing_full) if f.path.endswith(".json")]
    if prm_anio_reproceso > 0:
        files = [f for f in files if str(prm_anio_reproceso) in f]
        log(f"[REPROCESO] Filtrando JSONs para año {prm_anio_reproceso}: {files}", "INFO", entity_name)
    if not files:
        if prm_anio_reproceso > 0:
            log(f"No hay archivos JSON para año {prm_anio_reproceso} en landing. FIX omitido.", "WARN", entity_name)
            dbutils.notebook.exit("NO_DATA")
        raise Exception(f"No se encontraron archivos JSON en {landing_full}")

    df_landing = None
    for file_path in files:
        df_tmp = spark.read.option("multiline", "true").json(file_path)
        if "data" in df_tmp.columns:
            df_tmp = df_tmp.select(explode("data").alias("_tmp")).select("_tmp.*")
        if df_landing is None:
            df_landing = df_tmp
        else:
            df_landing = df_landing.unionByName(df_tmp, allowMissingColumns=True)

    df_landing.cache()
    row_count = df_landing.count()
    log(f"Total registros unificados: {row_count}", "INFO", entity_name)

    output_parquet = f"{raw_full}/data"
    write_parquet_adls(df_landing, output_parquet, mode="overwrite")
    df_landing.unpersist()
    log(f"Parquet unificado escrito en {output_parquet}", "INFO", entity_name)

except Exception as e:
    log(f"Error en ingesta: {e}", "ERROR", entity_name)
    raise

# ----------------------------------------------------------
# 2. TRANSFORMACIÓN CON LA FUNCIÓN DEL FIX
#    Se pasa df_raw_parquet completo para que la función aplique
#    el fallback inglés→español y el coalesce de fechas.
# ----------------------------------------------------------
try:
    log("Leyendo Parquet desde RAW", "INFO", entity_name)
    df_raw_parquet = spark.read.parquet(output_parquet)

    df_hm_partidos = read_delta_adls(spark, prm_ruta_hm_partidos)
    if is_dataframe_empty(df_hm_partidos):
        raise Exception(f"No se encontró data en {prm_ruta_hm_partidos}")

    df_final = carga_final_hm_estadisticas_partidos_fix(
        df_hm_partidos=df_hm_partidos,
        df_raw_estadisticas=df_raw_parquet,
        prm_cols_raw=prm_cols_raw,
        prm_rename=prm_rename,
        prm_schema=prm_schema_hm,
        prm_fecha_ddMMyyyy=prm_fecha_ddMMyyyy,
        prm_numeric_no_parse=prm_numeric_no_parse,
        prm_goles_cfg=prm_goles_cfg,
        prm_stats_with_pct=prm_stats_with_pct,
        prm_en_fallback_hm=prm_en_fallback_hm
    )

    log(f"DataFrame transformado: {df_final.count()} filas", "INFO", entity_name)

except Exception as e:
    log(f"Error en transformación: {e}", "ERROR", entity_name)
    raise

# ----------------------------------------------------------
# 3. MERGE COALESCE en hm_estadisticas_partidos
# ----------------------------------------------------------
cnt_entrada = row_count
cnt_salida  = 0
try:
    write_delta_udv(
        spark,
        df_final,
        prm_schema_tb,
        prm_tabla_output,
        mode="merge",
        merge_condition="delta.id_partido = df.id_partido",
        coalesce_on_match=True,
        coalesce_key_cols=["id_partido"]
    )

    cnt_salida = df_final.count()
    log("MERGE completado en hm_estadisticas_partidos", "SUCCESS", entity_name)
except Exception as e:
    log(f"Error en MERGE hm_estadisticas_partidos: {e}", "ERROR", entity_name)
    raise
finally:
    log_quality(prm_pipelineid, "FIX", f"{prm_schema_tb}.{prm_tabla_output}", cnt_entrada, cnt_salida, id_ejecucion=prm_id_ejecucion, id_ejecucion_e2e=prm_id_ejecucion_e2e)
