# Databricks notebook source
# ==========================================================
# FIX - MD_PLANTILLAS + HM_PLANTILLAS_EQUIPO (histórico 2020-2025)
# Fase 1: Lee CSVs desde landing → MERGE en md_plantillas (pie, altura, valor_mercado)
# Fase 2: Re-ejecuta hm_plantillas_equipo leyendo Delta directo (sin filtro flg_udv)
# Config propia en fix_plantillas.yml (secciones fix_plantillas y fix_hm)
# ==========================================================

from utils_liga1 import (
    get_dbutils, log, get_pipeline,
    get_pipeline_params, get_predecesor, get_yaml_from_param,
    write_parquet_adls, write_delta_udv, get_abfss_path,
    read_delta_adls, read_parquet_adls, is_dataframe_empty,
    log_wrong_records, log_quality
)
from fix_plantillas import carga_fix_md_plantillas, carga_fix_hm_plantillas
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, expr, row_number, lit
from delta.tables import DeltaTable
import traceback

spark = SparkSession.builder.getOrCreate()
# setup_adls()  # reemplazado por Access Connector (Managed Identity acc-liga1)

log("Inicio FIX plantillas (md + hm)", "INFO", "fix_plantillas")

# ----------------------------------------------------------
# PARÁMETROS PipelineId=36 (fix_plantillas)
# ----------------------------------------------------------
try:
    dbutils.widgets.text("prm_pipelineid", "36")
    prm_pipelineid = int(dbutils.widgets.get("prm_pipelineid"))
    dbutils.widgets.text("prm_id_ejecucion", "0")
    prm_id_ejecucion     = int(dbutils.widgets.get("prm_id_ejecucion"))
    dbutils.widgets.text("prm_id_ejecucion_e2e", "0")
    prm_id_ejecucion_e2e = int(dbutils.widgets.get("prm_id_ejecucion_e2e"))
    dbutils.widgets.text("prm_anio_reproceso", "0")
    _anio_str            = dbutils.widgets.get("prm_anio_reproceso")
    prm_anio_reproceso   = int(_anio_str) if _anio_str and _anio_str.strip().lstrip("-").isdigit() else 0

    pipeline_info    = get_pipeline(prm_pipelineid)
    entity_name      = pipeline_info["pipeline"]

    dict_params      = get_pipeline_params(prm_pipelineid)
    prm_yaml_path    = dict_params["YAML_PATH"]
    prm_landing_path = dict_params["LANDING_CSV_PATH"]
    prm_raw_path     = dict_params["RDV_FIX_PATH"]

    log("Parámetros fix cargados", "INFO", entity_name)
except Exception as e:
    log(f"Error al cargar parámetros fix: {e}", "ERROR", "fix_plantillas")
    print(traceback.format_exc())
    raise

# ----------------------------------------------------------
# RUTAS DE TABLAS DELTA (via predecesores de hm_plantillas_equipo)
# ----------------------------------------------------------
try:
    prm_pipelineid_hm     = 9
    pipeline_info_hm      = get_pipeline(prm_pipelineid_hm)

    dict_params_hm        = get_pipeline_params(prm_pipelineid_hm)
    prm_schema_tb_hm      = dict_params_hm["SCHEMA_TABLA"]
    prm_tabla_output_hm   = dict_params_hm["NOMBRE_TABLA"]

    dict_predecesores_hm  = get_predecesor(prm_pipelineid_hm)

    prm_ruta_md_plantillas        = dict_predecesores_hm["md_plantillas"]["Ruta_Predecesor"]
    prm_ruta_md_catalogo_equipos  = dict_predecesores_hm["md_catalogo_equipos"]["Ruta_Predecesor"]

    log("Rutas de predecesores cargadas", "INFO", entity_name)
except Exception as e:
    log(f"Error al cargar rutas de tablas: {e}", "ERROR", entity_name)
    print(traceback.format_exc())
    raise

# ----------------------------------------------------------
# LECTURA YAML fix_plantillas (self-contained: secciones fix_plantillas + fix_hm)
# ----------------------------------------------------------
try:
    yaml_fix = get_yaml_from_param(prm_yaml_path)

    # Sección fix_plantillas → config para Fase 1 (md_plantillas)
    prm_cols_raw       = yaml_fix["fix_plantillas"]["cols_raw_csv"]
    prm_case_rules_fix = yaml_fix["fix_plantillas"]["case_rules"]
    prm_schema_fix     = yaml_fix["fix_plantillas"]["schema_fix"]

    # Sección fix_hm → config para Fase 2 (hm_plantillas_equipo)
    prm_cols_catalogo_hm       = yaml_fix["fix_hm"]["cols_catalogo_equipos_hm"]
    prm_cols_raw_plantillas_hm = yaml_fix["fix_hm"]["cols_raw_plantillas_hm"]
    prm_dedup_cols_hm          = yaml_fix["fix_hm"]["dedup_cols_hm"]
    prm_rename_columns_hm      = yaml_fix["fix_hm"]["rename_columns_hm"]
    prm_case_rules_hm          = yaml_fix["fix_hm"]["case_rules"]
    prm_schema_hm              = yaml_fix["fix_hm"]["schema_hm"]

    log("YAML fix_plantillas cargado (secciones fix_plantillas + fix_hm)", "INFO", entity_name)
except Exception as e:
    log(f"Error al leer YAML fix: {e}", "ERROR", entity_name)
    raise

# ==========================================================
# FASE 1: MERGE en md_plantillas
# ==========================================================

# ----------------------------------------------------------
# 1a. INGESTA: CSVs → PARQUET UNIFICADO EN RAW
# ----------------------------------------------------------
try:
    log("[FASE 1] Leyendo CSVs desde landing", "INFO", entity_name)
    landing_full = get_abfss_path(prm_landing_path)
    raw_full     = get_abfss_path(prm_raw_path)

    files = [f.path for f in dbutils.fs.ls(landing_full) if f.path.endswith(".csv")]
    if prm_anio_reproceso > 0:
        files = [f for f in files if str(prm_anio_reproceso) in f]
        log(f"[REPROCESO] Filtrando CSVs para año {prm_anio_reproceso}: {files}", "INFO", entity_name)
    if not files:
        if prm_anio_reproceso > 0:
            log(f"No hay archivos CSV para año {prm_anio_reproceso} en landing. FIX omitido.", "WARN", entity_name)
            dbutils.notebook.exit("NO_DATA")
        raise Exception(f"No se encontraron archivos CSV en {landing_full}")

    df_landing = None
    for file_path in files:
        df_tmp = (
            spark.read
            .option("header", "true")
            .option("sep", "|")
            .option("encoding", "UTF-8")
            .csv(file_path)
        )
        df_landing = df_tmp if df_landing is None else df_landing.unionByName(df_tmp, allowMissingColumns=True)

    df_landing.cache()
    log(f"Total registros unificados: {df_landing.count()}", "INFO", entity_name)

    output_parquet = f"{raw_full}/data"
    write_parquet_adls(df_landing, output_parquet, mode="overwrite")
    df_landing.unpersist()
    log(f"Parquet escrito en {output_parquet}", "INFO", entity_name)
except Exception as e:
    log(f"Error en ingesta: {e}", "ERROR", entity_name)
    raise

# ----------------------------------------------------------
# 1b. TRANSFORMACIÓN y MERGE en md_plantillas
# ----------------------------------------------------------
cnt_entrada_md = 0
cnt_salida_md  = 0
try:
    log("[FASE 1] Transformando y aplicando MERGE en md_plantillas", "INFO", entity_name)
    df_raw_parquet = spark.read.parquet(output_parquet)

    df_fix = carga_fix_md_plantillas(
        df_raw_csv     = df_raw_parquet,
        prm_cols_raw   = prm_cols_raw,
        prm_case_rules = prm_case_rules_fix,
        prm_schema_fix = prm_schema_fix
    )
    cnt_entrada_md = df_fix.count()
    log(f"Registros a actualizar en md_plantillas: {cnt_entrada_md}", "INFO", entity_name)

    target_path = get_abfss_path(prm_ruta_md_plantillas)
    if not DeltaTable.isDeltaTable(spark, target_path):
        raise Exception(f"md_plantillas no existe en {target_path}. Ejecuta primero el pipeline UDV.")

    delta = DeltaTable.forPath(spark, target_path)
    columnas_fix = [c for c in df_fix.columns if c != "jugador"]
    update_dict  = {c: expr(f"COALESCE(source.`{c}`, target.`{c}`)") for c in columnas_fix}

    delta.alias("target") \
        .merge(df_fix.alias("source"), "lower(trim(target.jugador)) = source.jugador") \
        .whenMatchedUpdate(set=update_dict) \
        .execute()

    cnt_salida_md = cnt_entrada_md
    log("MERGE completado en md_plantillas", "SUCCESS", entity_name)
except Exception as e:
    log(f"Error en MERGE md_plantillas: {e}", "ERROR", entity_name)
    raise
finally:
    log_quality(prm_pipelineid, "FIX", "md_plantillas", cnt_entrada_md, cnt_salida_md, id_ejecucion=prm_id_ejecucion, id_ejecucion_e2e=prm_id_ejecucion_e2e)

# ==========================================================
# FASE 2: RE-EJECUTAR hm_plantillas_equipo
# Lee Delta directo (sin filtro flg_udv) para propagar correcciones
# Config en fix_plantillas.yml sección fix_hm (posicion, numero_camiseta, fecha_fichaje)
# ==========================================================
try:
    log("[FASE 2] Leyendo tablas Delta para re-ejecutar hm_plantillas_equipo", "INFO", entity_name)

    df_md_catalogo       = read_delta_adls(spark, prm_ruta_md_catalogo_equipos)
    df_md_plantillas_upd = read_delta_adls(spark, prm_ruta_md_plantillas)
    df_raw_plantillas    = read_parquet_adls(spark, f"{raw_full}/data")

    if is_dataframe_empty(df_md_catalogo):
        raise Exception(f"md_catalogo_equipos vacío: {prm_ruta_md_catalogo_equipos}")
    if is_dataframe_empty(df_md_plantillas_upd):
        raise Exception(f"md_plantillas vacío: {prm_ruta_md_plantillas}")
    if is_dataframe_empty(df_raw_plantillas):
        raise Exception(f"raw plantillas vacío: {raw_full}/data")

    log("Tablas Delta leídas correctamente", "INFO", entity_name)
except Exception as e:
    log(f"Error en lectura Delta: {e}", "ERROR", entity_name)
    raise

cnt_entrada_hm = 0
cnt_salida_hm  = 0
try:
    log("[FASE 2] Ejecutando carga hm_plantillas_equipo", "INFO", entity_name)

    df_hm = carga_fix_hm_plantillas(
        df_md_catalogo_equipos       = df_md_catalogo,
        prm_cols_catalogo_equipos_hm = prm_cols_catalogo_hm,
        df_raw_plantillas            = df_raw_plantillas,
        prm_cols_raw_plantillas_hm   = prm_cols_raw_plantillas_hm,
        df_md_plantillas             = df_md_plantillas_upd,
        prm_case_rules               = prm_case_rules_hm,
        prm_rename_columns_hm        = prm_rename_columns_hm,
        prm_schema_hm                = prm_schema_hm,
        prm_dedup_cols_hm            = prm_dedup_cols_hm
    )

    cnt_entrada_hm = df_hm.count()
    log(f"Registros hm_plantillas_equipo generados: {cnt_entrada_hm}", "INFO", entity_name)

    # Dedup por jugador+equipo+temporada — cuando dos fuentes traen distinta
    # posición para el mismo jugador, queda un solo registro
    w = Window.partitionBy("id_equipo", "id_jugador", "temporada").orderBy(lit(1))
    df_hm_ranked  = df_hm.withColumn("_rn", row_number().over(w))
    df_hm_clean   = df_hm_ranked.filter(col("_rn") == 1).drop("_rn")
    df_duplicados = df_hm_ranked.filter(col("_rn") > 1).drop("_rn")

    log_wrong_records(spark, df_duplicados, entity_name, "FIX", prm_pipelineid, "DUPLICADO_CLAVE")

    cnt_salida_hm = df_hm_clean.count()
    log(f"Registros tras dedup: {cnt_salida_hm} (rechazados: {cnt_entrada_hm - cnt_salida_hm})", "INFO", entity_name)

    write_delta_udv(
        spark,
        df_hm_clean,
        prm_schema_tb_hm,
        prm_tabla_output_hm,
        mode="merge",
        merge_condition="delta.id_equipo = df.id_equipo AND delta.id_jugador = df.id_jugador AND delta.temporada = df.temporada",
        coalesce_on_match=True,
        coalesce_key_cols=["id_equipo", "id_jugador", "temporada"]
    )

    log("MERGE completado en hm_plantillas_equipo", "SUCCESS", entity_name)
except Exception as e:
    log(f"Error en hm_plantillas_equipo: {e}", "ERROR", entity_name)
    raise
finally:
    log_quality(prm_pipelineid, "FIX", f"{prm_schema_tb_hm}.{prm_tabla_output_hm}", cnt_entrada_hm, cnt_salida_hm, id_ejecucion=prm_id_ejecucion, id_ejecucion_e2e=prm_id_ejecucion_e2e, registros_duplicados=cnt_entrada_hm - cnt_salida_hm)
