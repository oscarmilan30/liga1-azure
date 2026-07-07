# Databricks notebook source
# ==========================================================
# ML - FT_SCORE_ML
# Proyecto: Liga 1 Perú
# Autor: Oscar García Del Águila
# ==========================================================

from utils_liga1 import (
    is_dataframe_empty, get_predecesor, get_pipeline_params,
    get_yaml_from_param, write_delta_udv, log, log_quality,
    read_delta_adls, get_pipeline
)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from ft_score_ml import calcular_ft_score_ml
import mlflow
import traceback

# Desactivar autolog de MLflow — sklearn lo activa automáticamente en Databricks
# y genera runs innecesarios por cada KMeans/PCA fit
mlflow.autolog(disable=True)

# ----------------------------------------------------------
# CONFIGURACIÓN INICIAL
# ----------------------------------------------------------
entity_name = "ft_score_ml"
log("Inicio de ejecución del pipeline ML", "INFO", entity_name)

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# ----------------------------------------------------------
# PARÁMETROS Y PREDECESORES
# ----------------------------------------------------------
try:
    dbutils.widgets.text("prm_pipelineid",        "45")
    dbutils.widgets.text("prm_id_ejecucion",      "0")
    dbutils.widgets.text("prm_id_ejecucion_e2e",  "0")
    dbutils.widgets.text("prm_periodo",           "0")  # 0 = carga completa, YYYY = incremental

    prm_pipelineid       = int(dbutils.widgets.get("prm_pipelineid"))
    prm_id_ejecucion     = int(dbutils.widgets.get("prm_id_ejecucion"))
    prm_id_ejecucion_e2e = int(dbutils.widgets.get("prm_id_ejecucion_e2e"))
    prm_periodo          = int(dbutils.widgets.get("prm_periodo"))

    pipeline_info     = get_pipeline(prm_pipelineid)
    entity_name       = pipeline_info["pipeline"]

    dict_params       = get_pipeline_params(prm_pipelineid)
    dict_predecesores = get_predecesor(prm_pipelineid)

    # Predecesores
    prm_ruta_ft_estadisticas = dict_predecesores["ft_estadisticas_jugadores"]["Ruta_Predecesor"]
    prm_ruta_ft_plantillas   = dict_predecesores["ft_plantillas_historico"]["Ruta_Predecesor"]

    prm_capa_ddv     = dict_params["CAPA_DDV"]
    prm_ruta_base    = dict_params.get("RUTA_BASE", "")
    prm_formato      = dict_params["FORMATO_SALIDA"]
    prm_schema_tb    = dict_params["SCHEMA_TABLA"]
    prm_tabla_output = dict_params["NOMBRE_TABLA"]
    prm_ruta_yaml    = dict_params["YAML_PATH"]

    log("Parámetros cargados correctamente", "INFO", entity_name)

except Exception as e:
    log(f"Error al cargar parámetros: {e}", "ERROR", entity_name)
    print(traceback.format_exc())
    raise

# COMMAND ----------

# ----------------------------------------------------------
# LECTURA YAML
# ----------------------------------------------------------
try:
    yaml_conf             = get_yaml_from_param(prm_ruta_yaml)
    conf_ml               = yaml_conf[entity_name]
    cols_ft_estadisticas  = conf_ml["cols_ft_estadisticas"]
    cols_ft_plantillas    = conf_ml["cols_ft_plantillas"]
    features_por_posicion = conf_ml["features_por_posicion"]
    ref_signo             = conf_ml["ref_signo"]
    n_clusters            = conf_ml["n_clusters"]
    umbral_minutos        = conf_ml["umbral_minutos"]
    prm_schema            = conf_ml["schema"]
    log("YAML cargado correctamente", "INFO", entity_name)

except Exception as e:
    log(f"Error al leer YAML {prm_ruta_yaml}: {e}", "ERROR", entity_name)
    print(traceback.format_exc())
    raise

# COMMAND ----------

# ----------------------------------------------------------
# LECTURA PREDECESORES
# ML siempre lee TODOS los años para calibrar PCA correctamente.
# Solo se filtra en la escritura si prm_periodo > 0.
# Se seleccionan solo las columnas necesarias antes del toPandas.
# ----------------------------------------------------------
try:
    log("Leyendo ft_estadisticas_jugadores (todos los años para calibrar PCA)", "INFO", entity_name)
    df_stats_sp = read_delta_adls(spark, prm_ruta_ft_estadisticas)
    df_stats_sp = df_stats_sp.filter(col("datos_disponibles") == True)

    log("Leyendo ft_plantillas_historico (pie, altura, posicion)", "INFO", entity_name)
    df_jug_sp = read_delta_adls(spark, prm_ruta_ft_plantillas)

    log("Predecesores cargados correctamente", "INFO", entity_name)

except Exception as e:
    log(f"Error en lectura de predecesores: {e}", "ERROR", entity_name)
    print(traceback.format_exc())
    raise

# COMMAND ----------

# ----------------------------------------------------------
# EJECUCIÓN PRINCIPAL ML
# ----------------------------------------------------------
cnt_entrada = df_stats_sp.count()
cnt_salida  = 0

try:
    # Seleccionar solo columnas necesarias antes de convertir a pandas
    cols_stats_ok = [c for c in cols_ft_estadisticas if c in df_stats_sp.columns]
    cols_plant_ok = [c for c in cols_ft_plantillas   if c in df_jug_sp.columns]

    log("Convirtiendo a pandas (solo columnas ML necesarias)", "INFO", entity_name)
    df_stats_pd = df_stats_sp.select(cols_stats_ok).toPandas()
    df_jug_pd   = df_jug_sp.select(cols_plant_ok).toPandas()

    # ML siempre recalibra PCA + K-means sobre todos los años.
    # Como los scores de años anteriores cambian con cada recalibracion,
    # siempre se sobreescribe la tabla completa — no aplica escritura incremental.
    log("Iniciando PCA + K-means (calibracion sobre todos los años)", "INFO", entity_name)
    df_final = calcular_ft_score_ml(
        spark=spark,
        df_stats=df_stats_pd,
        df_jugadores=df_jug_pd,
        features_por_posicion=features_por_posicion,
        ref_signo=ref_signo,
        n_clusters=n_clusters,
        umbral_minutos=umbral_minutos,
        schema=prm_schema,
    )
    log(f"ML completado: {df_final.count()} jugadores procesados (todos los años)", "INFO", entity_name)

    if is_dataframe_empty(df_final):
        log("df_final vacio, no se realizara escritura en Delta", "WARN", entity_name)
    else:
        log("Escribiendo ft_score_ml completo (full overwrite siempre)", "INFO", entity_name)
        write_delta_udv(
            spark,
            df_final,
            prm_schema_tb,
            prm_tabla_output,
            mode="overwrite",
            partition_by=["temporada"],
            overwrite_dynamic_partition=True
        )

        cnt_salida = df_final.count()
        log("Escritura Delta completada exitosamente", "SUCCESS", entity_name)

except Exception as e:
    log(f"Error en ejecución principal: {e}", "ERROR", entity_name)
    print(traceback.format_exc())
    raise
finally:
    log_quality(
        prm_pipelineid, "ML", f"{prm_schema_tb}.{prm_tabla_output}",
        cnt_entrada, cnt_salida, id_ejecucion=prm_id_ejecucion, id_ejecucion_e2e=prm_id_ejecucion_e2e, registros_nulos_clave=cnt_nulos)

log("Fin ML ft_score_ml", "INFO", entity_name)
