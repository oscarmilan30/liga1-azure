# Databricks notebook source
# ==========================================================
# UNIFICAR 1FL HISTÓRICO
# Proyecto: Liga 1 Perú
# Autor: Oscar García Del Águila
# ==========================================================

from env_setup import *
from pyspark.sql import SparkSession
from utils_liga1 import get_dbutils, get_abfss_path, read_parquet_adls, write_parquet_adls, setup_adls, is_dataframe_empty
from unificar_1FL import procesar_unificacion_1FL

# ----------------------------------------------------------
# PARÁMETROS DESDE ADF
# ----------------------------------------------------------
dbutils.widgets.text("filesystem", "")
dbutils.widgets.text("capa_raw", "")
dbutils.widgets.text("rutaBase", "")
dbutils.widgets.text("nombre_archivo", "")
dbutils.widgets.text("historical_start_year", "")
dbutils.widgets.text("current_year", "")

filesystem = dbutils.widgets.get("filesystem")
capa_raw = dbutils.widgets.get("capa_raw").strip("/")
rutaBase = dbutils.widgets.get("rutaBase").strip("/")
nombre_archivo = dbutils.widgets.get("nombre_archivo")
historical_start_year = int(dbutils.widgets.get("historical_start_year"))
current_year = int(dbutils.widgets.get("current_year"))

# ----------------------------------------------------------
# CONFIGURACIÓN DE ADLS Y SPARK
# ----------------------------------------------------------
spark = SparkSession.builder.getOrCreate()
setup_adls()
dbutils = get_dbutils()

# COMMAND ----------

# ----------------------------------------------------------
# RUTAS BASE
# ----------------------------------------------------------
base_path = f"{capa_raw}/{rutaBase}/{nombre_archivo}"
ruta_abfss_base = get_abfss_path(base_path)
output_path = f"{base_path}/1FL/data"
ruta_abfss_output = get_abfss_path(output_path)

print("===============================================")
print("UNIFICADOR HISTÓRICO DE PARQUETS (1FL)")
print("===============================================")
print(f"Entidad        : {nombre_archivo}")
print(f"Años procesados: {historical_start_year} - {current_year}")
print(f"Ruta base RAW  : {ruta_abfss_base}")
print(f"Salida final   : {ruta_abfss_output}")
print("===============================================")

# ----------------------------------------------------------
# EJECUCIÓN SEGURA
# ----------------------------------------------------------
try:
    # 🔹 La función devuelve un DataFrame consolidado (sin escribir)
    df_final = procesar_unificacion_1FL(
        spark=spark,
        dbutils=dbutils,
        capa_raw=capa_raw,
        rutaBase=rutaBase,
        nombre_archivo=nombre_archivo,
        start_year=historical_start_year,
        end_year=current_year,
        read_parquet_adls=read_parquet_adls,  # funciones inyectadas
        get_abfss_path=get_abfss_path
    )

    # Validar si la unificación resultó vacía
    if is_dataframe_empty(df_final):
        raise Exception(f"No se generó data consolidada para la entidad {nombre_archivo}. Verifica las rutas de origen.")

    # El guardado se hace aquí (nivel notebook)
    write_parquet_adls(df_final, ruta_abfss_output)
    print(f"Consolidado 1FL guardado correctamente en {ruta_abfss_output}")

    # ----------------------------------------------------------
    # LIMPIEZA DE STAGING CURATED
    # ----------------------------------------------------------
    print("Iniciando limpieza de STG/CURATED tras consolidación...")
    for year in range(historical_start_year, current_year):
        ruta_stg_curated = get_abfss_path(f"{base_path}/stg/{year}/curated")
        try:
            dbutils.fs.rm(ruta_stg_curated, recurse=True)
            print(f"Eliminado staging: {ruta_stg_curated}")
        except Exception as e:
            print(f"[ADVERTENCIA] No se pudo eliminar {ruta_stg_curated}: {str(e)}")

    print("Limpieza completada exitosamente.")
    print("===============================================")
    print("PROCESO 1FL FINALIZADO CORRECTAMENTE")
    print("===============================================")

except Exception as e:
    print("Error durante la unificación 1FL:")
    import traceback
    print(traceback.format_exc())
    raise