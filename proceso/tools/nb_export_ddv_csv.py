# Databricks notebook source
# ==========================================================
# EXPORT DDV → CSV
# Lee todas las tablas DDV desde Unity Catalog y las exporta
# como CSV a ADLS para análisis externo (Power BI, etc.)
# ==========================================================

# COMMAND ----------

from pyspark.sql import SparkSession
from utils_liga1 import get_abfss_path, log

spark = SparkSession.builder.getOrCreate()
# setup_adls()  # reemplazado por Access Connector (Managed Identity acc-liga1)

# COMMAND ----------

dbutils.widgets.text("prm_output_path", "primera_division/temp/ddv_export")
prm_output_path = dbutils.widgets.get("prm_output_path")

output_base = get_abfss_path(prm_output_path)
log(f"Exportando tablas DDV a: {output_base}", "INFO", "export_ddv_csv")

# COMMAND ----------

TABLAS_DDV = [
    "tb_ddv.dm_equipos",
    "tb_ddv.ft_rendimiento_temporada",
    "tb_ddv.ft_rendimiento_acumulado",
    "tb_ddv.ft_plantillas_historico",
    "tb_ddv.ft_entrenadores_historico",
    "tb_ddv.ft_evolucion_valoracion",
    "tb_ddv.ft_partidos_detalle",
    "tb_ddv.ft_estadisticas_jugador"
]

# COMMAND ----------

for tabla in TABLAS_DDV:
    nombre = tabla.split(".")[1]
    tmp_path  = f"{output_base}/_tmp_{nombre}"
    final_csv = f"{output_base}/{nombre}.csv"
    try:
        df = spark.table(tabla)
        filas = df.count()

        # Escribir a carpeta temporal (genera part-*.csv)
        (
            df.coalesce(1)
            .write
            .mode("overwrite")
            .option("header", "true")
            .option("encoding", "UTF-8")
            .csv(tmp_path)
        )

        # Encontrar el part file y renombrarlo a nombre_tabla.csv
        part_file = [f.path for f in dbutils.fs.ls(tmp_path) if f.name.startswith("part-")][0]
        dbutils.fs.mv(part_file, final_csv)
        dbutils.fs.rm(tmp_path, recurse=True)

        log(f"[OK] {tabla} → {filas} filas → {nombre}.csv", "INFO", "export_ddv_csv")
    except Exception as e:
        log(f"[ERROR] {tabla}: {e}", "ERROR", "export_ddv_csv")

log("Export completado", "SUCCESS", "export_ddv_csv")
