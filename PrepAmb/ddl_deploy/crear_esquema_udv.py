# Databricks notebook source
# ==========================================================
# DDL - CREAR SCHEMAS UDV
# Proyecto: Liga 1 Perú
# Autor: Oscar García Del Águila
# ==========================================================

from utils_liga1 import get_dbutils
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
dbutils = get_dbutils()

dbutils.widgets.text("catalog_name", "catalog_liga1")
catalog_name = dbutils.widgets.get("catalog_name")
spark.sql(f"USE CATALOG {catalog_name}")
print(f"Catálogo activo: {catalog_name}")

# Crear schemas si no existen
spark.sql("""
    CREATE SCHEMA IF NOT EXISTS tb_udv
    COMMENT 'Tablas externas (Delta) para capa UDV - Liga 1 Perú';
""")

spark.sql("""
    CREATE SCHEMA IF NOT EXISTS vw_udv
    COMMENT 'Vistas derivadas UDV - Liga 1 Perú';
""")

print("✔ Schemas tb_udv y vw_udv creados (o ya existían).")

print("\n➡ DESCRIBE SCHEMA EXTENDED tb_udv:")
spark.sql(f"DESCRIBE SCHEMA EXTENDED {catalog_name}.tb_udv").show(truncate=False)

print("\n➡ DESCRIBE SCHEMA EXTENDED vw_udv:")
spark.sql(f"DESCRIBE SCHEMA EXTENDED {catalog_name}.vw_udv").show(truncate=False)
