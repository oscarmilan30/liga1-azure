# Databricks notebook source
# ==========================================================
# BUILD WHEEL — liga1_utils
# Ejecutar UNA VEZ cada vez que se modifica utils_liga1.py
# Construye el .whl y lo sube al Volume de Databricks.
# ==========================================================

# COMMAND ----------

import subprocess, os, shutil
from pyspark.sql import SparkSession

REPO_ROOT   = "/Workspace/Repos/garciadoscar1992@outlook.com/liga1-azure"
CATALOG     = "catalog_liga1"
SCHEMA      = "shared"
VOLUME      = "libs"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

# COMMAND ----------

# Crear schema y volume si no existen
spark = SparkSession.builder.getOrCreate()
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
print(f"OK: schema y volume listos — {CATALOG}.{SCHEMA}.{VOLUME}")

# COMMAND ----------

# Instalar build si no está disponible
subprocess.run(["pip", "install", "build", "--quiet"], check=True)
print("OK: build instalado")

# COMMAND ----------

# Limpiar dist anterior y construir wheel
dist_dir = os.path.join(REPO_ROOT, "dist")
if os.path.exists(dist_dir):
    shutil.rmtree(dist_dir)

result = subprocess.run(
    ["python", "-m", "build", "--wheel", "--no-isolation"],
    cwd=REPO_ROOT,
    capture_output=True,
    text=True
)
print(result.stdout)
if result.returncode != 0:
    raise Exception(f"Build falló:\n{result.stderr}")
print("OK: wheel construido")

# COMMAND ----------

# Copiar wheel al Volume
wheels = [f for f in os.listdir(dist_dir) if f.endswith(".whl")]
if not wheels:
    raise Exception("No se encontró ningún .whl en dist/")

whl_src  = os.path.join(dist_dir, wheels[0])
whl_dest = os.path.join(VOLUME_PATH, wheels[0])

shutil.copy2(whl_src, whl_dest)
print(f"OK: wheel subido a {whl_dest}")
print(f"\nPróximo paso: instalar en el cluster desde:\n  {whl_dest}")
