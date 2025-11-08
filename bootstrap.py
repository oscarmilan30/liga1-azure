# ==========================================================
# BOOTSTRAP - Configuración básica Liga 1 Perú
# ==========================================================
# Compatible con ejecución desde ADF y Databricks Cluster
# (sin necesidad de Spark Connect ni empaquetado de módulos)
# ==========================================================

import sys
import os
from pyspark.sql import SparkSession

try:
    # Detectar raíz del proyecto
    notebook_dir = os.getcwd()
    root = os.path.abspath(os.path.join(notebook_dir, ".."))
    if not os.path.exists(os.path.join(root, "util")):
        root = os.path.abspath(os.path.join(root, ".."))

    if root not in sys.path:
        sys.path.append(root)
    print(f"[BOOTSTRAP] Path raíz agregado: {root}")

    # Crear sesión Spark en modo CLUSTER clásico
    spark = (
        SparkSession.builder
        .config("spark.databricks.connect.enabled", "false")  # Desactiva Spark Connect
        .config("spark.databricks.session.share", "false")
        .getOrCreate()
    )

    print("[BOOTSTRAP] Sesión Spark inicializada en modo Cluster (ADF compatible)")

except Exception as e:
    print(f"[BOOTSTRAP WARNING] No se pudo configurar el entorno correctamente: {e}")
