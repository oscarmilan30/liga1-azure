# ==========================================================
# BOOTSTRAP - Configuración básica Liga 1 Perú
# ==========================================================
# Compatible con Spark Connect y clusters ADF (sin UDFs)
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

    # Inicializar sesión Spark
    spark = SparkSession.builder.getOrCreate()

    # Confirmar entorno
    if hasattr(spark, "sparkContext"):
        print("[BOOTSTRAP] Modo Cluster / ADF detectado")
    else:
        print("[BOOTSTRAP] Modo Spark Connect detectado")

except Exception as e:
    print(f"[BOOTSTRAP WARNING] No se pudo configurar el entorno correctamente: {e}")
