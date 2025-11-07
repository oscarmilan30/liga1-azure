# ==========================================================
# BOOTSTRAP - Configuración de entorno Liga 1 Perú
# ==========================================================
# Autor: Oscar García Del Águila
# Descripción:
#   - Detecta automáticamente la raíz del proyecto
#   - Configura sys.path para imports locales
#   - Distribuye el paquete del proyecto a todos los ejecutores
#     (solo si SparkContext está disponible, evitando errores en modo Connect)
# ==========================================================

import sys
import os
import shutil
import tempfile
from pyspark.sql import SparkSession

try:
    # ------------------------------------------------------
    # 1️⃣ DETECCIÓN DE RUTA RAÍZ DEL PROYECTO
    # ------------------------------------------------------
    notebook_dir = os.getcwd()
    root = os.path.abspath(os.path.join(notebook_dir, ".."))

    # Si no encuentra "util", sube otro nivel
    if not os.path.exists(os.path.join(root, "util")):
        root = os.path.abspath(os.path.join(root, ".."))

    # Evitar duplicados
    if root not in sys.path:
        sys.path.append(root)

    print(f"[BOOTSTRAP] Path raíz agregado: {root}")

    # ------------------------------------------------------
    # 2️⃣ CONFIGURACIÓN DE SPARK Y DISTRIBUCIÓN OPCIONAL
    # ------------------------------------------------------
    spark = SparkSession.builder.getOrCreate()

    # Verificar si se ejecuta en modo Connect
    if hasattr(spark, "sparkContext"):
        try:
            sc = spark.sparkContext
            # Crear ZIP temporal con todo el proyecto
            tmp_zip = tempfile.NamedTemporaryFile(suffix=".zip", delete=False)
            base_name = tmp_zip.name.replace(".zip", "")
            shutil.make_archive(base_name, "zip", root)

            # Distribuir paquete completo a los ejecutores
            sc.addPyFile(tmp_zip.name)

            print(f"[BOOTSTRAP] Paquete distribuido a ejecutores desde: {root}")

        except Exception as e:
            print(f"[BOOTSTRAP WARNING] No se pudo distribuir módulos a ejecutores: {e}")
    else:
        print("[BOOTSTRAP] Modo Spark Connect detectado → se omite distribución a ejecutores.")

except Exception as e:
    print(f"[BOOTSTRAP WARNING] No se pudo configurar el entorno correctamente: {e}")
