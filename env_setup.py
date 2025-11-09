# ==========================================================
# ENV_SETUP - Configuración dinámica de imports (ajustada)
# Proyecto: Liga 1 Perú
# Autor: Oscar García Del Águila
# ==========================================================
# ✔ Detecta automáticamente todos los módulos .py
# ✔ Ignora notebooks (nb_*.py)
# ✔ Compatible con Databricks y ADF
# ==========================================================

import os
import sys
import importlib
from pyspark.sql import SparkSession

def auto_import_modules(base_dir, exclude_folders=None):
    """
    Detecta todos los módulos .py dentro del proyecto y los registra
    como alias accesibles directamente por nombre.
    Ignora archivos nb_*.py (notebooks).
    """
    exclude_folders = exclude_folders or [".git", "__pycache__", ".ipynb_checkpoints", ".venv"]
    added_aliases = []

    for dirpath, _, filenames in os.walk(base_dir):
        if any(skip in dirpath for skip in exclude_folders):
            continue

        for file in filenames:
            if not file.endswith(".py"):
                continue
            if file == "__init__.py" or file.startswith("nb_"):
                continue  # ignorar notebooks

            module_name = file.replace(".py", "")
            rel_path = os.path.relpath(dirpath, base_dir).replace(os.sep, ".")
            module_path = f"{rel_path}.{module_name}" if rel_path != "." else module_name

            try:
                module = importlib.import_module(module_path)
                sys.modules[module_name] = module  # Alias corto
                added_aliases.append((module_name, module_path))
            except Exception as e:
                print(f"[ENV_SETUP WARNING] No se pudo importar {module_path}: {e}")

    return added_aliases


try:
    # ------------------------------------------------------
    # DETECTAR RUTA RAÍZ DEL PROYECTO
    # ------------------------------------------------------
    notebook_dir = os.getcwd()
    root = os.path.abspath(os.path.join(notebook_dir, ".."))
    if not os.path.exists(os.path.join(root, "util")):
        root = os.path.abspath(os.path.join(root, ".."))

    if root not in sys.path:
        sys.path.append(root)
        print(f"[ENV_SETUP] Path raíz agregado: {root}")

    # ------------------------------------------------------
    # INICIALIZAR SESIÓN SPARK
    # ------------------------------------------------------
    spark = (
        SparkSession.builder
        .config("spark.databricks.connect.enabled", "false")
        .config("spark.databricks.session.share", "false")
        .getOrCreate()
    )
    print("[ENV_SETUP] Sesión Spark inicializada correctamente")

    # ------------------------------------------------------
    # AUTOIMPORTAR TODOS LOS MÓDULOS
    # ------------------------------------------------------
    added = auto_import_modules(root)
    print("[ENV_SETUP] Módulos detectados y alias creados:")
    for name, path in added:
        print(f"  - {name} → {path}")

    print("[ENV_SETUP] Inicialización completa")

except Exception as e:
    print(f"[ENV_SETUP ERROR] Error durante configuración: {e}")
