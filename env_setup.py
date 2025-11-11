# ==========================================================
# ENV_SETUP - Configuración dinámica de imports
# Proyecto: Liga 1 Perú
# Autor: Oscar García Del Águila
# ==========================================================
# ✔ Detecta la raíz del proyecto automáticamente
# ✔ Importa módulos .py de todas las carpetas
# ✔ Ignora notebooks (nb_*.py)
# ✔ Crea alias automáticos (utils_liga1, curated_json, etc.)
# ✔ Compatible con Databricks y ADF
# ✔ Incluye helper get_workspace_path() para rutas relativas
# ==========================================================

import os
import sys
import importlib
from pyspark.sql import SparkSession

# ----------------------------------------------------------
# AUTOIMPORTADOR DE MÓDULOS
# ----------------------------------------------------------
def auto_import_modules(base_dir, exclude_folders=None):
    exclude_folders = exclude_folders or [".git", "__pycache__", ".ipynb_checkpoints", ".venv"]
    added_aliases = []
    for dirpath, _, filenames in os.walk(base_dir):
        if any(skip in dirpath for skip in exclude_folders):
            continue
        for file in filenames:
            if not file.endswith(".py"):
                continue
            if file == "__init__.py" or file.startswith("nb_"):
                continue
            module_name = file.replace(".py", "")
            rel_path = os.path.relpath(dirpath, base_dir).replace(os.sep, ".")
            module_path = f"{rel_path}.{module_name}" if rel_path != "." else module_name
            try:
                module = importlib.import_module(module_path)
                sys.modules[module_name] = module
                added_aliases.append((module_name, module_path))
            except Exception as e:
                print(f"[ENV_SETUP WARNING] No se pudo importar {module_path}: {e}")
    return added_aliases


# ----------------------------------------------------------
# RESOLVER RUTA COMPLETA EN WORKSPACE
# ----------------------------------------------------------
def get_workspace_path(relative_path: str) -> str:
    """
    Convierte una ruta relativa (por ejemplo '/frm_udv/conf/...') 
    en una ruta absoluta dentro del Workspace actual.
    """
    try:
        cwd = os.getcwd()
        project_root = cwd
        # Subir hasta el nivel que contiene 'util' (raíz del proyecto)
        while not os.path.exists(os.path.join(project_root, "util")) and len(project_root.split(os.sep)) > 3:
            project_root = os.path.abspath(os.path.join(project_root, ".."))

        clean_path = os.path.join(project_root, relative_path.lstrip("/"))
        print(f"[ENV_SETUP] Ruta YAML completa generada: {clean_path}")
        return clean_path
    except Exception as e:
        raise Exception(f"Error generando ruta de workspace: {e}")


# ----------------------------------------------------------
# CONFIGURACIÓN GLOBAL DE ENTORNO
# ----------------------------------------------------------
try:
    notebook_dir = os.getcwd()
    root = os.path.abspath(os.path.join(notebook_dir, ".."))
    while not os.path.exists(os.path.join(root, "util")) and len(root.split(os.sep)) > 3:
        root = os.path.abspath(os.path.join(root, ".."))

    if root not in sys.path:
        sys.path.append(root)
        print(f"[ENV_SETUP] Path raíz agregado: {root}")

    spark = (
        SparkSession.builder
        .config("spark.databricks.connect.enabled", "false")
        .config("spark.databricks.session.share", "false")
        .getOrCreate()
    )
    print("[ENV_SETUP] Sesión Spark inicializada correctamente")

    added = auto_import_modules(root)
    print("[ENV_SETUP] Módulos detectados y alias creados:")
    for name, path in added:
        print(f"  - {name} → {path}")
    print("[ENV_SETUP] Inicialización completa")

except Exception as e:
    print(f"[ENV_SETUP ERROR] Error durante configuración: {e}")
