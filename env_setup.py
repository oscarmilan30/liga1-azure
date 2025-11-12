# ==========================================================
# ENV_SETUP - Configuración dinámica de imports
# Proyecto: Liga 1 Perú
# Autor: Oscar García Del Águila
# ==========================================================
# Detecta automáticamente si está dentro de /Workspace/Repos
# Ajusta sys.path al repo raíz (funciona en ADF, jobs, local y manual)
# Autoimporta módulos .py de todas las carpetas
# Ignora notebooks (nb_*.py)
# Crea alias automáticos (utils_liga1, curated_json, etc.)
# Incluye get_workspace_path() para rutas relativas seguras
# ==========================================================

import os
import sys
import importlib
from pyspark.sql import SparkSession

# ----------------------------------------------------------
# DETECTAR Y CONFIGURAR RUTA DEL REPO
# ----------------------------------------------------------
def detect_repo_root():
    """
    Detecta automáticamente el root del repo Databricks,
    incluso cuando se ejecuta como Job desde ADF.
    """
    cwd = os.getcwd()

    # Si el path contiene /Workspace/Repos/, sube hasta el repo raíz
    if "/Workspace/Repos/" in cwd:
        parts = cwd.split("/Workspace/Repos/")[1].split("/")
        # /Workspace/Repos/usuario/repositorio
        if len(parts) >= 2:
            repo_root = f"/Workspace/Repos/{parts[0]}/{parts[1]}"
            if os.path.exists(repo_root):
                print(f"[ENV_SETUP] Repo raíz detectado automáticamente: {repo_root}")
                return repo_root

    # Si no lo detecta, intenta subir hasta encontrar la carpeta 'util'
    project_root = cwd
    while not os.path.exists(os.path.join(project_root, "util")) and len(project_root.split(os.sep)) > 3:
        project_root = os.path.abspath(os.path.join(project_root, ".."))

    print(f"[ENV_SETUP] Path raíz asumido: {project_root}")
    return project_root


# ----------------------------------------------------------
# AUTOIMPORTADOR DE MÓDULOS
# ----------------------------------------------------------
def auto_import_modules(base_dir, exclude_folders=None):
    """
    Importa dinámicamente todos los módulos .py del proyecto,
    excluyendo carpetas no deseadas y notebooks (nb_*).
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
# RESOLVER RUTA COMPLETA EN WORKSPACE / REPO
# ----------------------------------------------------------
def get_workspace_path(relative_path: str) -> str:
    """
    Convierte una ruta relativa (por ejemplo '/frm_udv/conf/...') 
    en una ruta absoluta dentro del Workspace o Repo actual.

    🔹 Funciona igual desde ADF, Databricks Job o sesión manual.
    🔹 Usa la raíz detectada del repo (detect_repo_root()).
    """
    try:
        repo_root = detect_repo_root()
        clean_relative = relative_path.lstrip("/")
        full_path = os.path.join(repo_root, clean_relative)
        print(f"[ENV_SETUP] Ruta absoluta generada: {full_path}")
        return full_path
    except Exception as e:
        raise Exception(f"[ENV_SETUP ERROR] Error generando ruta para {relative_path}: {e}")


# ----------------------------------------------------------
# CONFIGURACIÓN GLOBAL DE ENTORNO
# ----------------------------------------------------------
try:
    repo_root = detect_repo_root()

    if repo_root not in sys.path:
        sys.path.append(repo_root)
        print(f"[ENV_SETUP] Path raíz agregado a sys.path: {repo_root}")

    spark = (
        SparkSession.builder
        .config("spark.databricks.connect.enabled", "false")
        .config("spark.databricks.session.share", "false")
        .getOrCreate()
    )
    print("[ENV_SETUP] Sesión Spark inicializada correctamente")

    added = auto_import_modules(repo_root)
    print("[ENV_SETUP] Módulos detectados y alias creados:")
    for name, path in added:
        print(f"  - {name} → {path}")

    print("[ENV_SETUP] Inicialización completa ✅")

except Exception as e:
    print(f"[ENV_SETUP ERROR] Error durante configuración: {e}")
