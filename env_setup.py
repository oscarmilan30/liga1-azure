# ==========================================================
# ENV_SETUP.PY
# Proyecto: Liga 1 Perú - Configuración Universal Inteligente
# Autor: Oscar García Del Águila
# Versión: 3.0 (Hybrid Smart Mode)
# ==========================================================
# Compatible con notebooks interactivos y Jobs Git-linked (.internal)
# Inicializa Spark solo si no existe
# Detecta automáticamente el root del repo (sin hardcodear)
# Prepara sys.path con subcarpetas clave (utils, frm_udv, etc.)
# Compatible con lectura dinámica de YAML (get_workspace_path)
# ==========================================================

import os, sys
from pyspark.sql import SparkSession

# ==========================================================
# DETECCIÓN AUTOMÁTICA DE REPO
# ==========================================================

def detect_repo_root() -> str:
    """
    Detecta automáticamente la raíz del repositorio:
      - /Workspace/Repos/<user>/<repo>
      - /Workspace/Repos/.internal/<commit_hash>/<repo>
      - Fallback: cwd()
    """
    try:
        cwd = os.getcwd()

        # Caso 1: Job Git (.internal)
        if "/Repos/.internal" in cwd:
            parts = cwd.split("/Repos/.internal/")[1].split("/")
            commit_hash = parts[0]
            repo_root = f"/Workspace/Repos/.internal/{commit_hash}"

            # Buscar automáticamente el nombre real del repo
            subdirs = [d for d in os.listdir(repo_root) if os.path.isdir(os.path.join(repo_root, d))]
            if subdirs:
                repo_root = os.path.join(repo_root, subdirs[0])

            print(f"[ENV_SETUP] Modo Job Git detectado (.internal). Repo raíz: {repo_root}")
            return repo_root

        # Caso 2: Modo usuario / Repositorio normal
        elif "/Repos/" in cwd:
            parts = cwd.split("/Repos/")[1].split("/")
            repo_root = f"/Workspace/Repos/{parts[0]}/{parts[1]}"
            print(f"[ENV_SETUP] Modo interactivo detectado. Repo raíz: {repo_root}")
            return repo_root

        # Caso 3: Fallback genérico
        else:
            print(f"[ENV_SETUP] Ruta base genérica usada: {cwd}")
            return cwd

    except Exception as e:
        raise Exception(f"[ENV_SETUP ERROR] No se pudo detectar la raíz del repo: {e}")


# ==========================================================
# CONSTRUCCIÓN DE RUTA WORKSPACE
# ==========================================================

def get_workspace_path(relative_path: str) -> str:
    """
    Construye una ruta absoluta a partir de una ruta relativa dentro del repo.
    Ejemplo:
        get_workspace_path('/frm_udv/conf/m_catalogo_equipos/m_catalogo_equipos.yml')
    """
    try:
        repo_root = detect_repo_root()
        clean_relative = relative_path.lstrip("/")
        full_path = os.path.join(repo_root, clean_relative)
        print(f"[ENV_SETUP] Ruta absoluta generada: {full_path}")
        return full_path
    except Exception as e:
        raise Exception(f"[ENV_SETUP ERROR] Error generando ruta para {relative_path}: {e}")


# ==========================================================
# AUTO IMPORTACIÓN DE MÓDULOS
# ==========================================================

def auto_import_modules(repo_root: str):
    """
    Agrega subcarpetas del repo a sys.path para habilitar imports absolutos.
    Devuelve una lista con las carpetas agregadas.
    """
    added = []

    def add_path_if_exists(folder_name):
        path = os.path.join(repo_root, folder_name)
        if os.path.isdir(path) and path not in sys.path:
            sys.path.append(path)
            added.append((folder_name, path))

    # carpetas típicas del proyecto
    for folder in ["util", "frm_udv", "frm_curated", "frm_raw", "notebooks"]:
        add_path_if_exists(folder)

    return added


# ==========================================================
# INICIALIZACIÓN SPARK SEGURA
# ==========================================================

def get_or_create_spark():
    """
    Obtiene o crea una sesión Spark segura.
    No crea una nueva si ya existe.
    """
    try:
        spark = SparkSession.getActiveSession()
        if spark is None:
            spark = (
                SparkSession.builder
                .config("spark.databricks.connect.enabled", "false")
                .config("spark.databricks.session.share", "false")
                .getOrCreate()
            )
            print("[ENV_SETUP] Nueva sesión Spark creada.")
        else:
            print("[ENV_SETUP] Sesión Spark existente reutilizada.")
        return spark
    except Exception as e:
        raise Exception(f"[ENV_SETUP ERROR] No se pudo crear/obtener SparkSession: {e}")


# ==========================================================
# AUTO-EJECUCIÓN AL IMPORTAR
# ==========================================================

try:
    repo_root = detect_repo_root()

    # Insertar el repo raíz al sys.path
    if repo_root not in sys.path:
        sys.path.append(repo_root)
        print(f"[ENV_SETUP] Path raíz agregado a sys.path: {repo_root}")

    # Inicializar Spark de forma segura (solo si hace falta)
    spark = get_or_create_spark()

    # Registrar subcarpetas del repo
    added = auto_import_modules(repo_root)
    if added:
        print("[ENV_SETUP] Carpetas agregadas al sys.path:")
        for name, path in added:
            print(f"  - {name} → {path}")
    else:
        print("[ENV_SETUP] No se agregaron nuevas carpetas (ya estaban registradas).")

    print("[ENV_SETUP] Inicialización completa (Smart Mode).")

except Exception as e:
    print(f"[ENV_SETUP WARNING] Error durante setup inicial: {e}")
