# ==========================================================
# ENV_SETUP.PY
# Proyecto: Liga 1 Perú - Configuración Universal Inteligente
# Autor: Oscar García Del Águila
# Versión: 3.5 (SmartBranch + Dynamic Imports + YAML Support)
# ==========================================================
# Compatible con:
#   - Notebooks interactivos (Workspace)
#   - Jobs Git-linked (.internal)
#   - Ejecución local o pruebas unitarias
#
# Características:
#   Detección automática de modo (.internal, interactive, local)
#   Refresco Git (solo en Jobs)
#   Importación dinámica de subcarpetas (sin hardcode)
#   Construcción de rutas absolutas para YAML
#   Detección automática de branch y commit
#   Verbosidad adaptativa (main → silencioso / develop → debug)
# ==========================================================

import os, sys, subprocess
from pyspark.sql import SparkSession

# ----------------------------------------------------------
# DETECTAR RAÍZ DEL REPO
# ----------------------------------------------------------
def detect_repo_root(verbose=False) -> str:
    try:
        cwd = os.getcwd()

        # Caso Job Git (.internal)
        if "/Repos/.internal" in cwd:
            parts = cwd.split("/Repos/.internal/")[1].split("/")
            commit_hash = parts[0]
            repo_root = f"/Workspace/Repos/.internal/{commit_hash}"
            subdirs = [d for d in os.listdir(repo_root) if os.path.isdir(os.path.join(repo_root, d))]
            if subdirs:
                repo_root = os.path.join(repo_root, subdirs[0])
            if verbose:
                print(f"[ENV_SETUP] Modo Job Git (.internal) detectado.")
            return repo_root

        # Caso Workspace interactivo
        elif "/Repos/" in cwd:
            parts = cwd.split("/Repos/")[1].split("/")
            repo_root = f"/Workspace/Repos/{parts[0]}/{parts[1]}"
            if verbose:
                print(f"[ENV_SETUP] Modo interactivo (Workspace).")
            return repo_root

        # Caso local o fallback
        else:
            if verbose:
                print(f"[ENV_SETUP] Modo local detectado.")
            return cwd

    except Exception as e:
        raise Exception(f"[ENV_SETUP ERROR] No se pudo detectar la raíz del repo: {e}")

# ----------------------------------------------------------
# REFRESCAR REPO (solo en modo .internal)
# ----------------------------------------------------------
def refresh_repo_if_needed(repo_root: str, verbose=False):
    try:
        if "/Repos/.internal" not in repo_root:
            return
        if verbose:
            print("[ENV_SETUP] Refrescando repo desde GitHub (origin/main)...")
        subprocess.run(["git", "-C", repo_root, "fetch", "--all"], check=False)
        subprocess.run(["git", "-C", repo_root, "reset", "--hard", "origin/main"], check=False)
        print("[ENV_SETUP] Repo sincronizado con origin/main")
    except Exception as e:
        if verbose:
            print(f"[ENV_SETUP WARN] No se pudo refrescar el repo: {e}")

# ----------------------------------------------------------
# OBTENER BRANCH ACTIVO Y COMMIT
# ----------------------------------------------------------
def get_git_info(repo_root: str):
    branch, commit = "unknown", "unknown"
    try:
        if ".internal" in repo_root:
            internal_hash = repo_root.split("/.internal/")[1].split("/")[0]
            branch, commit = "job_snapshot", internal_hash
        else:
            branch = subprocess.check_output(["git", "-C", repo_root, "branch", "--show-current"]).decode().strip()
            commit = subprocess.check_output(["git", "-C", repo_root, "rev-parse", "HEAD"]).decode().strip()[:7]
    except Exception:
        pass
    return branch, commit

# ----------------------------------------------------------
# CONSTRUCCIÓN DE RUTA ABSOLUTA (para YAML u otros archivos)
# ----------------------------------------------------------
def get_workspace_path(relative_path: str) -> str:
    """
    Convierte una ruta relativa del repo en absoluta dentro de Databricks.
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

# ----------------------------------------------------------
# AUTO IMPORTACIÓN DE MÓDULOS (DINÁMICO)
# ----------------------------------------------------------
def auto_import_modules(repo_root: str, verbose=False, depth=2):
    """
    Escanea dinámicamente todas las subcarpetas del repo y las agrega a sys.path.
    Ignora carpetas internas (.git, .github, __pycache__, .idea, .vscode, venv).
    """
    added = []

    def is_valid_folder(name):
        invalid = [".git", ".github", "__pycache__", ".idea", ".vscode", "venv"]
        return not any(name.startswith(i) for i in invalid)

    for root, dirs, _ in os.walk(repo_root):
        if root[len(repo_root):].count(os.sep) > depth:
            continue

        for d in dirs:
            if is_valid_folder(d):
                full_path = os.path.join(root, d)
                if os.path.isdir(full_path) and full_path not in sys.path:
                    sys.path.append(full_path)
                    added.append(full_path)

    if verbose:
        print("[ENV_SETUP] Carpetas añadidas dinámicamente:")
        for p in added:
            print(f"  - {p.replace(repo_root, '') or '/'}")
    return added

# ----------------------------------------------------------
# INICIALIZACIÓN SEGURA DE SPARK
# ----------------------------------------------------------
def get_or_create_spark(verbose=False):
    try:
        spark = SparkSession.getActiveSession()
        if spark is None:
            spark = (
                SparkSession.builder
                .config("spark.databricks.connect.enabled", "false")
                .config("spark.databricks.session.share", "false")
                .getOrCreate()
            )
            if verbose:
                print("[ENV_SETUP] Nueva sesión Spark creada.")
        else:
            if verbose:
                print("[ENV_SETUP] Sesión Spark reutilizada.")
        return spark
    except Exception as e:
        raise Exception(f"[ENV_SETUP ERROR] No se pudo crear/obtener SparkSession: {e}")

# ----------------------------------------------------------
# INICIALIZACIÓN AUTOMÁTICA AL IMPORTAR
# ----------------------------------------------------------
try:
    repo_root = detect_repo_root()
    branch, commit = get_git_info(repo_root)

    # Verbosidad automática según branch
    verbose = branch.lower() in ["develop", "dev", "debug"]

    refresh_repo_if_needed(repo_root, verbose)
    if repo_root not in sys.path:
        sys.path.append(repo_root)

    added = auto_import_modules(repo_root, verbose=verbose)
    spark = get_or_create_spark(verbose=verbose)

    print(f"[ENV_SETUP] Inicialización completa")
    print(f"[ENV_SETUP] Branch: {branch} | Commit: {commit}")

except Exception as e:
    print(f"[ENV_SETUP ERROR] Falló la inicialización: {e}")
