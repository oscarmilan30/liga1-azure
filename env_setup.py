# ==========================================================
# ENV_SETUP.PY
# Proyecto: Liga 1 Perú - Configuración Universal Inteligente
# Autor: Oscar García Del Águila
# Versión: 3.8 (AutoGitSync Smart Dynamic + No Hardcoded URLs)
# ==========================================================

import os, sys, subprocess, tempfile, warnings
from pyspark.sql import SparkSession

warnings.filterwarnings("ignore")

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
                print(f"[ENV_SETUP] Modo Job Git (.internal) detectado → {repo_root}")
            return repo_root

        # Caso Workspace interactivo
        elif "/Repos/" in cwd:
            parts = cwd.split("/Repos/")[1].split("/")
            repo_root = f"/Workspace/Repos/{parts[0]}/{parts[1]}"
            if verbose:
                print(f"[ENV_SETUP] Modo interactivo detectado → {repo_root}")
            return repo_root

        # Caso local o fallback
        else:
            if verbose:
                print(f"[ENV_SETUP] Modo local detectado → {cwd}")
            return cwd
    except Exception as e:
        raise Exception(f"[ENV_SETUP ERROR] No se pudo detectar la raíz del repo: {e}")

# ----------------------------------------------------------
# OBTENER INFORMACIÓN GIT (rama, commit y URL)
# ----------------------------------------------------------
def get_git_info(repo_root):
    branch, commit, remote_url = "unknown", "unknown", None
    try:
        # URL del remoto
        remote_url = subprocess.check_output(
            ["git", "-C", repo_root, "config", "--get", "remote.origin.url"],
            stderr=subprocess.DEVNULL
        ).decode().strip()

        # Rama actual
        branch = subprocess.check_output(
            ["git", "-C", repo_root, "branch", "--show-current"],
            stderr=subprocess.DEVNULL
        ).decode().strip()

        # Último commit corto
        commit = subprocess.check_output(
            ["git", "-C", repo_root, "rev-parse", "HEAD"],
            stderr=subprocess.DEVNULL
        ).decode().strip()[:7]
    except Exception:
        if ".internal" in repo_root:
            commit = repo_root.split("/.internal/")[1].split("/")[0]
            branch = "job_snapshot"
    return branch or "unknown", commit or "unknown", remote_url

# ----------------------------------------------------------
# AUTO-GIT SYNC (solo si está en .internal)
# ----------------------------------------------------------
def auto_git_sync(repo_root, remote_url=None, branch_name=None, verbose=False):
    try:
        if "/Repos/.internal" not in repo_root or not remote_url:
            return repo_root  # No aplica

        print("[ENV_SETUP] 🔄 Activando AutoGitSync (modo Job Git).")

        tmp_dir = os.path.join(tempfile.gettempdir(), "liga1_repo_live")

        if not os.path.exists(tmp_dir):
            subprocess.run(
                ["git", "clone", "-b", branch_name or "main", remote_url, tmp_dir],
                check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            )
            print(f"[ENV_SETUP] Repositorio clonado desde {remote_url}")
        else:
            subprocess.run(["git", "-C", tmp_dir, "fetch", "--all"],
                           check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            subprocess.run(["git", "-C", tmp_dir, "reset", "--hard", f"origin/{branch_name or 'main'}"],
                           check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            print(f"[ENV_SETUP] Repositorio actualizado ({branch_name or 'main'})")

        return tmp_dir
    except Exception as e:
        print(f"[ENV_SETUP WARN] No se pudo clonar/actualizar el repo: {e}")
        return repo_root

# ----------------------------------------------------------
# CONSTRUCCIÓN DE RUTA ABSOLUTA (para YAML, etc.)
# ----------------------------------------------------------
def get_workspace_path(relative_path: str) -> str:
    repo_root = detect_repo_root()
    clean_relative = relative_path.lstrip("/")
    full_path = os.path.join(repo_root, clean_relative)
    print(f"[ENV_SETUP] Ruta absoluta generada: {full_path}")
    return full_path

# ----------------------------------------------------------
# AUTO IMPORTACIÓN DE MÓDULOS (DINÁMICO)
# ----------------------------------------------------------
def auto_import_modules(repo_root: str, verbose=False, depth=2):
    added = []
    invalid = [".git", ".github", "__pycache__", ".idea", ".vscode", "venv"]

    for root, dirs, _ in os.walk(repo_root):
        if root[len(repo_root):].count(os.sep) > depth:
            continue
        for d in dirs:
            if not any(d.startswith(i) for i in invalid):
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
# SPARK SAFE INITIALIZATION
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
# INICIALIZACIÓN AUTOMÁTICA
# ----------------------------------------------------------
try:
    repo_root = detect_repo_root()
    branch, commit, remote_url = get_git_info(repo_root)

    # Verbosidad automática
    verbose = branch.lower() in ["develop", "dev", "debug"]

    # Si es snapshot, sincroniza dinámicamente
    repo_root = auto_git_sync(repo_root, remote_url, branch, verbose)

    # Asegurar sys.path
    if repo_root not in sys.path:
        sys.path.append(repo_root)

    added = auto_import_modules(repo_root, verbose=verbose)
    spark = get_or_create_spark(verbose=verbose)

    print(f"[ENV_SETUP] Inicialización completa")
    print(f"[ENV_SETUP] Branch: {branch} | Commit: {commit}")

except Exception as e:
    print(f"[ENV_SETUP ERROR] Falló la inicialización: {e}")
