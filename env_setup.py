# ==========================================================
# ENV_SETUP.PY
# Proyecto: Liga 1 Perú - Configuración Universal Inteligente
# Autor: Oscar García Del Águila
# Versión: 3.8.3 (Usa Git Snapshot Commit + Auto Update)
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
# OBTENER COMMIT DEL SNAPSHOT DEL JOB
# ----------------------------------------------------------
def get_job_snapshot_commit(repo_root):
    """Obtiene el commit del snapshot que Databricks está usando"""
    try:
        if "/Repos/.internal" in repo_root:
            # Extraer el commit del path del snapshot
            commit_hash = repo_root.split("/.internal/")[1].split("/")[0]
            print(f"[ENV_SETUP] Commit del Snapshot del Job: {commit_hash}")
            return commit_hash
        return None
    except Exception as e:
        print(f"[ENV_SETUP WARN] No se pudo obtener commit del snapshot: {e}")
        return None

# ----------------------------------------------------------
# OBTENER ÚLTIMO COMMIT DE GITHUB
# ----------------------------------------------------------
def get_latest_github_commit(remote_url="https://github.com/oscarmilan30/liga1-azure.git", branch="main"):
    """Obtiene el último commit de GitHub sin clonar el repo completo"""
    try:
        print(f"[ENV_SETUP] Obteniendo último commit de GitHub...")
        print(f"[ENV_SETUP]   Repo: {remote_url}")
        print(f"[ENV_SETUP]   Rama: {branch}")
        
        # Usar git ls-remote para obtener el último commit sin clonar
        result = subprocess.run(
            ["git", "ls-remote", remote_url, f"refs/heads/{branch}"],
            capture_output=True, text=True, timeout=30
        )
        
        if result.returncode == 0:
            latest_commit = result.stdout.split()[0]
            short_commit = latest_commit[:7]
            print(f"[ENV_SETUP]   Último commit en GitHub: {short_commit}")
            return latest_commit, short_commit
        else:
            print(f"[ENV_SETUP WARN] Error al obtener último commit: {result.stderr}")
            return None, None
    except Exception as e:
        print(f"[ENV_SETUP WARN] No se pudo obtener último commit de GitHub: {e}")
        return None, None

# ----------------------------------------------------------
# SINCRONIZACIÓN INTELIGENTE CON GITHUB
# ----------------------------------------------------------
def smart_git_sync(repo_root, verbose=False):
    """Sincroniza solo si el snapshot está desactualizado"""
    try:
        if "/Repos/.internal" not in repo_root:
            return repo_root  # No aplica
        
        remote_url = "https://github.com/oscarmilan30/liga1-azure.git"
        branch = "main"
        
        # Obtener información de commits
        snapshot_commit = get_job_snapshot_commit(repo_root)
        latest_commit, latest_short = get_latest_github_commit(remote_url, branch)
        
        if not latest_commit:
            print(f"[ENV_SETUP] No se pudo verificar GitHub, usando snapshot del Job")
            return repo_root
        
        # Verificar si el snapshot está actualizado
        if snapshot_commit and latest_commit.startswith(snapshot_commit):
            print(f"[ENV_SETUP] El snapshot del Job está ACTUALIZADO (commit: {snapshot_commit})")
            return repo_root
        else:
            print(f"[ENV_SETUP] El snapshot está DESACTUALIZADO")
            print(f"[ENV_SETUP]   Snapshot: {snapshot_commit}")
            print(f"[ENV_SETUP]   GitHub:   {latest_short}")
            print(f"[ENV_SETUP]   Actualizando código...")
            
            # Clonar/actualizar el repo
            tmp_dir = os.path.join(tempfile.gettempdir(), f"liga1_github_{latest_short}")
            
            if not os.path.exists(tmp_dir):
                print(f"[ENV_SETUP]   Clonando último código de GitHub...")
                result = subprocess.run(
                    ["git", "clone", "-b", branch, remote_url, tmp_dir],
                    capture_output=True, text=True, timeout=120
                )
                if result.returncode != 0:
                    print(f"[ENV_SETUP ERROR] Clone falló: {result.stderr}")
                    return repo_root
            else:
                print(f"[ENV_SETUP]   Actualizando repo existente...")
                # Reset al último commit
                subprocess.run(["git", "-C", tmp_dir, "fetch"], capture_output=True)
                subprocess.run(["git", "-C", tmp_dir, "reset", "--hard", f"origin/{branch}"], capture_output=True)
            
            # Verificar el commit actual
            current_commit = subprocess.check_output(
                ["git", "-C", tmp_dir, "rev-parse", "--short", "HEAD"]
            ).decode().strip()
            
            print(f"[ENV_SETUP]   Código actualizado a commit: {current_commit}")
            return tmp_dir
            
    except Exception as e:
        print(f"[ENV_SETUP ERROR] Error en sincronización: {e}")
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
# INICIALIZACIÓN AUTOMÁTICA - MEJORADA
# ----------------------------------------------------------
try:
    # Detectar raíz del repo
    repo_root = detect_repo_root()
    print(f"[ENV_SETUP] Raíz del repo detectada: {repo_root}")
    
    # Obtener información del snapshot
    snapshot_commit = get_job_snapshot_commit(repo_root)

    # SINCRONIZACIÓN INTELIGENTE
    if "/Repos/.internal" in repo_root:
        print(f"[ENV_SETUP] 🚀 MODO JOB GIT DETECTADO")
        repo_root = smart_git_sync(repo_root, verbose=True)
        print(f"[ENV_SETUP] Nueva raíz del repo: {repo_root}")

    # Asegurar sys.path
    if repo_root not in sys.path:
        sys.path.append(repo_root)

    # Auto-importar módulos
    added = auto_import_modules(repo_root, verbose=True)
    
    # Crear sesión Spark
    spark = get_or_create_spark(verbose=True)

    print(f"[ENV_SETUP] INICIALIZACIÓN COMPLETADA EXITOSAMENTE")
    print(f"[ENV_SETUP] Directorio activo: {repo_root}")

except Exception as e:
    print(f"[ENV_SETUP ERROR] ❌ Falló la inicialización: {e}")
    import traceback
    traceback.print_exc()