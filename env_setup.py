# ==========================================================
# ENV_SETUP.PY
# Proyecto: Liga 1 Perú - Configuración Universal Inteligente
# Autor: Oscar García Del Águila
# Versión: 3.8.1 (AutoGitSync Fixed for Jobs + No Hardcoded URLs)
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
# OBTENER INFORMACIÓN GIT (rama, commit y URL) - MEJORADO
# ----------------------------------------------------------
def get_git_info(repo_root):
    branch, commit, remote_url = "unknown", "unknown", None
    
    try:
        # URL del remoto - con mejor manejo de errores
        try:
            remote_url = subprocess.check_output(
                ["git", "-C", repo_root, "config", "--get", "remote.origin.url"],
                stderr=subprocess.DEVNULL
            ).decode().strip()
        except:
            remote_url = None

        # Rama actual
        try:
            branch = subprocess.check_output(
                ["git", "-C", repo_root, "branch", "--show-current"],
                stderr=subprocess.DEVNULL
            ).decode().strip()
        except:
            branch = "unknown"

        # Último commit corto
        try:
            commit = subprocess.check_output(
                ["git", "-C", repo_root, "rev-parse", "HEAD"],
                stderr=subprocess.DEVNULL
            ).decode().strip()[:7]
        except:
            commit = "unknown"
            
    except Exception:
        # En modo .internal, extraer información disponible
        if ".internal" in repo_root:
            try:
                commit = repo_root.split("/.internal/")[1].split("/")[0]
                branch = "job_snapshot"
            except:
                pass
                
    return branch or "unknown", commit or "unknown", remote_url

# ----------------------------------------------------------
# AUTO-GIT SYNC (FIXED PARA JOBS) - MEJORADO
# ----------------------------------------------------------
def auto_git_sync(repo_root, remote_url=None, branch_name=None, verbose=False):
    try:
        # Si estamos en .internal (Job Git), forzar sincronización
        if "/Repos/.internal" in repo_root:
            # Si no tenemos remote_url, usar la URL del repo de GitHub
            if not remote_url:
                remote_url = "https://github.com/oscarmilan30/liga1-azure.git"
                print(f"[ENV_SETUP] URL de Git no detectada, usando URL por defecto: {remote_url}")
            
            # Usar la branch del Job si no se especifica (prioridad: branch_name > main)
            target_branch = branch_name or "main"
            
            print(f"[ENV_SETUP] ACTIVANDO AUTOGITSYNC PARA JOB")
            print(f"[ENV_SETUP]   Repo: {remote_url}")
            print(f"[ENV_SETUP]   Rama: {target_branch}")
            print(f"[ENV_SETUP]   Directorio original: {repo_root}")

            # Crear directorio temporal para el repo actualizado
            tmp_dir = os.path.join(tempfile.gettempdir(), "liga1_repo_live_sync")
            print(f"[ENV_SETUP]   Directorio temporal: {tmp_dir}")

            # Clonar o actualizar el repositorio
            if not os.path.exists(tmp_dir):
                print(f"[ENV_SETUP]   Clonando repositorio por primera vez...")
                result = subprocess.run(
                    ["git", "clone", "-b", target_branch, remote_url, tmp_dir],
                    capture_output=True, text=True
                )
                if result.returncode != 0:
                    print(f"[ENV_SETUP WARN] Error en clone: {result.stderr}")
                    return repo_root
                print(f"[ENV_SETUP]   Repositorio clonado exitosamente")
            else:
                print(f"[ENV_SETUP]   Actualizando repositorio existente...")
                # Fetch de todos los cambios
                fetch_result = subprocess.run(
                    ["git", "-C", tmp_dir, "fetch", "--all"],
                    capture_output=True, text=True
                )
                if fetch_result.returncode != 0:
                    print(f"[ENV_SETUP WARN] Error en fetch: {fetch_result.stderr}")
                
                # Reset hard a la branch especificada
                reset_result = subprocess.run(
                    ["git", "-C", tmp_dir, "reset", "--hard", f"origin/{target_branch}"],
                    capture_output=True, text=True
                )
                if reset_result.returncode != 0:
                    print(f"[ENV_SETUP WARN] Error en reset: {reset_result.stderr}")
                    return repo_root
                
                print(f"[ENV_SETUP]   Repositorio actualizado exitosamente")

            # Verificar el commit actual en el repo actualizado
            try:
                latest_commit = subprocess.check_output(
                    ["git", "-C", tmp_dir, "rev-parse", "--short", "HEAD"]
                ).decode().strip()
                print(f"[ENV_SETUP]   Commit más reciente: {latest_commit}")
            except:
                pass

            print(f"[ENV_SETUP]   Usando código actualizado de GitHub")
            return tmp_dir
        
        # Para otros casos, no hacer nada
        if verbose:
            print(f"[ENV_SETUP] AutoGitSync no aplica para este contexto")
        return repo_root
        
    except Exception as e:
        print(f"[ENV_SETUP ERROR] Fallo crítico en AutoGitSync: {e}")
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
    
    # Obtener información Git
    branch, commit, remote_url = get_git_info(repo_root)
    print(f"[ENV_SETUP] Info Git inicial - Branch: {branch}, Commit: {commit}, Remote: {remote_url}")

    # Verbosidad automática
    verbose = branch.lower() in ["develop", "dev", "debug"] or True  # Forzar verbose para debugging

    # SI ESTAMOS EN JOB GIT (.internal), SINCRONIZAR CON GITHUB
    if "/Repos/.internal" in repo_root:
        print(f"[ENV_SETUP] 🚀 MODO JOB GIT DETECTADO - ACTIVANDO SINCRONIZACIÓN")
        repo_root = auto_git_sync(repo_root, remote_url, branch, verbose=True)
        print(f"[ENV_SETUP] Nueva raíz del repo después de sync: {repo_root}")
        
        # Re-obtener información Git del repo actualizado
        branch, commit, remote_url = get_git_info(repo_root)
        print(f"[ENV_SETUP] Info Git después de sync - Branch: {branch}, Commit: {commit}")

    # Asegurar sys.path
    if repo_root not in sys.path:
        sys.path.append(repo_root)
        print(f"[ENV_SETUP] Raíz del repo añadida a sys.path: {repo_root}")

    # Auto-importar módulos
    added = auto_import_modules(repo_root, verbose=True)
    
    # Crear sesión Spark
    spark = get_or_create_spark(verbose=True)

    print(f"[ENV_SETUP] INICIALIZACIÓN COMPLETADA EXITOSAMENTE")
    print(f"[ENV_SETUP] Branch: {branch} | Commit: {commit}")
    if remote_url:
        print(f"[ENV_SETUP] Remote: {remote_url}")
    print(f"[ENV_SETUP] Directorio activo: {repo_root}")

except Exception as e:
    print(f"[ENV_SETUP ERROR] Falló la inicialización: {e}")
    # Forzar continuar pero mostrar error claro
    import traceback
    traceback.print_exc()