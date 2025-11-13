# ==========================================================
# ENV_SETUP.PY - VERSIÓN SIMPLIFICADA
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
                print(f"[ENV_SETUP] Modo Job Git (.internal) detectado -> {repo_root}")
            return repo_root

        # Caso Workspace interactivo
        elif "/Repos/" in cwd:
            parts = cwd.split("/Repos/")[1].split("/")
            repo_root = f"/Workspace/Repos/{parts[0]}/{parts[1]}"
            if verbose:
                print(f"[ENV_SETUP] Modo interactivo detectado -> {repo_root}")
            return repo_root

        # Caso local o fallback
        else:
            if verbose:
                print(f"[ENV_SETUP] Modo local detectado -> {cwd}")
            return cwd
    except Exception as e:
        raise Exception(f"[ENV_SETUP ERROR] No se pudo detectar la raiz del repo: {e}")

# ----------------------------------------------------------
# SINCRONIZACIÓN CON GITHUB
# ----------------------------------------------------------
def sync_with_github():
    """Sincroniza siempre con GitHub main"""
    try:
        remote_url = "https://github.com/oscarmilan30/liga1-azure.git"
        branch = "main"
        
        print(f"[ENV_SETUP] Sincronizando con GitHub main...")
        
        # Usar directorio temporal
        temp_dir = tempfile.mkdtemp()
        github_dir = os.path.join(temp_dir, "liga1_github_main")
        
        if not os.path.exists(github_dir):
            print(f"[ENV_SETUP] Clonando repositorio...")
            result = subprocess.run(
                ["git", "clone", "-b", branch, remote_url, github_dir],
                capture_output=True, text=True, timeout=120
            )
            if result.returncode != 0:
                print(f"[ENV_SETUP ERROR] Clone fallo: {result.stderr}")
                return None
        else:
            print(f"[ENV_SETUP] Actualizando repositorio...")
            # Fetch y reset hard a main
            fetch_result = subprocess.run(
                ["git", "-C", github_dir, "fetch", "origin"], 
                capture_output=True, text=True
            )
            reset_result = subprocess.run(
                ["git", "-C", github_dir, "reset", "--hard", "origin/main"],
                capture_output=True, text=True
            )
            if reset_result.returncode != 0:
                print(f"[ENV_SETUP ERROR] Reset fallo: {reset_result.stderr}")
                return None
        
        # Verificar commit actual
        current_commit = subprocess.check_output(
            ["git", "-C", github_dir, "rev-parse", "--short", "HEAD"]
        ).decode().strip()
        
        print(f"[ENV_SETUP] Codigo actualizado - Commit: {current_commit}")
        return github_dir
            
    except Exception as e:
        print(f"[ENV_SETUP ERROR] Error sincronizando GitHub: {e}")
        return None

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

    # Agregar la raiz principal primero
    if repo_root not in sys.path:
        sys.path.append(repo_root)
        added.append(repo_root)

    # Agregar subcarpetas dinamicamente
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
        print("[ENV_SETUP] Carpetas anadidas dinamicamente:")
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
                print("[ENV_SETUP] Nueva sesion Spark creada.")
        else:
            if verbose:
                print("[ENV_SETUP] Sesion Spark reutilizada.")
        return spark
    except Exception as e:
        raise Exception(f"[ENV_SETUP ERROR] No se pudo crear/obtener SparkSession: {e}")

# ----------------------------------------------------------
# INICIALIZACIÓN AUTOMÁTICA
# ----------------------------------------------------------
try:
    # Detectar raiz del repo original (snapshot)
    original_root = detect_repo_root()
    print(f"[ENV_SETUP] Raiz del repo detectada: {original_root}")
    
    # SIEMPRE intentar sincronizar con GitHub
    github_root = sync_with_github()
    
    if github_root:
        repo_root = github_root
        print(f"[ENV_SETUP] Usando codigo actualizado de GitHub")
    else:
        repo_root = original_root
        print(f"[ENV_SETUP] Usando snapshot del Job (fallo GitHub)")

    # Configurar importaciones
    if repo_root not in sys.path:
        sys.path.append(repo_root)

    # Auto-importar modulos dinamicamente
    added = auto_import_modules(repo_root, verbose=True)
    
    # Crear sesion Spark
    spark = get_or_create_spark(verbose=True)

    print(f"[ENV_SETUP] INICIALIZACION COMPLETADA EXITOSAMENTE")
    print(f"[ENV_SETUP] Directorio activo: {repo_root}")

except Exception as e:
    print(f"[ENV_SETUP ERROR] Fallo la inicializacion: {e}")
    import traceback
    traceback.print_exc()