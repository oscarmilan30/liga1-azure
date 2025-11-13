# ==========================================================
# ENV_SETUP.PY - VERSIÓN OPTIMIZADA (MENOS LOGS)
# ==========================================================

import os
import sys
from pyspark.sql import SparkSession
import warnings
warnings.filterwarnings("ignore")

def detect_repo_root():
    """
    Detecta automáticamente el entorno y retorna el directorio raíz
    """
    try:
        cwd = os.getcwd()
        
        # Job con Git
        if "/Repos/.internal" in cwd:
            parts = cwd.split("/Repos/.internal/")[1].split("/")
            commit_hash = parts[0]
            repo_root = f"/Workspace/Repos/.internal/{commit_hash}"
            
            subdirs = [d for d in os.listdir(repo_root) if os.path.isdir(os.path.join(repo_root, d))]
            if subdirs:
                repo_root = os.path.join(repo_root, subdirs[0])
            
            print(f"[ENV_SETUP] Job Git - Commit: {commit_hash}")
            return repo_root
        
        # Workspace interactivo
        elif "/Repos/" in cwd:
            parts = cwd.split("/Repos/")[1].split("/")
            repo_root = f"/Workspace/Repos/{parts[0]}/{parts[1]}"
            print(f"[ENV_SETUP] Workspace")
            return repo_root
        
        # Local
        else:
            return cwd
            
    except Exception as e:
        print(f"[ENV_SETUP ERROR] No se pudo detectar directorio raíz: {e}")
        return os.getcwd()

def get_workspace_path(relative_path: str) -> str:
    """
    Convierte rutas relativas a absolutas
    """
    repo_root = detect_repo_root()
    clean_relative = relative_path.lstrip("/")
    return os.path.join(repo_root, clean_relative)

def auto_import_modules(repo_root: str):
    """
    Configura automáticamente las importaciones entre carpetas
    """
    added = []
    invalid = [".git", ".github", "__pycache__", ".idea", ".vscode", "venv"]
    
    # Agregar raíz principal
    if repo_root not in sys.path:
        sys.path.append(repo_root)
        added.append(repo_root)
    
    # Agregar subcarpetas (hasta 2 niveles de profundidad)
    for root, dirs, _ in os.walk(repo_root):
        if root[len(repo_root):].count(os.sep) >= 2:
            continue
            
        for dir_name in dirs:
            if any(dir_name.startswith(inv) for inv in invalid):
                continue
                
            full_path = os.path.join(root, dir_name)
            if os.path.isdir(full_path) and full_path not in sys.path:
                sys.path.append(full_path)
                added.append(full_path)
    
    print(f"[ENV_SETUP] {len(added)} carpetas configuradas")
    return added

def get_or_create_spark():
    """
    Inicializa Spark de forma segura
    """
    try:
        spark = SparkSession.getActiveSession()
        if spark is None:
            spark = SparkSession.builder \
                .config("spark.databricks.connect.enabled", "false") \
                .config("spark.databricks.session.share", "false") \
                .getOrCreate()
        return spark
    except Exception:
        return None

# ==========================================================
# INICIALIZACIÓN AUTOMÁTICA
# ==========================================================
try:
    # Detectar directorio raíz
    repo_root = detect_repo_root()
    
    # Configurar sistema de importación
    if repo_root not in sys.path:
        sys.path.append(repo_root)
    
    # Agregar carpetas dinámicamente
    auto_import_modules(repo_root)
    
    # Inicializar Spark
    spark = get_or_create_spark()
    
    print("[ENV_SETUP] Configuración completada")
    
except Exception as e:
    print(f"[ENV_SETUP ERROR] {e}")