# ==========================================================
# ENV_SETUP.PY - VERSIÓN UNIVERSAL
# Funciona en Workspace Y Jobs Git sin modificar
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
        
        # Caso 1: Job con Git (.internal)
        if "/Repos/.internal" in cwd:
            parts = cwd.split("/Repos/.internal/")[1].split("/")
            commit_hash = parts[0]
            repo_root = f"/Workspace/Repos/.internal/{commit_hash}"
            
            # Buscar subdirectorio principal
            subdirs = [d for d in os.listdir(repo_root) if os.path.isdir(os.path.join(repo_root, d))]
            if subdirs:
                repo_root = os.path.join(repo_root, subdirs[0])
            
            print(f"[ENV_SETUP] Entorno: Job Git | Commit: {commit_hash}")
            return repo_root
        
        # Caso 2: Workspace interactivo
        elif "/Repos/" in cwd:
            parts = cwd.split("/Repos/")[1].split("/")
            repo_root = f"/Workspace/Repos/{parts[0]}/{parts[1]}"
            print(f"[ENV_SETUP] Entorno: Workspace")
            return repo_root
        
        # Caso 3: Local
        else:
            print(f"[ENV_SETUP] Entorno: Local")
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
    full_path = os.path.join(repo_root, clean_relative)
    return full_path

def auto_import_modules(repo_root: str):
    """
    Configura automáticamente las importaciones entre carpetas
    """
    added = []
    # Carpetas que NO deben agregarse al path
    invalid = [".git", ".github", "__pycache__", ".idea", ".vscode", "venv"]
    
    # 1. Agregar raíz principal
    if repo_root not in sys.path:
        sys.path.append(repo_root)
        added.append(repo_root)
    
    # 2. Agregar subcarpetas (hasta 2 niveles de profundidad)
    for root, dirs, _ in os.walk(repo_root):
        # Limitar profundidad para no sobrecargar
        if root[len(repo_root):].count(os.sep) >= 2:
            continue
            
        for dir_name in dirs:
            # Saltar carpetas inválidas
            if any(dir_name.startswith(inv) for inv in invalid):
                continue
                
            full_path = os.path.join(root, dir_name)
            if os.path.isdir(full_path) and full_path not in sys.path:
                sys.path.append(full_path)
                added.append(full_path)
    
    # Mostrar solo carpetas válidas (sin .git, etc.)
    valid_paths = [p for p in added if not any(inv in p for inv in invalid)]
    print(f"[ENV_SETUP] Carpetas disponibles: {len(valid_paths)}")
    for path in valid_paths:
        print(f"  - {path.replace(repo_root, '') or '/'}")
    
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
            print("[ENV_SETUP] Spark: Nueva sesión creada")
        else:
            print("[ENV_SETUP] Spark: Sesión existente reutilizada")
        return spark
    except Exception as e:
        print(f"[ENV_SETUP ERROR] No se pudo inicializar Spark: {e}")
        return None

# ==========================================================
# INICIALIZACIÓN AUTOMÁTICA
# ==========================================================
try:
    # 1. Detectar directorio raíz
    repo_root = detect_repo_root()
    print(f"[ENV_SETUP] Directorio principal: {repo_root}")
    
    # 2. Configurar sistema de importación
    if repo_root not in sys.path:
        sys.path.append(repo_root)
    
    # 3. Agregar carpetas dinámicamente
    auto_import_modules(repo_root)
    
    # 4. Inicializar Spark
    spark = get_or_create_spark()
    
    print("[ENV_SETUP] Configuración completada exitosamente")
    
except Exception as e:
    print(f"[ENV_SETUP ERROR] Error en inicialización: {e}")
    import traceback
    traceback.print_exc()