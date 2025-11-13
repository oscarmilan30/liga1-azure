# ==========================================================
# ENV_SETUP.PY
# Proyecto: Liga 1 Perú - Configuración Universal Inteligente
# Autor: Oscar García Del Águila
# Versión: 3.2 (Hybrid Smart Mode + Git Auto Refresh + Diagnostics)
# ==========================================================
# Compatible con:
#   - Notebooks interactivos (Workspace)
#   - Jobs Git-linked (.internal)
#
# Características:
#   Detecta automáticamente la raíz del repo (sin hardcodear)
#   Refresca automáticamente el código desde origin/main (solo en Jobs Git)
#   Muestra commit activo y carpeta .internal real
#   Inicializa Spark solo si no existe
#   Prepara sys.path con subcarpetas (util, frm_udv, frm_curated, frm_raw, notebooks)
#   Compatible con lectura dinámica de YAML
# ==========================================================

import os, sys, subprocess
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
# REFRESCO AUTOMÁTICO DEL REPO (.internal)
# ==========================================================

def refresh_repo_if_needed():
    """
    Fuerza la actualización del repo Git si se está ejecutando en un entorno .internal
    (Databricks Lakeflow Job). Realiza git fetch + reset a origin/main.
    """
    try:
        cwd = os.getcwd()
        if "/Repos/.internal" not in cwd:
            print("[ENV_SETUP] Repositorio Workspace normal, no se requiere refresh Git.")
            return

        repo_root = detect_repo_root()
        print(f"[ENV_SETUP] Repositorio interno detectado: {repo_root}")
        print("[ENV_SETUP] Intentando actualizar desde GitHub (origin/main)...")

        # Ejecutar fetch/reset (no rompe si no hay .git)
        subprocess.run(["git", "-C", repo_root, "fetch", "--all"], check=False)
        subprocess.run(["git", "-C", repo_root, "reset", "--hard", "origin/main"], check=False)

        print("[ENV_SETUP] Repositorio sincronizado correctamente con origin/main ✅")

    except Exception as e:
        print(f"[ENV_SETUP WARN] No se pudo refrescar el repo Git: {e}")

# ==========================================================
# DIAGNÓSTICO DEL COMMIT ACTIVO
# ==========================================================

def print_current_commit(repo_root: str):
    """
    Imprime el commit actual o la carpeta .internal activa.
    """
    try:
        # Intentar detectar HEAD (solo si existe .git)
        git_head = os.path.join(repo_root, ".git", "HEAD")
        if os.path.exists(git_head):
            with open(git_head, "r") as f:
                ref = f.read().strip()
            print(f"[ENV_SETUP] HEAD → {ref}")
        else:
            # En Jobs Git (.internal) no existe .git, mostrar hash del snapshot
            internal_path = repo_root.split("/.internal/")[-1].split("/")[0]
            print(f"[ENV_SETUP] Snapshot interno activo (.internal hash): {internal_path}")
    except Exception as e:
        print(f"[ENV_SETUP WARN] No se pudo obtener commit actual: {e}")

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
    # Paso 1: detectar raíz
    repo_root = detect_repo_root()

    # Paso 2: refrescar repo si es modo Job Git
    refresh_repo_if_needed()

    # Paso 3: agregar repo raíz al sys.path
    if repo_root not in sys.path:
        sys.path.append(repo_root)
        print(f"[ENV_SETUP] Path raíz agregado a sys.path: {repo_root}")

    # Paso 4: inicializar Spark
    spark = get_or_create_spark()

    # Paso 5: agregar subcarpetas del proyecto
    added = auto_import_modules(repo_root)
    if added:
        print("[ENV_SETUP] Carpetas agregadas al sys.path:")
        for name, path in added:
            print(f"  - {name} → {path}")
    else:
        print("[ENV_SETUP] No se agregaron nuevas carpetas (ya estaban registradas).")

    # Paso 6: mostrar commit o snapshot activo
    print_current_commit(repo_root)

    print("[ENV_SETUP] Inicialización completa (Smart Mode + AutoRefresh + Diagnostics).")

except Exception as e:
    print(f"[ENV_SETUP WARNING] Error durante setup inicial: {e}")
