# ==========================================================
# BOOTSTRAP CONFIG - Proyecto: Liga 1 Perú (Databricks + ADF)
# Autor: Oscar García Del Águila
# ==========================================================
"""
Este módulo garantiza que los notebooks y scripts del proyecto
puedan importar correctamente los paquetes internos (ej. util/, frm_raw/),
sin importar si se ejecutan desde:
    - Workspace personal (Users/)
    - Repositorios Git (Repos/)
    - Integraciones ADF / Jobs
    - Entornos locales (VSCode, spark-submit)

Debe ser importado al inicio de cada notebook:
    import bootstrap
"""

import sys
import os

try:
    # Detecta el directorio actual del notebook
    notebook_dir = os.getcwd()

    # Posibles ubicaciones raíz del proyecto
    root_candidates = [
        notebook_dir,
        os.path.abspath(os.path.join(notebook_dir, "..")),
        os.path.abspath(os.path.join(notebook_dir, "../..")),
        "/Workspace/Users/garciadoscar1994@outlook.com/liga1-azure",
        "/Workspace/Repos/garciadoscar1994@outlook.com/liga1-azure",
        os.path.expanduser("~/liga1-azure")  # fallback local
    ]

    # Busca la carpeta raíz que contiene 'util'
    root_found = None
    for candidate in root_candidates:
        if os.path.exists(os.path.join(candidate, "util")):
            root_found = candidate
            break

    # Agrega al sys.path si no está ya presente
    if root_found:
        if root_found not in sys.path:
            sys.path.append(root_found)
        print(f"[BOOTSTRAP] Path raíz agregado: {root_found}")
    else:
        print("[BOOTSTRAP WARNING] No se encontró carpeta raíz con 'util'.")

except Exception as e:
    print(f"[BOOTSTRAP ERROR] No se pudo configurar el path: {e}")

# ==========================================================
# NOTAS DE USO
# ==========================================================
# Este bootstrap garantiza la carga correcta de módulos internos como:
#   from util.utils_liga1 import setup_adls
#   from frm_raw.curated_json.curated_json import procesar_curated_json
#
# No es necesario modificar sys.path manualmente en los notebooks.
# ==========================================================
