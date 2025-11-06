import sys, os

try:
    # Detecta si se ejecuta desde subcarpeta (ej. frm_raw)
    notebook_dir = os.getcwd()
    root = os.path.abspath(os.path.join(notebook_dir, ".."))

    # Si no encuentra util/, sube otro nivel
    if not os.path.exists(os.path.join(root, "util")):
        root = os.path.abspath(os.path.join(root, ".."))

    if root not in sys.path:
        sys.path.append(root)

    print(f"[BOOTSTRAP] Path raíz agregado: {root}")

except Exception as e:
    print(f"[BOOTSTRAP WARNING] No se pudo configurar el path: {e}")
