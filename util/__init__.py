import os, sys

try:
    # Determina la ruta raíz del proyecto (sube un nivel)
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

    # Agrega al sys.path si no está
    if root not in sys.path:
        sys.path.append(root)

except Exception as e:
    print(f"[UTIL WARNING] No se pudo configurar sys.path dinámicamente: {e}")
