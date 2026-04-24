# ==========================================================
# FUNCIONES UDV - CATALOGO_EQUIPOS
# Proyecto: Liga 1 Perú
# Autor: Oscar García Del Águila
# ==========================================================

from pyspark.sql.functions import col, current_timestamp, date_format
from utils_liga1 import cast_dataframe_schema

def carga_final(df, prm_cols, prm_schema):
    """
    Selecciona las columnas del YAML, agrega campos de control (fecha_carga, periododia)
    y castea/ordena según el schema definido.

    Args:
        df: DataFrame leído desde RAW.
        prm_cols: lista de columnas base (yaml_conf[entity]["cols"])
        prm_schema: dict con columnas finales y tipos (yaml_conf[entity]["schema"])
    """
    # Filtrar solo las columnas definidas que existan en el DataFrame
    cols_existentes = [c for c in prm_cols if c in df.columns]
    missing_cols = [c for c in prm_cols if c not in df.columns]

    if missing_cols:
        print(f"[WARN] Columnas faltantes en DF origen: {missing_cols}")

    df_sel = df.select([col(c) for c in cols_existentes])

    # Agregar columnas de control (sin withColumn)
    df_ctrl = df_sel.select(
        *[col(c) for c in df_sel.columns],
        current_timestamp().alias("fecha_carga"),
        date_format(current_timestamp(), "yyyyMMdd").alias("periododia")
    )

    # Aplicar casteo y orden según schema del YAML
    df_final = cast_dataframe_schema(df_ctrl, prm_schema)

    return df_final
