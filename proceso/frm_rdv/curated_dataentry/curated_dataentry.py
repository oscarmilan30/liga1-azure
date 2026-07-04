# ==========================================================
# CURATED LÓGICO - CSV
# Proyecto: Liga 1 Perú
# Autor: Oscar García Del Águila
# ==========================================================

import re
import unicodedata
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lower, trim, regexp_replace, when, lit

def procesar_curated_dataentry(df: DataFrame) -> DataFrame:
    """
    Limpieza genérica para CSVs ingresados manualmente (data entry).
    Aplica normalización estándar para que el output sea compatible con el
    esquema esperado por los notebooks UDV que consumen estos catálogos.

    Transformaciones aplicadas:
    - Encabezados: minúsculas, sin tildes, espacios reemplazados por '_'
    - Valores: trim, minúsculas, normalización de vocales con tilde
    - Columnas de fecha (nombre contiene 'fecha'): dd/mm/yyyy → yyyy-mm-dd
    - Nulos: '', '-', 'n/a', 'none' se convierten en NULL

    Args:
        df: DataFrame con datos crudos del archivo CSV de data entry.

    Returns:
        DataFrame con columnas y valores normalizados listos para capa RDV.
    """

    # Normalizar encabezados: quitar tildes vía NFD encoding, pasar a minúsculas
    columnas = [
        unicodedata.normalize("NFD", c)
        .encode("ascii", "ignore")
        .decode("utf-8")
        .lower()
        .replace(" ", "_")
        for c in df.columns
    ]
    df = df.toDF(*columnas)

    # Limpieza de valores columna por columna
    columnas_limpias = []
    for c in df.columns:
        c_lower = c.lower()
        expr_col = lower(trim(col(c)))

        # Normalizar vocales con tilde (variantes aguda, grave, diéresis, circunflejo)
        expr_col = regexp_replace(expr_col, "[áàäâ]", "a")
        expr_col = regexp_replace(expr_col, "[éèëê]", "e")
        expr_col = regexp_replace(expr_col, "[íìïî]", "i")
        expr_col = regexp_replace(expr_col, "[óòöô]", "o")
        expr_col = regexp_replace(expr_col, "[úùüû]", "u")

        if "fecha" in c_lower:
            # Convertir formato dd/mm/yyyy a yyyy-mm-dd (ISO 8601)
            expr_col = regexp_replace(expr_col, r"(\d{1,2})/(\d{1,2})/(\d{4})", r"\3-\2-\1")

        expr_col = when(expr_col.isin("", "-", "n/a", "none"), lit(None)).otherwise(expr_col).alias(c)
        columnas_limpias.append(expr_col)

    df_limpio = df.select(*columnas_limpias)
    
    return df_limpio
