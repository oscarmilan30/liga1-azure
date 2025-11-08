# ==========================================================
# CURATED LÓGICO - JSON (versión sin UDF)
# Proyecto: Liga 1 Perú
# Autor: Oscar García Del Águila
# ==========================================================
import re, unicodedata
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lower, trim, regexp_replace, when, lit

# ----------------------------------------------------------
# NORMALIZACIÓN DE TEXTO
# ----------------------------------------------------------
def normalizar_texto_expr(c):
    """
    Expresión Spark para limpieza básica de texto:
    - Minúsculas
    - Sin tildes
    - Espacios múltiples → uno solo
    - Valores vacíos o no válidos → NULL
    """
    expr = lower(trim(col(c)))
    expr = regexp_replace(expr, "[áÁ]", "a")
    expr = regexp_replace(expr, "[éÉ]", "e")
    expr = regexp_replace(expr, "[íÍ]", "i")
    expr = regexp_replace(expr, "[óÓ]", "o")
    expr = regexp_replace(expr, "[úÚ]", "u")
    expr = regexp_replace(expr, "\\s+", " ")
    expr = when(expr.isin("", "-", "n/a", "none"), lit(None)).otherwise(expr)
    return expr.alias(c)

# ----------------------------------------------------------
# LIMPIEZA Y CURATED SPARK NATIVO
# ----------------------------------------------------------
def procesar_curated_json(df: DataFrame) -> DataFrame:
    """
    Limpieza universal JSON:
    - Limpia texto, fechas, %, valores numéricos
    - Compatible con Spark Connect (sin UDF)
    - Sin pandas / sin withColumn
    """
    columnas = df.columns
    exprs = []

    for c in columnas:
        expr = normalizar_texto_expr(c)

        # --- Fechas dd/mm/yyyy o dd-mm-yyyy ---
        expr = when(
            regexp_replace(expr, "\\s", "").rlike(r"^\\d{1,2}[-/]\\d{1,2}[-/]\\d{2,4}$"),
            regexp_replace(expr, r"(\\d{1,2})[-/](\\d{1,2})[-/](\\d{2,4})", "\\3-\\2-\\1")
        ).otherwise(expr)

        # --- Mes textual (ej. abril 2025) ---
        meses = {
            "enero": "01", "febrero": "02", "marzo": "03", "abril": "04",
            "mayo": "05", "junio": "06", "julio": "07", "agosto": "08",
            "septiembre": "09", "setiembre": "09", "octubre": "10",
            "noviembre": "11", "diciembre": "12"
        }
        for mes, num in meses.items():
            expr = when(
                expr.contains(mes),
                regexp_replace(expr, f".*{mes}.*(\\d{{4}})", f"\\1-{num}-01")
            ).otherwise(expr)

        # --- Porcentajes '55%' → 0.55 ---
        expr = when(
            regexp_replace(expr, "\\s", "").rlike(r"^\\d+%$"),
            (regexp_replace(expr, "%", "").cast("double") / 100)
        ).otherwise(expr)

        # --- Diferenciales '+25', '-10' ---
        expr = when(
            regexp_replace(expr, "\\s", "").rlike(r"^[+-]?\\d+$"),
            expr.cast("int")
        ).otherwise(expr)

        # --- Rangos '32-7' → "32-7" (por ahora sin dividir en columnas) ---
        expr = when(
            regexp_replace(expr, "\\s", "").rlike(r"^\\d+-\\d+$"),
            expr
        ).otherwise(expr)

        # --- Números simples ---
        expr = when(
            regexp_replace(expr, "\\s", "").rlike(r"^-?\\d+(\\.\\d+)?$"),
            expr.cast("double")
        ).otherwise(expr)

        exprs.append(expr.alias(c))

    df_final = df.select(*exprs)
    return df_final
