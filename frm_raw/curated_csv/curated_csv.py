# ==========================================================
# CURATED LÓGICO - CSV
# Proyecto: Liga 1 Perú
# Autor: Oscar García Del Águila
# ==========================================================

import re
import unicodedata
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lower, trim, regexp_replace, when, lit, translate


# ----------------------------------------------------------
# UTILIDADES
# ----------------------------------------------------------
def limpiar_texto(valor: str):
    if valor is None:
        return None
    valor = valor.strip().lower()
    valor = unicodedata.normalize("NFD", valor)
    valor = valor.encode("ascii", "ignore").decode("utf-8")
    valor = re.sub(r"\s+", " ", valor)
    if valor in ["", "-", "n/a", "none"]:
        return None
    return valor


def normalizar_fecha(valor: str):
    if not valor or not isinstance(valor, str):
        return valor
    valor = limpiar_texto(valor)

    # dd/mm/yyyy o dd-mm-yyyy
    match = re.match(r"(\d{1,2})[-/](\d{1,2})[-/](\d{2,4})", valor)
    if match:
        d, m, y = match.groups()
        y = int(y) + 2000 if len(y) == 2 else int(y)
        return f"{y:04d}-{int(m):02d}-{int(d):02d}"

    # mes año
    meses = {
        "enero": 1, "febrero": 2, "marzo": 3, "abril": 4,
        "mayo": 5, "junio": 6, "julio": 7, "agosto": 8,
        "septiembre": 9, "setiembre": 9, "octubre": 10,
        "noviembre": 11, "diciembre": 12
    }
    for nombre, mes in meses.items():
        if nombre in valor:
            match = re.search(r"(\d{4})", valor)
            anio = int(match.group(1)) if match else datetime.now().year
            return f"{anio:04d}-{mes:02d}-01"

    return valor


def limpiar_numero(valor: str):
    if valor is None:
        return None
    valor = limpiar_texto(str(valor))

    match = re.match(r"(\d+)\s*\((\d+)\s*%\)", valor)
    if match:
        v, _ = match.groups()
        return int(v)

    match = re.match(r"(\d+)\s*%", valor)
    if match:
        return int(match.group(1))

    if re.match(r"^-?\d+(\.\d+)?$", valor):
        return float(valor) if "." in valor else int(valor)

    return None
# ----------------------------------------------------------
# CURATED CSV (sin pandas, sin withColumn)
# ----------------------------------------------------------
def procesar_curated_csv(df: DataFrame) -> DataFrame:
    """
    Limpieza genérica CSV:
    - Normaliza encabezados
    - Normaliza texto
    - NO modifica el formato de las columnas que contienen 'fecha' en el nombre
    """

    # Normalizar encabezados
    columnas = [
        unicodedata.normalize("NFD", c)
        .encode("ascii", "ignore")
        .decode("utf-8")
        .lower()
        .replace(" ", "_")
        for c in df.columns
    ]
    df = df.toDF(*columnas)

    columnas_limpias = []
    for c in df.columns:
        c_lower = c.lower()

        # 🔒 Columnas de fecha: solo TRIM, nada más
        if "fecha" in c_lower:
            expr_col = trim(col(c))
        else:
            # 1) Base: lower + trim
            expr_col = lower(trim(col(c)))

            # 2) Normalizar tildes
            expr_col = translate(expr_col, "áéíóúÁÉÍÓÚ", "aeiouaeiou")

            # 3) Quitar espacios repetidos
            expr_col = regexp_replace(expr_col, r"\s+", " ")

            # 4) Porcentajes → quitar símbolo %
            if any(x in c_lower for x in ["porc", "porcentaje", "ratio", "ppp"]):
                expr_col = regexp_replace(expr_col, "%", "")

        # 5) Nulos vacíos
        expr_col = when(
            expr_col.isin("", "-", "n/a", "none"),
            lit(None)
        ).otherwise(expr_col).alias(c)

        columnas_limpias.append(expr_col)

    df_limpio = df.select(*columnas_limpias)
    return df_limpio