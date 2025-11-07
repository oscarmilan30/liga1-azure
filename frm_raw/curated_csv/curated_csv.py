# ==========================================================
# CURATED LÓGICO - CSV
# Proyecto: Liga 1 Perú
# Autor: Oscar García Del Águila
# ==========================================================
import re
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lower, regexp_replace, when, lit, trim

# ----------------------------------------------------------
# LIMPIEZA DE VALORES CSV
# ----------------------------------------------------------
def limpiar_texto(valor: str) -> str:
    """
    Normaliza tildes, espacios y caracteres especiales en texto.
    Convierte todo a minúsculas, elimina guiones y espacios duplicados.
    """
    if valor is None:
        return None
    valor = valor.strip().lower()
    valor = (
        valor.replace("á", "a")
             .replace("é", "e")
             .replace("í", "i")
             .replace("ó", "o")
             .replace("ú", "u")
    )
    if valor in ["", "-", "n/a", "none"]:
        return None
    return valor


def normalizar_fecha(valor: str):
    """
    Normaliza fechas de distintos formatos a YYYY-MM-DD.
    Soporta:
      - dd-mm-yyyy / dd/mm/yyyy
      - mes año ("abril 2025", "mar 24")
    """
    if not valor or not isinstance(valor, str):
        return valor

    valor = limpiar_texto(valor)

    # dd-mm-yyyy o dd/mm/yyyy
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
        "noviembre": 11, "diciembre": 12,
        "ene": 1, "feb": 2, "mar": 3, "abr": 4,
        "may": 5, "jun": 6, "jul": 7, "ago": 8,
        "sep": 9, "oct": 10, "nov": 11, "dic": 12
    }
    for nombre, mes in meses.items():
        if nombre in valor:
            match = re.search(r"(\d{4})", valor)
            anio = int(match.group(1)) if match else datetime.now().year
            return f"{anio:04d}-{mes:02d}-01"

    return valor


def limpiar_numero(valor: str):
    """
    Limpia valores numéricos tipo '55%', '30 (55%)', o vacíos.
    Devuelve int, float o None.
    """
    if valor is None:
        return None
    valor = limpiar_texto(str(valor))

    # '30 (55%)' → valor=30
    match = re.match(r"(\d+)\s*\((\d+)\s*%\)", valor)
    if match:
        v, _ = match.groups()
        return int(v)

    # '55%' → 55
    match = re.match(r"(\d+)\s*%", valor)
    if match:
        return int(match.group(1))

    # numérico plano
    if re.match(r"^-?\d+(\.\d+)?$", valor):
        return float(valor) if "." in valor else int(valor)

    return None


# ----------------------------------------------------------
# PROCESAMIENTO CURATED CSV SIN withColumn
# ----------------------------------------------------------
def procesar_curated_csv(df: DataFrame) -> DataFrame:
    """
    Limpieza genérica CSV:
    - Convierte a minúsculas y sin tildes
    - Campos vacíos → NULL
    - Nulos numéricos → 0
    - Normaliza fechas y elimina símbolos %
    (sin uso de withColumn ni funciones globales)
    """
    columnas = df.columns
    cols_limpios = []

    for c in columnas:
        tipo = df.schema[c].dataType.simpleString().lower()
        c_lower = c.lower()

        #STRING → limpiar tildes, vacíos, 'n/a', '-'
        if tipo == "string":
            expr_col = (
                when(
                    regexp_replace(lower(trim(col(c))), "[áéíóú]", "a").isin("", "n/a", "-", "none"),
                    lit(None)
                )
                .otherwise(
                    regexp_replace(lower(trim(col(c))), "[áéíóú]", "a")
                )
                .alias(c)
            )

        #NUMÉRICOS → reemplazar nulos por 0
        else:
            expr_col = when(col(c).isNull(), lit(0)).otherwise(col(c)).alias(c)

        cols_limpios.append(expr_col)

    # aplicar selección limpia
    df_limpio = df.select(*cols_limpios)

    #Correcciones adicionales sin withColumn
    expr_final = []
    for c in df_limpio.columns:
        c_lower = c.lower()

        # Normalización de columnas tipo fecha → remover % o limpiar formato textual
        if "fecha" in c_lower:
            expr_final.append(trim(col(c)).alias(c))

        # Limpieza de % o símbolos en campos porcentuales
        elif any(x in c_lower for x in ["porc", "porcentaje", "%", "ratio"]):
            expr_final.append(regexp_replace(trim(col(c)), "%", "").alias(c))

        else:
            expr_final.append(col(c))

    df_final = df_limpio.select(*expr_final)
    return df_final
