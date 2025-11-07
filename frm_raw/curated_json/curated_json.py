# ==========================================================
# CURATED LÓGICO - JSON
# Proyecto: Liga 1 Perú
# Autor: Oscar García Del Águila
# ==========================================================

import re, json, unicodedata
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_json, struct, udf, from_json, when
from pyspark.sql.types import StringType, MapType

# ----------------------------------------------------------
# NORMALIZAR TEXTO
# ----------------------------------------------------------
def normalizar_texto(texto: str) -> str:
    if texto is None or not isinstance(texto, str):
        return texto
    texto = texto.strip().lower()
    texto = unicodedata.normalize("NFD", texto)
    texto = texto.encode("ascii", "ignore").decode("utf-8")
    texto = re.sub(r"\s+", " ", texto)
    return texto


# ----------------------------------------------------------
# LIMPIEZA RECURSIVA
# ----------------------------------------------------------
def limpiar_valor(valor):
    """
    Limpieza semántica recursiva sin aplanar estructuras:
    - Normaliza texto
    - Fechas → YYYY-MM-DD
    - Porcentajes, diferenciales, valores mixtos ("30 (55%)", "+25", "32-7")
    """
    if valor is None:
        return None

    if isinstance(valor, str):
        valor = normalizar_texto(valor)
        if valor in ["", "-", "n/a", "none"]:
            return None

        # dd/mm/yyyy o dd-mm-yyyy
        match = re.match(r"(\d{1,2})[-/](\d{1,2})[-/](\d{2,4})", valor)
        if match:
            d, m, y = match.groups()
            y = int(y) + 2000 if len(y) == 2 else int(y)
            return f"{y:04d}-{int(m):02d}-{int(d):02d}"

        # mes año
        meses = {
            "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
            "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9, "octubre": 10,
            "noviembre": 11, "diciembre": 12
        }
        for mes, num in meses.items():
            if mes in valor:
                match = re.search(r"(\d{4})", valor)
                anio = int(match.group(1)) if match else datetime.now().year
                return f"{anio:04d}-{num:02d}-01"

        # "30 (55%)"
        match = re.match(r"(\d+)\s*\((\d+)\s*%\)", valor)
        if match:
            v, p = match.groups()
            return {"valor": int(v), "porc": round(int(p)/100, 2)}

        # "55%"
        match = re.match(r"(\d+)\s*%", valor)
        if match:
            return round(int(match.group(1))/100, 2)

        # "32-7"
        match = re.match(r"^(\d+)\s*[-]\s*(\d+)$", valor)
        if match:
            a, b = match.groups()
            return {"goles_favor": int(a), "goles_contra": int(b)}

        # "+25" o "-10"
        match = re.match(r"^[+-]?\d+$", valor)
        if match:
            return int(valor)

        # Numérico
        if re.match(r"^-?\d+[.,]?\d*$", valor):
            valor = valor.replace(",", ".")
            return float(valor) if "." in valor else int(valor)

        return valor

    elif isinstance(valor, dict):
        return {normalizar_texto(k): limpiar_valor(v) for k, v in valor.items()}

    elif isinstance(valor, list):
        return [limpiar_valor(v) for v in valor]

    return valor


# ----------------------------------------------------------
# PROCESO PRINCIPAL
# ----------------------------------------------------------
def procesar_curated_json(df: DataFrame) -> DataFrame:
    """
    Limpieza fila a fila, sin pandas ni withColumn.
    """
    limpiar_json_udf = udf(
        lambda row: json.dumps(limpiar_valor(json.loads(row))) if row else None,
        StringType()
    )

    # Compactar fila
    df_json = df.select(to_json(struct([col(c) for c in df.columns])).alias("json_data"))
    df_json_limpio = df_json.select(limpiar_json_udf(col("json_data")).alias("json_data"))

    # Parseo genérico como map (para flexibilidad)
    df_reconstruido = df_json_limpio.select(
        from_json(col("json_data"), MapType(StringType(), StringType())).alias("data")
    )

    # Si "data" no es struct, mantener la columna
    df_final = df_reconstruido.select(when(col("data").isNotNull(), col("data")).alias("data"))

    return df_final
