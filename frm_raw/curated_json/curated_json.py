# ==========================================================
# CURATED LÓGICO - JSON
# Proyecto: Liga 1 Perú
# Autor: Oscar García Del Águila
# ==========================================================
import re
import json
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_json, struct, udf, from_json
from pyspark.sql.types import StringType

# ----------------------------------------------------------
# LIMPIEZA DE VALORES
# ----------------------------------------------------------
def limpiar_valor(valor):
    """
    Limpieza semántica y normalización sin romper jerarquía del JSON.
    - Normaliza tildes y minúsculas
    - Elimina textos vacíos, nulos, '-'
    - Convierte '30 (55%)' → {"valor":30, "porc":0.55}
    - Convierte '55%' → 0.55
    - Convierte strings numéricos → int o float
    - Normaliza fechas como '13-04-2025' o 'abril 2025' → '2025-04-13' / '2025-04-01'
    """
    if valor is None:
        return None

    if isinstance(valor, str):
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

        # Fechas con formato dd-mm-yyyy o dd/mm/yyyy
        match = re.match(r"(\d{1,2})[-/](\d{1,2})[-/](\d{2,4})", valor)
        if match:
            d, m, y = match.groups()
            y = int(y) + 2000 if len(y) == 2 else int(y)
            return f"{y:04d}-{int(m):02d}-{int(d):02d}"

        # Fechas tipo "abril 2025", "mar 2024"
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

        # Expresión tipo '30 (55 %)'
        match = re.match(r"(\d+)\s*\((\d+)\s*%\)", valor)
        if match:
            v, p = match.groups()
            return {"valor": int(v), "porc": round(int(p)/100, 2)}

        # Expresión tipo '55%'
        match = re.match(r"(\d+)\s*%", valor)
        if match:
            return round(int(match.group(1))/100, 2)

        # Numérico plano
        if re.match(r"^-?\d+(\.\d+)?$", valor):
            return float(valor) if "." in valor else int(valor)

        return valor

    elif isinstance(valor, dict):
        return {k: limpiar_valor(v) for k, v in valor.items()}
    elif isinstance(valor, list):
        return [limpiar_valor(v) for v in valor]
    return valor


# ----------------------------------------------------------
# PROCESAMIENTO CURATED SIN WITHCOLUMN
# ----------------------------------------------------------
def procesar_curated_json(df: DataFrame) -> DataFrame:
    """
    Limpieza general aplicando limpiar_valor() de forma global:
    - Compacta la fila completa como JSON
    - Aplica limpiar_valor recursivo (a nivel de fila)
    - Reconstruye el DataFrame limpio sin perder jerarquía
    """
    # Definir UDF (una sola ejecución por fila)
    limpiar_json_udf = udf(
        lambda row: json.dumps(limpiar_valor(json.loads(row))) if row else None,
        StringType()
    )

    # Compactar fila a JSON y limpiar globalmente (sin withColumn)
    df_json = df.select(to_json(struct([col(c) for c in df.columns])).alias("json_data"))
    df_json_limpio = df_json.select(limpiar_json_udf(col("json_data")).alias("json_data"))

    # Reconstruir el DataFrame original con los mismos campos
    df_final = df_json_limpio.select(from_json(col("json_data"), "map<string,string>").alias("data")).select("data.*")

    return df_final
