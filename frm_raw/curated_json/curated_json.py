# ==========================================================
# CURATED LÓGICO - JSON (versión sin UDF ni withColumn)
# Proyecto: Liga 1 Perú
# Autor: Oscar García Del Águila
# ==========================================================

from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, to_json, col, when

# ----------------------------------------------------------
# FUNCIONES AUXILIARES
# ----------------------------------------------------------
def agregar_campos_minimos(df: DataFrame, anio: str) -> DataFrame:
    """
    Agrega columnas base si no existen:
    - fuente
    - fecha_carga
    - temporada
    """
    columnas = [c.lower() for c in df.columns]
    exprs = [col(c) for c in df.columns]

    # Agregar campos faltantes como expresiones literales
    if "fuente" not in columnas:
        exprs.append(lit("desconocido").alias("fuente"))
    if "fecha_carga" not in columnas:
        exprs.append(lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).alias("fecha_carga"))
    if "temporada" not in columnas:
        exprs.append(lit(anio).alias("temporada"))

    return df.select(*exprs)


def convertir_campos_complejos(df: DataFrame) -> DataFrame:
    """
    Convierte campos tipo array o struct en JSON string para evitar errores
    al escribir en Parquet y asegurar estructura estable.
    """
    tipos = dict(df.dtypes)
    exprs = []

    for c in df.columns:
        tipo = tipos[c]
        if tipo.startswith("array") or tipo.startswith("struct"):
            exprs.append(to_json(col(c)).alias(c))
        else:
            exprs.append(col(c))

    return df.select(*exprs)


# ----------------------------------------------------------
# FUNCIÓN PRINCIPAL
# ----------------------------------------------------------
def procesar_curated_json(df: DataFrame, anio: str) -> DataFrame:
    """
    Realiza transformaciones mínimas sobre un DataFrame JSON:
    - Agrega campos base (fuente, fecha_carga, temporada)
    - Convierte columnas complejas a JSON string
    Retorna un DataFrame listo para guardar.
    """
    print("===== INICIO PROCESAMIENTO CURATED JSON =====")

    df_proc = agregar_campos_minimos(df, anio)
    df_final = convertir_campos_complejos(df_proc)

    print("===== FIN PROCESAMIENTO CURATED JSON =====")
    return df_final
