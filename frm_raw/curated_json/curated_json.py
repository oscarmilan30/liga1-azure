# ==========================================================
# CURATED LÓGICO - JSON (versión sin UDF)
# Proyecto: Liga 1 Perú
# Autor: Oscar García Del Águila
# ==========================================================

from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, to_json, colv

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
    df_tmp = df

    if "fuente" not in columnas:
        df_tmp = df_tmp.withColumn("fuente", lit("desconocido"))
    if "fecha_carga" not in columnas:
        df_tmp = df_tmp.withColumn("fecha_carga", lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    if "temporada" not in columnas:
        df_tmp = df_tmp.withColumn("temporada", lit(anio))

    return df_tmp


def convertir_campos_complejos(df: DataFrame) -> DataFrame:
    """
    Convierte campos tipo array o struct en JSON string para evitar errores
    en escritura Parquet y asegurar estructura estable.
    """
    tipos = dict(df.dtypes)
    for c, tipo in tipos.items():
        if tipo.startswith("array") or tipo.startswith("struct"):
            df = df.withColumn(c, to_json(col(c)))
    return df


# ----------------------------------------------------------
# FUNCIÓN PRINCIPAL
# ----------------------------------------------------------
def procesar_curated_json(df: DataFrame, anio: str) -> DataFrame:
    """
    Realiza validaciones y transformaciones mínimas sobre un DataFrame JSON:
    - Agrega campos base (fuente, fecha_carga, temporada)
    - Convierte columnas complejas a JSON string
    Retorna un nuevo DataFrame listo para guardar.
    """

    print("===== INICIO PROCESAMIENTO CURATED JSON =====")
    df_proc = agregar_campos_minimos(df, anio)
    df_final = convertir_campos_complejos(df_proc)
    print("===== FIN PROCESAMIENTO CURATED JSON =====")

    return df_final







