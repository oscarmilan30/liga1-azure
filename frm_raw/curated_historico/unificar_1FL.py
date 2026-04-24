# ==========================================================
# UNIFICADOR HISTÓRICO (1FL)
# Proyecto: Liga 1 Perú
# Autor: Oscar García Del Águila
# ==========================================================

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import to_json, col


# ----------------------------------------------------------
# FUNCIÓN: obtener rutas válidas
# ----------------------------------------------------------
def obtener_rutas_stg(dbutils, base_path: str, start_year: int, end_year: int, get_abfss_path):
    """
    Retorna las rutas stg/{año}/curated que contengan archivos Parquet válidos.
    Solo busca en esa estructura exacta.
    """
    rutas = []
    for year in range(start_year, end_year):  # el año final no se incluye
        ruta_relativa = f"{base_path}/stg/{year}/curated"
        ruta_abfss = get_abfss_path(ruta_relativa)

        try:
            archivos = dbutils.fs.ls(ruta_abfss)
            if any(f.name.endswith(".parquet") for f in archivos):
                rutas.append(ruta_abfss)
                print(f"Ruta válida detectada: {ruta_abfss}")
            else:
                print(f"Carpeta sin archivos Parquet: {ruta_abfss}")
        except Exception:
            print(f"Carpeta no encontrada o vacía: {ruta_abfss}")
    return rutas


# ----------------------------------------------------------
# FUNCIÓN: unificar múltiples DataFrames
# ----------------------------------------------------------
def unificar_dataframes(spark: SparkSession, rutas: list, read_parquet_adls) -> DataFrame:
    """
    Une múltiples DataFrames Parquet permitiendo diferencias de columnas.
    Convierte arrays/structs a JSON string para compatibilidad.
    """
    df_final = None
    columnas_ref = set()

    for ruta in rutas:
        try:
            print(f"Leyendo datos desde: {ruta}")
            df = read_parquet_adls(spark, ruta)

            # Normalizar columnas complejas
            exprs = []
            tipos = dict(df.dtypes)
            for c in df.columns:
                tipo = tipos.get(c, "")
                if tipo.startswith("array") or tipo.startswith("struct"):
                    exprs.append(to_json(col(c)).alias(c))
                else:
                    exprs.append(col(c))
            df = df.select(*exprs)

            # Alinear columnas entre diferentes años
            columnas_actuales = set(df.columns)
            columnas_ref |= columnas_actuales

            if df_final is None:
                df_final = df
            else:
                faltantes_actual = list(columnas_ref - columnas_actuales)
                if faltantes_actual:
                    df = df.select(*df.columns, *[f"NULL as {c}" for c in faltantes_actual])

                faltantes_final = list(columnas_ref - set(df_final.columns))
                if faltantes_final:
                    df_final = df_final.select(*df_final.columns, *[f"NULL as {c}" for c in faltantes_final])

                df_final = df_final.unionByName(df, allowMissingColumns=True)

        except AnalysisException as e:
            print(f"Error de análisis leyendo {ruta}: {str(e)}")
        except Exception as e:
            import traceback
            print(f"Error general en {ruta}:\n{traceback.format_exc()}")

    if df_final is None:
        raise Exception("No se pudo generar el DataFrame consolidado.")
    print("Unificación completada con éxito.")
    return df_final


# ----------------------------------------------------------
# FUNCIÓN PRINCIPAL
# ----------------------------------------------------------
def procesar_unificacion_1FL(
    spark: SparkSession,
    dbutils,
    capa_raw: str,
    rutaBase: str,
    nombre_archivo: str,
    start_year: int,
    end_year: int,
    read_parquet_adls,
    get_abfss_path
) -> DataFrame:
    """
    Unifica los Parquets 'curated' ubicados en stg/{año}/curated.
    No escribe en disco (lo hace el notebook).
    """
    base_path = f"{capa_raw}/{rutaBase}/{nombre_archivo}"

    print("===============================================")
    print("INICIO PROCESO UNIFICACIÓN 1FL")
    print("===============================================")
    print(f"Entidad: {nombre_archivo}")
    print(f"Rango de años: {start_year} - {end_year - 1}")
    print(f"Base path: {base_path}")
    print("===============================================")

    rutas_stg = obtener_rutas_stg(dbutils, base_path, start_year, end_year, get_abfss_path)
    if not rutas_stg:
        raise Exception("No se encontraron carpetas válidas para unificar.")

    print(f"Carpetas detectadas ({len(rutas_stg)}):")
    for r in rutas_stg:
        print(f" - {r}")

    df_final = unificar_dataframes(spark, rutas_stg, read_parquet_adls)

    print("===============================================")
    print("FIN DEL PROCESO UNIFICACIÓN 1FL")
    print("===============================================")
    return df_final
