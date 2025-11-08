# ==========================================================
# UNIFICADOR HISTÓRICO (1FL)
# Proyecto: Liga 1 Perú
# Autor: Oscar García Del Águila
# ==========================================================

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import to_json
from util.utils_liga1 import get_dbutils, get_abfss_path, read_parquet_adls, write_parquet_adls

# ----------------------------------------------------------
# FUNCIÓN: obtener rutas válidas
# ----------------------------------------------------------
def obtener_rutas_stg(dbutils, base_path: str, start_year: int, end_year: int):
    """
    Retorna las rutas stg/{año}/curated que contengan archivos Parquet válidos.
    """
    rutas = []
    for year in range(start_year, end_year):
        ruta = f"{base_path}/stg/{year}/curated"
        try:
            archivos = dbutils.fs.ls(ruta)
            if any(f.name.endswith(".parquet") for f in archivos):
                rutas.append(ruta)
        except Exception:
            print(f"Carpeta no encontrada o vacía: {ruta}")
    return rutas

# ----------------------------------------------------------
# FUNCIÓN: unificar múltiples DataFrames
# ----------------------------------------------------------
def unificar_dataframes(spark: SparkSession, rutas: list) -> DataFrame:
    """
    Une múltiples DataFrames parquet permitiendo diferencias de columnas.
    Convierte arrays/structs en JSON string para compatibilidad.
    """
    df_final = None
    columnas_ref = set()

    for ruta in rutas:
        try:
            print(f"Leyendo datos desde: {ruta}")
            df = read_parquet_adls(spark, get_abfss_path(ruta))

            # Normalizar columnas complejas
            for c in df.columns:
                tipo = dict(df.dtypes).get(c, "")
                if tipo.startswith("array") or tipo.startswith("struct"):
                    df = df.selectExpr(*[f"to_json({c}) as {c}" if x == c else x for x in df.columns])

            columnas_actuales = set(df.columns)
            columnas_ref |= columnas_actuales

            if df_final is None:
                df_final = df
            else:
                # Alinear columnas faltantes
                faltantes_actual = list(columnas_ref - columnas_actuales)
                if faltantes_actual:
                    df = df.selectExpr(*df.columns, *[f"NULL as {c}" for c in faltantes_actual])

                faltantes_final = list(columnas_ref - set(df_final.columns))
                if faltantes_final:
                    df_final = df_final.selectExpr(*df_final.columns, *[f"NULL as {c}" for c in faltantes_final])

                df_final = df_final.unionByName(df, allowMissingColumns=True)

        except AnalysisException as e:
            print(f"Error de análisis leyendo {ruta}: {str(e)}")
        except Exception as e:
            import traceback
            print(f"Error general en {ruta}: {traceback.format_exc()}")

    if df_final is None:
        raise Exception("No se pudo generar el DataFrame consolidado.")
    
    print(f"Unificación completada")
    return df_final

# ----------------------------------------------------------
# FUNCIÓN PRINCIPAL: procesar unificación histórica
# ----------------------------------------------------------
def procesar_unificacion_1FL(spark: SparkSession, capa_raw: str, rutaBase: str, nombre_archivo: str,
                             start_year: int, end_year: int):
    """
    Unifica todos los Parquets 'curated' anuales en una sola carpeta 1FL/data.
    """
    dbutils = get_dbutils()
    base_path = f"{capa_raw}/{rutaBase}/{nombre_archivo}"
    ruta_abfss_base = get_abfss_path(base_path)

    print("===============================================")
    print("UNIFICADOR HISTÓRICO DE PARQUETS (1FL)")
    print("===============================================")
    print(f"Entidad        : {nombre_archivo}")
    print(f"Años procesados: {start_year} - {end_year}")
    print(f"Ruta base RAW  : {ruta_abfss_base}")
    print("===============================================")

    rutas_stg = obtener_rutas_stg(dbutils, base_path, start_year, end_year)

    if not rutas_stg:
        raise Exception("No se encontraron carpetas stg/{año}/curated para unificar.")

    print(f"Carpetas detectadas: {len(rutas_stg)}")
    for r in rutas_stg:
        print(f" - {r}")

    df_final = unificar_dataframes(spark, rutas_stg)

    # Guardar resultado final
    output_path = f"{base_path}/1FL/data"
    print("===============================================")
    print(f"Guardando consolidado final en carpeta: {output_path}")
    print("===============================================")

    write_parquet_adls(df_final, get_abfss_path(output_path))

    print(f"Consolidado 1FL guardado con éxito ({df_final.count()} registros).")
