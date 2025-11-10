# ==========================================================
# UTILITARIOS
# Proyecto: Liga 1 Perú
# Autor: Oscar García Del Águila
# ==========================================================

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, explode_outer, when, lit, expr, trim, regexp_replace, instr
from pyspark.sql.types import ArrayType, MapType, StringType
import json
from pyspark.sql.window import Window
from pyspark.sql import Row
import gc
import time


# ==========================================================
# FUNCIONES DE UTILIDAD GENERAL
# ==========================================================

def get_dbutils():
    """
    Obtiene la instancia de dbutils de forma segura.
    """
    try:
        spark = SparkSession.builder.getOrCreate()
        dbutils = spark._jvm.com.databricks.backend.daemon.dbutils.DBUtilsHolder.getDBUtils()
        return dbutils
    except:
        try:
            import IPython
            return IPython.get_ipython().user_ns["dbutils"]
        except:
            raise Exception("No se pudo obtener dbutils")


# ==========================================================
# ADLS (Azure Data Lake Storage)
# ==========================================================

def setup_adls():
    """
    Configuración rápida de ADLS.
    """
    dbutils = get_dbutils()
    try:
        # Carga de secretos desde Key Vault
        adls_account_name = dbutils.secrets.get(scope="secretliga1", key="storageaccount")
        client_id = dbutils.secrets.get(scope="secretliga1", key="clientid")
        client_secret = dbutils.secrets.get(scope="secretliga1", key="secretidt")
        tenant_id = dbutils.secrets.get(scope="secretliga1", key="tenantidt")

        spark = SparkSession.builder.getOrCreate()

        # Configuración de autenticación OAuth
        spark.conf.set(f"fs.azure.account.auth.type.{adls_account_name}.dfs.core.windows.net", "OAuth")
        spark.conf.set(f"fs.azure.account.oauth.provider.type.{adls_account_name}.dfs.core.windows.net",
                       "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
        spark.conf.set(f"fs.azure.account.oauth2.client.id.{adls_account_name}.dfs.core.windows.net", client_id)
        spark.conf.set(f"fs.azure.account.oauth2.client.secret.{adls_account_name}.dfs.core.windows.net", client_secret)
        spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{adls_account_name}.dfs.core.windows.net",
                       f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token")

        print(f"Autenticación ADLS configurada para: {adls_account_name}")

    except Exception as e:
        print(f"Error configurando ADLS: {e}")


def get_abfss_path(carpeta=""):
    """
    Retorna la ruta ABFSS completa.
    """
    dbutils = get_dbutils()
    try:
        container_name = dbutils.secrets.get(scope="secretliga1", key="filesystemname")
        adls_account_name = dbutils.secrets.get(scope="secretliga1", key="storageaccount")

        base_path = f"abfss://{container_name}@{adls_account_name}.dfs.core.windows.net/"
        return base_path + f"{carpeta}" if carpeta else base_path

    except Exception as e:
        print(f"Error obteniendo ruta ABFSS: {e}")
        return None


def test_conexion_adls():
    """
    Test rápido de conexión a ADLS.
    """
    try:
        ruta_raiz = get_abfss_path()
        if ruta_raiz:
            dbutils = get_dbutils()
            archivos = dbutils.fs.ls(ruta_raiz)
            print(f"Conexión exitosa. Carpetas encontradas: {len(archivos)}")
            return True
        else:
            print("No se pudo obtener la ruta base.")
            return False
    except Exception as e:
        print(f"Error en conexión ADLS: {e}")
        return False

# ==========================================================
# LECTURA DE ARCHIVOS ADLS
# ==========================================================
def read_json_adls(spark, path: str):
    print(f"Leyendo JSON desde {path}")
    return spark.read.option("multiLine", True).json(path)


def read_parquet_adls(spark, path: str):
    print(f"Leyendo Parquet desde {path}")
    return spark.read.parquet(path)


def write_parquet_adls(df, path: str, mode="overwrite"):
    print(f"Escribiendo Parquet en {path}")
    df.write.mode(mode).parquet(path)
    print("Archivo guardado correctamente.")


# ==========================================================
# CONEXIÓN A AZURE SQL DATABASE
# ==========================================================

def get_sql_connection():
    """
    Devuelve la configuración JDBC y credenciales del Azure SQL Database desde Key Vault.
    """
    dbutils = get_dbutils()
    try:
        sql_server = dbutils.secrets.get(scope="secretliga1", key="kv-sql-sqlserver")
        sql_db = dbutils.secrets.get(scope="secretliga1", key="kv-sql-sqldb")
        sql_user = dbutils.secrets.get(scope="secretliga1", key="kv-sql-sqluser")
        sql_pass = dbutils.secrets.get(scope="secretliga1", key="kv-sql-password")

        jdbc_url = (
            f"jdbc:sqlserver://{sql_server}:1433;"
            f"database={sql_db};"
            "encrypt=true;trustServerCertificate=false;"
            "hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
        )

        props = {
            "user": sql_user,
            "password": sql_pass,
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        }

        print(f"Conexión JDBC lista para BD: {sql_db}")
        return jdbc_url, props, sql_server, sql_db, sql_user, sql_pass

    except Exception as e:
        print(f"Error obteniendo conexión SQL: {e}")
        return None, None, None, None, None, None


# ==========================================================
# LECTURA DE ENTIDAD DESDE PATHS (FLG_UDV = 'N')
# ==========================================================

def get_entity_data(entidad: str):
    """
    Lee todos los paths con flg_udv='N' para una entidad específica,
    combina los Parquets en un solo DataFrame,
    elimina duplicados por temporada y actualiza flg_udv='S' solo para los IDs procesados.
    """
    spark = SparkSession.getActiveSession()
    jdbc_url, props, sql_server, sql_db, sql_user, sql_pass = get_sql_connection()

    if not jdbc_url:
        raise Exception("No se pudo establecer conexión JDBC con Azure SQL Database.")

    # Leer paths pendientes por entidad
    query = f"""
    (
        SELECT id, entidad, modoejecucion, rutaraw, flg_udv
        FROM dbo.tbl_paths
        WHERE flg_udv = 'N' AND entidad = '{entidad}'
    ) AS t
    """
    df_paths = spark.read.jdbc(url=jdbc_url, table=query, properties=props)
    total_paths = df_paths.count()

    if total_paths == 0:
        print(f"No hay rutas pendientes para la entidad '{entidad}'.")
        return None

    print(f"Se encontraron {total_paths} rutas pendientes para '{entidad}'.")

    # Lectura de Parquets
    df_union = None
    for row in df_paths.toLocalIterator():
        path_rel = row["rutaraw"]
        path = get_abfss_path(path_rel)
        print(f"Leyendo {path}")
        try:
            df_temp = spark.read.parquet(path)
            if df_union is None:
                df_union = df_temp
            else:
                df_union = df_union.unionByName(df_temp, allowMissingColumns=True)
        except Exception as e:
            print(f"Error leyendo {path}: {e}")

    if df_union is None:
        print(f"No se pudo leer ninguna ruta válida para '{entidad}'.")
        return None

    # Eliminar duplicados por temporada
    lower_cols = [c.lower() for c in df_union.columns]
    cols = [col(c) for c in df_union.columns]

    if "temporada" in lower_cols:
        print("Eliminando duplicados por temporada (última carga).")
        w = Window.partitionBy("temporada").orderBy(desc("fecha_carga"))
        df_union = (
            df_union
            .select(*cols, row_number().over(w).alias("row_num"))
            .filter(col("row_num") == 1)
            .drop("row_num")
        )
    else:
        print("Eliminando duplicados globales.")
        df_union = df_union.dropDuplicates()

    # Actualización por ID (via trigger SQL)
    try:
        id_rows = [Row(id=int(r.id)) for r in df_paths.collect()]
        df_trigger = spark.createDataFrame(id_rows)
        df_trigger.write.jdbc(
            url=jdbc_url,
            table="dbo.tbl_flag_update_queue_id",
            mode="append",
            properties=props
        )
        print(f"{len(id_rows)} IDs enviados a tbl_flag_update_queue_id (actualización automática en tbl_paths).")
    except Exception as e:
        print(f"Error al enviar IDs al trigger de actualización: {e}")

# ==========================================================
# TRANSFORMACION JSON
# ==========================================================

def parsear_json(df, campo_json="data"):
    campo_limpio = f"{campo_json}_limpio"
    return df.select(
        *[col(c) for c in df.columns],
        trim(regexp_replace(col(campo_json), r"^\d+\s+", "")).alias(campo_limpio)
    )


def convertir_array(df, campo_limpio):
    return df.select(
        *[col(c) for c in df.columns],
        from_json(
            when(col(campo_limpio).startswith("["), col(campo_limpio))
            .otherwise(expr(f"concat('[', {campo_limpio}, ']')")),
            ArrayType(MapType(StringType(), StringType()))
        ).alias("json_array")
    )


def extraer_claves_subclaves(df_exploded, campo="json_item", limite=50):
    claves, claves_anidadas = [], []

    rows = df_exploded.select(campo).limit(limite).take(limite)
    for row in rows:
        item = row[campo]
        if item:
            for key, value in item.items():
                if isinstance(value, str):
                    try:
                        parsed = json.loads(value)
                        if isinstance(parsed, list) and all(isinstance(x, dict) for x in parsed):
                            claves_anidadas.append(key)
                    except:
                        pass
                claves.append(key)
    return list(set(claves)), list(set(claves_anidadas))


def construir_columnas(df_exploded, claves, claves_anidadas, prefijo="json", limite=50):
    columnas = {}
    columnas_anidadas = {}

    for key in claves:
        if key not in claves_anidadas:
            alias = f"{prefijo}_{key.lower()}"
            columnas[alias] = col("json_item").getItem(key).alias(alias)

    for key in claves_anidadas:
        alias_base = f"{prefijo}_{key.lower()}"
        campo = col("json_item").getItem(key)
        campo_array = from_json(
            when(campo.startswith("[").cast("boolean") & (instr(campo, "{") > 0), campo)
            .otherwise(lit(None)),
            ArrayType(MapType(StringType(), StringType()))
        )

        df_anidado = df_exploded.select(explode_outer(campo_array).alias("submap"))
        subkeys = set()
        for row in df_anidado.select("submap").limit(limite).take(limite):
            submap = row["submap"]
            if isinstance(submap, dict):
                subkeys.update(submap.keys())

        for subkey in subkeys:
            alias = f"{alias_base}_{subkey.lower()}"
            columnas_anidadas[alias] = campo_array.getItem(0).getItem(subkey).alias(alias)

    return list(columnas.values()) + list(columnas_anidadas.values())

def procesar_json_generico(df, campo="data", limite=50):
    """
    Procesa cualquier JSON almacenado como string (sin esquema fijo).
    Infere claves y subclaves dinámicamente y retorna un DataFrame expandido.
    """
    if campo not in df.columns:
        raise ValueError(f"El campo '{campo}' no existe en el DataFrame.")
    if df.rdd.isEmpty():
        raise ValueError("El DataFrame de entrada está vacío.")

    print("===============================================")
    print("INICIO CURATED JSON GENÉRICO")
    print(f"Campo JSON: {campo}")
    print("===============================================")

    # Limpieza y conversión
    df_limpio = parsear_json(df, campo)
    campo_limpio = f"{campo}_limpio"
    df_array = convertir_array(df_limpio, campo_limpio)

    # Explosión del JSON
    columnas_base = [col(c) for c in df_array.columns if c not in [campo_limpio, "json_array"]]
    df_exploded = df_array.select(
        *columnas_base,
        explode_outer(col("json_array")).alias("json_item")
    )

    # Claves dinámicas
    claves, claves_anidadas = extraer_claves_subclaves(df_exploded, campo="json_item", limite=limite)
    columnas_dinamicas = construir_columnas(df_exploded, claves, claves_anidadas, prefijo="json", limite=limite)

    # Selección final
    df_final = df_exploded.select(
        *columnas_base,
        *columnas_dinamicas
    )

    print(f"[INFO] Columnas finales generadas: {len(df_final.columns)}")
    print("JSON genérico completado correctamente.")
    print("===============================================")

    return df_final

def generar_udv_json(entidad: str, campo_json="data", limite=50):
    df = get_entity_data(entidad)
    if not df:
        print(f"No hay registros para {entidad}")
        return None

    df_proc = procesar_json_generico(df, campo_json, limite)
    print(f"UDV generado para {entidad}")
    return df_proc



















    print(f"Entidad '{entidad}' leída y consolidada correctamente.")
    return df_union