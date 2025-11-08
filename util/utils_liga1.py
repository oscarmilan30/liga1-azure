# ==========================================================
# UTILITARIOS
# Proyecto: Liga 1 Perú
# Autor: Oscar García Del Águila
# ==========================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, desc
from pyspark.sql.window import Window
from pyspark.sql import Row
import gc
import time
from pyspark.sql import DataFrame


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


def write_parquet_adls_seguro(df: DataFrame, path: str, mode: str = "overwrite"):
    """
    Escritura segura optimizada para clústeres pequeños:
    - Reparte en 1 partición para evitar múltiples conexiones ADLS
    - Escribe temporalmente en disco local (/local_disk0)
    - Copia solo el archivo final al destino en ADLS
    - Libera memoria después de la escritura
    """
    from pyspark.sql import SparkSession
    dbutils = get_dbutils()
    spark = SparkSession.getActiveSession()

    temp_dir = f"/local_disk0/tmp/liga1_tmp_{int(time.time())}"
    print(f"[WRITE] Escribiendo temporalmente en: {temp_dir}")

    try:
        # Escribir localmente (1 partición)
        df.repartition(1).write.mode(mode).parquet(temp_dir)

        # Detectar el único parquet generado
        parquet_files = [f.path for f in dbutils.fs.ls(temp_dir) if f.path.endswith(".parquet")]
        if not parquet_files:
            raise Exception("No se generó ningún archivo Parquet temporal.")
        file_local = parquet_files[0]

        # Crear carpeta destino si no existe y copiar
        dbutils.fs.mkdirs(path)
        dbutils.fs.cp(file_local, path, True)
        print(f"[WRITE] Archivo movido correctamente a ADLS: {path}")

    except Exception as e:
        print(f"[WRITE ERROR] {e}")
        raise
    finally:
        # Limpieza de temporales y liberación de memoria
        try:
            dbutils.fs.rm(temp_dir, True)
        except Exception:
            pass
        try:
            df.unpersist(blocking=False)
        except Exception:
            pass
        gc.collect()
        print("[WRITE] Limpieza de memoria completada.")


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

    print(f"Entidad '{entidad}' leída y consolidada correctamente.")
    return df_union