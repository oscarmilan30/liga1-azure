# ==========================================================
# UTILITARIOS
# Proyecto: Liga 1 Perú
# Autor: Oscar García Del Águila
# ==========================================================
from env_setup import *
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.functions import col, from_json, explode_outer, when, lit, expr, trim, regexp_replace, instr, desc, row_number, to_date
from pyspark.sql.types import ArrayType, MapType, StringType
from pyspark.sql.window import Window
import json
import gc
import time
import yaml
import os
from datetime import datetime


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

def get_yaml_from_param(relative_yaml_path: str) -> dict:
    """
    Lee un archivo YAML usando una ruta relativa (por ejemplo '/frm_udv/conf/catalogo_equipos/catalogo_equipos.yml').

    - Usa get_workspace_path() para construir la ruta completa del Workspace.
    - Devuelve el contenido del YAML como un diccionario.
    """
    try:
        yaml_path = get_workspace_path(relative_yaml_path)
        if not os.path.exists(yaml_path):
            raise FileNotFoundError(f"No se encontró el archivo YAML en {yaml_path}")

        with open(yaml_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

        print(f"[OK] YAML leído correctamente desde: {yaml_path}")
        return config

    except Exception as e:
        raise Exception(f"Error en get_yaml_from_param: {e}")

def cast_dataframe_schema(df: DataFrame, schema: dict, date_format: str = "yyyy-MM-dd") -> DataFrame:
    """
    Castea y ordena las columnas de un DataFrame según el diccionario 'schema'.
    Selecciona solo las columnas definidas y en el mismo orden del schema.

    Args:
        df: DataFrame de entrada.
        schema: Diccionario {columna: tipo}.
        date_format: Formato para columnas tipo 'date'.

    Returns:
        DataFrame con columnas casteadas y ordenadas.
    """

    # Ajuste de política de parseo de fechas
    spark = df.sparkSession
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

    # Construcción ordenada del select
    select_exprs = []
    for col_name, col_type in schema.items():
        if col_name not in df.columns:
            # Si la columna no existe, se agrega nula con tipo
            select_exprs.append(lit(None).cast(col_type).alias(col_name))
        elif col_type.lower() == "date":
            select_exprs.append(to_date(col(col_name), date_format).alias(col_name))
        else:
            select_exprs.append(col(col_name).cast(col_type).alias(col_name))

    return df.select(*select_exprs)

def log(msg: str, level: str = "INFO", entity: str = None):
    """
    Muestra mensajes uniformes en el log de Databricks Jobs.

    Args:
        msg (str): Mensaje a mostrar
        level (str): Nivel del log (INFO, WARN, ERROR, SUCCESS)
        entity (str): Nombre opcional de entidad o proceso
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    prefix = f"[{timestamp}] [{level.upper()}]"
    if entity:
        prefix += f" [{entity}]"
    print(f"{prefix} {msg}")

# ==========================================================
# VALIDACIÓN UNIVERSAL DE DATAFRAMES
# ==========================================================

def is_dataframe_empty(df: DataFrame) -> bool:
    """
    Verifica si un DataFrame de Spark está vacío, de forma segura y portable.
    Compatible con todas las versiones de Spark (>= 2.4) y entornos Databricks, Synapse, Fabric.

    Retorna:
        True  → si el DataFrame es None o no tiene filas
        False → si contiene al menos 1 fila
    """
    if df is None:
        return True

    try:
        if hasattr(df, "isEmpty"):
            return df.isEmpty()
        else:
            return df.limit(1).count() == 0
    except Exception:
        return df.limit(1).count() == 0


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
        spark.conf.set(
            f"fs.azure.account.oauth.provider.type.{adls_account_name}.dfs.core.windows.net",
            "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
        )
        spark.conf.set(f"fs.azure.account.oauth2.client.id.{adls_account_name}.dfs.core.windows.net", client_id)
        spark.conf.set(f"fs.azure.account.oauth2.client.secret.{adls_account_name}.dfs.core.windows.net", client_secret)
        spark.conf.set(
            f"fs.azure.account.oauth2.client.endpoint.{adls_account_name}.dfs.core.windows.net",
            f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
        )

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
# LECTURA Y ESCRITURA DE ARCHIVOS ADLS
# ==========================================================

def read_json_adls(spark, path: str):
    print(f"Leyendo JSON desde {path}")
    return spark.read.option("multiLine", True).json(path)


def read_parquet_adls(spark, path: str):
    print(f"Leyendo Parquet desde {path}")
    return spark.read.parquet(path)


def write_parquet_adls(df, path: str, mode="overwrite"):
    if is_dataframe_empty(df):
        print(f"[WARN] No se escribirá en {path} porque el DataFrame está vacío.")
        return
    print(f"Escribiendo Parquet en {path}")
    df.write.mode(mode).parquet(path)
    print("Archivo guardado correctamente.")

def write_delta_udv(
    spark,
    df,
    schema: str,
    table_name: str,
    abfss_path: str,
    formato: str = "delta",
    mode: str = "overwrite",
    catalog: str = None,
    columns_sql: dict = None,
    table_comment: str = None,
    replace_where: str = None,
    force_recreate: bool = False
):
    """
    Escribe un DataFrame como tabla Delta en la capa UDV (Liga 1 Perú).

    - Crea el esquema (schema) si no existe.
    - Crea o registra la tabla Delta con LOCATION explícito.
    - Aplica comentarios de tabla y columnas desde YAML.
    - Escribe los datos con saveAsTable (overwrite/append).
    - force_recreate=True -> CREATE OR REPLACE TABLE.

    Args:
        spark: sesión Spark.
        df: DataFrame a escribir.
        schema: esquema destino (ej. 'tb_udv').
        table_name: nombre de tabla (ej. 'm_catalogo_equipos').
        abfss_path: ruta física en ADLS.
        formato: formato base (por defecto 'delta').
        mode: modo de escritura: 'overwrite', 'append', etc.
        catalog: catálogo UC (si None usa currentCatalog()).
        columns_sql: definición de columnas desde YAML.
        table_comment: comentario general de la tabla.
        replace_where: condición para overwrite selectivo.
        force_recreate: si True, hace CREATE OR REPLACE TABLE.
    """

    # ----------------------------------------------------
    # Validaciones iniciales
    # ----------------------------------------------------
    if is_dataframe_empty(df):
        print(f"[WARN] No se escribirá la tabla {table_name} porque el DataFrame está vacío.")
        return

    if not catalog:
        try:
            catalog = spark.catalog.currentCatalog()
        except Exception:
            catalog = "hive_metastore"

    full_schema = f"{catalog}.{schema}"
    full_table  = f"{catalog}.{schema}.{table_name}"

    # ----------------------------------------------------
    # Asegurar esquema (namespace) en UC
    # ----------------------------------------------------
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {full_schema}")
    print(f"[INFO] Catálogo activo: {catalog}, esquema: {schema}")

    # ----------------------------------------------------
    # Comprobar carpeta física y existencia de Delta
    # ----------------------------------------------------
    dbutils = get_dbutils()
    try:
        files = [f.name for f in dbutils.fs.ls(abfss_path)]
        delta_log_exists = any("_delta_log" in f for f in files)
        print(f"[INFO] Carpeta detectada en ADLS: {abfss_path}")
    except Exception:
        dbutils.fs.mkdirs(abfss_path)
        delta_log_exists = False
        print(f"[INFO] Carpeta creada en ADLS: {abfss_path}")

    existe_catalogo = spark.catalog.tableExists(full_table)

    # ----------------------------------------------------
    # Construir definición de columnas (si hace falta CREATE/REPLACE)
    # ----------------------------------------------------
    columnas_str = None
    table_comment_clause = ""

    if (not existe_catalogo and not delta_log_exists) or force_recreate:
        if not columns_sql:
            raise ValueError(
                "Bloque 'columns_sql' requerido para crear o reemplazar la tabla Delta."
            )

        columnas_def = []
        for col_name, props in columns_sql.items():
            tipo     = props.get("tipo", "string")
            nullable = "NOT NULL" if not props.get("nullable", True) else ""
            comment  = props.get("comment", None)

            safe_comment = comment.replace("'", "''") if comment else None
            comment_clause = f" COMMENT '{safe_comment}'" if safe_comment else ""

            #  ej: "id_equipo int NOT NULL COMMENT 'Identificador único del equipo'"
            columnas_def.append(f"  {col_name} {tipo} {nullable}{comment_clause}")

        columnas_str = ",\n".join(columnas_def)

        if table_comment:
            safe_table_comment = table_comment.replace("'", "''")
            table_comment_clause = f"COMMENT '{safe_table_comment}'"

    # ----------------------------------------------------
    # Crear / reemplazar / registrar tabla Delta
    # ----------------------------------------------------
    if not existe_catalogo and not delta_log_exists:
        # Primera vez: crear tabla nueva
        print(f"[INFO] Creando nueva tabla Delta: {full_table}")

        create_sql = f"""
        CREATE TABLE {full_table} (
        {columnas_str}
        )
        USING {formato}
        LOCATION '{abfss_path}'
        {table_comment_clause}
        """

        print("CREATE TABLE a ejecutar:\n", create_sql)
        spark.sql(create_sql)
        print(f"[OK] Tabla creada: {full_table}")

    elif force_recreate:
        # Forzar recreación de la tabla (sin borrar datos en la ruta)
        print(f"[INFO] Reemplazando definición de tabla Delta: {full_table}")

        create_sql = f"""
        CREATE OR REPLACE TABLE {full_table} (
        {columnas_str}
        )
        USING {formato}
        LOCATION '{abfss_path}'
        {table_comment_clause}
        """

        print("CREATE OR REPLACE TABLE a ejecutar:\n", create_sql)
        spark.sql(create_sql)
        print(f"[OK] Tabla reemplazada: {full_table}")

    elif not existe_catalogo and delta_log_exists:
        # Ya hay _delta_log en ruta: solo registrar
        print(f"[INFO] Detectado _delta_log. Registrando tabla {full_table}.")
        spark.sql(f"CREATE TABLE IF NOT EXISTS {full_table} USING DELTA LOCATION '{abfss_path}'")
        print(f"[OK] Tabla registrada correctamente en catálogo.")

    else:
        # Tabla ya existe en catálogo y no queremos reemplazarla
        print(f"[INFO] La tabla {full_table} ya existe. No se recreará.")

    # ----------------------------------------------------
    # Comentario de tabla (se asegura incluso si ya existía)
    # ----------------------------------------------------
    if table_comment:
        safe_table_comment = table_comment.replace("'", "''")
        try:
            spark.sql(f"COMMENT ON TABLE {full_table} IS '{safe_table_comment}'")
        except Exception as e:
            print(f"[WARN] No se pudo aplicar comentario a la tabla {full_table}: {e}")

    # ----------------------------------------------------
    # Comentarios de columnas (para tablas nuevas o existentes)
    # ----------------------------------------------------
    if columns_sql:
        for col_name, props in columns_sql.items():
            comment = props.get("comment", None)
            if not comment:
                continue

            safe_comment = comment.replace("'", "''")
            alter_col_sql = f"""
                ALTER TABLE {full_table}
                ALTER COLUMN {col_name} COMMENT '{safe_comment}'
            """
            try:
                spark.sql(alter_col_sql)
            except Exception as e:
                print(f"[WARN] No se pudo aplicar comentario a la columna {col_name}: {e}")

    print(f"[OK] Comentarios aplicados a tabla y columnas (cuando ha sido posible).")

    # ----------------------------------------------------
    # Escritura flexible de datos
    # ----------------------------------------------------
    writer = (
        df.write
        .format(formato)
        .mode(mode)
        .option("overwriteSchema", "true")
    )

    if replace_where and mode == "overwrite":
        writer = writer.option("replaceWhere", replace_where)
        print(f"[INFO] Reemplazo condicional habilitado: {replace_where}")

    print(f"[INFO] Guardando data en {full_table} (modo={mode})")
    writer.saveAsTable(full_table)
    print(f"[SUCCESS] Data escrita correctamente en {full_table}")

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

def get_pipeline(pipeline_name: str) -> dict:
    """
    Retorna PipelineId de pipeline destino.

    Tablas involucradas:
      - tbl_pipeline

    Retorna:
      valor: con PipelineId
    """
    spark = SparkSession.getActiveSession()
    jdbc_url, props, _, _, _, _ = get_sql_connection()

    if not jdbc_url:
        raise Exception("No se pudo establecer conexión JDBC con Azure SQL Database.")

    query = f"""
    (
         SELECT PipelineId,Nombre as pipeline
            FROM tbl_pipeline
            where Nombre='{pipeline_name}'
    ) AS info
    """

    df_info = spark.read.jdbc(url=jdbc_url, table=query, properties=props)

    if is_dataframe_empty(df_info):
        print(f"[WARN] No se encontró Pipeline {pipeline_name}")
        return None

    # Conversión segura sin pandas / sin collect
    registro = df_info.head(1)[0].asDict()

    print(f"[OK] PipelineId destino {pipeline_name}")
    return registro

def get_predecesor(pipeline_id_destino: int) -> dict:
    """
    Retorna un diccionario con la información completa del predecesor para un pipeline destino.

    Tablas involucradas:
      - tbl_predecesores

    Retorna:
      dict: con los campos principales o None si no hay coincidencias
    """
    spark = SparkSession.getActiveSession()
    jdbc_url, props, _, _, _, _ = get_sql_connection()

    if not jdbc_url:
        raise Exception("No se pudo establecer conexión JDBC con Azure SQL Database.")

    query = f"""
    (
         SELECT 
            PipelineId_Predecesor,
            Ruta_Predecesor
        FROM dbo.tbl_predecesores 
        WHERE PipelineId_Destino = {pipeline_id_destino}
    ) AS info
    """

    df_info = spark.read.jdbc(url=jdbc_url, table=query, properties=props)

    if is_dataframe_empty(df_info):
        print(f"[WARN] No se encontró predecesor para PipelineId destino {pipeline_id_destino}")
        return None

    # Conversión segura sin pandas / sin collect
    registro = df_info.head(1)[0].asDict()

    print(f"[OK] Predecesor encontrado para PipelineId destino {pipeline_id_destino}")
    return registro

def get_pipeline_params(pipeline_id: int) -> dict:
    """
    Retorna todos los parámetros de un pipeline en formato dict {Parametro: Valor}.
    
    Fuente: dbo.tbl_pipeline_parametros
    """
    spark = SparkSession.getActiveSession()
    jdbc_url, props, _, _, _, _ = get_sql_connection()

    if not jdbc_url:
        raise Exception("No se pudo establecer conexión JDBC con Azure SQL Database.")

    query = f"""
    (
        SELECT Parametro, Valor
        FROM dbo.tbl_pipeline_parametros
        WHERE PipelineId = {pipeline_id}
    ) AS params
    """

    df_params = spark.read.jdbc(url=jdbc_url, table=query, properties=props)

    if is_dataframe_empty(df_params):
        print(f"[WARN] No se encontraron parámetros para el PipelineId {pipeline_id}")
        return {}

    # Conversión nativa Spark → dict sin pandas
    params_dict = {row["Parametro"]: row["Valor"] for row in df_params.collect()}

    print(f"[OK] {len(params_dict)} parámetros cargados para PipelineId {pipeline_id}")
    return params_dict

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

    query = f"""
    (
        SELECT id, entidad, modoejecucion, rutaraw, flg_udv
        FROM dbo.tbl_paths
        WHERE flg_udv = 'N' AND entidad = '{entidad}'
    ) AS t
    """
    df_paths = spark.read.jdbc(url=jdbc_url, table=query, properties=props)
    if is_dataframe_empty(df_paths):
        print(f"[INFO] No hay rutas pendientes para la entidad '{entidad}'.")
        return None

    print("===============================================")
    print(f"[INFO] Entidad           : {entidad}")
    print(f"[INFO] Total de rutas    : {df_paths.count()}")
    print("===============================================")

    df_union = None
    for row in df_paths.toLocalIterator():
        path_rel = row["rutaraw"]
        path = get_abfss_path(path_rel)
        print(f"[INFO] Leyendo datos desde: {path}")

        try:
            df_temp = spark.read.parquet(path)
            if is_dataframe_empty(df_temp):
                print(f"[WARN] El archivo en {path} está vacío. Saltando...")
                continue

            df_union = df_temp if df_union is None else df_union.unionByName(df_temp, allowMissingColumns=True)
        except Exception as e:
            print(f"[WARN] No se pudo leer {path}: {e}")

    if is_dataframe_empty(df_union):
        print(f"[ERROR] No se pudo generar DataFrame consolidado para '{entidad}'.")
        return None

    lower_cols = [c.lower() for c in df_union.columns]
    cols = [col(c) for c in df_union.columns]

    if "temporada" in lower_cols:
        print("[INFO] Eliminando duplicados por temporada (última fecha_carga).")
        w = Window.partitionBy("temporada").orderBy(desc("fecha_carga"))
        df_union = (
            df_union.select(*cols, row_number().over(w).alias("row_num"))
            .filter(col("row_num") == 1)
            .drop("row_num")
        )
    elif "fecha_carga" in lower_cols:
        df_union = df_union.dropDuplicates(["fecha_carga"])
    else:
        df_union = df_union.dropDuplicates()

    try:
        print("[INFO] Enviando IDs a la cola de actualización tbl_flag_update_queue_id...")
        (
            df_paths
            .select(col("id").cast("int"))
            .write.jdbc(
                url=jdbc_url,
                table="dbo.tbl_flag_update_queue_id",
                mode="append",
                properties=props
            )
        )
        print(f"[OK] {df_paths.count()} IDs enviados a tbl_flag_update_queue_id.")
    except Exception as e:
        print(f"[WARN] Error al actualizar flags en SQL: {e}")

    print(f"[SUCCESS] Entidad '{entidad}' leída y consolidada correctamente.")
    print(f"[SUCCESS] Registros finales: {df_union.count()}")
    print("===============================================")
    return df_union


# ==========================================================
# TRANSFORMACIÓN JSON
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

def generar_udv_json(entidad: str, campo_json="data", limite=50):
    """
    Lee la entidad desde RAW (flg_udv = 'N'), procesa el campo JSON y genera un DataFrame UDV expandido.
    - Llama internamente a get_entity_data()
    - Explosiona y normaliza JSONs de estructura libre
    - Devuelve un DataFrame expandido listo para escribir en UDV
    """

    print("===============================================")
    print(f"[INICIO] Generación de UDV para entidad '{entidad}'")
    print("===============================================")

    # Leer data pendiente desde RAW
    df = get_entity_data(entidad)
    if df is None or is_dataframe_empty(df):
        print(f"[INFO] No se encontró data pendiente para '{entidad}'.")
        return None

    # Validar existencia del campo JSON
    if campo_json not in df.columns:
        raise ValueError(f"El campo '{campo_json}' no existe en la entidad '{entidad}'.")

    # Limpieza y conversión del JSON
    df_limpio = parsear_json(df, campo_json)
    campo_limpio = f"{campo_json}_limpio"
    df_array = convertir_array(df_limpio, campo_limpio)

    # Explosión del JSON
    columnas_base = [col(c) for c in df_array.columns if c not in [campo_limpio, "json_array"]]
    df_exploded = df_array.select(
        *columnas_base,
        explode_outer(col("json_array")).alias("json_item")
    )

    # Inferir claves dinámicas
    claves, claves_anidadas = extraer_claves_subclaves(df_exploded, campo="json_item", limite=limite)
    columnas_dinamicas = construir_columnas(df_exploded, claves, claves_anidadas, prefijo="json", limite=limite)

    # Selección final (sin el campo original 'data')
    columnas_finales = [c for c in df_array.columns if c not in [campo_json, f"{campo_json}_limpio", "json_array"]]
    df_final = df_exploded.select(
        *[col(c) for c in columnas_finales],
        *columnas_dinamicas
    )

    print(f"[INFO] Columnas generadas: {len(df_final.columns)}")
    print(f"[SUCCESS] UDV generado correctamente para '{entidad}'")
    print("===============================================")

    return df_final
