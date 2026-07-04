# ==========================================================
# UTILITARIOS
# Proyecto: Liga 1 Perú
# Autor: Oscar García Del Águila
# ==========================================================
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.functions import col, from_json, explode_outer, when, lit, expr, trim, regexp_replace, instr, desc, row_number, to_date, lower, translate, coalesce, regexp_extract
from pyspark.sql.types import ArrayType, MapType, StringType
from pyspark.sql.window import Window
import json
import gc
import time
import yaml
import os
from datetime import datetime
from delta.tables import DeltaTable


# ==========================================================
# FUNCIONES DE UTILIDAD GENERAL
# ==========================================================

def norm_col(col_expr):
    """
    Normaliza una expresión de columna para comparaciones de nombres:
    lower + trim + eliminación de tildes (á→a, é→e, í→i, ó→o, ú→u, ñ→n).
    Centralizado aquí para evitar duplicación en notebooks UDV/DDV.
    """
    return translate(
        lower(trim(col_expr)),
        "áéíóúàèìòùäëïöüñÁÉÍÓÚÀÈÌÒÙÄËÏÖÜÑ",
        "aeiouaeiouaeiounAEIOUAEIOUAEIOUN"
    )


def limpiar_fecha_nacimiento(df, column):
    """
    Extrae la fecha de nacimiento y la edad histórica desde un string con formato mixto.
    Ejemplo de entrada: '10/05/1990 (34)'

    Agrega dos columnas al DataFrame:
        - fecha_nacimiento_limpia: DATE en formato dd/MM/yyyy
        - edad_historica: INT extraído del paréntesis (si existe)
    """
    return df.select(
        *[col(c) for c in df.columns],
        regexp_extract(col(column), r"\((\d+)\)", 1).cast("int").alias("edad_historica"),
        to_date(
            regexp_extract(col(column), r"^(\d{2}/\d{2}/\d{4})", 1),
            "dd/MM/yyyy"
        ).alias("fecha_nacimiento_limpia")
    )

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

def _detect_repo_root() -> str:
    try:
        cwd = os.getcwd()
        if "/Repos/.internal" in cwd:
            # cwd: /Workspace/Repos/.internal/{run_id}/{commit_hash}/{path...}
            # Extraer run_id y commit_hash directamente del cwd para no depender
            # del orden de os.listdir cuando hay múltiples checkouts simultáneos.
            after = cwd.split("/Repos/.internal/")[1].split("/")
            if len(after) >= 2:
                return f"/Workspace/Repos/.internal/{after[0]}/{after[1]}"
            base = "/Workspace/Repos/.internal/" + after[0]
            subdirs = [d for d in os.listdir(base) if os.path.isdir(os.path.join(base, d))]
            return os.path.join(base, subdirs[0]) if subdirs else base
        elif "/Repos/" in cwd:
            parts = cwd.split("/Repos/")[1].split("/")
            return f"/Workspace/Repos/{parts[0]}/{parts[1]}"
        return cwd
    except Exception:
        return os.getcwd()

def get_workspace_path(relative_path: str) -> str:
    return os.path.join(_detect_repo_root(), relative_path.lstrip("/"))

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

def build_case_when(raw_col_name: str, logical_name: str, case_rules: dict):
    """
    Construye una expresión basada en CASE WHEN a partir del YAML.

    case_rules ejemplo:
      capacidad:
        expr: "CASE WHEN {col} IN ('0','0.0','0,0') THEN 'sin informacion' ELSE {col} END"
    """
    if not case_rules:
        return col(raw_col_name).alias(logical_name)

    conf = case_rules.get(logical_name)
    if not conf:
        return col(raw_col_name).alias(logical_name)

    expr_tpl = conf.get("expr")
    if not expr_tpl:
        return col(raw_col_name).alias(logical_name)

    # Reemplazamos {col} por el nombre físico (ej: 'b.capacidad')
    expr_str = expr_tpl.replace("{col}", raw_col_name)

    return expr(expr_str).alias(logical_name)

def rename_columns(
    df: DataFrame,
    rename_dict: dict,
) -> DataFrame:
    """
    Rename columns of DataFrame using a dictionary
    Args:
        df: DataFrame to be renamed
        rename_dict: dictionary to use to rename the columns
    Returns:
        DataFrame with columns renamed
    """
    renamed = df.select(
        *[col(column).alias(rename_dict.get(column, column)) for column in df.columns]
    )
    return renamed

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

def log_quality(
    pipeline_id, capa, tabla_nombre, registros_entrada, registros_salida,
    id_ejecucion=None, id_ejecucion_e2e=None,
    registros_nulos_clave=None, registros_duplicados=None, observaciones=None
):
    """
    Registra métricas de calidad de datos en tbl_data_quality via JDBC.
    No lanza excepción si falla — calidad es no bloqueante.
    """
    try:
        from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
        spark = SparkSession.getActiveSession()
        jdbc_url, props, _, _, _, _ = get_sql_connection()
        if not jdbc_url:
            print("[WARN] log_quality: no se pudo obtener conexión JDBC")
            return

        registros_descartados = abs((registros_entrada or 0) - (registros_salida or 0))

        schema = StructType([
            StructField("id_ejecucion",          IntegerType(), True),
            StructField("id_ejecucion_e2e",       IntegerType(), True),
            StructField("pipeline_id",            IntegerType(), False),
            StructField("capa",                   StringType(),  False),
            StructField("tabla_nombre",           StringType(),  False),
            StructField("registros_entrada",      IntegerType(), True),
            StructField("registros_salida",       IntegerType(), True),
            StructField("registros_descartados",  IntegerType(), True),
            StructField("registros_nulos_clave",  IntegerType(), True),
            StructField("registros_duplicados",   IntegerType(), True),
            StructField("observaciones",          StringType(),  True),
        ])

        row = [(
            int(id_ejecucion)          if id_ejecucion          is not None else None,
            int(id_ejecucion_e2e)      if id_ejecucion_e2e      is not None else None,
            int(pipeline_id),
            str(capa),
            str(tabla_nombre),
            int(registros_entrada)     if registros_entrada      is not None else None,
            int(registros_salida)      if registros_salida       is not None else None,
            int(registros_descartados),
            int(registros_nulos_clave) if registros_nulos_clave  is not None else None,
            int(registros_duplicados)  if registros_duplicados   is not None else None,
            str(observaciones)         if observaciones           is not None else None,
        )]

        df_dq = spark.createDataFrame(row, schema)
        df_dq.write.jdbc(url=jdbc_url, table="dbo.tbl_data_quality", mode="append", properties=props)
        print(f"[OK] log_quality: {tabla_nombre} | entrada={registros_entrada} | salida={registros_salida} | descartados={registros_descartados} | nulos_clave={registros_nulos_clave} | duplicados={registros_duplicados}")
    except Exception as e:
        print(f"[WARN] log_quality falló (no crítico): {e}")


def log_wrong_records(spark, df_rechazados, entidad, capa, pipeline_id, razon_rechazo, modoejecucion=None, periodo=None):
    """
    Guarda registros rechazados en Delta ADLS para auditoría de linaje.
    No lanza excepción si falla — calidad es no bloqueante.
    """
    try:
        from pyspark.sql.functions import to_json, struct, current_timestamp, lit
        if df_rechazados is None or is_dataframe_empty(df_rechazados):
            return

        cnt = df_rechazados.count()
        df_out = (
            df_rechazados
            .select(to_json(struct(*[col(c) for c in df_rechazados.columns])).alias("datos_registro"))
            .withColumn("entidad",       lit(str(entidad)))
            .withColumn("capa",          lit(str(capa)))
            .withColumn("pipeline_id",   lit(int(pipeline_id)))
            .withColumn("razon_rechazo", lit(str(razon_rechazo)))
            .withColumn("modoejecucion", lit(str(modoejecucion)) if modoejecucion else lit(None).cast("string"))
            .withColumn("periodo",       lit(str(periodo))       if periodo       else lit(None).cast("string"))
            .withColumn("fecha_carga",   current_timestamp())
        )

        wrongrecords_path = get_abfss_path("primera_division/rdv/wrongrecords/data")
        df_out.write.format("delta").mode("append").option("mergeSchema", "true").save(wrongrecords_path)
        print(f"[OK] log_wrong_records: {cnt} registros | entidad={entidad} | razón={razon_rechazo}")
    except Exception as e:
        print(f"[WARN] log_wrong_records falló (no crítico): {e}")


def extract_entity_name(full_name):
    """
    Extrae el nombre de la entidad removiendo el prefijo antes del primer guión bajo
    
    Args:
        full_name (str): Nombre completo con prefijo (ej: 'md_equipos')
    
    Returns:
        str: Nombre de la entidad sin prefijo (ej: 'equipos')
    """
    if '_' in full_name:
        return full_name.split('_', 1)[1]
    else:
        return full_name
    
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
    CONEXIÓN HISTÓRICA: autenticación OAuth via Service Principal (client_id / client_secret / tenant_id).
    Reemplazada por Access Connector con Managed Identity (acc-liga1, rg-liga1).
    Se conserva como referencia. No llamar en nuevos notebooks.
    """
    # dbutils = get_dbutils()
    # try:
    #     # Carga de secretos desde Key Vault
    #     adls_account_name = dbutils.secrets.get(scope="secretliga1", key="storageaccount")
    #     client_id         = dbutils.secrets.get(scope="secretliga1", key="clientid")
    #     client_secret     = dbutils.secrets.get(scope="secretliga1", key="secretidt")
    #     tenant_id         = dbutils.secrets.get(scope="secretliga1", key="tenantidt")
    #
    #     spark = SparkSession.builder.getOrCreate()
    #
    #     # Configuración de autenticación OAuth
    #     spark.conf.set(f"fs.azure.account.auth.type.{adls_account_name}.dfs.core.windows.net", "OAuth")
    #     spark.conf.set(
    #         f"fs.azure.account.oauth.provider.type.{adls_account_name}.dfs.core.windows.net",
    #         "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
    #     )
    #     spark.conf.set(f"fs.azure.account.oauth2.client.id.{adls_account_name}.dfs.core.windows.net", client_id)
    #     spark.conf.set(f"fs.azure.account.oauth2.client.secret.{adls_account_name}.dfs.core.windows.net", client_secret)
    #     spark.conf.set(
    #         f"fs.azure.account.oauth2.client.endpoint.{adls_account_name}.dfs.core.windows.net",
    #         f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    #     )
    #
    #     print(f"Autenticación ADLS configurada para: {adls_account_name}")
    # except Exception as e:
    #     print(f"Error configurando ADLS: {e}")
    pass


def get_abfss_path(carpeta="", ruta_base=""):
    """
    Retorna la ruta ABFSS completa.
    """
    dbutils = get_dbutils()
    try:
        container_name = dbutils.secrets.get(scope="secretliga1", key="filesystemname")
        adls_account_name = dbutils.secrets.get(scope="secretliga1", key="storageaccount")
        base_path = f"abfss://{container_name}@{adls_account_name}.dfs.core.windows.net/"
        prefix = f"{ruta_base}/" if ruta_base else ""
        full = prefix + carpeta if carpeta else prefix.rstrip("/")
        return base_path + full if full else base_path
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


def read_delta_adls(spark, ruta_relativa: str):
    """
    Lee una tabla Delta Lake desde una ruta ADLS relativa.
    """
    full_path = get_abfss_path(ruta_relativa)
    print(f"[INFO] Leyendo Delta desde: {full_path}")
    return spark.read.format("delta").load(full_path)


def read_udv_table(
    ruta_tabla: str,
    catalog_name: str = None,
) -> DataFrame | None:
    """
    Lee una tabla UDV a partir de RutaTabla ('schema.tabla') y el catálogo.
    """
    spark = SparkSession.builder.getOrCreate()

    if not catalog_name:
        catalog_name = spark.catalog.currentCatalog()

    full_table_name = f"{catalog_name}.{ruta_tabla}"

    print(f"[INFO] Leyendo tabla UDV: {full_table_name}")

    if not spark.catalog.tableExists(full_table_name):
        print(f"[WARN] La tabla {full_table_name} no existe en el catálogo. Se devolverá None.")
        return None

    return spark.table(full_table_name)

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
    formato: str = "delta",
    mode: str = "overwrite",
    catalog: str = None,
    replace_where: str = None,
    merge_condition: str = None,
    partition_by: list = None,
    overwrite_dynamic_partition: bool = False,
    coalesce_on_match: bool = False,
    coalesce_key_cols: list = None
):
    """
    Escribe un DataFrame como tabla Delta con todos los modos integrados.

    coalesce_on_match=True: en MERGE, preserva el valor existente en Delta si el nuevo es
    null, vacío o 'sin informacion'. Útil para actualizaciones parciales (p.ej. plantillas 2026
    donde algunos campos pueden venir vacíos).

    El catálogo se resuelve en este orden:
      1. Parámetro explícito 'catalog' (si se pasa al llamar la función)
      2. Secret 'catalogname' en el scope 'secretliga1' de Databricks
      3. Fallback: 'catalog_liga1'
    Esto permite que prod use 'catalog_liga1_prod' sin cambiar los notebooks.
    """
    if is_dataframe_empty(df):
        print(f"DataFrame vacío para {table_name} - omitiendo escritura.")
        return

    # Resolver catálogo dinámicamente
    if catalog is None:
        try:
            dbutils = get_dbutils()
            catalog = dbutils.secrets.get(scope="secretliga1", key="catalogname")
            print(f"[INFO] Catálogo leído desde secrets scope: {catalog}")
        except Exception:
            catalog = "catalog_liga1"
            print(f"[INFO] Catálogo por defecto: {catalog}")

    full_table = f"{catalog}.{schema}.{table_name}"

    if not spark.catalog.tableExists(full_table):
        raise ValueError(f"La tabla {full_table} no existe. Créala primero con DDL.")

    print(f"Ejecutando en: {full_table} (modo: {mode})")

    if mode == "merge":
        if not merge_condition:
            raise ValueError("Para MERGE se requiere merge_condition")
        try:
            delta_table = DeltaTable.forName(spark, full_table)
            merge_builder = delta_table.alias("delta").merge(df.alias("df"), merge_condition)

            if coalesce_on_match:
                key_cols = set(coalesce_key_cols or [])
                string_cols = {f.name for f in df.schema.fields if isinstance(f.dataType, StringType)}
                set_expr = {}
                for c in df.columns:
                    if c in key_cols:
                        continue
                    if c in string_cols:
                        # Preserva el valor Delta si el nuevo es null, '' o 'sin informacion'
                        set_expr[c] = f"COALESCE(NULLIF(NULLIF(df.`{c}`, ''), 'sin informacion'), delta.`{c}`)"
                    else:
                        set_expr[c] = f"COALESCE(df.`{c}`, delta.`{c}`)"
                merge_builder.whenMatchedUpdate(set=set_expr).whenNotMatchedInsertAll().execute()
                print(f"MERGE coalesce ejecutado exitosamente en {full_table}")
            else:
                merge_builder.whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
                print(f"MERGE (UPSERT) ejecutado exitosamente en {full_table}")
        except Exception as e:
            print(f"Error en MERGE: {str(e)}")
            raise
        return

    if mode == "overwrite" and overwrite_dynamic_partition and partition_by:
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        print("Activando overwrite dinámico por partición")
        writer = df.write.format(formato).mode("overwrite")
        writer = writer.partitionBy(*partition_by)
        try:
            writer.saveAsTable(full_table)
            print(f"Datos guardados en {full_table} con overwrite dinámico por partición")
        except Exception as e:
            print(f"Error guardando datos (overwrite dinámico): {str(e)}")
            raise
    else:
        writer = df.write.format(formato).mode(mode)
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        if replace_where and mode == "overwrite":
            writer = writer.option("replaceWhere", replace_where)
        try:
            writer.saveAsTable(full_table)
            print(f"Datos guardados en {full_table}")
        except Exception as e:
            print(f"Error guardando datos: {str(e)}")
            raise
        
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

def get_pipeline(pipeline_id: int) -> dict:
    spark = SparkSession.getActiveSession()
    jdbc_url, props, _, _, _, _ = get_sql_connection()

    if not jdbc_url:
        raise Exception("No se pudo establecer conexión JDBC con Azure SQL Database.")

    query = f"""
    (
         SELECT PipelineId,Nombre as pipeline,parent_pipelineid
            FROM tbl_pipeline
            where PipelineId='{pipeline_id}'
    ) AS info
    """

    df_info = spark.read.jdbc(url=jdbc_url, table=query, properties=props)

    if is_dataframe_empty(df_info):
        print(f"[WARN] No se encontró Pipeline {pipeline_id}")
        return None

    registro = df_info.head(1)[0].asDict()
    print(f"[OK] PipelineId destino {pipeline_id}")
    return registro

def get_predecesor(pipeline_id_destino: int) -> dict:
    spark = SparkSession.getActiveSession()
    jdbc_url, props, _, _, _, _ = get_sql_connection()

    if not jdbc_url:
        raise Exception("No se pudo establecer conexión JDBC con Azure SQL Database.")

    query = f"""
    (
         SELECT 
            PipelineId_Predecesor,
            Ruta_Predecesor,
            RutaTabla
        FROM dbo.tbl_predecesores 
        WHERE PipelineId_Destino = {pipeline_id_destino}
    ) AS info
    """

    df_info = spark.read.jdbc(url=jdbc_url, table=query, properties=props)

    if is_dataframe_empty(df_info):
        print(f"[WARN] No se encontraron predecesores para PipelineId destino {pipeline_id_destino}")
        return {}

    resultado = {}

    for row in df_info.toLocalIterator():
        reg = row.asDict()
        ruta_tabla = reg.get("RutaTabla")

        if ruta_tabla:
            if "." in ruta_tabla:
                key = ruta_tabla.split(".")[-1]
            else:
                key = ruta_tabla
        else:
            key = str(reg.get("PipelineId_Predecesor"))

        resultado[key] = reg

    print(f"[OK] {len(resultado)} predecesores encontrados para PipelineId destino {pipeline_id_destino}")
    return resultado


def get_pipeline_params(pipeline_id: int) -> dict:
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

    params_dict = {row["Parametro"]: row["Valor"] for row in df_params.toLocalIterator()}
    print(f"[OK] {len(params_dict)} parámetros cargados para PipelineId {pipeline_id}")
    return params_dict

# ==========================================================
# LECTURA DE ENTIDAD DESDE PATHS (FLG_UDV = 'N')
# ==========================================================

def get_entity_data(entidad: str, dedup_cols: list = None, modoejecucion: str = None):
    spark = SparkSession.getActiveSession()
    jdbc_url, props, sql_server, sql_db, sql_user, sql_pass = get_sql_connection()

    if not jdbc_url:
        raise Exception("No se pudo establecer conexión JDBC con Azure SQL Database.")

    if not modoejecucion:
        raise Exception("modoejecucion es requerido. Debe enviarse desde ADF via widget prm_modo_ejecucion.")

    query = f"""
    (
        SELECT id, entidad, modoejecucion, rutaraw, flg_udv
        FROM dbo.tbl_paths
        WHERE flg_udv = 'N' AND entidad = '{entidad}' AND modoejecucion = '{modoejecucion}'
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

    if dedup_cols:
        print(f"[INFO] Eliminando duplicados por {dedup_cols} (manteniendo la última fecha_carga si existe).")
        if "fecha_carga" in lower_cols:
            df_union = df_union.orderBy(desc("fecha_carga"))
        df_union = df_union.dropDuplicates(dedup_cols)
    else:
        print("[INFO] Aplicando dropDuplicates() global.")
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


def construir_columnas(df_exploded, claves, claves_anidadas, prefijo="", limite=50):
    columnas = {}
    columnas_anidadas = {}

    for key in claves:
        if key not in claves_anidadas:
            alias = key.lower()
            columnas[alias] = col("json_item").getItem(key).alias(alias)

    for key in claves_anidadas:
        alias_base = key.lower()
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

def generar_udv_json(entidad: str, campo_json: str, limite=50, dedup_cols: list = None, modoejecucion: str = None):
    """
    Lee la entidad desde RDV (flg_udv = 'N'), procesa el campo JSON y genera un DataFrame UDV expandido.
    """
    print("===============================================")
    print(f"[INICIO] Generación de UDV para entidad '{entidad}'")
    print(f"[CONFIG] Campo JSON: '{campo_json}' | dedup_cols={dedup_cols}")
    print("===============================================")

    df = get_entity_data(entidad, dedup_cols=dedup_cols, modoejecucion=modoejecucion)
    if df is None or is_dataframe_empty(df):
        print(f"[INFO] No se encontró data pendiente para '{entidad}'.")
        return None

    if campo_json not in df.columns:
        raise ValueError(f"El campo '{campo_json}' no existe en la entidad '{entidad}'. Campos disponibles: {df.columns}")

    df_limpio = parsear_json(df, campo_json)
    campo_limpio = f"{campo_json}_limpio"
    df_array = convertir_array(df_limpio, campo_limpio)

    columnas_base = [col(c) for c in df_array.columns if c not in [campo_limpio, "json_array"]]
    df_exploded = df_array.select(
        *columnas_base,
        explode_outer(col("json_array")).alias("json_item")
    )

    claves, claves_anidadas = extraer_claves_subclaves(df_exploded, campo="json_item", limite=limite)
    columnas_dinamicas = construir_columnas(df_exploded, claves, claves_anidadas, prefijo="", limite=limite)

    columnas_finales = [c for c in df_array.columns if c not in [campo_json, f"{campo_json}_limpio", "json_array"]]
    df_final = df_exploded.select(
        *[col(c) for c in columnas_finales],
        *columnas_dinamicas
    )

    print(f"[INFO] Columnas generadas: {len(df_final.columns)}")
    print(f"[SUCCESS] UDV generado correctamente para '{entidad}'")
    print("===============================================")

    return df_final
