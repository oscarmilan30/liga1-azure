# ==========================================================
# FUNCIONES UDV - EQUIPOS
# Proyecto: Liga 1 Perú
# Autor: Oscar García Del Águila
# ==========================================================

from pyspark.sql.functions import col, current_timestamp, date_format, lower, trim
from utils_liga1 import cast_dataframe_schema, rename_columns


def carga_final(df_catalogo_equipos, df_raw_equipos, prm_cols_catalogo_equipos, prm_cols_equipos, prm_drop_duplicates_equipos, prm_rename_columns, prm_schema):
    """
    Une los datos crudos de equipos (FotMob) con el catálogo maestro usando el nombre
    normalizado como clave de join. El catálogo resuelve el id_equipo canónico que
    unifica FotMob y Transfermarkt, por eso es obligatorio como fuente de identidad.

    Args:
        df_catalogo_equipos: DataFrame de md_catalogo_equipos (hub de identidades).
        df_raw_equipos: DataFrame RAW de equipos extraído de FotMob.
        prm_cols_catalogo_equipos: columnas a tomar del catálogo (definidas en YAML).
        prm_cols_equipos: columnas a tomar del RAW de equipos (definidas en YAML).
        prm_drop_duplicates_equipos: columnas clave para deduplicación.
        prm_rename_columns: dict de renombrado columna_origen → columna_destino.
        prm_schema: esquema final (nombre, tipo, comentario) definido en YAML.
    """

    from pyspark.sql.functions import lit
    df_catalogo_equipos_select = df_catalogo_equipos.select(*prm_cols_catalogo_equipos)

    df_raw_equipos_select = (
        df_raw_equipos
        .select(*prm_cols_equipos)
        .select(
            *prm_cols_equipos,
            lower(col("equipo")).alias("equipo_lower")
        )
        .dropDuplicates(["equipo_lower"])
    )

    df_join_left = (
        df_raw_equipos_select.alias("b")
        .join(df_catalogo_equipos_select.alias("a"),
              trim(col("a.nombre_fotmob")) == trim(col("b.equipo_lower")),
              "left")
    )

    df_sin_match = (
        df_join_left.filter(col("a.id_equipo").isNull())
        .select(*[col(f"b.{c}") for c in prm_cols_equipos])
        .withColumn("razon_rechazo", lit("SIN_MATCH_CATALOGO"))
    )

    df_join = (
        df_join_left.filter(col("a.id_equipo").isNotNull())
        .select(
            col("a.id_equipo"),
            col("a.nombre_equipo"),
            col("a.alias"),
            col("b.equipo_lower"),
            col("b.url"),
            col("b.fuente"),
            current_timestamp().alias("fecha_carga"),
            date_format(current_timestamp(), "yyyyMMdd").alias("periododia")
        )
    )

    df_rename = rename_columns(df_join, prm_rename_columns)
    df_cast   = cast_dataframe_schema(df_rename, prm_schema)

    return df_cast, df_sin_match