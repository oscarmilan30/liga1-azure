# ==========================================================
# FUNCIONES UDV - ESTADIOS
# Proyecto: Liga 1 Perú
# Autor: Oscar García Del Águila
# ==========================================================

from pyspark.sql.functions import col, current_timestamp, date_format, lower, trim, sha2, concat_ws
from utils_liga1 import cast_dataframe_schema, rename_columns, build_case_when

def carga_final(
    df_catalogo_equipos,
    df_raw_estadios,
    prm_cols_catalogo_equipos,
    prm_cols_estadios,
    prm_drop_duplicates_estadios,
    prm_rename_columns,
    prm_schema,
    prm_case_rules
):
    from pyspark.sql.functions import lit
    df_catalogo_equipos_select = df_catalogo_equipos.select(*prm_cols_catalogo_equipos)

    df_raw_estadios_select = (
        df_raw_estadios
        .select(*prm_cols_estadios)
        .dropDuplicates(prm_drop_duplicates_estadios)
        .select(
            *[col(c) for c in prm_cols_estadios],
            lower(col("club")).alias("club_lower")
        )
    )

    capacidad_expr = build_case_when("b.capacidad", "capacidad", prm_case_rules)
    aforo_expr     = build_case_when("b.aforo", "aforo", prm_case_rules)

    # Left join para capturar sin match
    df_join_left = (
        df_catalogo_equipos_select.alias("a")
        .join(df_raw_estadios_select.alias("b"),
              trim(lower(col("a.nombre_transfermarkt"))) == trim(col("b.club_lower")),
              "left")
    )

    df_sin_match = (
        df_join_left.filter(col("a.id_equipo").isNull())
        .select(*[col(f"b.{c}") for c in prm_cols_estadios])
        .withColumn("razon_rechazo", lit("SIN_MATCH_CATALOGO"))
    )

    df_join = (
        df_join_left.filter(col("a.id_equipo").isNotNull())
        .select(
            sha2(concat_ws("-", col("a.id_equipo").cast("string"), trim(lower(col("b.estadio")))), 256).alias("id_estadio"),
            col("a.id_equipo"),
            col("b.estadio"),
            capacidad_expr,
            aforo_expr,
            col("b.fuente"),
            current_timestamp().alias("fecha_carga"),
            date_format(current_timestamp(), "yyyyMMdd").alias("periododia")
        )
    )

    df_rename = rename_columns(df_join, prm_rename_columns)
    df_cast   = cast_dataframe_schema(df_rename, prm_schema)

    df_invalidos = (
        df_cast.filter(~(col("estadio").isNotNull() & (col("estadio") != "No disponible")))
        .withColumn("razon_rechazo", lit("REGLA_NEGOCIO"))
    )
    df_cast = df_cast.filter(col("estadio").isNotNull() & (col("estadio") != "No disponible"))

    df_discarded = df_sin_match.unionByName(df_invalidos, allowMissingColumns=True)

    return df_cast, df_discarded
