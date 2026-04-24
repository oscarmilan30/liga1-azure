# ==========================================================
# FUNCIONES UDV - ESTADIOS
# Proyecto: Liga 1 Perú
# Autor: Oscar García Del Águila
# ==========================================================

from pyspark.sql.functions import col, current_timestamp, date_format, lower,trim,sha2,concat_ws
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
    """
    Construye el maestro de estadios (md_estadios) a partir de:

      - df_catalogo_equipos: catálogo maestro de equipos (md_catalogo_equipos)
      - df_raw_estadios: datos RAW de estadios (entidad 'estadios')

    Pasos:
      1) Selecciona columnas del catálogo según YAML
      2) Selecciona y deduplica estadios según YAML
      3) Normaliza club a minúsculas (club_lower)
      4) Hace JOIN catálogo vs RAW por nombre de equipo/club
      5) Agrega campos de control (fecha_carga, periododia)
      6) Renombra columnas según YAML
      7) Castea/ordena columnas según schema YAML
    """

    # 1) Selección de catálogo
    df_catalogo_equipos_select = df_catalogo_equipos.select(*prm_cols_catalogo_equipos)

    # 2) Selección y deduplicación de RAW estadios
    df_raw_estadios_select = (
        df_raw_estadios
        .select(*prm_cols_estadios)
        .dropDuplicates(prm_drop_duplicates_estadios)
        .select(
            *[col(c) for c in prm_cols_estadios],
            lower(col("club") ).alias("club_lower")
        )
    )


    # 3) configuracion logicas YAML
    capacidad_expr = build_case_when("b.capacidad", "capacidad", prm_case_rules)
    aforo_expr     = build_case_when("b.aforo", "aforo", prm_case_rules)

    # 4) JOIN catálogo vs estadios
    df_join = (
        df_catalogo_equipos_select.alias("a")
        .join(
            df_raw_estadios_select.alias("b"),
            trim(lower(col("a.nombre_transfermarkt"))) == trim(col("b.club_lower")),
            "left"
        )
        .select(
            sha2(
            concat_ws(
                "-",                                
                col("a.id_equipo").cast("string"),
                trim(lower(col("b.estadio")))
            ),
            256
        ).alias("id_estadio"),
            col("a.id_equipo"),
            col("b.estadio"),
            capacidad_expr,
            aforo_expr,
            col("b.fuente"),
            current_timestamp().alias("fecha_carga"),
            date_format(current_timestamp(), "yyyyMMdd").alias("periododia")
        )
    )

    # 5) Renombrar columnas según YAML (club_lower → club_raw, fuente → fuente_estadio)
    df_rename = rename_columns(df_join, prm_rename_columns)

    # 6) Castear y ordenar columnas según schema
    df_cast = cast_dataframe_schema(df_rename, prm_schema)


    return df_cast
