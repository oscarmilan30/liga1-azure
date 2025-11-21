# ==========================================================
# FUNCIONES UDV - HM_VALORACION_EQUIPOS
# Proyecto: Liga 1 Perú
# Autor: Oscar García Del Águila
# ==========================================================

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lower, trim,  sha2, concat_ws, current_timestamp
from utils_liga1 import cast_dataframe_schema, rename_columns, build_case_when


def carga_final_hm_valoracion_equipos(
    df_md_catalogo_equipos: DataFrame,
    prm_cols_catalogo_equipos_hm: list,
    df_raw_liga1: DataFrame,
    prm_cols_raw_liga1_hm: list,
    prm_case_rules: dict,
    prm_rename_columns_hm: dict,
    prm_schema_hm: dict,
    prm_dedup_cols_hm: list
) -> DataFrame:
    """
    Construye la HM de valoración de equipos (hm_valoracion_equipos) a partir de:

      - df_md_catalogo_equipos: maestro de equipos (md_catalogo_equipos)
      - df_raw_liga1: datos RAW de liga1 (CSV anual YTD)

    Pasos:
      1) Selecciona columnas de catálogo según YAML y genera nombre_transfermarkt_lower
      2) Selecciona y deduplica RAW liga1 según YAML y genera club_lower
      3) JOIN catálogo vs RAW por nombre_transfermarkt_lower = club_lower
      4) Aplica reglas CASE-WHEN (jugadores, edad, extranjeros, valores)
      5) Genera id_valoracion_club (hash id_equipo + temporada)
      6) Agrega columnas técnicas (fecha_carga, periodo)
      7) Renombra columnas según YAML
      8) Castea y ordena columnas según schema_hm
    """

    # 1) Catálogo equipos: normalizamos nombre_transfermarkt
    df_cat = (
        df_md_catalogo_equipos
        .select(*prm_cols_catalogo_equipos_hm)
        .select(
            *[col(c) for c in prm_cols_catalogo_equipos_hm],
            lower(trim(col("nombre_transfermarkt"))).alias("nombre_transfermarkt_lower")
        )
    )

    # 2) RAW liga1 → dedup + club_lower
    df_raw_sel = df_raw_liga1.select(*prm_cols_raw_liga1_hm)

    df_raw_norm = (
        df_raw_sel
        .dropDuplicates(prm_dedup_cols_hm)
        .select(
            *[col(c) for c in prm_cols_raw_liga1_hm],
            lower(trim(col("club"))).alias("club_lower")
        )
    )

    # 3) JOIN catálogo equipos vs RAW liga1
    df_join = (
        df_cat.alias("e")
        .join(
            df_raw_norm.alias("b"),
            col("e.nombre_transfermarkt_lower") == col("b.club_lower"),
            "left"
        )
    )

    # 4) Reglas CASE-WHEN (desde YAML)
    jugadores_expr = build_case_when("b.jugadores_en_plantilla", "jugadores_en_plantilla", prm_case_rules)
    edad_promedio_expr = build_case_when("b.edad_promedio", "edad_promedio", prm_case_rules)
    extranjeros_expr = build_case_when("b.extranjeros", "extranjeros", prm_case_rules)
    valor_medio_expr = build_case_when("b.valor_medio", "valor_medio", prm_case_rules)
    valor_total_expr = build_case_when("b.valor_total", "valor_total", prm_case_rules)

    # 5) SELECT final con id_valoracion_club
    df_hm = df_join.select(
        sha2(
            concat_ws(
                "-",
                col("e.id_equipo").cast("string"),
                col("b.temporada").cast("string")
            ),
            256
        ).alias("id_valoracion_equipo"),
        col("e.id_equipo"),
        col("b.temporada").alias("temporada"),
        jugadores_expr.alias("jugadores_en_plantilla"),
        edad_promedio_expr.alias("edad_promedio"),
        extranjeros_expr.alias("extranjeros"),
        valor_medio_expr.alias("valor_medio"),
        valor_total_expr.alias("valor_total"),
        col("b.fuente"),
        current_timestamp().alias("fecha_carga"),
        col("b.temporada").alias("periodo")
    )

    # 6) Renombrar columnas (fuente → fuente_valoracion)
    df_rename = rename_columns(df_hm, prm_rename_columns_hm)

    # 7) Castear y ordenar columnas según schema_hm
    df_cast = cast_dataframe_schema(df_rename, prm_schema_hm)

    return df_cast
