# ==========================================================
# FUNCIONES UDV - HM_ESTADISTICAS_JUGADORES
# Proyecto: Liga 1 Perú
# Autor: Oscar García Del Águila
# ==========================================================

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lit, sha2, concat_ws, lower, trim, when,
    current_timestamp
)
from utils_liga1 import cast_dataframe_schema, rename_columns, build_case_when


def carga_final_hm_estadisticas_jugadores(
    df_raw_estadisticas:    DataFrame,
    prm_cols_raw:           list,
    df_md_catalogo_equipos: DataFrame,
    prm_cols_catalogo:      list,
    df_md_plantillas:       DataFrame,
    prm_case_rules:         dict,
    prm_rename_columns:     dict,
    prm_schema:             dict
) -> tuple:
    """
    Transforma el CSV de estadisticas_jugadores_{año} en la tabla UDV
    hm_estadisticas_jugadores.

    Flujo:
      1. Seleccionar columnas del raw (con cast de temporada a int en el mismo select)
      2. Renombrar
      3. JOIN con md_catalogo_equipos via nombre_transfermarkt → obtener id_equipo
      4. JOIN con md_plantillas via id_tm → obtener id_jugador
      5. SELECT final con PK sha2 + CASE-WHEN + cast schema
      6. Devolver df_ok y df_sin_match
    """

    # ----------------------------------------------------------------
    # 1) Seleccionar columnas raw
    #    temporada se castea a int directamente en el select.
    # ----------------------------------------------------------------
    select_exprs = []
    for c in prm_cols_raw:
        if c not in df_raw_estadisticas.columns:
            # Columnas nuevas ausentes en CSVs históricos → NULL con alias correcto
            select_exprs.append(lit(None).alias(c))
        elif c == "temporada":
            select_exprs.append(col(c).cast("int").alias(c))
        else:
            select_exprs.append(col(c))

    df_sel = df_raw_estadisticas.select(*select_exprs)

    # ----------------------------------------------------------------
    # 2) Renombrar columnas
    # ----------------------------------------------------------------
    df_renamed = rename_columns(df_sel, prm_rename_columns)

    # ----------------------------------------------------------------
    # 3) JOIN con md_catalogo_equipos via nombre_transfermarkt → id_equipo
    # ----------------------------------------------------------------
    df_cat = (
        df_md_catalogo_equipos
        .select(*prm_cols_catalogo)
        .select(
            col("id_equipo"),
            lower(trim(col("nombre_transfermarkt"))).alias("nombre_transfermarkt_lower")
        )
    )

    df_join_cat = (
        df_renamed.alias("e")
        .join(
            df_cat.alias("c"),
            lower(trim(col("e.club"))) == col("c.nombre_transfermarkt_lower"),
            "left"
        )
    )

    df_sin_match_catalogo = (
        df_join_cat
        .filter(col("c.id_equipo").isNull())
        .select(*[col(f"e.{c}") for c in df_renamed.columns], lit("SIN_MATCH_CATALOGO").alias("razon_rechazo"))
    )

    df_con_equipo = df_join_cat.filter(col("c.id_equipo").isNotNull())

    # ----------------------------------------------------------------
    # 4) JOIN con md_plantillas via id_tm → obtener id_jugador
    # ----------------------------------------------------------------
    df_dim = (
        df_md_plantillas
        .select(
            col("id_jugador"),
            col("id_tm").alias("id_tm_dim")
        )
        .filter(col("id_tm_dim").isNotNull() & (trim(col("id_tm_dim")) != ""))
    )

    df_join = (
        df_con_equipo.alias("e")
        .join(df_dim.alias("d"), col("e.id_tm") == col("d.id_tm_dim"), "left")
    )

    df_sin_match_plantillas = (
        df_join
        .filter(col("d.id_jugador").isNull())
        .select(*[col(f"e.{c}") for c in df_con_equipo.columns], lit("SIN_MATCH_MD_PLANTILLAS").alias("razon_rechazo"))
    )

    df_ok = df_join.filter(col("d.id_jugador").isNotNull())

    # ----------------------------------------------------------------
    # 5) SELECT final con PK + CASE-WHEN
    # ----------------------------------------------------------------
    competencia_expr = build_case_when("e.competencia", "competencia", prm_case_rules)

    df_final = df_ok.select(
        sha2(
            concat_ws(
                "-",
                col("e.id_tm"),
                col("e.id_equipo").cast("string"),
                col("e.temporada").cast("string"),
                lower(trim(col("e.competencia")))
            ),
            256
        ).alias("id_estadistica_jugador"),
        col("d.id_jugador"),
        col("e.id_tm"),
        col("e.jugador"),
        col("e.id_equipo"),
        col("e.temporada"),
        competencia_expr.alias("competencia"),
        col("e.partidos_jugados").cast("int"),
        col("e.titularidades").cast("int"),
        col("e.ppp").cast("double"),
        col("e.goles").cast("int"),
        col("e.asistencias").cast("int"),
        col("e.tarjetas_amarillas").cast("int"),
        col("e.tarjeta_amarilla_roja").cast("int"),
        col("e.tarjetas_rojas").cast("int"),
        col("e.entrada_suplente").cast("int"),
        col("e.salida_suplente").cast("int"),
        col("e.penaltis_anotados").cast("int"),
        col("e.goles_en_contra").cast("int"),
        col("e.partidos_imbatido").cast("int"),
        col("e.min_por_gol").cast("int"),
        col("e.minutos_jugados").cast("int"),
        when(col("e.datos_disponibles").cast("boolean") == True, True).otherwise(False).alias("datos_disponibles"),
        lit("Transfermarkt").alias("fuente"),
        current_timestamp().alias("fecha_carga"),
        col("e.temporada").alias("periodo")
    )

    # ----------------------------------------------------------------
    # 6) Cast schema YAML + unir rechazos
    # ----------------------------------------------------------------
    df_cast = cast_dataframe_schema(df_final, prm_schema)

    df_sin_match = df_sin_match_catalogo.unionByName(
        df_sin_match_plantillas, allowMissingColumns=True
    )

    return df_cast, df_sin_match
