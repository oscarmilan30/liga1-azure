# ==========================================================
# HM_ESTADISTICAS_PARTIDOS_FIX
# Transformación para carga histórica (2020-2025)
# ==========================================================

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    coalesce,
    concat,
    lpad,
    lower,
    regexp_extract,
    when,
    lit,
    to_date,
    current_timestamp
)
from utils_liga1 import cast_dataframe_schema, rename_columns


def _first_int_expr(col_name: str, alias: str):
    raw = col(col_name).cast("string")
    num_str = regexp_extract(raw, r"^(\d+)", 1)
    return when(raw.isNull() | (num_str == ""), lit("0")).otherwise(num_str).alias(alias)


def _pct_expr(col_name: str, alias: str):
    raw = col(col_name).cast("string")
    has_pct = raw.contains("%") | raw.contains("(")
    pct_str = regexp_extract(raw, r".*?(\d+)\D*$", 1)
    return (
        when(raw.isNull() | (pct_str == "") | (~has_pct), lit(0.0))
        .otherwise(pct_str.cast("double") / 100.0)
        .alias(alias)
    )


def _goles_expr(marcador_col: str, idx: int, alias: str):
    raw = col(marcador_col).cast("string")
    g_str = regexp_extract(raw, r"^(\d+)\s*-\s*(\d+)$", idx)
    return when(raw.isNull() | (g_str == ""), lit(0)).otherwise(g_str.cast("int")).alias(alias)


def carga_final_hm_estadisticas_partidos_fix(
    df_hm_partidos: DataFrame,
    df_raw_estadisticas: DataFrame,
    prm_cols_raw: list,
    prm_rename: dict,
    prm_schema: dict,
    prm_fecha_ddMMyyyy: list,
    prm_numeric_no_parse: list,
    prm_goles_cfg: list,
    prm_stats_with_pct: list,
    prm_en_fallback_hm: dict   # mapa ES→EN desde YAML en_fallback_hm
) -> DataFrame:
    # 1) Lowercase de columnas (JSON landing tiene casing mixto: "Tiros totales_local")
    df_raw_estadisticas = df_raw_estadisticas.toDF(
        *[c.lower() for c in df_raw_estadisticas.columns]
    )

    # Seleccionar columnas según YAML; coalesce ES|EN para FotMob bilingüe
    available_cols = set(df_raw_estadisticas.columns)
    select_exprs = []
    for c in prm_cols_raw:
        en = prm_en_fallback_hm.get(c)
        if c in available_cols and en and en in available_cols:
            select_exprs.append(coalesce(col(c), col(en)).alias(c))
        elif c in available_cols:
            select_exprs.append(col(c))
        elif en and en in available_cols:
            select_exprs.append(col(en).alias(c))
        else:
            select_exprs.append(lit(None).alias(c))
    df_raw_sel = df_raw_estadisticas.select(*select_exprs)

    # 2) Renombrar
    df_raw_ren = rename_columns(df_raw_sel, prm_rename)

    # 3) JOIN con hm_partidos
    df_partidos = df_hm_partidos.select(
        col("id_partido"),
        col("id_equipo_local"),
        col("id_equipo_visitante"),
        col("temporada").cast("int").alias("temporada_partido"),
        col("url").alias("url_partido")
    )
    df_join = df_raw_ren.join(df_partidos, df_raw_ren["url"] == df_partidos["url_partido"], "inner").drop("url_partido")

    # 4) Construir expresiones finales
    fecha_set = set(prm_fecha_ddMMyyyy or [])
    stats_pct_set = set(prm_stats_with_pct or [])
    numeric_no_parse_set = set(prm_numeric_no_parse or [])
    marcador_col, goles_local_col, goles_visit_col = prm_goles_cfg

    cols_join = df_join.columns
    exprs = []
    for c in cols_join:
        if c in ("temporada", "temporada_partido", "fecha_carga"):
            continue
        if c in fecha_set:
            raw_fecha = col(c).cast("string")
            dia = lpad(regexp_extract(raw_fecha, r"^[A-Za-z]+, [A-Za-z]+ ([0-9]{1,2})", 1), 2, "0")
            mes_name = lower(regexp_extract(raw_fecha, r"^[A-Za-z]+, ([A-Za-z]+) ", 1))
            mes_num = (
                when(mes_name == "january",   lit("01"))
                .when(mes_name == "february",  lit("02"))
                .when(mes_name == "march",     lit("03"))
                .when(mes_name == "april",     lit("04"))
                .when(mes_name == "may",       lit("05"))
                .when(mes_name == "june",      lit("06"))
                .when(mes_name == "july",      lit("07"))
                .when(mes_name == "august",    lit("08"))
                .when(mes_name == "september", lit("09"))
                .when(mes_name == "october",   lit("10"))
                .when(mes_name == "november",  lit("11"))
                .when(mes_name == "december",  lit("12"))
                .otherwise(lit("00"))
            )
            yr_raw = regexp_extract(raw_fecha, r"([0-9]{4})$", 1)
            yr_final = when(yr_raw == "", col("temporada_partido").cast("string")).otherwise(yr_raw)
            fecha_dd_MM_yyyy = concat(dia, lit("-"), mes_num, lit("-"), yr_final)
            exprs.append(
                coalesce(
                    to_date(raw_fecha, "dd-MM-yyyy"),
                    to_date(fecha_dd_MM_yyyy, "dd-MM-yyyy")
                ).alias(c)
            )
            continue
        if c in stats_pct_set:
            exprs.append(_first_int_expr(c, alias=c))
            exprs.append(_pct_expr(c, alias=f"{c}_pct"))
            continue
        if c in numeric_no_parse_set:
            exprs.append(col(c))
            continue
        exprs.append(col(c))

    exprs.append(col("temporada_partido").cast("int").alias("temporada"))
    exprs.append(_goles_expr(marcador_col, 1, goles_local_col))
    exprs.append(_goles_expr(marcador_col, 2, goles_visit_col))
    exprs.append(current_timestamp().alias("fecha_carga"))
    exprs.append(col("temporada_partido").cast("int").alias("periodo"))

    df_base = df_join.select(*exprs)

    # Cast final
    df_final = cast_dataframe_schema(df_base, prm_schema, date_format="yyyy-MM-dd")

    int_like = {"int", "integer", "bigint", "smallint"}
    double_like = {"double", "float", "decimal"}
    int_cols = [c for c, t in prm_schema.items() if t.lower() in int_like and c in df_final.columns]
    double_cols = [c for c, t in prm_schema.items() if t.lower() in double_like and c in df_final.columns]

    if int_cols:
        df_final = df_final.na.fill(0, subset=int_cols)
    if double_cols:
        df_final = df_final.na.fill(0.0, subset=double_cols)

    return df_final