# ==========================================================
# HM_ESTADISTICAS_PARTIDOS
# Proyecto: Liga 1 Perú
# Autor: Oscar García Del Águila
# ==========================================================

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    regexp_extract,
    when,
    lit,
    to_date,
    current_timestamp
)
from utils_liga1 import cast_dataframe_schema, rename_columns


def _first_int_expr(col_name: str, alias: str):
    """
    Extrae el primer número entero de una columna string.
    Si no hay número -> '0' (string).
    """
    raw = col(col_name).cast("string")
    num_str = regexp_extract(raw, r"^(\d+)", 1)
    return when(raw.isNull() | (num_str == ""), lit("0")).otherwise(num_str).alias(alias)


def _pct_expr(col_name: str, alias: str):
    """
    Extrae el porcentaje como DOUBLE [0,1] a partir del ÚLTIMO número de la cadena,
    pero solo si el texto tiene '%' o '(' (indicando que realmente es un porcentaje).

    Ejemplos:
      '7 (64 %)'   -> 0.64
      '7 (64 %)'   -> 0.64   (NBSP)
      '10 (0 %)'   -> 0.0
      '7'          -> 0.0    (no se considera porcentaje)
    """
    raw = col(col_name).cast("string")

    # Hay indicios de que esto lleva porcentaje
    has_pct = raw.contains("%") | raw.contains("(")

    # Tomamos el ÚLTIMO bloque de dígitos en la cadena (.*?(\d+)\D*$)
    pct_str = regexp_extract(raw, r".*?(\d+)\D*$", 1)

    return (
        when(
            raw.isNull()
            | (pct_str == "")
            | (~has_pct)  # si no hay '%' ni '(', no lo tratamos como %.
        , lit(0.0))
        .otherwise(pct_str.cast("double") / 100.0)
        .alias(alias)
    )


def _goles_expr(marcador_col: str, idx: int, alias: str):
    """
    Extrae goles desde marcador 'n - n'.
    idx=1 -> goles_local, idx=2 -> goles_visitante.
    """
    raw = col(marcador_col).cast("string")
    g_str = regexp_extract(raw, r"^(\d+)\s*-\s*(\d+)$", idx)
    return when(raw.isNull() | (g_str == ""), lit(0)).otherwise(
        g_str.cast("int")
    ).alias(alias)


def carga_final_hm_estadisticas_partidos(
    df_hm_partidos: DataFrame,
    df_raw_estadisticas: DataFrame,
    prm_cols_raw_estadisticas_hm: list,
    prm_rename_columns_hm: dict,
    prm_schema_hm: dict,
    prm_fecha_ddMMyyyy: list,
    prm_numeric_no_parse: list,
    prm_goles_cfg: list,      # [marcador, goles_local, goles_visitante]
    prm_stats_with_pct: list  # columnas base que generan *_pct
) -> DataFrame:
    """
    Transformación HM_ESTADISTICAS_PARTIDOS estilo HM_TABLAS_CLASIFICACION:
    - Nada de if por nombre de campo (id_partido, etc.).
    - Sin withColumn, sin collect, sin distinct, sin lambda.
    - Todo controlado por listas del YAML:
      fecha_ddMMyyyy, goles_from_marcador, stats_with_pct.
    """

    # ------------------------------------------------------
    # 1) RAW → seleccionar columnas definidas en YAML
    #    Soporta columnas que no existan en este archivo:
    #    si no está en df_raw_estadisticas, se crea como NULL.
    # ------------------------------------------------------
    available_cols = set(df_raw_estadisticas.columns)

    select_exprs = []
    for c in prm_cols_raw_estadisticas_hm:
        if c in available_cols:
            select_exprs.append(col(c))
        else:
            # Columna no viene en este RAW (ej. "Sin estadísticas disponibles")
            select_exprs.append(lit(None).alias(c))

    df_raw_sel = df_raw_estadisticas.select(*select_exprs)

    # ------------------------------------------------------
    # 2) Renombrar columnas RAW según YAML
    # ------------------------------------------------------
    df_raw_ren = rename_columns(df_raw_sel, prm_rename_columns_hm)

    # ------------------------------------------------------
    # 3) JOIN con HM_PARTIDOS (ids y temporada oficial)
    #    OJO: aquí ya vienen id_partido, id_equipo_local, id_equipo_visitante, temporada
    # ------------------------------------------------------
    df_partidos = df_hm_partidos.select(
        col("id_partido"),
        col("id_equipo_local"),
        col("id_equipo_visitante"),
        col("temporada").cast("int").alias("temporada_partido"),
        col("url").alias("url_partido")
    )

    df_join = df_raw_ren.join(
        df_partidos,
        df_raw_ren["url"] == df_partidos["url_partido"],
        "left"
    ).drop("url_partido")

    # ------------------------------------------------------
    # 4) Listas desde YAML (solo para lógica genérica)
    # ------------------------------------------------------
    fecha_set = set(prm_fecha_ddMMyyyy or [])
    stats_pct_set = set(prm_stats_with_pct or [])
    numeric_no_parse_set = set(prm_numeric_no_parse or [])

    marcador_col, goles_local_col, goles_visit_col = prm_goles_cfg

    cols_join = df_join.columns
    exprs = []

    # ------------------------------------------------------
    # 5) Construir expresiones para TODAS las columnas de df_join
    # ------------------------------------------------------
    for c in cols_join:
        # saltamos columnas que controlamos aparte
        if c in ("temporada", "temporada_partido", "fecha_carga"):
            continue

        # 5.1) columnas de fecha configuradas (dd-MM-yyyy)
        if c in fecha_set:
            exprs.append(to_date(col(c).cast("string"), "dd-MM-yyyy").alias(c))
            continue

        # 5.2) columnas con porcentaje (ej: regates_realizados_local → regates_realizados_local + regates_realizados_local_pct)
        if c in stats_pct_set:
            # cantidad (INT) desde mismo campo
            exprs.append(_first_int_expr(c, alias=c))
            # porcentaje como DOUBLE en [0,1]
            pct_col = f"{c}_pct"
            exprs.append(_pct_expr(c, alias=pct_col))
            continue

        # 5.3) columnas que NO queremos parsear (si quisieras usarlas)
        if c in numeric_no_parse_set:
            exprs.append(col(c))
            continue

        # 5.4) resto: se pasan tal cual
        exprs.append(col(c))

    # 5.5) Temporada oficial desde HM_PARTIDOS
    exprs.append(col("temporada_partido").cast("int").alias("temporada"))

    # ------------------------------------------------------
    # 6) Goles desde marcador (goles_from_marcador: lista de 3 posiciones)
    #    Estos campos se crean NUEVOS (no vienen en RAW).
    # ------------------------------------------------------
    exprs.append(_goles_expr(marcador_col, 1, goles_local_col))
    exprs.append(_goles_expr(marcador_col, 2, goles_visit_col))

    # ------------------------------------------------------
    # 7) Metadatos técnicos (NO vienen del RAW):
    #    - fecha_carga = current_timestamp()
    #    - periodo = temporada_partido
    # ------------------------------------------------------
    exprs.append(current_timestamp().alias("fecha_carga"))
    exprs.append(col("temporada_partido").cast("int").alias("periodo"))

    # ------------------------------------------------------
    # 8) SELECT final con todas las expresiones (sin withColumn)
    # ------------------------------------------------------
    df_base = df_join.select(*exprs)

    # ------------------------------------------------------
    # 9) Cast final al schema de YAML + relleno de NULL numéricos
    # ------------------------------------------------------
    df_final = cast_dataframe_schema(df_base, prm_schema_hm, date_format="yyyy-MM-dd")

    int_like = {"int", "integer", "bigint", "smallint"}
    double_like = {"double", "float", "decimal"}

    int_cols = [
        c for c, t in prm_schema_hm.items()
        if t.lower() in int_like and c in df_final.columns
    ]
    double_cols = [
        c for c, t in prm_schema_hm.items()
        if t.lower() in double_like and c in df_final.columns
    ]

    if int_cols:
        df_final = df_final.na.fill(0, subset=int_cols)
    if double_cols:
        df_final = df_final.na.fill(0.0, subset=double_cols)

    return df_final
