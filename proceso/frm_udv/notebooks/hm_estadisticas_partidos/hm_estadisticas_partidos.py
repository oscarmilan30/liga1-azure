# ==========================================================
# HM_ESTADISTICAS_PARTIDOS
# Proyecto: Liga 1 Perú
# Autor: Oscar García Del Águila
# ==========================================================

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    coalesce,
    lit,
    regexp_extract,
    to_date,
    current_timestamp,
    when,
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
      '10 (0 %)'   -> 0.0
      '7'          -> 0.0   (no se considera porcentaje)
    """
    raw = col(col_name).cast("string")
    has_pct = raw.contains("%") | raw.contains("(")
    pct_str = regexp_extract(raw, r".*?(\d+)\D*$", 1)
    return (
        when(
            raw.isNull()
            | (pct_str == "")
            | (~has_pct)
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
    prm_goles_cfg: list,        # [marcador, goles_local, goles_visitante]
    prm_stats_with_pct: list,   # columnas base que generan *_pct
    prm_en_fallback_hm: dict    # mapa ES→EN para coalesce de columnas FotMob (desde YAML)
) -> DataFrame:
    """
    Transformación HM_ESTADISTICAS_PARTIDOS estilo HM_TABLAS_CLASIFICACION:
    - Todo controlado por listas/dicts del YAML:
      fecha_ddMMyyyy, goles_from_marcador, stats_with_pct, en_fallback_hm.
    """

    # ----------------------------------------------------------
    # 1) RDV → seleccionar columnas definidas en YAML
    #    Soporta columnas que no existan en el archivo (→ NULL).
    #    Para métricas con nombre dual (ES/EN en FotMob) se usa
    #    coalesce(español, inglés) leído desde en_fallback_hm YAML.
    #
    #    NORMALIZACIÓN: FotMob usa nombres capitalizados (ej. "Total shots_local").
    #    Convertimos todas las columnas a lowercase para unificar.
    # ----------------------------------------------------------
    df_raw_estadisticas = df_raw_estadisticas.toDF(
        *[c.lower() for c in df_raw_estadisticas.columns]
    )

    available_cols = set(df_raw_estadisticas.columns)

    select_exprs = []
    for c in prm_cols_raw_estadisticas_hm:
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

    # ----------------------------------------------------------
    # 2) Renombrar columnas RDV según YAML
    # ----------------------------------------------------------
    df_raw_ren = rename_columns(df_raw_sel, prm_rename_columns_hm)

    df_raw_ren = df_raw_ren.dropDuplicates(["url"])

    # ----------------------------------------------------------
    # 3) JOIN con HM_PARTIDOS (ids y temporada oficial)
    # ----------------------------------------------------------
    df_partidos = df_hm_partidos.select(
        col("id_partido"),
        col("id_equipo_local"),
        col("id_equipo_visitante"),
        col("temporada").cast("int").alias("temporada"),
        col("url").alias("url_partidos")
    )

    df_joined = (
        df_raw_ren.alias("s")
        .join(df_partidos.alias("p"), col("s.url") == col("p.url_partidos"), "inner")
    )

    # ----------------------------------------------------------
    # 4) SELECT final controlado por schema YAML
    #    Reglas aplicadas en orden:
    #      - Campos de partidos (p): id_partido, id_equipo_*, temporada
    #      - estado → NULL (hm_partidos no lo provee actualmente)
    #      - goles_local / goles_visitante → _goles_expr sobre marcador
    #      - Campos en prm_fecha_ddMMyyyy → to_date dd/MM/yyyy
    #      - Col con sufijo _pct → _pct_expr sobre la columna base
    #      - Col en prm_stats_with_pct → _first_int_expr
    #      - periodo → alias de temporada (p)
    #      - fecha_carga → current_timestamp()
    #      - Strings (marcador, url, fuente) → col(s.col) directo
    #      - Resto de stats numéricas → _first_int_expr
    # ----------------------------------------------------------
    _from_p       = {"id_partido", "id_equipo_local", "id_equipo_visitante", "temporada"}
    _str_passthru = {"marcador", "url", "fuente"}
    _goles_local  = prm_goles_cfg[1]
    _goles_visit  = prm_goles_cfg[2]
    _marcador_col = prm_goles_cfg[0]

    final_exprs = []
    for c in prm_schema_hm:
        if c in _from_p:
            final_exprs.append(col(f"p.{c}"))
        elif c == "estado":
            final_exprs.append(lit(None).cast("string").alias("estado"))
        elif c == _goles_local:
            final_exprs.append(_goles_expr(f"s.{_marcador_col}", 1, c))
        elif c == _goles_visit:
            final_exprs.append(_goles_expr(f"s.{_marcador_col}", 2, c))
        elif c in prm_fecha_ddMMyyyy:
            final_exprs.append(to_date(col(f"s.{c}"), "dd/MM/yyyy").alias(c))
        elif c.endswith("_pct"):
            final_exprs.append(_pct_expr(f"s.{c[:-4]}", c))
        elif c in prm_stats_with_pct:
            final_exprs.append(_first_int_expr(f"s.{c}", c))
        elif c == "periodo":
            final_exprs.append(col("p.temporada").alias("periodo"))
        elif c == "fecha_carga":
            final_exprs.append(current_timestamp().alias("fecha_carga"))
        elif c in _str_passthru:
            final_exprs.append(col(f"s.{c}"))
        else:
            final_exprs.append(_first_int_expr(f"s.{c}", c))

    df_select = df_joined.select(*final_exprs)

    # ----------------------------------------------------------
    # 5) Castear schema final
    # ----------------------------------------------------------
    df_cast = cast_dataframe_schema(df_select, prm_schema_hm)

    return df_cast
