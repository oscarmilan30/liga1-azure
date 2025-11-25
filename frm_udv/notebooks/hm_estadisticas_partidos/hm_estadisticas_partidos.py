# ==========================================================
# TRANSFORMACIÓN HM_ESTADISTICAS_PARTIDOS
# Proyecto: Liga 1 Perú
# Autor: Oscar García Del Águila
# ==========================================================

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, regexp_extract, when, lit, to_date
from utils_liga1 import (
    cast_dataframe_schema,
    log,
    is_dataframe_empty,
    rename_columns,
)


def _first_int_expr(col_name: str, alias: str = None):
    """
    Extrae el primer número entero de una columna string.
    Si no hay número -> '0' (string; se castea a INT al final).
    """
    final_alias = alias if alias else col_name
    raw = col(col_name).cast("string")
    num_str = regexp_extract(raw, r"^(\d+)", 1)

    return (
        when(raw.isNull() | (num_str == ""), lit("0"))
        .otherwise(num_str)
        .alias(final_alias)
    )


def _pct_expr(col_name: str, alias: str = None):
    """
    Extrae el porcentaje dentro de paréntesis como DOUBLE en [0,1].
    Ejemplo:
      '6 (55 %)' -> 0.55
    Si NO hay porcentaje, o la columna es null -> 0.0 (NO null).
    """
    final_alias = alias if alias else f"{col_name}_pct"
    raw = col(col_name).cast("string")
    pct_str = regexp_extract(raw, r"\((\d+)\s*%?", 1)

    return (
        when(raw.isNull() | (pct_str == ""), lit(0.0))
        .otherwise(pct_str.cast("double") / 100.0)
        .alias(final_alias)
    )


def _goles_expr(col_name: str, idx: int, alias: str):
    """
    Extrae goles_local / goles_visitante desde marcador 'n - n'.
    idx=1 -> goles_local, idx=2 -> goles_visitante.
    Si no matchea -> 0.
    """
    raw = col(col_name).cast("string")
    g_str = regexp_extract(raw, r"^(\d+)\s*-\s*(\d+)$", idx)

    return (
        when(raw.isNull() | (g_str == ""), lit(0))
        .otherwise(g_str.cast("int"))
        .alias(alias)
    )


def carga_final_hm_estadisticas_partidos(
    df_hm_partidos: DataFrame,
    df_md_catalogo_equipos: DataFrame,
    df_raw_estadisticas: DataFrame,
    prm_cols_raw_estadisticas_hm: list,
    prm_rename_columns_hm: dict,
    prm_schema_hm: dict
) -> DataFrame:
    """
    Genera HM_ESTADISTICAS_PARTIDOS (1 fila por id_partido):

    - Renombra columnas RAW → snake_case usando YAML.
    - Cruza con HM_PARTIDOS (id_partido, temporada, ids de equipo).
    - Homologa nombres de equipos usando MD_CATALOGO_EQUIPOS.
    - Para cada stat X donde en el schema exista X y/o X_pct:
        * X      = INT (primer número, default 0)
        * X_pct  = DOUBLE (0–1), porcentaje dentro de '()' si existe, sino 0.0
    - Para stats sin _pct en el schema: solo se genera la cantidad.
    - Ningún numérico queda en NULL: siempre 0 / 0.0.
    """

    entidad = "hm_estadisticas_partidos"
    log("Inicio carga_final_hm_estadisticas_partidos", "INFO", entidad)

    if is_dataframe_empty(df_hm_partidos):
        raise Exception("df_hm_partidos viene vacío")

    if is_dataframe_empty(df_md_catalogo_equipos):
        raise Exception("df_md_catalogo_equipos viene vacío")

    if is_dataframe_empty(df_raw_estadisticas):
        raise Exception("df_raw_estadisticas viene vacío")

    # ------------------------------------------------------
    # 1) Seleccionar columnas crudas desde RAW (según YAML)
    # ------------------------------------------------------
    df_raw_sel = df_raw_estadisticas.select(*prm_cols_raw_estadisticas_hm)

    # ------------------------------------------------------
    # 2) Renombrar columnas RAW a nombres lógicos (snake_case)
    # ------------------------------------------------------
    df_raw_renamed = rename_columns(df_raw_sel, prm_rename_columns_hm)

    # ------------------------------------------------------
    # 3) Enriquecer HM_PARTIDOS con nombres estándar de equipos
    # ------------------------------------------------------
    df_cat_local = df_md_catalogo_equipos.select(
        col("id_equipo").alias("id_equipo_local"),
        col("nombre_equipo").alias("equipo_local_std")
    )

    df_cat_visitante = df_md_catalogo_equipos.select(
        col("id_equipo").alias("id_equipo_visitante"),
        col("nombre_equipo").alias("equipo_visitante_std")
    )

    df_partidos = (
        df_hm_partidos
        .select(
            col("id_partido"),
            col("temporada").alias("temporada_partido"),
            col("url").alias("url_partido"),
            col("id_equipo_local"),
            col("id_equipo_visitante")
        )
        .join(df_cat_local, ["id_equipo_local"], "left")
        .join(df_cat_visitante, ["id_equipo_visitante"], "left")
    )

    # ------------------------------------------------------
    # 4) JOIN con RAW por URL
    # ------------------------------------------------------
    df_join = (
        df_raw_renamed
        .join(df_partidos, df_raw_renamed["url"] == df_partidos["url_partido"], "left")
        .drop("url_partido")
    )

    available_cols = set(df_join.columns)

    # ------------------------------------------------------
    # 5) Construcción dinámica de columnas (SIN withColumn)
    # ------------------------------------------------------

    meta_cols = {
        "id_partido",
        "temporada",
        "estado",
        "equipo_local",
        "equipo_visitante",
        "marcador",
        "goles_local",
        "goles_visitante",
        "url",
        "fecha_stats",
        "fecha_carga",
        "fuente",
        "periodo",
    }

    all_schema_cols = list(prm_schema_hm.keys())
    derived_cols = {"goles_local", "goles_visitante"}

    all_stats_base = [
        c for c in all_schema_cols
        if c not in meta_cols and not c.endswith("_pct") and c not in derived_cols
    ]

    select_exprs = []

    # 5.1) Metadata principal
    select_exprs.append(col("id_partido").alias("id_partido"))
    select_exprs.append(col("temporada_partido").alias("temporada"))

    if "estado" in available_cols:
        select_exprs.append(col("estado").cast("string").alias("estado"))
    else:
        select_exprs.append(lit(None).cast("string").alias("estado"))

    if "equipo_local_std" in available_cols:
        select_exprs.append(col("equipo_local_std").alias("equipo_local"))
    else:
        select_exprs.append(col("equipo_local").cast("string").alias("equipo_local"))

    if "equipo_visitante_std" in available_cols:
        select_exprs.append(col("equipo_visitante_std").alias("equipo_visitante"))
    else:
        select_exprs.append(col("equipo_visitante").cast("string").alias("equipo_visitante"))

    if "marcador" in available_cols:
        select_exprs.append(col("marcador").cast("string").alias("marcador"))
        select_exprs.append(_goles_expr("marcador", 1, "goles_local"))
        select_exprs.append(_goles_expr("marcador", 2, "goles_visitante"))
    else:
        select_exprs.append(lit(None).cast("string").alias("marcador"))
        select_exprs.append(lit(0).cast("int").alias("goles_local"))
        select_exprs.append(lit(0).cast("int").alias("goles_visitante"))

    if "url" in available_cols:
        select_exprs.append(col("url").cast("string").alias("url"))
    else:
        select_exprs.append(lit(None).cast("string").alias("url"))

    if "fecha_stats" in available_cols:
        select_exprs.append(
            to_date(col("fecha_stats").cast("string"), "dd-MM-yyyy").alias("fecha_stats")
        )
    else:
        select_exprs.append(lit(None).cast("date").alias("fecha_stats"))

    # 5.2) Stats numéricos: TODOS los stats del schema
    for stat_col in all_stats_base:
        pct_name = f"{stat_col}_pct"

        if stat_col in available_cols:
            # valor base desde RAW
            select_exprs.append(_first_int_expr(stat_col, alias=stat_col))
            # porcentaje si está definido en el schema
            if pct_name in prm_schema_hm:
                select_exprs.append(_pct_expr(stat_col, alias=pct_name))
        else:
            # no existe en RAW -> 0 / 0.0
            select_exprs.append(lit("0").alias(stat_col))
            if pct_name in prm_schema_hm:
                select_exprs.append(lit(0.0).alias(pct_name))

    # 5.3) Metadata final
    if "fecha_carga" in available_cols:
        select_exprs.append(col("fecha_carga"))
    else:
        select_exprs.append(lit(None).cast("timestamp").alias("fecha_carga"))

    if "fuente" in available_cols:
        select_exprs.append(col("fuente").cast("string").alias("fuente"))
    else:
        select_exprs.append(lit(None).cast("string").alias("fuente"))

    select_exprs.append(col("temporada_partido").alias("periodo"))

    df_base = df_join.select(*select_exprs)

    # ------------------------------------------------------
    # 6) Casteo final y asegurar que NO haya NULLs numéricos
    # ------------------------------------------------------
    df_final = cast_dataframe_schema(df_base, prm_schema_hm, date_format="yyyy-MM-dd")

    # rellenar 0 para INT / BIGINT / DOUBLE / FLOAT / DECIMAL
    int_like_types = {"int", "integer", "bigint", "smallint"}
    double_like_types = {"double", "float", "decimal"}

    int_cols = [
        c for c, t in prm_schema_hm.items()
        if t.lower() in int_like_types and c in df_final.columns
    ]
    double_cols = [
        c for c, t in prm_schema_hm.items()
        if t.lower() in double_like_types and c in df_final.columns
    ]

    if int_cols:
        df_final = df_final.na.fill(0, subset=int_cols)
    if double_cols:
        df_final = df_final.na.fill(0.0, subset=double_cols)

    log("Fin carga_final_hm_estadisticas_partidos", "SUCCESS", entidad)
    return df_final
