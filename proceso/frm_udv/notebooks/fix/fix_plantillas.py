# ==========================================================
# FIX_PLANTILLAS
# Funciones de transformación para fix histórico (2020-2025)
# Sección fix_md : correcciones pie/altura/valor_mercado → MERGE en md_plantillas
# Sección fix_hm : reconstrucción hm_plantillas_equipo leyendo Delta directo
# ==========================================================

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lit, sha2, concat_ws, lower, trim, initcap,
    regexp_extract, to_date, split, element_at,
    row_number, translate, current_timestamp
)
from pyspark.sql.window import Window
from utils_liga1 import cast_dataframe_schema, rename_columns, build_case_when, norm_col


_ACENTS_FROM = "áéíóúÁÉÍÓÚ"
_ACENTS_TO   = "aeiouaeiou"

def _norm_hash(c):
    """Normaliza igual que curated_csv (minúsculas + sin tildes) para hash estable."""
    return translate(lower(trim(c)), _ACENTS_FROM, _ACENTS_TO)


# ==========================================================
# FASE 1 — MD_PLANTILLAS
# ==========================================================

def carga_fix_md_plantillas(
    df_raw_csv: DataFrame,
    prm_cols_raw: list,
    prm_case_rules: dict,
    prm_schema_fix: dict
) -> DataFrame:
    """
    Construye DataFrame de correcciones desde los CSVs de landing (dataentry).
    Clave de join: nombre del jugador normalizado (lower+trim) — los dataentry
    no tienen id_tm, así que no podemos usar sha2(id_tm). md_plantillas es único
    por jugador, por lo que el join por nombre es equivalente y seguro.
    Devuelve solo filas con al menos un campo corregido (pie, altura o valor_mercado no NULL).
    El MERGE posterior usa COALESCE, por lo que NULL = "no actualizar".
    """
    available = set(df_raw_csv.columns)
    select_exprs = [
        col(c) if c in available else lit(None).cast("string").alias(c)
        for c in prm_cols_raw
    ]
    df = df_raw_csv.select(*select_exprs)

    pie_expr           = build_case_when("pie",           "pie",           prm_case_rules)
    altura_expr        = build_case_when("altura",        "altura",        prm_case_rules)
    valor_mercado_expr = build_case_when("valor_mercado", "valor_mercado", prm_case_rules)

    df_transformed = df.select(
        lower(trim(col("jugador"))).alias("jugador"),   # clave de join normalizada
        col("temporada").cast("int").alias("temporada"),
        pie_expr,
        altura_expr,
        valor_mercado_expr,
    )

    # Por cada jugador tomar el registro más reciente (temporada más alta)
    w = Window.partitionBy("jugador").orderBy(col("temporada").desc())
    df_dedup = (
        df_transformed
        .select(*[col(c) for c in df_transformed.columns], row_number().over(w).alias("rn"))
        .filter(col("rn") == 1)
        .drop("rn", "temporada")
    )

    # Solo filas con al menos un campo corregido
    df_final = df_dedup.filter(
        col("pie").isNotNull() | col("altura").isNotNull() | col("valor_mercado").isNotNull()
    )

    return cast_dataframe_schema(df_final, prm_schema_fix)


# ==========================================================
# FASE 2 — HM_PLANTILLAS_EQUIPO
# ==========================================================

def carga_fix_hm_plantillas(
    df_md_catalogo_equipos: DataFrame,
    prm_cols_catalogo_equipos_hm: list,
    df_raw_plantillas: DataFrame,
    prm_cols_raw_plantillas_hm: list,
    df_md_plantillas: DataFrame,
    prm_case_rules: dict,
    prm_rename_columns_hm: dict,
    prm_schema_hm: dict,
    prm_dedup_cols_hm: list
) -> DataFrame:
    """
    Reconstruye hm_plantillas_equipo leyendo directamente desde Delta
    (sin filtro flg_udv) para propagar correcciones de md_plantillas.
    Config propia en fix_plantillas.yml sección fix_hm.
    """
    # 1) Catálogo equipos: normalizamos nombre_transfermarkt
    df_cat = (
        df_md_catalogo_equipos
        .select(
            *[col(c) for c in prm_cols_catalogo_equipos_hm],
            lower(trim(col("nombre_transfermarkt"))).alias("nombre_transfermarkt_lower")
        )
    )

    # 2) RDV plantillas → dedup + club_lower
    df_raw_sel = df_raw_plantillas.select(*prm_cols_raw_plantillas_hm)
    df_raw_norm = (
        df_raw_sel
        .dropDuplicates(prm_dedup_cols_hm)
        .select(
            *[col(c) for c in df_raw_sel.columns],
            lower(trim(col("club"))).alias("club_lower")
        )
    )

    # 3) JOIN catálogo equipos vs RDV plantillas — inner: solo equipos con data
    df_join_eq = (
        df_cat.alias("e")
        .join(
            df_raw_norm.alias("b"),
            col("e.nombre_transfermarkt_lower") == col("b.club_lower"),
            "inner"
        )
    )

    # 4) Lookup id_jugador desde md_plantillas — join por nombre normalizado
    #    (los dataentry no tienen id_tm, así que no podemos usar sha2(id_tm))
    df_dim_jug = df_md_plantillas.select(
        col("id_jugador"),
        lower(trim(col("jugador"))).alias("jugador_norm_dim")
    )

    # 5) Dataset intermedio con nombre normalizado para el join
    df_join_tmp = df_join_eq.select(
        col("e.id_equipo"),
        col("b.temporada"),
        col("b.jugador"),
        lower(trim(col("b.jugador"))).alias("jugador_lower_raw"),
        col("b.numero"),
        col("b.posicion"),
        col("b.fecha_fichaje"),
        col("b.fuente"),
        col("b.fecha_nacimiento"),
        col("b.nacionalidad")
    )

    # 6) JOIN con md_plantillas por nombre — obtiene id_jugador = sha2(id_tm) de la dim
    df_join_all = (
        df_join_tmp.alias("p")
        .join(
            df_dim_jug.alias("d"),
            col("p.jugador_lower_raw") == col("d.jugador_norm_dim"),
            "inner"
        )
        .filter(col("d.id_jugador").isNotNull())
    )

    # 7) Aplicar case_rules propias (posicion, numero_camiseta, fecha_fichaje)
    numero_expr        = build_case_when("p.numero",        "numero_camiseta", prm_case_rules)
    posicion_expr      = build_case_when("p.posicion",      "posicion",        prm_case_rules)
    fecha_fichaje_expr = build_case_when("p.fecha_fichaje", "fecha_fichaje",   prm_case_rules)

    # Materializar posición normalizada para alinear SHA con UDV
    df_join_all = df_join_all.withColumn("_pos_norm", posicion_expr)

    # 8) SELECT final con id_plantilla_equipo
    df_hm = df_join_all.select(
        sha2(
            concat_ws(
                "-",
                col("p.id_equipo").cast("string"),
                col("d.id_jugador"),
                col("p.temporada").cast("string"),
                lower(trim(col("_pos_norm")))
            ),
            256
        ).alias("id_plantilla_equipo"),
        col("p.id_equipo"),
        col("d.id_jugador"),
        col("p.temporada").cast("int").alias("temporada"),
        numero_expr.alias("numero_camiseta"),
        initcap(col("_pos_norm")).alias("posicion"),
        fecha_fichaje_expr.alias("fecha_fichaje"),
        col("p.fuente").alias("fuente"),
        current_timestamp().alias("fecha_carga"),
        col("p.temporada").cast("int").alias("periodo")
    )

    # 9) Renombrar y castear según schema_hm del YAML
    df_rename_hm = rename_columns(df_hm, prm_rename_columns_hm)
    df_cast_hm   = cast_dataframe_schema(df_rename_hm, prm_schema_hm)

    # 10) Dedup final por jugador+equipo+temporada
    return df_cast_hm.dropDuplicates(["id_equipo", "id_jugador", "temporada"])
