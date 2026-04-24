# ==========================================================
# FUNCIONES UDV - HM_PARTIDOS
# Proyecto: Liga 1 Perú
# Autor: Oscar García Del Águila
# ==========================================================

from pyspark.sql import DataFrame
from pyspark.sql.functions import col,lower,trim,sha2, concat_ws, regexp_extract
from utils_liga1 import cast_dataframe_schema, rename_columns, build_case_when


def carga_final_hm_partidos(
    df_md_catalogo_equipos: DataFrame,
    prm_cols_catalogo_equipos_hm: list,
    df_raw_partidos: DataFrame,
    prm_cols_raw_partidos_hm: list,
    prm_case_rules: dict,
    prm_rename_columns_hm: dict,
    prm_schema_hm: dict,
    prm_dedup_cols_hm: list | None = None
) -> DataFrame:
    """
    Construye la tabla histórica de partidos a partir de:

      - df_md_catalogo_equipos: maestro de equipos (md_catalogo_equipos)
      - df_raw_partidos: datos RAW de partidos (JSON partidos expandido vía generar_udv_json)

    Para partidos (fuente FotMob) el match de equipos se hace con nombre_fotmob.
    """

    # ================================================================
    # 1) Catálogo de equipos: normalizamos nombre_fotmob
    #    (porque la fuente de partidos es FotMob)
    # ================================================================
    df_cat = (
        df_md_catalogo_equipos
        .select(*prm_cols_catalogo_equipos_hm)
        .select(
            *[col(c) for c in prm_cols_catalogo_equipos_hm],
            lower(trim(col("nombre_fotmob"))).alias("nombre_fuente_lower")
        )
    )

    # ================================================================
    # 2) RAW partidos → selección (+ dedup opcional)
    # ================================================================
    df_raw_sel = df_raw_partidos.select(*prm_cols_raw_partidos_hm)

    if prm_dedup_cols_hm:
        # Ej: ["temporada", "url"] para quedarte con un registro por partido
        df_raw_sel = df_raw_sel.dropDuplicates(prm_dedup_cols_hm)

    # ================================================================
    # 3) Normalizar nombres local / visitante (FotMob)
    # ================================================================
    df_raw_norm = (
        df_raw_sel
        .select(
            *[col(c) for c in prm_cols_raw_partidos_hm],
            lower(trim(col("local"))).alias("local_lower"),
            lower(trim(col("visitante"))).alias("visitante_lower"),
        )
    )

    # ================================================================
    # 4) JOIN catálogo para obtener id_equipo_local
    # ================================================================
    df_join_local = (
        df_raw_norm.alias("r")
        .join(
            df_cat.alias("el"),
            col("el.nombre_fuente_lower") == col("r.local_lower"),
            "left"
        )
        .select(
            col("el.id_equipo").alias("id_equipo_local"),
            col("r.temporada"),
            col("r.fecha"),
            col("r.marcador"),
            col("r.url"),
            col("r.fuente"),
            col("r.fecha_carga"),
            col("r.local_lower"),
            col("r.visitante_lower")
        )
    )

    # ================================================================
    # 5) JOIN catálogo para obtener id_equipo_visitante
    # ================================================================
    df_join_all = (
        df_join_local.alias("p")
        .join(
            df_cat.alias("ev"),
            col("ev.nombre_fuente_lower") == col("p.visitante_lower"),
            "left"
        )
        .select(
            col("p.id_equipo_local"),
            col("ev.id_equipo").alias("id_equipo_visitante"),
            col("p.temporada"),
            col("p.fecha"),
            col("p.marcador"),
            col("p.url"),
            col("p.fuente"),
            col("p.fecha_carga")
        )
    )

    # ================================================================
    # 6) Reglas CASE-WHEN desde YAML (fecha_partido, marcador)
    #    - fecha_partido: parseo dd-MM-yyyy → date
    #    - marcador: si no cumple 'n - n' → 'sin informacion'
    # ================================================================
    fecha_partido_expr = build_case_when("p.fecha", "fecha_partido", prm_case_rules)
    marcador_expr      = build_case_when("p.marcador", "marcador", prm_case_rules)

    df_base = (
        df_join_all.alias("p")
        .select(
            col("p.id_equipo_local"),
            col("p.id_equipo_visitante"),
            col("p.temporada"),
            fecha_partido_expr,   # -> fecha_partido
            marcador_expr,        # -> marcador
            col("p.url"),
            col("p.fuente"),
            col("p.fecha_carga")
        )
    )

    # ================================================================
    # 7) Cálculo de goles + id_partido + periodo
    # ================================================================
    df_hm = df_base.select(
        # id_partido determinístico: temporada + url
        sha2(
            concat_ws(
                "-",
                col("temporada").cast("string"),
                col("url")
            ),
            256
        ).alias("id_partido"),

        col("id_equipo_local"),
        col("id_equipo_visitante"),
        col("temporada"),
        col("fecha_partido"),
        col("marcador"),

        # goles_local: sólo cuando marcador = 'n - n'
        regexp_extract(col("marcador"), r"^(\d+)\s*-\s*(\d+)$", 1)
            .cast("int")
            .alias("goles_local"),

        # goles_visitante
        regexp_extract(col("marcador"), r"^(\d+)\s*-\s*(\d+)$", 2)
            .cast("int")
            .alias("goles_visitante"),

        col("url"),
        col("fuente"),
        col("fecha_carga"),

        # periodo técnico = temporada (año)
        col("temporada").alias("periodo")
    )

    # ================================================================
    # 8) Renombrar columnas (fuente → fuente_partido, etc.) y castear
    # ================================================================
    df_rename_hm = rename_columns(df_hm, prm_rename_columns_hm)
    df_cast_hm   = cast_dataframe_schema(df_rename_hm, prm_schema_hm)

    return df_cast_hm
