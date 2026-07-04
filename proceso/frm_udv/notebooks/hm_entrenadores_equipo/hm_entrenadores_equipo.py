# ==========================================================
# FUNCIONES UDV - MD_ENTRENADORES
#                 HM_ENTRENADORES_EQUIPO
# Proyecto: Liga 1 Perú
# Autor: Oscar García Del Águila
# ==========================================================

from pyspark.sql import Window
from pyspark.sql.functions import row_number, col, regexp_extract, to_date, split, element_at, trim, sha2, concat_ws, lower, current_timestamp, date_format, months_between, floor, current_date, year, translate, coalesce
from utils_liga1 import cast_dataframe_schema, rename_columns, build_case_when, norm_col, limpiar_fecha_nacimiento


def carga_final_md_entrenadores(
    df_raw_entrenadores,
    prm_cols_raw_entrenadores_md,
    prm_case_rules,
    prm_rename_columns_md,
    prm_schema_md,
    df_nacionalidades=None
):
    window_spec = Window.partitionBy("entrenador").orderBy(col("temporada").desc())

    df_raw = (
        df_raw_entrenadores
        .select(
            *[col(c) for c in df_raw_entrenadores.columns],
            row_number().over(window_spec).alias("rn")
        )
        .filter(col("rn") == 1)
        .select(*prm_cols_raw_entrenadores_md)
    )

    df_step1 = limpiar_fecha_nacimiento(df_raw, "fecha_nacimiento")

    df_step1 = df_step1.select(
        *[col(c) for c in df_step1.columns],
        floor(
            months_between(current_date(), col("fecha_nacimiento_limpia")) / 12
        ).alias("edad")
    )

    df_step2 = df_step1.select(
        *[col(c) for c in df_step1.columns],
        trim(element_at(split(col("nacionalidad"), ","), 1)).alias("nacionalidad_principal"),
        trim(element_at(split(col("nacionalidad"), ","), 2)).alias("segunda_nacionalidad")
    )

    df_final = df_step2.select(
        sha2(
            concat_ws(
                "-",
                lower(col("entrenador")),
                col("fecha_nacimiento_limpia").cast("string")
            ),
            256
        ).alias("id_entrenador"),
        col("entrenador"),
        col("edad"),
        col("nacionalidad_principal"),
        col("segunda_nacionalidad"),
        col("fuente"),
        current_timestamp().alias("fecha_carga"),
        date_format(current_timestamp(), "yyyyMMdd").alias("periododia")
    )

    if df_nacionalidades is not None:
        df_nac_lookup = df_nacionalidades.select(
            lower(trim(col("nombre_raw"))).alias("__nac_raw"),
            col("nombre_es")
        )
        df_final = (
            df_final.alias("base")
            .join(df_nac_lookup.alias("__n1"),
                  lower(trim(col("base.nacionalidad_principal"))) == col("__n1.__nac_raw"), "left")
            .select(
                col("base.id_entrenador"),
                col("base.entrenador"),
                col("base.edad"),
                coalesce(col("__n1.nombre_es"), col("base.nacionalidad_principal")).alias("nacionalidad_principal"),
                col("base.segunda_nacionalidad"),
                col("base.fuente"),
                col("base.fecha_carga"),
                col("base.periododia")
            )
        )
        df_final = (
            df_final.alias("base")
            .join(df_nac_lookup.alias("__n2"),
                  lower(trim(col("base.segunda_nacionalidad"))) == col("__n2.__nac_raw"), "left")
            .select(
                col("base.id_entrenador"),
                col("base.entrenador"),
                col("base.edad"),
                col("base.nacionalidad_principal"),
                coalesce(col("__n2.nombre_es"), col("base.segunda_nacionalidad")).alias("segunda_nacionalidad"),
                col("base.fuente"),
                col("base.fecha_carga"),
                col("base.periododia")
            )
        )
    else:
        nac_principal_expr = build_case_when("nacionalidad_principal", "nacionalidad_principal", prm_case_rules)
        nac_segunda_expr   = build_case_when("segunda_nacionalidad",   "segunda_nacionalidad",   prm_case_rules)
        df_final = df_final.select(
            col("id_entrenador"),
            col("entrenador"),
            col("edad"),
            nac_principal_expr,
            nac_segunda_expr,
            col("fuente"),
            col("fecha_carga"),
            col("periododia")
        )

    df_rename = rename_columns(df_final, prm_rename_columns_md)
    df_cast = cast_dataframe_schema(df_rename, prm_schema_md)

    return df_cast


def carga_final_hm_entrenadores(
    df_md_catalogo_equipos,
    prm_cols_catalogo_equipos_hm,
    df_raw_entrenadores,
    prm_cols_raw_entrenadores_hm,
    df_md_entrenadores,
    prm_case_rules,
    prm_rename_columns_hm,
    prm_schema_hm,
    prm_dedup_cols_hm
):
    from pyspark.sql.functions import lit
    df_cat = (
        df_md_catalogo_equipos
        .select(*prm_cols_catalogo_equipos_hm)
        .select(
            *[col(c) for c in prm_cols_catalogo_equipos_hm],
            lower(trim(col("nombre_transfermarkt"))).alias("nombre_transfermarkt_lower")
        )
    )

    df_raw_sel = df_raw_entrenadores.select(*prm_cols_raw_entrenadores_hm)

    df_raw_norm = (
        df_raw_sel
        .dropDuplicates(prm_dedup_cols_hm)
        .select(
            *[col(c) for c in prm_cols_raw_entrenadores_hm],
            lower(trim(col("club"))).alias("club_lower")
        )
    )

    # Left join para capturar sin match con catálogo
    df_join_eq_left = (
        df_cat.alias("e")
        .join(
            df_raw_norm.alias("b"),
            col("e.nombre_transfermarkt_lower") == col("b.club_lower"),
            "left"
        )
    )

    df_sin_match_catalogo = (
        df_join_eq_left.filter(col("e.id_equipo").isNull())
        .select(*[col(f"b.{c}") for c in prm_cols_raw_entrenadores_hm])
        .withColumn("razon_rechazo", lit("SIN_MATCH_CATALOGO"))
    )

    df_join_eq = df_join_eq_left.filter(col("e.id_equipo").isNotNull())

    df_dim_ent = (
        df_md_entrenadores
        .select("id_entrenador", "entrenador")
        .select(
            col("id_entrenador"),
            norm_col(col("entrenador")).alias("entrenador_lower_dim")
        )
    )

    df_join_tmp = df_join_eq.select(
        col("e.id_equipo"),
        col("b.club").alias("club"),
        col("b.temporada"),
        col("b.entrenador"),
        norm_col(col("b.entrenador")).alias("entrenador_lower_raw"),
        col("b.comienzo"),
        col("b.salida"),
        col("b.tiempo_en_cargo"),
        col("b.partidos"),
        col("b.ppp"),
        col("b.fuente")
    )

    # Left join para capturar sin match con dimensión entrenadores
    df_join_all_left = (
        df_join_tmp.alias("p")
        .join(
            df_dim_ent.alias("d"),
            col("p.entrenador_lower_raw") == col("d.entrenador_lower_dim"),
            "left"
        )
    )

    df_sin_match_dimension = (
        df_join_all_left.filter(col("d.id_entrenador").isNull())
        .select(*[col(f"p.{c}") for c in prm_cols_raw_entrenadores_hm])
        .withColumn("razon_rechazo", lit("SIN_MATCH_DIMENSION"))
    )

    df_join_all = df_join_all_left.filter(col("d.id_entrenador").isNotNull())

    comienzo_expr        = build_case_when("p.comienzo",        "comienzo",        prm_case_rules)
    salida_expr          = build_case_when("p.salida",          "salida",          prm_case_rules)
    tiempo_cargo_expr    = build_case_when("p.tiempo_en_cargo", "tiempo_en_cargo", prm_case_rules)
    partidos_expr        = build_case_when("p.partidos",        "partidos",        prm_case_rules)
    ppp_expr             = build_case_when("p.ppp",             "ppp",             prm_case_rules)
    entrenador_activo_expr = build_case_when("p.salida", "entrenador_activo", prm_case_rules)

    temporada_col = year(comienzo_expr)

    df_hm = df_join_all.select(
        sha2(
            concat_ws(
                "-",
                col("p.id_equipo").cast("string"),
                col("d.id_entrenador"),
                col("p.comienzo").cast("string")
            ),
            256
        ).alias("id_entrenador_equipo"),
        col("p.id_equipo"),
        col("d.id_entrenador"),
        temporada_col.alias("temporada"),
        comienzo_expr.alias("comienzo"),
        salida_expr.alias("salida"),
        tiempo_cargo_expr.alias("tiempo_en_cargo"),
        partidos_expr.alias("partidos"),
        ppp_expr.alias("ppp"),
        entrenador_activo_expr.alias("entrenador_activo"),
        col("p.fuente").alias("fuente"),
        current_timestamp().alias("fecha_carga"),
        temporada_col.alias("periodo")
    )

    df_rename_hm = rename_columns(df_hm, prm_rename_columns_hm)
    df_cast_hm = cast_dataframe_schema(df_rename_hm, prm_schema_hm)

    df_discarded = df_sin_match_catalogo.unionByName(
        df_sin_match_dimension,
        allowMissingColumns=True
    )

    return df_cast_hm, df_discarded
