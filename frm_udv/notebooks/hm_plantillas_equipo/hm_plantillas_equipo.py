# ==========================================================
# FUNCIONES UDV - MD_PLANTILLAS
#                 HM_PLANTILLAS_EQUIPO
# Proyecto: Liga 1 Perú
# Autor: Oscar García Del Águila
# ==========================================================


from pyspark.sql import Window
from pyspark.sql.functions import row_number, col, regexp_extract, regexp_replace, to_date, split, element_at, trim, sha2, concat_ws, lower, current_timestamp, date_format, months_between, floor,current_date
from utils_liga1 import cast_dataframe_schema, rename_columns, build_case_when



# ================================================================
# 1) LIMPIAR FECHA DE NACIMIENTO → edad_historica + fecha_limpia
# ================================================================
def limpiar_fecha_nacimiento(df, column):
    return (
        df.select(
            *[col(c) for c in df.columns],
            regexp_extract(col(column), r"\((\d+)\)", 1).cast("int").alias("edad_historica"),
            to_date(
                regexp_extract(col(column), r"^(\d{2}/\d{2}/\d{4})", 1),
                "dd/MM/yyyy"
            ).alias("fecha_nacimiento_limpia")
        )
    )

def carga_final_md_plantillas(df_raw_plantillas, prm_cols_raw_plantillas_md, prm_case_rules,  prm_rename_columns_md,  prm_schema_md):
    
    # ================================================================
    # 2) OBTENER EL REGISTRO MÁS RECIENTE POR JUGADOR
    # ================================================================
    window_spec = Window.partitionBy("jugador").orderBy(col("temporada").desc())

    df_raw = (
        df_raw_plantillas
        .select(
            *[col(c) for c in df_raw_plantillas.columns],
            row_number().over(window_spec).alias("rn")
        )
        .filter(col("rn") == 1)
        .select(*prm_cols_raw_plantillas_md)
    )


    # ================================================================
    # 3) LIMPIAR FECHA DE NACIMIENTO
    # ================================================================
    df_step1 = limpiar_fecha_nacimiento(df_raw, "fecha_nacimiento")

    # edad_actual
    df_step1 = df_step1.select(*[col(c) for c in df_step1.columns],
        floor(months_between(current_date(), col("fecha_nacimiento_limpia")) / 12).alias("edad")
    )


    # ================================================================
    # 4) NACIONALIDAD: principal + segunda nacionalidad
    # ================================================================
    df_step2 = (
        df_step1
        .select(
            *[col(c) for c in df_step1.columns],
            trim(element_at(split(col("nacionalidad"), ","), 1)).alias("nacionalidad_principal"),
            trim(element_at(split(col("nacionalidad"), ","), 2)).alias("segunda_nacionalidad")
        )
    )


    # ================================================================
    # 5) APLICAR REGLAS CASE-WHEN DEL YAML
    # ================================================================
    altura_expr = build_case_when("altura", "altura", prm_case_rules).cast("double")
    valor_mercado_expr = build_case_when("valor_mercado", "valor_mercado", prm_case_rules)
    pie_expr = build_case_when("pie", "pie", prm_case_rules)
    nacionalidad_expr = build_case_when("nacionalidad_principal", "nacionalidad_principal", prm_case_rules)
    segunda_nacionalidad_expr = build_case_when("segunda_nacionalidad", "segunda_nacionalidad", prm_case_rules)


    # ================================================================
    # 6) CONSTRUIR ID_JUGADOR CONSISTENTE
    # ================================================================
    df_final = (
        df_step2
        .select(
            sha2(
                concat_ws(
                    "-",
                    lower(col("jugador")),
                    col("fecha_nacimiento_limpia").cast("string"),
                    lower(col("nacionalidad_principal"))
                ),
                256
            ).alias("id_jugador"),

            col("jugador"),
            col("edad"),
            nacionalidad_expr.alias("nacionalidad_principal"),
            segunda_nacionalidad_expr.alias("segunda_nacionalidad"),
            altura_expr.alias("altura"),
            pie_expr.alias("pie"),
            valor_mercado_expr.cast("decimal(20,4)").alias("valor_mercado"),
            col("fuente"),
            current_timestamp().alias("fecha_carga"),
            date_format(current_timestamp(), "yyyyMMdd").alias("periododia")
        )
    )


    # ================================================================
    # 7) RENOMBRAR Y CASTEAR SEGÚN YAML
    # ================================================================
    df_rename = rename_columns(df_final, prm_rename_columns_md)
    df_cast = cast_dataframe_schema(df_rename, prm_schema_md)

    return df_cast


def carga_final_hm_plantillas(df_md_catalogo_equipos, prm_cols_catalogo_equipos_hm, df_raw_plantillas, prm_cols_raw_plantillas_hm, df_md_plantillas, prm_case_rules,  prm_rename_columns_hm,  prm_schema_hm,prm_dedup_cols_hm):
    # 1) Catálogo equipos: normalizamos nombre_transfermarkt
    df_cat = (
        df_md_catalogo_equipos
        .select(*prm_cols_catalogo_equipos_hm)
        .select(
            *[col(c) for c in prm_cols_catalogo_equipos_hm],
            lower(trim(col("nombre_transfermarkt"))).alias("nombre_transfermarkt_lower")
        )
    )

    # 2) RAW plantillas → dedup + club_lower
    df_raw_sel = df_raw_plantillas.select(*prm_cols_raw_plantillas_hm)

    df_raw_norm = (
        df_raw_sel
        .dropDuplicates(prm_dedup_cols_hm)
        .select(
            *[col(c) for c in prm_cols_raw_plantillas_hm],
            lower(trim(col("club"))).alias("club_lower")
        )
    )

    # 3) JOIN catálogo equipos vs RAW plantillas
    df_join_eq = (
        df_cat.alias("e")
        .join(
            df_raw_norm.alias("b"),
            col("e.nombre_transfermarkt_lower") == col("b.club_lower"),
            "left"
        )
    )


    # 3.4) DIM jugadores (md_plantillas) → lookup id_jugador
    df_dim_jug = (
        df_md_plantillas
        .select("id_jugador", "jugador")
        .select(
            col("id_jugador"),
            lower(trim(col("jugador"))).alias("jugador_lower_dim")
        )
    )

    # 3.5) Dataset intermedio con alias 'p'
    df_join_tmp = df_join_eq.select(
        col("e.id_equipo"),
        col("b.temporada"),
        col("b.jugador"),
        lower(trim(col("b.jugador"))).alias("jugador_lower_raw"),
        col("b.numero"),
        col("b.posicion"),
        col("b.fecha_fichaje"),
        col("b.fuente")
    )

    df_join_all = (
        df_join_tmp.alias("p")
        .join(
            df_dim_jug.alias("d"),
            col("p.jugador_lower_raw") == col("d.jugador_lower_dim"),
            "left"
        )
    )

    # 3.6) Reglas CASE-WHEN (desde YAML)
    numero_expr        = build_case_when("p.numero",        "numero_camiseta", prm_case_rules)
    posicion_expr      = build_case_when("p.posicion",      "posicion",        prm_case_rules)
    fecha_fichaje_expr = build_case_when("p.fecha_fichaje", "fecha_fichaje",   prm_case_rules)

    # 3.7) SELECT final con id_plantilla_equipo
    df_hm = df_join_all.select(
        sha2(
            concat_ws(
                "-",
                col("p.id_equipo").cast("string"),
                col("d.id_jugador"),
                col("p.temporada").cast("string")
            ),
            256
        ).alias("id_plantilla_equipo"),
        col("p.id_equipo"),
        col("d.id_jugador"),
        col("p.temporada").alias("temporada"),
        numero_expr.alias("numero_camiseta"),
        posicion_expr.alias("posicion"),
        fecha_fichaje_expr.alias("fecha_fichaje"),
        col("p.fuente").alias("fuente"),
        current_timestamp().alias("fecha_carga"),
        col("p.temporada").alias("periodo")
    )

    # 3.8) Renombrar columnas (fuente → fuente_plantilla, periodo, etc.)
    df_rename_hm = rename_columns(df_hm, prm_rename_columns_hm)

    # 3.9) Castear según schema_hm
    df_cast_hm = cast_dataframe_schema(df_rename_hm, prm_schema_hm)
    
    return df_cast_hm












