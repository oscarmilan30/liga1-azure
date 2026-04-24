# ==========================================================
# FUNCIONES UDV - MD_ENTRENADORES
#                 HM_ENTRENADORES_EQUIPO
# Proyecto: Liga 1 Perú
# Autor: Oscar García Del Águila
# ==========================================================

from pyspark.sql import Window
from pyspark.sql.functions import row_number,col,regexp_extract,to_date,split,element_at, trim, sha2, concat_ws, lower, current_timestamp, date_format,months_between, floor, current_date, year
from utils_liga1 import cast_dataframe_schema, rename_columns, build_case_when


# ================================================================
# 1) LIMPIAR FECHA DE NACIMIENTO → fecha_nacimiento_limpia
#    (edad se calcula a partir de esa fecha)
# ================================================================
def limpiar_fecha_nacimiento(df, column):
    """
    Soporta:
      - 'dd/MM/yyyy'
      - 'dd/MM/yyyy (edad)'
    """
    return (
        df.select(
            *[col(c) for c in df.columns],
            to_date(
                regexp_extract(col(column), r"^(\d{2}/\d{2}/\d{4})", 1),
                "dd/MM/yyyy"
            ).alias("fecha_nacimiento_limpia")
        )
    )


# ================================================================
# 2) MD_ENTRENADORES
# ================================================================
def carga_final_md_entrenadores(
    df_raw_entrenadores,
    prm_cols_raw_entrenadores_md,
    prm_case_rules,
    prm_rename_columns_md,
    prm_schema_md
):
    """
    Construye md_entrenadores (maestro de entrenadores) a partir del CSV RAW.
    - Una fila por entrenador (última temporada)
    - id_entrenador = hash(entrenador + fecha_nacimiento_limpia)
    - edad actual calculada con current_date()
    """

    # 2.1) Último registro por entrenador (mayor temporada)
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

    # 2.2) Limpiar fecha_nacimiento
    df_step1 = limpiar_fecha_nacimiento(df_raw, "fecha_nacimiento")

    # 2.3) Calcular edad (sin withColumn, usando select)
    df_step1 = df_step1.select(
        *[col(c) for c in df_step1.columns],
        floor(
            months_between(current_date(), col("fecha_nacimiento_limpia")) / 12
        ).alias("edad")
    )

    # 2.4) CASE WHEN de nacionalidad desde YAML
    nacionalidad_expr = build_case_when("nacionalidad", "nacionalidad", prm_case_rules)

    # 2.5) SELECT final con id_entrenador + edad
    df_final = df_step1.select(
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
        nacionalidad_expr.alias("nacionalidad"),
        col("fuente"),
        current_timestamp().alias("fecha_carga"),
        date_format(current_timestamp(), "yyyyMMdd").alias("periododia")
    )

    # 2.6) Renombrar y castear según schema_md
    df_rename = rename_columns(df_final, prm_rename_columns_md)
    df_cast = cast_dataframe_schema(df_rename, prm_schema_md)

    return df_cast


# ================================================================
# 3) HM_ENTRENADORES_EQUIPO
# ================================================================
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
    """
    Construye hm_entrenadores_equipo (histórico entrenador-equipo-temporada)
    con:
      - una fila por periodo entrenador-equipo (club, entrenador, comienzo)
      - temporada = año de comienzo
      - periodo   = año de comienzo (partición)
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

    # 2) RAW entrenadores → dedup + club_lower
    df_raw_sel = df_raw_entrenadores.select(*prm_cols_raw_entrenadores_hm)

    # OJO: prm_dedup_cols_hm = ['club', 'entrenador', 'comienzo']
    df_raw_norm = (
        df_raw_sel
        .dropDuplicates(prm_dedup_cols_hm)
        .select(
            *[col(c) for c in prm_cols_raw_entrenadores_hm],
            lower(trim(col("club"))).alias("club_lower")
        )
    )

    # 3) JOIN catálogo equipos vs RAW entrenadores
    df_join_eq = (
        df_cat.alias("e")
        .join(
            df_raw_norm.alias("b"),
            col("e.nombre_transfermarkt_lower") == col("b.club_lower"),
            "left"
        )
    )

    # 3.4) DIM entrenadores (md_entrenadores) → lookup id_entrenador
    df_dim_ent = (
        df_md_entrenadores
        .select("id_entrenador", "entrenador")
        .select(
            col("id_entrenador"),
            lower(trim(col("entrenador"))).alias("entrenador_lower_dim")
        )
    )

    # 3.5) Dataset intermedio con alias 'p'
    df_join_tmp = df_join_eq.select(
        col("e.id_equipo"),
        col("b.temporada"),   # temporada RAW, ya no la usaremos como temporada final
        col("b.entrenador"),
        lower(trim(col("b.entrenador"))).alias("entrenador_lower_raw"),
        col("b.comienzo"),
        col("b.salida"),
        col("b.tiempo_en_cargo"),
        col("b.partidos"),
        col("b.ppp"),
        col("b.fuente")
    )

    df_join_all = (
        df_join_tmp.alias("p")
        .join(
            df_dim_ent.alias("d"),
            col("p.entrenador_lower_raw") == col("d.entrenador_lower_dim"),
            "left"
        )
    )

    # 3.6) Reglas CASE-WHEN (desde YAML)
    comienzo_expr        = build_case_when("p.comienzo",        "comienzo",        prm_case_rules)
    salida_expr          = build_case_when("p.salida",          "salida",          prm_case_rules)
    tiempo_cargo_expr    = build_case_when("p.tiempo_en_cargo", "tiempo_en_cargo", prm_case_rules)
    partidos_expr        = build_case_when("p.partidos",        "partidos",        prm_case_rules)
    ppp_expr             = build_case_when("p.ppp",             "ppp",             prm_case_rules)
    entrenador_activo_expr = build_case_when("p.salida", "entrenador_activo", prm_case_rules)

    # Año real de comienzo (temporada de ese periodo)
    temporada_col = year(comienzo_expr)

    # 3.7) SELECT final con id_entrenador_equipo usando temporada real
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


    # 3.8) Renombrar columnas (fuente → fuente_entrenador, etc.)
    df_rename_hm = rename_columns(df_hm, prm_rename_columns_hm)

    # 3.9) Castear según schema_hm
    df_cast_hm = cast_dataframe_schema(df_rename_hm, prm_schema_hm)
    
    return df_cast_hm