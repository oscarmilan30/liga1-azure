# ==========================================================
# HM – TABLAS CLASIFICACION
# Proyecto: Liga 1 Perú
# Autor: Oscar García Del Águila
# ==========================================================

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, explode_outer, from_json, trim, lower, when,
    sha2, concat_ws, current_timestamp,
    regexp_replace, concat, lit, translate, row_number
)
from pyspark.sql import Window
from pyspark.sql.types import ArrayType, MapType, StringType

from utils_liga1 import (
    cast_dataframe_schema,
    rename_columns
)

# ==========================================================
# 1) NORMALIZAR TITULOS
# ==========================================================
def normalizar_texto(col_expr):
    return translate(lower(trim(col_expr)),
        "áéíóúàèìòùäëïöüñÁÉÍÓÚÀÈÌÒÙÄËÏÖÜÑ",
        "aeiouaeiouaeiounAEIOUAEIOUAEIOUN")

# ==========================================================
# 2) EXPLOSIÓN DINÁMICA JSON
# ==========================================================
def transformar_raw_tablas(df_raw: DataFrame, campo_json: str, cols_json_item) -> DataFrame:

    raw_cols = df_raw.columns

    # Parse JSON
    df1 = df_raw.select(*(col(c) for c in raw_cols),
    from_json(
        when(col(campo_json).startswith("["), col(campo_json))
        .otherwise(concat(lit("["), col(campo_json), lit("]"))),
        ArrayType(
            MapType(
                StringType(),
                ArrayType(MapType(StringType(), StringType()))
            )
        )
    ).alias("json_array"))

    # explode 1
    df2 = df1.select(
        *(col(c) for c in raw_cols),
        explode_outer("json_array").alias("torneos_map")
    )

    # explode 2
    df3 = df2.select(
        *(col(c) for c in raw_cols),
        explode_outer("torneos_map").alias("tipo_tabla", "lista_items")
    )

    # explode 3
    df4 = df3.select(
        *(col(c) for c in raw_cols),
        col("tipo_tabla"),
        explode_outer("lista_items").alias("json_item")
    )

# ================================
#  CLAVES DINÁMICAS
# ================================
    df_expanded = df4.select(
        *(col(c) for c in raw_cols),
        col("tipo_tabla"),
        *[col("json_item").getItem(k).alias(k) for k in cols_json_item]
    )

    return df_expanded



# ==========================================================
# 3) TRANSFORMACIÓN COMPLETA HM
# ==========================================================
def carga_final_hm_tablas_clasificacion(
    df_raw: DataFrame,
    df_md_catalogo_equipos: DataFrame,
    prm_cols_raw_clasificacion_hm: list,
    prm_cols_json_item_hm: list,
    prm_rename_columns_hm: dict,
    prm_schema_hm: dict,
    campo_json: str
) -> DataFrame:
 
    # A) Explosión JSON
    df_expanded = transformar_raw_tablas(df_raw, campo_json, prm_cols_json_item_hm)

    # B) separación goles
    df_goles = df_expanded.select(*df_expanded.columns,
    col("goles_favor").alias("goles_favor_tmp"),
    col("goles_contra").alias("goles_contra_tmp")
     )

    # C) columnas YAML
    df_sel = df_goles.select(*(col(c) for c in prm_cols_raw_clasificacion_hm))

    # D) rename YAML
    df_ren = rename_columns(df_sel, prm_rename_columns_hm)

    # E) diferencia_goles = goles_favor - goles_contra (valor con signo correcto)
    df_abs = df_ren.select(
        *[col(c) for c in df_ren.columns if c != "diferencia_goles"],
        (col("goles_favor") - col("goles_contra")).cast("int").alias("diferencia_goles")
    )

    # F) join catálogo: intenta todos los campos de nombre para no perder equipos
    #    cuando FotMob usa nombres alternativos (ej: "Universitario" en 2025)
    _campos_nombre = [
        "nombre_fotmob_clasificacion",
        "nombre_fotmob_clas_alt",
        "nombre_equipo",
        "nombre_fotmob",
        "nombre_transfermarkt",
        "alias",
    ]
    df_lookup = None
    for _i, _campo in enumerate(_campos_nombre):
        _df_tmp = (
            df_md_catalogo_equipos
            .select(col("id_equipo"), normalizar_texto(col(_campo)).alias("_nombre_norm"), lit(_i).alias("_prio"))
            .filter(col("_nombre_norm").isNotNull() & (col("_nombre_norm") != ""))
        )
        df_lookup = _df_tmp if df_lookup is None else df_lookup.union(_df_tmp)

    _w = Window.partitionBy("_nombre_norm").orderBy("_prio")
    df_lookup = (
        df_lookup
        .withColumn("_rn", row_number().over(_w))
        .filter(col("_rn") == 1)
        .drop("_rn", "_prio")
    )

    df_join = df_abs.join(
        df_lookup,
        normalizar_texto(col("equipo")) == col("_nombre_norm"),
        "left"
    ).drop("_nombre_norm")

    _sin_match = (
        df_join.filter(col("id_equipo").isNull())
        .select("equipo")
        .withColumn("razon_rechazo", lit("SIN_MATCH_CATALOGO"))
    )
    if not _sin_match.isEmpty():
        print("[WARN] Registros sin match en md_catalogo_equipos (excluidos):")
        _sin_match.show(20, truncate=False)

    df_join = df_join.filter(col("id_equipo").isNotNull())

    # G) id_clasificacion + periodo + fecha_carga
    df_id = df_join.select(
        *df_join.columns,
        sha2(
            concat_ws(
                "-",
                col("temporada").cast("string"),
                col("tipo_tabla").cast("string"),
                col("id_equipo").cast("string")
            ), 256
        ).alias("id_clasificacion"),
        col("temporada").alias("periodo"),
        current_timestamp().alias("fecha_carga")
    )

    # H) Cast final
    df_final = cast_dataframe_schema(df_id, prm_schema_hm)

    # Recalcular puntos y partidos_jugados cuando el scraping retorna 0
    df_final = df_final.select(
        *[col(c) for c in df_final.columns if c not in ("puntos", "partidos_jugados")],
        when(
            col("puntos").isNull() | (col("puntos") == 0),
            (col("partidos_ganados") * 3) + col("partidos_empatados")
        ).otherwise(col("puntos")).alias("puntos"),
        when(
            col("partidos_jugados").isNull() | (col("partidos_jugados") == 0),
            col("partidos_ganados") + col("partidos_empatados") + col("partidos_perdidos")
        ).otherwise(col("partidos_jugados")).alias("partidos_jugados")
    )

    return df_final, _sin_match
