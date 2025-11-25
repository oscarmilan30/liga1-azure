# ==========================================================
# HM – TABLAS CLASIFICACION
# Proyecto: Liga 1 Perú
# Autor: Oscar García Del Águila
# ==========================================================

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, explode_outer, from_json, trim, lower,
    sha2, concat_ws, current_timestamp, split,
    regexp_replace,map_keys
)
from pyspark.sql.types import ArrayType, MapType, StringType

from utils_liga1 import (
    cast_dataframe_schema,
    rename_columns
)

# ==========================================================
# 1) EXPLOSIÓN DINÁMICA JSON
# ==========================================================
def transformar_raw_tablas(df_raw: DataFrame, campo_json: str) -> DataFrame:

    raw_cols = df_raw.columns

    # Parse JSON
    df1 = df_raw.select(
        *(col(c) for c in raw_cols),
        from_json(
            col(campo_json),
            ArrayType(
                MapType(
                    StringType(),
                    ArrayType(MapType(StringType(), StringType()))
                )
            )
        ).alias("json_array")
    )

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
    # 🔥 CLAVES DINÁMICAS DEL MAP
    # ================================
    df_keys = df4.select(map_keys(col("json_item")).alias("keys"))

    # extraemos claves sin collect → explosion controlada
    unique_keys = (
        df_keys
        .select(explode_outer("keys").alias("k"))
        .distinct()
        .rdd.map(lambda r: r["k"])
        .collect()
    )

    # ================================
    # 🔥 EXPANSIÓN DINÁMICA
    # ================================
    df_expanded = df4.select(
        *(col(c) for c in raw_cols),
        col("tipo_tabla"),
        *[col("json_item").getItem(k).alias(k) for k in unique_keys]
    )

    return df_expanded



# ==========================================================
# 2) TRANSFORMACIÓN COMPLETA HM
# ==========================================================
def carga_final_hm_tablas_clasificacion(
    df_raw: DataFrame,
    df_md_catalogo_equipos: DataFrame,
    prm_cols_raw_clasificacion_hm: list,
    prm_rename_columns_hm: dict,
    prm_schema_hm: dict,
    campo_json: str
) -> DataFrame:

    # A) Explosión JSON
    df_expanded = transformar_raw_tablas(df_raw, campo_json)

    # B) separación goles
    df_goles = df_expanded.select(
        *df_expanded.columns,
        split(col("goles_a_favor_contra"), "-").getItem(0).alias("goles_favor_tmp"),
        split(col("goles_a_favor_contra"), "-").getItem(1).alias("goles_contra_tmp")
    )

    # C) columnas YAML
    df_sel = df_goles.select(*(col(c) for c in prm_cols_raw_clasificacion_hm))

    # D) rename YAML
    df_ren = rename_columns(df_sel, prm_rename_columns_hm)

    # E) diferencia_goles → absoluto
    df_abs = df_ren.select(
        *[col(c) for c in df_ren.columns if c != "diferencia_goles"],
        regexp_replace(col("diferencia_goles"), r"[^0-9]", "").cast("int").alias("diferencia_goles")
    )

    # F) join catálogo usando nombre_fotmob
    df_cat = df_md_catalogo_equipos.select(
        col("id_equipo"),
        lower(trim(col("nombre_fotmob"))).alias("nombre_fotmob_norm")
    )

    df_join = df_abs.join(
        df_cat,
        lower(trim(col("equipo"))) == col("nombre_fotmob_norm"),
        "left"
    ).drop("nombre_fotmob_norm")

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

    return df_final
