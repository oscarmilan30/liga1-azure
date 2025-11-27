# ==========================================================
# HM_CAMPEONES
# Proyecto: Liga 1 Perú
# Autor: Oscar García Del Águila
# ==========================================================

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    trim,
    lower,
    current_timestamp,
    sha2,
    concat_ws,
)
from utils_liga1 import cast_dataframe_schema


def _norm(col_expr):
    """
    Normaliza un string para comparación:
    - trim
    - lower
    Si viene NULL se mantiene NULL.
    """
    return lower(trim(col_expr))


def carga_final_hm_campeones(
    df_md_catalogo_equipos: DataFrame,
    df_raw_campeones: DataFrame,
    prm_cols_raw_campeones_hm: list,
    prm_schema_hm: dict,
) -> DataFrame:
    """
    Transformación HM_CAMPEONES:

    - Lee MD catálogo equipos para mapear campeon/subcampeon a IDs.
    - Lee RAW campeones (JSON FotMob) vía generar_udv_json.
    - NO usa withColumn, distinct, collect ni lambda.
    - Todo lo configurable viene del YAML:
        * cols_raw_campeones_hm
        * schema_hm
    """

    # ------------------------------------------------------
    # 1) MD_CATALOGO_EQUIPOS: columnas mínimas necesarias
    #    (no vienen de YAML porque son estables en tu modelo)
    # ------------------------------------------------------
    df_md_sel = df_md_catalogo_equipos.select(
        col("id_equipo"),
        col("nombre_equipo"),
        col("nombre_fotmob"),
        col("alias"),
    )

    # ------------------------------------------------------
    # 2) RAW CAMPEONES: seleccionar columnas definidas en YAML
    #    (fecha_carga, fuente, temporada, campeon, subcampeon)
    # ------------------------------------------------------
    df_raw_sel = df_raw_campeones.select(
        *(col(c) for c in prm_cols_raw_campeones_hm)
    )

    # ------------------------------------------------------
    # 3) Join para obtener id_equipo_campeon
    #    Matchea campeon contra nombre_fotmob / nombre_equipo / alias
    # ------------------------------------------------------
    df_join_campeon = (
        df_raw_sel.alias("r")
        .join(
            df_md_sel.alias("c"),
            (
                _norm(col("r.campeon")) == _norm(col("c.nombre_fotmob"))
            )
            | (
                _norm(col("r.campeon")) == _norm(col("c.nombre_equipo"))
            )
            | (
                _norm(col("r.campeon")) == _norm(col("c.alias"))
            ),
            "left",
        )
        .select(
            col("r.temporada").alias("temporada"),
            col("r.campeon").alias("campeon"),
            col("r.subcampeon").alias("subcampeon"),
            col("r.fuente").alias("fuente"),
            col("c.id_equipo").alias("id_equipo_campeon"),
        )
    )

    # ------------------------------------------------------
    # 4) Join para obtener id_equipo_subcampeon
    # ------------------------------------------------------
    df_join_both = (
        df_join_campeon.alias("j")
        .join(
            df_md_sel.alias("s"),
            (
                _norm(col("j.subcampeon")) == _norm(col("s.nombre_fotmob"))
            )
            | (
                _norm(col("j.subcampeon")) == _norm(col("s.nombre_equipo"))
            )
            | (
                _norm(col("j.subcampeon")) == _norm(col("s.alias"))
            ),
            "left",
        )
        .select(
            # id_campeonato: hash determinístico de temporada + campeón + subcampeón
            sha2(
                concat_ws(
                    "|",
                    col("j.temporada").cast("string"),
                    col("j.campeon"),
                    col("j.subcampeon"),
                ),
                256,
            ).alias("id_campeonato"),
            col("j.id_equipo_campeon").alias("id_equipo_campeon"),
            col("s.id_equipo").alias("id_equipo_subcampeon"),
            col("j.temporada").alias("temporada"),
            current_timestamp().alias("fecha_carga"),
            col("j.fuente").alias("fuente"),
            col("j.temporada").alias("periodo")
        )
    )

    # ------------------------------------------------------
    # 5) Cast al schema final definido en YAML
    # ------------------------------------------------------
    df_final = cast_dataframe_schema(
        df_join_both,
        prm_schema_hm,
        date_format="yyyy-MM-dd",
    )

    return df_final
