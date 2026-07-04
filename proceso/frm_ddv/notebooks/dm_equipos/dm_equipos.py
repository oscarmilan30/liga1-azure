# ==========================================================
# FUNCIONES DDV - DM_EQUIPOS
# Proyecto: Liga 1 Perú
# Autor: Oscar García Del Águila
# ==========================================================

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, current_timestamp, row_number
from utils_liga1 import cast_dataframe_schema


def carga_final_dm_equipos(
    df_md_catalogo:   DataFrame, prm_cols_md_catalogo:   list,
    df_md_equipos:    DataFrame, prm_cols_md_equipos:    list,
    df_md_estadios:   DataFrame, prm_cols_md_estadios:   list,
    df_hm_valoracion: DataFrame, prm_cols_hm_valoracion: list,
    prm_schema: dict
) -> DataFrame:
    """
    Dimensión principal de equipos para el modelo DDV.

    Combina:
      - md_catalogo_equipos   → id_equipo, nombre_equipo
      - md_equipos            → alias_equipo, url_equipo
      - md_estadios           → estadio, capacidad, aforo (último registro por equipo)
      - hm_valoracion_equipos → valoración económica más reciente por equipo
    """

    # ----------------------------------------------------------------
    # 1) Valoración más reciente por equipo (mayor temporada)
    # ----------------------------------------------------------------
    w_val = Window.partitionBy("id_equipo").orderBy(col("temporada").desc())
    df_val_latest = (
        df_hm_valoracion.select(*prm_cols_hm_valoracion)
        .select(
            col("id_equipo"),
            col("temporada").alias("temporada_ultima_valoracion"),
            col("valor_total").alias("valor_total_actual"),
            col("valor_medio").alias("valor_medio_actual"),
            col("jugadores_en_plantilla").alias("jugadores_plantilla_actual"),
            col("edad_promedio").alias("edad_promedio_actual"),
            col("extranjeros").alias("extranjeros_actual"),
            row_number().over(w_val).alias("rn")
        )
        .filter(col("rn") == 1)
        .drop("rn")
    )

    # ----------------------------------------------------------------
    # 2) Estadio más reciente por equipo (mayor fecha_carga)
    # ----------------------------------------------------------------
    w_est = Window.partitionBy("id_equipo").orderBy(col("fecha_carga").desc())
    df_est = (
        df_md_estadios.select(*prm_cols_md_estadios)
        .select(
            col("id_equipo"), col("estadio"), col("capacidad"), col("aforo"), col("fecha_carga"),
            row_number().over(w_est).alias("rn")
        )
        .filter(col("rn") == 1)
        .drop("rn", "fecha_carga")
    )

    # ----------------------------------------------------------------
    # 3) md_equipos: alias + url
    # ----------------------------------------------------------------
    df_eq = df_md_equipos.select(*prm_cols_md_equipos)

    # ----------------------------------------------------------------
    # 4) JOIN progresivo desde catálogo
    # ----------------------------------------------------------------
    df_join = (
        df_md_catalogo.select(*prm_cols_md_catalogo).alias("c")
        .join(df_eq.alias("e"),         col("c.id_equipo") == col("e.id_equipo"),  "left")
        .join(df_est.alias("s"),        col("c.id_equipo") == col("s.id_equipo"),  "left")
        .join(df_val_latest.alias("v"), col("c.id_equipo") == col("v.id_equipo"), "left")
        .select(
            col("c.id_equipo"),
            col("c.nombre_equipo"),
            col("e.alias_equipo"),
            col("e.url_equipo"),
            col("s.estadio"),
            col("s.capacidad"),
            col("s.aforo"),
            col("v.valor_total_actual"),
            col("v.valor_medio_actual"),
            col("v.jugadores_plantilla_actual"),
            col("v.edad_promedio_actual"),
            col("v.extranjeros_actual"),
            col("v.temporada_ultima_valoracion"),
            current_timestamp().alias("fecha_carga")
        )
    )

    return cast_dataframe_schema(df_join, prm_schema)
