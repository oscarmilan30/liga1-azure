# ==========================================================
# FUNCIONES DDV - FT_PLANTILLAS_HISTORICO
# Proyecto: Liga 1 Perú
# Autor: Oscar García Del Águila
# ==========================================================

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp
from utils_liga1 import cast_dataframe_schema


def carga_final_ft_plantillas_historico(
    df_hm_plantillas: DataFrame, prm_cols_hm_plantillas: list,
    df_md_plantillas: DataFrame, prm_cols_md_plantillas: list,
    df_dm_equipos:    DataFrame, prm_cols_dm_equipos:    list,
    prm_schema: dict
) -> DataFrame:
    """
    Fact histórico de plantillas: vincula cada registro jugador-equipo-temporada
    con el perfil completo del jugador (md_plantillas) y el nombre del equipo (dm_equipos).

    JOIN:
      hm_plantillas_equipo
        INNER JOIN md_plantillas ON id_jugador
        LEFT  JOIN dm_equipos    ON id_equipo
    """

    # ----------------------------------------------------------------
    # 1) md_plantillas: perfil del jugador
    # ----------------------------------------------------------------
    df_jug = (
        df_md_plantillas.select(*prm_cols_md_plantillas)
        .select(
            col("id_jugador").alias("id_jugador_dim"),
            col("jugador"),
            col("edad"),
            col("nacionalidad_principal"),
            col("segunda_nacionalidad"),
            col("altura"),
            col("pie"),
            col("valor_mercado"),
            col("foto_url")
        )
    )

    # ----------------------------------------------------------------
    # 2) dm_equipos: nombre + alias
    # ----------------------------------------------------------------
    df_dim = (
        df_dm_equipos.select(*prm_cols_dm_equipos)
        .select(
            col("id_equipo").alias("id_equipo_dim"),
            col("nombre_equipo"),
            col("alias_equipo")
        )
    )

    # ----------------------------------------------------------------
    # 3) JOINs
    # ----------------------------------------------------------------
    df_join = (
        df_hm_plantillas.select(*prm_cols_hm_plantillas).alias("hp")
        .join(df_jug.alias("j"), col("hp.id_jugador") == col("j.id_jugador_dim"), "inner")
        .join(df_dim.alias("d"), col("hp.id_equipo")  == col("d.id_equipo_dim"),  "left")
        .select(
            col("hp.id_plantilla_equipo"),
            col("hp.id_equipo"),
            col("d.nombre_equipo"),
            col("d.alias_equipo"),
            col("hp.id_jugador"),
            col("hp.id_tm"),
            col("j.jugador"),
            col("hp.temporada"),
            col("hp.numero_camiseta"),
            col("hp.posicion"),
            col("hp.fecha_fichaje"),
            col("j.edad"),
            col("j.nacionalidad_principal"),
            col("j.segunda_nacionalidad"),
            col("j.altura"),
            col("j.pie"),
            col("j.valor_mercado"),
            col("j.foto_url"),
            current_timestamp().alias("fecha_carga"),
            col("hp.periodo")
        )
    )

    return cast_dataframe_schema(df_join, prm_schema)
