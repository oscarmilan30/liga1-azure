# ==========================================================
# FUNCIONES DDV - FT_ENTRENADORES_HISTORICO
# Proyecto: Liga 1 Perú
# Autor: Oscar García Del Águila
# ==========================================================

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp
from utils_liga1 import cast_dataframe_schema


def carga_final_ft_entrenadores_historico(
    df_hm_entrenadores: DataFrame, prm_cols_hm_entrenadores: list,
    df_md_entrenadores: DataFrame, prm_cols_md_entrenadores: list,
    df_dm_equipos:      DataFrame, prm_cols_dm_equipos:      list,
    prm_schema: dict
) -> DataFrame:
    """
    Fact histórico de entrenadores: vincula cada período entrenador-equipo
    con el perfil del técnico (md_entrenadores) y el nombre del equipo (dm_equipos).

    JOIN:
      hm_entrenadores_equipo
        INNER JOIN md_entrenadores ON id_entrenador
        LEFT  JOIN dm_equipos      ON id_equipo
    """

    # ----------------------------------------------------------------
    # 1) md_entrenadores: perfil del técnico
    # ----------------------------------------------------------------
    df_ent = (
        df_md_entrenadores.select(*prm_cols_md_entrenadores)
        .select(
            col("id_entrenador").alias("id_entrenador_dim"),
            col("entrenador"),
            col("edad"),
            col("nacionalidad_principal"),
            col("segunda_nacionalidad")
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
        df_hm_entrenadores.select(*prm_cols_hm_entrenadores).alias("he")
        .join(df_ent.alias("e"), col("he.id_entrenador") == col("e.id_entrenador_dim"), "inner")
        .join(df_dim.alias("d"), col("he.id_equipo")     == col("d.id_equipo_dim"),     "left")
        .select(
            col("he.id_entrenador_equipo"),
            col("he.id_equipo"),
            col("d.nombre_equipo"),
            col("d.alias_equipo"),
            col("he.id_entrenador"),
            col("e.entrenador"),
            col("e.edad"),
            col("e.nacionalidad_principal"),
            col("e.segunda_nacionalidad"),
            col("he.temporada"),
            col("he.fecha_inicio"),
            col("he.fecha_termino"),
            col("he.tiempo_en_cargo"),
            col("he.partidos"),
            col("he.ppp"),
            col("he.entrenador_activo"),
            current_timestamp().alias("fecha_carga"),
            col("he.periodo")
        )
    )

    return cast_dataframe_schema(df_join, prm_schema)
