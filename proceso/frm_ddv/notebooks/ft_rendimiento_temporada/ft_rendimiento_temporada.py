# ==========================================================
# FUNCIONES DDV - FT_RENDIMIENTO_TEMPORADA
# Proyecto: Liga 1 Perú
# Autor: Oscar García Del Águila
# ==========================================================

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp
from utils_liga1 import cast_dataframe_schema


def carga_final_ft_rendimiento_temporada(
    df_hm_clasificacion: DataFrame, prm_cols_hm_clasificacion: list,
    df_hm_valoracion:    DataFrame, prm_cols_hm_valoracion:    list,
    df_dm_equipos:       DataFrame, prm_cols_dm_equipos:       list,
    prm_schema: dict
) -> DataFrame:
    """
    Fact de rendimiento por equipo, temporada y tipo de tabla (Apertura/Clausura/General).

    Combina:
      - hm_tablas_clasificacion → posición y estadísticas de competición
      - hm_valoracion_equipos   → valor económico en esa misma temporada
      - dm_equipos              → nombre y alias del equipo
    """

    # ----------------------------------------------------------------
    # 1) Valoración: id_equipo + temporada + valores
    # ----------------------------------------------------------------
    df_val = (
        df_hm_valoracion.select(*prm_cols_hm_valoracion)
        .select(
            col("id_equipo").alias("id_equipo_val"),
            col("temporada").alias("temporada_val"),
            col("valor_total"),
            col("valor_medio")
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
    # 3) JOIN progresivo desde clasificación
    # ----------------------------------------------------------------
    df_join = (
        df_hm_clasificacion.select(*prm_cols_hm_clasificacion).alias("cl")
        .join(
            df_val.alias("v"),
            (col("cl.id_equipo") == col("v.id_equipo_val")) &
            (col("cl.temporada")  == col("v.temporada_val")),
            "left"
        )
        .join(
            df_dim.alias("d"),
            col("cl.id_equipo") == col("d.id_equipo_dim"),
            "left"
        )
        .select(
            col("cl.id_clasificacion"),
            col("cl.id_equipo"),
            col("d.nombre_equipo"),
            col("d.alias_equipo"),
            col("cl.temporada"),
            col("cl.tipo_tabla"),
            col("cl.posicion"),
            col("cl.partidos_jugados"),
            col("cl.partidos_ganados"),
            col("cl.partidos_empatados"),
            col("cl.partidos_perdidos"),
            col("cl.goles_favor"),
            col("cl.goles_contra"),
            col("cl.diferencia_goles"),
            col("cl.puntos"),
            col("v.valor_total"),
            col("v.valor_medio"),
            current_timestamp().alias("fecha_carga"),
            col("cl.periodo")
        )
    )

    return cast_dataframe_schema(df_join, prm_schema)
