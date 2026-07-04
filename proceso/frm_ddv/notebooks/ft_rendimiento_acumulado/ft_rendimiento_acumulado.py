# ==========================================================
# FUNCIONES DDV - FT_RENDIMIENTO_ACUMULADO
# Proyecto: Liga 1 Perú
# Autor: Oscar García Del Águila
# ==========================================================

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum, count, max, sha2, concat_ws, current_timestamp
from utils_liga1 import cast_dataframe_schema


def carga_final_ft_rendimiento_acumulado(
    df_hm_clasificacion: DataFrame, prm_cols_hm_clasificacion: list,
    df_hm_valoracion:    DataFrame, prm_cols_hm_valoracion:    list,
    df_dm_equipos:       DataFrame, prm_cols_dm_equipos:       list,
    prm_schema: dict
) -> DataFrame:
    """
    Fact de rendimiento acumulado por equipo y temporada (suma de todos los torneos: Apertura + Clausura + otros).

    Combina:
      - hm_tablas_clasificacion → estadísticas de competición agregadas por equipo y temporada
      - hm_valoracion_equipos   → valor económico en esa misma temporada
      - dm_equipos              → nombre y alias del equipo
    """

    # ----------------------------------------------------------------
    # 1) Valoración: max por id_equipo + temporada (defensive)
    # ----------------------------------------------------------------
    df_val = (
        df_hm_valoracion.select(*prm_cols_hm_valoracion)
        .groupBy("id_equipo", "temporada")
        .agg(
            max("valor_total").alias("valor_total"),
            max("valor_medio").alias("valor_medio")
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
    # 3) Agregar clasificación por equipo + temporada
    # ----------------------------------------------------------------
    df_agg = (
        df_hm_clasificacion.select(*prm_cols_hm_clasificacion)
        .groupBy("id_equipo", "temporada")
        .agg(
            count("tipo_tabla").alias("torneos_disputados"),
            sum("partidos_jugados").alias("total_pj"),
            sum("partidos_ganados").alias("total_pg"),
            sum("partidos_empatados").alias("total_pe"),
            sum("partidos_perdidos").alias("total_pp"),
            sum("goles_favor").alias("total_goles_favor"),
            sum("goles_contra").alias("total_goles_contra"),
            (sum("goles_favor") - sum("goles_contra")).alias("total_diferencia_goles"),
            sum("puntos").alias("total_puntos")
        )
    )

    # ----------------------------------------------------------------
    # 4) JOIN progresivo
    # ----------------------------------------------------------------
    df_join = (
        df_agg.alias("agg")
        .join(
            df_val.alias("v"),
            (col("agg.id_equipo") == col("v.id_equipo")) &
            (col("agg.temporada") == col("v.temporada")),
            "left"
        )
        .join(
            df_dim.alias("d"),
            col("agg.id_equipo") == col("d.id_equipo_dim"),
            "left"
        )
        .select(
            sha2(concat_ws("-", col("agg.id_equipo").cast("string"), col("agg.temporada").cast("string")), 256).alias("id_acumulado"),
            col("agg.id_equipo"),
            col("d.nombre_equipo"),
            col("d.alias_equipo"),
            col("agg.temporada"),
            col("torneos_disputados"),
            col("total_pj"),
            col("total_pg"),
            col("total_pe"),
            col("total_pp"),
            col("total_goles_favor"),
            col("total_goles_contra"),
            col("total_diferencia_goles"),
            col("total_puntos"),
            col("v.valor_total"),
            col("v.valor_medio"),
            current_timestamp().alias("fecha_carga"),
            col("agg.temporada").alias("periodo")
        )
    )

    return cast_dataframe_schema(df_join, prm_schema)
