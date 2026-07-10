# ==========================================================
# FUNCIONES DDV - FT_EVOLUCION_VALORACION
# Proyecto: Liga 1 Perú
# Autor: Oscar García Del Águila
# ==========================================================

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp
from utils_liga1 import cast_dataframe_schema


def carga_final_ft_evolucion_valoracion(
    df_hm_valoracion: DataFrame, prm_cols_hm_valoracion: list,
    df_dm_equipos:    DataFrame, prm_cols_dm_equipos:    list,
    prm_schema: dict
) -> DataFrame:
    """
    Fact de evolución económica por equipo y temporada.
    Enriquece hm_valoracion_equipos con el nombre del equipo desde dm_equipos.
    """

    # ----------------------------------------------------------------
    # 1) dm_equipos: nombre + alias
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
    # 2) JOIN hm_valoracion → dm_equipos
    # ----------------------------------------------------------------
    df_join = (
        df_hm_valoracion.select(*prm_cols_hm_valoracion).alias("v")
        .join(df_dim.alias("d"), col("v.id_equipo") == col("d.id_equipo_dim"), "left")
        .select(
            col("v.id_valoracion_equipo"),
            col("v.id_equipo"),
            col("d.nombre_equipo"),
            col("d.alias_equipo"),
            col("v.temporada"),
            col("v.jugadores_en_plantilla"),
            col("v.edad_promedio"),
            col("v.extranjeros"),
            col("v.valor_medio"),
            col("v.valor_total"),
            current_timestamp().alias("fecha_carga"),
            col("v.periodo")
        )
    )

    return cast_dataframe_schema(df_join, prm_schema)
