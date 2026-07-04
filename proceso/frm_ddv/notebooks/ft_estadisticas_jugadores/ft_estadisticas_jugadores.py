# ==========================================================
# FUNCIONES DDV - FT_ESTADISTICAS_JUGADORES
# Proyecto: Liga 1 Perú
# Autor: Oscar García Del Águila
# ==========================================================

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, current_timestamp, sha2, concat_ws,
    sum as spark_sum, max as spark_max, avg as spark_avg,
    when
)
from utils_liga1 import cast_dataframe_schema


def carga_final_ft_estadisticas_jugadores(
    df_hm_estadisticas:      DataFrame, prm_cols_hm_estadisticas:     list,
    df_md_plantillas:        DataFrame, prm_cols_md_plantillas:        list,
    df_hm_plantillas_equipo: DataFrame, prm_cols_hm_plantillas_equipo: list,
    df_dm_equipos:           DataFrame, prm_cols_dm_equipos:           list,
    prm_schema:              dict
) -> DataFrame:
    """
    Fact de estadísticas totalizada por jugador-temporada:

      hm_estadisticas_jugadores  →  GROUP BY id_jugador + temporada → SUM stats
        INNER JOIN md_plantillas        ON id_jugador  → foto_url, nacionalidad
        LEFT  JOIN hm_plantillas_equipo ON id_jugador + temporada → id_equipo, posicion
        LEFT  JOIN dm_equipos           ON id_equipo   → nombre_equipo, alias_equipo

    La granularidad del DDV es UN REGISTRO POR JUGADOR Y TEMPORADA.
    Las stats (partidos, goles, asistencias, tarjetas, minutos) son el total
    de todas las competencias de esa temporada (Apertura + Clausura + etc.)
    """

    # ----------------------------------------------------------------
    # 1) md_plantillas: perfil del jugador
    # ----------------------------------------------------------------
    df_jug = (
        df_md_plantillas.select(*prm_cols_md_plantillas)
        .select(
            col("id_jugador").alias("id_jugador_dim"),
            col("foto_url"),
            col("nacionalidad_principal")
        )
    )

    # ----------------------------------------------------------------
    # 2) hm_plantillas_equipo: posicion por jugador-equipo-temporada
    #    (id_equipo ya viene del groupBy de hm_estadisticas, solo necesitamos posicion)
    # ----------------------------------------------------------------
    df_plt = (
        df_hm_plantillas_equipo.select(*prm_cols_hm_plantillas_equipo)
        .select(
            col("id_jugador").alias("id_jugador_plt"),
            col("id_equipo").alias("id_equipo_plt"),
            col("temporada").alias("temporada_plt"),
            col("posicion")
        )
        .dropDuplicates(["id_jugador_plt", "id_equipo_plt", "temporada_plt"])
    )

    # ----------------------------------------------------------------
    # 3) dm_equipos: nombre y alias
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
    # 4) Agregar hm_estadisticas por jugador + equipo + temporada
    #    (suma todas las competencias: Apertura, Clausura, etc. — por equipo)
    # ----------------------------------------------------------------
    df_agg = (
        df_hm_estadisticas.select(*prm_cols_hm_estadisticas)
        .groupBy("id_jugador", "id_tm", "jugador", "id_equipo", "temporada", "periodo")
        .agg(
            spark_sum("partidos_jugados").alias("partidos_jugados"),
            spark_sum("titularidades").alias("titularidades"),
            spark_avg("ppp").alias("ppp"),
            (spark_sum("goles") + spark_sum("penaltis_anotados")).alias("goles"),
            spark_sum("asistencias").alias("asistencias"),
            spark_sum("tarjetas_amarillas").alias("tarjetas_amarillas"),
            spark_sum("tarjeta_amarilla_roja").alias("tarjeta_amarilla_roja"),
            spark_sum("tarjetas_rojas").alias("tarjetas_rojas"),
            spark_sum("entrada_suplente").alias("entrada_suplente"),
            spark_sum("salida_suplente").alias("salida_suplente"),
            spark_sum("goles_en_contra").alias("goles_en_contra"),
            spark_sum("partidos_imbatido").alias("partidos_imbatido"),
            spark_sum("minutos_jugados").alias("minutos_jugados"),
            spark_max("datos_disponibles").alias("datos_disponibles")
        )
        # min_por_gol recalculado a nivel temporada: minutos / goles (0 si sin goles)
        .withColumn(
            "min_por_gol",
            when(col("goles") > 0, (col("minutos_jugados") / col("goles")).cast("int")).otherwise(0)
        )
        .withColumn(
            "id_estadistica_jugador",
            sha2(concat_ws("-", col("id_jugador"), col("id_equipo").cast("string"), col("temporada").cast("string")), 256)
        )
    )

    # ----------------------------------------------------------------
    # 5) JOINs para enriquecer
    # ----------------------------------------------------------------
    df_join = (
        df_agg.alias("e")
        .join(df_jug.alias("j"),
              col("e.id_jugador") == col("j.id_jugador_dim"), "inner")
        .join(df_plt.alias("p"),
              (col("e.id_jugador") == col("p.id_jugador_plt")) &
              (col("e.id_equipo")  == col("p.id_equipo_plt")) &
              (col("e.temporada")  == col("p.temporada_plt")), "left")
        .join(df_dim.alias("d"),
              col("e.id_equipo") == col("d.id_equipo_dim"), "left")
        .select(
            col("e.id_estadistica_jugador"),
            col("e.id_jugador"),
            col("e.id_tm"),
            col("e.jugador"),
            col("j.foto_url"),
            col("e.id_equipo"),
            col("d.nombre_equipo"),
            col("d.alias_equipo"),
            col("e.temporada"),
            col("e.partidos_jugados"),
            col("e.titularidades"),
            col("e.ppp"),
            col("e.goles"),
            col("e.asistencias"),
            col("e.tarjetas_amarillas"),
            col("e.tarjeta_amarilla_roja"),
            col("e.tarjetas_rojas"),
            col("e.entrada_suplente"),
            col("e.salida_suplente"),
            col("e.goles_en_contra"),
            col("e.partidos_imbatido"),
            col("e.min_por_gol"),
            col("e.minutos_jugados"),
            col("e.datos_disponibles"),
            col("p.posicion"),
            col("j.nacionalidad_principal"),
      