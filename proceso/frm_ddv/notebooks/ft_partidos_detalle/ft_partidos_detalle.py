# ==========================================================
# FUNCIONES DDV - FT_PARTIDOS_DETALLE
# Proyecto: Liga 1 Perú
# Autor: Oscar García Del Águila
# ==========================================================

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp


def carga_final_ft_partidos_detalle(
    df_hm_partidos:         DataFrame,
    df_hm_estadisticas:     DataFrame,
    df_dm_equipos:          DataFrame,
    prm_cols_base:          list,
    prm_cols_excluir_stats: list,
    prm_cols_dm_equipos:    list
) -> DataFrame:
    """
    Fact de partidos detallados: combina hm_partidos con todas las estadísticas
    de hm_estadisticas_partidos y enriquece con nombres de equipo de dm_equipos.

    prm_cols_base:          columnas de hm_partidos a incluir en el bloque base
    prm_cols_excluir_stats: columnas de hm_estadisticas redundantes con el bloque base
    prm_cols_dm_equipos:    columnas a seleccionar de dm_equipos (id_equipo, nombre, alias)
    """

    # ----------------------------------------------------------------
    # 1) Preparar hm_estadisticas: eliminar columnas redundantes
    # ----------------------------------------------------------------
    stats_cols_keep = [
        c for c in df_hm_estadisticas.columns
        if c not in prm_cols_excluir_stats and c != "id_partido"
    ]
    df_stats = df_hm_estadisticas.select("id_partido", *stats_cols_keep)

    # ----------------------------------------------------------------
    # 2) dm_equipos pre-seleccionado → nombre y alias por equipo local y visitante
    # ----------------------------------------------------------------
    df_dm = df_dm_equipos.select(*prm_cols_dm_equipos)
    df_local = df_dm.select(
        col("id_equipo").alias("id_equipo_local_dim"),
        col("nombre_equipo").alias("nombre_equipo_local"),
        col("alias_equipo").alias("alias_equipo_local")
    )
    df_visit = df_dm.select(
        col("id_equipo").alias("id_equipo_visitante_dim"),
        col("nombre_equipo").alias("nombre_equipo_visitante"),
        col("alias_equipo").alias("alias_equipo_visitante")
    )

    # ----------------------------------------------------------------
    # 3) Base: hm_partidos con columnas seleccionadas
    # ----------------------------------------------------------------
    df_base = df_hm_partidos.select(*prm_cols_base)

    # ----------------------------------------------------------------
    # 4) JOINs
    # ----------------------------------------------------------------
    df_join = (
        df_base.alias("p")
        .join(df_stats.alias("s"),  col("p.id_partido") == col("s.id_partido"), "left")
        .join(df_local.alias("el"), col("p.id_equipo_local")     == col("el.id_equipo_local_dim"),    "left")
        .join(df_visit.alias("ev"), col("p.id_equipo_visitante") == col("ev.id_equipo_visitante_dim"), "left")
    )

    # ----------------------------------------------------------------
    # 5) SELECT final ordenado
    # ----------------------------------------------------------------
    df_final = df_join.select(
        col("p.id_partido"),
        col("p.id_equipo_local"),
        col("el.nombre_equipo_local"),
        col("el.alias_equipo_local"),
        col("p.id_equipo_visitante"),
        col("ev.nombre_equipo_visitante"),
        col("ev.alias_equipo_visitante"),
        col("p.temporada"),
        col("p.fecha_partido"),
        col("p.marcador"),
        col("p.goles_local"),
        col("p.goles_visitante"),
        *[col(f"s.{c}") for c in stats_cols_keep],
        col("p.url"),
        col("p.fuente_partido"),
        current_timestamp().alias("fecha_carga"),
        col("p.periodo")
    )

    return df_final
