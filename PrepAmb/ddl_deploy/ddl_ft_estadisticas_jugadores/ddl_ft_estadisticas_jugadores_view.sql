-- ==========================================================
-- DDV - VISTA FT_ESTADISTICAS_JUGADORES
-- Proyecto: Liga 1 Perú
-- ==========================================================

CREATE OR REPLACE VIEW ${catalog_name}.vw_ddv.ft_estadisticas_jugadores_vw 
AS
SELECT
    id_estadistica_jugador,
    id_jugador,
    id_tm,
    jugador,
    foto_url,
    id_equipo,
    nombre_equipo,
    alias_equipo,
    temporada,
    partidos_jugados,
    titularidades,
    ppp,
    goles,
    asistencias,
    tarjetas_amarillas,
    tarjeta_amarilla_roja as segunda_amarilla,
    tarjetas_rojas,
    entrada_suplente,
    salida_suplente,
    goles_en_contra,
    partidos_imbatido,
    min_por_gol,
    minutos_jugados,
    datos_disponibles,
    posicion,
    CASE
        WHEN posicion = 'Portero' THEN 'POR'
        WHEN posicion IN ('Defensa Central', 'Defensa') THEN 'DFC'
        WHEN posicion = 'Lateral Derecho' THEN 'LD'
        WHEN posicion = 'Lateral Izquierdo' THEN 'LI'
        WHEN posicion = 'Mediocampista Defensivo' THEN 'MCD'
        WHEN posicion IN ('Mediocampista Central', 'Mediocampista','Interior Izquierdo','Mediocampista Izquierdo','Mediocampista Derecho') THEN 'MC'
        WHEN posicion = 'Mediocampista Ofensivo' THEN 'MCO'
        WHEN posicion = 'Extremo Izquierdo' THEN 'EI'
        WHEN posicion = 'Extremo Derecho' THEN 'ED'
        WHEN posicion = 'Delantero Centro' THEN 'DC'
        ELSE 'OTRO'
    END AS posicion_xi
FROM ${catalog_name}.tb_ddv.ft_estadisticas_jugadores
WHERE datos_disponibles = true;