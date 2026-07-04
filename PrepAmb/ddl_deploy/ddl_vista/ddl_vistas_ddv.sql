-- ============================================================
-- DDL VISTAS DDV — Liga 1 Perú (extraído de ddl_vista notebook)
-- ============================================================

CREATE OR REPLACE VIEW catalog_liga1.vw_ddv.ft_rendimiento_general_vw 
AS
SELECT
    id_acumulado,
    id_equipo,
    nombre_equipo,
    alias_equipo,
    temporada,
    torneos_disputados,
    total_pj,
    total_pg,
    total_pe,
    total_pp,
    total_goles_favor,
    total_goles_contra,
    total_diferencia_goles,
    total_puntos,
    valor_total,
    valor_medio,
    'Acumulado' AS tipo_tabla
FROM catalog_liga1.vw_ddv.ft_rendimiento_acumulado_vw;

CREATE OR REPLACE VIEW catalog_liga1.vw_ddv.dm_temporada_vw AS
SELECT DISTINCT
    temporada
FROM (
    SELECT temporada FROM catalog_liga1.vw_ddv.ft_partidos_detalle_vw
    UNION
    SELECT temporada FROM catalog_liga1.vw_ddv.ft_rendimiento_temporada_vw
    UNION
    SELECT temporada FROM catalog_liga1.vw_ddv.ft_rendimiento_acumulado_vw
)
WHERE temporada IS NOT NULL;

CREATE OR REPLACE VIEW catalog_liga1.vw_ddv.dm_tipo_tabla_vw AS
SELECT DISTINCT
    tipo_tabla
FROM catalog_liga1.vw_ddv.ft_rendimiento_temporada_vw

UNION

SELECT
    'Acumulado' AS tipo_tabla;


CREATE OR REPLACE VIEW catalog_liga1.vw_ddv.ft_rendimiento_posiciones_vw AS

SELECT
    id_clasificacion                         AS id_rendimiento,
    id_equipo,
    nombre_equipo,
    alias_equipo,
    temporada,
    tipo_tabla,
    posicion,
    partidos_jugados,
    partidos_ganados,
    partidos_empatados,
    partidos_perdidos,
    goles_favor,
    goles_contra,
    diferencia_goles,
    puntos,
    valor_total,
    valor_medio,
    CAST(NULL AS INT)                        AS torneos_disputados
FROM catalog_liga1.vw_ddv.ft_rendimiento_temporada_vw

UNION ALL

SELECT
    id_acumulado                             AS id_rendimiento,
    id_equipo,
    nombre_equipo,
    alias_equipo,
    temporada,
    tipo_tabla,
    ROW_NUMBER() OVER (
        PARTITION BY temporada
        ORDER BY
            total_puntos DESC,
            total_diferencia_goles DESC,
            total_goles_favor DESC
    )                                        AS posicion,
    total_pj                                 AS partidos_jugados,
    total_pg                                 AS partidos_ganados,
    total_pe                                 AS partidos_empatados,
    total_pp                                 AS partidos_perdidos,
    total_goles_favor                        AS goles_favor,
    total_goles_contra                       AS goles_contra,
    total_diferencia_goles                   AS diferencia_goles,
    total_puntos                             AS puntos,
    valor_total,
    valor_medio,
    torneos_disputados
FROM catalog_liga1.vw_ddv.ft_rendimiento_general_vw;

CREATE OR REPLACE VIEW catalog_liga1.vw_ddv.ft_partidos_equipo_vw AS
-- LOCAL
SELECT
    id_partido,
    temporada,
    fecha_partido,
    estado,
    fecha_stats,
    id_equipo_local AS id_equipo,
    nombre_equipo_local AS nombre_equipo,
    alias_equipo_local AS alias_equipo,
    id_equipo_visitante AS id_rival,
    'LOCAL' AS rol_equipo,
    goles_local AS goles_favor,
    goles_visitante AS goles_contra,
    goles_local - goles_visitante AS diferencia_goles,
    CASE
        WHEN marcador = 'sin informacion' THEN 'S/A'
        WHEN goles_local > goles_visitante THEN 'G'
        WHEN goles_local = goles_visitante THEN 'E'
        ELSE 'P'
    END AS resultado,
    CASE
        WHEN marcador = 'sin informacion' THEN 1
        ELSE 0
    END AS partido_sin_info,
    CASE
        WHEN marcador = 'sin informacion' THEN 'Sin jugar/Aplazado'
        ELSE 'Disputado'
    END AS estado_partido,

    posesion_local_pct AS posesion_pct,
    tiros_totales_local AS tiros_totales,
    disparos_puerta_local AS disparos_puerta,
    CASE
        WHEN tiros_totales_local > 0 THEN ROUND(disparos_puerta_local / tiros_totales_local, 4)
    END AS precision_tiro_pct,
    CASE
        WHEN tiros_totales_local > 0 THEN ROUND(goles_local / tiros_totales_local, 4)
    END AS conversion_gol_pct,

    pases_local AS pases,
    pases_precisos_local AS pases_precisos,
    pases_precisos_local_pct AS pases_precisos_pct,
    regates_realizados_local AS regates_realizados,
    regates_realizados_local_pct AS regates_realizados_pct,
    saques_esquina_local AS saques_esquina,
    gran_probabilidad_local AS gran_probabilidad,
    tiros_area_local AS tiros_area,
    tiros_fuera_area_local AS tiros_fuera_area,
    tiros_bloqueados_local AS tiros_bloqueados,
    duelos_ganados_local AS duelos_ganados,
    entradas_local AS entradas,
    despejes_local AS despejes,
    bloqueos_local AS bloqueos,
    intercepciones_local AS intercepciones,
    tarjetas_amarillas_local AS tarjetas_amarillas,
    tarjetas_rojas_local AS tarjetas_rojas,
    fueras_juego_local AS fueras_juego,
    paradas_portero_local AS paradas_portero,
    tiros_largos_precisos_local AS tiros_largos_precisos,
    tiros_largos_precisos_local_pct AS tiros_largos_precisos_pct,
    tiros_libres_precisos_local AS tiros_libres_precisos,
    tiros_libres_precisos_local_pct AS tiros_libres_precisos_pct,
    grandes_oportunidades_perdidas_local AS grandes_oportunidades_perdidas,
    toques_area_rival_local AS toques_area_rival,
    faltas_local AS faltas,
    centros_precisos_local AS centros_precisos,
    mitad_propia_local AS mitad_propia,
    mitad_rival_local AS mitad_rival,
    lanzamientos_local AS lanzamientos,
    url,
    fuente_partido
FROM catalog_liga1.vw_ddv.ft_partidos_detalle_vw

UNION ALL

-- VISITANTE
SELECT
    id_partido,
    temporada,
    fecha_partido,
    estado,
    fecha_stats,
    id_equipo_visitante AS id_equipo,
    nombre_equipo_visitante AS nombre_equipo,
    alias_equipo_visitante AS alias_equipo,
    id_equipo_local AS id_rival,
    'VISITANTE' AS rol_equipo,
    goles_visitante AS goles_favor,
    goles_local AS goles_contra,
    goles_visitante - goles_local AS diferencia_goles,
    CASE
        WHEN marcador = 'sin informacion' THEN 'S/A'
        WHEN goles_visitante > goles_local THEN 'G'
        WHEN goles_visitante = goles_local THEN 'E'
        ELSE 'P'
    END AS resultado,
    CASE
        WHEN marcador = 'sin informacion' THEN 1
        ELSE 0
    END AS partido_sin_info,
    CASE
        WHEN marcador = 'sin informacion' THEN 'Sin jugar/Aplazado'
        ELSE 'Disputado'
    END AS estado_partido,

    posesion_visitante_pct AS posesion_pct,
    tiros_totales_visitante AS tiros_totales,
    disparos_puerta_visitante AS disparos_puerta,
    CASE
        WHEN tiros_totales_visitante > 0 THEN ROUND(disparos_puerta_visitante / tiros_totales_visitante, 4)
    END AS precision_tiro_pct,
    CASE
        WHEN tiros_totales_visitante > 0 THEN ROUND(goles_visitante / tiros_totales_visitante, 4)
    END AS conversion_gol_pct,

    pases_visitante AS pases,
    pases_precisos_visitante AS pases_precisos,
    pases_precisos_visitante_pct AS pases_precisos_pct,
    regates_realizados_visitante AS regates_realizados,
    regates_realizados_visitante_pct AS regates_realizados_pct,
    saques_esquina_visitante AS saques_esquina,
    gran_probabilidad_visitante AS gran_probabilidad,
    tiros_area_visitante AS tiros_area,
    tiros_fuera_area_visitante AS tiros_fuera_area,
    tiros_bloqueados_visitante AS tiros_bloqueados,
    duelos_ganados_visitante AS duelos_ganados,
    entradas_visitante AS entradas,
    despejes_visitante AS despejes,
    bloqueos_visitante AS bloqueos,
    interceptaciones_visitante AS intercepciones,
    tarjetas_amarillas_visitante AS tarjetas_amarillas,
    tarjetas_rojas_visitante AS tarjetas_rojas,
    fueras_juego_visitante AS fueras_juego,
    paradas_portero_visitante AS paradas_portero,
    tiros_largos_precisos_visitante AS tiros_largos_precisos,
    tiros_largos_precisos_visitante_pct AS tiros_largos_precisos_pct,
    tiros_libres_precisos_visitante AS tiros_libres_precisos,
    tiros_libres_precisos_visitante_pct AS tiros_libres_precisos_pct,
    grandes_oportunidades_perdidas_visitante AS grandes_oportunidades_perdidas,
    toques_area_rival_visitante AS toques_area_rival,
    faltas_visitante AS faltas,
    centros_precisos_visitante AS centros_precisos,
    mitad_propia_visitante AS mitad_propia,
    mitad_rival_visitante AS mitad_rival,
    lanzamientos_visitante AS lanzamientos,
    url,
    fuente_partido
FROM catalog_liga1.vw_ddv.ft_partidos_detalle_vw;



CREATE OR REPLACE VIEW catalog_liga1.vw_ddv.ft_estadisticas_jugadores_vw 
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
FROM catalog_liga1.tb_ddv.ft_estadisticas_jugadores
WHERE datos_disponibles = true;

CREATE OR REPLACE VIEW catalog_liga1.vw_ddv.dm_estilo_juego AS
SELECT 1 AS id_estilo,'Ofensivo' AS estilo
UNION ALL
SELECT 2,'Equilibrado'
UNION ALL
SELECT 3,'Defensivo';
CREATE OR REPLACE VIEW catalog_liga1.vw_ddv.dm_formacion_slots AS
 
-- 4-3-3 Ofensivo (id_formacion = 1)
SELECT 1 AS id_formacion, 'POR'  AS slot, 1  AS orden, 'POR' AS posicion_xi, 50 AS x_coord, 10 AS y_coord
UNION ALL SELECT 1, 'LI',   2, 'LI',  10, 32
UNION ALL SELECT 1, 'DFC1', 3, 'DFC', 33, 30
UNION ALL SELECT 1, 'DFC2', 4, 'DFC', 67, 30
UNION ALL SELECT 1, 'LD',   5, 'LD',  90, 32
UNION ALL SELECT 1, 'MC1',  6, 'MC',  20, 52
UNION ALL SELECT 1, 'MC2',  7, 'MC',  50, 50
UNION ALL SELECT 1, 'MC3',  8, 'MC',  80, 52
UNION ALL SELECT 1, 'EI',   9, 'EI',  15, 74
UNION ALL SELECT 1, 'DC',  10, 'DC',  50, 83
UNION ALL SELECT 1, 'ED',  11, 'ED',  85, 74
 
UNION ALL
-- 3-4-3 Ofensivo (id_formacion = 2)
-- FIXES: DFC center 24→29, MCO movido a misma fila que MC2 para evitar solapamiento
SELECT 2, 'POR',  1, 'POR', 50, 10
UNION ALL SELECT 2, 'DFC1', 2, 'DFC', 22, 31
UNION ALL SELECT 2, 'DFC2', 3, 'DFC', 50, 31
UNION ALL SELECT 2, 'DFC3', 4, 'DFC', 78, 31
UNION ALL SELECT 2, 'MC1',  5, 'MC',  12, 50
UNION ALL SELECT 2, 'MC2',  6, 'MC',  37, 52
UNION ALL SELECT 2, 'MCO',  7, 'MCO', 63, 52
UNION ALL SELECT 2, 'MC3',  8, 'MC',  88, 50
UNION ALL SELECT 2, 'EI',   9, 'EI',  15, 76
UNION ALL SELECT 2, 'DC',  10, 'DC',  50, 84
UNION ALL SELECT 2, 'ED',  11, 'ED',  85, 76
 
UNION ALL
-- 4-2-3-1 Equilibrado (id_formacion = 3)
SELECT 3, 'POR',  1, 'POR', 50, 10
UNION ALL SELECT 3, 'LI',   2, 'LI',  10, 32
UNION ALL SELECT 3, 'DFC1', 3, 'DFC', 33, 30
UNION ALL SELECT 3, 'DFC2', 4, 'DFC', 67, 30
UNION ALL SELECT 3, 'LD',   5, 'LD',  90, 32
UNION ALL SELECT 3, 'MCD1', 6, 'MCD', 33, 52
UNION ALL SELECT 3, 'MCD2', 7, 'MCD', 67, 52
UNION ALL SELECT 3, 'EI',   8, 'EI',  15, 66
UNION ALL SELECT 3, 'MCO',  9, 'MCO', 50, 66
UNION ALL SELECT 3, 'ED',  10, 'ED',  85, 66
UNION ALL SELECT 3, 'DC',  11, 'DC',  50, 87
 
UNION ALL
-- 4-4-2 Equilibrado (id_formacion = 4)
SELECT 4, 'POR',  1, 'POR', 50, 10
UNION ALL SELECT 4, 'LI',   2, 'LI',  10, 32
UNION ALL SELECT 4, 'DFC1', 3, 'DFC', 33, 30
UNION ALL SELECT 4, 'DFC2', 4, 'DFC', 67, 30
UNION ALL SELECT 4, 'LD',   5, 'LD',  90, 32
UNION ALL SELECT 4, 'EI',   6, 'EI',  12, 53
UNION ALL SELECT 4, 'MC1',  7, 'MC',  37, 52
UNION ALL SELECT 4, 'MC2',  8, 'MC',  63, 52
UNION ALL SELECT 4, 'ED',   9, 'ED',  88, 53
UNION ALL SELECT 4, 'DC1', 10, 'DC',  35, 80
UNION ALL SELECT 4, 'DC2', 11, 'DC',  65, 80
 
UNION ALL
-- 4-3-3 Equilibrado (id_formacion = 5)
SELECT 5, 'POR',  1, 'POR', 50, 10
UNION ALL SELECT 5, 'LI',   2, 'LI',  10, 32
UNION ALL SELECT 5, 'DFC1', 3, 'DFC', 33, 30
UNION ALL SELECT 5, 'DFC2', 4, 'DFC', 67, 30
UNION ALL SELECT 5, 'LD',   5, 'LD',  90, 32
UNION ALL SELECT 5, 'MC1',  6, 'MC',  22, 52
UNION ALL SELECT 5, 'MC2',  7, 'MC',  50, 50
UNION ALL SELECT 5, 'MC3',  8, 'MC',  78, 52
UNION ALL SELECT 5, 'EI',   9, 'EI',  15, 74
UNION ALL SELECT 5, 'DC',  10, 'DC',  50, 83
UNION ALL SELECT 5, 'ED',  11, 'ED',  85, 74
 
UNION ALL
-- 5-3-2 Defensivo (id_formacion = 6)
-- FIX: DFC center 24→28, LI/LD y=33
SELECT 6, 'POR',  1, 'POR', 50, 10
UNION ALL SELECT 6, 'LI',   2, 'LI',  10, 33
UNION ALL SELECT 6, 'DFC1', 3, 'DFC', 28, 30
UNION ALL SELECT 6, 'DFC2', 4, 'DFC', 50, 32
UNION ALL SELECT 6, 'DFC3', 5, 'DFC', 72, 30
UNION ALL SELECT 6, 'LD',   6, 'LD',  90, 33
UNION ALL SELECT 6, 'MC1',  7, 'MC',  22, 52
UNION ALL SELECT 6, 'MC2',  8, 'MC',  50, 54
UNION ALL SELECT 6, 'MC3',  9, 'MC',  78, 52
UNION ALL SELECT 6, 'DC1', 10, 'DC',  35, 78
UNION ALL SELECT 6, 'DC2', 11, 'DC',  65, 78
 
UNION ALL
-- 5-4-1 Defensivo (id_formacion = 7)
-- FIX: DFC center 24→28, LI/LD y=33, 4 mids en fila clara
SELECT 7, 'POR',  1, 'POR', 50, 10
UNION ALL SELECT 7, 'LI',   2, 'LI',  10, 33
UNION ALL SELECT 7, 'DFC1', 3, 'DFC', 28, 30
UNION ALL SELECT 7, 'DFC2', 4, 'DFC', 50, 32
UNION ALL SELECT 7, 'DFC3', 5, 'DFC', 72, 30
UNION ALL SELECT 7, 'LD',   6, 'LD',  90, 33
UNION ALL SELECT 7, 'EI',   7, 'EI',  12, 55
UNION ALL SELECT 7, 'MC1',  8, 'MC',  37, 57
UNION ALL SELECT 7, 'MC2',  9, 'MC',  63, 57
UNION ALL SELECT 7, 'ED',  10, 'ED',  88, 55
UNION ALL SELECT 7, 'DC',  11, 'DC',  50, 82
 
UNION ALL
-- 4-1-4-1 Defensivo (id_formacion = 8)
SELECT 8, 'POR',  1, 'POR', 50, 10
UNION ALL SELECT 8, 'LI',   2, 'LI',  10, 32
UNION ALL SELECT 8, 'DFC1', 3, 'DFC', 33, 30
UNION ALL SELECT 8, 'DFC2', 4, 'DFC', 67, 30
UNION ALL SELECT 8, 'LD',   5, 'LD',  90, 32
UNION ALL SELECT 8, 'MCD',  6, 'MCD', 50, 47
UNION ALL SELECT 8, 'EI',   7, 'EI',  12, 65
UNION ALL SELECT 8, 'MC1',  8, 'MC',  37, 66
UNION ALL SELECT 8, 'MC2',  9, 'MC',  63, 66
UNION ALL SELECT 8, 'ED',  10, 'ED',  88, 65
UNION ALL SELECT 8, 'DC',  11, 'DC',  50, 84;

CREATE OR REPLACE VIEW catalog_liga1.vw_ddv.vw_jugadores_slots AS
WITH base AS (
    SELECT 
        e.id_jugador, e.jugador, e.foto_url, e.id_equipo, e.nombre_equipo,
        e.alias_equipo, e.temporada, e.posicion_xi,
        e.goles, e.asistencias, e.minutos_jugados, e.partidos_jugados,
        e.tarjetas_amarillas, e.segunda_amarilla, e.tarjetas_rojas,
        e.titularidades, e.entrada_suplente, e.salida_suplente,
        e.min_por_gol, e.ppp,
        e.goles_en_contra, e.partidos_imbatido,
        p.altura, p.pie, p.valor_mercado, p.edad_historica, p.nacionalidad_principal,

        CASE e.posicion_xi

            -- ── DELANTERO CENTRO ──────────────────────────────────────────
            WHEN 'DC' THEN
                (e.goles*8) + (e.asistencias*3) + (e.minutos_jugados/100)
                + (e.partidos_jugados*0.5) + (e.titularidades*1.5)
                - (e.tarjetas_amarillas*1)
                - (e.segunda_amarilla*3) - (e.tarjetas_rojas*5)
                - (e.salida_suplente*0.5) - (e.entrada_suplente*0.3)
                + (e.ppp*2)
                + CASE WHEN e.min_por_gol > 0   AND e.min_por_gol <= 90  THEN 10
                       WHEN e.min_por_gol > 90  AND e.min_por_gol <= 130 THEN 6
                       WHEN e.min_por_gol > 130 AND e.min_por_gol <= 180 THEN 3
                       ELSE 0 END

            -- ── EXTREMO IZQUIERDO ─────────────────────────────────────────
            WHEN 'EI' THEN
                (e.goles*6) + (e.asistencias*5) + (e.minutos_jugados/100)
                + (e.partidos_jugados*0.5) + (e.titularidades*1.5)
                - (e.tarjetas_amarillas*1)
                - (e.segunda_amarilla*3) - (e.tarjetas_rojas*5)
                - (e.salida_suplente*0.5) - (e.entrada_suplente*0.3)
                + (e.ppp*2)
                + CASE WHEN e.min_por_gol > 0   AND e.min_por_gol <= 90  THEN 8
                       WHEN e.min_por_gol > 90  AND e.min_por_gol <= 130 THEN 5
                       WHEN e.min_por_gol > 130 AND e.min_por_gol <= 180 THEN 2
                       ELSE 0 END

            -- ── EXTREMO DERECHO ───────────────────────────────────────────
            WHEN 'ED' THEN
                (e.goles*6) + (e.asistencias*5) + (e.minutos_jugados/100)
                + (e.partidos_jugados*0.5) + (e.titularidades*1.5)
                - (e.tarjetas_amarillas*1)
                - (e.segunda_amarilla*3) - (e.tarjetas_rojas*5)
                - (e.salida_suplente*0.5) - (e.entrada_suplente*0.3)
                + (e.ppp*2)
                + CASE WHEN e.min_por_gol > 0   AND e.min_por_gol <= 90  THEN 8
                       WHEN e.min_por_gol > 90  AND e.min_por_gol <= 130 THEN 5
                       WHEN e.min_por_gol > 130 AND e.min_por_gol <= 180 THEN 2
                       ELSE 0 END

            -- ── MEDIOCAMPISTA OFENSIVO ────────────────────────────────────
            WHEN 'MCO' THEN
                (e.goles*5) + (e.asistencias*6) + (e.minutos_jugados/100)
                + (e.partidos_jugados*0.3) + (e.titularidades*1.5)
                - (e.tarjetas_amarillas*1.5)
                - (e.segunda_amarilla*3) - (e.tarjetas_rojas*5)
                - (e.salida_suplente*0.5) - (e.entrada_suplente*0.3)
                + (e.ppp*1.5)
                + CASE WHEN e.min_por_gol > 0   AND e.min_por_gol <= 90  THEN 5
                       WHEN e.min_por_gol > 90  AND e.min_por_gol <= 130 THEN 3
                       WHEN e.min_por_gol > 130 AND e.min_por_gol <= 180 THEN 1
                       ELSE 0 END

            -- ── MEDIOCAMPISTA CENTRAL ─────────────────────────────────────
            WHEN 'MC' THEN
                (e.goles*3) + (e.asistencias*4) + (e.minutos_jugados/100)
                + (e.partidos_jugados*0.3) + (e.titularidades*1.5)
                - (e.tarjetas_amarillas*1.5)
                - (e.segunda_amarilla*3) - (e.tarjetas_rojas*5)
                - (e.salida_suplente*0.5) - (e.entrada_suplente*0.3)
                + (e.ppp*1.5)
                + CASE WHEN e.min_por_gol > 0   AND e.min_por_gol <= 90  THEN 3
                       WHEN e.min_por_gol > 90  AND e.min_por_gol <= 130 THEN 2
                       WHEN e.min_por_gol > 130 AND e.min_por_gol <= 180 THEN 1
                       ELSE 0 END

            -- ── MEDIOCAMPISTA DEFENSIVO ───────────────────────────────────
            WHEN 'MCD' THEN
                (e.goles*2) + (e.asistencias*3) + (e.minutos_jugados/100)
                + (e.partidos_jugados*0.3) + (e.titularidades*1.5)
                - (e.tarjetas_amarillas*2)
                - (e.segunda_amarilla*3) - (e.tarjetas_rojas*5)
                - (e.salida_suplente*0.5) - (e.entrada_suplente*0.3)
                + (e.ppp*1.5)

            -- ── DEFENSA CENTRAL ───────────────────────────────────────────
            WHEN 'DFC' THEN
                (e.minutos_jugados/80) + (e.partidos_jugados*0.3) + (e.titularidades*1.5)
                - (e.tarjetas_amarillas*2)
                - (e.segunda_amarilla*3) - (e.tarjetas_rojas*5)
                - (e.salida_suplente*0.5) - (e.entrada_suplente*0.3)
                + (e.ppp*1)

            -- ── LATERAL DERECHO ───────────────────────────────────────────
            WHEN 'LD' THEN
                (e.minutos_jugados/90) + (e.asistencias*2) + (e.partidos_jugados*0.3) + (e.titularidades*1.5)
                - (e.tarjetas_amarillas*1.5)
                - (e.segunda_amarilla*3) - (e.tarjetas_rojas*5)
                - (e.salida_suplente*0.5) - (e.entrada_suplente*0.3)
                + (e.ppp*1)

            -- ── LATERAL IZQUIERDO ─────────────────────────────────────────
            WHEN 'LI' THEN
                (e.minutos_jugados/90) + (e.asistencias*2) + (e.partidos_jugados*0.3) + (e.titularidades*1.5)
                - (e.tarjetas_amarillas*1.5)
                - (e.segunda_amarilla*3) - (e.tarjetas_rojas*5)
                - (e.salida_suplente*0.5) - (e.entrada_suplente*0.3)
                + (e.ppp*1)

            -- ── PORTERO ───────────────────────────────────────────────────
            WHEN 'POR' THEN
                (e.minutos_jugados/70) + (e.partidos_jugados*0.3) + (e.titularidades*1.5)
                - (e.tarjetas_amarillas*2)
                - (e.segunda_amarilla*3) - (e.tarjetas_rojas*5)
                + (e.ppp*1.5)
                + (e.partidos_imbatido*2)
                - (e.goles_en_contra*0.5)

            ELSE 0
        END AS score

    FROM catalog_liga1.vw_ddv.ft_estadisticas_jugadores_vw e
    LEFT JOIN catalog_liga1.vw_ddv.ft_plantillas_historico_vw p
        ON  p.id_jugador = e.id_jugador
        AND p.temporada  = e.temporada
        AND p.id_equipo  = e.id_equipo
    WHERE e.partidos_jugados >= 5
),
player_ranks AS (
    SELECT 
        b.*,
        fp.id_formacion,
        DENSE_RANK() OVER (
            PARTITION BY b.temporada, fp.id_formacion, b.posicion_xi, b.id_equipo
            ORDER BY b.score DESC, b.id_jugador ASC
        ) AS player_rank
    FROM base b
    INNER JOIN (SELECT DISTINCT id_formacion, posicion_xi FROM catalog_liga1.vw_ddv.dm_formacion_slots) fp
        ON fp.posicion_xi = b.posicion_xi
),
slot_ranks AS (
    SELECT 
        id_formacion, posicion_xi, slot, orden, x_coord, y_coord,
        ROW_NUMBER() OVER (
            PARTITION BY id_formacion, posicion_xi
            ORDER BY orden ASC
        ) AS slot_rank
    FROM catalog_liga1.vw_ddv.dm_formacion_slots
)
SELECT 
    pr.id_jugador, pr.jugador, pr.foto_url, pr.id_equipo, pr.nombre_equipo,
    pr.alias_equipo, pr.temporada, pr.posicion_xi,
    pr.goles, pr.asistencias, pr.minutos_jugados, pr.partidos_jugados,
    pr.tarjetas_amarillas, pr.segunda_amarilla, pr.tarjetas_rojas,
    pr.titularidades, pr.entrada_suplente, pr.salida_suplente,
    pr.min_por_gol, pr.ppp,
    pr.score,
    pr.goles_en_contra, pr.partidos_imbatido,
    sr.id_formacion, sr.slot, sr.orden, sr.x_coord, sr.y_coord,
    pr.altura, pr.pie, pr.valor_mercado, pr.edad_historica, pr.nacionalidad_principal
FROM player_ranks pr
INNER JOIN slot_ranks sr
    ON  sr.id_formacion = pr.id_formacion
    AND sr.posicion_xi  = pr.posicion_xi
    AND sr.slot_rank    = pr.player_rank;