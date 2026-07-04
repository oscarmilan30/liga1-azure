-- ==========================================================
-- ML - VISTA FT_SCORE_ML
-- Proyecto: Liga 1 Peru
-- Patron: igual que vw_jugadores_slots — player_rank por equipo
--         permite filtro dinamico de equipos en Power BI (XI Ideal)
--
-- NOTA DEPLOY: si la vista ya esta en un Delta Share (liga1_share)
-- usar ALTER VIEW en lugar de CREATE OR REPLACE VIEW para no
-- romper el share. Sintaxis: ALTER VIEW <name> AS <query>
-- ==========================================================

CREATE OR REPLACE VIEW ${catalog_name}.vw_ddv.ft_score_ml_vw
AS
WITH player_ranks AS (
    -- Un registro por jugador x temporada x formacion.
    -- DENSE_RANK por (temporada, formacion, posicion, equipo) → cada equipo
    -- tiene su propio ranking interno. El DT de Power BI (DAX) luego elige
    -- el mejor entre los equipos visibles segun el filtro del usuario.
    SELECT
        ml.id_score_ml,
        ml.id_jugador,
        ml.id_tm,
        ml.jugador,
        ml.foto_url,
        ml.id_equipo,
        ml.nombre_equipo,
        ml.alias_equipo,
        ml.temporada,
        ml.posicion,
        CASE
            WHEN ml.posicion_xi = 'POR' THEN 'Portero'
            WHEN ml.posicion_xi = 'DFC' THEN 'Defensa Central'
            WHEN ml.posicion_xi = 'LD'  THEN 'Lateral Derecho'
            WHEN ml.posicion_xi = 'LI'  THEN 'Lateral Izquierdo'
            WHEN ml.posicion_xi = 'MCD' THEN 'Mediocampista Defensivo'
            WHEN ml.posicion_xi = 'MC'  THEN 'Mediocampista Central'
            WHEN ml.posicion_xi = 'MCO' THEN 'Mediocampista Ofensivo'
            WHEN ml.posicion_xi = 'EI'  THEN 'Extremo Izquierdo'
            WHEN ml.posicion_xi = 'ED'  THEN 'Extremo Derecho'
            WHEN ml.posicion_xi = 'DC'  THEN 'Delantero Centro'
            ELSE ml.posicion_xi
        END                                               AS posicion_xi_nombre,
        ml.posicion_xi,
        ml.partidos_jugados,
        ml.titularidades,
        ml.minutos_jugados,
        ml.goles,
        ml.asistencias,
        ml.goles + ml.asistencias                        AS participaciones_gol,
        ml.ppp,
        ml.tarjetas_amarillas,
        ml.tarjeta_amarilla_roja                         AS segunda_amarilla,
        ml.tarjetas_rojas,
        ml.entrada_suplente,
        ml.salida_suplente,
        ml.min_por_gol,
        ml.goles_en_contra,
        ml.partidos_imbatido,
        ROUND(ml.goles_p90, 3)                           AS goles_p90,
        ROUND(ml.asistencias_p90, 3)                     AS asistencias_p90,
        ROUND(ml.participacion_p90, 3)                   AS participacion_p90,
        ROUND(ml.gc_p90, 3)                              AS gc_p90,
        ROUND(ml.pi_p90, 3)                              AS pi_p90,
        ROUND(ml.score_ml, 4)                            AS score_ml,
        ROUND(ml.score_100, 2)                           AS score_100,
        ROUND(ml.var_explicada * 100, 1)                 AS varianza_explicada_pct,
        ml.nivel_num,
        ml.nivel,
        fp.id_formacion,
        DENSE_RANK() OVER (
            PARTITION BY ml.temporada, fp.id_formacion, ml.posicion_xi, ml.id_equipo
            ORDER BY ROUND(ml.score_100, 2) DESC, ml.id_jugador ASC
        )                                                AS player_rank
    FROM ${catalog_name}.tb_ddv.ft_score_ml ml
    INNER JOIN (
        SELECT DISTINCT id_formacion, posicion_xi
        FROM ${catalog_name}.vw_ddv.dm_formacion_slots
    ) fp ON fp.posicion_xi = ml.posicion_xi
),
slot_ranks AS (
    -- Numera los slots dentro de cada (formacion, posicion) por orden ASC.
    -- slot_rank=1 → DFC1 (mejor jugador), slot_rank=2 → DFC2 (segundo mejor), etc.
    SELECT
        id_formacion,
        posicion_xi,
        slot,
        orden,
        x_coord,
        y_coord,
        ROW_NUMBER() OVER (
            PARTITION BY id_formacion, posicion_xi
            ORDER BY orden ASC
        )                                                AS slot_rank
    FROM ${catalog_name}.vw_ddv.dm_formacion_slots
)
SELECT
    pr.id_score_ml,
    pr.id_jugador,
    pr.id_tm,
    pr.jugador,
    pr.foto_url,
    pr.id_equipo,
    pr.nombre_equipo,
    pr.alias_equipo,
    pr.temporada,
    pr.posicion,
    pr.posicion_xi_nombre,
    pr.posicion_xi,
    pr.partidos_jugados,
    pr.titularidades,
    pr.minutos_jugados,
    pr.goles,
    pr.asistencias,
    pr.participaciones_gol,
    pr.ppp,
    pr.tarjetas_amarillas,
    pr.segunda_amarilla,
    pr.tarjetas_rojas,
    pr.entrada_suplente,
    pr.salida_suplente,
    pr.min_por_gol,
    pr.goles_en_contra,
    pr.partidos_imbatido,
    pr.goles_p90,
    pr.asistencias_p90,
    pr.participacion_p90,
    pr.gc_p90,
    pr.pi_p90,
    pr.score_ml,
    pr.score_100,
    pr.varianza_explicada_pct,
    pr.nivel_num,
    pr.nivel,
    pr.id_formacion,
    sr.slot,
    sr.orden,
    sr.x_coord,
    sr.y_coord
FROM player_ranks pr
INNER JOIN slot_ranks sr
    ON  sr.id_formacion = pr.id_formacion
    AND sr.posicion_xi  = pr.posicion_xi
    AND sr.slot_rank    = pr.player_rank;
