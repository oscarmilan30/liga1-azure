-- ==========================================================
-- UDV - VISTA HM_ESTADISTICAS_JUGADORES
-- Proyecto: Liga 1 Perú
-- ==========================================================

CREATE OR REPLACE VIEW ${catalog_name}.vw_udv.hm_estadisticas_jugadores_vw
AS
SELECT
  id_estadistica_jugador,
  id_jugador,
  id_tm,
  jugador,
  id_equipo,
  temporada,
  competencia,
  partidos_jugados,
  titularidades,
  ppp,
  goles,
  asistencias,
  tarjetas_amarillas,
  tarjeta_amarilla_roja,
  tarjetas_rojas,
  entrada_suplente,
  salida_suplente,
  penaltis_anotados,
  goles_en_contra,
  partidos_imbatido,
  min_por_gol,
  minutos_jugados,
  datos_disponibles,
  periodo
FROM ${catalog_name}.tb_udv.hm_estadisticas_jugadores;
