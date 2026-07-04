-- ==========================================================
-- UDV - HM_TABLAS_CLASIFICACION_VW
-- Proyecto: Liga 1 Perú
-- ==========================================================

CREATE OR REPLACE VIEW ${catalog_name}.vw_udv.hm_tablas_clasificacion_vw
AS
SELECT
  id_clasificacion,
  id_equipo,
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
  puntos
FROM ${catalog_name}.tb_udv.hm_tablas_clasificacion;
