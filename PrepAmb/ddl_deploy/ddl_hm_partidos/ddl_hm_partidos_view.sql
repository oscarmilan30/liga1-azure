-- ==========================================================
-- UDV - HM_PARTIDOS_VW
-- Proyecto: Liga 1 Perú
-- ==========================================================

CREATE OR REPLACE VIEW ${catalog_name}.vw_udv.hm_partidos_vw
AS
SELECT
  id_partido,
  id_equipo_local,
  id_equipo_visitante,
  temporada,
  fecha_partido,
  marcador,
  goles_local,
  goles_visitante,
  url,
  fuente_partido
FROM ${catalog_name}.tb_udv.hm_partidos;