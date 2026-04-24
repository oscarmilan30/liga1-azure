-- ==========================================================
-- UDV - HM_CAMPEONES_VW
-- Proyecto: Liga 1 Perú
-- ==========================================================

CREATE OR REPLACE VIEW ${catalog_name}.vw_udv.hm_campeones_vw AS
SELECT
  id_campeonato,
  id_equipo_campeon,
  id_equipo_subcampeon,
  temporada,
  fuente,
  fecha_carga,
  periodo
FROM ${catalog_name}.tb_udv.hm_campeones;