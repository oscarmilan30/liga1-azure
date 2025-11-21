-- ==========================================================
-- UDV - HM_VALORACION_EQUIPOS_VW
-- Proyecto: Liga 1 Perú
-- ==========================================================

CREATE OR REPLACE VIEW ${catalog_name}.vw_udv.hm_valoracion_equipos_vw
AS
SELECT
  id_valoracion_equipo,
  id_equipo,
  temporada,
  jugadores_en_plantilla,
  edad_promedio,
  extranjeros,
  valor_medio,
  valor_total,
  fuente_valoracion,
  fecha_carga,
  periodo
FROM ${catalog_name}.tb_udv.hm_valoracion_equipos;
