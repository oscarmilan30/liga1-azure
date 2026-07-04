-- ==========================================================
-- DDV - FT_EVOLUCION_VALORACION_VW
-- Proyecto: Liga 1 Perú
-- ==========================================================

CREATE OR REPLACE VIEW ${catalog_name}.vw_ddv.ft_evolucion_valoracion_vw
AS
SELECT
  id_valoracion_equipo,
  id_equipo,
  nombre_equipo,
  alias_equipo,
  temporada,
  jugadores_en_plantilla,
  edad_promedio,
  extranjeros,
  valor_medio,
  valor_total
FROM ${catalog_name}.tb_ddv.ft_evolucion_valoracion;
