-- ==========================================================
-- DDV - DM_EQUIPOS_VW
-- Proyecto: Liga 1 Perú
-- ==========================================================

CREATE OR REPLACE VIEW ${catalog_name}.vw_ddv.dm_equipos_vw
AS
SELECT
  id_equipo,
  nombre_equipo,
  alias_equipo,
  url_equipo,
  estadio,
  capacidad,
  aforo,
  valor_total_actual,
  valor_medio_actual,
  jugadores_plantilla_actual,
  edad_promedio_actual,
  extranjeros_actual,
  temporada_ultima_valoracion
FROM ${catalog_name}.tb_ddv.dm_equipos;
