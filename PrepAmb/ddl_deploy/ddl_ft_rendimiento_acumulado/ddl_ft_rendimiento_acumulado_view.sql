-- ==========================================================
-- DDV - FT_RENDIMIENTO_ACUMULADO_VW
-- Proyecto: Liga 1 Perú
-- ==========================================================

CREATE OR REPLACE VIEW ${catalog_name}.vw_ddv.ft_rendimiento_acumulado_vw
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
  valor_medio
FROM ${catalog_name}.tb_ddv.ft_rendimiento_acumulado;
