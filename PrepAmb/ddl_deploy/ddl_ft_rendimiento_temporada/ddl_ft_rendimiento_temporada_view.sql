-- ==========================================================
-- DDV - FT_RENDIMIENTO_TEMPORADA_VW
-- Proyecto: Liga 1 Perú
-- ==========================================================

CREATE OR REPLACE VIEW ${catalog_name}.vw_ddv.ft_rendimiento_temporada_vw
AS
SELECT
  id_clasificacion,
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
  valor_medio
FROM ${catalog_name}.tb_ddv.ft_rendimiento_temporada;
