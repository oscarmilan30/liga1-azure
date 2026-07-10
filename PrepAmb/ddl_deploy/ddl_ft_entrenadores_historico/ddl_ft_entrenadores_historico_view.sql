-- ==========================================================
-- DDV - FT_ENTRENADORES_HISTORICO_VW
-- Proyecto: Liga 1 Perú
-- ==========================================================

CREATE OR REPLACE VIEW ${catalog_name}.vw_ddv.ft_entrenadores_historico_vw
AS
SELECT
  id_entrenador_equipo,
  id_equipo,
  nombre_equipo,
  alias_equipo,
  id_entrenador,
  entrenador,
  edad,
  nacionalidad_principal,
  segunda_nacionalidad,
  temporada,
  fecha_inicio,
  fecha_termino,
  tiempo_en_cargo,
  partidos,
  ppp,
  entrenador_activo
FROM ${catalog_name}.tb_ddv.ft_entrenadores_historico;
