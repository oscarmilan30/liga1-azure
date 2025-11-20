-- ==========================================================
-- UDV - MD_ENTRENADORES_VW
-- UDV - HM_ENTRENADORES_EQUIPO_VW
-- Proyecto: Liga 1 Perú
-- ==========================================================

CREATE OR REPLACE VIEW ${catalog_name}.vw_udv.md_entrenadores_vw
AS
SELECT
    id_entrenador,
    entrenador,
    edad,
    nacionalidad,
    fuente_entrenador,
    fecha_carga,
    periododia
FROM ${catalog_name}.tb_udv.md_entrenadores;


CREATE OR REPLACE VIEW ${catalog_name}.vw_udv.hm_entrenadores_equipo_vw
AS
SELECT
    id_entrenador_equipo,
    id_equipo,
    id_entrenador,
    temporada,
    fecha_inicio,
    fecha_termino,
    tiempo_en_cargo,
    partidos,
    ppp,
    entrenador_activo,
    fuente_entrenador,
    fecha_carga,
    periodo
FROM ${catalog_name}.tb_udv.hm_entrenadores_equipo;