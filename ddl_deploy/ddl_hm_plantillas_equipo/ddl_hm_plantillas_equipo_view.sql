-- ==========================================================
-- UDV - MD_PLANTILLAS_VW
-- UDV - HM_PLANTILLAS_EQUIPO_VW
-- Proyecto: Liga 1 Perú
-- ==========================================================

CREATE OR REPLACE VIEW ${catalog_name}.vw_udv.md_plantillas_vw
AS
SELECT
    id_jugador,
    jugador,
    edad,
    nacionalidad,
    altura,
    pie,
    valor_mercado,
    fuente_plantilla,
    fecha_carga,
    periododia
FROM ${catalog_name}.tb_udv.md_plantillas;

CREATE OR REPLACE VIEW ${catalog_name}.vw_udv.hm_plantillas_equipo_vw
AS
SELECT
    id_equipo,
    id_jugador,
    temporada,
    numero_camiseta,
    posicion,
    fecha_fichaje,
    fuente_plantilla,
    fecha_carga,
    periodo
FROM ${catalog_name}.tb_udv.hm_plantillas_equipo;