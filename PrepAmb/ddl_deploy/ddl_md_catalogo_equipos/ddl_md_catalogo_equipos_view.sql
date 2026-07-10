-- ==========================================================
-- UDV - VW_MD_CATALOGO_EQUIPOS  
-- Proyecto: Liga 1 Perú
-- ==========================================================

CREATE OR REPLACE VIEW ${catalog_name}.vw_udv.md_catalogo_equipos_vw
AS
SELECT
    id_equipo,
    nombre_equipo,
    nombre_fotmob,
    nombre_transfermarkt,
    alias,
    nombre_fotmob_clasificacion,
    nombre_fotmob_clas_alt
FROM ${catalog_name}.tb_udv.md_catalogo_equipos;