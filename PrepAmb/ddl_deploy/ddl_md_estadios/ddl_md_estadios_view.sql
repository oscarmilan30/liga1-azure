-- ==========================================================
-- UDV - VW_MD_ESTADIOS_VW
-- Proyecto: Liga 1 Perú
-- ==========================================================

CREATE OR REPLACE VIEW ${catalog_name}.vw_udv.md_estadios_vw
AS
SELECT
    id_estadio,
    id_equipo,
    estadio,
    capacidad,
    aforo,
    fuente_estadio
FROM ${catalog_name}.tb_udv.md_estadios;
