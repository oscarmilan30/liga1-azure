-- ==========================================================
-- UDV - VW_MD_EQUIPOS_VW
-- Proyecto: Liga 1 Perú
-- ==========================================================

CREATE OR REPLACE VIEW ${catalog_name}.vw_udv.md_equipos_vw
AS
SELECT
    id_equipo,
    nombre_equipo,
    alias_equipo,
    equipo_raw,
    url_equipo,
    fuente_equipo
FROM ${catalog_name}.tb_udv.md_equipos;