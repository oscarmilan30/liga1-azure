-- ==========================================================
-- UDV - VW_MD_EQUIPOS_VW
-- Proyecto: Liga 1 Perú
-- ==========================================================

CREATE OR REPLACE VIEW ${catalog_name}.vw_udv.md_equipos_vw
AS
SELECT
    id_equipo,
    nombre_equipo,
    alias,
    equipo_raw,
    url,
    fuente_ultima,
    fecha_carga_ultima,
    periododia
FROM ${catalog_name}.tb_udv.md_equipos;