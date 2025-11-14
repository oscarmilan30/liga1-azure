-- ==========================================================
-- UDV - TB_MD_CATALOGO_EQUIPOS
-- Proyecto: Liga 1 Perú
-- ==========================================================

CREATE OR REPLACE TABLE ${catalog_name}.tb_udv.md_catalogo_equipos (
  id_equipo int NOT NULL COMMENT 'Identificador único del equipo',
  nombre_equipo string NOT NULL COMMENT 'Nombre oficial del equipo',
  nombre_fotmob string COMMENT 'Nombre del equipo según FotMob',
  nombre_transfermarkt string COMMENT 'Nombre del equipo según Transfermarkt',
  alias string COMMENT 'Alias o abreviatura usada en prensa',
  fecha_carga timestamp NOT NULL COMMENT 'Fecha y hora en que se cargó el registro',
  periododia int NOT NULL COMMENT 'Periodo de carga en formato YYYYMMDD'
)
USING delta
LOCATION 'abfss://${container_name}@${storage_account}.dfs.core.windows.net/udv/Proyecto/liga1/tb_udv/md_catalogo_equipos'
COMMENT 'Catálogo maestro de equipos de la Liga 1 Perú';