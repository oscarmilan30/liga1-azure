-- ==========================================================
-- UDV - TB_MD_EQUIPOS
-- Proyecto: Liga 1 Perú
-- ==========================================================

CREATE OR REPLACE TABLE ${catalog_name}.tb_udv.md_equipos (
  id_equipo           INT NOT NULL COMMENT 'Identificador único del equipo (llave maestra del modelo)',
  nombre_equipo       STRING NOT NULL COMMENT 'Nombre oficial del equipo según el catálogo maestro',
  alias               STRING COMMENT 'Alias o nombre corto usado en el catálogo',
  equipo_raw          STRING NOT NULL COMMENT 'Nombre del equipo tal como viene de la fuente (JSON equipos)',
  url                 STRING COMMENT 'URL del equipo en la fuente (por ejemplo Transfermarkt)',
  fuente_ultima       STRING NOT NULL COMMENT 'Fuente de la última extracción utilizada para este equipo',
  fecha_carga_ultima  TIMESTAMP NOT NULL COMMENT 'Fecha y hora de la última carga efectiva en UDV para este equipo',
  periododia          INT NOT NULL COMMENT 'Periodo técnico de carga en formato YYYYMMDD, derivado de fecha_carga_ultima'
)
USING delta
LOCATION 'abfss://${container_name}@${storage_account}.dfs.core.windows.net/udv/Proyecto/liga1/tb_udv/md_equipos/data'
COMMENT 'Maestro de equipos homologado contra md_catalogo_equipos con el último registro disponible por equipo';