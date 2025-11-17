-- ==========================================================
-- UDV - TB_MD_ESTADIOS
-- Proyecto: Liga 1 Perú
-- ==========================================================

CREATE OR REPLACE TABLE ${catalog_name}.tb_udv.md_estadios (
  id_equipo      INT NOT NULL COMMENT 'Identificador único del equipo asociado al estadio',
  nombre_equipo  STRING NOT NULL COMMENT 'Nombre oficial del equipo según el catálogo maestro',
  equipo_raw     STRING NOT NULL COMMENT 'Nombre del club tal como viene de la fuente de estadios',
  estadio        STRING NOT NULL COMMENT 'Nombre del estadio principal del club según la fuente',
  capacidad      STRING COMMENT 'Capacidad del estadio según la fuente (texto original)',
  aforo          STRING COMMENT 'Aforo oficial del estadio según la fuente (texto original)',
  fuente_estadio STRING NOT NULL COMMENT 'Fuente de la información del estadio (por ejemplo Transfermarkt)',
  fecha_carga    TIMESTAMP NOT NULL COMMENT 'Fecha y hora de la carga efectiva en UDV para este registro',
  periododia     INT NOT NULL COMMENT 'Periodo técnico de carga en formato YYYYMMDD'
)
USING delta
LOCATION 'abfss://${container_name}@${storage_account}.dfs.core.windows.net/udv/Proyecto/liga1/tb_udv/md_estadios/data'
COMMENT 'Maestro de estadios por equipo, homologado a partir de la información de estadios de la Liga 1 Perú';