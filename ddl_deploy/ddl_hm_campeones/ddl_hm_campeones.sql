-- ==========================================================
-- UDV - HM_CAMPEONES
-- Proyecto: Liga 1 Perú
-- ==========================================================

CREATE OR REPLACE TABLE ${catalog_name}.tb_udv.hm_campeones (
  id_campeonato        STRING    COMMENT 'Identificador técnico del campeonato (hash de temporada + campeón + subcampeón).',
  id_equipo_campeon    INT       COMMENT 'ID técnico del equipo campeón según catálogo de equipos.',
  id_equipo_subcampeon INT       COMMENT 'ID técnico del equipo subcampeón según catálogo de equipos.',
  temporada            INT       COMMENT 'Temporada (año) del torneo de Liga 1.',
  fuente               STRING    COMMENT 'Fuente de los datos de campeones, por ejemplo FotMob.',
  fecha_carga          TIMESTAMP COMMENT 'Fecha y hora de la carga en UDV para el registro de campeones.',
  periodo              INT       COMMENT 'Periodo técnico de partición (YYYY), normalmente igual a temporada.'
)
USING delta
PARTITIONED BY (periodo)
LOCATION 'abfss://${container_name}@${storage_account}.dfs.core.windows.net/udv/Proyecto/liga1/tb_udv/hm_campeones/data'
COMMENT 'Histórico de campeones y subcampeones por temporada de la Liga 1 Perú, con mapeo a IDs técnicos de equipos.';