-- ==========================================================
-- UDV - HM_PARTIDOS
-- Proyecto: Liga 1 Perú
-- ==========================================================

CREATE OR REPLACE TABLE ${catalog_name}.tb_udv.hm_partidos (
  id_partido           STRING NOT NULL COMMENT 'Identificador único del partido (hash determinístico temporada + url)',
  id_equipo_local      INT NOT NULL COMMENT 'Identificador del equipo local',
  id_equipo_visitante  INT NOT NULL COMMENT 'Identificador del equipo visitante',
  temporada            INT NOT NULL COMMENT 'Temporada (año) del partido',
  fecha_partido        DATE COMMENT 'Fecha del partido (formato original del JSON)',
  marcador             STRING COMMENT 'Marcador en formato texto (ej. 2 - 1)',
  goles_local          INT COMMENT 'Goles anotados por el equipo local',
  goles_visitante      INT COMMENT 'Goles anotados por el equipo visitante',
  url                  STRING COMMENT 'URL de detalle del partido en la fuente',
  fuente_partido       STRING NOT NULL COMMENT 'Fuente de la información del partido',
  fecha_carga          TIMESTAMP NOT NULL COMMENT 'Fecha y hora de la carga efectiva en UDV',
  periodo              INT NOT NULL COMMENT 'Periodo técnico en formato YYYY (igual a temporada)'
)
USING delta
PARTITIONED BY (periodo)
LOCATION 'abfss://${container_name}@${storage_account}.dfs.core.windows.net/udv/Proyecto/liga1/tb_udv/hm_partidos/data'
COMMENT 'Histórico de partidos de la Liga 1 Perú, con equipos local/visitante, marcador y goles.';