-- ==========================================================
-- UDV - MD_PLANTILLAS
-- UDV - HM_PLANTILLAS_EQUIPO
-- Proyecto: Liga 1 Perú
-- ==========================================================

CREATE OR REPLACE TABLE ${catalog_name}.tb_udv.md_plantillas (
  id_jugador                   STRING NOT NULL COMMENT 'Identificador único del jugador (hash determinístico)',
  jugador                      STRING NOT NULL COMMENT 'Nombre completo del jugador',
  edad                         INT COMMENT 'Edad actual de jugador',
  nacionalidad_principal       STRING COMMENT 'Nacionalidad del jugador',
  segunda_nacionalidad         STRING COMMENT 'Segunda Nacionalidad del jugador',
  altura                       DOUBLE COMMENT 'Altura del jugador',
  pie                          STRING COMMENT 'Pierna hábil del jugador',
  valor_mercado                DECIMAL(20,4) COMMENT 'Valor de mercado según fuente',
  fuente_plantilla             STRING NOT NULL COMMENT 'Fuente de la información del jugador',
  fecha_carga                  TIMESTAMP NOT NULL COMMENT 'Fecha y hora de la carga efectiva en UDV',
  periododia                   INT NOT NULL COMMENT 'Periodo técnico de carga en formato YYYYMMDD'
)
USING delta
LOCATION 'abfss://${container_name}@${storage_account}.dfs.core.windows.net/udv/Proyecto/liga1/tb_udv/md_plantillas/data'
COMMENT 'Maestro de jugadores de la Liga 1 Perú, independiente del club';

CREATE OR REPLACE TABLE ${catalog_name}.tb_udv.hm_plantillas_equipo (
  id_plantilla_equipo         STRING NOT NULL COMMENT 'Identificador único de plantilla de equipo',
  id_equipo                   INT NOT NULL COMMENT 'Identificador único del equipo',
  id_jugador                  STRING NOT NULL COMMENT 'Identificador único del jugador',
  temporada                   INT NOT NULL COMMENT 'Temporada de la plantilla',
  numero_camiseta             STRING COMMENT 'Número de camiseta del jugador',
  posicion                    STRING COMMENT 'Posición principal del jugador',
  fecha_fichaje               DATE COMMENT 'Fecha de fichaje con el equipo',
  fuente_plantilla            STRING NOT NULL COMMENT 'Fuente de la información',
  fecha_carga                 TIMESTAMP NOT NULL COMMENT 'Fecha y hora de la carga efectiva en UDV',
  periodo                     INT NOT NULL COMMENT 'Periodo técnico de carga en formato YYYYMM'
)
USING delta
PARTITIONED BY (periodo)
LOCATION 'abfss://${container_name}@${storage_account}.dfs.core.windows.net/udv/Proyecto/liga1/tb_udv/hm_plantillas_equipo/data'
COMMENT 'Histórico de relación jugador-equipo-temporada en la Liga 1 Perú';