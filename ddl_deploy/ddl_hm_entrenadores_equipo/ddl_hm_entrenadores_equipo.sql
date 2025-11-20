-- ==========================================================
-- UDV - MD_ENTRENADORES
--     - HM_ENTRENADORES_EQUIPO
-- Proyecto: Liga 1 Perú
-- ==========================================================

CREATE OR REPLACE TABLE ${catalog_name}.tb_udv.md_entrenadores (
  id_entrenador      STRING   NOT NULL COMMENT 'Identificador único del entrenador (hash determinístico)',
  entrenador         STRING   NOT NULL COMMENT 'Nombre completo del entrenador',
  edad               INT               COMMENT 'Edad actual del entrenador',
  nacionalidad       STRING            COMMENT 'Nacionalidad del entrenador',
  fuente_entrenador  STRING   NOT NULL COMMENT 'Fuente de la información del entrenador',
  fecha_carga        TIMESTAMP NOT NULL COMMENT 'Fecha y hora de la carga efectiva en UDV',
  periododia         INT      NOT NULL COMMENT 'Periodo técnico de carga en formato YYYYMMDD'
)
USING delta
LOCATION 'abfss://${container_name}@${storage_account}.dfs.core.windows.net/udv/Proyecto/liga1/tb_udv/md_entrenadores/data'
COMMENT 'Maestro de entrenadores de la Liga 1 Perú, independiente del club';

CREATE OR REPLACE TABLE ${catalog_name}.tb_udv.hm_entrenadores_equipo (
  id_entrenador_equipo  STRING   NOT NULL COMMENT 'Identificador único de relación entrenador-equipo-temporada',
  id_equipo             INT      NOT NULL COMMENT 'Identificador único del equipo',
  id_entrenador         STRING   NOT NULL COMMENT 'Identificador único del entrenador',
  temporada             INT      NOT NULL COMMENT 'Temporada de la relación entrenador-equipo',
  fecha_inicio          DATE              COMMENT 'Fecha de inicio del periodo como entrenador',
  fecha_termino         DATE              COMMENT 'Fecha de término del periodo como entrenador',
  tiempo_en_cargo       STRING            COMMENT 'Tiempo en el cargo (texto tal como viene de la fuente)',
  partidos              INT               COMMENT 'Cantidad de partidos dirigidos',
  ppp                   DECIMAL(10,4)     COMMENT 'Puntos por partido',
  entrenador_activo     INT               COMMENT 'Indicador de si es el entrenador actual (1 = activo, 0 = no activo)',
  fuente_entrenador     STRING   NOT NULL COMMENT 'Fuente de la información',
  fecha_carga           TIMESTAMP NOT NULL COMMENT 'Fecha y hora de la carga efectiva en UDV',
  periodo               INT      NOT NULL COMMENT 'Periodo técnico de carga en formato YYYYMM'
)
USING delta
PARTITIONED BY (periodo)
LOCATION 'abfss://${container_name}@${storage_account}.dfs.core.windows.net/udv/Proyecto/liga1/tb_udv/hm_entrenadores_equipo/data'
COMMENT 'Histórico de relación entrenador-equipo-temporada en la Liga 1 Perú';

