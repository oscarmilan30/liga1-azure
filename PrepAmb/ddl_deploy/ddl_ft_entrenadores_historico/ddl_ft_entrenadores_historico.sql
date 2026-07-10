-- ==========================================================
-- DDV - FT_ENTRENADORES_HISTORICO
-- Proyecto: Liga 1 Perú
-- ==========================================================

CREATE OR REPLACE TABLE ${catalog_name}.tb_ddv.ft_entrenadores_historico (
  id_entrenador_equipo    STRING        NOT NULL COMMENT 'Identificador único de la relación entrenador-equipo-período',
  id_equipo               INT           NOT NULL COMMENT 'Identificador único del equipo',
  nombre_equipo           STRING                 COMMENT 'Nombre oficial del equipo',
  alias_equipo            STRING                 COMMENT 'Alias o nombre corto del equipo',
  id_entrenador           STRING        NOT NULL COMMENT 'Identificador único del entrenador (hash determinístico)',
  entrenador              STRING                 COMMENT 'Nombre completo del entrenador',
  edad                    INT                    COMMENT 'Edad del entrenador',
  nacionalidad_principal  STRING                 COMMENT 'Nacionalidad principal del entrenador',
  segunda_nacionalidad    STRING                 COMMENT 'Segunda nacionalidad del entrenador',
  temporada               INT           NOT NULL COMMENT 'Temporada (año) del período en el cargo',
  fecha_inicio            DATE                   COMMENT 'Fecha de inicio en el cargo',
  fecha_termino           DATE                   COMMENT 'Fecha de término en el cargo',
  tiempo_en_cargo         STRING                 COMMENT 'Tiempo total en el cargo en días (sin sufijo)',
  partidos                INT                    COMMENT 'Total de partidos dirigidos en el período',
  ppp                     DECIMAL(10,4)          COMMENT 'Puntos por partido (promedio)',
  entrenador_activo       INT                    COMMENT 'Indicador de si el entrenador está activo (1) o no (0)',
  fecha_carga             TIMESTAMP     NOT NULL COMMENT 'Fecha y hora de la carga en DDV',
  periodo                 INT           NOT NULL COMMENT 'Periodo técnico de carga en formato YYYYMM'
)
USING delta
PARTITIONED BY (periodo)
LOCATION 'abfss://${container_name}@${storage_account}.dfs.core.windows.net/${ruta_base}/ddv/ft_entrenadores_historico/data'
COMMENT 'Fact histórico de entrenadores por equipo y período de la Liga 1 Perú, con perfil completo del técnico y datos del equipo.';
