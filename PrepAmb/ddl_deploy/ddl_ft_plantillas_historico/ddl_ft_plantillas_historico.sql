-- ==========================================================
-- DDV - FT_PLANTILLAS_HISTORICO
-- Proyecto: Liga 1 Perú
-- ==========================================================

CREATE OR REPLACE TABLE ${catalog_name}.tb_ddv.ft_plantillas_historico (
  id_plantilla_equipo      STRING        NOT NULL COMMENT 'Identificador único de la relación jugador-equipo-temporada',
  id_equipo                INT           NOT NULL COMMENT 'Identificador único del equipo',
  nombre_equipo            STRING                 COMMENT 'Nombre oficial del equipo',
  alias_equipo             STRING                 COMMENT 'Alias o nombre corto del equipo',
  id_jugador               STRING        NOT NULL COMMENT 'Identificador único del jugador (hash determinístico)',
  id_tm                    STRING                 COMMENT 'ID del jugador en Transfermarkt',
  jugador                  STRING                 COMMENT 'Nombre completo del jugador',
  temporada                INT           NOT NULL COMMENT 'Temporada (año) de la plantilla',
  numero_camiseta          STRING                 COMMENT 'Número de camiseta del jugador',
  posicion                 STRING                 COMMENT 'Posición principal del jugador',
  fecha_fichaje            DATE                   COMMENT 'Fecha de fichaje con el equipo',
  edad                     INT                    COMMENT 'Edad del jugador',
  nacionalidad_principal   STRING                 COMMENT 'Nacionalidad principal del jugador',
  segunda_nacionalidad     STRING                 COMMENT 'Segunda nacionalidad del jugador',
  altura                   DOUBLE                 COMMENT 'Altura del jugador en metros',
  pie                      STRING                 COMMENT 'Pierna hábil del jugador',
  valor_mercado            DECIMAL(20,4)          COMMENT 'Valor de mercado del jugador según fuente',
  foto_url                 STRING                 COMMENT 'URL de la foto del jugador en Transfermarkt',
  fecha_carga              TIMESTAMP     NOT NULL COMMENT 'Fecha y hora de la carga en DDV',
  periodo                  INT           NOT NULL COMMENT 'Periodo técnico de carga en formato YYYYMM'
)
USING delta
PARTITIONED BY (periodo)
LOCATION 'abfss://${container_name}@${storage_account}.dfs.core.windows.net/${ruta_base}/ddv/ft_plantillas_historico/data'
COMMENT 'Fact histórico de plantillas por equipo y temporada de la Liga 1 Perú, con perfil completo del jugador y datos del equipo.';
