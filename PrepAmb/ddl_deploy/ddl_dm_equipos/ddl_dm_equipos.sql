-- ==========================================================
-- DDV - DM_EQUIPOS
-- Proyecto: Liga 1 Perú
-- ==========================================================

CREATE OR REPLACE TABLE ${catalog_name}.tb_ddv.dm_equipos (
  id_equipo                    INT           NOT NULL COMMENT 'Identificador único del equipo',
  nombre_equipo                STRING        NOT NULL COMMENT 'Nombre oficial del equipo',
  alias_equipo                 STRING                 COMMENT 'Alias o nombre corto del equipo',
  url_equipo                   STRING                 COMMENT 'URL del equipo en la fuente',
  estadio                      STRING                 COMMENT 'Nombre del estadio del equipo',
  capacidad                    STRING                 COMMENT 'Capacidad total del estadio',
  aforo                        STRING                 COMMENT 'Aforo permitido del estadio',
  valor_total_actual           DECIMAL(20,4)          COMMENT 'Valor total de mercado de la plantilla en la última temporada',
  valor_medio_actual           DECIMAL(20,4)          COMMENT 'Valor medio por jugador en la última temporada',
  jugadores_plantilla_actual   INT                    COMMENT 'Número de jugadores en plantilla en la última temporada',
  edad_promedio_actual         DOUBLE                 COMMENT 'Edad promedio de la plantilla en la última temporada',
  extranjeros_actual           INT                    COMMENT 'Cantidad de jugadores extranjeros en la última temporada',
  temporada_ultima_valoracion  INT                    COMMENT 'Temporada de la valoración más reciente registrada',
  fecha_carga                  TIMESTAMP     NOT NULL COMMENT 'Fecha y hora de la última carga en DDV'
)
USING delta
LOCATION 'abfss://${container_name}@${storage_account}.dfs.core.windows.net/${ruta_base}/ddv/dm_equipos/data'
COMMENT 'Dimensión de equipos de la Liga 1 Perú con información consolidada de catálogo, estadio y valoración de mercado más reciente.';
