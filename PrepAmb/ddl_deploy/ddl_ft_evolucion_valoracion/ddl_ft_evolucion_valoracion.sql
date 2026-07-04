-- ==========================================================
-- DDV - FT_EVOLUCION_VALORACION
-- Proyecto: Liga 1 Perú
-- ==========================================================

CREATE OR REPLACE TABLE ${catalog_name}.tb_ddv.ft_evolucion_valoracion (
  id_valoracion_equipo     STRING        NOT NULL COMMENT 'Identificador único de la valoración anual del equipo',
  id_equipo                INT           NOT NULL COMMENT 'Identificador único del equipo',
  nombre_equipo            STRING                 COMMENT 'Nombre oficial del equipo',
  alias_equipo             STRING                 COMMENT 'Alias o nombre corto del equipo',
  temporada                INT           NOT NULL COMMENT 'Temporada (año) de la valoración',
  jugadores_en_plantilla   INT                    COMMENT 'Número de jugadores en la plantilla',
  edad_promedio            DOUBLE                 COMMENT 'Edad promedio de la plantilla',
  extranjeros              INT                    COMMENT 'Cantidad de jugadores extranjeros',
  valor_medio              DECIMAL(20,4)          COMMENT 'Valor medio de mercado por jugador',
  valor_total              DECIMAL(20,4)          COMMENT 'Valor total de mercado de la plantilla',
  fecha_carga              TIMESTAMP     NOT NULL COMMENT 'Fecha y hora de la carga en DDV',
  periodo                  INT           NOT NULL COMMENT 'Periodo técnico de carga en formato YYYY'
)
USING delta
PARTITIONED BY (periodo)
LOCATION 'abfss://${container_name}@${storage_account}.dfs.core.windows.net/${ruta_base}/ddv/ft_evolucion_valoracion/data'
COMMENT 'Fact de evolución de la valoración de mercado por equipo y temporada de la Liga 1 Perú.';
