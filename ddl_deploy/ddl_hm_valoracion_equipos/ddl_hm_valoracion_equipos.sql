-- ==========================================================
-- UDV - HM_VALORACION_EQUIPOS
-- Proyecto: Liga 1 Perú
-- ==========================================================

CREATE OR REPLACE TABLE ${catalog_name}.tb_udv.hm_valoracion_equipos (
  id_valoracion_equipo      STRING NOT NULL COMMENT 'Identificador único de la valoración anual del equipo',
  id_equipo               INT NOT NULL COMMENT 'Identificador único del equipo',
  temporada               INT NOT NULL COMMENT 'Temporada (año) de la valoración',
  jugadores_en_plantilla  INT COMMENT 'Número de jugadores en la plantilla',
  edad_promedio           DOUBLE COMMENT 'Edad promedio de la plantilla',
  extranjeros             INT COMMENT 'Cantidad de jugadores extranjeros',
  valor_medio             DECIMAL(20,4) COMMENT 'Valor medio de mercado por jugador',
  valor_total             DECIMAL(20,4) COMMENT 'Valor total de mercado de la plantilla',
  fuente_valoracion       STRING NOT NULL COMMENT 'Fuente de la información de valoración',
  fecha_carga             TIMESTAMP NOT NULL COMMENT 'Fecha y hora de la carga efectiva en UDV',
  periodo                 INT NOT NULL COMMENT 'Periodo técnico de carga en formato YYYY'
)
USING delta
PARTITIONED BY (periodo)
LOCATION 'abfss://${container_name}@${storage_account}.dfs.core.windows.net/udv/Proyecto/liga1/tb_udv/hm_valoracion_equipos/data'
COMMENT 'Histórico de valoración anual de equipos de la Liga 1 Perú (número de jugadores, edad promedio, extranjeros, valor de mercado, etc.).';
