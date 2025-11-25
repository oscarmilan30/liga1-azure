-- ==========================================================
-- UDV - HM_TABLAS_CLASIFICACION
-- Proyecto: Liga 1 Perú
-- ==========================================================

CREATE OR REPLACE TABLE ${catalog_name}.tb_udv.hm_tablas_clasificacion (
  id_clasificacion      STRING       NOT NULL COMMENT 'Identificador único (hash: temporada + tipo_tabla + id_equipo)',
  id_equipo             INT          NOT NULL COMMENT 'Identificador homologado del equipo según catálogo FotMob',
  temporada             INT          NOT NULL COMMENT 'Temporada (año) correspondiente a la tabla de clasificación',
  tipo_tabla            STRING       NOT NULL COMMENT 'Ej: 1 - Apertura, 1 - Clausura, Grupo A, Grupo B, etc.',
  posicion              INT          COMMENT 'Posición del equipo en la tabla',
  partidos_jugados      INT          COMMENT 'Cantidad de partidos jugados',
  partidos_ganados      INT          COMMENT 'Partidos ganados',
  partidos_empatados    INT          COMMENT 'Partidos empatados',
  partidos_perdidos     INT          COMMENT 'Partidos perdidos',
  goles_favor           INT          COMMENT 'Goles a favor',
  goles_contra          INT          COMMENT 'Goles en contra',
  diferencia_goles      INT          COMMENT 'Diferencia de goles en valor absoluto',
  puntos                INT          COMMENT 'Puntaje total en la tabla',
  fecha_carga           TIMESTAMP    NOT NULL COMMENT 'Fecha y hora de carga al UDV',
  periodo               INT          NOT NULL COMMENT 'Periodo técnico YYYY (igual a temporada)'
)
USING delta
PARTITIONED BY (periodo)
LOCATION 'abfss://${container_name}@${storage_account}.dfs.core.windows.net/udv/Proyecto/liga1/tb_udv/hm_tablas_clasificacion/data'
COMMENT 'Histórico de tablas de clasificación de la Liga 1 Perú, procesado desde JSON FotMob y homologado con catálogo de equipos.';
