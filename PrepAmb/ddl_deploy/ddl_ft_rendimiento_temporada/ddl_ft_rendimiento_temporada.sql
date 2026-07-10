-- ==========================================================
-- DDV - FT_RENDIMIENTO_TEMPORADA
-- Proyecto: Liga 1 Perú
-- ==========================================================

CREATE OR REPLACE TABLE ${catalog_name}.tb_ddv.ft_rendimiento_temporada (
  id_clasificacion    STRING        NOT NULL COMMENT 'Identificador único del registro de clasificación',
  id_equipo           INT           NOT NULL COMMENT 'Identificador único del equipo',
  nombre_equipo       STRING                 COMMENT 'Nombre oficial del equipo',
  alias_equipo        STRING                 COMMENT 'Alias o nombre corto del equipo',
  temporada           INT           NOT NULL COMMENT 'Temporada (año) del registro',
  tipo_tabla          STRING                 COMMENT 'Tipo de tabla: Apertura, Clausura o General',
  posicion            INT                    COMMENT 'Posición en la tabla al cierre del período',
  partidos_jugados    INT                    COMMENT 'Total de partidos jugados',
  partidos_ganados    INT                    COMMENT 'Total de partidos ganados',
  partidos_empatados  INT                    COMMENT 'Total de partidos empatados',
  partidos_perdidos   INT                    COMMENT 'Total de partidos perdidos',
  goles_favor         INT                    COMMENT 'Goles a favor acumulados',
  goles_contra        INT                    COMMENT 'Goles en contra acumulados',
  diferencia_goles    INT                    COMMENT 'Diferencia de goles (favor - contra)',
  puntos              INT                    COMMENT 'Puntos acumulados en la tabla',
  valor_total         DECIMAL(20,4)          COMMENT 'Valor total de mercado del equipo en esa temporada',
  valor_medio         DECIMAL(20,4)          COMMENT 'Valor medio por jugador en esa temporada',
  fecha_carga         TIMESTAMP     NOT NULL COMMENT 'Fecha y hora de la carga en DDV',
  periodo             INT           NOT NULL COMMENT 'Periodo técnico de carga en formato YYYY'
)
USING delta
PARTITIONED BY (periodo)
LOCATION 'abfss://${container_name}@${storage_account}.dfs.core.windows.net/${ruta_base}/ddv/ft_rendimiento_temporada/data'
COMMENT 'Fact de rendimiento competitivo por equipo y temporada (Apertura, Clausura, General) enriquecido con valoración de mercado de la Liga 1 Perú.';
