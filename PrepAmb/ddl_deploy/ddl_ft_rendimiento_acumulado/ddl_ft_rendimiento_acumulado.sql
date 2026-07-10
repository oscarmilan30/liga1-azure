-- ==========================================================
-- DDV - FT_RENDIMIENTO_ACUMULADO
-- Proyecto: Liga 1 Perú
-- ==========================================================

CREATE OR REPLACE TABLE ${catalog_name}.tb_ddv.ft_rendimiento_acumulado (
  id_acumulado            STRING        NOT NULL COMMENT 'Identificador único (hash: id_equipo + temporada)',
  id_equipo               INT           NOT NULL COMMENT 'Identificador único del equipo',
  nombre_equipo           STRING                 COMMENT 'Nombre oficial del equipo',
  alias_equipo            STRING                 COMMENT 'Alias o nombre corto del equipo',
  temporada               INT           NOT NULL COMMENT 'Temporada (año)',
  torneos_disputados      INT                    COMMENT 'Cantidad de torneos disputados en la temporada',
  total_pj                INT                    COMMENT 'Total partidos jugados en la temporada',
  total_pg                INT                    COMMENT 'Total partidos ganados',
  total_pe                INT                    COMMENT 'Total partidos empatados',
  total_pp                INT                    COMMENT 'Total partidos perdidos',
  total_goles_favor       INT                    COMMENT 'Total goles a favor en la temporada',
  total_goles_contra      INT                    COMMENT 'Total goles en contra en la temporada',
  total_diferencia_goles  INT                    COMMENT 'Diferencia de goles acumulada',
  total_puntos            INT                    COMMENT 'Total puntos acumulados en la temporada',
  valor_total             DECIMAL(20,4)          COMMENT 'Valor total de mercado del equipo en esa temporada',
  valor_medio             DECIMAL(20,4)          COMMENT 'Valor medio por jugador en esa temporada',
  fecha_carga             TIMESTAMP     NOT NULL COMMENT 'Fecha y hora de carga en DDV',
  periodo                 INT           NOT NULL COMMENT 'Periodo técnico YYYY (igual a temporada)'
)
USING delta
PARTITIONED BY (periodo)
LOCATION 'abfss://${container_name}@${storage_account}.dfs.core.windows.net/${ruta_base}/ddv/ft_rendimiento_acumulado/data'
COMMENT 'Fact de rendimiento acumulado por equipo y temporada (suma de todos los torneos: Apertura + Clausura + otros), enriquecido con valoración de mercado.';
