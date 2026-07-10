-- ==========================================================
-- DDV - FT_ESTADISTICAS_JUGADORES
-- Proyecto: Liga 1 Perú
-- ==========================================================

CREATE OR REPLACE TABLE ${catalog_name}.tb_ddv.ft_estadisticas_jugadores (
  id_estadistica_jugador   STRING        NOT NULL COMMENT 'PK: hash(id_jugador + id_equipo + temporada), granularidad jugador/equipo/temporada',
  id_jugador               STRING        NOT NULL COMMENT 'FK hacia md_plantillas (hash deterministico)',
  id_tm                    STRING                 COMMENT 'ID del jugador en Transfermarkt',
  jugador                  STRING                 COMMENT 'Nombre completo del jugador',
  foto_url                 STRING                 COMMENT 'URL de la foto del jugador en Transfermarkt',
  id_equipo                INT           NOT NULL COMMENT 'FK hacia dm_equipos, equipo cuyas stats se acumulan (parte de la PK natural)',
  nombre_equipo            STRING                 COMMENT 'Nombre oficial del equipo en la temporada',
  alias_equipo             STRING                 COMMENT 'Alias o nombre corto del equipo',
  temporada                INT           NOT NULL COMMENT 'Temporada (año) de las estadisticas',
  partidos_jugados         INT                    COMMENT 'Total de partidos jugados en la temporada (suma todas las competencias)',
  titularidades            INT                    COMMENT 'Total de partidos como titular en la temporada',
  ppp                      DOUBLE                 COMMENT 'Promedio de puntos por partido del equipo cuando el jugador jugo (avg sobre competencias)',
  goles                    INT                    COMMENT 'Total de goles en la temporada (incluye penaltis anotados)',
  asistencias              INT                    COMMENT 'Total de asistencias en la temporada',
  tarjetas_amarillas       INT                    COMMENT 'Total de tarjetas amarillas en la temporada',
  tarjeta_amarilla_roja    INT                    COMMENT 'Total de dobles amarillas en la temporada',
  tarjetas_rojas           INT                    COMMENT 'Total de tarjetas rojas en la temporada',
  entrada_suplente         INT                    COMMENT 'Total de veces que entro como suplente en la temporada (0 para porteros)',
  salida_suplente          INT                    COMMENT 'Total de veces que fue sustituido en la temporada (0 para porteros)',
  goles_en_contra          INT                    COMMENT 'Total de goles recibidos en la temporada, solo porteros (0 para otros)',
  partidos_imbatido        INT                    COMMENT 'Total de partidos sin goles en contra en la temporada, solo porteros (0 para otros)',
  min_por_gol              INT                    COMMENT 'Minutos por gol en la temporada, minutos_jugados dividido goles (0 para porteros)',
  minutos_jugados          INT                    COMMENT 'Total de minutos jugados en la temporada',
  datos_disponibles        BOOLEAN                COMMENT 'True si al menos una competencia tiene stats disponibles',
  posicion                 STRING                 COMMENT 'Posición del jugador en esa temporada',
  nacionalidad_principal   STRING                 COMMENT 'Nacionalidad principal del jugador',
  fecha_carga              TIMESTAMP     NOT NULL COMMENT 'Fecha y hora de la carga en DDV',
  periodo                  INT           NOT NULL COMMENT 'Periodo técnico de carga en formato YYYYMM'
)
USING delta
PARTITIONED BY (periodo)
LOCATION 'abfss://${container_name}@${storage_account}.dfs.core.windows.net/${ruta_base}/ddv/ft_estadisticas_jugadores/data'
COMMENT 'Fact de estadisticas individuales de jugadores por temporada (totalizado sobre todas las competencias), enriquecida con datos de Transfermarkt';
