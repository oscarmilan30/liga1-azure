-- ==========================================================
-- UDV - HM_ESTADISTICAS_JUGADORES
-- Proyecto: Liga 1 Perú
-- ==========================================================

CREATE OR REPLACE TABLE ${catalog_name}.tb_udv.hm_estadisticas_jugadores (
  id_estadistica_jugador   STRING        NOT NULL COMMENT 'PK: hash(id_tm + id_equipo + temporada + competencia)',
  id_jugador               STRING        NOT NULL COMMENT 'FK hacia md_plantillas (hash determinístico)',
  id_tm                    STRING        NOT NULL COMMENT 'ID del jugador en Transfermarkt',
  jugador                  STRING                 COMMENT 'Nombre completo del jugador',
  id_equipo                INT           NOT NULL COMMENT 'FK hacia md_catalogo_equipos (homologado via nombre_transfermarkt, parte de la PK natural)',
  temporada                INT           NOT NULL COMMENT 'Temporada (año) de las estadisticas',
  competencia              STRING        NOT NULL COMMENT 'Nombre de la competencia (Apertura, Clausura, etc.)',
  partidos_jugados         INT                    COMMENT 'Partidos jugados en la competencia',
  titularidades            INT                    COMMENT 'Partidos como titular',
  ppp                      DOUBLE                 COMMENT 'Puntos por partido del equipo cuando el jugador participo',
  goles                    INT                    COMMENT 'Goles anotados',
  asistencias              INT                    COMMENT 'Asistencias',
  tarjetas_amarillas       INT                    COMMENT 'Tarjetas amarillas',
  tarjeta_amarilla_roja    INT                    COMMENT 'Dobles amarillas (expulsion)',
  tarjetas_rojas           INT                    COMMENT 'Tarjetas rojas directas',
  entrada_suplente         INT                    COMMENT 'Veces que entro como suplente (0 para porteros)',
  salida_suplente          INT                    COMMENT 'Veces que fue sustituido (0 para porteros)',
  penaltis_anotados        INT                    COMMENT 'Penaltis anotados en la competencia (0 para porteros)',
  goles_en_contra          INT                    COMMENT 'Goles recibidos en la competencia, solo porteros (0 para otros)',
  partidos_imbatido        INT                    COMMENT 'Partidos sin goles en contra, portero invicto, solo porteros (0 para otros)',
  min_por_gol              INT                    COMMENT 'Minutos por gol ratio (0 para porteros)',
  minutos_jugados          INT                    COMMENT 'Minutos jugados totales',
  datos_disponibles        BOOLEAN                COMMENT 'True si TM devolvió stats, False si jugador sin datos',
  fuente                   STRING        NOT NULL COMMENT 'Fuente de los datos (Transfermarkt)',
  fecha_carga              TIMESTAMP     NOT NULL COMMENT 'Fecha y hora de la carga efectiva en UDV',
  periodo                  INT           NOT NULL COMMENT 'Periodo técnico de carga en formato YYYYMM'
)
USING delta
PARTITIONED BY (periodo)
LOCATION 'abfss://${container_name}@${storage_account}.dfs.core.windows.net/${ruta_base}/udv/hm_estadisticas_jugadores/data'
COMMENT 'Histórico de estadísticas individuales de jugadores por competenc