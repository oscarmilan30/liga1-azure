-- ==========================================================
-- ML - FT_SCORE_ML
-- Proyecto: Liga 1 Peru
-- ==========================================================

CREATE OR REPLACE TABLE ${catalog_name}.tb_ddv.ft_score_ml (
  id_score_ml            STRING        NOT NULL COMMENT 'PK: hash(id_jugador + id_equipo + temporada)',
  id_jugador             STRING        NOT NULL COMMENT 'FK hacia md_plantillas',
  id_tm                  STRING                 COMMENT 'ID del jugador en Transfermarkt',
  jugador                STRING                 COMMENT 'Nombre completo del jugador',
  foto_url               STRING                 COMMENT 'URL foto del jugador en Transfermarkt',
  id_equipo              INT           NOT NULL COMMENT 'FK hacia dm_equipos',
  nombre_equipo          STRING                 COMMENT 'Nombre oficial del equipo en la temporada',
  alias_equipo           STRING                 COMMENT 'Alias o nombre corto del equipo',
  temporada              INT           NOT NULL COMMENT 'Temporada (anio) de las estadisticas',
  posicion               STRING                 COMMENT 'Posicion textual del jugador',
  posicion_xi            STRING                 COMMENT 'Codigo de posicion estandar (DC, EI, ED, MCO, MC, MCD, DFC, LD, LI, POR)',
  partidos_jugados       INT                    COMMENT 'Total partidos jugados en la temporada',
  titularidades          INT                    COMMENT 'Total partidos como titular',
  minutos_jugados        INT                    COMMENT 'Total minutos jugados en la temporada',
  goles                  INT                    COMMENT 'Total goles en la temporada',
  asistencias            INT                    COMMENT 'Total asistencias en la temporada',
  ppp                    DOUBLE                 COMMENT 'Promedio de puntos por partido del equipo cuando el jugador jugo',
  tarjetas_amarillas     INT                    COMMENT 'Total tarjetas amarillas',
  tarjeta_amarilla_roja  INT                    COMMENT 'Total dobles amarillas (segunda amarilla)',
  tarjetas_rojas         INT                    COMMENT 'Total tarjetas rojas directas',
  entrada_suplente       INT                    COMMENT 'Veces que entro como suplente',
  salida_suplente        INT                    COMMENT 'Veces que fue sustituido',
  min_por_gol            INT                    COMMENT 'Minutos por gol (0 para porteros)',
  goles_en_contra        INT                    COMMENT 'Goles recibidos (solo porteros, 0 para otros)',
  partidos_imbatido      INT                    COMMENT 'Partidos sin goles en contra (solo porteros, 0 para otros)',
  goles_p90              DOUBLE                 COMMENT 'Goles por 90 minutos',
  asistencias_p90        DOUBLE                 COMMENT 'Asistencias por 90 minutos',
  participacion_p90      DOUBLE                 COMMENT 'Participacion en gol (goles+asistencias) por 90 minutos',
  amarillas_p90          DOUBLE                 COMMENT 'Tarjetas amarillas por 90 minutos (negativo = mas tarjetas)',
  rojas_p90              DOUBLE                 COMMENT 'Tarjetas rojas por 90 minutos (negativo = mas tarjetas)',
  segunda_am_p90         DOUBLE                 COMMENT 'Dobles amarillas por 90 minutos (negativo = mas tarjetas)',
  gc_p90                 DOUBLE                 COMMENT 'Goles recibidos por 90 min invertido (negativo = mas goles, solo porteros)',
  pi_p90                 DOUBLE                 COMMENT 'Partidos imbatido por 90 minutos (solo porteros)',
  score_ml               DOUBLE                 COMMENT 'Score bruto PCA (PC1), centrado en 0 por posicion',
  score_100              DOUBLE                 COMMENT 'Score normalizado 0-100 dentro de la posicion (0=peor, 100=mejor)',
  var_explicada          DOUBLE                 COMMENT 'Varianza explicada por PC1 (proporcion 0-1)',
  nivel_num              INT                    COMMENT 'Cluster K-means ordenado por score_ml: 0=Suplente, 1=Regular, 2=Bueno, 3=Elite',
  nivel                  STRING                 COMMENT 'Etiqueta del nivel: Suplente / Regular / Bueno / Elite',
  fecha_carga            TIMESTAMP     NOT NULL COMMENT 'Fecha y hora de carga en Delta',
  periodo                INT           NOT NULL COMMENT 'Anio de la temporada (particion tecnica)'
)
USING delta
PARTITIONED BY (temporada)
LOCATION 'abfss://${container_name}@${storage_account}.dfs.core.windows.net/${ruta_base}/ddv/ft_score_ml/data'
COMMENT 'Score ML de jugadores Liga 1 Peru: PCA por posicion (score_100) + K-means (nivel Elite/Bueno/Regular/Suplente)