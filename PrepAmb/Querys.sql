-- ══════════════════════════════════════════════════════════════
-- PASO 1: DROP Y RECREAR EN ORDEN CORRECTO (hijos antes que padres)
-- ══════════════════════════════════════════════════════════════
DROP TABLE IF EXISTS dbo.tbl_predecesores;
DROP TABLE IF EXISTS dbo.tbl_data_quality;
DROP TABLE IF EXISTS dbo.tbl_control_ejecucion;
DROP TABLE IF EXISTS dbo.tbl_pipeline_parametros;
DROP TABLE IF EXISTS dbo.tbl_paths;
DROP TABLE IF EXISTS dbo.tbl_flag_update_queue_id;
DROP TABLE IF EXISTS dbo.tbl_archivos_liga1;
DROP TABLE IF EXISTS dbo.tbl_pipeline;
GO

CREATE TABLE tbl_pipeline (
    PipelineId        INT IDENTITY(1,1) PRIMARY KEY,
    Nombre            VARCHAR(100) NOT NULL,
    Descripcion       VARCHAR(255) NULL,
    Nivel             VARCHAR(50)  NOT NULL,
    RutaBase          VARCHAR(200) NOT NULL,
    Activo            BIT DEFAULT 1,
    FechaCreacion     DATETIME DEFAULT GETDATE(),
    FechaModificacion DATETIME DEFAULT GETDATE(),
    Ruta              NVARCHAR(500) NULL,
    parent_pipelineid INT NULL
);
GO

-- ══════════════════════════════════════════════════════════════
-- PASO 2: INSERTAR EN ORDEN PARA QUE LOS IDs COINCIDAN CON LOS NOTEBOOKS
-- Convención de rutas (Ruta incluye prefijo primera_division/):
--   RDV  histórico   → primera_division/rdv/{entidad}/1FL/data/
--   RDV  incremental → primera_division/rdv/{entidad}/{año}/data/
--   RDV  dataentry   → primera_division/rdv/{entidad}/data/
--   UDV              → primera_division/udv/{tabla}/data/
--   DDV              → primera_division/ddv/{tabla}/data/
-- ══════════════════════════════════════════════════════════════
-- ID 1
INSERT INTO tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('Rdv_Liga1','Carga datos de Liga 1 Perú','RDV','liga1',1,NULL);
-- ID 2
INSERT INTO tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('md_equipos','Carga datos de equipos Liga 1 Perú','UDV','liga1',1,'primera_division/udv/md_equipos/data/');
-- ID 3
INSERT INTO tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('catalogo_equipos','Carga archivo de equipos Liga 1 Perú','RDV','liga1',1,'primera_division/rdv/catalogo_equipos/data/');
-- ID 4
INSERT INTO tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('md_catalogo_equipos','Carga catalogo de equipos Liga 1 Perú','UDV','liga1',1,'primera_division/udv/md_catalogo_equipos/data/');
-- ID 5
INSERT INTO tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('Job_ddl','Ejecucion de job ddl','-','-',1,'-');
-- ID 6
INSERT INTO tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('Create_jobs','Ejecucion de creacion de jobs','-','-',1,'-');
-- ID 7
INSERT INTO tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('md_estadios','Carga datos de estadios Liga 1 Perú','UDV','liga1',1,'primera_division/udv/md_estadios/data/');
-- ID 8
INSERT INTO tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('md_plantillas','Carga datos de catalogo de jugadores Liga 1 Perú','UDV','liga1',1,'primera_division/udv/md_plantillas/data/');
-- ID 9
INSERT INTO tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta, parent_pipelineid)
VALUES ('hm_plantillas_equipo','Carga datos de jugadores historico equipos Liga 1 Perú','UDV','liga1',1,'primera_division/udv/hm_plantillas_equipo/data/',8);
-- ID 10
INSERT INTO tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('plantillas','Carga archivo de plantillas Liga 1 Perú','RDV','liga1',1,'primera_division/rdv/plantillas/');
-- ID 11
INSERT INTO tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('md_entrenadores','Carga datos de catalogo de entrenadores Liga 1 Perú','UDV','liga1',1,'primera_division/udv/md_entrenadores/data/');
-- ID 12
INSERT INTO tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta, parent_pipelineid)
VALUES ('hm_entrenadores_equipo','Carga datos de entrenadores historico equipos Liga 1 Perú','UDV','liga1',1,'primera_division/udv/hm_entrenadores_equipo/data/',11);
-- ID 13
INSERT INTO tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('entrenadores','Carga archivo de entrenadores Liga 1 Perú','RDV','liga1',1,'primera_division/rdv/entrenadores/');
-- ID 14
INSERT INTO tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('hm_valoracion_equipos','Carga datos de valoracion clubes historico Liga 1 Perú','UDV','liga1',1,'primera_division/udv/hm_valoracion_equipos/data/');
-- ID 15
INSERT INTO tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('liga1','Carga archivo de valoracion equipos Liga 1 Perú','RDV','liga1',1,'primera_division/rdv/liga1/');
-- ID 16
INSERT INTO tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('hm_partidos','Carga datos hm_partidos historico Liga 1 Perú','UDV','liga1',1,'primera_division/udv/hm_partidos/data/');
-- ID 17
INSERT INTO tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('partidos','Carga archivo de partidos Liga 1 Perú','RDV','liga1',1,'primera_division/rdv/partidos/');
-- ID 18
INSERT INTO tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('hm_estadisticas_partidos','Carga datos de estadisticas partidos historico Liga 1 Perú','UDV','liga1',1,'primera_division/udv/hm_estadisticas_partidos/data/');
-- ID 19
INSERT INTO tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('estadisticas_partidos','Carga archivo de estadisticas partidos Liga 1 Perú','RDV','liga1',1,'primera_division/rdv/estadisticas_partidos/');
-- ID 20
INSERT INTO tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('hm_tablas_clasificacion','Carga datos de tablas clasificacion historico Liga 1 Perú','UDV','liga1',1,'primera_division/udv/hm_tablas_clasificacion/data/');
-- ID 21
INSERT INTO tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('tablas_clasificacion','Carga archivo de tablas clasificacion Liga 1 Perú','RDV','liga1',1,'primera_division/rdv/tablas_clasificacion/');
-- ID 22
INSERT INTO tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('hm_campeones','Carga datos de campeones Liga 1 Perú','UDV','liga1',1,'primera_division/udv/hm_campeones/data/');
-- ID 23
INSERT INTO tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('campeones','Carga archivo de campeones Liga 1 Perú','RDV','liga1',1,'primera_division/rdv/campeones/');
GO

-- ══════════════════════════════════════════════════════════════
-- PASO 3: RECREAR TABLAS DE SOPORTE
-- ══════════════════════════════════════════════════════════════
CREATE TABLE tbl_pipeline_parametros (
    ParametroId       INT IDENTITY(1,1) PRIMARY KEY,
    PipelineId        INT NOT NULL,
    Parametro         VARCHAR(100) NOT NULL,
    Valor             VARCHAR(500) NULL,
    Descripcion       VARCHAR(255) NULL,
    FechaCreacion     DATETIME DEFAULT GETDATE(),
    FechaModificacion DATETIME DEFAULT GETDATE(),
    FOREIGN KEY (PipelineId) REFERENCES tbl_pipeline(PipelineId)
);

CREATE TABLE tbl_control_ejecucion (
    IdEjecucion      INT IDENTITY(1,1) PRIMARY KEY,
    PipelineId       INT NOT NULL,
    pipeline_name    VARCHAR(200) NOT NULL,
    pipeline_type    VARCHAR(200) NOT NULL,
    execution_id     UNIQUEIDENTIFIER NOT NULL,
    id_ejecucion_e2e INT NULL,
    id_sesion        INT NULL,           -- IdEjecucion del E2E padre que agrupa toda la ejecución
    status           VARCHAR(20) NOT NULL,
    start_time       DATETIME NULL,
    end_time         DATETIME NULL,
    mensaje_error    VARCHAR(500) NULL,
    FOREIGN KEY (PipelineId) REFERENCES tbl_pipeline(PipelineId)
);

CREATE TABLE dbo.tbl_data_quality (
    id                    INT            IDENTITY(1,1) PRIMARY KEY,
    id_ejecucion          INT            NULL,
    id_ejecucion_e2e      INT            NULL,
    pipeline_id           INT            NOT NULL,
    capa                  VARCHAR(10)    NOT NULL,
    tabla_nombre          VARCHAR(100)   NOT NULL,
    registros_entrada     INT            NULL,
    registros_salida      INT            NULL,
    registros_descartados INT            NULL,
    registros_nulos_clave INT            NULL,
    registros_duplicados  INT            NULL,
    observaciones         VARCHAR(500)   NULL,
    fecha_ejecucion       DATETIME       DEFAULT GETDATE(),
    CONSTRAINT FK_dq_ejecucion FOREIGN KEY (id_ejecucion)
        REFERENCES tbl_control_ejecucion(IdEjecucion)
);

CREATE TABLE dbo.tbl_archivos_liga1 (
    Id     INT IDENTITY(1,1) PRIMARY KEY,
    Nombre NVARCHAR(100),
    Tipo   NVARCHAR(10)
);

INSERT INTO tbl_archivos_liga1 (Nombre, Tipo) VALUES
('campeones',               'json'),
('equipos',                 'json'),
('estadisticas_partidos',   'json'),
('partidos',                'json'),
('tablas_clasificacion',    'json'),
('entrenadores',            'csv'),
('estadios',                'csv'),
('liga1',                   'csv'),
('plantillas',              'csv'),
('estadisticas_jugadores',  'csv');

CREATE TABLE dbo.tbl_paths (
    Id                  INT IDENTITY(1,1) PRIMARY KEY,
    PipelineId          INT NULL,
    RunId               NVARCHAR(100) NULL,
    Entidad             VARCHAR(100) NOT NULL,
    ModoEjecucion       VARCHAR(50) NOT NULL,
    Periodo             VARCHAR(50) NULL,
    RutaRaw             VARCHAR(500) NOT NULL,
    flg_udv             CHAR(1) DEFAULT 'N',
    FechaCreacion       DATETIME DEFAULT GETDATE(),
    FechaActualizacion  DATETIME DEFAULT GETDATE(),
    UsuarioCreacion     VARCHAR(100),
    UsuarioActualizacion VARCHAR(100),
    CONSTRAINT FK_tbl_paths_pipeline FOREIGN KEY (PipelineId) REFERENCES dbo.tbl_pipeline(PipelineId)
);

CREATE TABLE dbo.tbl_predecesores (
    IdPredecesor          INT IDENTITY(1,1) PRIMARY KEY,
    PipelineId_Predecesor INT NOT NULL,
    PipelineId_Destino    INT NOT NULL,
    Ruta_Predecesor       NVARCHAR(500) NOT NULL,
    FechaCreacion         DATETIME DEFAULT GETDATE(),
    RutaTabla             NVARCHAR(200) NULL
);

CREATE TABLE dbo.tbl_flag_update_queue_id (
    id    INT,
    fecha DATETIME DEFAULT GETDATE()
);
GO

-- ══════════════════════════════════════════════════════════════
-- PASO 4: PARÁMETROS
-- Notas de rutas:
--   SCHEMA_TABLA = 'tb_udv' → nombre de schema en Unity Catalog (ADLS: udv/{tabla}/data/)
--   SCHEMA_TABLA = 'tb_ddv' → nombre de schema en Unity Catalog (ADLS: ddv/{tabla}/data/)
--   RUTA_BASE    = ''    → ADF concat queda rdv/{entidad}/data/
-- ══════════════════════════════════════════════════════════════

-- ID 1: Raw_Liga1 (orquestador RAW principal)
INSERT INTO tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(1,'RUTA_BASE',           'primera_division',                                      'División dentro del contenedor ADLS'),
(1,'URL_BASE_FOTMOB',         'https://www.fotmob.com/es',                        'URL base FotMob'),
(1,'LEAGUE_ID:FOTMOB',        '131',                                               'ID Liga 1 en FotMob'),
(1,'URL_SEASON_PARAM_FOTMOB', 'season',                                            'Filtro temporada'),
(1,'FILESYSTEM',              'liga1',                                             'Contenedor ADLS'),
(1,'CAPA_LANDING',            'primera_division/landing',                                           'Contenedor landing'),
(1,'RUTABASE',                '/fotmob/liga1',                                     'Ruta base landing fotmob'),
(1,'CAPA_RDV',                'primera_division/rdv',                                               'Contenedor rdv'),
(1,'MODO_EJECUCION',          'HISTORICO',                                         'HISTORICO/INCREMENTAL/REPROCESO'),
(1,'CURRENT_YEAR',            CAST(YEAR(GETDATE()) AS VARCHAR(4)),                 'Año actual'),
(1,'HISTORIC_START_YEAR',     '2020',                                              'Año inicio histórico'),
(1,'FLG_REPROCESO',           '0',                                                 '0=Normal, 1=Reproceso'),
(1,'ANIO_REPROCESO',          NULL,                                                'Año de reproceso'),
(1,'SPARK_VERSION',           '15.4.x-scala2.12',                                  'Versión Spark'),
(1,'NODE_TYPE',               'Standard_D4s_v3',                                   'Tipo de nodo'),
(1,'NUM_WORKERS',             '0',                                                  'Workers (0=single node)'),
(1,'AUTOTERMINATION',         '15',                                                 'Minutos auto-terminación');

-- ID 2: md_equipos (UDV)
INSERT INTO tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(2,'RUTA_BASE',     'primera_division',    'División dentro del contenedor ADLS'),
(2,'FILESYSTEM',    'liga1',              'Contenedor ADLS'),
(2,'CAPA_UDV',      'udv',               'Capa UDV'),
(2,'FORMATO_SALIDA','delta',             'Formato Delta'),
(2,'TIPO_CARGA',    'FULL',              'Carga completa'),
(2,'SCHEMA_TABLA',  'tb_udv',              'Esquema tablas Delta → carpeta ADLS udv/'),
(2,'SCHEMA_VISTA',  'vw_udv',           'Esquema vistas'),
(2,'RUTA_TABLA',    '/md_equipos',       'Ruta tabla Delta'),
(2,'NOMBRE_TABLA',  'md_equipos',        'Nombre tabla UDV'),
(2,'YAML_PATH',     '/frm_udv/conf/md_equipos/md_equipos.yml','Ruta YAML');

-- ID 3: catalogo_equipos (RDV DataEntry)
INSERT INTO tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(3,'RUTA_BASE',     'primera_division',    'División dentro del contenedor ADLS'),
(3,'FILESYSTEM',    'liga1',   'Contenedor ADLS'),
(3,'CAPA_LANDING',  'primera_division/landing', 'Contenedor landing'),
(3,'CAPA_RDV',      'primera_division/rdv',     'Contenedor rdv'),
(3,'FORMATO_SALIDA','parquet', 'Formato salida'),
(3,'TIPO_CARGA',    'FULL',    'Tipo carga'),
(3,'NOMBRE_ARCHIVO','catalogo_equipos',                                   'Nombre entidad'),
(3,'JOB_ID',        '714992434497721',                                   'Job Databricks');

-- ID 4: md_catalogo_equipos (UDV)
INSERT INTO tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(4,'RUTA_BASE',     'primera_division',    'División dentro del contenedor ADLS'),
(4,'FILESYSTEM',    'liga1',               'Contenedor ADLS'),
(4,'SCHEMA_TABLA',  'tb_udv',               'Esquema tablas Delta → carpeta ADLS udv/'),
(4,'SCHEMA_VISTA',  'vw_udv',            'Esquema vistas'),
(4,'CAPA_UDV',      'udv',               'Capa UDV'),
(4,'RUTA_TABLA',    '/md_catalogo_equipos','Ruta tabla Delta'),
(4,'FORMATO_SALIDA','delta',             'Formato Delta'),
(4,'TIPO_CARGA',    'FULL',              'Carga completa'),
(4,'NOMBRE_TABLA',  'md_catalogo_equipos','Nombre tabla UDV'),
(4,'YAML_PATH',     '/frm_udv/conf/md_catalogo_equipos/md_catalogo_equipos.yml','Ruta YAML');

-- ID 5: Job_ddl
INSERT INTO tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(5,'JOB_ID', '370187792936402',                                   'Job DDL Databricks');

-- ID 6: Create_jobs
INSERT INTO tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(6,'JOB_ID',     '57622873974351',                                     'Job create workflows');

-- ID 7: md_estadios (UDV)
INSERT INTO tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(7,'RUTA_BASE',     'primera_division',    'División dentro del contenedor ADLS'),
(7,'SCHEMA_TABLA',  'tb_udv',             'Esquema tablas Delta → carpeta ADLS udv/'),
(7,'CAPA_UDV',      'udv',             'Capa UDV'),
(7,'RUTA_TABLA',    '/md_estadios',    'Ruta tabla Delta'),
(7,'FORMATO_SALIDA','delta',           'Formato Delta'),
(7,'TIPO_CARGA',    'FULL',            'Carga completa'),
(7,'NOMBRE_TABLA',  'md_estadios',     'Nombre tabla UDV'),
(7,'YAML_PATH',     '/frm_udv/conf/md_estadios/md_estadios.yml','Ruta YAML');

-- ID 8: md_plantillas (UDV)
INSERT INTO tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(8,'RUTA_BASE',     'primera_division',    'División dentro del contenedor ADLS'),
(8,'SCHEMA_TABLA',  'tb_udv',                                                      'Esquema tablas Delta → carpeta ADLS udv/'),
(8,'CAPA_UDV',      'udv',                                                      'Capa UDV'),
(8,'RUTA_TABLA',    '/md_plantillas',                                           'Ruta tabla Delta'),
(8,'FORMATO_SALIDA','delta',                                                    'Formato Delta'),
(8,'TIPO_CARGA',    'FULL',                                                     'Carga completa'),
(8,'NOMBRE_TABLA',  'md_plantillas',                                            'Nombre tabla UDV'),
(8,'YAML_PATH',     '/frm_udv/conf/hm_plantillas_equipo/hm_plantillas_equipo.yml','Ruta YAML');

-- ID 9: hm_plantillas_equipo (UDV)
INSERT INTO tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(9,'RUTA_BASE',     'primera_division',    'División dentro del contenedor ADLS'),
(9,'SCHEMA_TABLA',  'tb_udv',                                                      'Esquema tablas Delta → carpeta ADLS udv/'),
(9,'CAPA_UDV',      'udv',                                                      'Capa UDV'),
(9,'RUTA_TABLA',    '/hm_plantillas_equipo',                                    'Ruta tabla Delta'),
(9,'FORMATO_SALIDA','delta',                                                    'Formato Delta'),
(9,'TIPO_CARGA',    'INCREMENTAL',                                              'Tipo carga'),
(9,'NOMBRE_TABLA',  'hm_plantillas_equipo',                                     'Nombre tabla UDV'),
(9,'YAML_PATH',     '/frm_udv/conf/hm_plantillas_equipo/hm_plantillas_equipo.yml','Ruta YAML');

-- ID 10: plantillas (RAW)
INSERT INTO tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(10,'RUTA_BASE',     'primera_division',    'División dentro del contenedor ADLS'),
(10,'FILESYSTEM',    'liga1',       'Contenedor ADLS'),
(10,'CAPA_LANDING',  'primera_division/landing',     'Contenedor landing'),
(10,'CAPA_RDV',      'primera_division/rdv',         'Contenedor rdv'),
(10,'FORMATO_SALIDA','parquet',     'Formato salida'),
(10,'TIPO_CARGA',    'INCREMENTAL', 'Tipo carga'),
(10,'NOMBRE_ARCHIVO','plantillas',  'Nombre entidad');

-- ID 11: md_entrenadores (UDV)
INSERT INTO tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(11,'RUTA_BASE',     'primera_division',    'División dentro del contenedor ADLS'),
(11,'SCHEMA_TABLA',  'tb_udv',                                                          'Esquema tablas Delta → carpeta ADLS udv/'),
(11,'CAPA_UDV',      'udv',                                                          'Capa UDV'),
(11,'RUTA_TABLA',    '/md_entrenadores',                                             'Ruta tabla Delta'),
(11,'FORMATO_SALIDA','delta',                                                        'Formato Delta'),
(11,'TIPO_CARGA',    'FULL',                                                         'Carga completa'),
(11,'NOMBRE_TABLA',  'md_entrenadores',                                              'Nombre tabla UDV'),
(11,'YAML_PATH',     '/frm_udv/conf/hm_entrenadores_equipo/hm_entrenadores_equipo.yml','Ruta YAML');

-- ID 12: hm_entrenadores_equipo (UDV)
INSERT INTO tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(12,'RUTA_BASE',     'primera_division',    'División dentro del contenedor ADLS'),
(12,'SCHEMA_TABLA',  'tb_udv',                                                          'Esquema tablas Delta → carpeta ADLS udv/'),
(12,'CAPA_UDV',      'udv',                                                          'Capa UDV'),
(12,'RUTA_TABLA',    '/hm_entrenadores_equipo',                                      'Ruta tabla Delta'),
(12,'FORMATO_SALIDA','delta',                                                        'Formato Delta'),
(12,'TIPO_CARGA',    'INCREMENTAL',                                                  'Tipo carga'),
(12,'NOMBRE_TABLA',  'hm_entrenadores_equipo',                                       'Nombre tabla UDV'),
(12,'YAML_PATH',     '/frm_udv/conf/hm_entrenadores_equipo/hm_entrenadores_equipo.yml','Ruta YAML');

-- ID 13: entrenadores (RAW)
INSERT INTO tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(13,'RUTA_BASE',     'primera_division',    'División dentro del contenedor ADLS'),
(13,'FILESYSTEM',    'liga1',          'Contenedor ADLS'),
(13,'CAPA_LANDING',  'primera_division/landing',        'Contenedor landing'),
(13,'CAPA_RDV',      'primera_division/rdv',            'Contenedor rdv'),
(13,'FORMATO_SALIDA','parquet',        'Formato salida'),
(13,'TIPO_CARGA',    'INCREMENTAL',    'Tipo carga'),
(13,'NOMBRE_ARCHIVO','entrenadores',   'Nombre entidad');

-- ID 14: hm_valoracion_equipos (UDV)
INSERT INTO tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(14,'RUTA_BASE',     'primera_division',    'División dentro del contenedor ADLS'),
(14,'SCHEMA_TABLA',  'tb_udv',                                                       'Esquema tablas Delta → carpeta ADLS udv/'),
(14,'CAPA_UDV',      'udv',                                                       'Capa UDV'),
(14,'RUTA_TABLA',    '/hm_valoracion_equipos',                                    'Ruta tabla Delta'),
(14,'FORMATO_SALIDA','delta',                                                     'Formato Delta'),
(14,'TIPO_CARGA',    'INCREMENTAL',                                               'Tipo carga'),
(14,'NOMBRE_TABLA',  'hm_valoracion_equipos',                                     'Nombre tabla UDV'),
(14,'YAML_PATH',     '/frm_udv/conf/hm_valoracion_equipos/hm_valoracion_equipos.yml','Ruta YAML');

-- ID 15: liga1 (RAW)
INSERT INTO tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(15,'RUTA_BASE',     'primera_division',    'División dentro del contenedor ADLS'),
(15,'FILESYSTEM',    'liga1',       'Contenedor ADLS'),
(15,'CAPA_LANDING',  'primera_division/landing',     'Contenedor landing'),
(15,'CAPA_RDV',      'primera_division/rdv',         'Contenedor rdv'),
(15,'FORMATO_SALIDA','parquet',     'Formato salida'),
(15,'TIPO_CARGA',    'INCREMENTAL', 'Tipo carga'),
(15,'NOMBRE_ARCHIVO','liga1',       'Nombre entidad');

-- ID 16: hm_partidos (UDV)
INSERT INTO tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(16,'RUTA_BASE',     'primera_division',    'División dentro del contenedor ADLS'),
(16,'SCHEMA_TABLA',  'tb_udv',                                    'Esquema tablas Delta → carpeta ADLS udv/'),
(16,'CAPA_UDV',      'udv',                                    'Capa UDV'),
(16,'RUTA_TABLA',    '/hm_partidos',                           'Ruta tabla Delta'),
(16,'FORMATO_SALIDA','delta',                                  'Formato Delta'),
(16,'TIPO_CARGA',    'INCREMENTAL',                            'Tipo carga'),
(16,'NOMBRE_TABLA',  'hm_partidos',                            'Nombre tabla UDV'),
(16,'YAML_PATH',     '/frm_udv/conf/hm_partidos/hm_partidos.yml','Ruta YAML');

-- ID 17: partidos (RAW)
INSERT INTO tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(17,'RUTA_BASE',     'primera_division',    'División dentro del contenedor ADLS'),
(17,'FILESYSTEM',    'liga1',       'Contenedor ADLS'),
(17,'CAPA_LANDING',  'primera_division/landing',     'Contenedor landing'),
(17,'CAPA_RDV',      'primera_division/rdv',         'Contenedor rdv'),
(17,'FORMATO_SALIDA','parquet',     'Formato salida'),
(17,'TIPO_CARGA',    'INCREMENTAL', 'Tipo carga'),
(17,'NOMBRE_ARCHIVO','partidos',    'Nombre entidad');

-- ID 18: hm_estadisticas_partidos (UDV)
INSERT INTO tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(18,'RUTA_BASE',     'primera_division',    'División dentro del contenedor ADLS'),
(18,'SCHEMA_TABLA',  'tb_udv',                                                                     'Esquema tablas Delta → carpeta ADLS udv/'),
(18,'CAPA_UDV',      'udv',                                                                     'Capa UDV'),
(18,'RUTA_TABLA',    '/hm_estadisticas_partidos',                                               'Ruta tabla Delta'),
(18,'FORMATO_SALIDA','delta',                                                                   'Formato Delta'),
(18,'TIPO_CARGA',    'INCREMENTAL',                                                             'Tipo carga'),
(18,'NOMBRE_TABLA',  'hm_estadisticas_partidos',                                                'Nombre tabla UDV'),
(18,'YAML_PATH',     '/frm_udv/conf/hm_estadisticas_partidos/hm_estadisticas_partidos.yml',    'Ruta YAML');

-- ID 19: estadisticas_partidos (RAW)
INSERT INTO tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(19,'RUTA_BASE',     'primera_division',             'División dentro del contenedor ADLS'),
(19,'FILESYSTEM',    'liga1',                   'Contenedor ADLS'),
(19,'CAPA_LANDING',  'primera_division/landing',                 'Contenedor landing'),
(19,'CAPA_RDV',      'primera_division/rdv',                     'Contenedor rdv'),
(19,'FORMATO_SALIDA','parquet',                 'Formato salida'),
(19,'TIPO_CARGA',    'INCREMENTAL',             'Tipo carga'),
(19,'NOMBRE_ARCHIVO','estadisticas_partidos',   'Nombre entidad');

-- ID 20: hm_tablas_clasificacion (UDV)
INSERT INTO tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(20,'RUTA_BASE',     'primera_division',    'División dentro del contenedor ADLS'),
(20,'SCHEMA_TABLA',  'tb_udv',                                                                  'Esquema tablas Delta → carpeta ADLS udv/'),
(20,'CAPA_UDV',      'udv',                                                                  'Capa UDV'),
(20,'RUTA_TABLA',    '/hm_tablas_clasificacion',                                             'Ruta tabla Delta'),
(20,'FORMATO_SALIDA','delta',                                                                'Formato Delta'),
(20,'TIPO_CARGA',    'INCREMENTAL',                                                         'Tipo carga'),
(20,'NOMBRE_TABLA',  'hm_tablas_clasificacion',                                              'Nombre tabla UDV'),
(20,'YAML_PATH',     '/frm_udv/conf/hm_tablas_clasificacion/hm_tablas_clasificacion.yml',   'Ruta YAML');

-- ID 21: tablas_clasificacion (RAW)
INSERT INTO tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(21,'RUTA_BASE',     'primera_division',          'División dentro del contenedor ADLS'),
(21,'FILESYSTEM',    'liga1',                'Contenedor ADLS'),
(21,'CAPA_LANDING',  'primera_division/landing',              'Contenedor landing'),
(21,'CAPA_RDV',      'primera_division/rdv',                  'Contenedor rdv'),
(21,'FORMATO_SALIDA','parquet',              'Formato salida'),
(21,'TIPO_CARGA',    'INCREMENTAL',          'Tipo carga'),
(21,'NOMBRE_ARCHIVO','tablas_clasificacion', 'Nombre entidad');

-- ID 22: hm_campeones (UDV)
INSERT INTO tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(22,'RUTA_BASE',     'primera_division',    'División dentro del contenedor ADLS'),
(22,'SCHEMA_TABLA',  'tb_udv',                                             'Esquema tablas Delta → carpeta ADLS udv/'),
(22,'CAPA_UDV',      'udv',                                             'Capa UDV'),
(22,'RUTA_TABLA',    '/hm_campeones',                                   'Ruta tabla Delta'),
(22,'FORMATO_SALIDA','delta',                                           'Formato Delta'),
(22,'TIPO_CARGA',    'INCREMENTAL',                                     'Tipo carga'),
(22,'NOMBRE_TABLA',  'hm_campeones',                                    'Nombre tabla UDV'),
(22,'YAML_PATH',     '/frm_udv/conf/hm_campeones/hm_campeones.yml',    'Ruta YAML');

-- ID 23: campeones (RAW)
INSERT INTO tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(23,'RUTA_BASE',     'primera_division',    'División dentro del contenedor ADLS'),
(23,'FILESYSTEM',    'liga1',       'Contenedor ADLS'),
(23,'CAPA_LANDING',  'primera_division/landing',     'Contenedor landing'),
(23,'CAPA_RDV',      'primera_division/rdv',         'Contenedor rdv'),
(23,'FORMATO_SALIDA','parquet',     'Formato salida'),
(23,'TIPO_CARGA',    'INCREMENTAL', 'Tipo carga'),
(23,'NOMBRE_ARCHIVO','campeones',   'Nombre entidad');
GO


-- ══════════════════════════════════════════════════════════════
-- STORED PROCEDURES
-- ══════════════════════════════════════════════════════════════

-- SP PARA INICIAR REGISTRO DE EJECUCIÓN
CREATE OR ALTER PROCEDURE sp_log_start
    @PipelineId INT,
    @pipeline_name VARCHAR(200),
    @pipeline_type VARCHAR(50),
    @execution_id UNIQUEIDENTIFIER,
    @id_ejecucion_e2e INT = NULL,
    @id_sesion INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO tbl_control_ejecucion (
        PipelineId,
        pipeline_name,
        pipeline_type,
        execution_id,
        id_ejecucion_e2e,
        id_sesion,
        status,
        start_time,
        end_time
    )
    VALUES (
        @PipelineId,
        @pipeline_name,
        @pipeline_type,
        @execution_id,
        @id_ejecucion_e2e,
        @id_sesion,
        'Running',
        GETDATE(),
        NULL
    );

    SELECT CAST(SCOPE_IDENTITY() AS INT) as IdEjecucion;
END
GO

-- SP PARA FINALIZAR REGISTRO DE EJECUCIÓN
CREATE OR ALTER PROCEDURE dbo.sp_log_end
    @IdEjecucion INT = NULL,
    @ExecutionId UNIQUEIDENTIFIER = NULL,
    @PipelineId INT = NULL,
    @Status VARCHAR(20),
    @MensajeError VARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    IF @IdEjecucion IS NOT NULL
    BEGIN
        UPDATE dbo.tbl_control_ejecucion
        SET status = @Status,
            end_time = GETDATE(),
            mensaje_error = @MensajeError
        WHERE IdEjecucion = @IdEjecucion;
    END
    ELSE IF @ExecutionId IS NOT NULL
    BEGIN
        UPDATE dbo.tbl_control_ejecucion
        SET status = @Status,
            end_time = GETDATE(),
            mensaje_error = @MensajeError
        WHERE execution_id = @ExecutionId;
    END
    ELSE IF @PipelineId IS NOT NULL
    BEGIN
        ;WITH cte AS (
            SELECT TOP (1) IdEjecucion
            FROM dbo.tbl_control_ejecucion
            WHERE PipelineId = @PipelineId AND status = 'Running'
            ORDER BY start_time DESC
        )
        UPDATE t
        SET status = @Status,
            end_time = GETDATE(),
            mensaje_error = @MensajeError
        FROM dbo.tbl_control_ejecucion t
        INNER JOIN cte ON t.IdEjecucion = cte.IdEjecucion;
    END

    SELECT @@ROWCOUNT AS RowsAffected;
END
GO


CREATE OR ALTER PROCEDURE sp_obtener_parametros
    @PipelineId INT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @cols NVARCHAR(MAX);
    DECLARE @sql NVARCHAR(MAX);

    SELECT @cols = STRING_AGG(QUOTENAME(Parametro), ',')
    FROM dbo.tbl_pipeline_parametros
    WHERE PipelineId = @PipelineId;

    SET @sql = N'
        SELECT ' + @cols + '
        FROM (
            SELECT Parametro, Valor
            FROM dbo.tbl_pipeline_parametros
            WHERE PipelineId = @PipelineIdParam
        ) AS src
        PIVOT (
            MAX(Valor) FOR Parametro IN (' + @cols + ')
        ) AS pvt;
    ';

    EXEC sp_executesql @sql, N'@PipelineIdParam INT', @PipelineIdParam = @PipelineId;
END
GO

CREATE OR ALTER PROCEDURE sp_obtener_rango_anios
    @StartYear INT,
    @EndYear INT
AS
BEGIN
    WITH YearsCTE AS (
        SELECT @StartYear as Anio
        UNION ALL
        SELECT Anio + 1
        FROM YearsCTE
        WHERE Anio < @EndYear - 1
    )
    SELECT Anio
    FROM YearsCTE
END
GO


CREATE OR ALTER PROCEDURE sp_obtener_archivos
AS
BEGIN
    SET NOCOUNT ON;

    SELECT (
        SELECT Nombre AS nombre, Tipo AS tipo
        FROM tbl_archivos_liga1
        FOR JSON PATH
    ) AS lista_archivos;
END;
GO

CREATE OR ALTER PROCEDURE dbo.sp_cancelar_ejecuciones_huerfanas
    @MinutosUmbral INT = 30
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE dbo.tbl_control_ejecucion
    SET status        = 'CANCELLED',
        end_time      = GETDATE(),
        mensaje_error = 'Pipeline cancelado o interrumpido — limpieza automática al inicio de nueva ejecución'
    WHERE status = 'Running'
      AND DATEDIFF(MINUTE, start_time, GETDATE()) >= @MinutosUmbral;

    SELECT @@ROWCOUNT AS RegistrosCancelados;
END
GO

CREATE OR ALTER PROCEDURE [dbo].[sp_actualizar_modo_ejecucion]
    @NuevoModo VARCHAR(20),
    @PipelineId INT
AS
BEGIN
    UPDATE tbl_pipeline_parametros
    SET Valor = '0',
        FechaModificacion = GETDATE()
    WHERE PipelineId = @PipelineId
    AND Parametro = 'FLG_REPROCESO';

    UPDATE tbl_pipeline_parametros
    SET Valor = 'INCREMENTAL',
        FechaModificacion = GETDATE()
    WHERE PipelineId = @PipelineId
    AND Parametro = 'MODO_EJECUCION';
END
GO

CREATE OR ALTER PROCEDURE [dbo].[sp_registrar_path]
(
    @PipelineId      INT = NULL,
    @RunId           NVARCHAR(100) = NULL,
    @Entidad         VARCHAR(100),
    @ModoEjecucion   VARCHAR(50),
    @Periodo         VARCHAR(50),
    @RutaRaw         VARCHAR(500)
)
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        DECLARE @Usuario NVARCHAR(100) = SYSTEM_USER;

        IF @Entidad IS NULL OR @RutaRaw IS NULL
        BEGIN
            RAISERROR('Los parámetros Entidad y RutaRaw son obligatorios.', 16, 1);
            RETURN;
        END;

        IF @ModoEjecucion = 'HISTORICO'
        BEGIN
            PRINT 'Modo HISTORICO detectado: no se registrará en tbl_paths (manejado en SP independiente).';
            RETURN;
        END;

        INSERT INTO dbo.tbl_paths (
            PipelineId, RunId, Entidad, ModoEjecucion, Periodo,
            RutaRaw, flg_udv, FechaCreacion, FechaActualizacion,
            UsuarioCreacion, UsuarioActualizacion
        )
        VALUES (
            @PipelineId, @RunId, @Entidad, @ModoEjecucion, @Periodo,
            @RutaRaw, 'N', GETDATE(), GETDATE(), @Usuario, @Usuario
        );

        PRINT CONCAT('Ruta registrada correctamente para ', @Entidad, ' → ', @RutaRaw);
    END TRY

    BEGIN CATCH
        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
        DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
        DECLARE @ErrorState INT = ERROR_STATE();
        PRINT CONCAT('Error en sp_registrar_path: ', @ErrorMessage);
        RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);
    END CATCH;
END;
GO


CREATE OR ALTER PROCEDURE [dbo].[sp_registrar_path_historico]
(
    @PipelineId      INT = NULL,
    @RunId           NVARCHAR(100) = NULL,
    @Entidad         VARCHAR(100),
    @ModoEjecucion   VARCHAR(50) = 'HISTORICO',
    @Periodo         VARCHAR(50),
    @RutaRaw         VARCHAR(500)
)
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        DECLARE @Usuario NVARCHAR(100) = SYSTEM_USER;

        IF @Entidad IS NULL OR @RutaRaw IS NULL
        BEGIN
            RAISERROR('Los parámetros Entidad y RutaRaw son obligatorios.', 16, 1);
            RETURN;
        END;

        MERGE dbo.tbl_paths AS target
        USING (SELECT @Entidad AS Entidad, @ModoEjecucion AS ModoEjecucion, @RutaRaw AS RutaRaw) AS src
        ON  target.Entidad       = src.Entidad
        AND target.ModoEjecucion = src.ModoEjecucion
        AND target.RutaRaw       = src.RutaRaw
        WHEN MATCHED THEN
            UPDATE SET
                RunId               = @RunId,
                PipelineId          = @PipelineId,
                flg_udv             = 'N',
                FechaActualizacion  = GETDATE(),
                UsuarioActualizacion = @Usuario
        WHEN NOT MATCHED THEN
            INSERT (PipelineId, RunId, Entidad, ModoEjecucion, Periodo,
                    RutaRaw, flg_udv, FechaCreacion, FechaActualizacion,
                    UsuarioCreacion, UsuarioActualizacion)
            VALUES (@PipelineId, @RunId, @Entidad, @ModoEjecucion, @Periodo,
                    @RutaRaw, 'N', GETDATE(), GETDATE(), @Usuario, @Usuario);

        PRINT CONCAT('Ruta histórica registrada/actualizada correctamente: ', @RutaRaw);
    END TRY

    BEGIN CATCH
        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
        DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
        DECLARE @ErrorState INT = ERROR_STATE();
        PRINT CONCAT('Error en sp_registrar_path_historico: ', @ErrorMessage);
        RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);
    END CATCH;
END;
GO


CREATE OR ALTER TRIGGER trg_update_tbl_paths_by_id
ON dbo.tbl_flag_update_queue_id
AFTER INSERT
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE p
      SET p.flg_udv = 'S',
          p.FechaActualizacion = SYSDATETIME()
      FROM dbo.tbl_paths AS p
      INNER JOIN inserted AS i
              ON p.id = i.id
     WHERE p.flg_udv = 'N';
END;
GO

CREATE OR ALTER PROCEDURE dbo.sp_registrar_predecesor
    @PipelineId_Predecesor INT,
    @PipelineId_Destino   INT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @Ruta           NVARCHAR(500);
    DECLARE @SchemaTabla    SYSNAME;
    DECLARE @NombreTabla    SYSNAME;
    DECLARE @RutaTabla      NVARCHAR(200);
    DECLARE @NombrePipeline SYSNAME;

    SELECT
        @Ruta           = Ruta,
        @NombrePipeline = Nombre
    FROM dbo.tbl_pipeline
    WHERE PipelineId = @PipelineId_Predecesor;

    IF @Ruta IS NULL
    BEGIN
        RAISERROR('No se encontró la ruta del pipeline predecesor.', 16, 1);
        RETURN;
    END;

    SELECT
        @SchemaTabla = MAX(CASE WHEN Parametro = 'SCHEMA_TABLA' THEN Valor END),
        @NombreTabla = MAX(CASE WHEN Parametro = 'NOMBRE_TABLA'  THEN Valor END)
    FROM dbo.tbl_pipeline_parametros
    WHERE PipelineId = @PipelineId_Predecesor;

    IF @SchemaTabla IS NOT NULL AND @NombreTabla IS NOT NULL
        SET @RutaTabla = @SchemaTabla + N'.' + @NombreTabla;  -- ej: udv.md_catalogo_equipos
    ELSE
        SET @RutaTabla = @NombrePipeline;  -- ej: 'catalogo_equipos'

    INSERT INTO dbo.tbl_predecesores (
        PipelineId_Predecesor,
        PipelineId_Destino,
        Ruta_Predecesor,
        RutaTabla
    )
    VALUES (
        @PipelineId_Predecesor,
        @PipelineId_Destino,
        @Ruta,
        @RutaTabla
    );

    PRINT 'Relación registrada: '
        + CAST(@PipelineId_Predecesor AS NVARCHAR(10))
        + ' → '
        + CAST(@PipelineId_Destino AS NVARCHAR(10))
        + ' | RutaTabla = ' + ISNULL(@RutaTabla,'(sin nombre lógico)');
END;
GO


-- ══════════════════════════════════════════════════════════════
-- PASO 5: PREDECESORES UDV
-- ══════════════════════════════════════════════════════════════
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 3,  @PipelineId_Destino = 4;  -- catalogo RDV → md_catalogo
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 4,  @PipelineId_Destino = 2;  -- md_catalogo → md_equipos
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 4,  @PipelineId_Destino = 7;  -- md_catalogo → md_estadios
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 10, @PipelineId_Destino = 8;  -- plantillas RDV → md_plantillas
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 8,  @PipelineId_Destino = 9;  -- md_plantillas → hm_plantillas_equipo
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 4,  @PipelineId_Destino = 9;  -- md_catalogo → hm_plantillas_equipo
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 13, @PipelineId_Destino = 11; -- entrenadores RDV → md_entrenadores
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 11, @PipelineId_Destino = 12; -- md_entrenadores → hm_entrenadores_equipo
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 4,  @PipelineId_Destino = 12; -- md_catalogo → hm_entrenadores_equipo
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 15, @PipelineId_Destino = 14; -- liga1 RDV → hm_valoracion_equipos
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 4,  @PipelineId_Destino = 14; -- md_catalogo → hm_valoracion_equipos
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 17, @PipelineId_Destino = 16; -- partidos RDV → hm_partidos
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 4,  @PipelineId_Destino = 16; -- md_catalogo → hm_partidos
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 19, @PipelineId_Destino = 18; -- estadisticas RDV → hm_estadisticas
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 16, @PipelineId_Destino = 18; -- hm_partidos → hm_estadisticas
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 4,  @PipelineId_Destino = 18; -- md_catalogo → hm_estadisticas
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 21, @PipelineId_Destino = 20; -- tablas_clas RDV → hm_tablas_clasificacion
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 4,  @PipelineId_Destino = 20; -- md_catalogo → hm_tablas_clasificacion
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 23, @PipelineId_Destino = 22; -- campeones RDV → hm_campeones
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 4,  @PipelineId_Destino = 22; -- md_catalogo → hm_campeones
GO


-- ══════════════════════════════════════════════════════════════
-- INSERTAR JOB_ID (UDV)
-- NOTA: AREA, USER y CLUSTER_ID se configuran como ADF Global Parameters en adf-ligafutbol
-- ══════════════════════════════════════════════════════════════

-- PipelineId 2 — md_equipos
INSERT INTO dbo.tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(2, 'JOB_ID',     '44750911017741',             'Job Databricks');

-- PipelineId 7 — md_estadios
INSERT INTO dbo.tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(7, 'JOB_ID',     '978795707605016',            'Job Databricks');

-- PipelineId 9 — hm_plantillas_equipo
INSERT INTO dbo.tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(9, 'JOB_ID',     '936359907070631',            'Job Databricks');

-- PipelineId 12 — hm_entrenadores_equipo
INSERT INTO dbo.tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(12, 'JOB_ID',     '534622475772964',            'Job Databricks');

-- PipelineId 14 — hm_valoracion_equipos
INSERT INTO dbo.tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(14, 'JOB_ID',     '885217775361142',            'Job Databricks');

-- PipelineId 16 — hm_partidos
INSERT INTO dbo.tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(16, 'JOB_ID',     '784070021485241',            'Job Databricks');

-- PipelineId 18 — hm_estadisticas_partidos
INSERT INTO dbo.tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(18, 'JOB_ID',     '868442276579652',            'Job Databricks');

-- PipelineId 20 — hm_tablas_clasificacion
INSERT INTO dbo.tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(20, 'JOB_ID',     '1112548248273773',           'Job Databricks');

-- PipelineId 22 — hm_campeones
INSERT INTO dbo.tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(22, 'JOB_ID',     '651368301409624',            'Job Databricks');

-- ID 24: Orquestador UDV
INSERT INTO dbo.tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('pl_Orchestrator_ligaperuana_udv','Orquestador principal capa UDV Liga 1 Perú','UDV','liga1',1,NULL);
INSERT INTO dbo.tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(24, 'NOMBRE_TABLA', 'pl_Orchestrator_ligaperuana_udv', 'Nombre identificador del orquestador'),
(24, 'NIVEL',        'UDV',                             'Nivel de capa');

-- ID 25: Workflow UDV
INSERT INTO dbo.tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta, parent_pipelineid)
VALUES ('sch_ligaperuana_udv','Workflow Databricks capa UDV Liga 1 Perú','UDV','liga1',1,NULL,24);
INSERT INTO dbo.tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(25, 'JOB_ID',  '355338234078938',            'ID job Databricks');

GO


-- ══════════════════════════════════════════════════════════════
-- ORQUESTADOR FIX + DDV: tbl_pipeline (IDs 26-32)
-- ══════════════════════════════════════════════════════════════
-- ID 26: Orquestador Fix
INSERT INTO dbo.tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('pl_Orchestrator_fix','Orquestador Fix pipelines Liga 1 Perú','FIX','liga1',1,NULL);
-- ID 27
INSERT INTO dbo.tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('dm_equipos','Dimension equipos capa DDV Liga 1 Peru','DDV','liga1',1,'primera_division/ddv/dm_equipos/data/');
-- ID 28
INSERT INTO dbo.tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('ft_rendimiento_temporada','Fact rendimiento temporada capa DDV Liga 1 Peru','DDV','liga1',1,'primera_division/ddv/ft_rendimiento_temporada/data/');
-- ID 29
INSERT INTO dbo.tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('ft_partidos_detalle','Fact detalle de partidos capa DDV Liga 1 Peru','DDV','liga1',1,'primera_division/ddv/ft_partidos_detalle/data/');
-- ID 30
INSERT INTO dbo.tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('ft_plantillas_historico','Fact plantillas historicas capa DDV Liga 1 Peru','DDV','liga1',1,'primera_division/ddv/ft_plantillas_historico/data/');
-- ID 31
INSERT INTO dbo.tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('ft_entrenadores_historico','Fact entrenadores historicos capa DDV Liga 1 Peru','DDV','liga1',1,'primera_division/ddv/ft_entrenadores_historico/data/');
-- ID 32
INSERT INTO dbo.tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('ft_evolucion_valoracion','Fact evolucion valoracion capa DDV Liga 1 Peru','DDV','liga1',1,'primera_division/ddv/ft_evolucion_valoracion/data/');
GO

-- ══════════════════════════════════════════════════════════════
-- DDV: tbl_pipeline_parametros (IDs 27-32)
-- ══════════════════════════════════════════════════════════════

-- ID 26: pl_Orchestrator_fix
INSERT INTO dbo.tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(26, 'NOMBRE_TABLA', 'pl_Orchestrator_fix', 'Nombre identificador del orquestador Fix'),
(26, 'NIVEL',        'FIX',                 'Nivel de capa');

-- ID 27: dm_equipos (DDV)
INSERT INTO dbo.tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(27, 'RUTA_BASE',     'primera_division',    'División dentro del contenedor ADLS'),
(27, 'FILESYSTEM',    'liga1',                                          'Contenedor ADLS'),
(27, 'CAPA_DDV',      'ddv',                                           'Capa DDV'),
(27, 'FORMATO_SALIDA','delta',                                         'Formato Delta'),
(27, 'SCHEMA_TABLA',  'tb_ddv',                                          'Esquema tablas Delta → carpeta ADLS ddv/'),
(27, 'NOMBRE_TABLA',  'dm_equipos',                                   'Nombre tabla DDV'),
(27, 'YAML_PATH',     '/frm_ddv/conf/dm_equipos/dm_equipos.yml',      'Ruta YAML');

-- ID 28: ft_rendimiento_temporada (DDV)
INSERT INTO dbo.tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(28, 'RUTA_BASE',     'primera_division',    'División dentro del contenedor ADLS'),
(28, 'FILESYSTEM',    'liga1',                                                                        'Contenedor ADLS'),
(28, 'CAPA_DDV',      'ddv',                                                                         'Capa DDV'),
(28, 'FORMATO_SALIDA','delta',                                                                       'Formato Delta'),
(28, 'SCHEMA_TABLA',  'tb_ddv',                                                                        'Esquema tablas Delta → carpeta ADLS ddv/'),
(28, 'NOMBRE_TABLA',  'ft_rendimiento_temporada',                                                   'Nombre tabla DDV'),
(28, 'YAML_PATH',     '/frm_ddv/conf/ft_rendimiento_temporada/ft_rendimiento_temporada.yml',        'Ruta YAML');

-- ID 29: ft_partidos_detalle (DDV)
INSERT INTO dbo.tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(29, 'RUTA_BASE',     'primera_division',    'División dentro del contenedor ADLS'),
(29, 'FILESYSTEM',    'liga1',                                                          'Contenedor ADLS'),
(29, 'CAPA_DDV',      'ddv',                                                           'Capa DDV'),
(29, 'FORMATO_SALIDA','delta',                                                         'Formato Delta'),
(29, 'SCHEMA_TABLA',  'tb_ddv',                                                          'Esquema tablas Delta → carpeta ADLS ddv/'),
(29, 'NOMBRE_TABLA',  'ft_partidos_detalle',                                          'Nombre tabla DDV'),
(29, 'YAML_PATH',     '/frm_ddv/conf/ft_partidos_detalle/ft_partidos_detalle.yml',    'Ruta YAML');

-- ID 30: ft_plantillas_historico (DDV)
INSERT INTO dbo.tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(30, 'RUTA_BASE',     'primera_division',    'División dentro del contenedor ADLS'),
(30, 'FILESYSTEM',    'liga1',                                                                      'Contenedor ADLS'),
(30, 'CAPA_DDV',      'ddv',                                                                       'Capa DDV'),
(30, 'FORMATO_SALIDA','delta',                                                                     'Formato Delta'),
(30, 'SCHEMA_TABLA',  'tb_ddv',                                                                      'Esquema tablas Delta → carpeta ADLS ddv/'),
(30, 'NOMBRE_TABLA',  'ft_plantillas_historico',                                                  'Nombre tabla DDV'),
(30, 'YAML_PATH',     '/frm_ddv/conf/ft_plantillas_historico/ft_plantillas_historico.yml',        'Ruta YAML');

-- ID 31: ft_entrenadores_historico (DDV)
INSERT INTO dbo.tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(31, 'RUTA_BASE',     'primera_division',    'División dentro del contenedor ADLS'),
(31, 'FILESYSTEM',    'liga1',                                                                          'Contenedor ADLS'),
(31, 'CAPA_DDV',      'ddv',                                                                           'Capa DDV'),
(31, 'FORMATO_SALIDA','delta',                                                                         'Formato Delta'),
(31, 'SCHEMA_TABLA',  'tb_ddv',                                                                          'Esquema tablas Delta → carpeta ADLS ddv/'),
(31, 'NOMBRE_TABLA',  'ft_entrenadores_historico',                                                    'Nombre tabla DDV'),
(31, 'YAML_PATH',     '/frm_ddv/conf/ft_entrenadores_historico/ft_entrenadores_historico.yml',        'Ruta YAML');

-- ID 32: ft_evolucion_valoracion (DDV)
INSERT INTO dbo.tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(32, 'RUTA_BASE',     'primera_division',    'División dentro del contenedor ADLS'),
(32, 'FILESYSTEM',    'liga1',                                                                      'Contenedor ADLS'),
(32, 'CAPA_DDV',      'ddv',                                                                       'Capa DDV'),
(32, 'FORMATO_SALIDA','delta',                                                                     'Formato Delta'),
(32, 'SCHEMA_TABLA',  'tb_ddv',                                                                      'Esquema tablas Delta → carpeta ADLS ddv/'),
(32, 'NOMBRE_TABLA',  'ft_evolucion_valoracion',                                                  'Nombre tabla DDV'),
(32, 'YAML_PATH',     '/frm_ddv/conf/ft_evolucion_valoracion/ft_evolucion_valoracion.yml',        'Ruta YAML');
GO

-- ══════════════════════════════════════════════════════════════
-- PREDECESORES DDV (IDs 27-32)
-- ══════════════════════════════════════════════════════════════

-- dm_equipos (27)
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 4,  @PipelineId_Destino = 27; -- md_catalogo_equipos  → dm_equipos
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 2,  @PipelineId_Destino = 27; -- md_equipos           → dm_equipos
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 7,  @PipelineId_Destino = 27; -- md_estadios          → dm_equipos
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 14, @PipelineId_Destino = 27; -- hm_valoracion_equipos → dm_equipos

-- ft_rendimiento_temporada (28)
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 20, @PipelineId_Destino = 28; -- hm_tablas_clasificacion → ft_rendimiento_temporada
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 14, @PipelineId_Destino = 28; -- hm_valoracion_equipos  → ft_rendimiento_temporada
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 27, @PipelineId_Destino = 28; -- dm_equipos             → ft_rendimiento_temporada

-- ft_partidos_detalle (29)
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 16, @PipelineId_Destino = 29; -- hm_partidos              → ft_partidos_detalle
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 18, @PipelineId_Destino = 29; -- hm_estadisticas_partidos → ft_partidos_detalle
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 27, @PipelineId_Destino = 29; -- dm_equipos               → ft_partidos_detalle

-- ft_plantillas_historico (30)
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 9,  @PipelineId_Destino = 30; -- hm_plantillas_equipo → ft_plantillas_historico
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 8,  @PipelineId_Destino = 30; -- md_plantillas        → ft_plantillas_historico
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 27, @PipelineId_Destino = 30; -- dm_equipos           → ft_plantillas_historico

-- ft_entrenadores_historico (31)
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 12, @PipelineId_Destino = 31; -- hm_entrenadores_equipo → ft_entrenadores_historico
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 11, @PipelineId_Destino = 31; -- md_entrenadores        → ft_entrenadores_historico
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 27, @PipelineId_Destino = 31; -- dm_equipos             → ft_entrenadores_historico

-- ft_evolucion_valoracion (32)
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 14, @PipelineId_Destino = 32; -- hm_valoracion_equipos → ft_evolucion_valoracion
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 27, @PipelineId_Destino = 32; -- dm_equipos            → ft_evolucion_valoracion
GO


-- ══════════════════════════════════════════════════════════════
-- ORQUESTADORES DDV/E2E + FIX + DATAENTRYS: tbl_pipeline (IDs 33-38)
-- ══════════════════════════════════════════════════════════════
-- ID 33: Orquestador DDV
INSERT INTO dbo.tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('pl_Orchestrator_ligaperuana_ddv','Orquestador principal capa DDV Liga 1 Perú','DDV','liga1',1,NULL);
-- ID 34: Workflow DDV (hijo de ID 33)
INSERT INTO dbo.tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta, parent_pipelineid)
VALUES ('sch_ddv_liga1','Workflow Databricks capa DDV Liga 1 Perú','DDV','liga1',1,NULL,33);
-- ID 35: Orquestador E2E
INSERT INTO dbo.tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('pl_Orchestrator_E2E_liga1','Orquestador End-to-End Liga 1 Perú','E2E','liga1',1,NULL);
-- ID 36: Fix plantillas
INSERT INTO dbo.tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('pl_fix_plantillas','Pipeline fix de plantillas Liga 1 Perú','FIX','liga1',1,NULL);
-- ID 37: Dataentry posiciones
INSERT INTO dbo.tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('posiciones','Dataentry posiciones jugadores en español','RDV','liga1',1,'primera_division/rdv/posiciones/data/');
-- ID 38: Dataentry nacionalidades
INSERT INTO dbo.tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('nacionalidades','Dataentry nacionalidades en español','RDV','liga1',1,'primera_division/rdv/nacionalidades/data/');
-- ID 39: Orquestador Dataentrys
INSERT INTO dbo.tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('pl_Orchestrator_dataentrys','Orquestador dataentrys (posiciones, nacionalidades, catálogo equipos)','DATAENTRY','liga1',1,NULL);
GO

-- ══════════════════════════════════════════════════════════════
-- tbl_pipeline_parametros (IDs 33-39)
-- ══════════════════════════════════════════════════════════════

-- ID 33: pl_Orchestrator_ligaperuana_ddv
INSERT INTO dbo.tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(33, 'NOMBRE_TABLA', 'pl_Orchestrator_ligaperuana_ddv', 'Nombre identificador del orquestador DDV'),
(33, 'NIVEL',        'DDV',                              'Nivel de capa');

-- ID 34: sch_ddv_liga1 (Workflow DDV Databricks)
-- NOTA: actualizar JOB_ID con el valor del nuevo entorno; AREA y USER se configuran como ADF Global Parameters en adf-ligafutbol
INSERT INTO dbo.tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(34, 'NOMBRE_TABLA', 'sch_ddv_liga1',         'Nombre workflow DDV Databricks'),
(34, 'NIVEL',        'DDV',                   'Nivel de capa'),
(34, 'JOB_ID',       '<REEMPLAZAR_JOB_ID>',  'Job ID workflow DDV (crear en Databricks)');

-- ID 35: pl_Orchestrator_E2E_liga1
INSERT INTO dbo.tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(35, 'NOMBRE_TABLA', 'pl_Orchestrator_E2E_liga1', 'Nombre identificador del orquestador E2E'),
(35, 'NIVEL',        'E2E',                        'Nivel de capa');

-- ID 36: pl_fix_plantillas
INSERT INTO dbo.tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(36, 'NOMBRE_TABLA', 'pl_fix_plantillas',      'Nombre pipeline fix plantillas'),
(36, 'NIVEL',        'FIX',                    'Nivel de capa');

-- ID 37: posiciones (dataentry)
INSERT INTO dbo.tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(37, 'RUTA_BASE',     'primera_division',    'División dentro del contenedor ADLS'),
(37, 'FILESYSTEM',    'liga1',       'Contenedor ADLS'),
(37, 'CAPA_LANDING',  'primera_division/landing',    'Contenedor landing'),
(37, 'CAPA_RDV',      'primera_division/rdv',        'Contenedor rdv'),
(37, 'FORMATO_SALIDA','parquet',    'Formato salida'),
(37, 'TIPO_CARGA',    'FULL',       'Tipo carga'),
(37, 'NOMBRE_ARCHIVO','posiciones', 'Nombre entidad');

-- ID 38: nacionalidades (dataentry)
INSERT INTO dbo.tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(38, 'RUTA_BASE',     'primera_division',    'División dentro del contenedor ADLS'),
(38, 'FILESYSTEM',    'liga1',           'Contenedor ADLS'),
(38, 'CAPA_LANDING',  'primera_division/landing',        'Contenedor landing'),
(38, 'CAPA_RDV',      'primera_division/rdv',            'Contenedor rdv'),
(38, 'FORMATO_SALIDA','parquet',        'Formato salida'),
(38, 'TIPO_CARGA',    'FULL',           'Tipo carga'),
(38, 'NOMBRE_ARCHIVO','nacionalidades', 'Nombre entidad');

-- ID 39: pl_Orchestrator_dataentrys
INSERT INTO dbo.tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(39, 'NOMBRE_TABLA', 'pl_Orchestrator_dataentrys', 'Nombre identificador del orquestador dataentrys'),
(39, 'NIVEL',        'DATAENTRY',                   'Nivel de capa');
GO

-- Predecesores posiciones (37) y nacionalidades (38)
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 37, @PipelineId_Destino = 9;  -- posiciones    → hm_plantillas_equipo
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 38, @PipelineId_Destino = 8;  -- nacionalidades → md_plantillas
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 38, @PipelineId_Destino = 11; -- nacionalidades → md_entrenadores
GO


-- ══════════════════════════════════════════════════════════════
-- DDV: tbl_pipeline (ID 40)
-- ══════════════════════════════════════════════════════════════
-- ID 40: ft_rendimiento_acumulado (DDV)
INSERT INTO dbo.tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('ft_rendimiento_acumulado','Fact rendimiento acumulado por equipo y temporada capa DDV','DDV','liga1',1,'primera_division/ddv/ft_rendimiento_acumulado/data/');
GO

-- ══════════════════════════════════════════════════════════════
-- DDV: tbl_pipeline_parametros (ID 40)
-- ══════════════════════════════════════════════════════════════

-- ID 40: ft_rendimiento_acumulado (DDV)
INSERT INTO dbo.tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(40, 'RUTA_BASE',     'primera_division',    'División dentro del contenedor ADLS'),
(40, 'FILESYSTEM',    'liga1',                                                                              'Contenedor ADLS'),
(40, 'CAPA_DDV',      'ddv',                                                                               'Capa DDV'),
(40, 'FORMATO_SALIDA','delta',                                                                             'Formato Delta'),
(40, 'SCHEMA_TABLA',  'tb_ddv',                                                                            'Esquema tablas Delta → carpeta ADLS ddv/'),
(40, 'NOMBRE_TABLA',  'ft_rendimiento_acumulado',                                                         'Nombre tabla DDV'),
(40, 'YAML_PATH',     '/frm_ddv/conf/ft_rendimiento_acumulado/ft_rendimiento_acumulado.yml',              'Ruta YAML');
GO

-- ══════════════════════════════════════════════════════════════
-- PREDECESORES DDV (ID 40)
-- ══════════════════════════════════════════════════════════════

-- ft_rendimiento_acumulado (40)
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 20, @PipelineId_Destino = 40; -- hm_tablas_clasificacion → ft_rendimiento_acumulado
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 14, @PipelineId_Destino = 40; -- hm_valoracion_equipos  → ft_rendimiento_acumulado
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 27, @PipelineId_Destino = 40; -- dm_equipos             → ft_rendimiento_acumulado
GO


-- ══════════════════════════════════════════════════════════════
-- FIX: parámetros faltantes pipeline 36 (pl_fix_plantillas)
-- ══════════════════════════════════════════════════════════════
INSERT INTO dbo.tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(36, 'FILESYSTEM',        'liga1',                                                      'Contenedor ADLS'),
(36, 'YAML_PATH',         '/frm_udv/conf/fix_plantillas/fix_plantillas.yml',            'Ruta YAML config fix'),
(36, 'LANDING_CSV_PATH',  'primera_division/landing/dataentrys/plantillas',             'CSVs plantillas en landing'),
(36, 'RDV_FIX_PATH',      'primera_division/rdv/plantillas_fix',                        'Carpeta fix parquet — separada de rdv/plantillas FotMob');
GO

-- ══════════════════════════════════════════════════════════════
-- FIX: tbl_pipeline (ID 41) — fix_estadisticas_partidos
-- ══════════════════════════════════════════════════════════════
-- ID 41: fix_estadisticas_partidos
INSERT INTO dbo.tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('fix_estadisticas_partidos','Pipeline fix hm_estadisticas_partidos histórico 2020-2025','FIX','liga1',1,NULL);
GO

-- ══════════════════════════════════════════════════════════════
-- FIX: tbl_pipeline_parametros (ID 41)
-- ══════════════════════════════════════════════════════════════
INSERT INTO dbo.tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(41, 'FILESYSTEM',         'liga1',                                                                          'Contenedor ADLS'),
(41, 'YAML_PATH',          '/frm_udv/conf/hm_estadisticas_partidos_fix/hm_estadisticas_partidos_fix.yml',   'Ruta YAML config fix'),
(41, 'SCHEMA_TABLA',       'tb_udv',                                                                        'Schema Unity Catalog'),
(41, 'NOMBRE_TABLA',       'hm_estadisticas_partidos',                                                      'Nombre tabla UDV de salida'),
(41, 'LANDING_JSON_PATH',  'primera_division/landing/dataentrys/estadisticas_historicas',                   'JSONs estadísticas en landing'),
(41, 'RDV_FIX_PATH',       'primera_division/rdv/estadisticas_historicas',                                  'Carpeta temporal parquet fix'),
(41, 'OUTPUT_UDV_PATH',    'primera_division/udv/hm_estadisticas_partidos/data',                            'Ruta Delta ADLS tabla UDV destino');
GO

-- ══════════════════════════════════════════════════════════════
-- FIX: predecesores (ID 41)
-- ══════════════════════════════════════════════════════════════
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 16, @PipelineId_Destino = 41; -- hm_partidos → fix_estadisticas_partidos
GO



-- ══════════════════════════════════════════════════════════════
-- ESTADISTICAS JUGADORES: tbl_pipeline (IDs 42-44)
-- ══════════════════════════════════════════════════════════════
-- ID 42: estadisticas_jugadores (RDV CSV)
INSERT INTO dbo.tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('estadisticas_jugadores','Carga archivo de estadisticas jugadores Liga 1 Perú','RDV','liga1',1,'primera_division/rdv/estadisticas_jugadores/');
-- ID 43: hm_estadisticas_jugadores (UDV)
INSERT INTO dbo.tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('hm_estadisticas_jugadores','Carga datos de estadisticas jugadores historico Liga 1 Perú','UDV','liga1',1,'primera_division/udv/hm_estadisticas_jugadores/data/');
-- ID 44: ft_estadisticas_jugadores (DDV)
INSERT INTO dbo.tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('ft_estadisticas_jugadores','Fact estadisticas jugadores capa DDV Liga 1 Peru','DDV','liga1',1,'primera_division/ddv/ft_estadisticas_jugadores/data/');
GO

-- ══════════════════════════════════════════════════════════════
-- ESTADISTICAS JUGADORES: tbl_pipeline_parametros (IDs 42-44)
-- ══════════════════════════════════════════════════════════════

-- ID 42: estadisticas_jugadores (RDV)
INSERT INTO tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(42,'RUTA_BASE',     'primera_division',         'División dentro del contenedor ADLS'),
(42,'FILESYSTEM',    'liga1',                    'Contenedor ADLS'),
(42,'CAPA_LANDING',  'primera_division/landing', 'Contenedor landing'),
(42,'CAPA_RDV',      'primera_division/rdv',     'Contenedor rdv'),
(42,'FORMATO_SALIDA','parquet',                  'Formato salida'),
(42,'TIPO_CARGA',    'INCREMENTAL',              'Tipo carga'),
(42,'NOMBRE_ARCHIVO','estadisticas_jugadores',   'Nombre entidad');

-- ID 43: hm_estadisticas_jugadores (UDV)
INSERT INTO tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(43,'RUTA_BASE',     'primera_division',                                                               'División dentro del contenedor ADLS'),
(43,'SCHEMA_TABLA',  'tb_udv',                                                                         'Esquema tablas Delta → carpeta ADLS udv/'),
(43,'CAPA_UDV',      'udv',                                                                            'Capa UDV'),
(43,'RUTA_TABLA',    '/hm_estadisticas_jugadores',                                                     'Ruta tabla Delta'),
(43,'FORMATO_SALIDA','delta',                                                                          'Formato Delta'),
(43,'TIPO_CARGA',    'INCREMENTAL',                                                                    'Tipo carga'),
(43,'NOMBRE_TABLA',  'hm_estadisticas_jugadores',                                                      'Nombre tabla UDV'),
(43,'YAML_PATH',     '/frm_udv/conf/hm_estadisticas_jugadores/hm_estadisticas_jugadores.yml',          'Ruta YAML');

-- ID 44: ft_estadisticas_jugadores (DDV)
INSERT INTO tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(44,'RUTA_BASE',     'primera_division',                                                                    'División dentro del contenedor ADLS'),
(44,'FILESYSTEM',    'liga1',                                                                               'Contenedor ADLS'),
(44,'CAPA_DDV',      'ddv',                                                                                'Capa DDV'),
(44,'FORMATO_SALIDA','delta',                                                                              'Formato Delta'),
(44,'SCHEMA_TABLA',  'tb_ddv',                                                                              'Esquema tablas Delta → carpeta ADLS ddv/'),
(44,'NOMBRE_TABLA',  'ft_estadisticas_jugadores',                                                           'Nombre tabla DDV'),
(44,'YAML_PATH',     '/frm_ddv/conf/ft_estadisticas_jugadores/ft_estadisticas_jugadores.yml',               'Ruta YAML');
GO

-- ══════════════════════════════════════════════════════════════
-- ESTADISTICAS JUGADORES: PREDECESORES
-- ══════════════════════════════════════════════════════════════
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 42, @PipelineId_Destino = 43; -- estadisticas_jugadores RDV → hm_estadisticas_jugadores
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 8,  @PipelineId_Destino = 43; -- md_plantillas             → hm_estadisticas_jugadores
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 4,  @PipelineId_Destino = 43; -- md_catalogo_equipos       → hm_estadisticas_jugadores
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 43, @PipelineId_Destino = 44; -- hm_estadisticas_jugadores → ft_estadisticas_jugadores
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 8,  @PipelineId_Destino = 44; -- md_plantillas             → ft_estadisticas_jugadores
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 27, @PipelineId_Destino = 44; -- dm_equipos                → ft_estadisticas_jugadores

-- JOB_IDs pendientes estadisticas_jugadores (configurar tras crear jobs en Databricks)
-- INSERT INTO dbo.tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
-- (43, 'JOB_ID', '<REEMPLAZAR_JOB_ID_HM_EST_JUG>', 'Job Databricks hm_estadisticas_jugadores');
-- (44, 'JOB_ID', '<REEMPLAZAR_JOB_ID_FT_EST_JUG>', 'Job Databricks ft_estadisticas_jugadores');
GO

-- ══════════════════════════════════════════════════════════════
-- PASO 6: VERIFICACIÓN FINAL
-- ══════════════════════════════════════════════════════════════
SELECT p.PipelineId, p.Nombre, p.Nivel,
       COUNT(pp.ParametroId) AS Params,
       p.parent_pipelineid
FROM tbl_pipeline p
LEFT JOIN tbl_pipeline_parametros pp ON p.PipelineId = pp.PipelineId
GROUP BY p.PipelineId, p.Nombre, p.Nivel, p.parent_pipelineid
ORDER BY p.PipelineId;

SELECT * FROM dbo.tbl_predecesores ORDER BY PipelineId_Destino, PipelineId_Predecesor;


-- ══════════════════════════════════════════════════════════════
-- PASO 7: JOB_IDs PENDIENTES DE CONFIGURAR
-- Los siguientes JOB_IDs aún requieren valor real del entorno Databricks.
-- Actualizar antes de ejecutar pipelines DDV.
-- NOTA: AREA, USER y CLUSTER_ID se configuran como ADF Global Parameters,
--       NO en esta tabla.
-- ══════════════════════════════════════════════════════════════

-- JOB_IDs ya configurados en los INSERTs anteriores:
--   PipelineId  1  (Rdv_Liga1 scraper)      → actualizar si cambia el job
--   PipelineId  3  (catalogo_equipos)        → 714992434497721
--   PipelineId  5  (Job_ddl)                 → 370187792936402
--   PipelineId  6  (Create_jobs)             → 57622873974351
--   PipelineId  2  (md_equipos)              → 44750911017741
--   PipelineId  7  (md_estadios)             → 978795707605016
--   PipelineId  9  (hm_plantillas_equipo)    → 936359907070631
--   PipelineId 12  (hm_entrenadores_equipo)  → 534622475772964
--   PipelineId 14  (hm_valoracion_equipos)   → 885217775361142
--   PipelineId 16  (hm_partidos)             → 784070021485241
--   PipelineId 18  (hm_estadisticas_partidos)→ 868442276579652
--   PipelineId 20  (hm_tablas_clasificacion) → 1112548248273773
--   PipelineId 22  (hm_campeones)            → 651368301409624
--   PipelineId 25  (sch_ligaperuana_udv)     → 355338234078938

-- JOB_ID PENDIENTE — reemplazar con el valor real del workflow DDV:
UPDATE dbo.tbl_pipeline_parametros
SET Valor = '<REEMPLAZAR_JOB_ID_DDV>'
WHERE PipelineId = 34 AND Parametro = 'JOB_ID';
GO

-- ══════════════════════════════════════════════════════════════
-- PASO 8: VERIFICACIÓN FINAL DE JOB_IDs
-- ══════════════════════════════════════════════════════════════
SELECT p.PipelineId, p.Nombre, p.Nivel, pp.Parametro, pp.Valor
FROM dbo.tbl_pipeline p
JOIN dbo.tbl_pipeline_parametros pp ON p.PipelineId = pp.PipelineId
WHERE pp.Parametro = 'JOB_ID'
ORDER BY p.PipelineId;



select*from tbl_pipeline_parametros
select*from tbl_paths


-- ══════════════════════════════════════════════════════════════
-- ML SCORE: tbl_pipeline (IDs 45-47)
-- ══════════════════════════════════════════════════════════════
-- ID 45: ft_score_ml (notebook ML individual)
INSERT INTO dbo.tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('ft_score_ml','Score ML jugadores Liga 1 Perú: PCA + K-means por posición','ML','liga1',1,'primera_division/ddv/ft_score_ml/data/');
-- ID 46: pl_Orchestrator_ligaperuana_ml (orquestador ADF)
INSERT INTO dbo.tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('pl_Orchestrator_ligaperuana_ml','Pipeline orquestador ADF para capa ML Liga 1 Perú','ML','liga1',1,'');
-- ID 47: sch_ml_liga1 (Databricks workflow scheduler)
INSERT INTO dbo.tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('sch_ml_liga1','Databricks workflow scheduler capa ML Liga 1 Perú','ML','liga1',1,'');
GO

-- ══════════════════════════════════════════════════════════════
-- ML SCORE: tbl_pipeline_parametros (IDs 45-47)
-- ══════════════════════════════════════════════════════════════

-- ID 45: ft_score_ml (notebook ML)
INSERT INTO tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(45,'RUTA_BASE',     'primera_division',                                              'División dentro del contenedor ADLS'),
(45,'FILESYSTEM',    'liga1',                                                         'Contenedor ADLS'),
(45,'CAPA_DDV',      'ddv',                                                           'Capa DDV (tabla de salida)'),
(45,'FORMATO_SALIDA','delta',                                                         'Formato Delta'),
(45,'SCHEMA_TABLA',  'tb_ddv',                                                        'Esquema tablas Delta → carpeta ADLS ddv/'),
(45,'NOMBRE_TABLA',  'ft_score_ml',                                                   'Nombre tabla ML'),
(45,'YAML_PATH',     '/frm_ml/conf/ft_score_ml/ft_score_ml.yml',                     'Ruta YAML configuración ML');

-- ID 46: pl_Orchestrator_ligaperuana_ml
-- Sin parámetros adicionales; PipelineId y MLWorkflowPipelineId se pasan desde E2E.

-- ID 47: sch_ml_liga1 (Databricks workflow — JOB_ID a configurar tras crear job)
INSERT INTO tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(47,'JOB_ID', '<REEMPLAZAR_JOB_ID_ML>', 'Job Databricks workflow ML Liga 1');
GO

-- ══════════════════════════════════════════════════════════════
-- ML SCORE: PREDECESORES
-- ft_score_ml (45) lee desde:
--   - ft_estadisticas_jugadores (44) → stats por jugador/temporada (DDV)
--   - ft_plantillas_historico (30)   → perfil jugador: pie, altura, posicion_xi (DDV)
-- ══════════════════════════════════════════════════════════════
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 44, @PipelineId_Destino = 45; -- ft_estadisticas_jugadores → ft_score_ml
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 30, @PipelineId_Destino = 45; -- ft_plantillas_historico   → ft_score_ml
GO

-- ══════════════════════════════════════════════════════════════
-- ML SCORE: JOB_ID PENDIENTE
-- Reemplazar con el valor real tras crear el Databricks workflow sch_ml_liga1
-- ══════════════════════════════════════════════════════════════
-- UPDATE dbo.tbl_pipeline_parametros
-- SET Valor = '<REEMPLAZAR_JOB_ID_ML>'
-- WHERE PipelineId = 47 AND Parametro = 'JOB_ID';
-- GO
