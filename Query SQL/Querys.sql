-- ══════════════════════════════════════════════════════════════
-- PASO 1: DROP Y RECREAR EN ORDEN CORRECTO
-- ══════════════════════════════════════════════════════════════
DROP TABLE IF EXISTS dbo.tbl_predecesores;
DROP TABLE IF EXISTS dbo.tbl_control_ejecucion;
DROP TABLE IF EXISTS dbo.tbl_pipeline_parametros;
DROP TABLE IF EXISTS dbo.tbl_archivos_liga1;
DROP TABLE IF EXISTS dbo.tbl_paths;
DROP TABLE IF EXISTS dbo.tbl_pipeline;
DROP TABLE IF EXISTS dbo.tbl_flag_update_queue_id;
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
-- ID 1
INSERT INTO tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('Raw_Liga1','Carga datos de Liga 1 Perú','RAW','/Proyecto/liga1',1,NULL);
-- ID 2
INSERT INTO tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('md_equipos','Carga datos de equipos Liga 1 Perú','UDV','/Proyecto/liga1',1,'udv/Proyecto/liga1/tb_udv/md_equipos/data/');
-- ID 3
INSERT INTO tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('catalogo_equipos','Carga archivo de equipos Liga 1 Perú','RAW','/Proyecto/liga1',1,'raw/Proyecto/liga1/catalogo_equipos/data/');
-- ID 4
INSERT INTO tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('md_catalogo_equipos','Carga catalogo de equipos Liga 1 Perú','UDV','/Proyecto/liga1',1,'udv/Proyecto/liga1/tb_udv/md_catalogo_equipos/data/');
-- ID 5
INSERT INTO tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('Job_ddl','Ejecucion de job ddl','-','-',1,'-');
-- ID 6
INSERT INTO tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('Create_jobs','Ejecucion de creacion de jobs','-','-',1,'-');
-- ID 7
INSERT INTO tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('md_estadios','Carga datos de estadios Liga 1 Perú','UDV','/Proyecto/liga1',1,'udv/Proyecto/liga1/tb_udv/md_estadios/data/');
-- ID 8
INSERT INTO tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('md_plantillas','Carga datos de catalogo de jugadores Liga 1 Perú','UDV','/Proyecto/liga1',1,'udv/Proyecto/liga1/tb_udv/md_plantillas/data/');
-- ID 9
INSERT INTO tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta, parent_pipelineid)
VALUES ('hm_plantillas_equipo','Carga datos de jugadores historico equipos Liga 1 Perú','UDV','/Proyecto/liga1',1,'udv/Proyecto/liga1/tb_udv/hm_plantillas_equipo/data/',8);
-- ID 10
INSERT INTO tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('plantillas','Carga archivo de plantillas Liga 1 Perú','RAW','/Proyecto/liga1',1,'raw/Proyecto/liga1/plantillas/');
-- ID 11
INSERT INTO tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('md_entrenadores','Carga datos de catalogo de entrenadores Liga 1 Perú','UDV','/Proyecto/liga1',1,'udv/Proyecto/liga1/tb_udv/md_entrenadores/data/');
-- ID 12 ← hm_entrenadores_equipo (FALTABA en tu tabla anterior)
INSERT INTO tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta, parent_pipelineid)
VALUES ('hm_entrenadores_equipo','Carga datos de entrenadores historico equipos Liga 1 Perú','UDV','/Proyecto/liga1',1,'udv/Proyecto/liga1/tb_udv/hm_entrenadores_equipo/data/',11);
-- ID 13 ← entrenadores RAW (FALTABA en tu tabla anterior)
INSERT INTO tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('entrenadores','Carga archivo de entrenadores Liga 1 Perú','RAW','/Proyecto/liga1',1,'raw/Proyecto/liga1/entrenadores/');
-- ID 14
INSERT INTO tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('hm_valoracion_equipos','Carga datos de valoracion clubes historico Liga 1 Perú','UDV','/Proyecto/liga1',1,'udv/Proyecto/liga1/tb_udv/hm_valoracion_equipos/data/');
-- ID 15
INSERT INTO tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('liga1','Carga archivo de valoracion equipos Liga 1 Perú','RAW','/Proyecto/liga1',1,'raw/Proyecto/liga1/liga1/');
-- ID 16
INSERT INTO tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('hm_partidos','Carga datos hm_partidos historico Liga 1 Perú','UDV','/Proyecto/liga1',1,'udv/Proyecto/liga1/tb_udv/hm_partidos/data/');
-- ID 17
INSERT INTO tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('partidos','Carga archivo de partidos Liga 1 Perú','RAW','/Proyecto/liga1',1,'raw/Proyecto/liga1/partidos/');
-- ID 18
INSERT INTO tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('hm_estadisticas_partidos','Carga datos de estadisticas partidos historico Liga 1 Perú','UDV','/Proyecto/liga1',1,'udv/Proyecto/liga1/tb_udv/hm_estadisticas_partidos/data/');
-- ID 19
INSERT INTO tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('estadisticas_partidos','Carga archivo de estadisticas partidos Liga 1 Perú','RAW','/Proyecto/liga1',1,'raw/Proyecto/liga1/estadisticas_partidos/');
-- ID 20
INSERT INTO tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('hm_tablas_clasificacion','Carga datos de tablas clasificacion historico Liga 1 Perú','UDV','/Proyecto/liga1',1,'udv/Proyecto/liga1/tb_udv/hm_tablas_clasificacion/data/');
-- ID 21
INSERT INTO tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('tablas_clasificacion','Carga archivo de tablas clasificacion Liga 1 Perú','RAW','/Proyecto/liga1',1,'raw/Proyecto/liga1/tablas_clasificacion/');
-- ID 22
INSERT INTO tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('hm_campeones','Carga datos de campeones Liga 1 Perú','UDV','/Proyecto/liga1',1,'udv/Proyecto/liga1/tb_udv/hm_campeones/data/');
-- ID 23
INSERT INTO tbl_pipeline (Nombre, Descripcion, Nivel, RutaBase, Activo, Ruta)
VALUES ('campeones','Carga archivo de campeones Liga 1 Perú','RAW','/Proyecto/liga1',1,'raw/Proyecto/liga1/campeones/');
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
    IdEjecucion    INT IDENTITY(1,1) PRIMARY KEY,
    PipelineId     INT NOT NULL,
    pipeline_name  VARCHAR(200) NOT NULL,
    pipeline_type  VARCHAR(200) NOT NULL,
    execution_id   UNIQUEIDENTIFIER NOT NULL,
    status         VARCHAR(20) NOT NULL,
    start_time     DATETIME NULL,
    end_time       DATETIME NULL,
    mensaje_error  VARCHAR(500) NULL,
    FOREIGN KEY (PipelineId) REFERENCES tbl_pipeline(PipelineId)
);

CREATE TABLE dbo.tbl_archivos_liga1 (
    Id     INT IDENTITY(1,1) PRIMARY KEY,
    Nombre NVARCHAR(100),
    Tipo   NVARCHAR(10)
);

INSERT INTO tbl_archivos_liga1 (Nombre, Tipo) VALUES
('campeones',              'json'),
('equipos',                'json'),
('estadisticas_partidos',  'json'),
('partidos',               'json'),
('tablas_clasificacion',   'json'),
('entrenadores',           'csv'),
('estadios',               'csv'),
('liga1',                  'csv'),
('plantillas',             'csv');

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
-- PASO 4: PARÁMETROS CORREGIDOS
-- ══════════════════════════════════════════════════════════════

-- ID 1: Raw_Liga1 (orquestador RAW principal)
INSERT INTO tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(1,'URL_BASE_FOTMOB',         'https://www.fotmob.com/es',                        'URL base FotMob'),
(1,'LEAGUE_ID:FOTMOB',        '131',                                               'ID Liga 1 en FotMob'),
(1,'URL_SEASON_PARAM_FOTMOB', 'season',                                            'Filtro temporada'),
(1,'FILESYSTEM',              'liga1',                                             'Contenedor ADLS'),
(1,'CAPA_LANDING',            'landing',                                           'Contenedor landing'),
(1,'RUTABASE',                '/fotmob/liga1',                                     'Ruta base landing'),
(1,'CAPA_RAW',                'raw',                                               'Contenedor raw'),
(1,'MODO_EJECUCION',          'HISTORICO',                                         'HISTORICO/INCREMENTAL/REPROCESO'),
(1,'CURRENT_YEAR',            CAST(YEAR(GETDATE()) AS VARCHAR(4)),                 'Año actual'),
(1,'HISTORIC_START_YEAR',     '2020',                                              'Año inicio histórico'),
(1,'FLG_REPROCESO',           '0',                                                 '0=Normal, 1=Reproceso'),
(1,'ANIO_REPROCESO',          NULL,                                                'Año de reproceso'),
(1,'SPARK_VERSION',           '15.4.x-scala2.12',                                  'Versión Spark'),
(1,'NODE_TYPE',               'Standard_D4pds_v6',                                 'Tipo de nodo'),
(1,'NUM_WORKERS',             '0',                                                  'Workers (0=single node)'),
(1,'AUTOTERMINATION',         '15',                                                 'Minutos auto-terminación'),
(1,'CLUSTER_ID',              '0919-172542-yodzh4ch',                              'ID cluster Databricks'),
(1,'AREA',                    'https://adb-1676851685217423.3.azuredatabricks.net/','Workspace Databricks');

-- ID 2: md_equipos (UDV)
INSERT INTO tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(2,'FILESYSTEM',    'liga1',                                              'Contenedor ADLS'),
(2,'CAPA_UDV',      'udv',                                               'Capa UDV'),
(2,'RUTA_BASE',     '/Proyecto/liga1',                                   'Ruta raíz en ADLS'),
(2,'FORMATO_SALIDA','delta',                                              'Formato Delta'),
(2,'TIPO_CARGA',    'FULL',                                              'Carga completa'),
(2,'SCHEMA_TABLA',  'tb_udv',                                            'Esquema tablas Delta'),
(2,'SCHEMA_VISTA',  'vw_udv',                                            'Esquema vistas'),
(2,'RUTA_TABLA',    '/tb_udv/md_equipos',                                'Ruta tabla Delta'),
(2,'NOMBRE_TABLA',  'md_equipos',                                        'Nombre tabla UDV'),
(2,'YAML_PATH',     '/frm_udv/conf/md_equipos/md_equipos.yml',           'Ruta YAML');

-- ID 3: catalogo_equipos (RAW DataEntry)
INSERT INTO tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(3,'FILESYSTEM',    'liga1',                                              'Contenedor ADLS'),
(3,'CAPA_LANDING',  'landing',                                           'Contenedor landing'),
(3,'CAPA_RAW',      'raw',                                               'Contenedor raw'),
(3,'RUTA_BASE',     '/Proyecto/liga1',                                   'Ruta raíz'),
(3,'FORMATO_SALIDA','parquet',                                            'Formato salida'),
(3,'TIPO_CARGA',    'FULL',                                              'Tipo carga'),
(3,'CLUSTER_ID',    '1104-003006-pihx9xuw',                              'ID cluster'),
(3,'AREA',          'https://adb-1676851685217423.3.azuredatabricks.net/','Workspace'),
(3,'NOMBRE_ARCHIVO','catalogo_equipos',                                   'Nombre entidad'),
(3,'JOB_ID',        '612739497183835',                                   'Job Databricks');

-- ID 4: md_catalogo_equipos (UDV)
INSERT INTO tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(4,'FILESYSTEM',    'liga1',                                              'Contenedor ADLS'),
(4,'SCHEMA_TABLA',  'tb_udv',                                            'Esquema tablas Delta'),
(4,'SCHEMA_VISTA',  'vw_udv',                                            'Esquema vistas'),
(4,'CAPA_UDV',      'udv',                                               'Capa UDV'),
(4,'RUTA_BASE',     '/Proyecto/liga1',                                   'Ruta raíz'),
(4,'RUTA_TABLA',    '/tb_udv/md_catalogo_equipos',                       'Ruta tabla Delta'),
(4,'FORMATO_SALIDA','delta',                                              'Formato Delta'),
(4,'TIPO_CARGA',    'FULL',                                              'Carga completa'),
(4,'NOMBRE_TABLA',  'md_catalogo_equipos',                               'Nombre tabla UDV'),
(4,'YAML_PATH',     '/frm_udv/conf/md_catalogo_equipos/md_catalogo_equipos.yml','Ruta YAML');

-- ID 5: Job_ddl
INSERT INTO tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(5,'AREA',   'https://adb-1676851685217423.3.azuredatabricks.net/','Workspace'),
(5,'JOB_ID', '612739497183835',                                   'Job DDL Databricks');

-- ID 6: Create_jobs
INSERT INTO tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(6,'AREA',       'https://adb-1676851685217423.3.azuredatabricks.net/','Workspace'),
(6,'CLUSTER_ID', '1104-003006-pihx9xuw',                              'ID cluster'),
(6,'JOB_ID',     '57622873974351',                                     'Job create workflows');

-- ID 7: md_estadios (UDV)
INSERT INTO tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(7,'SCHEMA_TABLA',  'tb_udv',                                            'Esquema tablas Delta'),
(7,'CAPA_UDV',      'udv',                                               'Capa UDV'),
(7,'RUTA_BASE',     '/Proyecto/liga1',                                   'Ruta raíz'),
(7,'RUTA_TABLA',    '/tb_udv/md_estadios',                               'Ruta tabla Delta'),
(7,'FORMATO_SALIDA','delta',                                              'Formato Delta'),
(7,'TIPO_CARGA',    'FULL',                                              'Carga completa'),
(7,'NOMBRE_TABLA',  'md_estadios',                                       'Nombre tabla UDV'),
(7,'YAML_PATH',     '/frm_udv/conf/md_estadios/md_estadios.yml',         'Ruta YAML');

-- ID 8: md_plantillas (UDV) — RUTA_TABLA CORREGIDA
INSERT INTO tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(8,'SCHEMA_TABLA',  'tb_udv',                                                      'Esquema tablas Delta'),
(8,'CAPA_UDV',      'udv',                                                         'Capa UDV'),
(8,'RUTA_BASE',     '/Proyecto/liga1',                                             'Ruta raíz'),
(8,'RUTA_TABLA',    '/tb_udv/md_plantillas',                                       'Ruta tabla Delta'),
(8,'FORMATO_SALIDA','delta',                                                        'Formato Delta'),
(8,'TIPO_CARGA',    'FULL',                                                        'Carga completa'),
(8,'NOMBRE_TABLA',  'md_plantillas',                                               'Nombre tabla UDV'),
(8,'YAML_PATH',     '/frm_udv/conf/hm_plantillas_equipo/hm_plantillas_equipo.yml', 'Ruta YAML');

-- ID 9: hm_plantillas_equipo (UDV)
INSERT INTO tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(9,'SCHEMA_TABLA',  'tb_udv',                                                      'Esquema tablas Delta'),
(9,'CAPA_UDV',      'udv',                                                         'Capa UDV'),
(9,'RUTA_BASE',     '/Proyecto/liga1',                                             'Ruta raíz'),
(9,'RUTA_TABLA',    '/tb_udv/hm_plantillas_equipo',                                'Ruta tabla Delta'),
(9,'FORMATO_SALIDA','delta',                                                        'Formato Delta'),
(9,'TIPO_CARGA',    'INCREMENTAL',                                                  'Tipo carga'),
(9,'NOMBRE_TABLA',  'hm_plantillas_equipo',                                         'Nombre tabla UDV'),
(9,'YAML_PATH',     '/frm_udv/conf/hm_plantillas_equipo/hm_plantillas_equipo.yml', 'Ruta YAML');

-- ID 10: plantillas (RAW) — sin CLUSTER_ID, ADF lo maneja vía linked service
INSERT INTO tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(10,'FILESYSTEM',    'liga1',           'Contenedor ADLS'),
(10,'CAPA_LANDING',  'landing',         'Contenedor landing'),
(10,'CAPA_RAW',      'raw',             'Contenedor raw'),
(10,'RUTA_BASE',     '/Proyecto/liga1', 'Ruta raíz'),
(10,'FORMATO_SALIDA','parquet',         'Formato salida'),
(10,'TIPO_CARGA',    'INCREMENTAL',     'Tipo carga'),
(10,'NOMBRE_ARCHIVO','plantillas',      'Nombre entidad');

-- ID 11: md_entrenadores (UDV) — YAML_PATH AÑADIDO
INSERT INTO tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(11,'SCHEMA_TABLA',  'tb_udv',                                                          'Esquema tablas Delta'),
(11,'CAPA_UDV',      'udv',                                                             'Capa UDV'),
(11,'RUTA_BASE',     '/Proyecto/liga1',                                                 'Ruta raíz'),
(11,'RUTA_TABLA',    '/tb_udv/md_entrenadores',                                         'Ruta tabla Delta'),
(11,'FORMATO_SALIDA','delta',                                                            'Formato Delta'),
(11,'TIPO_CARGA',    'FULL',                                                            'Carga completa'),
(11,'NOMBRE_TABLA',  'md_entrenadores',                                                  'Nombre tabla UDV'),
(11,'YAML_PATH',     '/frm_udv/conf/hm_entrenadores_equipo/hm_entrenadores_equipo.yml', 'Ruta YAML');

-- ID 12: hm_entrenadores_equipo (UDV) — NUEVO
INSERT INTO tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(12,'SCHEMA_TABLA',  'tb_udv',                                                          'Esquema tablas Delta'),
(12,'CAPA_UDV',      'udv',                                                             'Capa UDV'),
(12,'RUTA_BASE',     '/Proyecto/liga1',                                                 'Ruta raíz'),
(12,'RUTA_TABLA',    '/tb_udv/hm_entrenadores_equipo',                                  'Ruta tabla Delta'),
(12,'FORMATO_SALIDA','delta',                                                            'Formato Delta'),
(12,'TIPO_CARGA',    'INCREMENTAL',                                                      'Tipo carga'),
(12,'NOMBRE_TABLA',  'hm_entrenadores_equipo',                                           'Nombre tabla UDV'),
(12,'YAML_PATH',     '/frm_udv/conf/hm_entrenadores_equipo/hm_entrenadores_equipo.yml', 'Ruta YAML');

-- ID 13: entrenadores (RAW) — NUEVO
INSERT INTO tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(13,'FILESYSTEM',    'liga1',           'Contenedor ADLS'),
(13,'CAPA_LANDING',  'landing',         'Contenedor landing'),
(13,'CAPA_RAW',      'raw',             'Contenedor raw'),
(13,'RUTA_BASE',     '/Proyecto/liga1', 'Ruta raíz'),
(13,'FORMATO_SALIDA','parquet',         'Formato salida'),
(13,'TIPO_CARGA',    'INCREMENTAL',     'Tipo carga'),
(13,'NOMBRE_ARCHIVO','entrenadores',    'Nombre entidad');

-- ID 14: hm_valoracion_equipos (UDV)
INSERT INTO tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(14,'SCHEMA_TABLA',  'tb_udv',                                                       'Esquema tablas Delta'),
(14,'CAPA_UDV',      'udv',                                                          'Capa UDV'),
(14,'RUTA_BASE',     '/Proyecto/liga1',                                              'Ruta raíz'),
(14,'RUTA_TABLA',    '/tb_udv/hm_valoracion_equipos',                               'Ruta tabla Delta'),
(14,'FORMATO_SALIDA','delta',                                                         'Formato Delta'),
(14,'TIPO_CARGA',    'INCREMENTAL',                                                   'Tipo carga'),
(14,'NOMBRE_TABLA',  'hm_valoracion_equipos',                                         'Nombre tabla UDV'),
(14,'YAML_PATH',     '/frm_udv/conf/hm_valoracion_equipos/hm_valoracion_equipos.yml','Ruta YAML');

-- ID 15: liga1 (RAW)
INSERT INTO tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(15,'FILESYSTEM',    'liga1',           'Contenedor ADLS'),
(15,'CAPA_LANDING',  'landing',         'Contenedor landing'),
(15,'CAPA_RAW',      'raw',             'Contenedor raw'),
(15,'RUTA_BASE',     '/Proyecto/liga1', 'Ruta raíz'),
(15,'FORMATO_SALIDA','parquet',         'Formato salida'),
(15,'TIPO_CARGA',    'INCREMENTAL',     'Tipo carga'),
(15,'NOMBRE_ARCHIVO','liga1',           'Nombre entidad');

-- ID 16: hm_partidos (UDV)
INSERT INTO tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(16,'SCHEMA_TABLA',  'tb_udv',                                         'Esquema tablas Delta'),
(16,'CAPA_UDV',      'udv',                                            'Capa UDV'),
(16,'RUTA_BASE',     '/Proyecto/liga1',                                'Ruta raíz'),
(16,'RUTA_TABLA',    '/tb_udv/hm_partidos',                            'Ruta tabla Delta'),
(16,'FORMATO_SALIDA','delta',                                           'Formato Delta'),
(16,'TIPO_CARGA',    'INCREMENTAL',                                     'Tipo carga'),
(16,'NOMBRE_TABLA',  'hm_partidos',                                     'Nombre tabla UDV'),
(16,'YAML_PATH',     '/frm_udv/conf/hm_partidos/hm_partidos.yml',      'Ruta YAML');

-- ID 17: partidos (RAW)
INSERT INTO tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(17,'FILESYSTEM',    'liga1',           'Contenedor ADLS'),
(17,'CAPA_LANDING',  'landing',         'Contenedor landing'),
(17,'CAPA_RAW',      'raw',             'Contenedor raw'),
(17,'RUTA_BASE',     '/Proyecto/liga1', 'Ruta raíz'),
(17,'FORMATO_SALIDA','parquet',         'Formato salida'),
(17,'TIPO_CARGA',    'INCREMENTAL',     'Tipo carga'),
(17,'NOMBRE_ARCHIVO','partidos',        'Nombre entidad');

-- ID 18: hm_estadisticas_partidos (UDV)
INSERT INTO tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(18,'SCHEMA_TABLA',  'tb_udv',                                                                     'Esquema tablas Delta'),
(18,'CAPA_UDV',      'udv',                                                                        'Capa UDV'),
(18,'RUTA_BASE',     '/Proyecto/liga1',                                                            'Ruta raíz'),
(18,'RUTA_TABLA',    '/tb_udv/hm_estadisticas_partidos',                                           'Ruta tabla Delta'),
(18,'FORMATO_SALIDA','delta',                                                                       'Formato Delta'),
(18,'TIPO_CARGA',    'INCREMENTAL',                                                                 'Tipo carga'),
(18,'NOMBRE_TABLA',  'hm_estadisticas_partidos',                                                    'Nombre tabla UDV'),
(18,'YAML_PATH',     '/frm_udv/conf/hm_estadisticas_partidos/hm_estadisticas_partidos.yml',        'Ruta YAML');

-- ID 19: estadisticas_partidos (RAW)
INSERT INTO tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(19,'FILESYSTEM',    'liga1',                   'Contenedor ADLS'),
(19,'CAPA_LANDING',  'landing',                 'Contenedor landing'),
(19,'CAPA_RAW',      'raw',                     'Contenedor raw'),
(19,'RUTA_BASE',     '/Proyecto/liga1',         'Ruta raíz'),
(19,'FORMATO_SALIDA','parquet',                 'Formato salida'),
(19,'TIPO_CARGA',    'INCREMENTAL',             'Tipo carga'),
(19,'NOMBRE_ARCHIVO','estadisticas_partidos',   'Nombre entidad');

-- ID 20: hm_tablas_clasificacion (UDV)
INSERT INTO tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(20,'SCHEMA_TABLA',  'tb_udv',                                                                  'Esquema tablas Delta'),
(20,'CAPA_UDV',      'udv',                                                                     'Capa UDV'),
(20,'RUTA_BASE',     '/Proyecto/liga1',                                                         'Ruta raíz'),
(20,'RUTA_TABLA',    '/tb_udv/hm_tablas_clasificacion',                                         'Ruta tabla Delta'),
(20,'FORMATO_SALIDA','delta',                                                                    'Formato Delta'),
(20,'TIPO_CARGA',    'INCREMENTAL',                                                              'Tipo carga'),
(20,'NOMBRE_TABLA',  'hm_tablas_clasificacion',                                                  'Nombre tabla UDV'),
(20,'YAML_PATH',     '/frm_udv/conf/hm_tablas_clasificacion/hm_tablas_clasificacion.yml',       'Ruta YAML');

-- ID 21: tablas_clasificacion (RAW)
INSERT INTO tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(21,'FILESYSTEM',    'liga1',                'Contenedor ADLS'),
(21,'CAPA_LANDING',  'landing',              'Contenedor landing'),
(21,'CAPA_RAW',      'raw',                  'Contenedor raw'),
(21,'RUTA_BASE',     '/Proyecto/liga1',      'Ruta raíz'),
(21,'FORMATO_SALIDA','parquet',              'Formato salida'),
(21,'TIPO_CARGA',    'INCREMENTAL',          'Tipo carga'),
(21,'NOMBRE_ARCHIVO','tablas_clasificacion', 'Nombre entidad');

-- ID 22: hm_campeones (UDV)
INSERT INTO tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(22,'SCHEMA_TABLA',  'tb_udv',                                               'Esquema tablas Delta'),
(22,'CAPA_UDV',      'udv',                                                  'Capa UDV'),
(22,'RUTA_BASE',     '/Proyecto/liga1',                                      'Ruta raíz'),
(22,'RUTA_TABLA',    '/tb_udv/hm_campeones',                                 'Ruta tabla Delta'),
(22,'FORMATO_SALIDA','delta',                                                  'Formato Delta'),
(22,'TIPO_CARGA',    'INCREMENTAL',                                            'Tipo carga'),
(22,'NOMBRE_TABLA',  'hm_campeones',                                           'Nombre tabla UDV'),
(22,'YAML_PATH',     '/frm_udv/conf/hm_campeones/hm_campeones.yml',           'Ruta YAML');

-- ID 23: campeones (RAW)
INSERT INTO tbl_pipeline_parametros (PipelineId, Parametro, Valor, Descripcion) VALUES
(23,'FILESYSTEM',    'liga1',       'Contenedor ADLS'),
(23,'CAPA_LANDING',  'landing',     'Contenedor landing'),
(23,'CAPA_RAW',      'raw',         'Contenedor raw'),
(23,'RUTA_BASE',     '/Proyecto/liga1','Ruta raíz'),
(23,'FORMATO_SALIDA','parquet',     'Formato salida'),
(23,'TIPO_CARGA',    'INCREMENTAL', 'Tipo carga'),
(23,'NOMBRE_ARCHIVO','campeones',   'Nombre entidad');
GO

-- ══════════════════════════════════════════════════════════════
-- PASO 5: PREDECESORES CON IDs CORRECTOS
-- ══════════════════════════════════════════════════════════════
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 3,  @PipelineId_Destino = 4;  -- catalogo RAW → md_catalogo
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 4,  @PipelineId_Destino = 2;  -- md_catalogo → md_equipos
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 4,  @PipelineId_Destino = 7;  -- md_catalogo → md_estadios
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 10, @PipelineId_Destino = 8;  -- plantillas RAW → md_plantillas
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 8,  @PipelineId_Destino = 9;  -- md_plantillas → hm_plantillas_equipo
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 4,  @PipelineId_Destino = 9;  -- md_catalogo → hm_plantillas_equipo
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 13, @PipelineId_Destino = 11; -- entrenadores RAW → md_entrenadores
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 11, @PipelineId_Destino = 12; -- md_entrenadores → hm_entrenadores_equipo
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 4,  @PipelineId_Destino = 12; -- md_catalogo → hm_entrenadores_equipo
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 15, @PipelineId_Destino = 14; -- liga1 RAW → hm_valoracion_equipos
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 4,  @PipelineId_Destino = 14; -- md_catalogo → hm_valoracion_equipos
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 17, @PipelineId_Destino = 16; -- partidos RAW → hm_partidos
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 4,  @PipelineId_Destino = 16; -- md_catalogo → hm_partidos
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 19, @PipelineId_Destino = 18; -- estadisticas RAW → hm_estadisticas
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 16, @PipelineId_Destino = 18; -- hm_partidos → hm_estadisticas
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 4,  @PipelineId_Destino = 18; -- md_catalogo → hm_estadisticas
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 21, @PipelineId_Destino = 20; -- tablas_clas RAW → hm_tablas_clasificacion
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 4,  @PipelineId_Destino = 20; -- md_catalogo → hm_tablas_clasificacion
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 23, @PipelineId_Destino = 22; -- campeones RAW → hm_campeones
EXEC dbo.sp_registrar_predecesor @PipelineId_Predecesor = 4,  @PipelineId_Destino = 22; -- md_catalogo → hm_campeones
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