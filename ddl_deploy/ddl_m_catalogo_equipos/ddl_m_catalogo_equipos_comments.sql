-- Usa catálogo y esquema correctos
USE CATALOG adbliga1futbol;
USE SCHEMA tb_udv;

-- Comentario de la tabla
COMMENT ON TABLE m_catalogo_equipos IS
  'Catálogo maestro de equipos de la Liga 1 Perú';

-- Comentarios de columnas (idénticos a tu YAML)
COMMENT ON COLUMN m_catalogo_equipos.id_equipo IS
  'Identificador único del equipo';

COMMENT ON COLUMN m_catalogo_equipos.nombre_equipo IS
  'Nombre oficial del equipo';

COMMENT ON COLUMN m_catalogo_equipos.nombre_fotmob IS
  'Nombre del equipo según FotMob';

COMMENT ON COLUMN m_catalogo_equipos.nombre_transfermarkt IS
  'Nombre del equipo según Transfermarkt';

COMMENT ON COLUMN m_catalogo_equipos.alias IS
  'Alias o abreviatura usada en prensa';

COMMENT ON COLUMN m_catalogo_equipos.fecha_carga IS
  'Fecha y hora en que se cargó el registro';

COMMENT ON COLUMN m_catalogo_equipos.periododia IS
  'Periodo de carga en formato YYYYMMDD';
