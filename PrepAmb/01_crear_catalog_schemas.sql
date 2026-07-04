-- ============================================================
-- PREP AMBIENTE — Paso 1: Catalog, Schemas y Volume
-- Ejecutar en Databricks SQL con permisos de metastore admin
-- ============================================================

-- Catálogo principal
CREATE CATALOG IF NOT EXISTS catalog_liga1
  COMMENT 'Catálogo principal Liga 1 Perú Data Platform';

-- Schemas UDV
CREATE SCHEMA IF NOT EXISTS catalog_liga1.tb_udv
  COMMENT 'Unified Data Vault — tablas Delta integradas';
CREATE SCHEMA IF NOT EXISTS catalog_liga1.vw_udv
  COMMENT 'Vistas UDV expuestas';

-- Schemas DDV
CREATE SCHEMA IF NOT EXISTS catalog_liga1.tb_ddv
  COMMENT 'Delivery Data Vault — Data Marts Delta para BI';
CREATE SCHEMA IF NOT EXISTS catalog_liga1.vw_ddv
  COMMENT 'Vistas DDV expuestas a Power BI y Delta Sharing';

-- Schema shared (para librería wheel)
CREATE SCHEMA IF NOT EXISTS catalog_liga1.shared
  COMMENT 'Recursos compartidos: librerías, utilidades';

-- Volume para wheel liga1_utils
CREATE VOLUME IF NOT EXISTS catalog_liga1.shared.libs
  COMMENT 'Librería liga1_utils wheel para instalación en clusters';
