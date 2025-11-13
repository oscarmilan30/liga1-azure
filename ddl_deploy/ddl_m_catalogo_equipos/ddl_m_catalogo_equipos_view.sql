-- Databricks notebook source
-- ==========================================================
-- UDV - VW_M_CATALOGO_EQUIPOS
-- Proyecto: Liga 1 Perú
-- Autor: Oscar García Del Águila
-- ==========================================================

-- Seleccionar catálogo y schema
USE CATALOG adbliga1futbol;
USE SCHEMA vw_udv;

-- Crear o reemplazar vista
CREATE OR REPLACE VIEW m_catalogo_equipos_vw
AS
SELECT 
    id_equipo,
    nombre_equipo,
    nombre_fotmob,
    nombre_transfermarkt,
    alias,
    fecha_carga,
    periododia
FROM adbliga1futbol.tb_udv.m_catalogo_equipos;
