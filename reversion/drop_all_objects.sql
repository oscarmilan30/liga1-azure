-- ============================================================
-- REVERSION — Liga 1 Perú Data Engineering Platform
-- Elimina todas las tablas Delta, vistas y schemas del catálogo
-- ADVERTENCIA: Ejecutar solo en caso de rollback completo
-- Orden: vistas primero, luego tablas, luego schemas, catálogo
-- ============================================================

-- ── DDV: Vistas del archivo ddl_vista ────────────────────
DROP VIEW IF EXISTS catalog_liga1_prod.vw_ddv.ft_rendimiento_posiciones_vw;
DROP VIEW IF EXISTS catalog_liga1_prod.vw_ddv.ft_rendimiento_general_vw;
DROP VIEW IF EXISTS catalog_liga1_prod.vw_ddv.ft_estadisticas_jugadores_vw;
DROP VIEW IF EXISTS catalog_liga1_prod.vw_ddv.ft_partidos_equipo_vw;
DROP VIEW IF EXISTS catalog_liga1_prod.vw_ddv.vw_jugadores_slots;
DROP VIEW IF EXISTS catalog_liga1_prod.vw_ddv.dm_temporada_vw;
DROP VIEW IF EXISTS catalog_liga1_prod.vw_ddv.dm_tipo_tabla_vw;
DROP VIEW IF EXISTS catalog_liga1_prod.vw_ddv.dm_estilo_juego;
DROP VIEW IF EXISTS catalog_liga1_prod.vw_ddv.dm_formacion_slots;

-- ── DDV: Vistas por entidad ───────────────────────────────
DROP VIEW IF EXISTS catalog_liga1_prod.vw_ddv.ft_score_ml_vw;
DROP VIEW IF EXISTS catalog_liga1_prod.vw_ddv.ft_plantillas_historico_vw;
DROP VIEW IF EXISTS catalog_liga1_prod.vw_ddv.ft_partidos_detalle_vw;
DROP VIEW IF EXISTS catalog_liga1_prod.vw_ddv.ft_rendimiento_temporada_vw;
DROP VIEW IF EXISTS catalog_liga1_prod.vw_ddv.ft_rendimiento_acumulado_vw;
DROP VIEW IF EXISTS catalog_liga1_prod.vw_ddv.ft_evolucion_valoracion_vw;
DROP VIEW IF EXISTS catalog_liga1_prod.vw_ddv.ft_entrenadores_historico_vw;
DROP VIEW IF EXISTS catalog_liga1_prod.vw_ddv.dm_equipos_vw;
DROP VIEW IF EXISTS catalog_liga1_prod.vw_ddv.hm_campeones_vw;

-- ── DDV: Tablas Delta ─────────────────────────────────────
DROP TABLE IF EXISTS catalog_liga1_prod.tb_ddv.ft_score_ml;
DROP TABLE IF EXISTS catalog_liga1_prod.tb_ddv.ft_estadisticas_jugadores;
DROP TABLE IF EXISTS catalog_liga1_prod.tb_ddv.ft_plantillas_historico;
DROP TABLE IF EXISTS catalog_liga1_prod.tb_ddv.ft_partidos_detalle;
DROP TABLE IF EXISTS catalog_liga1_prod.tb_ddv.ft_rendimiento_temporada;
DROP TABLE IF EXISTS catalog_liga1_prod.tb_ddv.ft_rendimiento_acumulado;
DROP TABLE IF EXISTS catalog_liga1_prod.tb_ddv.ft_evolucion_valoracion;
DROP TABLE IF EXISTS catalog_liga1_prod.tb_ddv.ft_entrenadores_historico;
DROP TABLE IF EXISTS catalog_liga1_prod.tb_ddv.dm_equipos;

-- ── UDV: Tablas maestro e histórico ──────────────────────
DROP TABLE IF EXISTS catalog_liga1_prod.tb_udv.md_equipos;
DROP TABLE IF EXISTS catalog_liga1_prod.tb_udv.md_estadios;
DROP TABLE IF EXISTS catalog_liga1_prod.tb_udv.md_catalogo_equipos;
DROP TABLE IF EXISTS catalog_liga1_prod.tb_udv.hm_campeones;
DROP TABLE IF EXISTS catalog_liga1_prod.tb_udv.hm_partidos;
DROP TABLE IF EXISTS catalog_liga1_prod.tb_udv.hm_estadisticas_partidos;
DROP TABLE IF EXISTS catalog_liga1_prod.tb_udv.hm_estadisticas_jugadores;
DROP TABLE IF EXISTS catalog_liga1_prod.tb_udv.hm_plantillas_equipo;
DROP TABLE IF EXISTS catalog_liga1_prod.tb_udv.hm_entrenadores_equipo;
DROP TABLE IF EXISTS catalog_liga1_prod.tb_udv.hm_valoracion_equipos;
DROP TABLE IF EXISTS catalog_liga1_prod.tb_udv.hm_tablas_clasificacion;

-- ── Schemas ───────────────────────────────────────────────
DROP SCHEMA IF EXISTS catalog_liga1_prod.vw_ddv;
DROP SCHEMA IF EXISTS catalog_liga1_prod.tb_ddv;
DROP SCHEMA IF EXISTS catalog_liga1_prod.vw_udv;
DROP SCHEMA IF EXISTS catalog_liga1_prod.tb_udv;

-- ── Catálogo ──────────────────────────────────────────────
DROP CATALOG IF EXISTS catalog_liga1_prod;

-- ── Azure SQL — Plano de control (ejecutar en Azure SQL prod)
--DROP TABLE IF EXISTS dbo.tbl_predecesores;
--DROP TABLE IF EXISTS dbo.tbl_data_quality;
--DROP TABLE IF EXISTS dbo.tbl_control_ejecucion;
--DROP TABLE IF EXISTS dbo.tbl_pipeline_parametros;
--DROP TABLE IF EXISTS dbo.tbl_paths;
--DROP TABLE IF EXISTS dbo.tbl_flag_update_queue_id;
--DROP TABLE IF EXISTS dbo.tbl_archivos_liga1;
--DROP TABLE IF EXISTS dbo.tbl_pipeline;
--GO
