-- ============================================================
-- SEGURIDAD — Liga 1 Perú Data Engineering Platform
-- GRANTS sobre Unity Catalog para grupos y usuarios de lectura
-- Ejecutar en Databricks SQL con permisos de CATALOG OWNER
-- Schemas reales: tb_udv, vw_udv, tb_ddv, vw_ddv
-- ============================================================

-- ── Catálogo ──────────────────────────────────────────────
GRANT USE CATALOG ON CATALOG catalog_liga1 TO `data-readers`;
GRANT USE CATALOG ON CATALOG catalog_liga1 TO `data-analysts`;

-- ── DDV — Tablas (solo analistas internos) ────────────────
GRANT USE SCHEMA ON SCHEMA catalog_liga1.tb_ddv TO `data-analysts`;
GRANT SELECT    ON ALL TABLES IN SCHEMA catalog_liga1.tb_ddv TO `data-analysts`;

-- ── DDV — Vistas (lectura para Power BI y Delta Sharing) ──
GRANT USE SCHEMA ON SCHEMA catalog_liga1.vw_ddv TO `data-readers`;
GRANT SELECT    ON ALL TABLES IN SCHEMA catalog_liga1.vw_ddv TO `data-readers`;

-- ── UDV — Tablas (solo ingeniería) ────────────────────────
GRANT USE SCHEMA ON SCHEMA catalog_liga1.tb_udv TO `data-analysts`;
GRANT SELECT    ON ALL TABLES IN SCHEMA catalog_liga1.tb_udv TO `data-analysts`;

-- ── UDV — Vistas ──────────────────────────────────────────
GRANT USE SCHEMA ON SCHEMA catalog_liga1.vw_udv TO `data-analysts`;
GRANT SELECT    ON ALL TABLES IN SCHEMA catalog_liga1.vw_udv TO `data-analysts`;

-- ── Service Principal ADF (escritura en tb_udv y tb_ddv) ──
GRANT USE CATALOG ON CATALOG catalog_liga1                       TO `adf-sp-liga1`;
GRANT USE SCHEMA  ON SCHEMA catalog_liga1.tb_udv                 TO `adf-sp-liga1`;
GRANT USE SCHEMA  ON SCHEMA catalog_liga1.tb_ddv                 TO `adf-sp-liga1`;
GRANT MODIFY      ON ALL TABLES IN SCHEMA catalog_liga1.tb_udv   TO `adf-sp-liga1`;
GRANT MODIFY      ON ALL TABLES IN SCHEMA catalog_liga1.tb_ddv   TO `adf-sp-liga1`;

-- ── Delta Sharing — vista expuesta a Power BI Scouting ML ─
-- GRANT SELECT ON TABLE catalog_liga1.vw_ddv.ft_score_ml_vw TO SHARE liga1_scouting;
