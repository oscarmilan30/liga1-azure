# Databricks notebook source
# ==========================================================
# DELTA SHARING - SETUP LIGA 1 PERU
# Proyecto: Liga 1 Peru
# Autor: Oscar Garcia Del Aguila
# Descripcion: Crea el share, agrega ft_score_ml_vw (fact) y
#              las vistas de dimension necesarias para el .pbix
#              de scouting ML, crea el recipient y genera el
#              activation link para conectar Power BI.
#
# Vistas en el share:
#   - ft_score_ml_vw  : fact (score ML + coordenadas campo)
#   - dm_formacion    : slicer alineacion
#   - dm_estilo_juego : slicer estilo de juego
#   - dm_temporada_vw : slicer temporada
#   - dm_equipos_vw   : selector de equipos (XI Ideal)
# ==========================================================

from utils_liga1 import get_dbutils
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
dbutils = get_dbutils()

dbutils.widgets.text("catalog_name", "catalog_liga1")
catalog_name = dbutils.widgets.get("catalog_name")

print(f"Catalogo activo: {catalog_name}")

# COMMAND ----------

# ----------------------------------------------------------
# 1. Crear el share
# ----------------------------------------------------------
spark.sql("""
    CREATE SHARE IF NOT EXISTS liga1_share
    COMMENT 'Share Liga 1 Peru - Score ML jugadores'
""")
print("[OK] Share liga1_share creado (o ya existia)")

# COMMAND ----------
# ----------------------------------------------------------
# 2. Agregar vistas al share (idempotente — salta si ya existe)
# ----------------------------------------------------------
def add_view_to_share(share, view_fqn, comment):
    try:
        spark.sql(f"""
            ALTER SHARE {share}
            ADD VIEW {view_fqn}
            COMMENT '{comment}'
        """)
        print(f"[OK]   {view_fqn} agregada al share")
    except Exception as e:
        if "RESOURCE_ALREADY_EXISTS" in str(e):
            print(f"[SKIP] {view_fqn} ya existia en el share")
        else:
            raise

add_view_to_share("liga1_share", "catalog_liga1.vw_ddv.ft_score_ml_vw",  "Fact ML")
add_view_to_share("liga1_share", "catalog_liga1.vw_ddv.dm_formacion",     "Dimension formaciones")
add_view_to_share("liga1_share", "catalog_liga1.vw_ddv.dm_estilo_juego",  "Dimension estilos")
add_view_to_share("liga1_share", "catalog_liga1.vw_ddv.dm_temporada_vw",  "Dimension temporadas")
add_view_to_share("liga1_share", "catalog_liga1.vw_ddv.dm_equipos_vw",    "Dimension equipos")

# COMMAND ----------

# ----------------------------------------------------------
# 3. Crear el recipient (Power BI)
# ----------------------------------------------------------
spark.sql("""
    CREATE RECIPIENT IF NOT EXISTS liga1_powerbi
    COMMENT 'Acceso Power BI externo a datos Liga 1 Peru'
""")
print("[OK] Recipient liga1_powerbi creado (o ya existia)")

# COMMAND ----------

# ----------------------------------------------------------
# 4. Dar acceso al share
# ----------------------------------------------------------
spark.sql("""
    GRANT SELECT ON SHARE liga1_share TO RECIPIENT liga1_powerbi
""")
print("[OK] Grant SELECT otorgado a liga1_powerbi sobre liga1_share")

# COMMAND ----------

# ----------------------------------------------------------
# 5. Mostrar el activation link
#    Copia la columna activation_link — la pegas en Power BI
# ----------------------------------------------------------
print("Activation link para Power BI:")
spark.sql("DESCRIBE RECIPIENT liga1_powerbi").display()

# COMMAND ----------

# ----------------------------------------------------------
# Verificacion: listar shares y recipients creados
# ----------------------------------------------------------
print("Shares existentes:")
spark.sql("SHOW SHARES").show(truncate=False)

print("Recipients existentes:")
spark.sql("SHOW RECIPIENTS").show(truncate=False)
