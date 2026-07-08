# Databricks notebook source
# ==========================================================
# VALIDACIÓN DE DATOS — Liga 1
# Ejecutar después de cada run E2E para validar integridad
# ==========================================================

from utils_liga1 import log
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, when, isnan

spark = SparkSession.builder.getOrCreate()
# setup_adls()  # reemplazado por Access Connector (Managed Identity acc-liga1)

resultados = []

def validar_tabla(schema, tabla, pk_col, temporal_col=None):
    full_name = f"{schema}.{tabla}"
    try:
        df = spark.table(full_name)
        total = df.count()
        nulos_pk = df.filter(col(pk_col).isNull()).count()
        duplicados_pk = total - df.select(pk_col).distinct().count()
        
        result = {
            "tabla": full_name,
            "total": total,
            "nulos_pk": nulos_pk,
            "duplicados_pk": duplicados_pk,
            "status": "OK" if total > 0 and nulos_pk == 0 and duplicados_pk == 0 else "WARN"
        }
        
        if temporal_col and total > 0:
            dist_temporal = df.groupBy(temporal_col).count().orderBy(temporal_col)
            result["dist_temporal"] = dist_temporal
        
        return result
    except Exception as e:
        return {"tabla": full_name, "total": -1, "nulos_pk": -1, "duplicados_pk": -1, "status": f"ERROR: {e}"}

# === UDV ===
tablas_udv = [
    ("tb_udv", "md_equipos",              "id_equipo",            None),
    ("tb_udv", "md_estadios",             "id_estadio",           None),
    ("tb_udv", "md_entrenadores",         "id_entrenador",        None),
    ("tb_udv", "md_plantillas",           "id_jugador",           None),
    ("tb_udv", "md_catalogo_equipos",     "id_equipo",            None),
    ("tb_udv", "hm_plantillas_equipo",    "id_plantilla_equipo",  "temporada"),
    ("tb_udv", "hm_entrenadores_equipo",  "id_entrenador_equipo", "temporada"),
    ("tb_udv", "hm_campeones",            "id_campeonato",        "temporada"),
    ("tb_udv", "hm_estadisticas_partidos","id_partido",           "temporada"),
    ("tb_udv", "hm_partidos",             "id_partido",           "temporada"),
    ("tb_udv", "hm_tablas_clasificacion", "id_clasificacion",     "temporada"),
    ("tb_udv", "hm_estadisticas_jugadores", "id_estadistica_jugador","temporada"),
]

tablas_ddv = [
    ("tb_ddv", "dm_equipos",               "id_equipo",            None),
    ("tb_ddv", "ft_plantillas_historico",   "id_plantilla_equipo",  "temporada"),
    ("tb_ddv", "ft_partidos_detalle",       "id_partido",           "temporada"),
    ("tb_ddv", "ft_rendimiento_temporada",  "id_clasificacion",     "temporada"),
    ("tb_ddv", "ft_rendimiento_acumulado",  "id_acumulado",         "temporada"),
    ("tb_ddv", "ft_entrenadores_historico", "id_entrenador_equipo", "temporada"),
    ("tb_ddv", "ft_evolucion_valoracion",   "id_valoracion_equipo", "temporada"),
    ("tb_ddv", "ft_estadisticas_jugador",   "id_estadistica_jugador", "temporada"),
]

# Print report
print("\n" + "="*70)
print("REPORTE DE VALIDACIÓN — Liga 1")
print("="*70)

for capa, tablas in [("UDV", tablas_udv), ("DDV", tablas_ddv)]:
    print(f"\n{'─'*30} {capa} {'─'*30}")
    for schema, tabla, pk_col, temporal_col in tablas:
        r = validar_tabla(schema, tabla, pk_col, temporal_col)
        status = r.get("status", "?")
        print(f"\n[{status}] {r['tabla']}")
        print(f"  Total registros : {r['total']}")
        print(f"  Nulos en PK     : {r['nulos_pk']}")
        print(f"  Duplicados en PK: {r['duplicados_pk']}")
        if "dist_temporal" in r and r["dist_temporal"] is not None:
            print(f"  Distribución por {temporal_col}:")
            r["dist_temporal"].show(20, truncate=False)
        resultados.append(r)

print("\n" + "="*70)
print("FIN DEL REPORTE")
print("="*70)

# === WARNING SUMMARY ===
print("\n" + "="*70)
print("RESUMEN DE ADVERTENCIAS")
print("="*70)
warnings = [r for r in resultados if r.get("total") == 0]
if warnings:
    for r in warnings:
        log(f"WARNING: La tabla {r['tabla']} tiene 0 registros")
else:
    log("Todas las tablas contienen registros. Sin advertencias.")
