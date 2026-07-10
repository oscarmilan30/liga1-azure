"""
rag.py — Lógica RAG para Liga 1 Perú
Consulta tablas Delta en Databricks y usa Azure OpenAI (gpt-5.4-mini).
Cubre los mismos casos de uso que el Power BI: RENDIMIENTO, PARTIDOS,
ENTRENADORES, VALORACIÓN, PLANTILLAS, POSICIONES, XI IDEAL.
"""

import os
import re
import base64
import sys
import datetime
import pandas as pd
from databricks import sql
from openai import AzureOpenAI

AÑO_ACTUAL = datetime.datetime.now().year


# ─── Lectura segura de secrets ────────────────────────────────────────────────

def _read_secret(scope: str, key: str, env_var: str = "") -> str:
    val = os.environ.get(env_var or key, "")
    if val:
        print(f"[RAG] {env_var or key} → env var OK", file=sys.stderr)
        return val
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        result = w.secrets.get_secret(scope=scope, key=key)
        decoded = base64.b64decode(result.value).decode("utf-8") if result.value else ""
        if decoded:
            print(f"[RAG] {scope}/{key} → SDK OK", file=sys.stderr)
            return decoded
    except Exception as e:
        print(f"[RAG] SDK secret read FAILED for {scope}/{key}: {e}", file=sys.stderr)
    print(f"[RAG] {env_var or key} → MISSING", file=sys.stderr)
    return ""


# ─── Configuración ────────────────────────────────────────────────────────────

_SECRET_SCOPE    = "secretliga1"
AOAI_ENDPOINT    = os.environ.get("AOAI_ENDPOINT", "")
AOAI_DEPLOYMENT  = os.environ.get("AOAI_DEPLOYMENT", "gpt-5.4-mini")
AOAI_API_VERSION = os.environ.get("AOAI_API_VERSION", "2025-01-01-preview")
DBX_HOST         = os.environ.get("DATABRICKS_HOST", "")
# Lee del scope secretliga1 para que dev y prod usen automáticamente sus propios valores.
# Fallback a env var para desarrollo local (override con variable de entorno).
DBX_HTTP_PATH    = _read_secret(_SECRET_SCOPE, "databricks-http-path", "DATABRICKS_HTTP_PATH")
CATALOG          = _read_secret(_SECRET_SCOPE, "catalogname",           "LIGA1_CATALOG")
AOAI_API_KEY     = _read_secret(_SECRET_SCOPE, "aoai-api-key",         "AOAI_API_KEY")
DBX_TOKEN        = _read_secret(_SECRET_SCOPE, "rag-databricks-token", "DATABRICKS_TOKEN")

print(f"[RAG] AOAI_ENDPOINT={AOAI_ENDPOINT}", file=sys.stderr)
print(f"[RAG] CATALOG={CATALOG} | AÑO_ACTUAL={AÑO_ACTUAL}", file=sys.stderr)


# ─── SQL helper ───────────────────────────────────────────────────────────────

def _run_query(sql_query: str, max_retries: int = 2) -> pd.DataFrame:
    """Ejecuta SQL en Databricks con retry automático.
    Timeout 120s para tolerar cold-start del warehouse (puede tardar 30-60s si estaba suspendido).
    """
    import time
    hostname = DBX_HOST.replace("https://", "").replace("http://", "").strip("/")
    last_exc = None
    for attempt in range(max_retries + 1):
        try:
            with sql.connect(
                server_hostname=hostname,
                http_path=DBX_HTTP_PATH,
                access_token=DBX_TOKEN,
                _socket_timeout=120,          # 120s — cubre warm-up de serverless warehouse
            ) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql_query)
                    cols = [d[0] for d in cursor.description]
                    rows = cursor.fetchall()
            df = pd.DataFrame(rows, columns=cols)
            for col in ["temporada", "periodo"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
            return df
        except Exception as e:
            last_exc = e
            if attempt < max_retries:
                wait = 4 * (attempt + 1)   # 4s, 8s
                print(f"[RAG] Query retry {attempt+1}/{max_retries} (wait {wait}s): {e}", file=sys.stderr)
                time.sleep(wait)
            else:
                print(f"[RAG] Query failed after {max_retries+1} attempts: {e}", file=sys.stderr)
    raise last_exc


# ─── POSICIONES ───────────────────────────────────────────────────────────────

def get_tabla_posiciones(temporada: int | None = None, tipo_tabla: str = "Acumulado") -> pd.DataFrame:
    """Tabla de posiciones limpia (un tipo_tabla exacto, sin equipos con 0 partidos).
    Usa dos queries separadas: primero obtiene el año máximo, luego filtra.
    Esto evita subqueries inline en Spark que pueden resolver columnas de forma inesperada.
    """
    def _tipo_filter(tt: str) -> str:
        if tt.lower() == "acumulado":
            return "lower(tipo_tabla) = 'acumulado'"
        return f"lower(tipo_tabla) LIKE lower('%{tt}%')"

    def _get_max_year(tt: str) -> int:
        tf = _tipo_filter(tt)
        try:
            yr = _run_query(f"""
                SELECT MAX(temporada) AS max_year
                FROM {CATALOG}.vw_ddv.ft_rendimiento_posiciones_vw
                WHERE {tf} AND partidos_jugados > 0""")
            return int(yr.iloc[0]["max_year"])
        except Exception:
            return AÑO_ACTUAL

    def _q(tt: str, year: int) -> pd.DataFrame:
        tf = _tipo_filter(tt)
        return _run_query(f"""
            SELECT nombre_equipo, temporada, tipo_tabla, posicion,
                   partidos_jugados, partidos_ganados, partidos_empatados, partidos_perdidos,
                   goles_favor, goles_contra, diferencia_goles, puntos
            FROM {CATALOG}.vw_ddv.ft_rendimiento_posiciones_vw
            WHERE temporada = {year} AND {tf} AND partidos_jugados > 0
            ORDER BY posicion ASC LIMIT 20""")

    year = temporada if temporada else _get_max_year(tipo_tabla)
    df = _q(tipo_tabla, year)
    if df.empty and tipo_tabla == "Acumulado":
        year2 = temporada if temporada else _get_max_year("Apertura")
        df = _q("Apertura", year2)
    return df


def get_tabla_clasificacion_historica(temporada: int, tipo_tabla: str = "Acumulado") -> pd.DataFrame:
    return _run_query(f"""
        SELECT nombre_equipo, temporada, tipo_tabla, posicion,
               partidos_jugados, partidos_ganados, partidos_empatados, partidos_perdidos,
               goles_favor, goles_contra, diferencia_goles, puntos
        FROM {CATALOG}.vw_ddv.ft_rendimiento_posiciones_vw
        WHERE temporada = {temporada} AND lower(tipo_tabla) LIKE lower('%{tipo_tabla}%')
          AND partidos_jugados > 0
        ORDER BY posicion ASC LIMIT 20""")


def get_eficiencia_historica(nombre_equipo: str | None = None) -> pd.DataFrame:
    """Eficiencia histórica (PPP) — página POSICIONES de Power BI.
    Igual que la barra 'Eficiencia Histórica (Puntos por Partido)' del reporte."""
    eq_filter = f"AND lower(nombre_equipo) LIKE lower('%{nombre_equipo}%')" if nombre_equipo else ""
    return _run_query(f"""
        SELECT nombre_equipo,
               ROUND(SUM(puntos) * 1.0 / NULLIF(SUM(partidos_jugados), 0), 2) AS ppp_historico,
               SUM(puntos)                  AS puntos_totales,
               COUNT(DISTINCT temporada)    AS temporadas,
               SUM(partidos_ganados)        AS victorias_totales,
               SUM(partidos_jugados)        AS partidos_totales
        FROM {CATALOG}.vw_ddv.ft_rendimiento_posiciones_vw
        WHERE lower(tipo_tabla) LIKE '%acumulado%' AND partidos_jugados > 0
        {eq_filter}
        GROUP BY nombre_equipo
        ORDER BY ppp_historico DESC
        LIMIT 15""")


def get_carrera_titulo(top_n: int = 5) -> pd.DataFrame:
    """Carrera por el título: puntos acumulados por temporada para los top equipos históricos.
    Igual que el gráfico de líneas 'Carrera por el Título (Top 5 Histórico)' de Power BI.
    """
    return _run_query(f"""
        WITH top_equipos AS (
            SELECT nombre_equipo
            FROM {CATALOG}.vw_ddv.ft_rendimiento_posiciones_vw
            WHERE lower(tipo_tabla) LIKE '%acumulado%' AND partidos_jugados > 0
            GROUP BY nombre_equipo
            ORDER BY SUM(puntos) DESC
            LIMIT {top_n}
        )
        SELECT r.nombre_equipo, r.temporada, r.puntos, r.posicion
        FROM {CATALOG}.vw_ddv.ft_rendimiento_posiciones_vw r
        INNER JOIN top_equipos t ON r.nombre_equipo = t.nombre_equipo
        WHERE lower(r.tipo_tabla) LIKE '%acumulado%' AND r.partidos_jugados > 0
        ORDER BY r.nombre_equipo, r.temporada""")


# ─── RENDIMIENTO (evolución de un equipo) ────────────────────────────────────

def get_evolucion_puntos_equipo(nombre_equipo: str, tipo_tabla: str = "Acumulado") -> pd.DataFrame:
    """Evolución de puntos/posición por temporada para un equipo.
    Misma lógica que el gráfico de líneas 'Evolución de puntos por temporada' de Power BI RENDIMIENTO.
    """
    return _run_query(f"""
        SELECT nombre_equipo, temporada, tipo_tabla, posicion, puntos,
               partidos_jugados, partidos_ganados, partidos_empatados, partidos_perdidos,
               goles_favor, goles_contra, diferencia_goles
        FROM {CATALOG}.vw_ddv.ft_rendimiento_posiciones_vw
        WHERE lower(nombre_equipo) LIKE lower('%{nombre_equipo}%')
          AND lower(tipo_tabla) LIKE lower('%{tipo_tabla}%')
          AND partidos_jugados > 0
        ORDER BY temporada ASC""")


def get_gep_equipo(nombre_equipo: str, temporada: int | None = None) -> pd.DataFrame:
    """G/E/P acumulado de un equipo — donut de Power BI RENDIMIENTO."""
    where_temp = f"AND temporada = {temporada}" if temporada else ""
    return _run_query(f"""
        SELECT nombre_equipo,
               SUM(partidos_ganados)  AS ganados,
               SUM(partidos_empatados) AS empatados,
               SUM(partidos_perdidos) AS perdidos,
               SUM(partidos_jugados)  AS jugados,
               SUM(puntos)            AS puntos
        FROM {CATALOG}.vw_ddv.ft_rendimiento_posiciones_vw
        WHERE lower(nombre_equipo) LIKE lower('%{nombre_equipo}%')
          AND lower(tipo_tabla) LIKE '%acumulado%'
          AND partidos_jugados > 0
        {where_temp}
        GROUP BY nombre_equipo""")


# ─── PARTIDOS ─────────────────────────────────────────────────────────────────

def get_partidos_equipo(nombre_equipo: str, temporada: int | None = None) -> pd.DataFrame:
    """Últimos partidos con nombre del rival (id_rival → dm_equipos_vw), igual que Power BI."""
    where_temp = f"AND a.temporada = {temporada}" if temporada else ""
    return _run_query(f"""
        SELECT a.nombre_equipo, a.temporada, a.fecha_partido, a.rol_equipo,
               e.nombre_equipo AS rival,
               a.goles_favor, a.goles_contra, a.resultado,
               a.posesion_pct, a.tiros_totales, a.disparos_puerta,
               a.tarjetas_amarillas, a.tarjetas_rojas
        FROM {CATALOG}.vw_ddv.ft_partidos_equipo_vw a
        JOIN {CATALOG}.vw_ddv.dm_equipos_vw e ON e.id_equipo = a.id_rival
        WHERE lower(a.nombre_equipo) LIKE lower('%{nombre_equipo}%')
        {where_temp} AND a.estado_partido = 'Disputado'
        ORDER BY a.fecha_partido DESC LIMIT 10""")


def get_mayor_goleada(temporada: int | None = None) -> pd.DataFrame:
    """Mayor goleada usando id_rival → dm_equipos_vw y diferencia_goles pre-calculada."""
    year_cond = f"a.temporada = {temporada}" if temporada else \
        f"a.temporada = (SELECT MAX(t.temporada) FROM {CATALOG}.vw_ddv.ft_partidos_equipo_vw t WHERE t.estado_partido = 'Disputado')"
    return _run_query(f"""
        SELECT a.nombre_equipo AS equipo_ganador, e.nombre_equipo AS equipo_perdedor,
               a.temporada, a.fecha_partido, a.resultado,
               a.goles_favor, a.goles_contra, a.diferencia_goles
        FROM {CATALOG}.vw_ddv.ft_partidos_equipo_vw a
        JOIN {CATALOG}.vw_ddv.dm_equipos_vw e ON e.id_equipo = a.id_rival
        WHERE {year_cond} AND a.estado_partido = 'Disputado' AND a.goles_favor > a.goles_contra
        ORDER BY a.diferencia_goles DESC, a.goles_favor DESC LIMIT 10""")


def get_vs_equipos(equipo1: str, equipo2: str, temporada: int | None = None) -> pd.DataFrame:
    """Comparativa entre dos equipos — página PARTIDOS de Power BI.
    Stats promedio por partido para comparar directamente.
    """
    where_temp = f"AND temporada = {temporada}" if temporada else ""
    return _run_query(f"""
        SELECT nombre_equipo,
               COUNT(*)                                       AS partidos,
               SUM(goles_favor)                              AS goles_marcados,
               SUM(goles_contra)                             AS goles_recibidos,
               ROUND(AVG(posesion_pct), 1)                   AS posesion_media_pct,
               ROUND(AVG(tiros_totales), 1)                  AS tiros_promedio,
               ROUND(AVG(disparos_puerta), 1)                AS disparos_puerta_promedio,
               SUM(CASE WHEN goles_favor > goles_contra THEN 1 ELSE 0 END) AS victorias,
               SUM(CASE WHEN goles_favor = goles_contra THEN 1 ELSE 0 END) AS empates,
               SUM(CASE WHEN goles_favor < goles_contra THEN 1 ELSE 0 END) AS derrotas,
               ROUND(AVG(tarjetas_amarillas), 2)             AS amarillas_promedio
        FROM {CATALOG}.vw_ddv.ft_partidos_equipo_vw
        WHERE (lower(nombre_equipo) LIKE lower('%{equipo1}%')
            OR lower(nombre_equipo) LIKE lower('%{equipo2}%'))
          AND estado_partido = 'Disputado'
        {where_temp}
        GROUP BY nombre_equipo
        ORDER BY nombre_equipo""")


def get_partido_rival(equipo1: str, equipo2: str, temporada: int | None = None) -> pd.DataFrame:
    """Historial de enfrentamientos directos entre dos equipos (H2H)."""
    where_temp = f"AND a.temporada = {temporada}" if temporada else ""
    return _run_query(f"""
        SELECT a.nombre_equipo AS equipo, e.nombre_equipo AS rival,
               a.temporada, a.fecha_partido, a.resultado, a.rol_equipo,
               a.goles_favor, a.goles_contra,
               CASE WHEN a.goles_favor > a.goles_contra THEN 'Victoria'
                    WHEN a.goles_favor = a.goles_contra THEN 'Empate'
                    ELSE 'Derrota' END AS resultado_final
        FROM {CATALOG}.vw_ddv.ft_partidos_equipo_vw a
        JOIN {CATALOG}.vw_ddv.dm_equipos_vw e ON e.id_equipo = a.id_rival
        WHERE lower(a.nombre_equipo) LIKE lower('%{equipo1}%')
          AND lower(e.nombre_equipo) LIKE lower('%{equipo2}%')
          AND a.estado_partido = 'Disputado'
        {where_temp}
        ORDER BY a.fecha_partido DESC LIMIT 10""")


def get_goles_por_equipo(temporada: int | None = None) -> pd.DataFrame:
    """Goles marcados por equipo en una temporada — para 'qué equipo metió más goles'."""
    where = f"WHERE temporada = {temporada} AND estado_partido = 'Disputado'" if temporada else \
        f"""WHERE temporada = (
            SELECT MAX(temporada) FROM {CATALOG}.vw_ddv.ft_partidos_equipo_vw
            WHERE estado_partido = 'Disputado')
        AND estado_partido = 'Disputado'"""
    return _run_query(f"""
        SELECT nombre_equipo, temporada,
               SUM(goles_favor)  AS goles_marcados,
               SUM(goles_contra) AS goles_recibidos,
               COUNT(*)          AS partidos_jugados
        FROM {CATALOG}.vw_ddv.ft_partidos_equipo_vw
        {where}
        GROUP BY nombre_equipo, temporada
        ORDER BY goles_marcados DESC LIMIT 15""")


def get_precision_tiro(temporada: int | None = None, nombre_equipo: str | None = None) -> pd.DataFrame:
    """Precisión de tiro por equipo — donut 'Precisión de Tiros' de Power BI PARTIDOS."""
    where_parts = ["estado_partido = 'Disputado'", "tiros_totales > 0"]
    if temporada:
        where_parts.append(f"temporada = {temporada}")
    if nombre_equipo:
        where_parts.append(f"lower(nombre_equipo) LIKE lower('%{nombre_equipo}%')")
    where = "WHERE " + " AND ".join(where_parts)
    return _run_query(f"""
        SELECT nombre_equipo, temporada,
               SUM(tiros_totales)   AS tiros_totales,
               SUM(disparos_puerta) AS disparos_puerta,
               SUM(goles_favor)     AS goles,
               ROUND(SUM(disparos_puerta) * 100.0 / NULLIF(SUM(tiros_totales), 0), 1)  AS precision_tiro_pct,
               ROUND(SUM(goles_favor) * 100.0 / NULLIF(SUM(disparos_puerta), 0), 1)    AS conversion_pct
        FROM {CATALOG}.vw_ddv.ft_partidos_equipo_vw
        {where}
        GROUP BY nombre_equipo, temporada
        ORDER BY precision_tiro_pct DESC LIMIT 20""")


# ─── JUGADORES ────────────────────────────────────────────────────────────────

def _nombre_cond(nombre: str, col: str = "jugador") -> str:
    """Genera condición SQL LIKE tolerante a typos parciales.
    Divide el nombre en palabras y exige que CADA palabra de ≥3 chars esté en el campo.
    Ej: 'emanel herrera' → col LIKE '%emanel%' AND col LIKE '%herrera%'
    Si hay solo una palabra corta, usa LIKE '%nombre%' directo.
    """
    words = [w for w in nombre.lower().strip().split() if len(w) >= 3]
    if not words:
        return f"lower({col}) LIKE lower('%{nombre}%')"
    return " AND ".join(f"lower({col}) LIKE lower('%{w}%')" for w in words)


def get_estadisticas_jugador(nombre: str, temporada: int | None = None) -> pd.DataFrame:
    where_temp = f"AND temporada = {temporada}" if temporada else ""
    cond = _nombre_cond(nombre)
    return _run_query(f"""
        SELECT jugador, nombre_equipo, temporada, posicion,
               partidos_jugados, titularidades, goles, asistencias,
               tarjetas_amarillas, tarjetas_rojas, minutos_jugados, ppp
        FROM {CATALOG}.vw_ddv.ft_estadisticas_jugadores_vw
        WHERE {cond} {where_temp}
        ORDER BY temporada DESC LIMIT 10""")


def get_top_goleadores(temporada: int | None = None, limit: int = 10) -> pd.DataFrame:
    where = f"WHERE temporada = {temporada}" if temporada else \
        f"WHERE temporada = (SELECT MAX(temporada) FROM {CATALOG}.vw_ddv.ft_estadisticas_jugadores_vw WHERE goles > 0)"
    return _run_query(f"""
        SELECT jugador, nombre_equipo, temporada, posicion, goles, asistencias,
               partidos_jugados, minutos_jugados
        FROM {CATALOG}.vw_ddv.ft_estadisticas_jugadores_vw
        {where} AND goles > 0
        ORDER BY goles DESC LIMIT {limit}""")


def get_vs_jugadores(jugador1: str, jugador2: str, temporada: int | None = None) -> pd.DataFrame:
    """Comparativa estadísticas entre dos jugadores. Usa _nombre_cond para tolerar typos."""
    where_temp = f"AND temporada = {temporada}" if temporada else ""
    cond1 = _nombre_cond(jugador1)
    cond2 = _nombre_cond(jugador2)
    return _run_query(f"""
        SELECT jugador, nombre_equipo, temporada, posicion,
               partidos_jugados, titularidades, goles, asistencias,
               minutos_jugados, tarjetas_amarillas, ppp
        FROM {CATALOG}.vw_ddv.ft_estadisticas_jugadores_vw
        WHERE (({cond1}) OR ({cond2}))
        {where_temp}
        ORDER BY jugador, temporada DESC""")


def get_score_ml(temporada: int | None = None, posicion: str | None = None) -> pd.DataFrame:
    """Top jugadores por score ML.
    Two-step para evitar subquery inline en Spark + GROUP BY para deduplicar por formacion/slot.
    posicion: filtro opcional ('portero', 'defensor', 'mediocampista', 'delantero').
    """
    if temporada:
        yr = temporada
    else:
        try:
            yr_df = _run_query(
                f"SELECT MAX(temporada) AS max_yr FROM {CATALOG}.vw_ddv.ft_score_ml_vw")
            yr = int(yr_df.iloc[0]["max_yr"])
        except Exception:
            yr = AÑO_ACTUAL
    posicion_cond = f"AND lower(posicion) LIKE lower('%{posicion}%')" if posicion else ""
    return _run_query(f"""
        SELECT jugador, nombre_equipo,
               MAX(posicion)  AS posicion,
               MAX(nivel)     AS nivel,
               MAX(score_100) AS score_100,
               temporada
        FROM {CATALOG}.vw_ddv.ft_score_ml_vw
        WHERE temporada = {yr} {posicion_cond}
        GROUP BY jugador, nombre_equipo, temporada
        ORDER BY score_100 DESC LIMIT 15""")


def get_score_ml_evolucion(nombre_jugador: str) -> pd.DataFrame:
    """Evolución del score ML de un jugador por temporada.
    GROUP BY para deduplicar filas por formacion/slot en ft_score_ml_vw.
    """
    cond = _nombre_cond(nombre_jugador, col="jugador")
    return _run_query(f"""
        SELECT jugador, nombre_equipo, temporada,
               MAX(score_100)        AS score_100,
               MAX(nivel)            AS nivel,
               MAX(goles)            AS goles,
               MAX(asistencias)      AS asistencias,
               MAX(partidos_jugados) AS partidos_jugados
        FROM {CATALOG}.vw_ddv.ft_score_ml_vw
        WHERE {cond}
        GROUP BY jugador, nombre_equipo, temporada
        ORDER BY temporada ASC""")


def get_gemas_ocultas(temporada: int | None = None) -> pd.DataFrame:
    """Jugadores con alto score ML pero poca exposición (pocos partidos / minutos).
    Criterio: score_100 >= 65 (nivel Bueno o Elite) y partidos_jugados <= 12.
    """
    if temporada:
        yr = temporada
    else:
        try:
            yr_df = _run_query(
                f"SELECT MAX(temporada) AS max_yr FROM {CATALOG}.vw_ddv.ft_score_ml_vw")
            yr = int(yr_df.iloc[0]["max_yr"])
        except Exception:
            yr = AÑO_ACTUAL
    return _run_query(f"""
        SELECT jugador, nombre_equipo,
               MAX(posicion)         AS posicion,
               MAX(nivel)            AS nivel,
               MAX(score_100)        AS score_100,
               MAX(partidos_jugados) AS partidos_jugados,
               MAX(minutos_jugados)  AS minutos_jugados,
               MAX(goles)            AS goles,
               MAX(asistencias)      AS asistencias,
               temporada
        FROM {CATALOG}.vw_ddv.ft_score_ml_vw
        WHERE temporada = {yr}
        GROUP BY jugador, nombre_equipo, temporada
        HAVING MAX(score_100) >= 65 AND MAX(partidos_jugados) <= 12
        ORDER BY score_100 DESC LIMIT 15""")


def get_jugadores_bajo_rendimiento(temporada: int | None = None) -> pd.DataFrame:
    """Jugadores con muchos partidos pero score ML bajo — rindiendo por debajo de lo esperado.
    Criterio: partidos_jugados >= 15 (son regulares/titulares) y score_100 < 50.
    """
    if temporada:
        yr = temporada
    else:
        try:
            yr_df = _run_query(
                f"SELECT MAX(temporada) AS max_yr FROM {CATALOG}.vw_ddv.ft_score_ml_vw")
            yr = int(yr_df.iloc[0]["max_yr"])
        except Exception:
            yr = AÑO_ACTUAL
    return _run_query(f"""
        SELECT jugador, nombre_equipo,
               MAX(posicion)         AS posicion,
               MAX(nivel)            AS nivel,
               MAX(score_100)        AS score_100,
               MAX(partidos_jugados) AS partidos_jugados,
               MAX(minutos_jugados)  AS minutos_jugados,
               MAX(goles)            AS goles,
               MAX(asistencias)      AS asistencias,
               temporada
        FROM {CATALOG}.vw_ddv.ft_score_ml_vw
        WHERE temporada = {yr}
        GROUP BY jugador, nombre_equipo, temporada
        HAVING MAX(partidos_jugados) >= 15 AND MAX(score_100) < 50
        ORDER BY score_100 ASC LIMIT 15""")


def get_perfil_jugador(nombre: str, temporada: int | None = None) -> pd.DataFrame:
    where_temp = f"AND p.temporada = {temporada}" if temporada else ""
    return _run_query(f"""
        SELECT e.jugador, e.nombre_equipo, e.temporada, e.posicion,
               e.goles, e.asistencias, e.partidos_jugados, e.minutos_jugados,
               p.altura, p.pie, p.valor_mercado, p.edad_historica,
               p.nacionalidad_principal
        FROM {CATALOG}.vw_ddv.ft_estadisticas_jugadores_vw e
        LEFT JOIN {CATALOG}.vw_ddv.ft_plantillas_historico_vw p
            ON p.id_jugador = e.id_jugador AND p.temporada = e.temporada
        WHERE lower(e.jugador) LIKE lower('%{nombre}%') {where_temp}
        ORDER BY e.temporada DESC LIMIT 10""")


def _detectar_posicion(pregunta: str) -> str | None:
    """Detecta si la pregunta pide score ML filtrado por posición."""
    p = pregunta.lower()
    if any(w in p for w in ["portero", "arquero", "guardameta"]):
        return "portero"
    if any(w in p for w in ["defensa", "defensor", "zaguero", "lateral"]):
        return "defensor"
    if any(w in p for w in ["mediocampista", "volante", "centrocampista"]):
        return "mediocampista"
    if any(w in p for w in ["delantero", "atacante", "extremo", "punta"]):
        return "delantero"
    return None


def _detectar_formacion(pregunta: str) -> str | None:
    """Extrae formación táctica: '4-4-2', '4-3-3', '3-5-2', etc."""
    match = re.search(r'\b(\d[-\s]\d[-\s]\d(?:[-\s]\d)?)\b', pregunta)
    if match:
        return match.group(1).replace(" ", "-")
    return None


def _detectar_estilo(pregunta: str) -> str | None:
    """Detecta el estilo de juego pedido."""
    p = pregunta.lower()
    if any(w in p for w in ["ofensivo", "ataque", "goleador", "más goles", "mas goles", "ofensiva"]):
        return "ofensivo"
    if any(w in p for w in ["defensivo", "sólido", "solido", "defensiva", "defensivo"]):
        return "defensivo"
    if any(w in p for w in ["equilibrado", "balanceado", "mixto", "balanceada"]):
        return "equilibrado"
    return None


_formaciones_cache: dict | None = None


def _resolver_id_formacion(formacion_str: str | None, estilo: str | None = None,
                            temporada: int | None = None) -> int:
    """Mapea '4-4-2' / estilo → id_formacion.
    Lee de dm_formacion (vista separada, NO ft_score_ml_vw).
    dm_formacion tiene: id_formacion, formacion ('4-3-3'), estilo ('Ofensivo'), formacion_display.
    """
    global _formaciones_cache
    if _formaciones_cache is None:
        try:
            df = _run_query(f"""
                SELECT id_formacion,
                       formacion       AS nombre_formacion,
                       estilo          AS nombre_estilo,
                       formacion_display
                FROM {CATALOG}.vw_ddv.dm_formacion
                ORDER BY id_formacion""")
            _formaciones_cache = df.to_dict("records")
            print(f"[RAG] formaciones: {[r['formacion_display'] for r in _formaciones_cache]}", file=sys.stderr)
        except Exception as e:
            print(f"[RAG] dm_formacion error: {e}", file=sys.stderr)
            # Fallback estático (mirror del DDL real)
            _formaciones_cache = [
                {"id_formacion": 1, "nombre_formacion": "4-3-3", "nombre_estilo": "Ofensivo"},
                {"id_formacion": 2, "nombre_formacion": "3-4-3", "nombre_estilo": "Ofensivo"},
                {"id_formacion": 3, "nombre_formacion": "4-2-3-1", "nombre_estilo": "Equilibrado"},
                {"id_formacion": 4, "nombre_formacion": "4-4-2", "nombre_estilo": "Equilibrado"},
                {"id_formacion": 5, "nombre_formacion": "4-3-3", "nombre_estilo": "Equilibrado"},
                {"id_formacion": 6, "nombre_formacion": "5-3-2", "nombre_estilo": "Defensivo"},
                {"id_formacion": 7, "nombre_formacion": "5-4-1", "nombre_estilo": "Defensivo"},
                {"id_formacion": 8, "nombre_formacion": "4-1-4-1", "nombre_estilo": "Defensivo"},
            ]

    if not _formaciones_cache:
        return 1

    # 1) Busca match EXACTO de formación + estilo (si ambos especificados)
    if formacion_str and estilo:
        clean = formacion_str.replace("-", "").replace(" ", "")
        for row in _formaciones_cache:
            nf = str(row.get("nombre_formacion") or "").replace("-", "").replace(" ", "")
            ne = str(row.get("nombre_estilo") or "").lower()
            if (clean == nf or clean in nf) and estilo in ne:
                return int(row["id_formacion"])

    # 2) Busca solo por formación (ej: "4-4-2" → id=4)
    if formacion_str:
        clean = formacion_str.replace("-", "").replace(" ", "")
        for row in _formaciones_cache:
            nf = str(row.get("nombre_formacion") or "").replace("-", "").replace(" ", "")
            if clean == nf:
                return int(row["id_formacion"])
        # Match parcial como fallback
        for row in _formaciones_cache:
            nf = str(row.get("nombre_formacion") or "").replace("-", "").replace(" ", "")
            if clean in nf or nf in clean:
                return int(row["id_formacion"])

    # 3) Busca solo por estilo
    if estilo:
        for row in _formaciones_cache:
            ne = str(row.get("nombre_estilo") or "").lower()
            if estilo in ne:
                return int(row["id_formacion"])

    # 4) Fallback: primera formación (4-3-3 Ofensivo)
    return int(_formaciones_cache[0]["id_formacion"])


def get_formaciones_disponibles() -> pd.DataFrame:
    """Retorna las formaciones disponibles de dm_formacion."""
    return _run_query(f"""
        SELECT id_formacion, formacion, estilo, formacion_display
        FROM {CATALOG}.vw_ddv.dm_formacion
        ORDER BY id_formacion""")


def get_mejor_xi(temporada: int | None = None, id_formacion: int = 1) -> pd.DataFrame:
    """XI ideal con coordenadas de campo para renderizar en plotly."""
    year_filter = f"temporada = {temporada}" if temporada else \
        f"temporada = (SELECT MAX(temporada) FROM {CATALOG}.vw_ddv.ft_score_ml_vw)"
    return _run_query(f"""
        WITH ranked AS (
            SELECT jugador, nombre_equipo, alias_equipo, posicion_xi, posicion_xi_nombre,
                   score_100, nivel, nivel_num, foto_url,
                   goles, asistencias, participaciones_gol,
                   partidos_jugados, minutos_jugados,
                   x_coord, y_coord, slot, orden,
                   ROW_NUMBER() OVER (
                       PARTITION BY slot ORDER BY score_100 DESC, id_jugador ASC
                   ) AS rn
            FROM {CATALOG}.vw_ddv.ft_score_ml_vw
            WHERE {year_filter} AND id_formacion = {id_formacion}
        )
        SELECT jugador, nombre_equipo, alias_equipo, posicion_xi, posicion_xi_nombre,
               score_100, nivel, foto_url,
               goles, asistencias, participaciones_gol,
               partidos_jugados, minutos_jugados,
               x_coord, y_coord, slot, orden
        FROM ranked WHERE rn = 1
        ORDER BY orden ASC""")


# ─── PLANTILLAS ───────────────────────────────────────────────────────────────

def get_plantilla_equipo(nombre_equipo: str, temporada: int | None = None) -> pd.DataFrame:
    where_temp = f"AND temporada = {temporada}" if temporada else \
        f"AND temporada = (SELECT MAX(temporada) FROM {CATALOG}.vw_ddv.ft_plantillas_historico_vw)"
    return _run_query(f"""
        SELECT jugador, posicion, edad_historica, nacionalidad_principal, pie,
               numero_camiseta, valor_mercado, nombre_equipo, temporada
        FROM {CATALOG}.vw_ddv.ft_plantillas_historico_vw
        WHERE lower(nombre_equipo) LIKE lower('%{nombre_equipo}%') {where_temp}
        ORDER BY valor_mercado DESC LIMIT 25""")


# ─── VALORACIÓN ───────────────────────────────────────────────────────────────

def get_valoracion_equipos(
    temporada: int | None = None,
    nombre_equipo: str | None = None,
    año_inicio: int | None = None,
    año_fin: int | None = None,
) -> pd.DataFrame:
    """Valor de mercado por equipo. Soporta rango de años y equipo específico."""
    where_parts = []
    if año_inicio and año_fin:
        where_parts.append(f"temporada BETWEEN {año_inicio} AND {año_fin}")
    elif temporada:
        where_parts.append(f"temporada = {temporada}")
    else:
        where_parts.append(
            f"temporada = (SELECT MAX(temporada) FROM {CATALOG}.vw_ddv.ft_evolucion_valoracion_vw)"
        )
    if nombre_equipo:
        where_parts.append(f"lower(nombre_equipo) LIKE lower('%{nombre_equipo}%')")
    where = "WHERE " + " AND ".join(where_parts)
    order = "temporada ASC, valor_total DESC" if (año_inicio or nombre_equipo) else "valor_total DESC"
    return _run_query(f"""
        SELECT nombre_equipo, temporada, jugadores_en_plantilla,
               edad_promedio, extranjeros, valor_medio, valor_total
        FROM {CATALOG}.vw_ddv.ft_evolucion_valoracion_vw
        {where} ORDER BY {order} LIMIT 100""")


def get_evolucion_valor_liga() -> pd.DataFrame:
    """Evolución del valor total de la liga por temporada.
    Equivale al gráfico de líneas de la página VALORACIÓN de Power BI.
    """
    return _run_query(f"""
        SELECT temporada,
               ROUND(SUM(valor_total) / 1000000, 2) AS valor_total_mm_eur,
               ROUND(AVG(edad_promedio), 1)          AS edad_promedio_liga,
               ROUND(AVG(extranjeros), 1)            AS extranjeros_promedio,
               COUNT(DISTINCT nombre_equipo)         AS equipos
        FROM {CATALOG}.vw_ddv.ft_evolucion_valoracion_vw
        GROUP BY temporada ORDER BY temporada ASC""")


# ─── ENTRENADORES ─────────────────────────────────────────────────────────────

def get_entrenadores(nombre_equipo: str | None = None, temporada: int | None = None) -> pd.DataFrame:
    """Entrenadores ordenados por PPP DESC. Criterio 'Mejor DT' de Power BI = PPP con mín. 5 partidos."""
    where_parts = ["partidos >= 5"]
    if nombre_equipo:
        where_parts.append(f"lower(nombre_equipo) LIKE lower('%{nombre_equipo}%')")
    if temporada:
        where_parts.append(f"temporada = {temporada}")
    where = "WHERE " + " AND ".join(where_parts)
    return _run_query(f"""
        SELECT nombre_equipo, entrenador, nacionalidad_principal, temporada,
               partidos, ppp, tiempo_en_cargo, entrenador_activo,
               fecha_inicio, fecha_termino
        FROM {CATALOG}.vw_ddv.ft_entrenadores_historico_vw
        {where}
        ORDER BY ppp DESC, partidos DESC LIMIT 15""")


# ─── RENDIMIENTO ──────────────────────────────────────────────────────────────

def get_vs_detalle_estadisticas(equipo1: str, equipo2: str, temporada: int | None = None) -> pd.DataFrame:
    """Comparativa pases, duelos y precisión entre dos equipos usando ft_partidos_detalle_vw.
    Solo incluye partidos donde hay estadísticas (pases_local > 0).
    Cobertura: 2026≈94%, 2025≈58%, 2024-2022≈48%. Antes de 2022 es escasa.
    """
    year_cond = f"AND temporada = {temporada}" if temporada else ""
    return _run_query(f"""
        SELECT equipo,
               COUNT(*)                                        AS partidos_con_datos,
               ROUND(AVG(pases), 0)                           AS pases_promedio,
               ROUND(AVG(pases_precisos), 0)                  AS pases_precisos_promedio,
               ROUND(AVG(precision_pases_pct) * 100, 1)       AS precision_pases_pct,
               ROUND(AVG(duelos_ganados), 0)                  AS duelos_ganados_promedio,
               ROUND(AVG(aereos_ganados), 0)                  AS aereos_ganados_promedio,
               ROUND(AVG(entradas), 0)                        AS entradas_promedio,
               ROUND(AVG(regates), 0)                         AS regates_realizados_promedio
        FROM (
            SELECT nombre_equipo_local          AS equipo,
                   pases_local                  AS pases,
                   pases_precisos_local         AS pases_precisos,
                   pases_precisos_local_pct     AS precision_pases_pct,
                   duelos_ganados_local         AS duelos_ganados,
                   aereo_ganado_local           AS aereos_ganados,
                   entradas_local               AS entradas,
                   regates_realizados_local     AS regates
            FROM {CATALOG}.vw_ddv.ft_partidos_detalle_vw
            WHERE (lower(nombre_equipo_local) LIKE lower('%{equipo1}%')
                OR lower(nombre_equipo_local) LIKE lower('%{equipo2}%'))
              AND pases_local > 0
            {year_cond}
            UNION ALL
            SELECT nombre_equipo_visitante      AS equipo,
                   pases_visitante              AS pases,
                   pases_precisos_visitante     AS pases_precisos,
                   pases_precisos_visitante_pct AS precision_pases_pct,
                   duelos_ganados_visitante     AS duelos_ganados,
                   aereo_ganado_visitante       AS aereos_ganados,
                   entradas_visitante           AS entradas,
                   regates_realizados_visitante AS regates
            FROM {CATALOG}.vw_ddv.ft_partidos_detalle_vw
            WHERE (lower(nombre_equipo_visitante) LIKE lower('%{equipo1}%')
                OR lower(nombre_equipo_visitante) LIKE lower('%{equipo2}%'))
              AND pases_visitante > 0
            {year_cond}
        ) t
        GROUP BY equipo
        ORDER BY equipo""")


def get_vs_entrenadores(dt1: str, dt2: str) -> pd.DataFrame:
    """Comparativa estadísticas históricas entre dos entrenadores."""
    return _run_query(f"""
        SELECT entrenador,
               COUNT(DISTINCT nombre_equipo)  AS equipos_dirigidos,
               COUNT(DISTINCT temporada)      AS temporadas,
               SUM(partidos)                  AS partidos_totales,
               ROUND(AVG(ppp), 2)             AS ppp_promedio,
               ROUND(MAX(ppp), 2)             AS mejor_ppp_temporada,
               ROUND(MIN(ppp), 2)             AS peor_ppp_temporada,
               MAX(tiempo_en_cargo)           AS max_dias_en_cargo
        FROM {CATALOG}.vw_ddv.ft_entrenadores_historico_vw
        WHERE (lower(entrenador) LIKE lower('%{dt1}%')
            OR lower(entrenador) LIKE lower('%{dt2}%'))
          AND partidos >= 3
        GROUP BY entrenador
        ORDER BY ppp_promedio DESC""")


def get_rendimiento_equipo(nombre_equipo: str, temporada: int | None = None) -> pd.DataFrame:
    where_temp = f"AND temporada = {temporada}" if temporada else ""
    return _run_query(f"""
        SELECT nombre_equipo, temporada, tipo_tabla, posicion,
               partidos_jugados, partidos_ganados, partidos_empatados, partidos_perdidos,
               goles_favor, goles_contra, diferencia_goles, puntos
        FROM {CATALOG}.vw_ddv.ft_rendimiento_posiciones_vw
        WHERE lower(nombre_equipo) LIKE lower('%{nombre_equipo}%')
          AND partidos_jugados > 0 {where_temp}
        ORDER BY temporada DESC, tipo_tabla, posicion LIMIT 20""")


# ─── OTROS ────────────────────────────────────────────────────────────────────

def get_campeones() -> pd.DataFrame:
    return _run_query(f"""
        SELECT c.temporada,
               e_camp.nombre_equipo AS campeon,
               e_sub.nombre_equipo  AS subcampeon
        FROM {CATALOG}.vw_udv.hm_campeones_vw c
        LEFT JOIN {CATALOG}.vw_ddv.dm_equipos_vw e_camp ON e_camp.id_equipo = c.id_equipo_campeon
        LEFT JOIN {CATALOG}.vw_ddv.dm_equipos_vw e_sub  ON e_sub.id_equipo  = c.id_equipo_subcampeon
        ORDER BY c.temporada DESC LIMIT 20""")


# ─── Detección de intención ────────────────────────────────────────────────────

def _detectar_rango_años(pregunta: str) -> tuple[int, int] | None:
    """Detecta un rango de años. También maneja 'desde 2020 al actual/ahora/hoy'."""
    p = pregunta.lower()
    años = re.findall(r'\b(20\d{2})\b', pregunta)
    # Caso explícito: "desde 2020 al actual / hasta ahora / hasta hoy / al año actual"
    if len(años) == 1 and any(w in p for w in [
        "al actual", "hasta ahora", "hasta hoy", "al año actual",
        "hasta el actual", "a la fecha", "hasta la fecha", "al presente",
    ]):
        return (int(años[0]), AÑO_ACTUAL)
    # Caso dos años distintos en la pregunta
    if len(años) >= 2:
        a, b = int(años[0]), int(años[-1])
        if a != b:
            return (min(a, b), max(a, b))
    return None


def _detectar_temporada(pregunta: str) -> int | None:
    """Retorna la temporada explícita. Si hay un rango, retorna None para no fijar un solo año."""
    p = pregunta.lower()
    # Si hay "este año / año actual" sin un año numérico → año actual
    if any(w in p for w in ["este año", "año actual", "temporada actual"]):
        if not re.search(r'\b(20\d{2})\b', pregunta):
            return AÑO_ACTUAL
    # Si hay un rango no fijamos una sola temporada
    if _detectar_rango_años(pregunta):
        return None
    match = re.search(r'\b(20\d{2})\b', pregunta)
    return int(match.group(1)) if match else None


def _detectar_vs(pregunta: str) -> tuple[str, str] | None:
    """Detecta comparativa VS entre dos entidades (equipos o jugadores).
    Funciona con cualquier capitalización.
    """
    p = pregunta.lower()
    if not any(w in p for w in [" vs ", " versus ", "compara", "comparar", " contra ", " entre "]):
        return None

    # Primero: buscar dos equipos conocidos (ya en lowercase)
    equipos = sorted(_get_equipos_conocidos(), key=len, reverse=True)
    found = [eq for eq in equipos if eq in p]
    if len(found) >= 2:
        return found[0], found[1]

    # Segundo: separar por el token VS/VERSUS/CONTRA y extraer las dos entidades
    _RUIDO = {
        "compara", "comparar", "compara a", "comparame", "dame",
        "muestra", "quiero", "ver", "el", "la", "los", "las",
        "un", "una", "de", "del", "al", "a", "en", "con",
        "para", "me", "entre", "y", "que",
    }
    for sep in [" vs ", " versus ", " contra "]:
        if sep in p:
            partes = p.split(sep, 1)
            izq_raw, der_raw = partes[0].strip(), partes[1].strip()

            # Limpiar izquierda: quitar coma y texto posterior, luego ruido al inicio
            izq_raw = izq_raw.split(",")[0].strip()
            izq_palabras = izq_raw.split()
            while izq_palabras and izq_palabras[0] in _RUIDO:
                izq_palabras = izq_palabras[1:]
            izq = " ".join(izq_palabras[-3:])  # máx 3 palabras

            # Limpiar derecha: primero coma (ej: "alex valera, gráfico radar"), luego año/keywords
            der_raw = der_raw.split(",")[0].strip()
            der_raw = re.split(r'\s+(?:para|en\s+\d{4}|\d{4}|del|de\s+\d{4}|\?)', der_raw)[0].strip()
            der_palabras = der_raw.split()
            der = " ".join(der_palabras[:3])  # máx 3 palabras

            if izq and der and len(izq) > 2 and len(der) > 2:
                # Intentar corregir typos de equipos con fuzzy match
                izq_fix = _fuzzy_equipo(izq)
                der_fix = _fuzzy_equipo(der)
                return (izq_fix or izq), (der_fix or der)

    # Tercero: patrón "entre X y Y" (para h2h partido)
    m = re.search(r'entre\s+(.+?)\s+y\s+(.+?)(?:\s+(?:para|en|del|\?)|$)', p)
    if m:
        izq = " ".join(m.group(1).strip().split()[-3:])
        der = " ".join(m.group(2).strip().split()[:3])
        if izq and der:
            return izq, der

    return None


_equipos_cache: list[str] | None = None


def _get_equipos_conocidos() -> list[str]:
    global _equipos_cache
    if _equipos_cache is not None:
        return _equipos_cache
    try:
        df = _run_query(f"""
            SELECT nombre_equipo, alias_equipo
            FROM {CATALOG}.vw_ddv.dm_equipos_vw ORDER BY nombre_equipo""")
        keywords = []
        for _, row in df.iterrows():
            nombre = str(row["nombre_equipo"] or "").strip().lower()
            if nombre and len(nombre) >= 4:
                keywords.append(nombre)
            alias = str(row["alias_equipo"] or "").strip().lower()
            # Filtrar aliases muy cortos (≤3 chars) → causan falsos positivos
            if alias and len(alias) >= 4:
                keywords.append(alias)
        keywords += ["alianza lima", "sporting cristal", "cristal", "universitario",
                     "vallejo", "grau", "boys", "chankas", "stein", "san martin",
                     "juan pablo", "ayacucho", "mannucci", "cienciano", "melgar",
                     "binacional", "cajamarca", "cantolao", "huancayo"]
        _equipos_cache = list(set(keywords))
    except Exception:
        _equipos_cache = ["alianza lima", "universitario", "cristal", "melgar",
                          "vallejo", "cienciano", "cajamarca", "boys", "grau",
                          "binacional", "mannucci", "ayacucho", "cusco",
                          "cantolao", "chankas", "huancayo"]
    return _equipos_cache


def _detectar_entidad(pregunta: str) -> dict:
    p = pregunta.lower()
    return {
        "campeones":      any(w in p for w in ["campeon", "campeón", "ganó la liga", "titulo", "título", "quien ganó"]),
        "entrenadores":   any(w in p for w in ["entrenador", "técnico", "tecnico", "dt ", "director técnico"]),
        "valoracion":     any(w in p for w in ["valor", "mercado", "precio", "caro", "valioso", "valoracion", "valorizacion", "valorización"]),
        "plantilla":      any(w in p for w in ["plantel", "plantilla", "jugadores de", "jugadores del", "nómina", "nomina", "elenco"]),
        "mejor_xi":       any(w in p for w in ["xi ideal", "mejor xi", "once ideal", "11 ideal", "formacion ideal", "formación ideal"])
                          or bool(re.search(r'\bxi\b', p)),
        "perfil":         any(w in p for w in ["altura", "pie dominante", "nacionalidad", "edad", "perfil", "caracteristicas"]),
        "gemas_ocultas":  any(w in p for w in ["gema", "oculta", "talento oculto", "poco conocido",
                                               "baja exposicion", "baja exposición", "poca exposicion",
                                               "poca exposición", "desconocido", "infrautilizado"]),
        "bajo_rendimiento": any(w in p for w in ["bajo rendimiento", "rinde poco", "no rinde",
                                                  "por debajo de lo esperado", "decepcionante",
                                                  "titulares que rinden", "mal rendimiento",
                                                  "rindiendo por debajo"]),
        "score_ml":       any(w in p for w in ["score", " ml ", "nivel ml", "rendimiento ml",
                                               "mejor jugador ml", "top jugador", "score ml",
                                               "puntuacion ml", "puntuación ml"]),
        "goleadores":     any(w in p for w in ["goleador", "maximo goleador", "máximo goleador",
                                               "más goles", "mas goles", "top goleador"]),
        "goleada":        any(w in p for w in ["goleada", "mayor goleada", "paliza", "aplastante", "abultado"]),
        "h2h":            any(w in p for w in ["resultado del partido", "último partido", "ultimo partido",
                                                "quien ganó", "quién ganó", "quien gano",
                                                "partido entre", "resultado entre",
                                                "enfrentamiento", "cuanto fue", "cuánto fue"]),
        "goles_equipo":   (any(w in p for w in [
                               "equipo goleador", "mas anotaciones", "más anotaciones",
                               "equipo anoto mas", "equipo anotó más",
                           ]) or ("equipo" in p and any(w in p for w in ["mas goles", "más goles",
                                                                          "goles marcados", "más anotó",
                                                                          "mas anoto"]))),
        "posiciones":     any(w in p for w in ["tabla", "posicion", "posición", "clasificacion", "lider", "líder", "primero"]),
        "eficiencia":     any(w in p for w in ["eficiencia", "historico", "histórico",
                                               "históricamente", "historicamente",
                                               "puntos por partido", "ppp", "mejor equipo historico",
                                               "dominado", "ha dominado", "dominio", "domina",
                                               "consistencia", "consistente"]),
        "carrera_titulo": any(w in p for w in ["carrera", "titulo historico", "título histórico", "top equipos", "mejores equipos"]),
        "evolucion":      any(w in p for w in [
                              "evolución", "evolucion", "tendencia", "por temporada", "por año",
                              "histórico de puntos", "historico de puntos",
                              "rendimiento historico", "rendimiento histórico",
                              "grafico lineal", "gráfico lineal", "lineal",
                              "todos los años", "a lo largo", "año a año",
                              "desde el año", "desde que", "últimos años", "ultimos años",
                          ]),
        "partidos":       any(w in p for w in ["partido", "resultado", "jugó contra", "marcador"])
                          or ("vs" in p and not _detectar_vs(pregunta)),
        "gep":            any(w in p for w in ["ganados", "empatados", "perdidos", "g/e/p", "victorias empates"]),
        "precision_tiro": any(w in p for w in ["precisión", "precision", "tiro", "disparo", "puerta", "efectividad ofensiva"]),
        "vs":             _detectar_vs(pregunta) is not None,
        "jugador":        any(w in p for w in ["goles", "asistencia", "minutos", "jugó", "jugo", "amarilla", "tarjeta"]),
        "equipo":         any(eq in p for eq in _get_equipos_conocidos()),
    }


def _detectar_tipo_tabla(pregunta: str) -> str:
    p = pregunta.lower()
    if "clausura" in p:
        return "Clausura"
    if "apertura" in p:
        return "Apertura"
    if "fase 1" in p:
        return "Fase 1"
    if "fase 2" in p:
        return "Fase 2"
    return "Acumulado"


def _fuzzy_equipo(texto: str) -> str | None:
    """Devuelve el equipo conocido más parecido usando Jaccard de bigramas.
    Umbral 0.55 — cubre typos como 'aliza lima' → 'alianza lima'.
    """
    equipos = _get_equipos_conocidos()
    t = texto.lower().strip()
    if not t or len(t) < 3:
        return None
    # 1) Exact
    if t in equipos:
        return t
    # 2) Substring directo
    for eq in sorted(equipos, key=len, reverse=True):
        if t in eq or eq in t:
            return eq
    # 3) Jaccard bigramas
    def bigrams(s: str):
        s = s.replace(" ", "_")
        return set(s[i:i+2] for i in range(len(s) - 1))
    bg_t = bigrams(t)
    if not bg_t:
        return None
    best, best_score = None, 0.55
    for eq in equipos:
        if len(eq) < 4:
            continue
        bg_e = bigrams(eq)
        union = bg_t | bg_e
        if not union:
            continue
        j = len(bg_t & bg_e) / len(union)
        if j > best_score:
            best_score, best = j, eq
    return best


def _extraer_equipo(pregunta: str) -> str | None:
    p = pregunta.lower()
    equipos = sorted(_get_equipos_conocidos(), key=len, reverse=True)
    return next((eq for eq in equipos if eq in p), None)


def _extraer_equipos_multiples(pregunta: str) -> list[str]:
    """Extrae todos los equipos mencionados en la pregunta (para evolución multi-equipo)."""
    p = pregunta.lower()
    equipos = sorted(_get_equipos_conocidos(), key=len, reverse=True)
    found = []
    p_rem = p
    for eq in equipos:
        if len(eq) < 4:
            continue
        if eq in p_rem:
            found.append(eq)
            p_rem = p_rem.replace(eq, " ", 1)
    return found


def construir_contexto(pregunta: str) -> tuple[str, pd.DataFrame | None, str | None]:
    """
    Detecta intención y consulta las tablas relevantes.
    Retorna (contexto_texto, dataframe, chart_type).
    chart_type: 'bar' | 'line' | 'radar' | 'scatter' | 'xi' | None
    """
    temporada  = _detectar_temporada(pregunta)
    rango      = _detectar_rango_años(pregunta)
    intent     = _detectar_entidad(pregunta)
    equipo     = _extraer_equipo(pregunta)
    tipo_tabla = _detectar_tipo_tabla(pregunta)
    vs_par     = _detectar_vs(pregunta)
    formacion  = _detectar_formacion(pregunta)
    estilo     = _detectar_estilo(pregunta)
    df         = None
    chart_type = None

    try:
        # ── H2H: último partido o resultado entre dos equipos ─────────────────
        if intent["h2h"] and vs_par:
            e1, e2 = vs_par
            df = get_partido_rival(e1, e2, temporada)
            contexto = f"Historial de enfrentamientos directos {e1} vs {e2}:\n{df.to_string(index=False)}"

        # ── Goles por equipo ──────────────────────────────────────────────────
        elif intent["goles_equipo"]:
            df = get_goles_por_equipo(temporada)
            contexto = f"Goles marcados por equipo Liga 1:\n{df.to_string(index=False)}"
            chart_type = "bar"

        # ── VS (comparativa estadísticas) ─────────────────────────────────────
        elif intent["vs"] and vs_par:
            e1, e2 = vs_par
            equipos_conocidos = _get_equipos_conocidos()
            if intent["entrenadores"]:
                # VS entrenadores (contiene "entrenador", "técnico", "dt")
                df = get_vs_entrenadores(e1, e2)
                contexto = f"Comparativa entrenadores {e1} vs {e2}:\n{df.to_string(index=False)}"
                chart_type = "bar_vs"
            elif any(e in equipos_conocidos for e in [e1, e2]):
                # SIEMPRE: estadísticas agregadas para el gráfico comparativo
                df = get_vs_equipos(e1, e2, temporada)
                contexto = f"Comparativa estadísticas {e1} vs {e2}:\n{df.to_string(index=False)}"
                chart_type = "bar_vs"
                # Estadísticas de detalle (pases, duelos) — disponibles desde 2022
                df_det = get_vs_detalle_estadisticas(e1, e2, temporada)
                if not df_det.empty:
                    contexto += (
                        f"\n\nEstadísticas de detalle (pases, duelos) "
                        f"— basado en partidos con datos disponibles:\n"
                        f"{df_det.to_string(index=False)}"
                    )
                # Si hay año → TAMBIÉN añadir los encuentros individuales con fecha y resultado
                if temporada:
                    df_h2h = get_partido_rival(e1, e2, temporada)
                    if not df_h2h.empty:
                        contexto += (
                            f"\n\nEncuentros directos {e1} vs {e2} en {temporada} "
                            f"(fecha, resultado, local/visitante):\n{df_h2h.to_string(index=False)}"
                        )
            else:
                # VS jugadores: queries individuales, luego filtrar temporadas comunes
                df1 = get_estadisticas_jugador(e1, temporada)
                df2 = get_estadisticas_jugador(e2, temporada)
                # Solo temporadas en que ambos coincidieron (si no hay año explícito)
                if not df1.empty and not df2.empty and not temporada:
                    t1 = set(df1["temporada"].unique())
                    t2 = set(df2["temporada"].unique())
                    comunes = t1 & t2
                    if comunes:
                        df1 = df1[df1["temporada"].isin(comunes)]
                        df2 = df2[df2["temporada"].isin(comunes)]
                df = pd.concat([df1, df2], ignore_index=True)
                if df.empty:
                    df = get_vs_jugadores(e1, e2, temporada)

                # ── Fallback automático a entrenadores si sigue vacío ──────────
                # Cubre "Juan Reynoso vs Guillermo Salas" sin keyword "entrenador"
                if df.empty:
                    df_dt = get_vs_entrenadores(e1, e2)
                    if not df_dt.empty:
                        df = df_dt
                        contexto = (
                            f"Comparativa entrenadores {e1} vs {e2} — "
                            f"historial en Liga 1 (PPP, partidos, temporadas):\n"
                            f"{df.to_string(index=False)}"
                        )
                        chart_type = "bar_vs"
                    else:
                        contexto = (
                            f"No se encontraron datos de jugadores ni entrenadores "
                            f"para {e1} y {e2} en Liga 1."
                        )
                        chart_type = None
                else:
                    contexto = (
                        f"ESTADÍSTICAS INDIVIDUALES DE JUGADORES — comparativa {e1} vs {e2}.\n"
                        f"Estos son datos de JUGADORES (no equipos): goles, asistencias, minutos, ppp.\n"
                        f"Solo se muestran temporadas en que ambos jugadores coincidieron en Liga 1.\n"
                        f"Compara directamente los valores de cada jugador por temporada.\n"
                        f"{df.to_string(index=False)}"
                    )
                    chart_type = "radar"

        # ── Campeones ─────────────────────────────────────────────────────────
        elif intent["campeones"]:
            df = get_campeones()
            contexto = f"Campeones históricos Liga 1:\n{df.to_string(index=False)}"

        # ── Entrenadores ──────────────────────────────────────────────────────
        elif intent["entrenadores"]:
            df = get_entrenadores(equipo, temporada)
            contexto = f"Entrenadores Liga 1 (ordenados por PPP):\n{df.to_string(index=False)}"
            chart_type = "bar"

        # ── XI Ideal ──────────────────────────────────────────────────────────
        elif intent["mejor_xi"]:
            id_form = _resolver_id_formacion(formacion, estilo, temporada)
            df = get_mejor_xi(temporada, id_form)
            # Buscar la descripción real de la formación seleccionada
            form_display = f"{formacion or '4-3-3'}"
            if _formaciones_cache:
                for row in _formaciones_cache:
                    if int(row["id_formacion"]) == id_form:
                        form_display = str(row.get("formacion_display") or
                                           f"{row.get('nombre_formacion','?')} - {row.get('nombre_estilo','?')}")
                        break
            contexto = (f"XI Ideal Liga 1 según score ML — formación: {form_display}\n"
                        f"Formaciones disponibles: 4-3-3 Ofensivo, 3-4-3 Ofensivo, "
                        f"4-2-3-1 Equilibrado, 4-4-2 Equilibrado, 4-3-3 Equilibrado, "
                        f"5-3-2 Defensivo, 5-4-1 Defensivo, 4-1-4-1 Defensivo\n"
                        f"{df.to_string(index=False)}")
            chart_type = "xi"

        # ── Gemas ocultas ─────────────────────────────────────────────────────
        elif intent["gemas_ocultas"]:
            df = get_gemas_ocultas(temporada)
            contexto = (
                f"Gemas ocultas Liga 1 — jugadores con alto score ML (≥65) "
                f"pero poca exposición (≤12 partidos):\n{df.to_string(index=False)}"
            )
            chart_type = "bar"

        # ── Jugadores bajo rendimiento ────────────────────────────────────────
        elif intent["bajo_rendimiento"]:
            df = get_jugadores_bajo_rendimiento(temporada)
            contexto = (
                f"Jugadores con alta participación (≥15 partidos) pero bajo score ML (<50) "
                f"— rindiendo por debajo de lo esperado en Liga 1:\n{df.to_string(index=False)}"
            )
            chart_type = "bar"

        # ── Score ML evolución de jugador ─────────────────────────────────────
        elif intent["score_ml"] and any(
            w in pregunta.lower() for w in ["evolución", "evolucion", "por año", "historial de",
                                             "a lo largo", "cada año", "todos los años"]
        ):
            # Extraer nombre: eliminar GREEDILY todo hasta el ÚLTIMO "de/del" antes del nombre
            # Ej: "Evolución del score de Alex Valera" → "Alex Valera"
            nombre = re.sub(r'^.*\bde[l]?\s+', '', pregunta, flags=re.IGNORECASE).strip().rstrip("?")
            df = get_score_ml_evolucion(nombre)
            contexto = f"Evolución score ML de {nombre}:\n{df.to_string(index=False)}"
            chart_type = "line"

        elif intent["score_ml"]:
            posicion = _detectar_posicion(pregunta)
            df = get_score_ml(temporada, posicion)
            pos_label = f" — posición: {posicion}s" if posicion else ""
            contexto = f"Score ML de jugadores Liga 1{pos_label}:\n{df.to_string(index=False)}"
            chart_type = "bar"

        # ── Valoración liga (evolución) ────────────────────────────────────────
        elif intent["valoracion"] and not equipo and intent["evolucion"]:
            df = get_evolucion_valor_liga()
            contexto = f"Evolución del valor total de la Liga 1:\n{df.to_string(index=False)}"
            chart_type = "line"

        # ── Valoración por equipo (con rango o single) ───────────────────────
        elif intent["valoracion"]:
            año_ini, año_fin = rango if rango else (None, None)
            df = get_valoracion_equipos(
                temporada=temporada if not rango else None,
                nombre_equipo=equipo,
                año_inicio=año_ini,
                año_fin=año_fin,
            )
            contexto = f"Valor de mercado Liga 1:\n{df.to_string(index=False)}"
            chart_type = "line" if (equipo and rango) else "bar"

        # ── Plantilla ─────────────────────────────────────────────────────────
        elif intent["plantilla"] and equipo:
            df = get_plantilla_equipo(equipo, temporada)
            contexto = f"Plantel de {equipo}:\n{df.to_string(index=False)}"

        # ── Perfil jugador ────────────────────────────────────────────────────
        elif intent["perfil"] and equipo is None:
            nombre_jugador = pregunta.split("de ")[-1].strip().rstrip("?")
            df = get_perfil_jugador(nombre_jugador, temporada)
            contexto = f"Perfil del jugador:\n{df.to_string(index=False)}"

        # ── Mayor goleada ─────────────────────────────────────────────────────
        elif intent["goleada"]:
            df = get_mayor_goleada(temporada)
            contexto = f"Mayores goleadas Liga 1:\n{df.to_string(index=False)}"

        # ── Goleadores ────────────────────────────────────────────────────────
        elif intent["goleadores"]:
            df = get_top_goleadores(temporada)
            contexto = f"Top goleadores Liga 1:\n{df.to_string(index=False)}"
            chart_type = "bar"

        # ── Eficiencia histórica (PPP histórico) ──────────────────────────────
        elif intent["eficiencia"]:
            df = get_eficiencia_historica(equipo)
            contexto = f"Eficiencia histórica (PPP) Liga 1:\n{df.to_string(index=False)}"
            chart_type = "bar"

        # ── Carrera por el título ─────────────────────────────────────────────
        elif intent["carrera_titulo"]:
            df = get_carrera_titulo()
            contexto = f"Carrera por el título — top equipos históricos Liga 1:\n{df.to_string(index=False)}"
            chart_type = "line"

        # ── Evolución de puntos (uno o múltiples equipos) ─────────────────────
        # Se activa con keyword de evolución O con rango de años + equipo detectado
        elif (intent["evolucion"] or rango) and equipo and not intent["valoracion"]:
            equipos_lista = _extraer_equipos_multiples(pregunta)
            if len(equipos_lista) > 1:
                dfs = []
                for eq in equipos_lista:
                    df_eq = get_evolucion_puntos_equipo(eq, tipo_tabla)
                    if not df_eq.empty:
                        dfs.append(df_eq)
                df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
                contexto = f"Evolución de {', '.join(equipos_lista)} por temporada:\n{df.to_string(index=False)}"
            else:
                df = get_evolucion_puntos_equipo(equipo, tipo_tabla)
                contexto = f"Evolución de {equipo} por temporada:\n{df.to_string(index=False)}"
            chart_type = "line"

        # ── G/E/P de un equipo ────────────────────────────────────────────────
        elif intent["gep"] and equipo:
            df = get_gep_equipo(equipo, temporada)
            contexto = f"Victorias, empates y derrotas acumuladas de {equipo}:\n{df.to_string(index=False)}"

        # ── Precisión de tiro ─────────────────────────────────────────────────
        elif intent["precision_tiro"]:
            df = get_precision_tiro(temporada, equipo)
            contexto = f"Precisión de tiro Liga 1:\n{df.to_string(index=False)}"
            chart_type = "bar"

        # ── Partidos de un equipo ─────────────────────────────────────────────
        elif intent["partidos"] and equipo:
            df = get_partidos_equipo(equipo, temporada)
            contexto = f"Partidos de {equipo}:\n{df.to_string(index=False)}"

        # ── Rendimiento de un equipo ──────────────────────────────────────────
        elif intent["equipo"] and equipo:
            df = get_rendimiento_equipo(equipo, temporada)
            contexto = f"Rendimiento de {equipo}:\n{df.to_string(index=False)}"

        # ── Tabla de posiciones ───────────────────────────────────────────────
        elif intent["posiciones"]:
            df = get_tabla_posiciones(temporada, tipo_tabla)
            contexto = f"Tabla {tipo_tabla} Liga 1 (temporada más reciente):\n{df.to_string(index=False)}"
            chart_type = "bar"

        # ── Default ───────────────────────────────────────────────────────────
        else:
            df = get_tabla_posiciones(tipo_tabla="Acumulado")
            contexto = f"Tabla de posiciones Liga 1 (más reciente):\n{df.to_string(index=False)}"
            chart_type = "bar"

    except Exception as e:
        contexto = f"[Error al consultar datos: {e}]"

    return contexto, df, chart_type


# ─── Pipeline RAG ─────────────────────────────────────────────────────────────

AÑO_ACTUAL_STR = str(AÑO_ACTUAL)

SYSTEM_PROMPT = (
    "Eres un asistente experto en Liga 1 Perú (fútbol peruano).\n"
    f"El año actual es {AÑO_ACTUAL}. \"Este año\" y \"año actual\" siempre significan {AÑO_ACTUAL}.\n"
    "Respondes SOLO con los datos del contexto. Si algo no está, dilo claramente.\n\n"
    "Reglas de respuesta:\n"
    "- Responde de forma NARRATIVA en español. Nunca listes siglas crudas.\n"
    "- Usa nombres completos: \"partidos jugados\" no PJ, \"puntos por partido\" no PPP,\n"
    "  \"goles a favor\" no GF, \"diferencia de goles\" no DG.\n"
    "- Para rankings: identifica al líder claramente y explica con los datos.\n"
    "- Para entrenadores: el mejor se mide por PPP (puntos por partido, mín. 5 partidos).\n"
    "- Para goleadas: menciona el marcador (campo \"resultado\"), el ganador y el rival.\n"
    "- Para evolución / tendencia: describe el patrón (mejora, caída, estabilidad).\n"
    "- Para VS: compara las métricas clave de forma directa y concisa.\n"
    "- Para gemas ocultas: destaca el contraste entre alto score y poca exposición.\n"
    "- Para bajo rendimiento: menciona los partidos jugados y el score bajo como indicadores.\n"
    "- Para pases y duelos: aclarar que la cobertura es parcial (mejor en 2025-2026).\n"
    "- Máximo 8 oraciones."
)


def responder(pregunta: str) -> tuple[str, pd.DataFrame | None, str | None]:
    contexto, df, chart_type = construir_contexto(pregunta)
    client = get_openai_client()
    response = client.chat.completions.create(
        model=AOAI_DEPLOYMENT,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": f"Contexto:\n{contexto}\n\nPregunta: {pregunta}"},
        ],
        temperature=0.2,
        max_completion_tokens=500,
    )
    return response.choices[0].message.content, df, chart_type


def get_openai_client() -> AzureOpenAI:
    return AzureOpenAI(
        azure_endpoint=AOAI_ENDPOINT,
        api_key=AOAI_API_KEY,
        api_version=AOAI_API_VERSION,
    )
