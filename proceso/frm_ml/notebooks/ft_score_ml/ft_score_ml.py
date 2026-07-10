"""
ft_score_ml.py
==============
Logica ML para la capa ft_score_ml.
Recibe DataFrames pandas + config leida del YAML, devuelve Spark DataFrame casteado listo para Delta.

Algoritmos:
  - PCA (n_components=1 por posicion)  → score_ml continuo → normalizado a score_100 (0-100)
  - K-means (k=4 por posicion)         → nivel: Elite / Bueno / Regular / Suplente

La configuracion (features, ref_signo, n_clusters, umbral_minutos) viene del YAML
ft_score_ml.yml — NO se duplica aqui.

Arquitectura igual que DDV: el .py devuelve Spark DataFrame con cast_dataframe_schema aplicado.
El notebook solo llama la funcion y escribe en Delta.

Autor: Oscar Garcia Del Aguila
Proyecto: Liga 1 Peru
"""

import os
os.environ.setdefault('OPENBLAS_NUM_THREADS', '1')
os.environ.setdefault('MKL_NUM_THREADS',      '1')
os.environ.setdefault('OMP_NUM_THREADS',      '1')

import warnings
warnings.filterwarnings('ignore', message='.*KMeans is known to have a memory leak.*')

import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans
import hashlib
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from utils_liga1 import cast_dataframe_schema

ETIQUETAS = {0: 'Suplente', 1: 'Regular', 2: 'Bueno', 3: 'Elite'}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _gen_id(row) -> str:
    """PK determinista: hash(id_jugador + id_equipo + temporada)."""
    clave = f"{row['id_jugador']}_{row['id_equipo']}_{row['temporada']}"
    return hashlib.md5(clave.encode()).hexdigest()


def _p90(serie: pd.Series, minutos: pd.Series) -> pd.Series:
    """Normaliza a estadística por 90 minutos. Evita división por cero."""
    return np.where(minutos > 0, serie / minutos * 90, 0.0)


# ---------------------------------------------------------------------------
# Paso 1: enriquecer y computar p90
# ---------------------------------------------------------------------------
def preparar_features(
    df_stats: pd.DataFrame,
    df_jugadores: pd.DataFrame,
    umbral_minutos: int,
) -> pd.DataFrame:
    """
    Une estadísticas con ft_plantillas_historico y genera variables p90 y binarias.

    df_stats       : ft_estadisticas_jugadores (todos los años)
    df_jugadores   : ft_plantillas_historico (id_jugador, temporada, pie, altura, posicion)
    umbral_minutos : mínimo de minutos jugados (viene del YAML)
    """
    df = df_stats[df_stats['minutos_jugados'] >= umbral_minutos].copy()

    # Join por id_jugador + temporada (granularidad de ft_plantillas_historico)
    join_keys = [k for k in ['id_jugador', 'temporada'] if k in df_jugadores.columns]
    cols_jug  = join_keys + [c for c in ['pie', 'altura', 'posicion'] if c in df_jugadores.columns]
    df_jug_dedup = df_jugadores[cols_jug].drop_duplicates(subset=join_keys)
    df = df.merge(df_jug_dedup, on=join_keys, how='left', suffixes=('', '_pl'))

    # Stats p90
    mins = df['minutos_jugados']
    df['goles_p90']         = _p90(df.get('goles', 0),                                      mins)
    df['asistencias_p90']   = _p90(df.get('asistencias', 0),                                mins)
    df['participacion_p90'] = _p90(df.get('goles', 0) + df.get('asistencias', 0),           mins)
    df['amarillas_p90']     = -_p90(df.get('tarjetas_amarillas', 0),                        mins)  # menos = mejor
    df['rojas_p90']         = -_p90(df.get('tarjetas_rojas', 0),                            mins)
    df['segunda_am_p90']    = -_p90(df.get('tarjeta_amarilla_roja', 0),                     mins)
    df['gc_p90']            = -_p90(df.get('goles_en_contra', 0),                           mins)  # porteros
    df['pi_p90']            =  _p90(df.get('partidos_imbatido', 0),                         mins)  # porteros

    # pie_zurdo binario
    if 'pie' in df.columns:
        df['pie_zurdo'] = df['pie'].str.lower().isin(['left', 'zurdo', 'izquierdo']).astype(int)
    else:
        df['pie_zurdo'] = 0

    # altura: fillna con mediana
    if 'altura' in df.columns:
        df['altura'] = pd.to_numeric(df['altura'], errors='coerce')
        df['altura'] = df['altura'].fillna(df['altura'].median())
    else:
        df['altura'] = 0

    # posicion_xi desde ft_plantillas_historico (la vista DDV la calcula igual)
    posicion_map = {
        'Portero': 'POR',
        'Defensa Central': 'DFC', 'Defensa': 'DFC',
        'Lateral Derecho': 'LD', 'Lateral Izquierdo': 'LI',
        'Mediocampista Defensivo': 'MCD',
        'Mediocampista Central': 'MC', 'Mediocampista': 'MC',
        'Interior Izquierdo': 'MC', 'Mediocampista Izquierdo': 'MC',
        'Mediocampista Derecho': 'MC',
        'Mediocampista Ofensivo': 'MCO',
        'Extremo Izquierdo': 'EI', 'Extremo Derecho': 'ED',
        'Delantero Centro': 'DC',
    }
    if 'posicion_xi' not in df.columns:
        df['posicion_xi'] = df['posicion'].map(posicion_map).fillna('OTRO')

    return df


# ---------------------------------------------------------------------------
# Paso 2: PCA por posición → score_ml + score_100 + var_explicada
# ---------------------------------------------------------------------------
def calcular_pca(
    df: pd.DataFrame,
    features_por_posicion: dict,
    ref_signo: dict,
) -> pd.DataFrame:
    """
    Aplica PCA (n_components=1) por posición sobre TODOS los años.
    features_por_posicion y ref_signo vienen del YAML.
    """
    resultados = []
    for posicion, feats in features_por_posicion.items():
        grupo = df[df['posicion_xi'] == posicion].copy().reset_index(drop=True)
        if len(grupo) < 2:
            continue

        feats_ok = [f for f in feats if f in grupo.columns]
        if not feats_ok:
            continue

        X = StandardScaler().fit_transform(grupo[feats_ok].fillna(0))
        pca = PCA(n_components=1)
        scores = pca.fit_transform(X).flatten()

        # Corrección de signo
        ref = ref_signo.get(posicion, feats_ok[0])
        if ref in grupo.columns:
            if np.corrcoef(scores, grupo[ref].fillna(0))[0, 1] < 0:
                scores = -scores

        grupo['score_ml']      = scores
        grupo['var_explicada'] = pca.explained_variance_ratio_[0]
        resultados.append(grupo)

    if not resultados:
        df['score_ml'] = df['var_explicada'] = np.nan
        return df

    df_out = pd.concat(resultados, ignore_index=True)

    # score_100: normalizar 0-100 dentro de cada posición
    def _norm100(g):
        mn, mx = g['score_ml'].min(), g['score_ml'].max()
        g = g.copy()
        g['score_100'] = (g['score_ml'] - mn) / (mx - mn) * 100 if mx > mn else 50.0
        return g

    return df_out.groupby('posicion_xi', group_keys=False).apply(_norm100)


# ---------------------------------------------------------------------------
# Paso 3: K-means por posición → nivel_num + nivel
# ---------------------------------------------------------------------------
def calcular_kmeans(
    df: pd.DataFrame,
    features_por_posicion: dict,
    n_clusters: int,
) -> pd.DataFrame:
    """
    Aplica K-means por posición.
    features_por_posicion y n_clusters vienen del YAML.
    Requiere score_ml calculado previamente.
    """
    resultados = []
    for posicion, feats in features_por_posicion.items():
        grupo = df[df['posicion_xi'] == posicion].copy().reset_index(drop=True)
        if len(grupo) < n_clusters * 2:
            continue

        feats_ok = [f for f in feats if f in grupo.columns]
        if not feats_ok:
            continue

        X = StandardScaler().fit_transform(grupo[feats_ok].fillna(0))
        km = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
        grupo['cluster_raw'] = km.fit_predict(X)

        # Reordenar clusters por score_ml promedio → etiquetas consistentes
        orden = (grupo.groupby('cluster_raw')['score_ml']
                 .mean().sort_values()
                 .reset_index().reset_index()
                 .rename(columns={'index': 'nivel'}))
        mapa = dict(zip(orden['cluster_raw'], orden['nivel']))

        grupo['nivel_num'] = grupo['cluster_raw'].map(mapa)
        grupo['nivel']     = grupo['nivel_num'].map(ETIQUETAS)
        resultados.append(grupo[['id_jugador', 'temporada', 'id_equipo', 'nivel_num', 'nivel']])

    if not resultados:
        df['nivel_num'] = df['nivel'] = np.nan
        return df

    df_niveles = pd.concat(resultados, ignore_index=True)
    return df.merge(df_niveles, on=['id_jugador', 'temporada', 'id_equipo'], how='left')


# ---------------------------------------------------------------------------
# Función principal
# ---------------------------------------------------------------------------
def calcular_ft_score_ml(
    spark: SparkSession,
    df_stats: pd.DataFrame,
    df_jugadores: pd.DataFrame,
    features_por_posicion: dict,
    ref_signo: dict,
    n_clusters: int,
    umbral_minutos: int,
    schema: dict,
) -> DataFrame:
    """
    Funcion principal. El notebook lee el YAML y pasa los parametros ya extraidos.
    Devuelve Spark DataFrame con cast_dataframe_schema aplicado — listo para write_delta_udv.
    Igual que las DDV: el .py devuelve el DataFrame final, el notebook solo escribe.

    Parametros
    ----------
    spark                 : SparkSession activa
    df_stats              : ft_estadisticas_jugadores en pandas (todos los anos)
    df_jugadores          : ft_plantillas_historico en pandas (pie, altura, posicion)
    features_por_posicion : dict posicion → lista de features (del YAML)
    ref_signo             : dict posicion → variable referencia signo PCA (del YAML)
    n_clusters            : numero de clusters K-means (del YAML)
    umbral_minutos        : minutos minimos para incluir jugador (del YAML)
    schema                : dict col → tipo Spark para el DataFrame de salida (del YAML)
    """
    # 1. Features + p90
    df_ml = preparar_features(df_stats, df_jugadores, umbral_minutos)

    # 2. PCA → score_ml + score_100
    df_ml = calcular_pca(df_ml, features_por_posicion, ref_signo)

    # 3. K-means → nivel
    df_ml = calcular_kmeans(df_ml, features_por_posicion, n_clusters)

    # 4. Metadatos
    df_ml['id_score_ml'] = df_ml.apply(_gen_id, axis=1)
    df_ml['fecha_carga'] = datetime.utcnow()
    df_ml['periodo']     = df_ml['temporada'].astype(int)

    # 5. Seleccionar columnas del schema antes de convertir a Spark (reduce transferencia)
    cols_ok = [c for c in schema if c in df_ml.columns]
    df_out  = df_ml[cols_ok]

    # 6. Convertir a Spark y castear tipos exactos del DDL — igual que DDV
    return cast_dataframe_schema(spark.createDataFrame(df_out), schema)
