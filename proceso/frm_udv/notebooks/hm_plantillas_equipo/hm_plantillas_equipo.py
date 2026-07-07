# ==========================================================
# FUNCIONES UDV - MD_PLANTILLAS
#                 HM_PLANTILLAS_EQUIPO
# Proyecto: Liga 1 Perú
# Autor: Oscar García Del Águila
# ==========================================================


import re
import requests

from pyspark.sql import Window, DataFrame
from pyspark.sql.functions import (
    first as spark_first, row_number, col, lit, when,
    regexp_extract, regexp_replace, to_date, split, element_at,
    trim, sha2, concat_ws, lower, initcap, current_timestamp, date_format,
    months_between, floor, current_date, translate, coalesce
)
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
from utils_liga1 import cast_dataframe_schema, rename_columns, build_case_when, norm_col, limpiar_fecha_nacimiento

# ----------------------------------------------------------
# CONSTANTES foto_url
# ----------------------------------------------------------
_DEFAULT_FOTO_URL = "https://img.a.transfermarkt.technology/portrait/header/default.jpg?lm=1"
_FOTO_HEADERS     = {"User-Agent": "Mozilla/5.0 (compatible; Liga1Bot/1.0)"}
_FOTO_TIMEOUT     = 6   # segundos por HEAD request


def _swap_ext_case(url: str) -> str:
    """Invierte el case de la extension de imagen (.jpg<>.JPG, .jpeg<>.JPEG, .png<>.PNG)."""
    def _toggle(m):
        ext = m.group(1)
        return "." + (ext.upper() if ext == ext.lower() else ext.lower()) + "?"
    return re.sub(r"\.(jpg|jpeg|png|JPG|JPEG|PNG)\?", _toggle, url)


def _resolver_url(url: str) -> str:
    """
    Verifica si la foto_url responde 200 via HEAD request.
    - Si 200 -> retorna URL original.
    - Si 404 -> prueba extension con case alternativo (.jpg->.JPG o viceversa).
      - Si el alternativo da 200 -> retorna URL alternativa.
      - Si el alternativo da 404 -> retorna URL default.
    - Si otro error HTTP (403, 500) o excepcion de red -> retorna URL original
      (no podemos saber si existe; evitamos reemplazar por default incorrectamente).
    """
    if not url or not url.strip():
        return _DEFAULT_FOTO_URL
    if "default" in url:
        return url  # ya es la imagen por defecto, no validar

    try:
        r = requests.head(url, timeout=_FOTO_TIMEOUT,
                          allow_redirects=True, headers=_FOTO_HEADERS)
        if r.status_code == 200:
            return url

        if r.status_code == 404:
            url_alt = _swap_ext_case(url)
            if url_alt != url:
                r2 = requests.head(url_alt, timeout=_FOTO_TIMEOUT,
                                   allow_redirects=True, headers=_FOTO_HEADERS)
                if r2.status_code == 200:
                    return url_alt
                if r2.status_code == 404:
                    return _DEFAULT_FOTO_URL
            return _DEFAULT_FOTO_URL

        # 403, 429, 5xx -> no sabemos; conservar URL original
        return url

    except Exception:
        # Error de conexion/timeout -> conservar URL original
        return url


def normalizar_foto_url_batch(spark, df: DataFrame, col_name: str = "foto_url") -> DataFrame:
    """
    Valida y normaliza la columna foto_url de un DataFrame.

    Estrategia de resolucion por URL unica (driver-side, con cache):
      1. HEAD request a la URL original.
      2. Si 404 -> prueba con extension alternativa (.jpg<>.JPG / .jpeg<>.JPEG / .png<>.PNG).
      3. Si sigue 404 -> reemplaza con la imagen default de Transfermarkt.
      4. Si error de red / 403 / otro -> conserva URL original (safe fallback).

    Solo se hacen requests a las URLs unicas (no por cada fila), reduciendo
    el numero de llamadas HTTP al minimo.
    """
    unique_urls = [
        row[col_name]
        for row in df.select(col_name).distinct().collect()
    ]

    url_map = {url: _resolver_url(url) for url in unique_urls}

    url_map_bc = spark.sparkContext.broadcast(url_map)

    @udf(returnType=StringType())
    def _resolver_udf(url):
        return url_map_bc.value.get(url, _DEFAULT_FOTO_URL)

    return df.withColumn(col_name, _resolver_udf(col(col_name)))


_EMPTY_VALS = ["", "null", "na", "none", "0", "0.0", "0,0"]

def _null_if_empty(c):
    """Convierte vacios y placeholders a null para ignorenulls en window."""
    return when(col(c).isNull() | lower(trim(col(c))).isin(_EMPTY_VALS), lit(None)).otherwise(col(c))

def carga_final_md_plantillas(df_raw_plantillas, prm_cols_raw_plantillas_md, prm_case_rules, prm_rename_columns_md, prm_schema_md, df_nacionalidades=None):

    # ================================================================
    # 2) MEJOR VALOR POR JUGADOR: mas reciente no-nulo por columna
    # Particion por id_tm para manejar correctamente jugadores distintos
    # que comparten el mismo nombre.
    # Fallback a jugador cuando id_tm es null (jugadores sin TM).
    # ================================================================
    df_with_pk = (
        df_raw_plantillas
        .select(*prm_cols_raw_plantillas_md, col("temporada"))
        .withColumn("_pk", coalesce(col("id_tm"), col("jugador")))
    )

    window_desc = Window.partitionBy("_pk").orderBy(col("temporada").desc()) \
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    nullable_fields = list(prm_cols_raw_plantillas_md)  # incluye id_tm y jugador

    df_raw = (
        df_with_pk
        .select(
            col("_pk"),
            *[spark_first(_null_if_empty(f), ignorenulls=True).over(window_desc).alias(f)
              for f in nullable_fields]
        )
        .dropDuplicates(["_pk"])
        .drop("_pk")
    )

    # ================================================================
    # 3) LIMPIAR FECHA DE NACIMIENTO
    # ================================================================
    df_step1 = limpiar_fecha_nacimiento(df_raw, "fecha_nacimiento")

    df_step1 = df_step1.select(*[col(c) for c in df_step1.columns],
        floor(months_between(current_date(), col("fecha_nacimiento_limpia")) / 12).alias("edad")
    )

    # ================================================================
    # 4) NACIONALIDAD: principal + segunda nacionalidad
    # ================================================================
    df_step2 = (
        df_step1
        .select(
            *[col(c) for c in df_step1.columns],
            trim(element_at(split(col("nacionalidad"), ","), 1)).alias("nacionalidad_principal"),
            trim(element_at(split(col("nacionalidad"), ","), 2)).alias("segunda_nacionalidad")
        )
    )

    # ================================================================
    # 5) APLICAR REGLAS CASE-WHEN DEL YAML (altura, pie, valor_mercado)
    # ================================================================
    altura_expr        = build_case_when("altura",        "altura",        prm_case_rules).cast("double")
    valor_mercado_expr = build_case_when("valor_mercado", "valor_mercado", prm_case_rules)
    pie_expr           = build_case_when("pie",           "pie",           prm_case_rules)

    # ================================================================
    # 6) CONSTRUIR ID_JUGADOR y aplicar traduccion de nacionalidades
    # ================================================================
    df_final = (
        df_step2
        .select(
            sha2(coalesce(col("id_tm"), norm_col(col("jugador"))), 256).alias("id_jugador"),  # PK: sha2(id_tm) o sha2(nombre_norm) si id_tm es null
            col("id_tm"),
            col("jugador"),
            col("edad"),
            col("nacionalidad_principal"),
            col("segunda_nacionalidad"),
            altura_expr.alias("altura"),
            pie_expr.alias("pie"),
            valor_mercado_expr.cast("decimal(20,4)").alias("valor_mercado"),
            col("foto_url"),
            col("fuente"),
            current_timestamp().alias("fecha_carga"),
            date_format(current_timestamp(), "yyyyMMdd").alias("periododia")
        )
    )

    if df_nacionalidades is not None:
        df_nac_lookup = df_nacionalidades.select(
            lower(trim(col("nombre_raw"))).alias("__nac_raw"),
            col("nombre_es")
        )
        df_final = (
            df_final.alias("base")
            .join(df_nac_lookup.alias("__n1"),
                  lower(trim(col("base.nacionalidad_principal"))) == col("__n1.__nac_raw"), "left")
            .select(
                col("base.id_jugador"),
                col("base.id_tm"),
                col("base.jugador"),
                col("base.edad"),
                coalesce(col("__n1.nombre_es"), col("base.nacionalidad_principal")).alias("nacionalidad_principal"),
                col("base.segunda_nacionalidad"),
                col("base.altura"),
                col("base.pie"),
                col("base.valor_mercado"),
                col("base.foto_url"),
                col("base.fuente"),
                col("base.fecha_carga"),
                col("base.periododia")
            )
        )
        df_final = (
            df_final.alias("base")
            .join(df_nac_lookup.alias("__n2"),
                  lower(trim(col("base.segunda_nacionalidad"))) == col("__n2.__nac_raw"), "left")
            .select(
                col("base.id_jugador"),
                col("base.id_tm"),
                col("base.jugador"),
                col("base.edad"),
                col("base.nacionalidad_principal"),
                coalesce(col("__n2.nombre_es"), col("base.segunda_nacionalidad")).alias("segunda_nacionalidad"),
                col("base.altura"),
                col("base.pie"),
                col("base.valor_mercado"),
                col("base.foto_url"),
                col("base.fuente"),
                col("base.fecha_carga"),
                col("base.periododia")
            )
        )
    else:
        nac_expr     = build_case_when("nacionalidad_principal", "nacionalidad_principal", prm_case_rules)
        seg_nac_expr = build_case_when("segunda_nacionalidad",   "segunda_nacionalidad",   prm_case_rules)
        df_final = df_final.select(
            col("id_jugador"),
            col("id_tm"),
            col("jugador"),
            col("edad"),
            nac_expr,
            seg_nac_expr,
            col("altura"),
            col("pie"),
            col("valor_mercado"),
            col("foto_url"),
            col("fuente"),
            col("fecha_carga"),
            col("periododia")
        )

    # ================================================================
    # 7) RENOMBRAR Y CASTEAR SEGUN YAML
    # ================================================================
    df_rename = rename_columns(df_final, prm_rename_columns_md)
    df_cast = cast_dataframe_schema(df_rename, prm_schema_md)

    return df_cast


def carga_final_hm_plantillas(df_md_catalogo_equipos, prm_cols_catalogo_equipos_hm, df_raw_plantillas, prm_cols_raw_plantillas_hm, df_md_plantillas, prm_case_rules, prm_rename_columns_hm, prm_schema_hm, prm_dedup_cols_hm, df_posiciones=None):
    # 1) Catalogo equipos: normalizamos nombre_transfermarkt
    df_cat = (
        df_md_catalogo_equipos
        .select(*prm_cols_catalogo_equipos_hm)
        .select(
            *[col(c) for c in prm_cols_catalogo_equipos_hm],
            lower(trim(col("nombre_transfermarkt"))).alias("nombre_transfermarkt_lower")
        )
    )

    # 2) RDV plantillas -> dedup + club_lower
    df_raw_sel = df_raw_plantillas.select(*prm_cols_raw_plantillas_hm)

    df_raw_norm = (
        df_raw_sel
        .dropDuplicates(prm_dedup_cols_hm)
        .select(
            *[col(c) for c in prm_cols_raw_plantillas_hm],
            lower(trim(col("club"))).alias("club_lower")
        )
    )

    # 3) LEFT JOIN catalogo equipos vs RDV plantillas — captura sin match
    df_join_eq_left = (
        df_cat.alias("e")
        .join(
            df_raw_norm.alias("b"),
            col("e.nombre_transfermarkt_lower") == col("b.club_lower"),
            "left"
        )
    )

    df_sin_match_catalogo = (
        df_join_eq_left.filter(col("e.id_equipo").isNull())
        .select(*[col(f"b.{c}") for c in prm_cols_raw_plantillas_hm])
        .withColumn("razon_rechazo", lit("SIN_MATCH_CATALOGO"))
    )

    df_join_eq = df_join_eq_left.filter(col("e.id_equipo").isNotNull())

    # 3.4) DIM jugadores (md_plantillas) -> lookup id_jugador
    # Join por sha2(coalesce(id_tm, jugador)) para alinearse con la _pk
    # usada en carga_final_md_plantillas. Maneja jugadores con y sin id_tm.
    df_dim_jug = (
        df_md_plantillas
        .select("id_jugador", "id_tm", "jugador")
        .withColumn("_pk_dim", sha2(coalesce(col("id_tm"), norm_col(col("jugador"))), 256))
    )

    # 3.5) Dataset intermedio con alias 'p'
    df_join_tmp = df_join_eq.select(
        col("e.id_equipo"),
        col("b.club").alias("club"),
        col("b.temporada"),
        col("b.jugador"),
        norm_col(col("b.jugador")).alias("jugador_lower_raw"),
        col("b.numero"),
        col("b.posicion"),
        col("b.fecha_fichaje"),
        col("b.fuente"),
        col("b.id_tm")
    ).withColumn("_pk_raw", sha2(coalesce(col("id_tm"), col("jugador_lower_raw")), 256))

    # Left join por _pk (sha2 de id_tm o nombre si id_tm es null)
    df_join_all_left = (
        df_join_tmp.alias("p")
        .join(
            df_dim_jug.alias("d"),
            col("p._pk_raw") == col("d._pk_dim"),
            "left"
        )
    )

    df_sin_match_dimension = (
        df_join_all_left.filter(col("d.id_jugador").isNull())
        .select(*[col(f"p.{c}") for c in prm_cols_raw_plantillas_hm])
        .withColumn("razon_rechazo", lit("SIN_MATCH_DIMENSION"))
    )

    # Descartar filas donde el jugador no tiene match en md_plantillas
    df_join_all = df_join_all_left.filter(col("d.id_jugador").isNotNull())

    # 3.6) Reglas CASE-WHEN (desde YAML)
    numero_expr        = build_case_when("p.numero",        "numero_camiseta", prm_case_rules)
    fecha_fichaje_expr = build_case_when("p.fecha_fichaje", "fecha_fichaje",   prm_case_rules)

    # 3.6b) Posicion: lookup CSV si disponible, CASE-WHEN como fallback
    if df_posiciones is not None:
        df_pos_lookup = df_posiciones.select(
            lower(trim(col("nombre_raw"))).alias("__pos_raw"),
            col("nombre_es")
        )
        df_join_all = (
            df_join_all
            .join(df_pos_lookup,
                  lower(trim(col("p.posicion"))) == col("__pos_raw"), "left")
        )
        posicion_col = coalesce(col("nombre_es"), lit("sin informacion"))
    else:
        posicion_col = build_case_when("p.posicion", "posicion", prm_case_rules)

    # Materializar posicion normalizada para que SHA y columna almacenada sean consistentes
    df_join_all = df_join_all.withColumn("_pos_norm", posicion_col)

    # 3.7) SELECT final con id_plantilla_equipo
    df_hm = df_join_all.select(
        sha2(
            concat_ws(
                "-",
                col("p.id_equipo").cast("string"),
                col("d.id_jugador"),
                col("p.temporada").cast("string"),
                lower(trim(col("_pos_norm")))
            ),
            256
        ).alias("id_plantilla_equipo"),
        col("p.id_equipo"),
        col("d.id_jugador"),
        col("p.id_tm"),
        col("p.temporada").alias("temporada"),
        numero_expr.alias("numero_camiseta"),
        initcap(col("_pos_norm")).alias("posicion"),
        fecha_fichaje_expr.alias("fecha_fichaje"),
        col("p.fuente").alias("fuente"),
        current_timestamp().alias("fecha_carga"),
        col("p.temporada").alias("periodo")
    )

    # 3.8) Renombrar columnas
    df_rename_hm = rename_columns(df_hm, prm_rename_columns_hm)

    # 3.9) Castear schema final
    df_cast = cast_dataframe_schema(df_rename_hm, prm_schema_hm)

    # 3.10) Consolidar descartados (sin match catálogo + sin match dimensión jugadores)
    df_discarded = df_sin_match_catalogo.unionByName(df_sin_match_dimension, allowMissingColumns=True)

    return df_cast, df_discarded
