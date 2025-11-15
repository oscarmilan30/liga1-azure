# ==========================================================
# FUNCIONES UDV - EQUIPOS
# Proyecto: Liga 1 Perú
# Autor: Oscar García Del Águila
# ==========================================================

from pyspark.sql.functions import col, current_timestamp, date_format,lower
from utils_liga1 import cast_dataframe_schema, rename_columns


def carga_final(df_catalogo_equipos, df_raw_equipos, prm_cols_catalogo_equipos, prm_cols_equipos, prm_drop_duplicates_equipos,prm_rename_columns, prm_schema):
    """
    Selecciona las columnas del YAML, agrega campos de control (fecha_carga, periododia)
    y castea/ordena según el schema definido.

    Args:
        df_catalogo_equipos: DataFrame leído desde UDV.
        df_raw_equipos: DataFrame leído desde RAW.
        prm_cols_catalogo_equipos: lista de columnas base (yaml_conf[entity]["prm_cols_catalogo_equipos"])
        prm_cols_equipos: lista de columnas base (yaml_conf[entity]["prm_cols_equipos"])
        prm_drop_duplicates_equipos: lista de columnas base para quitar duplicados (yaml_conf[entity]["prm_drop_duplicates_equipos"])
        prm_rename_columns: lista de columnas renombradas (yaml_conf[entity]["prm_rename_columns"])
        prm_schema: dict con columnas finales y tipos (yaml_conf[entity]["schema"])
    """
    
    df_catalogo_equipos_select=df_catalogo_equipos.select(*prm_cols_catalogo_equipos)

    df_raw_equipos_select = df_raw_equipos.select(*prm_cols_equipos)\
                                   .dropDuplicates(prm_drop_duplicates_equipos)\
                                   .select(*prm_cols_equipos,
                                           lower(col("equipo")).alias("equipo_lower")
                                           )
    
    df_join=df_catalogo_equipos_select.alias("a")\
                             .join(df_raw_equipos_select.alias("b"),col("a.nombre_fotmob")==col("b.equipo_lower"),"left")\
                            .select(col("a.id_equipo"),
                                    col("a.nombre_equipo"),
                                    col("a.alias"),
                                    col("b.equipo_lower"),
                                    col("b.url"),
                                    col("b.fuente"),
                                    current_timestamp().alias("fecha_carga"),
                                    date_format(current_timestamp(), "yyyyMMdd").alias("periododia")
                                    )

    df_rename = rename_columns(df_join,prm_rename_columns)
    
    df_cast = cast_dataframe_schema(df_rename,prm_schema)
    
    return df_cast                      










