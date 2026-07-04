-- ==========================================================
-- DDV - FT_PLANTILLAS_HISTORICO_VW
-- Proyecto: Liga 1 Perú
-- ==========================================================

CREATE OR REPLACE VIEW ${catalog_name}.vw_ddv.ft_plantillas_historico_vw
AS

SELECT
    id_plantilla_equipo,
    id_equipo,
    nombre_equipo,
    alias_equipo,
    id_jugador,
    jugador,
    temporada,
    numero_camiseta,
    posicion,
    fecha_fichaje,

    edad AS edad_actual,

    edad - (
        YEAR(fecha_carga) - temporada
    ) AS edad_historica,

    nacionalidad_principal,
    segunda_nacionalidad,
    altura,
    pie,
    valor_mercado
FROM ${catalog_name}.tb_ddv.ft_plantillas_historico;
