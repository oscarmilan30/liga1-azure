# Catálogo de Entidades — Liga 1 Perú Data Platform

**Proyecto:** Liga 1 Perú — Data Engineering en Azure  
**Autor:** Oscar García Del Águila  
**Fecha:** Junio 2026

Diccionario de datos del modelo. Cubre las tres capas de datos: **Landing** (archivos fuente), **UDV** (tablas curadas e integradas) y **DDV** (data marts para Power BI).

---

## Tabla de Contenidos

- [Modelo de relaciones](#modelo-de-relaciones)
- [Landing — Archivos fuente](#landing--archivos-fuente)
- [UDV — Tablas Maestras (md_)](#udv--tablas-maestras-md_)
- [UDV — Tablas Históricas (hm_)](#udv--tablas-históricas-hm_)
- [DDV — Data Marts (ft_ / dm_)](#ddv--data-marts-ft--dm_)
- [Vistas DDV expuestas a Power BI](#vistas-ddv-expuestas-a-power-bi)

---

## Modelo de relaciones

```
Landing (JSON/CSV)
    │
    ▼  [curated_json / curated_csv]
  RDV (Parquet)
    │
    ▼  [notebooks UDV + YAML]
  UDV ─ Maestros ──────────────────────────────────────────────┐
    │                                                           │
    │  md_catalogo_equipos  ◄── HUB central de resolución      │
    │      └── md_equipos                                       │
    │      └── md_estadios                                      │
    │      └── md_plantillas      (generado por hm_plantillas)  │
    │      └── md_entrenadores    (generado por hm_entrenadores)│
    │                                                           │
  UDV ─ Históricos ◄─────────────────────────────────────────┘
    │      hm_partidos
    │      hm_estadisticas_partidos
    │      hm_plantillas_equipo
    │      hm_tablas_clasificacion
    │      hm_valoracion_equipos
    │      hm_entrenadores_equipo
    │      hm_campeones
    │      hm_estadisticas_jugadores
    │
    ▼  [notebooks DDV + YAML]
  DDV ─ Data Marts
         dm_equipos
         ft_rendimiento_temporada
         ft_rendimiento_acumulado
         ft_partidos_detalle
         ft_plantillas_historico
         ft_entrenadores_historico
         ft_evolucion_valoracion
         ft_estadisticas_jugadores
    │
    ▼  [vistas vw_ddv.*]
  Power BI (DirectQuery)
```

La clave del modelo es `md_catalogo_equipos`: resuelve que FotMob y Transfermarkt usan IDs distintos para los mismos equipos. Casi todas las entidades históricas dependen de él para cruzar datos entre fuentes.

---

## Landing — Archivos fuente

Generados por el scraping y depositados en `landing/archivos_scraping/{año}/`. Un archivo por año por entidad.

### FotMob — JSON

| Archivo | Entidad destino | Descripción |
|---|---|---|
| `equipos_{año}.json` | `hm_partidos`, `md_catalogo_equipos` | Lista de equipos participantes con ID FotMob y nombre |
| `partidos_{año}.json` | `hm_partidos` | Resultados de todos los partidos del año: fecha, equipos, marcador, torneo, jornada |
| `estadisticas_partidos_{año}.json` | `hm_estadisticas_partidos` | +80 métricas por equipo por partido: posesión, tiros, pases, duelos, tarjetas |
| `tablas_clasificacion_{año}.json` | `hm_tablas_clasificacion` | Posiciones por torneo (Apertura / Clausura / General) |
| `campeones_{año}.json` | `hm_campeones` | Campeón y subcampeón de la temporada |

Estructura JSON estándar:
```json
{ "fuente": "FotMob", "temporada": 2024, "fecha_carga": "...", "data": [...] }
```

### Transfermarkt — CSV (sep=`|`)

| Archivo | Entidad destino | Descripción |
|---|---|---|
| `liga1_{año}.csv` | `hm_valoracion_equipos`, `md_equipos` | Valor de mercado del plantel por equipo, edad promedio, cantidad de extranjeros |
| `plantillas_{año}.csv` | `hm_plantillas_equipo`, `md_plantillas` | Plantel completo: jugadores, posición, edad, valor de mercado, pie dominante |
| `estadios_{año}.csv` | `md_estadios` | Nombre, capacidad y ubicación de estadios |
| `entrenadores_{año}.csv` | `hm_entrenadores_equipo`, `md_entrenadores` | Historial de entrenadores por equipo desde 2020 |
| `estadisticas_jugadores_{año}.csv` | `hm_estadisticas_jugadores` | Estadísticas individuales por competencia: goles, asistencias, minutos, partidos, tarjetas, titularidades, entrada/salida como suplente, penaltis anotados; y para porteros: goles en contra y partidos imbatido |

> Transfermarkt usa `temporada X-1` para lo que en el proyecto se guarda como año `X`. El scraper aplica la conversión automáticamente.

---

## UDV — Tablas Maestras (md_)

Tablas de referencia estables. Se cargan en modo `MERGE` (actualiza registros existentes e inserta nuevos por clave de negocio). Almacenadas en `catalog_liga1.tb_udv` · ruta ADLS: `primera_division/udv/{entidad}/data/`.

---

### md_catalogo_equipos

**Rol:** Hub central de resolución de identidad entre fuentes. Mapea cada equipo entre FotMob y Transfermarkt.

**Fuente:** `catalogo_equipos` (data entry manual en CSV) · `curated_dataentry`

| Columna | Tipo | Descripción |
|---|---|---|
| `id_fm` | string | ID del equipo en FotMob |
| `nombre_fm` | string | Nombre del equipo en FotMob |
| `id_tm` | string | ID del equipo en Transfermarkt |
| `nombre_tm` | string | Nombre del equipo en Transfermarkt |
| `nombre_normalizado` | string | Nombre canónico del equipo (usado en todo el modelo) |
| `abreviatura` | string | Abreviatura de 3 letras (ej: `ALI`, `UNI`, `SPB`) |
| `activo` | integer | 1 = activo en Liga 1, 0 = ya no participa |
| `temporada` | integer | Año de la temporada |
| `fecha_carga` | timestamp | Timestamp de la última carga |

---

### md_equipos

**Rol:** Atributos actuales del equipo por temporada: valor de mercado, composición del plantel.

**Fuente:** `liga1_{año}.csv` (Transfermarkt) + `md_catalogo_equipos`

| Columna | Tipo | Descripción |
|---|---|---|
| `temporada` | integer | Año de la temporada |
| `id_equipo` | string | ID FotMob del equipo |
| `nombre_equipo` | string | Nombre normalizado |
| `valor_mercado_eur` | double | Valor total del plantel en EUR |
| `num_jugadores` | integer | Número de jugadores en el plantel |
| `edad_promedio` | double | Edad promedio del plantel |
| `extranjeros` | integer | Número de jugadores extranjeros |
| `fecha_carga` | timestamp | Timestamp de carga |

---

### md_estadios

**Rol:** Catálogo de recintos deportivos de los equipos de Liga 1.

**Fuente:** `estadios_{año}.csv` (Transfermarkt)

| Columna | Tipo | Descripción |
|---|---|---|
| `temporada` | integer | Año de la temporada |
| `nombre_equipo` | string | Equipo local del estadio |
| `nombre_estadio` | string | Nombre oficial del estadio |
| `capacidad` | integer | Aforo del estadio |
| `fecha_carga` | timestamp | Timestamp de carga |

---

### md_plantillas

**Rol:** Catálogo de jugadores únicos. Un jugador mantiene el mismo `id_jugador` aunque cambie de equipo. Creado dentro del notebook `hm_plantillas_equipo`.

**Clave única:** `id_jugador = sha2(id_tm, 256)` — hash del ID de Transfermarkt, estable entre temporadas.

| Columna | Tipo | Descripción |
|---|---|---|
| `id_jugador` | string | Hash SHA-256 del id_tm (clave estable) |
| `id_tm` | string | ID del jugador en Transfermarkt |
| `nombre_jugador` | string | Nombre completo |
| `fecha_nacimiento` | date | Fecha de nacimiento |
| `edad` | integer | Edad al momento de la carga |
| `altura` | string | Altura del jugador |
| `pie` | string | Pie dominante (`izquierdo` / `derecho` / `ambos`) |
| `posicion` | string | Posición principal |
| `posicion_secundaria` | string | Posición alternativa |
| `nacionalidad` | string | País de nacimiento |
| `foto_url` | string | URL de la foto en Transfermarkt |
| `fecha_carga` | timestamp | Timestamp de carga |

---

### md_entrenadores

**Rol:** Catálogo de entrenadores únicos. Creado dentro del notebook `hm_entrenadores_equipo`.

| Columna | Tipo | Descripción |
|---|---|---|
| `id_entrenador` | string | ID del entrenador en Transfermarkt |
| `nombre_entrenador` | string | Nombre completo |
| `edad` | integer | Edad al momento de la carga |
| `nacionalidad` | string | País de nacimiento |
| `fecha_carga` | timestamp | Timestamp de carga |

---

## UDV — Tablas Históricas (hm_)

Series temporales por temporada. Se cargan en modo `OVERWRITE` por partición (replaceWhere por período). Almacenadas en `catalog_liga1.tb_udv`.

---

### hm_partidos

**Rol:** Resultados de todos los partidos de Liga 1 por temporada.

**Fuente:** `partidos_{año}.json` (FotMob)  
**Carga:** INCREMENTAL · clave: `(temporada, fecha, equipo_local, equipo_visitante)`

| Columna | Tipo | Descripción |
|---|---|---|
| `temporada` | integer | Año de la temporada |
| `fecha` | date | Fecha del partido |
| `equipo_local` | string | Nombre normalizado del equipo local |
| `equipo_visitante` | string | Nombre normalizado del equipo visitante |
| `goles_local` | integer | Goles del equipo local (extraído de `marcador`) |
| `goles_visitante` | integer | Goles del equipo visitante (extraído de `marcador`) |
| `marcador` | string | Marcador original (`"2-1"`) |
| `resultado_local` | string | `G` / `E` / `P` desde la perspectiva del local |
| `torneo` | string | `Apertura` / `Clausura` / `General` |
| `jornada` | integer | Número de jornada |
| `url_partido` | string | URL del partido en FotMob |
| `fuente` | string | `FotMob` |
| `fecha_carga` | timestamp | Timestamp de carga |
| `periodo` | integer | Año (partición Delta) |

> **Regla de negocio:** `goles_local` y `goles_visitante` se extraen del string `marcador` con regex. `resultado_local` se calcula comparando ambos valores.

---

### hm_estadisticas_partidos

**Rol:** Estadísticas detalladas por equipo por partido. 80+ columnas de métricas.

**Fuente:** `estadisticas_partidos_{año}.json` (FotMob)  
**Clave:** `(temporada, fecha_partido, equipo_local, equipo_visitante, equipo)`

| Columna | Tipo | Descripción |
|---|---|---|
| `temporada` | integer | Año de la temporada |
| `fecha_partido` | date | Fecha del partido |
| `equipo_local` | string | Equipo local del partido |
| `equipo_visitante` | string | Equipo visitante del partido |
| `equipo` | string | Equipo al que corresponden las estadísticas |
| `condicion` | string | `local` / `visitante` |
| `posesion_pct` | double | Porcentaje de posesión |
| `tiros_total` | integer | Total de tiros |
| `tiros_a_puerta` | integer | Tiros al arco |
| `tiros_a_puerta_pct` | double | % de tiros al arco sobre tiros totales |
| `pases_total` | integer | Total de pases intentados |
| `pases_completados` | integer | Pases completados |
| `pases_pct` | double | % de pases completados |
| `duelos_total` | integer | Total de duelos disputados |
| `duelos_ganados` | integer | Duelos ganados |
| `duelos_pct` | double | % de duelos ganados |
| `faltas` | integer | Faltas cometidas |
| `tarjetas_amarillas` | integer | Tarjetas amarillas |
| `tarjetas_rojas` | integer | Tarjetas rojas |
| `fuera_de_juego` | integer | Posiciones de fuera de juego |
| `corners` | integer | Tiros de esquina |
| *(+ ~60 columnas adicionales)* | | Otros indicadores tácticos de FotMob |
| `fecha_carga` | timestamp | Timestamp de carga |
| `periodo` | integer | Año (partición Delta) |

> **Regla de negocio:** FotMob devuelve las métricas como `"14 (56%)"`. El parser extrae el entero y el porcentaje en columnas separadas (`_total`, `_pct`).

---

### hm_plantillas_equipo

**Rol:** Plantel de cada equipo por temporada. También genera `md_plantillas`.

**Fuente:** `plantillas_{año}.csv` (Transfermarkt) + `md_catalogo_equipos`  
**Clave:** `(temporada, id_equipo, id_jugador)`

| Columna | Tipo | Descripción |
|---|---|---|
| `temporada` | integer | Año de la temporada |
| `id_equipo` | string | ID FotMob del equipo |
| `nombre_equipo` | string | Nombre normalizado del equipo |
| `id_jugador` | string | Hash SHA-256 del id_tm |
| `nombre_jugador` | string | Nombre completo del jugador |
| `posicion` | string | Posición en el equipo (portero, defensa, centrocampista, delantero) |
| `numero_camiseta` | integer | Número de camiseta |
| `edad` | integer | Edad en la temporada |
| `fecha_nacimiento` | date | Fecha de nacimiento |
| `valor_mercado_eur` | double | Valor de mercado en EUR (Transfermarkt) |
| `nacionalidad` | string | País del jugador |
| `pie` | string | Pie dominante |
| `altura` | string | Altura |
| `foto_url` | string | URL de la foto |
| `fecha_carga` | timestamp | Timestamp de carga |
| `periodo` | integer | Año (partición Delta) |

---

### hm_tablas_clasificacion

**Rol:** Clasificación por torneo y temporada.

**Fuente:** `tablas_clasificacion_{año}.json` (FotMob)  
**Clave:** `(temporada, torneo, equipo)`

| Columna | Tipo | Descripción |
|---|---|---|
| `temporada` | integer | Año de la temporada |
| `torneo` | string | `Apertura` / `Clausura` / `General` |
| `posicion` | integer | Posición en la tabla |
| `equipo` | string | Nombre normalizado del equipo |
| `pj` | integer | Partidos jugados |
| `pg` | integer | Partidos ganados |
| `pe` | integer | Partidos empatados |
| `pp` | integer | Partidos perdidos |
| `gf` | integer | Goles a favor |
| `gc` | integer | Goles en contra |
| `gd` | integer | Diferencia de goles |
| `puntos` | integer | Puntos acumulados |
| `fecha_carga` | timestamp | Timestamp de carga |
| `periodo` | integer | Año (partición Delta) |

---

### hm_valoracion_equipos

**Rol:** Evolución del valor de mercado del plantel por equipo y temporada.

**Fuente:** `liga1_{año}.csv` (Transfermarkt) + `md_catalogo_equipos`  
**Clave:** `(temporada, id_equipo)`

| Columna | Tipo | Descripción |
|---|---|---|
| `temporada` | integer | Año de la temporada |
| `id_equipo` | string | ID FotMob del equipo |
| `nombre_equipo` | string | Nombre normalizado |
| `valor_mercado_eur` | double | Valor total del plantel en EUR |
| `num_jugadores` | integer | Jugadores en el plantel |
| `edad_promedio` | double | Edad promedio del plantel |
| `extranjeros` | integer | Cantidad de jugadores extranjeros |
| `fecha_carga` | timestamp | Timestamp de carga |
| `periodo` | integer | Año (partición Delta) |

> **Regla de negocio:** Transfermarkt expresa el valor en formatos como `"5,2 mill."` o `"450 mil"`. El parser normaliza a EUR decimal: `5200000.0`, `450000.0`.

---

### hm_entrenadores_equipo

**Rol:** Historial de entrenadores por equipo y temporada. También genera `md_entrenadores`.

**Fuente:** `entrenadores_{año}.csv` (Transfermarkt) + `md_catalogo_equipos`  
**Clave:** `(temporada, id_equipo, id_entrenador)`

| Columna | Tipo | Descripción |
|---|---|---|
| `temporada` | integer | Año de la temporada |
| `id_equipo` | string | ID FotMob del equipo |
| `nombre_equipo` | string | Nombre normalizado del equipo |
| `id_entrenador` | string | ID del entrenador en Transfermarkt |
| `nombre_entrenador` | string | Nombre completo |
| `edad` | integer | Edad del entrenador |
| `nacionalidad` | string | País del entrenador |
| `fecha_inicio` | date | Inicio del cargo (si disponible) |
| `fecha_fin` | date | Fin del cargo (`null` si es actual) |
| `fecha_carga` | timestamp | Timestamp de carga |
| `periodo` | integer | Año (partición Delta) |

---

### hm_campeones

**Rol:** Registro histórico de campeones y subcampeones de Liga 1.

**Fuente:** `campeones_{año}.json` (FotMob)  
**Clave:** `(temporada, torneo)`

| Columna | Tipo | Descripción |
|---|---|---|
| `temporada` | integer | Año de la temporada |
| `torneo` | string | `Apertura` / `Clausura` / `General` |
| `campeon` | string | Nombre normalizado del campeón |
| `subcampeon` | string | Nombre normalizado del subcampeón |
| `fecha_carga` | timestamp | Timestamp de carga |
| `periodo` | integer | Año (partición Delta) |

---

### hm_estadisticas_jugadores

**Rol:** Estadísticas individuales de jugadores por equipo, competencia y temporada. Una fila por jugador × equipo × competencia (Apertura / Clausura).

**Fuente:** `estadisticas_jugadores_{año}.csv` (Transfermarkt)  
**Clave:** `(id_tm + id_equipo + temporada + competencia)`

| Columna | Tipo | Descripción |
|---|---|---|
| `id_estadistica_jugador` | string | PK: hash(id_tm + id_equipo + temporada + competencia) |
| `id_jugador` | string | FK hacia md_plantillas (hash SHA-256 del id_tm) |
| `id_tm` | string | ID del jugador en Transfermarkt |
| `jugador` | string | Nombre completo del jugador |
| `id_equipo` | integer | FK hacia md_catalogo_equipos |
| `temporada` | integer | Año de la temporada |
| `competencia` | string | Nombre de la competencia (`Apertura`, `Clausura`, etc.) |
| `partidos_jugados` | integer | Partidos jugados en la competencia |
| `titularidades` | integer | Partidos como titular |
| `ppp` | double | Puntos por partido del equipo cuando el jugador participó |
| `goles` | integer | Goles anotados |
| `asistencias` | integer | Asistencias |
| `tarjetas_amarillas` | integer | Tarjetas amarillas |
| `tarjeta_amarilla_roja` | integer | Dobles amarillas (expulsión) |
| `tarjetas_rojas` | integer | Tarjetas rojas directas |
| `entrada_suplente` | integer | Veces que entró como suplente (0 para porteros) |
| `salida_suplente` | integer | Veces que fue sustituido (0 para porteros) |
| `penaltis_anotados` | integer | Penaltis anotados en la competencia (0 para porteros) |
| `goles_en_contra` | integer | Goles recibidos en la competencia, solo porteros (0 para otros) |
| `partidos_imbatido` | integer | Partidos sin goles en contra, solo porteros (0 para otros) |
| `min_por_gol` | integer | Minutos por gol ratio (0 para porteros) |
| `minutos_jugados` | integer | Minutos jugados totales |
| `datos_disponibles` | boolean | `true` si TM devolvió stats; `false` si jugador sin datos |
| `fuente` | string | Fuente de los datos (`Transfermarkt`) |
| `fecha_carga` | timestamp | Timestamp de carga |
| `periodo` | integer | Período técnico de carga en formato YYYYMM (partición Delta) |

> **Reglas de negocio:**
> - `penaltis_anotados` solo se popula para jugadores de campo; se absorbe en `goles` en la DDV.
> - `goles_en_contra` y `partidos_imbatido` solo se populan para porteros; el resto recibe 0.
> - `entrada_suplente` y `salida_suplente` siempre son 0 para porteros (Transfermarkt no los registra).
> - Cuando TM no tiene stats de un jugador, `datos_disponibles = false` y todos los campos numéricos quedan en 0.

---

## DDV — Data Marts (ft_ / dm_)

Tablas agregadas y desnormalizadas para consumo directo en Power BI. Almacenadas en `catalog_liga1.tb_ddv` · ruta ADLS: `primera_division/ddv/{entidad}/data/`.

---

### dm_equipos

**Rol:** Dimensión de equipos con atributos consolidados para filtros y slicers en Power BI.

| Columna | Tipo | Descripción |
|---|---|---|
| `id_equipo` | string | ID FotMob (clave) |
| `nombre_equipo` | string | Nombre normalizado |
| `abreviatura` | string | Abreviatura de 3 letras |
| `logo_url` | string | URL del logo del equipo en FotMob (para `dataCategory: ImageUrl` en Power BI) |
| `nombre_estadio` | string | Estadio del equipo |
| `capacidad_estadio` | integer | Aforo del estadio |

---

### ft_rendimiento_temporada

**Rol:** Rendimiento de cada equipo por torneo y temporada. Base del dashboard POSICIONES.

| Columna | Tipo | Descripción |
|---|---|---|
| `temporada` | integer | Año de la temporada |
| `torneo` | string | `Apertura` / `Clausura` / `General` |
| `posicion` | integer | Posición final en la tabla |
| `id_equipo` | string | ID FotMob del equipo |
| `nombre_equipo` | string | Nombre normalizado |
| `pj` | integer | Partidos jugados |
| `pg` | integer | Partidos ganados |
| `pe` | integer | Partidos empatados |
| `pp` | integer | Partidos perdidos |
| `gf` | integer | Goles a favor |
| `gc` | integer | Goles en contra |
| `gd` | integer | Diferencia de goles |
| `puntos` | integer | Puntos acumulados |

---

### ft_rendimiento_acumulado

**Rol:** Estadísticas históricas acumuladas por equipo a través de todas las temporadas.

| Columna | Tipo | Descripción |
|---|---|---|
| `id_equipo` | string | ID FotMob del equipo |
| `nombre_equipo` | string | Nombre normalizado |
| `total_temporadas` | integer | Temporadas en Liga 1 (2020-presente) |
| `total_pj` | integer | Partidos jugados acumulados |
| `total_pg` | integer | Partidos ganados acumulados |
| `total_pe` | integer | Partidos empatados acumulados |
| `total_pp` | integer | Partidos perdidos acumulados |
| `total_gf` | integer | Goles a favor acumulados |
| `total_gc` | integer | Goles en contra acumulados |
| `total_puntos` | integer | Puntos acumulados |
| `pct_victorias` | double | % de partidos ganados |
| `promedio_puntos` | double | Promedio de puntos por partido |

---

### ft_partidos_detalle

**Rol:** Detalle completo de partidos desnormalizado para el dashboard PARTIDOS.

| Columna | Tipo | Descripción |
|---|---|---|
| `temporada` | integer | Año de la temporada |
| `fecha_partido` | date | Fecha del partido |
| `equipo_local` | string | Equipo local |
| `equipo_visitante` | string | Equipo visitante |
| `goles_local` | integer | Goles del local |
| `goles_visitante` | integer | Goles del visitante |
| `marcador` | string | Marcador final |
| `resultado` | string | `Local` / `Visitante` / `Empate` |
| `torneo` | string | Torneo al que corresponde |
| `jornada` | integer | Número de jornada |
| *(estadísticas del partido)* | | Columnas de `hm_estadisticas_partidos` unidas |

---

### ft_plantillas_historico

**Rol:** Evolución de planteles por equipo y temporada. Base del dashboard PLANTILLAS.

| Columna | Tipo | Descripción |
|---|---|---|
| `temporada` | integer | Año de la temporada |
| `id_equipo` | string | ID FotMob del equipo |
| `nombre_equipo` | string | Nombre del equipo |
| `id_jugador` | string | Clave del jugador |
| `nombre_jugador` | string | Nombre del jugador |
| `posicion` | string | Posición táctica |
| `edad` | integer | Edad en la temporada |
| `valor_mercado_eur` | double | Valor de mercado en EUR |
| `numero_camiseta` | integer | Dorsal |
| `nacionalidad` | string | País del jugador |
| `foto_url` | string | URL de la foto |

---

### ft_entrenadores_historico

**Rol:** Historial de cuerpos técnicos. Base del dashboard ENTRENADORES.

| Columna | Tipo | Descripción |
|---|---|---|
| `temporada` | integer | Año de la temporada |
| `id_equipo` | string | ID FotMob del equipo |
| `nombre_equipo` | string | Nombre del equipo |
| `nombre_entrenador` | string | Nombre del entrenador |
| `edad` | integer | Edad del entrenador |
| `nacionalidad` | string | Nacionalidad del entrenador |
| `fecha_inicio` | date | Inicio del cargo |
| `fecha_fin` | date | Fin del cargo (`null` = actual) |

---

### ft_evolucion_valoracion

**Rol:** Valoraciones de mercado por equipo y temporada. Base del dashboard VALORACIÓN.

| Columna | Tipo | Descripción |
|---|---|---|
| `temporada` | integer | Año de la temporada |
| `id_equipo` | string | ID FotMob del equipo |
| `nombre_equipo` | string | Nombre del equipo |
| `valor_mercado_eur` | double | Valor total del plantel en EUR |
| `num_jugadores` | integer | Jugadores en el plantel |
| `edad_promedio` | double | Edad promedio del plantel |
| `extranjeros` | integer | Jugadores extranjeros |

> **Conversión a PEN:** el campo `valor_mercado_eur` se mantiene en EUR en la DDV. La conversión a soles peruanos se realiza en Power BI con el parámetro `TipoCambioEURPEN` en la tabla `_Parametros`.

---

### ft_estadisticas_jugadores

**Rol:** Estadísticas individuales totalizadas por jugador y temporada (suma de todas las competencias). Base del dashboard XI IDEAL.

**Clave:** `(id_jugador + id_equipo + temporada)`

| Columna | Tipo | Descripción |
|---|---|---|
| `id_estadistica_jugador` | string | PK: hash(id_jugador + id_equipo + temporada) |
| `id_jugador` | string | FK hacia md_plantillas (hash SHA-256 del id_tm) |
| `id_tm` | string | ID del jugador en Transfermarkt |
| `jugador` | string | Nombre completo del jugador |
| `foto_url` | string | URL de la foto del jugador en Transfermarkt |
| `id_equipo` | integer | FK hacia dm_equipos |
| `nombre_equipo` | string | Nombre oficial del equipo en la temporada |
| `alias_equipo` | string | Alias o nombre corto del equipo |
| `temporada` | integer | Año de la temporada |
| `partidos_jugados` | integer | Total de partidos jugados (suma todas las competencias) |
| `titularidades` | integer | Total de partidos como titular |
| `ppp` | double | Promedio de puntos por partido del equipo cuando el jugador jugó |
| `goles` | integer | Total de goles en la temporada (incluye penaltis anotados) |
| `asistencias` | integer | Total de asistencias |
| `tarjetas_amarillas` | integer | Total de tarjetas amarillas |
| `tarjeta_amarilla_roja` | integer | Total de dobles amarillas |
| `tarjetas_rojas` | integer | Total de tarjetas rojas |
| `entrada_suplente` | integer | Total de veces que entró como suplente (0 para porteros) |
| `salida_suplente` | integer | Total de veces que fue sustituido (0 para porteros) |
| `goles_en_contra` | integer | Total de goles recibidos en la temporada, solo porteros (0 para otros) |
| `partidos_imbatido` | integer | Total de partidos sin goles en contra, solo porteros (0 para otros) |
| `min_por_gol` | integer | Minutos por gol en la temporada (0 para porteros) |
| `minutos_jugados` | integer | Total de minutos jugados |
| `datos_disponibles` | boolean | `true` si al menos una competencia tiene stats disponibles |
| `posicion` | string | Posición del jugador en esa temporada |
| `nacionalidad_principal` | string | Nacionalidad principal del jugador |
| `fecha_carga` | timestamp | Timestamp de carga en DDV |
| `periodo` | integer | Período técnico de carga en formato YYYYMM (partición Delta) |

> **Reglas de negocio:**
> - `goles` ya incluye `penaltis_anotados` (absorbidos en DDV — no existe columna separada).
> - `goles_en_contra` y `partidos_imbatido` provienen directamente de la UDV y solo tienen valor para porteros.
> - `ppp` se calcula como promedio ponderado sobre las competencias (avg, no suma).

---

### ft_score_ml

**Rol:** Score ML por jugador × temporada × posición en el XI. Generado por PCA (score_100) y clasificado por K-means (nivel). Es la fuente exclusiva del dashboard Power BI Scouting ML, expuesta vía Delta Sharing.

**Origen:** Notebook manual `proceso/frm_ml/notebooks/ft_score_ml/nb_ft_score_ml.py` · lee de `hm_estadisticas_jugadores` (UDV)  
**Clave:** `(id_jugador + id_equipo + temporada)` → `id_score_ml` (hash)  
**Ruta ADLS:** `primera_division/ddv/ft_score_ml/data/`  
**Partición:** `temporada`

| Columna | Tipo | Descripción |
|---|---|---|
| `id_score_ml` | string | PK: hash(id_jugador

| Columna | Tipo | Descripción |
|---|---|---|
| `id_score_ml` | string | PK: hash(id_jugador + id_equipo + temporada + posicion_xi) |
| `id_jugador` | string | FK → md_plantillas |
| `id_equipo` | string | FK → md_equipos |
| `temporada` | integer | Año de la temporada |
| `posicion_xi` | string | Posición táctica en el XI ideal |
| `score_100` | double | Score ML normalizado 0–100 (PCA) |
| `nivel_ml` | string | Clasificación K-means: Elite / Bueno / Regular / Suplente |
| `cluster_ml` | integer | Número de cluster K-means (0–3) |
| `minutos_jugados` | integer | Minutos jugados en la temporada |
| `goles` | integer | Goles anotados |
| `asistencias` | integer | Asistencias |
| `ppp` | double | Puntos por partido del equipo cuando jugó |

---

*Catálogo de entidades — Liga 1 Perú Data Engineering Platform · Oscar García Del Águila · 2025–2026*
