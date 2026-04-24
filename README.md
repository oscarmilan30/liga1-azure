# Liga 1 Perú — Proyecto de Data Engineering en Azure

Proyecto personal de ingeniería de datos orientado a la extracción, transformación y carga (ETL/ELT) de estadísticas de la **Liga 1 Peruana de Fútbol**, construido sobre una arquitectura moderna en **Microsoft Azure** con Databricks y Delta Lake.

> **Estado actual:** Capas Landing, RAW y UDV implementadas. Capa DDV (visualización) en diseño pendiente de exploración de datos reales.

---

## Tabla de Contenidos

- [Visión General](#visión-general)
- [Stack Tecnológico](#stack-tecnológico)
- [Arquitectura por Capas](#arquitectura-por-capas)
- [Capa de Control — Azure SQL Database](#capa-de-control--azure-sql-database)
- [Capa Landing — Web Scraping](#capa-landing--web-scraping)
- [Capa RAW — Azure Data Factory](#capa-raw--azure-data-factory)
- [Capa UDV — Databricks Delta Lake](#capa-udv--databricks-delta-lake)
- [Capa DDV — Pendiente](#capa-ddv--pendiente)
- [Modelo de Datos](#modelo-de-datos)
- [Estructura del Repositorio](#estructura-del-repositorio)
- [Configuración del Entorno](#configuración-del-entorno)

---

## Visión General

El proyecto construye una plataforma de datos completa para analizar la Liga 1 Peruana. Extrae información de dos fuentes web mediante scraping con Selenium, la almacena en Azure Data Lake Storage, la procesa a través de pipelines orquestados por Azure Data Factory y la transforma con notebooks PySpark en Databricks, generando tablas Delta listas para consumo en Power BI.

**Fuentes de datos:**

| Fuente | Tipo | Datos extraídos |
|---|---|---|
| **FotMob** | Web scraping (Selenium + BeautifulSoup) | Partidos, estadísticas, equipos, clasificación, campeones |
| **Transfermarkt** | Web scraping (Selenium) | Plantillas, valoraciones, estadios, entrenadores |

**Lógica de temporadas entre fuentes:**

FotMob y Transfermarkt usan calendarios distintos. Para unificar correctamente:

```
Año de entrada X:
  → FotMob busca temporada:        X      (ej. 2024 = temporada 2024)
  → Transfermarkt busca temporada: X-1    (ej. 2024 → busca 2023/2024)
  → Todo se guarda como año:       X
```

---

## Stack Tecnológico

| Servicio / Herramienta | Rol en el proyecto |
|---|---|
| **Azure Data Lake Storage Gen2** | Almacenamiento de todas las capas (Landing, RAW, UDV) |
| **Azure Data Factory** | Orquestación de pipelines ETL con modos Histórico / Incremental / Reproceso |
| **Azure Databricks** | Procesamiento distribuido PySpark, Delta Lake, Unity Catalog |
| **Azure SQL Database** | Control de ejecución, parametría y metadatos de todos los pipelines |
| **Azure Key Vault** | Gestión segura de credenciales y secretos |
| **Selenium + BeautifulSoup** | Web scraping de FotMob y Transfermarkt (ejecutado en Jupyter local) |
| **Delta Lake** | Formato de almacenamiento para capa UDV con soporte ACID |
| **Power BI** | Capa de visualización y dashboards (consume vistas UDV) |
| **Jupyter Notebook** | Entorno de ejecución local del scraping |

> **Nota sobre el scraping:** Selenium requiere ChromeDriver y acceso a un navegador real, lo cual no está disponible en el plan gratuito de Databricks Community Edition. Por ello el scraping se ejecuta localmente en Jupyter y los archivos resultantes se cargan directamente a ADLS.

---

## Arquitectura por Capas

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           FUENTES DE DATOS                                  │
│                                                                             │
│   ┌─────────────────────────┐      ┌───────────────────────────────────┐   │
│   │         FotMob          │      │          Transfermarkt             │   │
│   │  Partidos · Estadísticas│      │  Plantillas · Valoraciones        │   │
│   │  Equipos · Clasificación│      │  Estadios · Entrenadores          │   │
│   │  Campeones              │      │  (temporada X-1)                  │   │
│   └────────────┬────────────┘      └──────────────┬────────────────────┘   │
└────────────────┼─────────────────────────────────-┼────────────────────────┘
                 │  Selenium (Jupyter local)          │
                 ▼                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    CAPA LANDING  —  ADLS: landing/{año}/                    │
│                                                                             │
│  JSON: equipos, partidos, estadisticas_partidos,                           │
│        tablas_clasificacion, campeones                                      │
│  CSV:  liga1, plantillas, estadios, entrenadores                           │
│                                                                             │
│  Trigger ADF: landing/temp/ejecucion/scraping_completado.json              │
│  (ADF detecta este archivo y dispara el pipeline RAW automáticamente)      │
└───────────────────────────────────┬─────────────────────────────────────────┘
                                    │  ADF Storage Event Trigger
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│               CAPA RAW  —  ADF Orchestrator + ADLS: raw/{entidad}/         │
│                                                                             │
│  Databricks Notebooks PySpark:                                             │
│    curated_csv       → normalización de CSVs de Transfermarkt             │
│    curated_json      → parsing de JSONs de FotMob                         │
│    curated_historico → unificación de datos históricos                    │
│    curated_dataentry → ingesta de archivos de catálogo manual             │
│                                                                             │
│  Salida: Parquet en ADLS raw/Proyecto/liga1/{entidad}/{yyyy}/{MM}/{dd}/   │
│  Control: tbl_paths en Azure SQL  (flg_udv='N' = pendiente)              │
└───────────────────────────────────┬─────────────────────────────────────────┘
                                    │  ADF → Databricks Jobs
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│           CAPA UDV  —  Databricks + Delta Lake + Unity Catalog              │
│                                                                             │
│  Notebooks PySpark por entidad (nb_md_*, nb_hm_*)                         │
│  Configuración: YAML por entidad (columnas, tipos, reglas de negocio)     │
│  Parametría desde Azure SQL (tbl_pipeline, tbl_pipeline_parametros)       │
│                                                                             │
│  Salida: Delta Tables en tb_udv + Vistas SQL en vw_udv                    │
│  tbl_paths actualizado: flg_udv='S' (procesado)                          │
└───────────────────────────────────┬─────────────────────────────────────────┘
                                    │  [PENDIENTE]
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│              CAPA DDV  —  Agregaciones para Visualización                   │
│                                                                             │
│  Tablas y vistas de agregación optimizadas para Power BI                  │
│  A diseñar una vez explorados los datos reales en UDV                     │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
                    ┌───────────────────────────┐
                    │         Power BI          │
                    │   Dashboards Liga 1 Perú  │
                    └───────────────────────────┘
```

---

## Capa de Control — Azure SQL Database

El cerebro de toda la arquitectura. Ningún notebook tiene rutas o parámetros hardcodeados: todo se lee desde estas tablas en tiempo de ejecución.

### Tablas principales

**`tbl_pipeline`** — Registro maestro de todos los procesos lógicos del proyecto.

| Campo | Descripción |
|---|---|
| `PipelineId` | Identificador único del pipeline |
| `Nombre` | Nombre lógico (ej. `hm_partidos`, `Raw_Liga1`) |
| `Nivel` | Capa a la que pertenece: `RAW` o `UDV` |
| `RutaBase` | Ruta raíz en ADLS (ej. `/Proyecto/liga1`) |
| `Ruta` | Ruta física completa de salida en ADLS |
| `parent_pipelineid` | Referencia al pipeline padre |
| `Activo` | Flag para activar/desactivar el pipeline |

**`tbl_pipeline_parametros`** — Todos los parámetros de ejecución por pipeline.

Parámetros clave para pipelines RAW:

| Parámetro | Ejemplo | Descripción |
|---|---|---|
| `MODO_EJECUCION` | `HISTORICO` / `INCREMENTAL` / `REPROCESO` | Controla el flujo ADF |
| `FILESYSTEM` | `liga1` | Contenedor de ADLS |
| `HISTORIC_START_YEAR` | `2020` | Año de inicio para carga histórica |
| `CURRENT_YEAR` | `2025` | Año actual de procesamiento |
| `FLG_REPROCESO` | `0` o `1` | Activa el modo reproceso |
| `ANIO_REPROCESO` | `2022` | Año específico a reprocesar |
| `CLUSTER_ID` | `xxxx-xxxxxx-xxxxxxxx` | ID del clúster Databricks |
| `AREA` | URL workspace | Databricks workspace URL para API |

Parámetros clave para pipelines UDV:

| Parámetro | Ejemplo | Descripción |
|---|---|---|
| `SCHEMA_TABLA` | `tb_udv` | Esquema Delta en Unity Catalog |
| `SCHEMA_VISTA` | `vw_udv` | Esquema de vistas SQL |
| `RUTA_TABLA` | `/tb_udv/hm_partidos` | Ruta física de la tabla Delta |
| `TIPO_CARGA` | `INCREMENTAL` o `FULL` | Modo de escritura Delta |
| `NOMBRE_TABLA` | `hm_partidos` | Nombre de la tabla en Unity Catalog |
| `YAML_PATH` | `/frm_udv/conf/hm_partidos/hm_partidos.yml` | Ruta del YAML de configuración |

**`tbl_predecesores`** — Grafo de dependencias. Define qué tablas deben existir antes de ejecutar cada pipeline UDV.

```
catalogo_equipos RAW (3)
        └──► md_catalogo_equipos UDV (4)  ◄── HUB CENTRAL DE REFERENCIAS
                    ├──► md_equipos (2)
                    ├──► md_estadios (7)
                    ├──► hm_valoracion_equipos (14)  ◄── liga1 RAW (15)
                    ├──► hm_partidos (16)            ◄── partidos RAW (17)
                    ├──► hm_estadisticas (18)        ◄── estadisticas RAW (19)
                    │                                ◄── hm_partidos (16)
                    ├──► hm_tablas_clasificacion (20) ◄── tablas_clasificacion RAW (21)
                    ├──► hm_campeones (22)           ◄── campeones RAW (23)
                    └──► hm_plantillas_equipo (9)    ◄── md_plantillas (8)
                                                          ◄── plantillas RAW (10)
```

`md_catalogo_equipos` es el hub central: casi todos los pipelines UDV dependen de él para resolver identidades de equipos entre fuentes.

**`tbl_paths`** — Cola de rutas RAW procesadas. Es el mecanismo de comunicación entre ADF y Databricks.

| Campo | Descripción |
|---|---|
| `Entidad` | Nombre de la entidad (ej. `partidos`, `equipos`) |
| `RutaRaw` | Ruta física del Parquet en ADLS |
| `flg_udv` | `'N'` = pendiente de procesar, `'S'` = ya procesado |
| `ModoEjecucion` | `HISTORICO`, `INCREMENTAL` o `REPROCESO` |
| `Periodo` | Año al que corresponde el archivo |

**`tbl_archivos_liga1`** — Catálogo de entidades con su tipo de archivo.

| Entidad | Tipo |
|---|---|
| campeones, equipos, estadisticas_partidos, partidos, tablas_clasificacion | JSON |
| entrenadores, estadios, liga1, plantillas | CSV |

**`tbl_control_ejecucion`** — Log de cada ejecución con inicio, fin y estado (`Running` / `Completed` / `Failed`).

**`tbl_flag_update_queue_id`** — Tabla auxiliar con un trigger SQL que actualiza automáticamente `flg_udv='S'` en `tbl_paths` cuando Databricks termina de procesar un archivo.

### Stored Procedures principales

| SP | Descripción |
|---|---|
| `sp_log_start` | Registra inicio de ejecución, retorna `IdEjecucion` |
| `sp_log_end` | Cierra registro con estado y mensaje de error |
| `sp_obtener_parametros` | Devuelve todos los parámetros de un pipeline en formato pivotado |
| `sp_obtener_rango_anios` | Genera el rango de años para la carga histórica |
| `sp_obtener_archivos` | Retorna el catálogo de entidades en formato JSON |
| `sp_registrar_path` | Inserta ruta RAW en `tbl_paths` con `flg_udv='N'` |
| `sp_registrar_path_historico` | Igual que el anterior pero específico para modo HISTORICO |
| `sp_actualizar_modo_ejecucion` | Cambia `MODO_EJECUCION` a `INCREMENTAL` al terminar un reproceso |
| `sp_registrar_predecesor` | Registra relaciones de dependencia entre pipelines |

---

## Capa Landing — Web Scraping

### Entorno de ejecución

El scraping se ejecuta en **Jupyter Notebook local** usando Python con Selenium y ChromeDriver. No se ejecuta en Databricks porque Selenium requiere acceso a un navegador real, lo cual no está disponible en el plan gratuito de Databricks Community Edition.

El flujo completo desde el lado del scraping:

```
Jupyter ejecuta el scraping
        │
        ├── Guarda archivos localmente en C:\...\liga1\data\{año}\
        ├── Sube archivos a ADLS en landing/{año}/
        └── Genera scraping_completado.json en landing/temp/ejecucion/
                            │
                            └── ADF Storage Event Trigger detecta el archivo
                                y dispara pl_Orchestrator_ligaperuana_Raw
```

### Fuentes y archivos generados

Por cada año procesado se generan hasta 9 archivos:

| Archivo | Fuente | Formato | Descripción |
|---|---|---|---|
| `equipos_{año}.json` | FotMob | JSON | Lista de equipos con nombre y URL |
| `partidos_{año}.json` | FotMob | JSON | Todos los partidos con fecha, marcador y URL |
| `estadisticas_partidos_{año}.json` | FotMob | JSON | Estadísticas detalladas por partido |
| `tablas_clasificacion_{año}.json` | FotMob | JSON | Clasificación por torneo (Apertura / Clausura) |
| `campeones_{año}.json` | FotMob | JSON | Campeón y subcampeón de la temporada |
| `liga1_{año}.csv` | Transfermarkt | CSV (sep=\|) | Datos generales de equipos (valor, edad, extranjeros) |
| `plantillas_{año}.csv` | Transfermarkt | CSV (sep=\|) | Plantel completo (jugadores, posición, valor mercado) |
| `estadios_{año}.csv` | Transfermarkt | CSV (sep=\|) | Estadios (nombre, capacidad) |
| `entrenadores_{año}.csv` | Transfermarkt | CSV (sep=\|) | Historial de entrenadores desde 2020 |

> Para el año actual, `campeones` es opcional ya que la temporada puede no haber terminado.

### Estructura de archivos JSON

Todos los archivos JSON siguen la misma estructura de envoltura:

```json
{
  "fuente": "FotMob",
  "temporada": 2024,
  "fecha_carga": "2025-01-15 10:30:00",
  "data": [ ... ]
}
```

### Estructura de archivos CSV

Todos los archivos CSV tienen separador `|` e incluyen campos de metadatos adicionales:

```
fuente|temporada|fecha_carga|... columnas de negocio ...
Transfermarkt|2024|2025-01-15 10:30:00|...
```

Los nombres de columnas pasan por sanitización automática: eliminación de tildes, caracteres especiales, espacios y normalización a `snake_case` en minúsculas.

### Modos de ejecución del scraping

El scraping tiene tres modos que se corresponden exactamente con los modos de ADF:

**Modo Histórico** — Procesa un rango de años completo. Genera un único trigger al finalizar todo el rango.

```python
run_scraping_liga1(modo="historica", anio_inicio=2020, anio_fin=2025)
# También acepta solo anio_inicio y toma el año actual como fin:
run_scraping_liga1(modo="historica", anio_inicio=2020)
```

**Modo Incremental** — Procesa únicamente el año actual. Ideal para ejecuciones periódicas.

```python
run_scraping_liga1(modo="incremental")
```

**Modo Reproceso** — Reprocessa un año específico sin afectar a los demás.

```python
run_scraping_liga1(modo="reproceso", anio_objetivo=2022)
```

### Lógica de años entre fuentes

```python
# Para cualquier año X ingresado:
año_guardado      = X        # Nombre de carpeta y archivos
temporada_fotmob  = str(X)   # FotMob: misma temporada
año_transfermarkt = X - 1    # Transfermarkt: temporada anterior (su calendario va desfasado)
```

Ejemplo para 2024:
- FotMob extrae la Liga 1 temporada 2024
- Transfermarkt extrae la temporada 2023/2024 (que en su sistema es el año 2023)
- Todo queda guardado en la carpeta `2024/`

### Sistema de triggers para ADF

Al completar el scraping exitosamente se generan dos archivos en ADLS:

```
landing/temp/ejecucion/scraping_completado.json   ← ADF escucha este archivo
landing/temp/logs/ultima_ejecucion.json           ← Log detallado de la ejecución
```

Contenido del trigger:

```json
{
  "proceso": "scraping_liga1",
  "modo_ejecucion": "incremental",
  "año_procesado": 2025,
  "estado": "completado",
  "archivos_generados": [ { "nombre": "partidos_2025.json", "tamaño_mb": 1.2 }, ... ],
  "total_archivos": 9,
  "timestamp_ejecucion": "2025-01-15T10:45:00",
  "trigger_id": "trigger_2025_20250115_104500"
}
```

Si la validación de archivos falla, el trigger se genera con `"estado": "error"` y ADF no procesa el año.

### Validación de archivos antes del trigger

Antes de generar el trigger, el sistema valida que todos los archivos obligatorios estén presentes:

```
Años 2020 a (año_actual - 1): 9 archivos obligatorios (incluye campeones)
Año actual:                    8 archivos obligatorios (campeones es opcional)
```

Si falta algún archivo obligatorio la validación falla, no se genera el trigger y ADF no es notificado.

### Reintentos y tolerancia a fallos

Cada función de scraping implementa hasta 2 reintentos automáticos con pausa entre intentos. En modo histórico, si un año falla después de todos los reintentos el sistema continúa con el siguiente año y reporta los años fallidos al finalizar. Los años fallidos pueden reprocesarse con `modo="reproceso"`.

### Normalización de datos (CSVs Transfermarkt)

Los CSVs de Transfermarkt pasan por tres normalizaciones automáticas:

**Valores monetarios** — Convierte múltiples formatos al valor numérico en euros:

```
"5,2 mill."  → 5200000.0
"450 mil"    → 450000.0
"1.200.000"  → 1200000.0
```

**Decimales europeos** — Convierte el formato europeo al anglosajón:

```
"1,85"  →  1.85
"78,5"  →  78.5
```

**Nombres de columnas** — Sanitización completa a snake_case:

```
"Valor Mercado (€)"  →  "valor_mercado_eur_"
"Año"                →  "anio"
"Edad Prom."         →  "edad_prom_"
```

### Conexión a Azure desde Jupyter

La autenticación usa **Interactive Browser Credential** (abre ventana de navegador en el primer uso). Los secretos se obtienen desde Azure Key Vault:

```
kv-liga1-secreto.vault.azure.net
  → storageaccount      → nombre de la cuenta ADLS
  → storageaccountkey   → clave de acceso
  → filesystemname      → nombre del contenedor
```

Una vez autenticado, la credencial se reutiliza durante toda la sesión de Jupyter.

---

## Capa RAW — Azure Data Factory

### Flujo de orquestación completo

```
ADF Storage Trigger detecta scraping_completado.json
                │
                ▼
pl_Orchestrator_ligaperuana_Raw
    ├── pl_ctrl_start  (registra inicio en tbl_control_ejecucion)
    │
    ├── pl_ligaperuana_Raw
    │       └── pl_condition_proceso
    │               ├── MODO = HISTORICO   → pl_ligaperuana_Historico
    │               ├── MODO = INCREMENTAL → pl_ligaperuana_Incremental
    │               └── MODO = REPROCESO   → pl_ligaperuana_Reproceso
    │                         │                      │
    │                         └──────────────────────┘
    │                                    │
    │                         pl_procesar_archivos_por_anio
    │                           (ForEach sobre entidades del año)
    │                                    │
    │                         pl_procesar_archivos
    │                           (Copy Data: Landing → RAW)
    │                           (Registra ruta en tbl_paths flg_udv='N')
    │
    └── pl_ctrl_end_success / pl_ctrl_end_fail
        (cierra registro en tbl_control_ejecucion)
```

### Descripción de cada pipeline

**`pl_Orchestrator_ligaperuana_Raw`** — Orquestador macro. Llama al control de inicio, ejecuta el flujo completo y registra el resultado final.

**`pl_ligaperuana_Raw`** — Lee el parámetro `MODO_EJECUCION` desde Azure SQL y delega a `pl_condition_proceso`.

**`pl_condition_proceso`** — If/Else que decide el flujo según el modo. Pasa los parámetros `RUTA_BASE`, `CAPA_RAW`, `FILESYSTEM`, `ANIO`, etc. al pipeline correspondiente.

**`pl_ligaperuana_Historico`** — Llama a `sp_obtener_rango_anios` para obtener el rango de años. ForEach por año llamando a `pl_procesar_archivos_por_anio`. Al terminar ejecuta `pl_historico_unificador`.

**`pl_ligaperuana_Incremental`** — Igual que histórico pero solo para el año actual.

**`pl_ligaperuana_Reproceso`** — Procesa el año definido en `ANIO_REPROCESO`. Al terminar llama a `sp_actualizar_modo_ejecucion` para volver a `INCREMENTAL`.

**`pl_procesar_archivos_por_anio`** — ForEach sobre la lista de archivos (de `sp_obtener_archivos`) para un año. Para cada archivo llama a `pl_procesar_archivos`.

**`pl_procesar_archivos`** — Ejecuta el Copy Data de Landing a RAW:
- Origen: `landing/{año}/{archivo}_{año}.json|csv`
- Destino: `raw/Proyecto/liga1/{entidad}/{yyyy}/{MM}/{dd}/data/`
- Al terminar: registra la ruta en `tbl_paths` con `flg_udv='N'` vía `sp_registrar_path`

**Pipelines de control y soporte:**

| Pipeline | Descripción |
|---|---|
| `pl_ctrl_start` | Registra inicio con `sp_log_start`, retorna `IdEjecucion` |
| `pl_ctrl_end_success` | Cierra ejecución con estado `Completed` |
| `pl_ctrl_end_fail` | Cierra ejecución con estado `Failed` y mensaje de error |
| `pl_delete_controlado` | Eliminación segura de archivos en ADLS |
| `pl_execute_jobs_databricks` | Lanza Jobs de Databricks vía API REST |
| `pl_execute_jobs_ddl` | Ejecuta scripts DDL en Databricks |
| `pl_execute_create_jobs` | Crea Databricks Jobs dinámicamente |
| `pl_get_csv_mapping` | Obtiene mapeo de columnas para CSVs |
| `pl_Orchestrator_dataentrys` | Orquesta la ingesta de archivos de catálogo manual |
| `pl_dataentry_catalogo_equipos` | Pipeline específico para el catálogo de equipos |

---

## Capa UDV — Databricks Delta Lake

### Patrón de diseño

Todos los notebooks UDV siguen el mismo patrón. Nada está hardcodeado: los parámetros vienen de Azure SQL y el esquema viene del YAML de cada entidad.

```
Notebook UDV
    │
    ├── 1. SETUP
    │       setup_adls()            → Configura acceso ABFSS con secretos de Key Vault
    │       SparkSession            → Inicializa sesión Spark
    │
    ├── 2. PARÁMETROS (desde Azure SQL vía JDBC)
    │       get_pipeline(id)        → Nombre de entidad, ruta, nivel
    │       get_pipeline_params()   → YAML_PATH, TIPO_CARGA, SCHEMA_TABLA, RUTA_TABLA
    │       get_predecesor(id)      → Rutas de tablas predecesoras UDV
    │
    ├── 3. YAML DE CONFIGURACIÓN (leído desde ADLS)
    │       get_yaml_from_param()   → Lee YAML desde la ruta registrada en SQL
    │       Extrae:
    │         cols_raw_hm           → Columnas a leer del RAW
    │         rename_columns_hm     → Mapeo de nombres RAW → nombres UDV
    │         case_rules            → Reglas de limpieza y normalización
    │         dedup_cols_hm         → Claves para deduplicación
    │         schema_hm             → Esquema final (nombre, tipo, comentario)
    │
    ├── 4. LECTURA DE DATOS
    │       get_entity_data()       → Lee Parquets RAW desde rutas en tbl_paths (flg_udv='N')
    │       read_udv_table()        → Lee tablas predecesoras UDV para enriquecer
    │       is_dataframe_empty()    → Validación antes de continuar
    │
    ├── 5. TRANSFORMACIÓN (módulo .py específico por entidad)
    │       cast_dataframe_schema() → Aplica tipos de datos según esquema YAML
    │       rename_columns()        → Mapea nombres según YAML
    │       Reglas case_rules       → Limpieza y normalización de texto/números
    │       Deduplicación           → Por claves de negocio (dedup_cols)
    │
    ├── 6. ESCRITURA DELTA
    │       write_delta_udv()       → Escribe Delta Lake
    │         FULL:        overwrite completo de la tabla
    │         INCREMENTAL: append con deduplicación por periodo
    │       Ruta: udv/Proyecto/liga1/tb_udv/{tabla}/data/
    │
    └── 7. FLAGS Y LOGS
            tbl_flag_update_queue_id → Trigger SQL actualiza flg_udv='S' en tbl_paths
            sp_log_end               → Cierra registro en tbl_control_ejecucion
```

### Configuración por YAML

Cada entidad tiene un archivo YAML que define su esquema completo sin tocar código Python. Ejemplo simplificado para `hm_partidos`:

```yaml
hm_partidos:
  cols_raw_hm:
    - fecha
    - local
    - visitante
    - marcador
    - url

  dedup_cols_hm:
    - temporada
    - fecha
    - local
    - visitante

  rename_columns_hm:
    local: equipo_local
    visitante: equipo_visitante

  goles_from_marcador:
    columna_origen: marcador
    col_local: goles_local
    col_visitante: goles_visitante

  schema_hm:
    - { name: temporada,       type: integer }
    - { name: fecha,           type: date    }
    - { name: equipo_local,    type: string  }
    - { name: equipo_visitante,type: string  }
    - { name: goles_local,     type: integer }
    - { name: goles_visitante, type: integer }
    - { name: marcador,        type: string  }
    - { name: fuente,          type: string  }
    - { name: fecha_carga,     type: timestamp }
```

### Módulos de transformación por entidad

Cada entidad tiene su módulo Python con lógica de negocio específica:

| Módulo | Particularidades |
|---|---|
| `md_catalogo_equipos.py` | Normaliza nombres entre FotMob y Transfermarkt para unificar identidades |
| `md_equipos.py` | Enriquece con catálogo de equipos, normaliza valores monetarios |
| `md_estadios.py` | Normaliza capacidades a enteros |
| `hm_partidos.py` | Extrae `goles_local` y `goles_visitante` desde la columna `marcador` ("2-1") |
| `hm_estadisticas_partidos.py` | Descompone métricas en formato `"14 (56%)"` → entero y porcentaje decimal |
| `hm_tablas_clasificacion.py` | Maneja múltiples torneos por temporada (Apertura / Clausura / Único) |
| `hm_valoracion_equipos.py` | Normaliza valores monetarios en millones de euros |
| `hm_plantillas_equipo.py` | Une con `md_catalogo_equipos` para resolver IDs de equipos |
| `hm_entrenadores_equipo.py` | Genera tabla maestra `md_entrenadores` + tabla histórica `hm_entrenadores_equipo` |

### Particularidad de estadísticas de partidos

FotMob devuelve métricas con el formato `"14 (56%)"` (valor + porcentaje). El YAML define en `stats_with_pct` cuáles columnas tienen este formato y el módulo las descompone automáticamente:

```python
# "14 (56%)"  →  valor_int = 14, valor_pct = 0.56
_first_int_expr(col)   # extrae el número entero
_pct_expr(col)         # extrae el porcentaje y lo convierte a decimal [0,1]
```

### DDL y vistas en Unity Catalog

Por cada entidad se crean dos objetos en Databricks Unity Catalog ejecutados por `pl_execute_jobs_ddl`:

**Tabla física Delta** — Apunta a la ruta en ADLS con particionado por `periodo`:

```sql
CREATE OR REPLACE TABLE ${catalog_name}.tb_udv.hm_partidos (
    temporada        INT       COMMENT 'Año de la temporada',
    fecha            DATE      COMMENT 'Fecha del partido',
    equipo_local     STRING    COMMENT 'Nombre del equipo local',
    equipo_visitante STRING    COMMENT 'Nombre del equipo visitante',
    goles_local      INT       COMMENT 'Goles del equipo local',
    goles_visitante  INT       COMMENT 'Goles del equipo visitante',
    marcador         STRING    COMMENT 'Marcador original',
    fuente           STRING    COMMENT 'Fuente del dato',
    fecha_carga      TIMESTAMP COMMENT 'Timestamp de carga',
    periodo          INT       COMMENT 'Año de particionado'
)
PARTITIONED BY (periodo)
LOCATION 'abfss://${container_name}@${storage_account}.dfs.core.windows.net/
          udv/Proyecto/liga1/tb_udv/hm_partidos/data'
USING delta;
```

**Vista lógica** — Expone la tabla con alias limpios para consumo desde Power BI:

```sql
CREATE OR REPLACE VIEW ${catalog_name}.vw_udv.hm_partidos_vw AS
SELECT temporada, fecha, equipo_local, equipo_visitante,
       goles_local, goles_visitante, marcador, fuente, fecha_carga
FROM tb_udv.hm_partidos;
```

Power BI y otras herramientas siempre consumen desde las vistas `vw_udv.*`, nunca directamente de las tablas Delta.

---

## Capa DDV — Pendiente

La capa DDV (Data de Visualización) es la capa final de agregaciones optimizadas para consumo directo en Power BI. Está pendiente de diseño e implementación.

**Estrategia de implementación:**

1. Levantar la arquitectura completa en Azure
2. Ejecutar el scraping histórico (2020 → año actual)
3. Correr los pipelines RAW y UDV completos
4. Explorar las tablas resultantes en Databricks para entender la riqueza y calidad de los datos
5. Diseñar las tablas DDV basadas en los datos reales disponibles
6. Implementar notebooks DDV siguiendo el mismo patrón que UDV

Las agregaciones previstas incluirán: rendimiento por equipo y temporada, métricas comparativas entre temporadas, ranking histórico, análisis de estadísticas por torneo y tendencias de valoración de mercado.

---

## Modelo de Datos

### Tablas maestras (md\_)

| Tabla | Pipeline ID | Tipo carga | Fuente RAW | Descripción |
|---|---|---|---|---|
| `md_catalogo_equipos` | 4 | FULL | `catalogo_equipos` (DataEntry) | Correspondencia de equipos entre FotMob y Transfermarkt |
| `md_equipos` | 2 | FULL | `md_catalogo_equipos` (UDV) | Atributos generales: valor mercado, edad promedio, extranjeros |
| `md_estadios` | 7 | FULL | `estadios` (CSV) | Estadios con nombre y capacidad |
| `md_plantillas` | 8 | FULL | `plantillas` (CSV) | Catálogo de jugadores |
| `md_entrenadores` | 11 | FULL | `entrenadores` (CSV) | Catálogo de entrenadores |

### Tablas históricas (hm\_)

| Tabla | Pipeline ID | Tipo carga | Fuente RAW | Descripción |
|---|---|---|---|---|
| `hm_partidos` | 16 | INCREMENTAL | `partidos` (JSON) | Partidos por temporada con resultado y marcador |
| `hm_estadisticas_partidos` | 18 | INCREMENTAL | `estadisticas_partidos` (JSON) | +20 métricas por partido: tiros, pases, duelos, tarjetas |
| `hm_plantillas_equipo` | 9 | INCREMENTAL | `plantillas` (CSV) | Plantel de cada equipo por temporada |
| `hm_tablas_clasificacion` | 20 | INCREMENTAL | `tablas_clasificacion` (JSON) | Clasificación por torneo y temporada |
| `hm_valoracion_equipos` | 14 | INCREMENTAL | `liga1` (CSV) | Valor de mercado por equipo y temporada |
| `hm_entrenadores_equipo` | 12 | INCREMENTAL | `entrenadores` (CSV) | Historial de entrenadores por equipo |
| `hm_campeones` | 22 | INCREMENTAL | `campeones` (JSON) | Campeones y subcampeones históricos |

---

## Estructura del Repositorio

```
liga1-azure/
│
├── cluster/
│   └── cluster-scraping-liga1.json        # Configuración del clúster Databricks
│
├── dataset/                                # Datasets ADF
│   ├── ds_adls_binary.json
│   ├── ds_adls_csv.json
│   ├── ds_adls_csv_dataentry.json
│   ├── ds_adls_json.json
│   ├── ds_adls_parquet.json
│   └── ds_sql_generic.json
│
├── ddl_deploy/                             # DDL de tablas y vistas en Unity Catalog
│   ├── crear_esquema_udv.py
│   ├── ddl_hm_campeones/
│   ├── ddl_hm_entrenadores_equipo/
│   ├── ddl_hm_estadisticas_partidos/
│   ├── ddl_hm_partidos/
│   ├── ddl_hm_plantillas_equipo/
│   ├── ddl_hm_tablas_clasificacion/
│   ├── ddl_hm_valoracion_equipos/
│   ├── ddl_md_catalogo_equipos/
│   ├── ddl_md_equipos/
│   └── ddl_md_estadios/
│
├── factory/                                # Configuración del factory ADF
│
├── frm_landing/
│   └── scraping/
│       └── fotmob_scraping_piloto.py       # Versión piloto del scraping (referencia)
│
├── frm_raw/                                # Capa RAW — notebooks de curación
│   ├── curated_csv/
│   ├── curated_dataentry/
│   ├── curated_historico/
│   └── curated_json/
│
├── frm_udv/                                # Capa UDV — transformación de negocio
│   ├── conf/                               # Configuraciones YAML por entidad
│   │   ├── hm_campeones/
│   │   ├── hm_entrenadores_equipo/
│   │   ├── hm_estadisticas_partidos/
│   │   ├── hm_partidos/
│   │   ├── hm_plantillas_equipo/
│   │   ├── hm_tablas_clasificacion/
│   │   ├── hm_valoracion_equipos/
│   │   ├── md_catalogo_equipos/
│   │   ├── md_equipos/
│   │   └── md_estadios/
│   └── notebooks/                          # Notebooks PySpark por entidad
│
├── linkedService/                          # Linked Services de ADF
│   ├── ls_adls.json
│   ├── ls_azure_keyvault.json
│   ├── ls_databricks.json
│   ├── ls_databricks_kv_clusterparam.json
│   └── ls_sql_liga1.json
│
├── pipeline/                               # Pipelines de ADF (JSON)
│
├── util/
│   └── utils_liga1.py                      # Librería de utilidades PySpark compartida
│
├── workflow_deploy/                        # Deploy de Databricks Workflows
│
├── env_setup.py                            # Configuración automática de entorno
├── Querys.sql                              # DDL y datos iniciales de Azure SQL
└── README.md
```

---

## Configuración del Entorno

### Detección automática de entorno (`env_setup.py`)

El archivo `env_setup.py` detecta el entorno de ejecución y configura las rutas de importación sin intervención manual:

| Entorno | Detección | Comportamiento |
|---|---|---|
| Databricks Job (Git) | Detecta commit hash | Construye ruta de Workspace desde el hash |
| Databricks interactivo | Detecta Repo activo | Resuelve ruta desde directorio del Repo |
| Local / Jupyter | No es Databricks | Usa directorio de trabajo actual |

### Clúster Databricks

| Configuración | Valor |
|---|---|
| Runtime | Databricks 15.4 LTS (Spark 3.5, Scala 2.12) |
| Nodo | Standard\_D4s\_v3 |
| Modo | Single Node (sin workers, compatible con plan gratuito) |
| Auto-terminación | 15 minutos de inactividad |
| AQE | Habilitado |
| Arrow | Habilitado |

### Linked Services ADF

| Linked Service | Conexión |
|---|---|
| `ls_adls` | Azure Data Lake Storage Gen2 |
| `ls_databricks` | Azure Databricks con clúster predefinido |
| `ls_databricks_kv_clusterparam` | Databricks con parámetros de clúster desde Key Vault |
| `ls_azure_keyvault` | Azure Key Vault para resolución de secretos |
| `ls_sql_liga1` | Azure SQL Database — base de datos de control y metadatos |

### Dependencias del scraping (Jupyter local)

```bash
pip install selenium webdriver-manager beautifulsoup4 pandas
pip install azure-storage-file-datalake azure-keyvault-secrets azure-identity
pip install unidecode
```

---

*Proyecto desarrollado por Oscar García Del Águila — Lima, Perú.*
