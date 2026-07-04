# Guía de Despliegue — Liga 1 Perú Data Platform

**Proyecto:** Liga 1 Perú — Data Engineering en Azure  
**Autor:** Oscar García Del Águila  
**Fecha:** Junio 2026

Paso a paso para reproducir la arquitectura desde cero usando el **Portal de Azure** (portal.azure.com). Sigue el orden exacto — cada recurso depende del anterior.

---

## Prerrequisitos

- Cuenta de Azure con rol **Owner** o **Contributor** en la suscripción
- Acceso a **Azure Active Directory** para crear Service Principals
- **Power BI Desktop** instalado con el formato PBIP habilitado
- **Python 3.x** instalado localmente (para el scraping)
- Repositorio clonado localmente: `git clone https://github.com/oscarmilan30/liga1-azure.git`

---

## 1. Resource Group

En **portal.azure.com**:

1. Barra de búsqueda → **Grupos de recursos** → **Crear**
2. Suscripción: seleccionar la suscripción activa
3. Nombre del grupo de recursos: `rg-liga1`
4. Región: **East US**
5. Clic en **Revisar y crear** → **Crear**

Todos los recursos siguientes se crean dentro de `rg-liga1`.

---

## 2. Azure Data Lake Storage Gen2

1. Barra de búsqueda → **Cuentas de almacenamiento** → **Crear**
2. Grupo de recursos: `rg-liga1`
3. Nombre de la cuenta: `datalakelig1peru`
4. Región: **East US**
5. Rendimiento: **Estándar** · Redundancia: **LRS**
6. Pestaña **Avanzado** → activar **Espacio de nombres jerárquico** (Hierarchical namespace)
7. **Revisar y crear** → **Crear**

Crear el contenedor principal:

1. Abrir `datalakelig1peru` → **Contenedores** → **+ Contenedor**
2. Nombre: `liga1` · Nivel de acceso público: **Privado**
3. **Crear**

---

## 3. Azure Key Vault

1. Barra de búsqueda → **Almacenes de claves** → **Crear**
2. Grupo de recursos: `rg-liga1`
3. Nombre del almacén: `kv-liga1-secreto`
4. Región: **East US**
5. Plan de tarifa: **Estándar**
6. **Revisar y crear** → **Crear**

---

## 4. Azure SQL Database

### 4.1 Servidor SQL

1. Barra de búsqueda → **Servidores SQL** → **Crear**
2. Grupo de recursos: `rg-liga1`
3. Nombre del servidor: `serverfutbol`
4. Ubicación: **East US**
5. Método de autenticación: **Autenticación de SQL**
6. Inicio de sesión de administrador: `adminliga1`
7. Contraseña: definir y guardar en un lugar seguro
8. **Revisar y crear** → **Crear**

### 4.2 Base de datos

1. Abrir el servidor `serverfutbol` → **Bases de datos SQL** → **+ Crear base de datos**
2. Nombre de la base de datos: `ligafutbol`
3. Proceso y almacenamiento: **Sin servidor** (Serverless) · vCores mínimos: 0.5 · máximos: 2
4. **Revisar y crear** → **Crear**

### 4.3 Reglas de firewall

1. Abrir `serverfutbol` → **Redes** → **Firewalls y redes virtuales**
2. Activar **Permitir que los servicios y recursos de Azure accedan a este servidor**
3. Agregar la IP pública del equipo local para ejecutar scripts desde local
4. **Guardar**

### 4.4 Inicializar el plano de control

1. Abrir `ligafutbol` → **Editor de consultas (versión preliminar)**
2. Iniciar sesión con `adminliga1` y la contraseña configurada
3. Copiar y ejecutar el contenido completo de `PrepAmb/Querys.sql` del repositorio

Esto crea las tablas (`tbl_pipeline`, `tbl_paths`, `tbl_predecesores`, etc.), stored procedures, triggers y registra los 44 pipelines con sus relaciones de predecesor.

---

## 5. Azure Databricks

### 5.1 Workspace

1. Barra de búsqueda → **Azure Databricks** → **Crear**
2. Grupo de recursos: `rg-liga1`
3. Nombre del workspace: `dbw-liga1`
4. Región: **East US**
5. Plan de tarifa: **Premium** (requerido para Unity Catalog)
6. **Revisar y crear** → **Crear**

### 5.2 Access Connector (para Unity Catalog)

1. Barra de búsqueda → **Access Connector for Azure Databricks** → **Crear**
2. Grupo de recursos: `rg-liga1`
3. Nombre: `acc-liga1`
4. Región: **East US**
5. **Revisar y crear** → **Crear**

---

## 6. Azure Data Factory

1. Barra de búsqueda → **Fábricas de datos** → **Crear**
2. Grupo de recursos: `rg-liga1`
3. Nombre: `adf-ligafutbol`
4. Región: **East US**
5. Versión: **V2**
6. **Revisar y crear** → **Crear**

---

## 7. Service Principal (sp-liga1)

El Service Principal permite que GitHub Actions se autentique contra Azure sin credenciales de usuario.

1. Barra de búsqueda → **Azure Active Directory** → **Registros de aplicaciones** → **Nuevo registro**
2. Nombre: `sp-liga1`
3. Tipos de cuenta compatibles: **Solo esta organización**
4. **Registrar**
5. Anotar el **Id. de aplicación (cliente)** y el **Id. de directorio (inquilino)**

Crear el secreto del cliente:

1. Abrir `sp-liga1` → **Certificados y secretos** → **Nuevo secreto de cliente**
2. Descripción: `github-actions` · Expiración: 24 meses
3. **Agregar** → copiar el **Valor** inmediatamente (solo se muestra una vez)

---

## 8. Configurar IAM y Permisos

### 8.1 ADF → Key Vault (Managed Identity)

1. Abrir `kv-liga1-secreto` → **Control de acceso (IAM)** → **Agregar asignación de roles**
2. Rol: **Lector de secretos de Key Vault**
3. Asignar acceso a: **Identidad administrada** → seleccionar `adf-ligafutbol`
4. **Guardar**

### 8.2 Access Connector → ADLS (para Unity Catalog)

1. Abrir `datalakelig1peru` → **Control de acceso (IAM)** → **Agregar asignación de roles**
2. Rol: **Colaborador de datos de Storage Blob**
3. Asignar acceso a: **Identidad administrada** → seleccionar `acc-liga1`
4. **Guardar**

### 8.3 Service Principal → ADF (para GitHub Actions)

1. Abrir `adf-ligafutbol` → **Control de acceso (IAM)** → **Agregar asignación de roles**
2. Rol: **Colaborador de Data Factory**
3. Asignar acceso a: **Usuario, grupo o entidad de servicio** → buscar `sp-liga1`
4. **Guardar**

### 8.4 Service Principal → ADLS (para scraping desde GitHub Actions)

1. Abrir `datalakelig1peru` → **Control de acceso (IAM)** → **Agregar asignación de roles**
2. Rol: **Colaborador de datos de Storage Blob**
3. Asignar acceso a: **Usuario, grupo o entidad de servicio** → buscar `sp-liga1`
4. **Guardar**

---

## 9. Secretos en Azure Key Vault

1. Abrir `kv-liga1-secreto` → **Secretos** → **Generar o importar**
2. Crear un secreto por cada fila de la siguiente tabla:

| Nombre del secreto | Valor |
|---|---|
| `adls-account-key` | Account Key de `datalakelig1peru` (Portal → Claves de acceso) |
| `databricks-pat` | Personal Access Token de `dbw-liga1` (Databricks → Settings → Developer) |
| `sql-server` | `serverfutbol.database.windows.net` |
| `sql-database` | `ligafutbol` |
| `sql-username` | `adminliga1` |
| `sql-password` | Contraseña del administrador SQL |
| `sp-client-id` | Id. de aplicación (cliente) de `sp-liga1` |
| `sp-client-secret` | Valor del secreto de `sp-liga1` |
| `sp-tenant-id` | Id. de directorio (inquilino) |
| `sp-subscription-id` | Id. de la suscripción de Azure |

---

## 10. Configurar Databricks y Unity Catalog

### 10.1 Cluster de cómputo

1. Abrir `dbw-liga1` → **Lanzar workspace**
2. Menú izquierdo → **Compute** → **Create Compute**
3. Nombre: `cluster-liga1`
4. Modo: **Single Node**
5. Runtime: **15.4 LTS (Spark 3.5, Scala 2.12)**
6. Nodo: `Standard_DS3_v2`
7. Activar **Terminate after 30 minutes of inactivity**
8. **Create Compute**

### 10.2 Storage Credential (Unity Catalog)

1. Menú izquierdo → **Catalog** → **External Data** → **Credentials** → **Create credential**
2. Tipo: **Azure Managed Identity**
3. Nombre: `sc-datalakelig1peru`
4. Access Connector ID: ID de recurso de `acc-liga1` (Portal → acc-liga1 → Información esencial → Id. de recurso)
5. **Create**

### 10.3 External Location

1. **External Data** → **External Locations** → **Create location**
2. Nombre: `ext-loc-datalakelig1peru`
3. URL: `abfss://liga1@datalakelig1peru.dfs.core.windows.net/`
4. Storage credential: `sc-datalakelig1peru`
5. **Create**

### 10.4 Catalog

1. **Catalog** → **+** → **Create catalog**
2. Nombre: `catalog_liga1`
3. Storage location: `ext-loc-datalakelig1peru`
4. **Create**

### 10.5 Secret Scope (para acceso a Key Vault desde notebooks)

1. Ir a `https://{workspace-url}#secrets/createScope`
2. Nombre: `secretliga1`
3. Manage Principal: **All Users**
4. DNS Name: URL del Key Vault (`https://kv-liga1-secreto.vault.azure.net/`)
5. Resource ID: Id. de recurso de `kv-liga1-secreto`
6. **Create**

---

## 11. Configurar Azure Data Factory

### 11.1 Conectar al repositorio Git

1. Abrir `adf-ligafutbol` → **Author & Monitor** → **Manage** → **Git configuration** → **Configure**
2. Tipo: **GitHub**
3. Repositorio: `oscarmilan30/liga1-azure`
4. Branch de colaboración: `test-construccion`
5. Branch de publicación: `adf_publish`
6. Carpeta raíz: `/`
7. **Apply**

### 11.2 Linked Services

Crear 4 Linked Services en **Manage** → **Linked services** → **New**:

**ADLS Gen2:**
- Tipo: **Azure Data Lake Storage Gen2**
- Nombre: `ls_adls_liga1`
- Método de autenticación: **Account Key** → referenciar desde Key Vault: secreto `adls-account-key`
- URL de la cuenta: `https://datalakelig1peru.dfs.core.windows.net`

**Azure Databricks:**
- Tipo: **Azure Databricks**
- Nombre: `ls_databricks_liga1`
- Workspace: seleccionar `dbw-liga1`
- Autenticación: **Access token** → referenciar desde Key Vault: secreto `databricks-pat`
- Cluster existente: seleccionar `cluster-liga1`

**Azure SQL:**
- Tipo: **Azure SQL Database**
- Nombre: `ls_sql_liga1`
- Servidor: `serverfutbol.database.windows.net`
- Base de datos: `ligafutbol`
- Autenticación: **SQL Authentication** → usuario `adminliga1` → contraseña desde Key Vault: secreto `sql-password`

**Azure Key Vault:**
- Tipo: **Azure Key Vault**
- Nombre: `ls_keyvault_liga1`
- URL: `https://kv-liga1-secreto.vault.azure.net/`
- Autenticación: **Managed Identity** (ADF usa su identidad administrada)

Clic en **Publish All** para publicar los Linked Services.

---

## 12. Configurar GitHub Actions

En el repositorio de GitHub → **Settings** → **Secrets and variables** → **Actions** → **New repository secret**. Crear un secreto por cada fila:

| Nombre del secret | Valor |
|---|---|
| `AZURE_TENANT_ID` | Id. de directorio (inquilino) de Azure |
| `AZURE_CLIENT_ID` | Id. de aplicación (cliente) de `sp-liga1` |
| `AZURE_CLIENT_SECRET` | Valor del secreto de `sp-liga1` |
| `ADF_SUBSCRIPTION_ID` | Id. de la suscripción de Azure |
| `ADF_RESOURCE_GROUP` | `rg-liga1` |
| `ADF_FACTORY_NAME` | `adf-ligafutbol` |
| `ADF_KV_URL` | `https://kv-liga1-secreto.vault.azure.net/` |
| `ADF_SQL_SERVER` | `serverfutbol.database.windows.net` |
| `ADF_SQL_DATABASE` | `ligafutbol` |

---

## 13. Primer Run E2E

### 13.1 Ejecutar el workflow de GitHub Actions

1. Repositorio GitHub → pestaña **Actions**
2. Seleccionar **Liga 1 - Web Scraping** (`liga1-scraping.yml`)
3. Clic en **Run workflow**
4. Parámetros:
   - `modo`: `incremental`
   - `anio`: año actual (ej: `2026`)
5. Clic en **Run workflow**

El workflow ejecuta el scraping, sube los archivos a ADLS y dispara `pl_Orchestrator_E2E_liga1` en ADF.

### 13.2 Monitorear en ADF

1. Abrir `adf-ligafutbol` → **Monitor** → **Pipeline runs**
2. Verificar que `pl_Orchestrator_E2E_liga1` aparezca en estado **In Progress** y luego **Succeeded**

### 13.3 Verificar datos en Databricks

1. Workspace `dbw-liga1` → **Catalog** → `catalog_liga1`
2. Verificar que existan las schemas `tb_rdv`, `tb_udv`, `tb_ddv` y `vw_ddv` con datos

---

## 14. Configurar Power BI — Modelo Analítico

1. Abrir **Power BI Desktop**
2. **Archivo** → **Opciones y configuración** → **Opciones** → **Características de vista previa** → activar **Almacenar archivos de Power BI usando formato basado en Power BI Project (.pbip)**
3. **Archivo** → **Abrir** → navegar a `dashboard/Liga 1 Perú - Modelo Analítico v2.pbip`
4. Al abrir, Power BI pedirá credenciales para conectar al Databricks SQL Warehouse
5. Ingresar el token PAT de Databricks cuando se solicite
6. Clic en **Actualizar** para importar los datos

---

## 15. Despliegue ML — ft_score_ml

La capa ML se ejecuta automáticamente como último paso del pipeline ADF E2E (`pl_Orchestrator_E2E_liga1`), después de DDV. Los pasos de esta sección solo son necesarios la primera vez para crear los objetos Delta y configurar Delta Sharing.
## 15. Despliegue ML — ft_score_ml

La capa ML se ejecuta automáticamente como último paso del pipeline ADF E2E, después de DDV. Los pasos de esta sección solo son necesarios la primera vez.

### 15.1 Crear la tabla Delta ft_score_ml

1. En Databricks → **Workspace** → abrir `PrepAmb/ddl_deploy/ddl_ft_score_ml/ddl_ft_score_ml.sql`
2. Ejecutar — crea `catalog_liga1.tb_ddv.ft_score_ml` como tabla Delta
3. Abrir `PrepAmb/ddl_deploy/ddl_ft_score_ml/ddl_vista_ft_score_ml.sql` → ejecutar (vista para Delta Sharing)

### 15.2 Configurar Delta Sharing

1. Databricks → **Catalog** → **Delta Sharing** → **Shares** → **+ Share**
2. Nombre: `share_scouting_ml` → agregar tabla `catalog_liga1.tb_ddv.ft_score_ml`
3. Crear recipient → descargar archivo `.share`
4. En Power BI Desktop: **Obtener datos** → **Delta Sharing** → adjuntar el `.share`

### 15.3 Ejecutar el notebook ML

1. Abrir `proceso/frm_ml/notebooks/ft_score_ml/nb_ft_score_ml.py` en Databricks
2. Ejecutar manualmente o disparar job `sch_ml_liga1`
3. Verificar en **Catalog** que `ft_score_ml` tiene columnas `score_100`, `nivel_ml`, `cluster_ml`

---

## 16. Despliegue a Producción — CI/CD Automatizado

El entorno de producción está **completamente aislado** de dev — usa `catalog_liga1_prod` apuntando a `datalakelig1peruprod`, con su propio secrets scope `secretliga1` (valores prod) y su propio ADF `adf-ligafutbol-prod`. El despliegue es automático via GitHub Actions al hacer merge a `main`.

### Arquitectura de aislamiento prod (Opción A)

| Componente | Dev | Prod |
|---|---|---|
| Storage | `datalakelig1peru` | `datalakelig1peruprod` |
| Unity Catalog | `catalog_liga1` | `catalog_liga1_prod` |
| Secrets scope `secretliga1` (Databricks) | valores dev | valores prod (GitHub Actions) |
| Secret `catalogname` (scope) | no existe → fallback | `catalog_liga1_prod` |
| ADF | `adf-ligafutbol-prod-dev` | `adf-ligafutbol-prod` |
| SQL Database | `ligafutbol` | `ligafutbol-prod` |
| Delta Share | `liga1_share` | `liga1_share_prod` |
| Delta Recipient | `liga1_powerbi` | `liga1_powerbi_prod` |

`write_delta_udv()` y `execute_job_ddl.py` leen `catalogname` del secrets scope — en prod usan `catalog_liga1_prod` automáticamente, sin tocar los notebooks.

### 16.1 Fase 1 — Crear recursos Azure (Cloud Shell)

```bash
curl -sO https://raw.githubusercontent.com/oscarmilan30/liga1-azure/main/PrepAmb/crear_recursos_prod.sh
chmod +x crear_recursos_prod.sh
./crear_recursos_prod.sh
```

El script crea: Access Connector `acc-liga1-prod`, Storage Account `datalakelig1peruprod` con container `liga1`, IAM, Databricks Workspace Premium `dbw-liga1-prod`, SQL Database `ligafutbol-prod`, ADF `adf-ligafutbol-prod`, y secret `storageaccountkey-prod` en Key Vault.

Agregar los secrets adicionales en Key Vault:

```bash
# Password SQL para prod (reutiliza el mismo servidor, misma contraseña)
az keyvault secret set --vault-name kv-liga1-secreto \
  --name kv-sql-password-prod \
  --value "$(az keyvault secret show --vault-name kv-liga1-secreto --name kv-sql-password --query value -o tsv)"

# Storage account prod (nombre para el scraping)
az keyvault secret set --vault-name kv-liga1-secreto \
  --name storageaccount-prod \
  --value "datalakelig1peruprod"
```

### 16.2 Fase 1b — IAM de producción (verificar antes del deploy)

Los permisos mínimos necesarios para que el GitHub Actions y los pipelines funcionen en prod:

| Recurso | Identidad | Rol | Para qué |
|---|---|---|---|
| `rg-liga1` (Resource Group) | `sp-liga1` | `Data Factory Contributor` | Desplegar Linked Services, Datasets, Pipelines y Global Parameters en ADF |
| `kv-liga1-secreto` | `sp-liga1` | `Key Vault Secrets Officer` | El Actions lee/escribe secretos KV (ej. storageaccount-prod) |
| `kv-liga1-secreto` | MI de `adf-ligafutbol-prod` | `Key Vault Secrets User` | ADF prod lee secretos en runtime (credenciales SQL, Databricks token, ADLS key). **Verificar que esté asignada — la MI de dev `adf-ligafutbol` NO es suficiente** |
| `datalakelig1peruprod` | `acc-liga1-prod` (Access Connector) | `Storage Blob Data Contributor` | Databricks accede a ADLS prod vía Unity Catalog External Location |

> **⚠️ Paso crítico manual (una sola vez antes del primer deploy):**
>
> **Paso 1 — Habilitar Managed Identity en ADF prod:**
> Portal → `adf-ligafutbol-prod` → Settings → **Identity** → System assigned → Status: **On** → Save
> (Si no está On, la MI no existe y no aparecerá en los selectores de IAM de otros recursos)
>
> **Paso 2 — Asignar rol en Key Vault:**
> Portal → `kv-liga1-secreto` → Access control (IAM) → **+ Add** → Add role assignment → `Key Vault Secrets User` → Managed identity → **`adf-ligafutbol-prod`**
>
> Resultado esperado en KV → IAM: 4 entradas bajo `Key Vault Secrets User`: `adf-ligafutbol` (dev), `adf-ligafutbol-prod` (prod), `AzureDatabricks`, `unity-catalog-access-connector`. Sin `adf-ligafutbol-prod`, los pipelines ADF prod fallarán al leer credenciales en runtime.

### 16.2 Fase 2 — Configurar Databricks prod (manual)

En `dbw-liga1-prod`:

1. **Unity Catalog**: adjuntar el mismo metastore que dev (cuenta-nivel, compartido)
2. **Catalog Explorer → + Add → Storage Credential**: tipo Managed Identity, usar `acc-liga1-prod`, nombre `sc-liga1-prod`
3. **Catalog Explorer → + Add → External Location**:
   - URL: `abfss://liga1@datalakelig1peruprod.dfs.core.windows.net/`
   - Credential: `sc-liga1-prod`
   - Nombre: `el-liga1-prod`
4. **Compute → Create compute**: Runtime 15.4 LTS, Single node — anotar **Cluster ID**
5. **SQL Warehouses**: anotar **Warehouse ID** (Serverless Starter o nuevo)
6. **Settings → Developer → Access tokens → Generate**: anotar PAT `dapi...`

```bash
az keyvault secret set --vault-name kv-liga1-secreto \
  --name databricks-token-prod --value "dapi..."
```

### 16.3 Fase 3 — Configurar GitHub Secrets

GitHub repo → **Settings → Secrets and variables → Actions**:

| Secret | Valor |
|---|---|
| `DATABRICKS_HOST_PROD` | `https://adb-XXXXXX.azuredatabricks.net` |
| `DATABRICKS_TOKEN_PROD` | PAT generado |
| `DATABRICKS_USER_PROD` | Email en Databricks prod |
| `DATABRICKS_CLUSTER_PROD` | Cluster ID (ej. `0704-032223-khisrcma`) |
| `DATABRICKS_WAREHOUSE_PROD` | Warehouse ID |
| `ADF_RESOURCE_GROUP_PROD` | `rg-liga1` |
| `ADF_FACTORY_NAME_PROD` | `adf-ligafutbol-prod` |
| `SQL_SERVER_PROD` | `serverfutbol.database.windows.net` |
| `SQL_DATABASE_PROD` | `ligafutbol-prod` |
| `SQL_USERNAME_PROD` | `adminliga1` |
| `SQL_PASSWORD_PROD` | Contraseña SQL |
| `STORAGE_ACCOUNT_PROD` | `datalakelig1peruprod` |
| `AZURE_SUBSCRIPTION_ID` | ID suscripción |
| `AZURE_CLIENT_ID` | Client ID de `sp-liga1` |
| `AZURE_CLIENT_SECRET` | Secret de `sp-liga1` |
| `AZURE_TENANT_ID` | Tenant ID Azure |

### 16.4 Fase 4 — Merge y deploy automático

```bash
git checkout develop && git merge academico && git push origin develop
git checkout main    && git merge develop   && git push origin main
```

El push a `main` dispara `liga1-deploy-prod.yml` con estos jobs en orden:

**Job 1 — deploy-adf**
- Verifica si `adf-ligafutbol-prod` existe (lo crea vía REST solo si no existe)
- Imprime el Principal ID de la Managed Identity del ADF (el rol KV se asigna manualmente — ver sección 16.2 Fase 1b)
- Prepara Linked Services con sed: storage prod, `databricks-token-prod`, `kv-sql-password-prod`, BD prod
- Prepara Pipelines: reemplaza `databricks-token` → `databricks-token-prod` en los dos pipelines que lo tienen hardcodeado
- Despliega 29 pipelines en orden de dependencia
- Configura Global Parameters: `AREA` (host prod), `USER` (email prod), `CLUSTER_ID` (cluster prod)

**Job 2 — deploy-sql**
- Ejecuta `PrepAmb/Querys.sql` en `ligafutbol-prod` (plano de control: tablas, SP, datos base)

**Job 3 — deploy-databricks** (depende de job 2)
- Crea/actualiza Repo en workspace prod (branch `main`)
- Crea secrets scope `secretliga1` con valores prod:
  - `filesystemname = liga1`
  - `storageaccount = datalakelig1peruprod`
  - `catalogname = catalog_liga1_prod` ← aislamiento total
  - credenciales SQL prod
  - `databricks-token` prod
- Crea `catalog_liga1_prod` con `MANAGED LOCATION` en `datalakelig1peruprod` (External Location `el-liga1-prod`)
- Crea 5 schemas: `tb_udv`, `vw_udv`, `tb_ddv`, `vw_ddv`, `shared` + Volume `libs`
- Ejecuta todos los DDL de `PrepAmb/ddl_deploy/` reemplazando variables:
  - `${catalog_name}` → `catalog_liga1_prod`
  - `${storage_account}` → `datalakelig1peruprod`
  - `${container_name}` → `liga1`
  - `${ruta_base}` → `primera_division`
  - referencias hardcodeadas `catalog_liga1.` → `catalog_liga1_prod.`
- Configura **Delta Sharing prod**: `liga1_share_prod` con 5 vistas, recipient `liga1_powerbi_prod`, GRANT SELECT
- **Imprime el Activation Link** en el log de GitHub Actions (copiar para Power BI)
- Sube wheel `liga1_utils` a `/Volumes/catalog_liga1_prod/shared/libs/`
- Crea los 5 jobs Databricks (4 schedules + execute_job_ddl)

**Job 4 — actualizar-job-ids** (depende de jobs 2 y 3)
- Actualiza `dbo.tbl_pipeline_parametros` (columna `Valor` donde `Parametro='JOB_ID'`) con los IDs reales asignados por Databricks a cada job en `ligafutbol-prod`

### 16.6 Idempotencia — qué ocurre al volver a hacer push a `main`

Cada push a `main` re-ejecuta el workflow completo. **Todos los pasos son idempotentes**; ningún push posterior borra datos:

| Paso | Mecanismo | Efecto en re-ejecución |
|---|---|---|
| ADF (linked services, pipelines) | `az datafactory ... create` = upsert | Actualiza sin borrar |
| Azure SQL (`Querys.sql`) | Solo corre si `tbl_pipeline` no existe | **Omitido** — historial de `tbl_control_ejecucion` conservado |
| Catalog / schemas / Volume | `CREATE ... IF NOT EXISTS` | Sin efecto |
| DDL tablas Delta | `CREATE OR REPLACE TABLE ... LOCATION` | **Tablas externas** — el DDL recrea solo los metadatos (definición de columnas); los archivos Parquet y el Delta log en ADLS **no se tocan** |
| DDL vistas | `CREATE OR REPLACE VIEW` | Recrea la vista (sin datos) |
| Wheel | Sobreescribe el `.whl` en el Volume | Sin pérdida |
| Delta Sharing | `CREATE ... IF NOT EXISTS` + `ALTER SHARE ADD VIEW` | Sin efecto si ya existe |
| Jobs Databricks | `jobs/reset` si existe, `jobs/create` si no | Actualiza configuración |
| Job IDs en SQL | `UPDATE tbl_pipeline_parametros SET Valor = ...` | Actualiza el valor |

**¿Por qué las tablas Delta son seguras con `CREATE OR REPLACE`?**
Las tablas DDL usan `LOCATION 'abfss://liga1@datalakelig1peruprod...'`, que apunta a una ruta **fuera** del `MANAGED LOCATION` del catalog (`unity-catalog/`). Databricks las trata como **tablas externas**: el `DROP` implícito del `CREATE OR REPLACE` elimina solo el registro de metadatos en Unity Catalog; los archivos de datos y el Delta log en ADLS permanecen intactos. Al recrear la tabla apuntando al mismo `LOCATION`, Delta lee el log existente y todos los datos anteriores siguen disponibles.

### 16.5 Fase 5 — Primera ejecución E2E

Una vez desplegado, cargar datos iniciales en prod:

**Opción A — Histórico completo (recomendada primera vez):**
1. GitHub Actions → `liga1-scraping.yml` → **Run workflow**
   - `modo`: `historico`
   - `anio_inicio`: `2020`
   - `ambiente`: `prod`
   - `disparar_adf`: `true`

Esto scrapea FotMob + Transfermarkt + Wikipedia hacia `datalakelig1peruprod/liga1/primera_division/landing/` y al terminar dispara automáticamente `pl_Orchestrator_E2E_liga1` en `adf-ligafutbol-prod`.

**Opción B — Solo ADF (si ya tienes datos en landing prod):**
1. GitHub Actions → `liga1-trigger-adf.yml` → **Run workflow**
   - `modo`: `HISTORICO`
   - `ambiente`: `prod`

### 16.6 Fase 6 — Power BI conectar a prod

**Modelo Analítico** (`Liga1_Dashboard.pbip`):

1. Power BI Desktop → **Transformar datos → Configuración del origen de datos**
2. Cambiar el SQL Warehouse por el de `dbw-liga1-prod`
3. Cambiar el catálogo de `catalog_liga1` a `catalog_liga1_prod`
4. Publicar en Power BI Service

**Modelo Scouting ML** (Delta Sharing):

1. En el log del job 3 de GitHub Actions, copiar el **Activation Link** de `liga1_powerbi_prod`
2. Power BI Desktop → **Obtener datos → Delta Sharing**
3. Pegar el Activation Link y autenticar
4. Las tablas aparecen desde `catalog_liga1_prod.vw_ddv` vía Delta Sharing

### 16.7 Schedule automático

Para que prod corra semanalmente, descomentar en `liga1-scraping.yml`:

```yaml
schedule:
  - cron: '0 14 * * 1'   # lunes 09:00 AM hora Perú (UTC-5)
```

Y agregar `ambiente: prod` como input por defecto en el schedule (o disparar manualmente con `ambiente=prod`).

### 16.8 Verificar y capturar evidencias

Guardar capturas en `evidencias/`:

- GitHub Actions: los 4 jobs en ✅, Activation Link visible en logs del job 3
- ADF prod: 29 pipelines publicados + Global Parameters configurados
- Databricks prod: `catalog_liga1_prod` con 5 schemas, Volume y tablas
- Unity Catalog: `liga1_share_prod` con 5 vistas y recipient `liga1_powerbi_prod`
- Databricks prod: 5 jobs con status RUNNING tras primera ejecución E2E
- Query sobre `ligafutbol-prod.dbo.TB_PIPELINES` mostrando Job IDs
- Power BI: modelos conectados a prod con datos reales

---

*Guía de Despliegue — Liga 1 Perú Data Engineering Platform · Oscar García Del Águila · 2025–2026*
