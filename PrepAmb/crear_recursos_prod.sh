#!/bin/bash
# ============================================================
# CREAR RECURSOS PROD — Liga 1 Perú
# Ejecutar desde Azure Cloud Shell o terminal con az login
# Crea todos los recursos prod en rg-liga1, sufijo _prod
#
# Uso:
#   chmod +x crear_recursos_prod.sh
#   ./crear_recursos_prod.sh
# ============================================================

set -e  # detener si algo falla

# ── Variables ──────────────────────────────────────────────
RG="rg-liga1"
LOCATION="eastus"
LOCATION_SQL="westus"          # SQL Server dev está en West US

# Recursos prod (solo agregar _prod al nombre)
ACC_PROD="acc-liga1-prod"
ADF_PROD="adf-ligafutbol-prod"
STORAGE_PROD="datalakelig1peruprod"   # storage account: sin guiones, max 24 chars
DBW_PROD="dbw-liga1-prod"
SQL_SERVER="serverfutbol"             # reutilizar mismo servidor
SQL_DB_PROD="ligafutbol-prod"
KV="kv-liga1-secreto"                 # reutilizar mismo Key Vault

echo "============================================="
echo " Liga 1 Perú — Creando recursos PROD"
echo " Resource Group: $RG"
echo "============================================="

# ── 1. Access Connector prod ───────────────────────────────
echo ""
echo "▶ [1/6] Creando Access Connector: $ACC_PROD"
az databricks access-connector create \
  --name "$ACC_PROD" \
  --resource-group "$RG" \
  --location "$LOCATION" \
  --identity-type SystemAssigned \
  --only-show-errors
echo "✅ Access Connector creado"

# ── 2. Storage Account prod ────────────────────────────────
echo ""
echo "▶ [2/6] Creando Storage Account: $STORAGE_PROD"
az storage account create \
  --name "$STORAGE_PROD" \
  --resource-group "$RG" \
  --location "$LOCATION" \
  --sku Standard_LRS \
  --kind StorageV2 \
  --enable-hierarchical-namespace true \
  --only-show-errors

# Crear 1 container "liga1" (igual que dev: datalakelig1peru/liga1/primera_division/...)
az storage container create \
  --account-name "$STORAGE_PROD" \
  --name "liga1" \
  --auth-mode login \
  --only-show-errors
echo "  → Container: liga1"
echo "✅ Storage Account y container liga1 creados"

# ── 3. Asignar Storage Blob Data Contributor al Access Connector ──
echo ""
echo "▶ [3/6] Asignando rol IAM: $ACC_PROD → $STORAGE_PROD"
STORAGE_ID=$(az storage account show \
  --name "$STORAGE_PROD" \
  --resource-group "$RG" \
  --query id --output tsv)

ACC_PRINCIPAL=$(az databricks access-connector show \
  --name "$ACC_PROD" \
  --resource-group "$RG" \
  --query identity.principalId --output tsv)

az role assignment create \
  --assignee "$ACC_PRINCIPAL" \
  --role "Storage Blob Data Contributor" \
  --scope "$STORAGE_ID" \
  --only-show-errors
echo "✅ Rol IAM asignado"

# ── 4. Databricks Workspace prod ───────────────────────────
echo ""
echo "▶ [4/6] Creando Databricks Workspace: $DBW_PROD (Premium)"
az databricks workspace create \
  --name "$DBW_PROD" \
  --resource-group "$RG" \
  --location "$LOCATION" \
  --sku premium \
  --only-show-errors
echo "✅ Databricks Workspace creado (puede tardar 3-5 min)"

# ── 5. Azure SQL Database prod (mismo servidor) ────────────
echo ""
echo "▶ [5/6] Creando SQL Database: $SQL_DB_PROD en $SQL_SERVER"
az sql db create \
  --name "$SQL_DB_PROD" \
  --server "$SQL_SERVER" \
  --resource-group "$RG" \
  --edition "Basic" \
  --only-show-errors
echo "✅ SQL Database creada"

# ── 6. Azure Data Factory prod ─────────────────────────────
# Nota: az datafactory no está disponible en todas las versiones del CLI.
# Se usa az rest con la API REST de Azure directamente.
echo ""
echo "▶ [6/6] Creando Data Factory: $ADF_PROD"
SUBSCRIPTION_ID=$(az account show --query id --output tsv)
az rest --method PUT \
  --url "https://management.azure.com/subscriptions/${SUBSCRIPTION_ID}/resourceGroups/${RG}/providers/Microsoft.DataFactory/factories/${ADF_PROD}?api-version=2018-06-01" \
  --body "{\"location\": \"${LOCATION}\", \"properties\": {}}" \
  --only-show-errors
echo "✅ Data Factory creada"

# ── 7. Agregar secrets prod al Key Vault existente ─────────
echo ""
echo "▶ [7/7] Agregando secrets prod al Key Vault: $KV"
echo "  (Necesitas ingresar los valores reales de prod)"

STORAGE_KEY=$(az storage account keys list \
  --account-name "$STORAGE_PROD" \
  --resource-group "$RG" \
  --query "[0].value" --output tsv)

az keyvault secret set \
  --vault-name "$KV" \
  --name "storageaccountkey-prod" \
  --value "$STORAGE_KEY" \
  --only-show-errors
echo "  → storageaccountkey-prod: ✅ (valor auto-obtenido)"

echo ""
echo "  ⚠️  Agrega manualmente estos secrets en el KV ($KV):"
echo "     databricks-token-prod   → PAT del workspace $DBW_PROD"
echo "     kv-sql-password-prod    → misma contraseña SQL (mismo servidor)"

# ── Resumen final ──────────────────────────────────────────
echo ""
echo "============================================="
echo " ✅ Todos los recursos prod creados en $RG"
echo "============================================="
echo ""
echo " Próximos pasos MANUALES en Databricks prod:"
echo "  1. Ir a $DBW_PROD → Settings → Unity Catalog → Attach metastore"
echo "  2. Crear Storage Credential usando $ACC_PROD"
echo "  3. Crear External Location → abfss://liga1@${STORAGE_PROD}.dfs.core.windows.net/"
echo "  4. Crear cluster (copiar config de proceso/cluster/*.json)"
echo "     → Anotar Cluster ID para GitHub Secret DATABRICKS_CLUSTER_PROD"
echo "  5. Crear SQL Warehouse (Small) → anotar ID para DATABRICKS_WAREHOUSE_PROD"
echo "  6. Crear PAT → Settings → Developer → Access Tokens"
echo "     → Agregar como GitHub Secret DATABRICKS_TOKEN_PROD"
echo "  7. Agregar secret 'databricks-token-prod' en KV con ese PAT"
echo ""
echo " GitHub Secrets a configurar (Settings → Secrets → Actions):"
DBW_URL=$(az databricks workspace show \
  --name "$DBW_PROD" \
  --resource-group "$RG" \
  --query workspaceUrl --output tsv 2>/dev/null || echo "ver en Azure Portal")
echo "  DATABRICKS_HOST_PROD      = https://$DBW_URL"
echo "  DATABRICKS_TOKEN_PROD     = <PAT generado en el workspace>"
echo "  DATABRICKS_USER_PROD      = <tu email en Databricks>"
echo "  DATABRICKS_CLUSTER_PROD   = <ID del cluster creado>"
echo "  DATABRICKS_WAREHOUSE_PROD = <ID del SQL Warehouse>"
echo "  ADF_RESOURCE_GROUP_PROD   = $RG"
echo "  ADF_FACTORY_NAME_PROD     = $ADF_PROD"
echo "  SQL_SERVER_PROD           = $SQL_SERVER.database.windows.net"
echo "  SQL_DATABASE_PROD         = $SQL_DB_PROD"
echo "  SQL_USERNAME_PROD         = adminliga1"
echo "  SQL_PASSWORD_PROD         = <misma contraseña del servidor>"
echo "============================================="
