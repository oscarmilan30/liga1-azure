# Databricks notebook source
# Databricks notebook source
# ==========================================================
# EXECUTE CREATE JOB FROM JSON
# Proyecto: Liga 1 Perú
# Crea jobs dinámicamente desde archivos JSON
# ==========================================================

# COMMAND ----------
# Celda 1: Configuración inicial y parámetros
from env_setup import *
from utils_liga1 import get_dbutils
import requests
import json
import os

# Inicializar entorno
spark = get_or_create_spark()
dbutils = get_dbutils()

# Recibir parámetro de ADF
dbutils.widgets.text("prm_job_name", "")
dbutils.widgets.text("prm_area", "")

prm_job_name = dbutils.widgets.get("prm_job_name")
prm_area = dbutils.widgets.get("prm_area")

# Configuración de Databricks
DATABRICKS_INSTANCE = prm_area
TOKEN = dbutils.secrets.get(scope="secretliga1", key="databricks-token")

print("CONFIGURACIÓN INICIAL:")
print(f" Job a crear: {prm_job_name}")
print(f"Instancia: {DATABRICKS_INSTANCE}")

# COMMAND ----------
# Celda 2: Función para cargar JSON del job - RUTA CORREGIDA
def cargar_definicion_job(nombre_job):
    """
    Carga la definición del job desde: workflow_deploy/[nombre_job]/[nombre_job].json
    """
    # Construir ruta del archivo JSON
    nombre_archivo = f"{nombre_job}.json"
    ruta_json = get_workspace_path(f"workflow_deploy/{nombre_job}/{nombre_archivo}")
    
    print(f"Buscando archivo JSON en: {ruta_json}")
    
    # Verificar si existe
    if not os.path.exists(ruta_json):
        # Mostrar jobs disponibles para debug
        carpeta_base = get_workspace_path("workflow_deploy")
        jobs_disponibles = []
        if os.path.exists(carpeta_base):
            jobs_disponibles = [f for f in os.listdir(carpeta_base) if os.path.isdir(os.path.join(carpeta_base, f))]
        
        print(f"Jobs disponibles en workflow_deploy: {jobs_disponibles}")
        raise FileNotFoundError(f"No se encontró: {ruta_json}")
    
    # Leer y parsear JSON
    with open(ruta_json, 'r', encoding='utf-8') as archivo:
        job_definition = json.load(archivo)
    
    print(f"JSON cargado: {nombre_archivo}")
    return job_definition

# COMMAND ----------
# Celda 3: Función para crear job via API
def crear_job_databricks(job_definition, job_name):
    """
    Crea un job en Databricks via API REST
    """
    # Endpoint para crear job
    url = f"{DATABRICKS_INSTANCE}/api/2.1/jobs/create"
    
    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/json"
    }
    
    print(f"Creando job: {job_name}")
    print("=" * 50)
    
    # Log de la definición (sin datos sensibles)
    print(f"Configuración del job:")
    print(f"   - Nombre: {job_definition.get('name', 'N/A')}")
    print(f"   - Tareas: {len(job_definition.get('tasks', []))}")
    print(f"   - Timeout: {job_definition.get('timeout_seconds', 'N/A')}s")
    
    # Enviar request
    response = requests.post(url, headers=headers, json=job_definition)
    
    print("=" * 50)
    print(f"📡 Response Status: {response.status_code}")
    
    if response.status_code == 200:
        job_id = response.json()["job_id"]
        print(f"Job creado exitosamente")
        print(f"Job ID: {job_id}")
        return job_id
    else:
        print(f"Error creando job: {response.text}")
        raise Exception(f"API Error: {response.text}")

# COMMAND ----------
# Celda 4: Función para verificar job existente
def verificar_job_existente(job_name):
    """
    Verifica si ya existe un job con el mismo nombre
    """
    url = f"{DATABRICKS_INSTANCE}/api/2.1/jobs/list"
    
    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            jobs = response.json().get("jobs", [])
            for job in jobs:
                if job["settings"]["name"] == job_name:
                    print(f" Job ya existe: {job_name} (ID: {job['job_id']})")
                    return job["job_id"]
        return None
    except Exception as e:
        print(f"Error verificando jobs existentes: {e}")
        return None

# COMMAND ----------
# Celda 5: Ejecución principal CORREGIDA
try:
    print("\nINICIANDO CREACIÓN DE JOB...")
    print("=" * 60)
    
    # VALIDACIÓN INMEDIATA de parámetros
    if not prm_job_name or not prm_job_name.strip():
        raise ValueError("El parámetro 'prm_job_name' está VACÍO")
    
    if not prm_area or not prm_area.strip():
        raise ValueError("El parámetro 'prm_area' está VACÍO")
    
    print(f"Parámetros validados: job_name='{prm_job_name}', area='{prm_area}'")
    
    # 1. Cargar definición del job desde JSON
    job_definition = cargar_definicion_job(prm_job_name)
    
    # 2. Verificar si el job ya existe
    job_name = job_definition.get("name", prm_job_name)
    job_existente_id = verificar_job_existente(job_name)
    
    if job_existente_id:
        print(f"Saltando creación - Job ya existe con ID: {job_existente_id}")
        resultado_json = {
            "status": "EXISTING",
            "job_id": job_existente_id,
            "job_name": job_name,
            "message": f"Job ya existía: {job_existente_id}"
        }
    else:
        # 3. Crear nuevo job
        job_id = crear_job_databricks(job_definition, job_name)
        print(f"Job creado exitosamente: {job_id}")
        resultado_json = {
            "status": "CREATED", 
            "job_id": job_id,
            "job_name": job_name,
            "message": f"Job creado: {job_id}"
        }
    
    print("=" * 60)
    print(f"PROCESO COMPLETADO: {resultado_json['status']}")
    
    # SOLO usar exit() para casos de ÉXITO
    dbutils.notebook.exit(json.dumps(resultado_json))
    
except Exception as e:
    print("=" * 60)
    print(f"ERROR CRÍTICO: {str(e)}")
    
    # NO usar notebook.exit() para errores - DEJAR que falle naturalmente
    # Esto hará que el job marque como FAILED en Databricks
    raise Exception(f"Fallo en creación de job: {str(e)}")