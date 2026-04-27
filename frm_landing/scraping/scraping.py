"""
SCRAPING COMPLETO LIGA 1 PERÚ - VERSIÓN CORREGIDA CON AÑO DINÁMICO
==========================================================
SISTEMA UNIFICADO CON MÚLTIPLES MODOS DE EJECUCIÓN
Mejoras implementadas:
  ✅ AÑO MÁXIMO DINÁMICO: Se toma del sistema, no hardcodeado
  ✅ Modo Histórico: Rango de años (2020-año_actual) → Guarda mismos años
  ✅ Modo Incremental: Solo año actual → Guarda mismo año
  ✅ Modo Reproceso: Año específico → Misma lógica que histórico
  ✅ Reintentos inteligentes por año con manejo de errores
  ✅ Continuación automática después de fallos
  ✅ Optimización de logs: Menos prints, más eficiente
  ✅ SOLO Reporte de archivos: Local y ADLS
  ✅ Estado por año: éxito/error/reintentos
  ✅ LÓGICA CORREGIDA: Entrada X → FotMob: X, Transfermarkt: X-1, Guardado: X
  ✅ VALIDACIÓN DE ARCHIVOS: 9 archivos para 2020-2024, 8 archivos para año actual
  ✅ TRIGGERS PARA ADF: Archivos en landing/temp/ejecucion/ con timestamp
  ✅ LOGS COMPLETOS: Ejecución detallada local y ADLS
==========================================================
"""

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime, timedelta
from unidecode import unidecode
from concurrent.futures import ThreadPoolExecutor
from azure.keyvault.secrets import SecretClient
from azure.identity import InteractiveBrowserCredential, ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient
import pandas as pd
import time
import unicodedata
import queue
import json
import os
import re
import traceback

# =============================================================================
# CONFIGURACIÓN GLOBAL CON AÑO DINÁMICO
# =============================================================================

_AÑO_ACTUAL = datetime.now().year
_AÑO_MINIMO = 2020
_AÑO_MAXIMO = _AÑO_ACTUAL  # ¡CORREGIDO! Ya no es 2025 hardcodeado

_ADLS_CLIENT = None
_CREDENTIAL = None
_ARCHIVOS_PROCESADOS = {}
_EJECUCION_LOG = []

# =============================================================================
# MÓDULO TRIGGERS Y LOGS PARA ADF
# =============================================================================

def registrar_ejecucion(accion, detalles=""):
    """Registra cada paso de la ejecución para el log completo"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    registro = {
        "timestamp": timestamp,
        "accion": accion,
        "detalles": detalles
    }
    _EJECUCION_LOG.append(registro)
    
    # También imprimir en consola
    print(f"[{timestamp}] {accion} {detalles}")

def guardar_trigger_adf(año_guardado, modo, archivos_generados, estado="completado", adls_client=None):
    """
    Crea UN SOLO archivo trigger para ADF que se sobreescribe siempre
    """
    try:
        # Generar timestamp único para el ID, pero el archivo siempre se llama igual
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        trigger_id = f"trigger_{año_guardado}_{timestamp}"
        
        trigger_data = {
            "proceso": "scraping_liga1",
            "modo_ejecucion": modo,
            "año_procesado": año_guardado,
            "estado": estado,
            "archivos_generados": archivos_generados,
            "total_archivos": len(archivos_generados),
            "timestamp_ejecucion": datetime.now().isoformat(),
            "trigger_id": trigger_id,
            "version": "2.0"
        }
        
        # 1. GUARDADO LOCAL - SIEMPRE EL MISMO ARCHIVO
        trigger_local_path = guardar_trigger_local(trigger_data)
        
        # 2. GUARDADO EN ADLS - SIEMPRE EL MISMO ARCHIVO
        trigger_adls_path = None
        if adls_client and estado == "completado":
            trigger_adls_path = guardar_trigger_adls(trigger_data, adls_client)
        
        registrar_ejecucion("TRIGGER_CREADO", f"ID: {trigger_id}, Estado: {estado}")
        print(f"🎯 TRIGGER ADF CREADO: {trigger_id}")
        print(f"   - Modo: {modo}")
        print(f"   - Año: {año_guardado}") 
        print(f"   - Archivos: {len(archivos_generados)}")
        print(f"   - Local: {trigger_local_path}")
        if trigger_adls_path:
            print(f"   - ADLS: {trigger_adls_path}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error creando trigger ADF: {e}")
        return False

def guardar_trigger_local(trigger_data):
    """Guarda trigger localmente - SIEMPRE EL MISMO ARCHIVO"""
    try:
        # Ruta fija para el trigger
        base_local_path = r"C:\Users\milu_\Documents\Proyectos\liga1\triggers"
        os.makedirs(base_local_path, exist_ok=True)
        
        # SIEMPRE EL MISMO NOMBRE
        trigger_path = os.path.join(base_local_path, "scraping_completado.json")
        
        with open(trigger_path, 'w', encoding='utf-8') as f:
            json.dump(trigger_data, f, ensure_ascii=False, indent=2)
        
        return trigger_path
        
    except Exception as e:
        print(f"❌ Error guardando trigger local: {e}")
        return None

def guardar_trigger_adls(trigger_data, adls_client):
    """Guarda trigger en ADLS - SIEMPRE EL MISMO ARCHIVO"""
    try:
        # Ruta fija en ADLS
        adls_path = "landing/temp/ejecucion/scraping_completado.json"
        
        # Convertir a JSON
        json_content = json.dumps(trigger_data, ensure_ascii=False, indent=2)
        
        # Subir a ADLS - SIEMPRE SOBREESCRIBE EL MISMO ARCHIVO
        file_client = adls_client.get_file_client(adls_path)
        file_client.upload_data(json_content, overwrite=True)
        
        return adls_path
        
    except Exception as e:
        print(f"❌ Error guardando trigger ADLS: {e}")
        return None

def guardar_log_unico(año_guardado, modo, adls_client=None):
    """Guarda UN SOLO log de la ejecución completa"""
    try:
        log_data = {
            "ejecucion_id": f"ejecucion_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "modo": modo,
            "año_procesado": año_guardado,
            "fecha_inicio": _EJECUCION_LOG[0]['timestamp'] if _EJECUCION_LOG else datetime.now().isoformat(),
            "fecha_fin": datetime.now().isoformat(),
            "total_pasos": len(_EJECUCION_LOG),
            "pasos": _EJECUCION_LOG
        }
        
        # 1. Guardado local - SIEMPRE EL MISMO ARCHIVO
        base_local_path = r"C:\Users\milu_\Documents\Proyectos\liga1\logs"
        os.makedirs(base_local_path, exist_ok=True)
        
        log_local_path = os.path.join(base_local_path, "ultima_ejecucion.json")
        with open(log_local_path, 'w', encoding='utf-8') as f:
            json.dump(log_data, f, ensure_ascii=False, indent=2)
        
        # 2. Guardado ADLS - SIEMPRE EL MISMO ARCHIVO
        if adls_client:
            try:
                adls_log_path = "landing/temp/logs/ultima_ejecucion.json"
                json_content = json.dumps(log_data, ensure_ascii=False, indent=2)
                file_client = adls_client.get_file_client(adls_log_path)
                file_client.upload_data(json_content, overwrite=True)
                registrar_ejecucion("LOG_GUARDADO_ADLS", f"Ruta: {adls_log_path}")
            except Exception as e:
                registrar_ejecucion("ERROR_LOG_ADLS", f"Error: {str(e)}")
        
        registrar_ejecucion("LOG_GUARDADO_LOCAL", f"Ruta: {log_local_path}")
        return True
        
    except Exception as e:
        registrar_ejecucion("ERROR_LOG_GUARDADO", f"Error: {str(e)}")
        return False
        
def contar_archivos_generados(año_guardado):
    """Cuenta y lista todos los archivos generados para un año"""
    base_path = r"C:\Users\milu_\Documents\Proyectos\liga1\data"
    año_path = os.path.join(base_path, str(año_guardado))
    
    if not os.path.exists(año_path):
        return []
    
    archivos = []
    for archivo in os.listdir(año_path):
        if archivo.endswith(('.csv', '.json', '.txt')):
            archivo_path = os.path.join(año_path, archivo)
            tamaño = os.path.getsize(archivo_path)
            archivos.append({
                "nombre": archivo,
                "tamaño_bytes": tamaño,
                "tamaño_mb": round(tamaño / (1024 * 1024), 2)
            })
    
    return archivos

def guardar_log_ejecucion(año_guardado, modo, adls_client=None):
    """Guarda log completo de la ejecución"""
    try:
        log_data = {
            "ejecucion_id": f"ejecucion_{año_guardado}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "modo": modo,
            "año_procesado": año_guardado,
            "fecha_inicio": _EJECUCION_LOG[0]['timestamp'] if _EJECUCION_LOG else datetime.now().isoformat(),
            "fecha_fin": datetime.now().isoformat(),
            "total_pasos": len(_EJECUCION_LOG),
            "pasos": _EJECUCION_LOG
        }
        
        # 1. Guardado local
        fecha_carpeta = datetime.now().strftime("%Y/%m/%d")
        base_local_path = r"C:\Users\milu_\Documents\Proyectos\liga1\logs"
        log_dir = os.path.join(base_local_path, fecha_carpeta)
        os.makedirs(log_dir, exist_ok=True)
        
        log_local_path = os.path.join(log_dir, f"{log_data['ejecucion_id']}.json")
        with open(log_local_path, 'w', encoding='utf-8') as f:
            json.dump(log_data, f, ensure_ascii=False, indent=2)
        
        # 2. Guardado ADLS
        if adls_client:
            try:
                adls_log_path = f"landing/temp/logs/{fecha_carpeta}/{log_data['ejecucion_id']}.json"
                json_content = json.dumps(log_data, ensure_ascii=False, indent=2)
                file_client = adls_client.get_file_client(adls_log_path)
                file_client.upload_data(json_content, overwrite=True)
                registrar_ejecucion("LOG_GUARDADO_ADLS", f"Ruta: {adls_log_path}")
            except Exception as e:
                registrar_ejecucion("ERROR_LOG_ADLS", f"Error: {str(e)}")
        
        registrar_ejecucion("LOG_GUARDADO_LOCAL", f"Ruta: {log_local_path}")
        return True
        
    except Exception as e:
        registrar_ejecucion("ERROR_LOG_GUARDADO", f"Error: {str(e)}")
        return False

# =============================================================================
# FUNCIONES EXISTENTES DEL SCRAPING
# =============================================================================

def registrar_archivo(nombre_archivo, estado, detalles=""):
    """Registra el estado individual de cada archivo"""
    _ARCHIVOS_PROCESADOS[nombre_archivo] = {
        'estado': estado,
        'detalles': detalles,
        'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    
    if estado == "éxito":
        print(f"✅ {nombre_archivo} - GUARDADO EXITOSO")
    elif estado == "error":
        print(f"❌ {nombre_archivo} - ERROR: {detalles}")

def generar_reporte_archivos():
    """Genera un reporte detallado de todos los archivos"""
    reporte = []
    reporte.append("=" * 70)
    reporte.append("📊 REPORTE DETALLADO DE ARCHIVOS - SCRAPING LIGA 1")
    reporte.append("=" * 70)
    
    exitosos = 0
    errores = 0
    otros = 0
    
    for archivo, info in _ARCHIVOS_PROCESADOS.items():
        estado_char = "✅" if info['estado'] == "éxito" else "❌" if info['estado'] == "error" else "⚠️"
        reporte.append(f"{estado_char} {archivo} | Estado: {info['estado']} | Hora: {info['timestamp']}")
        
        if info['estado'] == "éxito":
            exitosos += 1
        elif info['estado'] == "error":
            errores += 1
        else:
            otros += 1
    
    reporte.append("=" * 70)
    reporte.append(f"📈 RESUMEN: {exitosos} exitosos, {errores} errores, {otros} otros")
    reporte.append("=" * 70)
    
    return "\n".join(reporte)

def validar_archivos_generados(año_guardado):
    """
    VALIDACIÓN POR AÑO GUARDADO - VERSIÓN DINÁMICA
    ==============================================
    - Años 2020-(año_actual-1): 9 archivos obligatorios
    - Año año_actual: 8 archivos obligatorios (campeones opcional)
    """
    base_path = r"C:\Users\milu_\Documents\Proyectos\liga1\data"
    año_path = os.path.join(base_path, str(año_guardado))
    
    if not os.path.exists(año_path):
        return False, "No existe directorio del año"
    
    # Año actual = 8 archivos obligatorios (campeones opcional)
    if año_guardado == _AÑO_ACTUAL:
        archivos_obligatorios = [
            "equipos", "tablas_clasificacion", "partidos", "estadisticas_partidos",
            "liga1", "plantillas", "estadios", "entrenadores"
        ]
        archivos_opcionales = ["campeones"]
    else:
        # Años anteriores = 9 archivos obligatorios
        archivos_obligatorios = [
            "equipos", "tablas_clasificacion", "partidos", "estadisticas_partidos",
            "liga1", "plantillas", "estadios", "entrenadores", "campeones"
        ]
        archivos_opcionales = []
    
    archivos_faltantes = []
    
    for archivo in archivos_obligatorios:
        json_path = os.path.join(año_path, f"{archivo}_{año_guardado}.json")
        csv_path = os.path.join(año_path, f"{archivo}_{año_guardado}.csv")
        
        if not os.path.exists(json_path) and not os.path.exists(csv_path):
            archivos_faltantes.append(archivo)
    
    if archivos_faltantes:
        return False, f"Faltan archivos obligatorios: {archivos_faltantes}"
    
    return True, f"OK - {len(archivos_obligatorios)} archivos obligatorios presentes"

def guardar_reporte_archivos(año_guardado, adls_client=None, adls_conectado=False):
    """Guarda el reporte de archivos SOLO si la validación es exitosa - EN AMBOS LADOS"""
    try:
        # Primero validar que los archivos necesarios estén presentes
        validacion_ok, mensaje_validacion = validar_archivos_generados(año_guardado)
        
        if not validacion_ok:
            log_error(f"❌ NO se genera reporte - Validación fallida: {mensaje_validacion}")
            return False
        
        # Solo generar reporte si la validación es exitosa
        reporte = generar_reporte_archivos()
        
        # Guardar localmente SIEMPRE cuando validación OK
        base_path = r"C:\Users\milu_\Documents\Proyectos\liga1\data"
        año_path = os.path.join(base_path, str(año_guardado))
        os.makedirs(año_path, exist_ok=True)
        
        reporte_local_path = os.path.join(año_path, f"reporte_archivos_{año_guardado}.txt")
        with open(reporte_local_path, 'w', encoding='utf-8') as f:
            f.write(reporte)
        
        registrar_ejecucion("REPORTE_GUARDADO_LOCAL", f"Ruta: {reporte_local_path}")
        
        # Guardar en ADLS SIEMPRE cuando validación OK y ADLS conectado
        if adls_conectado and adls_client:
            try:
                adls_path = f"landing/{año_guardado}/reporte_archivos_{año_guardado}.txt"
                file_client = adls_client.get_file_client(adls_path)
                file_client.upload_data(reporte, overwrite=True)
                registrar_ejecucion("REPORTE_GUARDADO_ADLS", f"Ruta: {adls_path}")
            except Exception as e:
                registrar_ejecucion("ERROR_REPORTE_ADLS", f"Error: {str(e)}")
                # No retornar False aquí, porque el guardado local fue exitoso
        
        return True
        
    except Exception as e:
        registrar_ejecucion("ERROR_REPORTE", f"Error: {str(e)}")
        return False

def log_info(mensaje):
    """Log optimizado - solo mensajes importantes"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    mensaje_completo = f"[{timestamp}] {mensaje}"
    print(mensaje_completo)

def log_error(mensaje, excepcion=None):
    """Log de errores"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    mensaje_completo = f"[{timestamp}] [ERROR] {mensaje}"
    if excepcion:
        mensaje_completo += f" | Excepción: {str(excepcion)}"
    print(mensaje_completo)

def conectar_adls_keyvault():
    """
    CONEXIÓN SEGURA A AZURE DATA LAKE STORAGE
    ==========================================
    Propósito: Establecer conexión única con ADLS usando Azure Key Vault
    """
    global _ADLS_CLIENT, _CREDENTIAL
    
    if _ADLS_CLIENT is not None:
        return _ADLS_CLIENT
    
    try:
        log_info("Conectando a Azure Key Vault...")
        
        if _CREDENTIAL is None:
            tenant_id = os.environ.get("AZURE_TENANT_ID")
            client_id = os.environ.get("AZURE_CLIENT_ID")
            client_secret = os.environ.get("AZURE_CLIENT_SECRET")
            if tenant_id and client_id and client_secret:
                _CREDENTIAL = ClientSecretCredential(
                    tenant_id=tenant_id,
                    client_id=client_id,
                    client_secret=client_secret
                )
            else:
                _CREDENTIAL = InteractiveBrowserCredential()
            
        key_vault_url = "https://kv-liga1-secreto.vault.azure.net/"
        secret_client = SecretClient(vault_url=key_vault_url, credential=_CREDENTIAL)
        log_info("✅ Conectado a Key Vault")
        
        storage_account = secret_client.get_secret("storageaccount").value
        storage_key = secret_client.get_secret("storageaccountkey").value
        container_name = secret_client.get_secret("filesystemname").value
        
        connection_string = f"DefaultEndpointsProtocol=https;AccountName={storage_account};AccountKey={storage_key};EndpointSuffix=core.windows.net"
        service_client = DataLakeServiceClient.from_connection_string(connection_string)
        _ADLS_CLIENT = service_client.get_file_system_client(container_name)
        log_info("✅ Conexión ADLS exitosa")
        return _ADLS_CLIENT
        
    except Exception as e:
        log_error("Error en conexión Azure", e)
        return None

# =============================================================================
# MEJORAS PARA DATAFACTORY - NORMALIZACIÓN NUMÉRICA Y CSV
# =============================================================================

def normalizar_valores_monetarios(dataframe):
    """Normalización de valores monetarios - versión optimizada"""
    for columna in dataframe.columns:
        if any(keyword in columna.lower() for keyword in ['valor', 'precio', 'mercado', '€', 'eur', 'ppp']):
            try:
                def convertir_valor_monetario(valor):
                    if pd.isna(valor) or valor in ['', 'None', 'nan', 'No disponible']:
                        return None
                    
                    valor_str = str(valor).strip().lower()
                    
                    if 'mill' in valor_str:
                        numero_match = re.search(r'([\d,]+)', valor_str)
                        if numero_match:
                            numero_limpio = numero_match.group(1).replace(',', '.')
                            try:
                                return float(numero_limpio) * 1000000
                            except:
                                return valor
                    
                    elif 'mil' in valor_str:
                        numero_match = re.search(r'([\d,]+)', valor_str)
                        if numero_match:
                            numero_limpio = numero_match.group(1).replace(',', '.')
                            try:
                                return float(numero_limpio) * 1000
                            except:
                                return valor
                    
                    elif ',' in valor_str and re.match(r'^\d+,\d+$', valor_str):
                        try:
                            return float(valor_str.replace(',', '.'))
                        except:
                            return valor
                    
                    elif '.' in valor_str and ',' in valor_str:
                        try:
                            limpio = valor_str.replace('.', '').replace(',', '.')
                            return float(limpio)
                        except:
                            return valor
                    
                    try:
                        return float(valor_str)
                    except:
                        return valor
                
                nuevos_valores = dataframe[columna].apply(convertir_valor_monetario)
                cambios = sum(dataframe[columna] != nuevos_valores)
                if cambios > 0:
                    dataframe[columna] = nuevos_valores
                
            except Exception:
                pass
    
    return dataframe

def normalizar_decimales_europeos(dataframe):
    """Normalización de decimales europeos - versión optimizada"""
    for columna in dataframe.columns:
        if dataframe[columna].dtype == 'object':
            try:
                muestra = dataframe[columna].dropna().head(5)
                tiene_decimales_europeos = any(
                    ',' in str(x) and re.match(r'^\d+,\d+$', str(x).replace(' ', '')) 
                    for x in muestra if pd.notna(x) and str(x).strip()
                )
                
                if tiene_decimales_europeos:
                    def convertir_decimal(valor):
                        if pd.isna(valor) or not str(valor).strip():
                            return valor
                        valor_str = str(valor).strip()
                        if re.match(r'^\d+,\d+$', valor_str):
                            return float(valor_str.replace(',', '.'))
                        return valor
                    
                    dataframe[columna] = dataframe[columna].apply(convertir_decimal)
                    
            except Exception:
                pass
    
    return dataframe

def sanitizar_nombres_columnas(dataframe):
    """Sanitización de nombres de columnas - versión optimizada"""
    dataframe = normalizar_valores_monetarios(dataframe)
    dataframe = normalizar_decimales_europeos(dataframe)
    
    nuevos_nombres = {}
    for columna in dataframe.columns:
        if not isinstance(columna, str):
            columna = str(columna)
            
        nombre_limpio = columna
        
        if nombre_limpio == 'Ano':
            nombre_limpio = 'Anio'
        
        nombre_limpio = unicodedata.normalize('NFKD', nombre_limpio)
        nombre_limpio = nombre_limpio.encode('ASCII', 'ignore').decode('ASCII')
        
        caracteres_problematicos = ['[', ']', ',', ';', '{', '}', '(', ')', '\n', '\t', '=', ' ']
        for char in caracteres_problematicos:
            nombre_limpio = nombre_limpio.replace(char, '_')
        
        nombre_limpio = nombre_limpio.replace('€', 'eur_')
        nombre_limpio = nombre_limpio.replace('%', 'porcentaje_')
        nombre_limpio = nombre_limpio.replace('º', '')
        nombre_limpio = nombre_limpio.replace('ª', '')
        nombre_limpio = nombre_limpio.replace('-', '_')
        
        nombre_limpio = re.sub(r'_+', '_', nombre_limpio)
        nombre_limpio = nombre_limpio.strip('_')
        nombre_limpio = nombre_limpio.lower()
        
        if not nombre_limpio:
            nombre_limpio = 'columna'
        
        nuevos_nombres[columna] = nombre_limpio
    
    dataframe_renombrado = dataframe.rename(columns=nuevos_nombres)
    return dataframe_renombrado

def guardar_csv_local_mejorado(dataframe, año_guardado, tipo_archivo, fuente="Transfermarkt"):
    """Guarda CSV localmente con registro"""
    nombre_archivo = f"{tipo_archivo}_{año_guardado}.csv"
    
    try:
        base_path = r"C:\Users\milu_\Documents\Proyectos\liga1\data"
        año_path = os.path.join(base_path, str(año_guardado))
        os.makedirs(año_path, exist_ok=True)
        
        archivo_path = os.path.join(año_path, nombre_archivo)
        
        if isinstance(dataframe, pd.DataFrame):
            dataframe_con_metadatos = dataframe.copy()
            dataframe_con_metadatos['fuente'] = fuente
            dataframe_con_metadatos['temporada'] = año_guardado
            dataframe_con_metadatos['fecha_carga'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            dataframe_limpio = sanitizar_nombres_columnas(dataframe_con_metadatos)
            dataframe_limpio.to_csv(archivo_path, index=False, encoding="utf-8-sig", sep='|')
            filas = len(dataframe_limpio)
            
            registrar_archivo(nombre_archivo, "éxito", f"{filas} filas")
            return True
    except Exception as e:
        error_msg = f"Error guardando CSV: {str(e)}"
        registrar_archivo(nombre_archivo, "error", error_msg)
        return False

def guardar_json_local_mejorado(datos, año_guardado, tipo_archivo):
    """Guarda JSON localmente con registro"""
    nombre_archivo = f"{tipo_archivo}_{año_guardado}.json"
    
    try:
        base_path = r"C:\Users\milu_\Documents\Proyectos\liga1\data"
        año_path = os.path.join(base_path, str(año_guardado))
        os.makedirs(año_path, exist_ok=True)
        
        archivo_path = os.path.join(año_path, nombre_archivo)
        
        with open(archivo_path, 'w', encoding='utf-8') as f:
            json.dump(datos, f, ensure_ascii=False, indent=2)
        
        if isinstance(datos, dict) and 'data' in datos:
            cantidad = len(datos['data']) if isinstance(datos['data'], list) else 1
        else:
            cantidad = len(datos) if isinstance(datos, list) else 1
            
        registrar_archivo(nombre_archivo, "éxito", f"{cantidad} registros")
        return True
    except Exception as e:
        error_msg = f"Error guardando JSON: {str(e)}"
        registrar_archivo(nombre_archivo, "error", error_msg)
        return False

# =============================================================================
# MÓDULO FOTMOB - FUNCIONES COMPLETAS OPTIMIZADAS
# =============================================================================

def extraer_tablas_clasificacion(temporada_fotmob, max_reintentos=2):
    """Extrae tablas de clasificación con reintentos"""
    for reintento in range(max_reintentos):
        try:
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--window-size=1920,1080")
            
            driver = webdriver.Chrome(options=chrome_options)
            
            url = f"https://www.fotmob.com/es/leagues/131/table/liga-1?season={temporada_fotmob}"
            driver.get(url)
            
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "[class*='SubTableCSS']"))
            )
            time.sleep(3)
            
            script = """
            var resultados_script = {};
            var tablas = document.querySelectorAll('[class*="SubTableCSS"]');
            
            if (tablas.length === 0) {
                return {error: "No se encontraron tablas"};
            }
            
            tablas.forEach(function(tabla) {
                var nombreTorneo = tabla.querySelector('[class*="SubTableHeaderCSS"]')?.textContent || "Torneo Desconocido";
                resultados_script[nombreTorneo] = [];
                
                var filas = tabla.querySelectorAll('[class*="TableRowCSS"]');
                filas.forEach(function(fila) {
                    var posicion = fila.querySelector('[class*="TablePositionCell"]')?.textContent?.trim() || "";
                    var equipo = fila.querySelector('[class*="TeamName"]')?.textContent?.trim() || "";
                    
                    var datos = [];
                    var selectores = [
                        '[class*="17h6d1a-TableCell"]',
                        '[class*="vf87vt-TableCell"]',
                        '[class*="8x6t1e-TableCell"]',
                        '[class*="1g9oxd-TableCell"]',
                        '[class*="bcah6n-TableCell"]',
                        '[class*="1ins92n-TableCell"]',
                        '[class*="1s6mphf-TableCell"]'
                    ];
                    
                    selectores.forEach(function(selector) {
                        var elemento = fila.querySelector(selector);
                        if (elemento) {
                            datos.push(elemento.textContent.trim());
                        }
                    });
                    
                    if (posicion && equipo) {
                        resultados_script[nombreTorneo].push({
                            posicion: posicion,
                            equipo: equipo,
                            datos: datos
                        });
                    }
                });
            });
            
            return resultados_script;
            """
            
            resultados_script = driver.execute_script(script)
            driver.quit()
            
            if resultados_script and not resultados_script.get('error'):
                datos_finales = {}
                
                for nombre_torneo, equipos in resultados_script.items():
                    tabla_procesada = []
                    
                    for equipo in equipos:
                        datos = equipo['datos']
                        if len(datos) >= 7:
                            try:
                                tabla_procesada.append({
                                    "posicion": int(equipo['posicion']),
                                    "equipo": equipo['equipo'],
                                    "partidos_jugados": int(datos[0]) if datos[0] and datos[0].isdigit() else 0,
                                    "partidos_ganados": int(datos[1]) if datos[1] and datos[1].isdigit() else 0,
                                    "partidos_empatados": int(datos[2]) if datos[2] and datos[2].isdigit() else 0,
                                    "partidos_perdidos": int(datos[3]) if datos[3] and datos[3].isdigit() else 0,
                                    "goles_a_favor_contra": datos[4],
                                    "diferencia_goles": datos[5],
                                    "puntos": int(datos[6]) if datos[6] and datos[6].isdigit() else 0
                                })
                            except ValueError:
                                continue
                    
                    if tabla_procesada:
                        datos_finales[nombre_torneo] = tabla_procesada
                
                resultado_tablas = {
                    "fuente": "FotMob",
                    "temporada": int(temporada_fotmob),
                    "fecha_carga": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "data": [datos_finales]
                }
                
                log_info(f"✅ Tablas clasificación: {len(datos_finales)} torneos")
                return resultado_tablas
            
            if reintento < max_reintentos - 1:
                time.sleep(2)
                
        except Exception as e:
            if reintento < max_reintentos - 1:
                time.sleep(2)
            try:
                driver.quit()
            except:
                pass
    
    log_error(f"No se pudieron extraer tablas para {temporada_fotmob}")
    return {
        "fuente": "FotMob",
        "temporada": int(temporada_fotmob),
        "fecha_carga": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "data": []
    }

def extraer_campeones_temporada(temporada_fotmob, max_reintentos=2):
    """Extrae campeones y subcampeones con reintentos"""
    for reintento in range(max_reintentos):
        try:
            url = "https://www.fotmob.com/es/leagues/131/seasons/liga-1"
            
            options = Options()
            options.add_argument("--headless")
            options.add_argument("--disable-gpu")
            options.add_argument("--window-size=1920x1080")
            
            with webdriver.Chrome(options=options) as driver:
                driver.get(url)
                
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                
                time.sleep(3)
                
                page_text = driver.find_element(By.TAG_NAME, "body").text
                lines = [line.strip() for line in page_text.split('\n') if line.strip()]
                
                for i, line in enumerate(lines):
                    if line == temporada_fotmob:
                        campeon = None
                        subcampeon = None
                        
                        for j in range(i+1, min(i+10, len(lines))):
                            current_line = lines[j]
                            
                            if 'Campeón' in current_line and not campeon:
                                if j > 0:
                                    campeon = lines[j-1]
                            
                            elif 'Subcampeón' in current_line and not subcampeon:
                                if j > 0:
                                    subcampeon = lines[j-1]
                            
                            if campeon and subcampeon:
                                break
                        
                        if campeon and subcampeon:
                            resultado = {
                                "fuente": "FotMob",
                                "temporada": int(temporada_fotmob),
                                "fecha_carga": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                "data": [{
                                    'campeon': campeon,
                                    'subcampeon': subcampeon
                                }]
                            }
                            log_info(f"✅ Campeones: {campeon} | {subcampeon}")
                            return resultado
            
            if reintento < max_reintentos - 1:
                time.sleep(2)
                
        except Exception as e:
            if reintento < max_reintentos - 1:
                time.sleep(2)
    
    log_error(f"No se pudieron extraer campeones para {temporada_fotmob}")
    return {
        "fuente": "FotMob",
        "temporada": int(temporada_fotmob),
        "fecha_carga": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "data": []
    }

def obtener_equipos(temporada_fotmob, max_reintentos=2):
    """Obtiene lista de equipos con reintentos"""
    for reintento in range(max_reintentos):
        try:
            url = f"https://www.fotmob.com/es/leagues/131/overview/liga-1?season={temporada_fotmob}"
            equipos_lista = []
            
            options = Options()
            options.add_argument("--headless")
            options.add_argument("--disable-gpu")
            options.add_argument("--window-size=1920x1080")
            
            with webdriver.Chrome(options=options) as driver:
                driver.get(url)
                try:
                    WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "div[class*='TableRowCSS']"))
                    )
                except Exception:
                    pass

                soup = BeautifulSoup(driver.page_source, 'html.parser')
                filas_equipos = soup.select("div[class*='TableRowCSS'] div[class*='TableTeamCell'] a")
                
                for equipo in filas_equipos:
                    nombre_tag = equipo.select_one("span.TeamName")
                    nombre = nombre_tag.text.strip() if nombre_tag else "N/A"
                    href = equipo.get("href")
                    if href:
                        url_equipo = "https://www.fotmob.com" + href
                        equipos_lista.append({
                            "equipo": nombre,
                            "url": url_equipo
                        })

            # Eliminar duplicados manteniendo el orden
            equipos_unicos = []
            equipos_vistos = set()
            for equipo in equipos_lista:
                if equipo["equipo"] not in equipos_vistos:
                    equipos_unicos.append(equipo)
                    equipos_vistos.add(equipo["equipo"])
            
            # Ordenar alfabéticamente
            equipos_unicos = sorted(equipos_unicos, key=lambda x: x["equipo"])
            
            resultado_equipos = {
                "fuente": "FotMob",
                "temporada": int(temporada_fotmob),
                "fecha_carga": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "data": equipos_unicos
            }
            
            log_info(f"✅ Equipos: {len(equipos_unicos)} equipos")
            return resultado_equipos
            
        except Exception as e:
            if reintento < max_reintentos - 1:
                time.sleep(2)
    
    log_error(f"No se pudieron extraer equipos para {temporada_fotmob}")
    return {
        "fuente": "FotMob",
        "temporada": int(temporada_fotmob),
        "fecha_carga": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "data": []
    }

def obtener_partidos(liga, id, temporada_fotmob, max_reintentos=2):
    """Obtiene partidos con reintentos"""
    for reintento in range(max_reintentos):
        try:
            base_url = f"https://www.fotmob.com/es/leagues/{id}/matches/{liga}?season={temporada_fotmob}&group=by-date&page="
            partidos_totales = []
            fechas_vistas = set()
            page = 0

            options = Options()
            options.add_argument("--headless")
            options.add_argument("--disable-gpu")
            options.add_argument("--window-size=1920x1080")

            with webdriver.Chrome(options=options) as driver:
                while True:
                    url = base_url + str(page)
                    driver.get(url)
                    try:
                        WebDriverWait(driver, 5).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "section[class*='LeagueMatchesSectionCSS']"))
                        )
                    except Exception:
                        break

                    soup = BeautifulSoup(driver.page_source, 'html.parser')
                    secciones = soup.select('section[class*="LeagueMatchesSectionCSS"]')

                    if not secciones:
                        break

                    nuevas_fechas = 0

                    for seccion in secciones:
                        fecha_tag = seccion.select_one('h3')
                        fecha_texto = fecha_tag.text.strip() if fecha_tag else "N/A"
                        if fecha_texto in fechas_vistas:
                            continue

                        fechas_vistas.add(fecha_texto)
                        nuevas_fechas += 1

                        partidos = seccion.select('a[class*="MatchWrapper"]')
                        for p in partidos:
                            try:
                                local = p.select_one('div[class*="StatusAndHomeTeamWrapper"] span[class*="TeamName"]').text.strip()
                                visitante = p.select_one('div[class*="AwayTeamAndFollowWrapper"] span[class*="TeamName"]').text.strip()
                                marcador_tag = p.select_one('span[class*="LSMatchStatusScore"]')
                                if marcador_tag:
                                    marcador = marcador_tag.text.strip()
                                else:
                                    hora_tag = p.select_one('span[class*="LSMatchStatusTime"] div[class*="TimeCSS"]')
                                    if hora_tag:
                                        marcador = hora_tag.text.strip()
                                    else:
                                        marcador = "Sin jugar"

                                href = p.get("href")
                                url_partido = "https://www.fotmob.com" + href if href else "N/A"

                                partidos_totales.append({
                                    "fecha": fecha_texto,
                                    "local": local,
                                    "visitante": visitante,
                                    "marcador": marcador,
                                    "url": url_partido
                                })
                            except Exception:
                                continue

                    if nuevas_fechas == 0:
                        break

                    page += 1
                    time.sleep(1)

            # Convertir fechas a formato estándar
            partidos_con_fecha_normalizada = []
            for partido in partidos_totales:
                partido_copy = partido.copy()
                partido_copy["fecha"] = _convertir_formato_fecha_DD_MM_YYYY(partido["fecha"])
                partidos_con_fecha_normalizada.append(partido_copy)

            resultado_partidos = {
                "fuente": "FotMob",
                "temporada": int(temporada_fotmob),
                "fecha_carga": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "data": partidos_con_fecha_normalizada
            }
            
            log_info(f"✅ Partidos: {len(partidos_con_fecha_normalizada)} partidos")
            return resultado_partidos
            
        except Exception as e:
            if reintento < max_reintentos - 1:
                time.sleep(2)
    
    log_error(f"No se pudieron extraer partidos para {temporada_fotmob}")
    return {
        "fuente": "FotMob",
        "temporada": int(temporada_fotmob),
        "fecha_carga": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "data": []
    }

def _convertir_formato_fecha_DD_MM_YYYY(fecha):
    """Convierte fechas textuales a formato estándar DD-MM-YYYY"""
    meses = {
        'enero': '01', 'febrero': '02', 'marzo': '03', 'abril': '04',
        'mayo': '05', 'junio': '06', 'julio': '07', 'agusto': '08',
        'septiembre': '09', 'octubre': '10', 'noviembre': '11', 'diciembre': '12'
    }

    if isinstance(fecha, str):
        try:
            fecha_sin_tilde = unidecode(fecha.strip().lower())
            partes = fecha_sin_tilde.replace(',', '').split()

            if len(partes) >= 6:
                dia = partes[1]
                mes = meses.get(partes[3], '01')
                año = partes[5]
                return f"{dia.zfill(2)}-{mes}-{año}"
            elif len(partes) >= 4:
                dia = partes[1]
                mes = meses.get(partes[3], '01')
                año = str(_AÑO_ACTUAL)
                return f"{dia.zfill(2)}-{mes}-{año}"
            elif fecha_sin_tilde == 'ayer':
                ayer = datetime.now() - timedelta(days=1)
                return ayer.strftime("%d-%m-%Y")
            elif fecha_sin_tilde == 'hoy':
                hoy = datetime.now()
                return hoy.strftime("%d-%m-%Y")
            elif fecha_sin_tilde == 'manana':
                manana = datetime.now() + timedelta(days=1)
                return manana.strftime("%d-%m-%Y")

        except Exception:
            return fecha
    return fecha

def extraer_stats_partidos_mejorada(partidos_data, max_workers=2):
    """Extrae estadísticas de partidos optimizada"""
    partidos_list = partidos_data["data"]
    urls = [partido["url"] for partido in partidos_list]
    
    if not urls:
        return partidos_data

    def worker(url_queue, results_list):
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        driver = webdriver.Chrome(options=options)

        while not url_queue.empty():
            try:
                idx, url = url_queue.get_nowait()
            except queue.Empty:
                break

            try:
                driver.get(url + ":tab=stats")
                WebDriverWait(driver, 3).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                time.sleep(1)
                
                soup = BeautifulSoup(driver.page_source, "html.parser")
                
                # Verificar si el partido está aplazado
                texto_pagina = soup.get_text().lower()
                if any(palabra in texto_pagina for palabra in ['aplazado', 'postpuesto', 'cancelado', 'suspendido']):
                    results_list.append((idx, {"estado": "Aplazado/Sin estadísticas"}))
                    continue
                
                # Extraer estadísticas
                resultados = {}
                stats = soup.find_all("li", class_=lambda c: c and "Stat" in c)
                
                for stat in stats:
                    titulo = stat.select_one("span.title")
                    if titulo:
                        nombre_stat = titulo.text.strip()
                        valores = stat.select("span[class*='StatValue'], span[class*='value']")
                        
                        if len(valores) >= 2:
                            resultados[nombre_stat + "_local"] = valores[0].text.strip()
                            resultados[nombre_stat + "_visitante"] = valores[1].text.strip()
                        elif len(valores) == 1:
                            resultados[nombre_stat] = valores[0].text.strip()
                
                if resultados:
                    results_list.append((idx, resultados))
                else:
                    results_list.append((idx, {"estado": "Sin estadísticas disponibles"}))

            except Exception:
                results_list.append((idx, {"estado": "Error al cargar estadísticas"}))
            finally:
                url_queue.task_done()

        driver.quit()

    url_queue = queue.Queue()
    for i, url in enumerate(urls):
        url_queue.put((i, url))

    results_list = []
    total = url_queue.qsize()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(worker, url_queue, results_list) for _ in range(max_workers)]
        while not url_queue.empty():
            time.sleep(0.5)

    results_list.sort(key=lambda x: x[0])
    
    # Actualizar los partidos con estadísticas
    partidos_actualizados = partidos_list.copy()
    for idx, stats in results_list:
        for col, val in stats.items():
            partidos_actualizados[idx][col] = val

    # Contar resultados
    contador_exitos = sum(1 for idx, stats in results_list if "estado" not in stats or stats["estado"] != "Aplazado/Sin estadísticas")
    contador_aplazados = sum(1 for idx, stats in results_list if stats.get("estado") == "Aplazado/Sin estadísticas")
    
    log_info(f"📊 Estadísticas partidos: {contador_exitos} exitosos, {contador_aplazados} aplazados")
    
    # Retornar con la misma estructura
    resultado_estadisticas = {
        "fuente": "FotMob",
        "temporada": partidos_data["temporada"],
        "fecha_carga": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "data": partidos_actualizados
    }
    
    return resultado_estadisticas

# =============================================================================
# MÓDULO TRANSFERMARKT - FUNCIONES COMPLETAS OPTIMIZADAS
# =============================================================================

def manejar_banner(driver):
    """Maneja banners de cookies"""
    time.sleep(2)
    try:
        iframes = driver.find_elements(By.TAG_NAME, "iframe")
        for iframe in iframes:
            try:
                driver.switch_to.frame(iframe)
                botones = driver.find_elements(By.XPATH, '//button[contains(., "Aceptar") or contains(., "Accept")]')
                for boton in botones:
                    if boton.is_displayed():
                        driver.execute_script("arguments[0].click();", boton)
                        break
                driver.switch_to.default_content()
            except:
                driver.switch_to.default_content()
                continue
    except:
        pass

def extraer_nacionalidad_avanzada(driver, elemento):
    """Extrae nacionalidad de jugadores"""
    try:
        banderas = elemento.find_elements(By.XPATH, './/img[contains(@class, "flaggenkurz") or contains(@src, "flag")]')
        nacionalidades = []
        
        for bandera in banderas:
            alt_text = bandera.get_attribute('alt')
            title_text = bandera.get_attribute('title')
            if title_text and len(title_text) > 1:
                nacionalidades.append(title_text)
            elif alt_text and len(alt_text) > 1:
                nacionalidades.append(alt_text)
                
        if nacionalidades:
            return ', '.join(nacionalidades)
        return "Perú"
    except:
        return "No disponible"

def extraer_nacionalidad_entrenador(driver, fila_entrenador):
    """Extrae nacionalidad de entrenadores"""
    try:
        banderas = fila_entrenador.find_elements(By.XPATH, './/img[contains(@class, "flaggenkurz") or contains(@src, "flag")]')
        nacionalidades = []
        
        for bandera in banderas:
            alt_text = bandera.get_attribute('alt')
            title_text = bandera.get_attribute('title')
            if title_text and len(title_text) > 1:
                nacionalidades.append(title_text)
            elif alt_text and len(alt_text) > 1:
                nacionalidades.append(alt_text)
                
        if nacionalidades:
            return ', '.join(nacionalidades)
        return "Perú"
    except:
        return "No disponible"

def verificar_y_seleccionar_temporada(driver, club, año_parametro):
    """Selecciona temporada en Transfermarkt"""
    try:
        selectores_temporada = [
            '//select[@name="saison_id"]',
            '//select[contains(@class, "saison")]',
        ]
        
        for selector in selectores_temporada:
            try:
                selector_element = driver.find_element(By.XPATH, selector)
                opciones = selector_element.find_elements(By.XPATH, f'.//option[@value="{año_parametro}"]')
                if opciones:
                    driver.execute_script("arguments[0].value = arguments[1]; arguments[0].dispatchEvent(new Event('change'))", 
                                       selector_element, año_parametro)
                    time.sleep(2)
                    return True
            except:
                continue
        return True
    except:
        return True

def extraer_plantillas_mejorado(driver, club, url_equipo, año_real):
    """Extrae plantillas de jugadores"""
    try:
        url_plantilla = url_equipo.replace('/startseite/', '/kader/') + '/plus/1'
        driver.get(url_plantilla)
        time.sleep(3)
        
        verificar_y_seleccionar_temporada(driver, club, año_real - 1)
        
        datos_plantillas = []
        
        selectores_tabla = [
            '//table[contains(@class,"items")]/tbody/tr[contains(@class, "odd") or contains(@class, "even")]',
            '//table[@class="items"]/tbody/tr',
        ]
        
        filas_jugadores = []
        for selector in selectores_tabla:
            try:
                filas_jugadores = driver.find_elements(By.XPATH, selector)
                if filas_jugadores:
                    break
            except:
                continue
        
        jugadores_procesados = 0
        
        for fila_jugador in filas_jugadores:
            try:
                celdas = fila_jugador.find_elements(By.XPATH, './/td')
                if len(celdas) >= 8:
                    numero = celdas[0].text.strip()
                    if not numero:
                        numero = "Sin Número"
                    
                    nombre_completo = celdas[1].text.strip() if len(celdas) > 1 else ""
                    if not nombre_completo:
                        continue
                    
                    lineas = nombre_completo.split('\n')
                    nombre = lineas[0] if lineas else ""
                    posicion = lineas[1] if len(lineas) > 1 else ""
                    
                    fecha_nacimiento = ""
                    for idx in [5, 4, 3, 2]:
                        if len(celdas) > idx and celdas[idx].text.strip() and '/' in celdas[idx].text:
                            fecha_nacimiento = celdas[idx].text.strip()
                            break
                    
                    nacionalidad = extraer_nacionalidad_avanzada(driver, fila_jugador)
                    
                    altura = ""
                    for idx in [8, 7, 6]:
                        if len(celdas) > idx and 'm' in celdas[idx].text:
                            altura = celdas[idx].text.strip()
                            break
                    
                    pie = ""
                    for idx in [9, 8, 7]:
                        if len(celdas) > idx and celdas[idx].text.strip() in ['Derecho', 'Izquierdo', 'Ambidiestro']:
                            pie = celdas[idx].text.strip()
                            break
                    
                    fecha_fichaje = ""
                    for idx in [10, 9, 8]:
                        if len(celdas) > idx and '/' in celdas[idx].text:
                            fecha_fichaje = celdas[idx].text.strip()
                            break
                    
                    valor_mercado = ""
                    for idx in [12, 11, 10, 9]:
                        if len(celdas) > idx and ('€' in celdas[idx].text or 'mil' in celdas[idx].text or 'mill' in celdas[idx].text):
                            valor_mercado = celdas[idx].text.strip()
                            break
                    
                    datos_plantillas.append({
                        "Año": año_real,
                        "Club": club,
                        "Número": numero,
                        "Jugador": nombre,
                        "Posición": posicion,
                        "Fecha Nacimiento": fecha_nacimiento,
                        "Nacionalidad": nacionalidad,
                        "Altura": altura,
                        "Pie": pie,
                        "Fecha Fichaje": fecha_fichaje,
                        "Valor Mercado (€)": valor_mercado
                    })
                    
                    jugadores_procesados += 1
                    
            except Exception:
                continue
        
        return datos_plantillas, jugadores_procesados
        
    except Exception as e:
        return [], 0

def extraer_entrenadores_corregido(driver, club, url_equipo):
    """Extrae historial de entrenadores"""
    try:
        url_entrenadores = url_equipo.replace('/startseite/', '/mitarbeiterhistorie/')
        driver.get(url_entrenadores)
        time.sleep(2)
        
        datos_entrenadores = []
        
        filas_entrenadores = driver.find_elements(By.XPATH, '//table[contains(@class,"items")]/tbody/tr[contains(@class, "odd") or contains(@class, "even")]')
        
        for fila in filas_entrenadores:
            try:
                celdas = fila.find_elements(By.XPATH, './/td')
                if len(celdas) >= 10:
                    nombre_completo = celdas[0].text.strip()
                    lineas_nombre = nombre_completo.split('\n')
                    nombre = lineas_nombre[0] if lineas_nombre else ""
                    fecha_nacimiento = lineas_nombre[1] if len(lineas_nombre) > 1 else ""
                    
                    nacionalidad = extraer_nacionalidad_entrenador(driver, fila)
                    
                    comienzo = celdas[5].text.strip() if len(celdas) > 5 else ""
                    
                    if comienzo:
                        try:
                            dia, mes, año = comienzo.split('/')
                            fecha_comienzo = f"{año}-{mes.zfill(2)}-{dia.zfill(2)}"
                            
                            if fecha_comienzo > "2019-12-31":
                                salida = celdas[6].text.strip() if len(celdas) > 6 else ""
                                tiempo_cargo = celdas[7].text.strip() if len(celdas) > 7 else ""
                                partidos = celdas[8].text.strip() if len(celdas) > 8 else ""
                                ppp = celdas[9].text.strip() if len(celdas) > 9 else ""
                                
                                datos_entrenadores.append({
                                    "Club": club,
                                    "Entrenador": nombre,
                                    "Fecha Nacimiento": fecha_nacimiento,
                                    "Nacionalidad": nacionalidad,
                                    "Comienzo": comienzo,
                                    "Salida": salida,
                                    "Tiempo en cargo": tiempo_cargo,
                                    "Partidos": partidos,
                                    "PPP": ppp
                                })
                        except:
                            salida = celdas[6].text.strip() if len(celdas) > 6 else ""
                            tiempo_cargo = celdas[7].text.strip() if len(celdas) > 7 else ""
                            partidos = celdas[8].text.strip() if len(celdas) > 8 else ""
                            ppp = celdas[9].text.strip() if len(celdas) > 9 else ""
                            
                            datos_entrenadores.append({
                                "Club": club,
                                "Entrenador": nombre,
                                "Fecha Nacimiento": fecha_nacimiento,
                                "Nacionalidad": nacionalidad,
                                "Comienzo": comienzo,
                                "Salida": salida,
                                "Tiempo en cargo": tiempo_cargo,
                                "Partidos": partidos,
                                "PPP": ppp
                            })
            except Exception:
                continue
                
        return datos_entrenadores
        
    except Exception as e:
        return []

def extraer_info_estadio_corregido(driver, club, url_equipo):
    """Extrae información de estadio"""
    try:
        driver.get(url_equipo)
        time.sleep(2)
        
        datos_estadio = []
        nombre_estadio = "No disponible"
        capacidad = "No disponible"
        aforo = "No disponible"
        
        try:
            elemento_estadio = driver.find_element(By.XPATH, '//*[contains(text(), "Estadio:")]')
            texto_completo = elemento_estadio.text.strip()
            
            if "Estadio:" in texto_completo:
                partes = texto_completo.replace("Estadio:", "").strip()
                numeros = re.findall(r'[\d.,]+', partes)
                
                if numeros:
                    capacidad = numeros[-1].replace(',', '.')
                    aforo = capacidad
                    nombre_estadio = re.split(r'[\d.,]', partes)[0].strip()
                else:
                    nombre_estadio = partes
        except:
            try:
                tabla_info = driver.find_element(By.XPATH, '//table[@class="profilheader"]')
                filas = tabla_info.find_elements(By.XPATH, './/tr')
                
                for fila in filas:
                    celdas = fila.find_elements(By.XPATH, './/td')
                    if len(celdas) >= 2:
                        etiqueta = celdas[0].text.strip()
                        valor = celdas[1].text.strip()
                        
                        if "Estadio" in etiqueta:
                            nombre_estadio = valor
                            if any(char.isdigit() for char in valor):
                                numeros = re.findall(r'[\d.,]+', valor)
                                if numeros:
                                    capacidad = numeros[-1].replace(',', '.')
                                    aforo = capacidad
                                    nombre_estadio = re.split(r'[\d.,]', valor)[0].strip()
            except:
                pass
        
        datos_estadio.append({
            "Club": club,
            "Estadio": nombre_estadio,
            "Capacidad": capacidad,
            "Aforo": aforo
        })
        
        return datos_estadio
        
    except Exception as e:
        return [{"Club": club, "Estadio": "No disponible", "Capacidad": "No disponible", "Aforo": "No disponible"}]

def extraer_plantillas_con_reintentos(driver, club, url_equipo, año_real, max_reintentos=2):
    """Extrae plantillas con reintentos"""
    for intento in range(max_reintentos + 1):
        try:
            plantillas, jugadores_procesados = extraer_plantillas_mejorado(driver, club, url_equipo, año_real)
            
            if jugadores_procesados > 0:
                return plantillas, jugadores_procesados
                
            if intento < max_reintentos:
                time.sleep(3)
                driver.get(url_equipo)
                time.sleep(2)
                
        except Exception as e:
            if intento < max_reintentos:
                time.sleep(3)
                
    return [], 0

def extraer_entrenadores_con_reintentos(driver, club, url_equipo, max_reintentos=2):
    """Extrae entrenadores con reintentos"""
    for intento in range(max_reintentos + 1):
        try:
            entrenadores = extraer_entrenadores_corregido(driver, club, url_equipo)
            if entrenadores:
                return entrenadores
            elif intento < max_reintentos:
                time.sleep(2)
        except Exception as e:
            if intento < max_reintentos:
                time.sleep(2)
    return []

def extraer_estadio_con_reintentos(driver, club, url_equipo, max_reintentos=2):
    """Extrae estadio con reintentos"""
    for intento in range(max_reintentos + 1):
        try:
            estadio = extraer_info_estadio_corregido(driver, club, url_equipo)
            if estadio and estadio[0]['Estadio'] != "No disponible":
                return estadio
            elif intento < max_reintentos:
                time.sleep(2)
        except Exception as e:
            if intento < max_reintentos:
                time.sleep(2)
    return [{"Club": club, "Estadio": "No disponible", "Capacidad": "No disponible", "Aforo": "No disponible"}]

def scraping_transfermarkt_por_año(año_usuario):
    """Scraping completo de Transfermarkt para un año"""
    año_guardado = año_usuario + 1
    año_transfermarkt = año_usuario
    
    try:
        options = webdriver.ChromeOptions()
        options.add_argument("--headless")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        url_liga = f"https://www.transfermarkt.pe/liga-1-apertura/startseite/wettbewerb/TDeA/plus/?saison_id={año_transfermarkt}"
        
        driver.get(url_liga)
        manejar_banner(driver)
        
        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.XPATH, '//table[contains(@class,"items")]//td[2]//a'))
            )
            time.sleep(2)
        except:
            log_error(f"No se pudo cargar la tabla general para año {año_transfermarkt}")
            driver.quit()
            return [], [], [], []

        filas = driver.find_elements(By.XPATH, '//div[@id="yw1"]//table[contains(@class,"items")]/tbody/tr')
        datos_generales = []
        urls_equipos = []
        
        for fila in filas:
            try:
                columnas = fila.find_elements(By.XPATH, './/td')
                if len(columnas) >= 7:
                    club = columnas[1].text.strip()
                    if club and len(club) > 2:
                        link_equipo = columnas[1].find_element(By.XPATH, './/a').get_attribute('href')
                        urls_equipos.append((club, link_equipo))
                        
                        datos_generales.append({
                            "Año": año_guardado,
                            "Club": club,
                            "Jugadores en plantilla": columnas[2].text.strip(),
                            "Edad promedio": columnas[3].text.strip(),
                            "Extranjeros": columnas[4].text.strip(),
                            "Valor medio (€)": columnas[5].text.strip(),
                            "Valor total (€)": columnas[6].text.strip()
                        })
            except:
                continue

        datos_plantillas = []
        datos_estadios = []
        datos_entrenadores = []
        
        log_info(f"🔍 Procesando {len(urls_equipos)} equipos de Transfermarkt...")
        
        for i, (club, url_equipo) in enumerate(urls_equipos):
            try:
                plantillas, jugadores_procesados = extraer_plantillas_con_reintentos(
                    driver, club, url_equipo, año_guardado, max_reintentos=1
                )
                datos_plantillas.extend(plantillas)
                
                estadio = extraer_estadio_con_reintentos(driver, club, url_equipo, max_reintentos=1)
                datos_estadios.extend(estadio)
                
                entrenadores = extraer_entrenadores_con_reintentos(driver, club, url_equipo, max_reintentos=1)
                datos_entrenadores.extend(entrenadores)
                
                if (i + 1) % 5 == 0:
                    log_info(f"   📊 {i + 1}/{len(urls_equipos)} equipos procesados")
                    
            except Exception as e:
                continue

        driver.quit()
        
        log_info(f"✅ Transfermarkt: {len(datos_plantillas)} jugadores, {len(datos_estadios)} estadios, {len(datos_entrenadores)} entrenadores")
        return datos_generales, datos_plantillas, datos_estadios, datos_entrenadores
        
    except Exception as e:
        log_error(f"Error en scraping Transfermarkt para {año_guardado}", e)
        try:
            driver.quit()
        except:
            pass
        return [], [], [], []

# =============================================================================
# LÓGICA DEFINITIVA DE AÑOS - VERSIÓN CORREGIDA CON AÑO DINÁMICO
# =============================================================================

def determinar_logica_año(año_usuario, modo="historico"):
    """
    LÓGICA CORREGIDA CON AÑO DINÁMICO:
    
    HISTÓRICO/REPROCESO:
    - Entrada 2020 → FotMob: 2020, Transfermarkt: 2019, Guardado: 2020
    - Entrada 2021 → FotMob: 2021, Transfermarkt: 2020, Guardado: 2021
    - ...
    - Entrada año_actual → FotMob: año_actual, Transfermarkt: año_actual-1, Guardado: año_actual
    - NO acepta años futuros
    
    INCREMENTAL:
    - Año actual → FotMob: año_actual, Transfermarkt: año_actual-1, Guardado: año_actual
    """
    
    if modo in ["historico", "reproceso"]:
        # MODO HISTÓRICO/REPROCESO - Permite hasta año actual
        if año_usuario > _AÑO_ACTUAL:
            raise ValueError(f"❌ Modo {modo} no acepta años después de {_AÑO_ACTUAL}. Año máximo: {_AÑO_ACTUAL}")
        
        if año_usuario < _AÑO_MINIMO:
            raise ValueError(f"❌ Modo {modo} no acepta años antes de {_AÑO_MINIMO}. Año mínimo: {_AÑO_MINIMO}")
        
        año_guardado = año_usuario
        año_transfermarkt = año_usuario - 1  # Transfermarkt busca año anterior
        temporada_fotmob = str(año_usuario)  # FotMob busca mismo año
        
        log_info(f"🔍 MODO {modo.upper()}: Entrada {año_usuario} → Guardado: {año_guardado}")
        
    else:  # MODO INCREMENTAL
        # MODO INCREMENTAL - Solo año actual
        if año_usuario != _AÑO_ACTUAL:
            raise ValueError(f"❌ Modo incremental solo acepta año actual ({_AÑO_ACTUAL})")
        
        año_guardado = año_usuario
        año_transfermarkt = año_usuario - 1  # Transfermarkt busca año anterior
        temporada_fotmob = str(año_usuario)  # FotMob busca mismo año
        
        log_info(f"🔍 MODO INCREMENTAL: Entrada {año_usuario} → Guardado: {año_guardado}")
    
    log_info(f"📊 FotMob: {temporada_fotmob} | Transfermarkt: {año_transfermarkt}")
    
    return año_guardado, año_transfermarkt, modo

# =============================================================================
# FUNCIONES PARA GUARDAR EN ADLS
# =============================================================================

def guardar_csv_adls(dataframe, año_guardado, tipo_archivo, adls_client, fuente="Transfermarkt"):
    """Guarda CSV en ADLS con registro"""
    nombre_archivo = f"{tipo_archivo}_{año_guardado}.csv"
    
    try:
        if isinstance(dataframe, pd.DataFrame):
            # Preparar dataframe con metadatos
            dataframe_con_metadatos = dataframe.copy()
            dataframe_con_metadatos['fuente'] = fuente
            dataframe_con_metadatos['temporada'] = año_guardado
            dataframe_con_metadatos['fecha_carga'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Sanitizar nombres de columnas
            dataframe_limpio = sanitizar_nombres_columnas(dataframe_con_metadatos)
            
            # Convertir a CSV
            csv_content = dataframe_limpio.to_csv(index=False, encoding="utf-8-sig", sep='|')
            
            # Subir a ADLS
            adls_path = f"landing/{año_guardado}/{nombre_archivo}"
            file_client = adls_client.get_file_client(adls_path)
            file_client.upload_data(csv_content, overwrite=True)
            
            log_info(f"📁 CSV guardado en ADLS: {adls_path}")
            return True
            
    except Exception as e:
        error_msg = f"Error guardando CSV en ADLS: {str(e)}"
        log_error(error_msg)
        return False

def guardar_json_adls(datos, año_guardado, tipo_archivo, adls_client):
    """Guarda JSON en ADLS con registro"""
    nombre_archivo = f"{tipo_archivo}_{año_guardado}.json"
    
    try:
        # Convertir a JSON
        json_content = json.dumps(datos, ensure_ascii=False, indent=2)
        
        # Subir a ADLS
        adls_path = f"landing/{año_guardado}/{nombre_archivo}"
        file_client = adls_client.get_file_client(adls_path)
        file_client.upload_data(json_content, overwrite=True)
        
        log_info(f"📁 JSON guardado en ADLS: {adls_path}")
        return True
        
    except Exception as e:
        error_msg = f"Error guardando JSON en ADLS: {str(e)}"
        log_error(error_msg)
        return False

def subir_archivos_a_adls(año_guardado, adls_client):
    """Sube todos los archivos del año a ADLS - INCLUYENDO REPORTE"""
    try:
        base_path = r"C:\Users\milu_\Documents\Proyectos\liga1\data"
        año_path = os.path.join(base_path, str(año_guardado))
        
        if not os.path.exists(año_path):
            log_error(f"No existe directorio local para año {año_guardado}")
            return False
        
        archivos_subidos = 0
        archivos_fallidos = []
        
        # Listar todos los archivos en el directorio del año
        for archivo in os.listdir(año_path):
            if archivo.endswith(('.csv', '.json', '.txt')):
                archivo_path = os.path.join(año_path, archivo)
                
                try:
                    # Determinar modo de lectura según tipo de archivo
                    if archivo.endswith('.csv'):
                        with open(archivo_path, 'r', encoding='utf-8-sig') as f:
                            contenido = f.read()
                    else:
                        with open(archivo_path, 'r', encoding='utf-8') as f:
                            contenido = f.read()
                    
                    # Subir a ADLS
                    adls_path = f"landing/{año_guardado}/{archivo}"
                    file_client = adls_client.get_file_client(adls_path)
                    file_client.upload_data(contenido, overwrite=True)
                    
                    archivos_subidos += 1
                    log_info(f"✅ Subido a ADLS: {archivo}")
                    
                except Exception as e:
                    archivos_fallidos.append(archivo)
                    log_error(f"Error subiendo {archivo} a ADLS: {str(e)}")
        
        log_info(f"📊 ADLS: {archivos_subidos} archivos subidos, {len(archivos_fallidos)} fallidos")
        
        # Verificar si el reporte está incluido
        reporte_path = os.path.join(año_path, f"reporte_archivos_{año_guardado}.txt")
        if os.path.exists(reporte_path):
            log_info("✅ Reporte de archivos incluido en subida masiva")
        else:
            log_info("⚠️  Reporte de archivos no encontrado para subida masiva")
        
        return archivos_subidos > 0
        
    except Exception as e:
        log_error(f"Error en subida masiva a ADLS: {str(e)}")
        return False

# =============================================================================
# ORQUESTADOR PRINCIPAL - EJECUCIÓN POR AÑO (VERSIÓN MEJORADA CON TRIGGERS)
# =============================================================================

def scraping_completo_por_año(año_usuario, max_reintentos_año=2, modo="historico", adls_client_externo=None):
    """
    EJECUCIÓN COMPLETA CON LÓGICA CORREGIDA DE AÑOS, ADLS Y TRIGGERS ADF
    =====================================================================
    """
    # Reiniciar log de ejecución
    global _EJECUCION_LOG
    _EJECUCION_LOG = []
    
    registrar_ejecucion("INICIANDO_EJECUCION", f"Modo: {modo}, Año entrada: {año_usuario}")
    
    try:
        # DETECCIÓN CON MODO ESPECÍFICO
        año_guardado, año_transfermarkt, modo_detectado = determinar_logica_año(año_usuario, modo)
        registrar_ejecucion("LOGICA_AÑO_DETERMINADA", 
                          f"Guardado: {año_guardado}, Transfermarkt: {año_transfermarkt}")
        
    except ValueError as e:
        error_msg = str(e)
        registrar_ejecucion("ERROR_LOGICA_AÑO", error_msg)
        log_error(error_msg)
        return False
    
    temporada_fotmob = str(año_guardado)
    
    # Reiniciar contador de archivos procesados
    global _ARCHIVOS_PROCESADOS
    _ARCHIVOS_PROCESADOS = {}
    
    log_info(f"🎯 Procesando: Entrada {año_usuario} → Guardado {año_guardado} (Modo: {modo})")
    log_info(f"📊 FotMob: {temporada_fotmob} | Transfermarkt: {año_transfermarkt}")

    # CONEXIÓN ADLS (usa el externo si se proporciona, sino crea uno nuevo)
    if adls_client_externo is not None:
        adls_client = adls_client_externo
        adls_conectado = True
    else:
        adls_client = conectar_adls_keyvault()
        adls_conectado = adls_client is not None
    
    registrar_ejecucion("CONEXION_ADLS", f"Conectado: {adls_conectado}")
    
    if adls_conectado:
        log_info("🌐 Conexión ADLS establecida - Se guardarán archivos en la nube")
    else:
        log_info("⚠️  ADLS no disponible - Solo guardado local")

    for reintento_año in range(max_reintentos_año):
        try:
            registrar_ejecucion("INTENTO_SCRAPING", f"Intento {reintento_año + 1}/{max_reintentos_año}")
            
            # SCRAPING FOTMOB
            log_info(f"📊 Iniciando scraping FotMob...")
            registrar_ejecucion("INICIANDO_FOTMOB", f"Temporada: {temporada_fotmob}")
            
            # 1. CAMPEONES
            registrar_ejecucion("EXTRACCION_CAMPEONES", "Iniciando...")
            campeones = extraer_campeones_temporada(temporada_fotmob)
            if campeones and campeones['data']:
                guardar_json_local_mejorado(campeones, año_guardado, "campeones")
                if adls_conectado:
                    guardar_json_adls(campeones, año_guardado, "campeones", adls_client)
                registrar_ejecucion("CAMPEONES_GUARDADOS", "Datos de campeones guardados")
            else:
                if año_guardado != _AÑO_ACTUAL:
                    registrar_archivo(f"campeones_{año_guardado}.json", "error", "Sin datos de campeones")
                    registrar_ejecucion("CAMPEONES_ERROR", "Sin datos de campeones")

            # 2. EQUIPOS
            registrar_ejecucion("EXTRACCION_EQUIPOS", "Iniciando...")
            equipos = obtener_equipos(temporada_fotmob)
            if equipos and equipos['data']:
                guardar_json_local_mejorado(equipos, año_guardado, "equipos")
                if adls_conectado:
                    guardar_json_adls(equipos, año_guardado, "equipos", adls_client)
                registrar_ejecucion("EQUIPOS_GUARDADOS", f"{len(equipos['data'])} equipos guardados")
            else:
                registrar_archivo(f"equipos_{año_guardado}.json", "error", "Sin datos de equipos")
                registrar_ejecucion("EQUIPOS_ERROR", "Sin datos de equipos")

            # 3. TABLAS CLASIFICACIÓN
            registrar_ejecucion("EXTRACCION_TABLAS", "Iniciando...")
            tablas_clasificacion = extraer_tablas_clasificacion(temporada_fotmob)
            if tablas_clasificacion and tablas_clasificacion['data']:
                guardar_json_local_mejorado(tablas_clasificacion, año_guardado, "tablas_clasificacion")
                if adls_conectado:
                    guardar_json_adls(tablas_clasificacion, año_guardado, "tablas_clasificacion", adls_client)
                registrar_ejecucion("TABLAS_GUARDADAS", "Tablas de clasificación guardadas")
            else:
                registrar_archivo(f"tablas_clasificacion_{año_guardado}.json", "error", "Sin datos de clasificación")
                registrar_ejecucion("TABLAS_ERROR", "Sin datos de clasificación")

            # 4. PARTIDOS
            registrar_ejecucion("EXTRACCION_PARTIDOS", "Iniciando...")
            partidos = obtener_partidos("liga-1", "131", temporada_fotmob)
            if partidos and partidos['data']:
                guardar_json_local_mejorado(partidos, año_guardado, "partidos")
                if adls_conectado:
                    guardar_json_adls(partidos, año_guardado, "partidos", adls_client)
                registrar_ejecucion("PARTIDOS_GUARDADOS", f"{len(partidos['data'])} partidos guardados")
                
                # 5. ESTADÍSTICAS PARTIDOS
                if len(partidos['data']) > 0:
                    registrar_ejecucion("EXTRACCION_ESTADISTICAS", "Iniciando...")
                    estadisticas_partidos = extraer_stats_partidos_mejorada(partidos)
                    if estadisticas_partidos and estadisticas_partidos['data']:
                        guardar_json_local_mejorado(estadisticas_partidos, año_guardado, "estadisticas_partidos")
                        if adls_conectado:
                            guardar_json_adls(estadisticas_partidos, año_guardado, "estadisticas_partidos", adls_client)
                        registrar_ejecucion("ESTADISTICAS_GUARDADAS", "Estadísticas de partidos guardadas")
                    else:
                        registrar_archivo(f"estadisticas_partidos_{año_guardado}.json", "error", "Sin estadísticas de partidos")
                        registrar_ejecucion("ESTADISTICAS_ERROR", "Sin estadísticas de partidos")
                else:
                    registrar_archivo(f"estadisticas_partidos_{año_guardado}.json", "error", "No hay partidos para procesar")
                    registrar_ejecucion("ESTADISTICAS_ERROR", "No hay partidos para procesar")
            else:
                registrar_archivo(f"partidos_{año_guardado}.json", "error", "Sin datos de partidos")
                registrar_ejecucion("PARTIDOS_ERROR", "Sin datos de partidos")

            # SCRAPING TRANSFERMARKT (CON AÑO CORREGIDO)
            log_info(f"👥 Iniciando scraping Transfermarkt...")
            registrar_ejecucion("INICIANDO_TRANSFERMARKT", f"Año: {año_transfermarkt}")
            
            # Pasar el año correcto para Transfermarkt
            datos_generales, datos_plantillas, datos_estadios, datos_entrenadores = scraping_transfermarkt_por_año(año_transfermarkt)
            
            # 6. DATOS GENERALES
            registrar_ejecucion("PROCESANDO_DATOS_GENERALES", "Iniciando...")
            if datos_generales:
                df_general = pd.DataFrame(datos_generales)
                guardar_csv_local_mejorado(df_general, año_guardado, "liga1", "Transfermarkt")
                if adls_conectado:
                    guardar_csv_adls(df_general, año_guardado, "liga1", adls_client, "Transfermarkt")
                registrar_ejecucion("DATOS_GENERALES_GUARDADOS", f"{len(datos_generales)} registros guardados")
            else:
                registrar_archivo(f"liga1_{año_guardado}.csv", "error", "Sin datos generales")
                registrar_ejecucion("DATOS_GENERALES_ERROR", "Sin datos generales")

            # 7. PLANTILLAS
            registrar_ejecucion("PROCESANDO_PLANTILLAS", "Iniciando...")
            if datos_plantillas:
                df_plantillas = pd.DataFrame(datos_plantillas)
                guardar_csv_local_mejorado(df_plantillas, año_guardado, "plantillas", "Transfermarkt")
                if adls_conectado:
                    guardar_csv_adls(df_plantillas, año_guardado, "plantillas", adls_client, "Transfermarkt")
                registrar_ejecucion("PLANTILLAS_GUARDADAS", f"{len(datos_plantillas)} jugadores guardados")
            else:
                registrar_archivo(f"plantillas_{año_guardado}.csv", "error", "Sin datos de plantillas")
                registrar_ejecucion("PLANTILLAS_ERROR", "Sin datos de plantillas")

            # 8. ESTADIOS
            registrar_ejecucion("PROCESANDO_ESTADIOS", "Iniciando...")
            if datos_estadios:
                df_estadios = pd.DataFrame(datos_estadios)
                guardar_csv_local_mejorado(df_estadios, año_guardado, "estadios", "Transfermarkt")
                if adls_conectado:
                    guardar_csv_adls(df_estadios, año_guardado, "estadios", adls_client, "Transfermarkt")
                registrar_ejecucion("ESTADIOS_GUARDADOS", f"{len(datos_estadios)} estadios guardados")
            else:
                registrar_archivo(f"estadios_{año_guardado}.csv", "error", "Sin datos de estadios")
                registrar_ejecucion("ESTADIOS_ERROR", "Sin datos de estadios")

            # 9. ENTRENADORES
            registrar_ejecucion("PROCESANDO_ENTRENADORES", "Iniciando...")
            if datos_entrenadores:
                df_entrenadores = pd.DataFrame(datos_entrenadores)
                guardar_csv_local_mejorado(df_entrenadores, año_guardado, "entrenadores", "Transfermarkt")
                if adls_conectado:
                    guardar_csv_adls(df_entrenadores, año_guardado, "entrenadores", adls_client, "Transfermarkt")
                registrar_ejecucion("ENTRENADORES_GUARDADOS", f"{len(datos_entrenadores)} entrenadores guardados")
            else:
                registrar_archivo(f"entrenadores_{año_guardado}.csv", "error", "Sin datos de entrenadores")
                registrar_ejecucion("ENTRENADORES_ERROR", "Sin datos de entrenadores")

            # ✅ VALIDAR Y SOLO GUARDAR REPORTE SI TODO ESTÁ OK
            registrar_ejecucion("VALIDANDO_ARCHIVOS", "Iniciando validación...")
            validacion_ok, mensaje = validar_archivos_generados(año_guardado)
            
            if validacion_ok:
                registrar_ejecucion("VALIDACION_EXITOSA", f"Año {año_guardado}")
                
                # ✅ GUARDAR REPORTE EN AMBOS LADOS SIEMPRE cuando validación OK
                reporte_guardado = guardar_reporte_archivos(año_guardado, adls_client, adls_conectado)
                
                # ✅ CONTAR ARCHIVOS GENERADOS (solo para retorno)
                archivos_generados = contar_archivos_generados(año_guardado)
                registrar_ejecucion("ARCHIVOS_CONTABILIZADOS", f"{len(archivos_generados)} archivos")
                
                # ✅ SOLO CREAR TRIGGER SI NO ES MODO HISTÓRICO (para histórico se crea uno al final)
                if modo != "historico":
                    trigger_creado = guardar_trigger_adf(
                        año_guardado=año_guardado,
                        modo=modo,
                        archivos_generados=archivos_generados,
                        estado="completado",
                        adls_client=adls_client if adls_conectado else None
                    )
                    
                    # ✅ GUARDAR LOG ÚNICO
                    log_guardado = guardar_log_unico(año_guardado, modo, 
                                                   adls_client if adls_conectado else None)
                    
                    if trigger_creado:
                        registrar_ejecucion("TRIGGER_ADF_CREADO", f"Año {año_guardado} listo para ADF")
                        log_info(f"🎯 TRIGGER ADF ACTIVADO para año {año_guardado}")
                
                registrar_ejecucion("EJECUCION_COMPLETADA", "Proceso finalizado exitosamente")
                log_info(f"🎉 PROCESAMIENTO COMPLETADO para año {año_guardado}")
                return True
            else:
                # ❌ SI FALLA, CREAR TRIGGER DE ERROR (solo si no es histórico)
                log_error(f"❌ Validación fallida para año {año_guardado}: {mensaje}")
                registrar_ejecucion("VALIDACION_FALLIDA", mensaje)
                
                if modo != "historico":
                    guardar_trigger_adf(
                        año_guardado=año_guardado,
                        modo=modo,
                        archivos_generados=[],
                        estado="error",
                        adls_client=adls_client if adls_conectado else None
                    )
                    guardar_log_unico(año_guardado, modo, adls_client if adls_conectado else None)
                return False
                
        except Exception as e:
            error_msg = f"Error en intento {reintento_año + 1}: {str(e)}"
            registrar_ejecucion("ERROR_EJECUCION", error_msg)
            log_error(f"Error en intento {reintento_año + 1} para año {año_guardado}", e)
            
            if reintento_año < max_reintentos_año - 1:
                log_info(f"🔄 Reintentando año {año_guardado} en 5 segundos...")
                time.sleep(5)
            else:
                log_error(f"💥 Año {año_guardado} falló después de {max_reintentos_año} intentos")
                # Guardar log de ejecución fallida (solo si no es histórico)
                if modo != "historico":
                    guardar_log_unico(año_guardado, modo, adls_client if adls_conectado else None)
                return False
    
    return False
    
# =============================================================================
# MODOS DE EJECUCIÓN (VERSIÓN CORREGIDA CON AÑO DINÁMICO)
# =============================================================================

def ejecutar_modo_historico(año_inicio=2020, año_fin=None):
    """Ejecuta scraping para rango de años históricos - UN SOLO TRIGGER AL FINAL"""
    
    # Si no se especifica año_fin, usar año actual
    if año_fin is None:
        año_fin = _AÑO_ACTUAL
    
    # Validar rango
    if año_inicio < _AÑO_MINIMO:
        año_inicio = _AÑO_MINIMO
        log_info(f"⚠️  Ajustando año inicio a {_AÑO_MINIMO} (mínimo permitido)")
    
    if año_fin > _AÑO_ACTUAL:
        año_fin = _AÑO_ACTUAL
        log_info(f"⚠️  Ajustando año fin a {_AÑO_ACTUAL} (máximo permitido)")
    
    log_info(f"📅 MODO HISTÓRICO: {año_inicio} - {año_fin}")
    log_info(f"   → FotMob: mismos años | Transfermarkt: años-1 | Guardado: mismos años")
    
    años_procesados = 0
    años_exitosos = 0
    años_fallidos = []
    todos_archivos_generados = []
    
    # CONEXIÓN ADLS UNA SOLA VEZ PARA TODOS LOS AÑOS
    adls_client = conectar_adls_keyvault()
    adls_conectado = adls_client is not None
    
    for año in range(año_inicio, año_fin + 1):
        log_info(f"\n{'='*50}")
        log_info(f"PROCESANDO AÑO: {año}")
        log_info(f"FotMob: {año} | Transfermarkt: {año-1} | Guardado: {año}")
        log_info(f"{'='*50}")
        
        try:
            # Reiniciar log de ejecución para cada año
            global _EJECUCION_LOG
            _EJECUCION_LOG = []
            
            registrar_ejecucion("INICIANDO_AÑO", f"Año: {año}")
            resultado = scraping_completo_por_año(año, modo="historico", adls_client_externo=adls_client)
            años_procesados += 1
            
            if resultado:
                años_exitosos += 1
                # Acumular archivos generados para el trigger final
                archivos_año = contar_archivos_generados(año)
                todos_archivos_generados.extend(archivos_año)
                log_info(f"✅ Año {año} COMPLETADO")
            else:
                años_fallidos.append(año)
                log_info(f"❌ Año {año} FALLADO")
                
        except Exception as e:
            log_error(f"Error crítico procesando año {año}", e)
            años_fallidos.append(año)
    
    # ✅ CREAR UN SOLO TRIGGER AL FINAL DEL MODO HISTÓRICO
    if años_exitosos > 0:
        log_info(f"🎯 CREANDO TRIGGER FINAL PARA MODO HISTÓRICO")
        trigger_creado = guardar_trigger_adf(
            año_guardado=f"rango_{año_inicio}_{año_fin}",
            modo="historico",
            archivos_generados=todos_archivos_generados,
            estado="completado",
            adls_client=adls_client if adls_conectado else None
        )
        
        # ✅ GUARDAR LOG ÚNICO DE LA EJECUCIÓN COMPLETA
        log_guardado = guardar_log_unico(
            año_guardado=f"rango_{año_inicio}_{año_fin}", 
            modo="historico",
            adls_client=adls_client if adls_conectado else None
        )
        
        if trigger_creado:
            log_info(f"🎉 MODO HISTÓRICO COMPLETADO - {años_exitosos} años exitosos")
            log_info(f"📊 Total archivos generados: {len(todos_archivos_generados)}")
            log_info(f"🎯 TRIGGER ADF ACTIVADO para rango completo")
    
    return años_exitosos, años_fallidos

def ejecutar_modo_incremental():
    """Ejecuta scraping solo para el año actual"""
    log_info(f"🔄 MODO INCREMENTAL: Año actual {_AÑO_ACTUAL} → Guarda: {_AÑO_ACTUAL}")
    log_info(f"   → FotMob: {_AÑO_ACTUAL} | Transfermarkt: {_AÑO_ACTUAL-1} | Guardado: {_AÑO_ACTUAL}")
    
    # Pasar modo "incremental" explícitamente
    resultado = scraping_completo_por_año(_AÑO_ACTUAL, modo="incremental")
    return resultado

def ejecutar_modo_reproceso(año_reproceso):
    """Ejecuta scraping para un año específico (2020-año_actual)"""
    
    # Validar año de reproceso
    if año_reproceso < _AÑO_MINIMO or año_reproceso > _AÑO_ACTUAL:
        log_error(f"Solo se puede reprocesar años entre {_AÑO_MINIMO}-{_AÑO_ACTUAL}")
        return False
    
    log_info(f"🔧 MODO REPROCESO: Año {año_reproceso}")
    log_info(f"   → FotMob: {año_reproceso} | Transfermarkt: {año_reproceso-1} | Guardado: {año_reproceso}")
    
    resultado = scraping_completo_por_año(año_reproceso, modo="reproceso")
    return resultado

# =============================================================================
# FUNCIÓN PRINCIPAL UNIFICADA (VERSIÓN CORREGIDA CON AÑO DINÁMICO)
# =============================================================================

def run_scraping_liga1(modo="historica", anio_inicio=2020, anio_fin=None, anio_objetivo=None):
    """
    FUNCIÓN PRINCIPAL UNIFICADA - SCRAPING LIGA 1 PERÚ CON AÑO DINÁMICO
    ====================================================================
    
    Parámetros:
    -----------
    modo : str
        "historica", "incremental" o "reproceso"
    anio_inicio : int (2020-año_actual)
        Para modo histórica: año inicial del rango
    anio_fin : int (2020-año_actual)  
        Para modo histórica: año final del rango (default: año_actual)
    anio_objetivo : int (2020-año_actual)
        Para modo reproceso: año específico a reprocesar
    """
    
    print("🎯 SCRAPING LIGA 1 PERÚ - AÑO DINÁMICO DEL SISTEMA")
    print("=" * 70)
    print(f"📊 COMPORTAMIENTO (Año actual: {_AÑO_ACTUAL}):")
    print("   - Entrada X → FotMob: X, Transfermarkt: X-1, Guardado: X")
    print(f"   - Rango permitido: {_AÑO_MINIMO}-{_AÑO_ACTUAL}")
    print("=" * 70)
    
    inicio_total = time.time()
    
    try:
        if modo == "historica":
            # Si no se especifica año_fin, usar año actual
            if anio_fin is None:
                anio_fin = _AÑO_ACTUAL
                
            # Validar y ajustar rango
            if anio_inicio < _AÑO_MINIMO: 
                anio_inicio = _AÑO_MINIMO
            if anio_fin > _AÑO_ACTUAL: 
                anio_fin = _AÑO_ACTUAL
                
            print(f"MODO HISTÓRICA: Procesando {anio_inicio}..{anio_fin}")
            print(f"   FotMob: mismos años | Transfermarkt: años-1 | Guardado: mismos años")
            
            años_exitosos, años_fallidos = ejecutar_modo_historico(anio_inicio, anio_fin)
            
        elif modo == "incremental":
            print(f"MODO INCREMENTAL: Año actual {_AÑO_ACTUAL}")
            print(f"   FotMob: {_AÑO_ACTUAL} | Transfermarkt: {_AÑO_ACTUAL-1} | Guardado: {_AÑO_ACTUAL}")
            
            resultado = ejecutar_modo_incremental()
            años_exitosos = 1 if resultado else 0
            años_fallidos = [] if resultado else [_AÑO_ACTUAL]
            
        elif modo == "reproceso":
            # Validar parámetro para modo reproceso
            if anio_objetivo is None:
                anio_objetivo = _AÑO_ACTUAL  # Default: año actual
                
            if anio_objetivo < _AÑO_MINIMO or anio_objetivo > _AÑO_ACTUAL:
                print(f"❌ Error: Solo se puede reprocesar años {_AÑO_MINIMO}-{_AÑO_ACTUAL}")
                return 0, [anio_objetivo]
                
            print(f"MODO REPROCESO: Año {anio_objetivo}")
            print(f"   FotMob: {anio_objetivo} | Transfermarkt: {anio_objetivo-1} | Guardado: {anio_objetivo}")
            
            resultado = ejecutar_modo_reproceso(anio_objetivo)
            años_exitosos = 1 if resultado else 0
            años_fallidos = [] if resultado else [anio_objetivo]
            
        else:
            print("❌ Modo no reconocido. Usando modo incremental por defecto.")
            resultado = ejecutar_modo_incremental()
            años_exitosos = 1 if resultado else 0
            años_fallidos = [] if resultado else [_AÑO_ACTUAL]
        
        tiempo_total = time.time() - inicio_total
        
        print(f"\n{'='*70}")
        print("🎉 PROCESAMIENTO COMPLETADO")
        print(f"{'='*70}")
        print(f"⏱️  Tiempo total: {tiempo_total:.1f} segundos")
        print(f"📊 Años exitosos: {años_exitosos}")
        print(f"❌ Años fallidos: {len(años_fallidos)}")
        
        if años_fallidos:
            print(f"🔧 Años para reprocesar: {años_fallidos}")
        
        return años_exitosos, años_fallidos
        
    except Exception as e:
        log_error("Error en ejecución principal", e)
        print(f"\n💥 PROCESAMIENTO INTERRUMPIDO POR ERROR")
        return 0, [anio_inicio] if anio_inicio else [_AÑO_MINIMO]

# =============================================================================
# EJECUCIÓN PRINCIPAL
# =============================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Scraping Liga 1 Perú")
    parser.add_argument(
        "--modo",
        choices=["historica", "incremental", "reproceso"],
        default=os.environ.get("SCRAPING_MODO", "incremental")
    )
    parser.add_argument(
        "--anio-inicio", type=int,
        default=int(os.environ.get("SCRAPING_ANIO_INICIO", _AÑO_MINIMO))
    )
    parser.add_argument("--anio-fin", type=int, default=None)
    parser.add_argument(
        "--anio-objetivo", type=int,
        default=int(os.environ.get("SCRAPING_ANIO_OBJETIVO") or _AÑO_ACTUAL)
    )
    args = parser.parse_args()

    result_global = run_scraping_liga1(
        modo=args.modo,
        anio_inicio=args.anio_inicio,
        anio_fin=args.anio_fin,
        anio_objetivo=args.anio_objetivo
    )
    exitosos, fallidos = result_global if isinstance(result_global, tuple) else (result_global, [])
    print(f"Exitosos: {exitosos} | Fallidos: {len(fallidos)}")
    if fallidos:
        raise SystemExit(1)