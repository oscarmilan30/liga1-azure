# =============================================================================
# SCRAPING COMPLETO LIGA 1 PERÚ - FOTMOB + TRANSFERMARKT + WIKIPEDIA (SOLO ADLS)
# =============================================================================
# VERSIÓN CON LIMPIEZA DE NOMBRES DE COLUMNA Y NORMALIZACIÓN DE VALORES MONETARIOS
# =============================================================================

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
try:
    import undetected_chromedriver as uc
    _UC_DISPONIBLE = True
except ImportError:
    _UC_DISPONIBLE = False
try:
    from curl_cffi import requests as _curl_requests
    _CURL_CFFI_DISPONIBLE = True
except ImportError:
    _CURL_CFFI_DISPONIBLE = False
from datetime import datetime, timedelta
from unidecode import unidecode
from concurrent.futures import ThreadPoolExecutor, as_completed
from azure.keyvault.secrets import SecretClient
from azure.identity import ClientSecretCredential, InteractiveBrowserCredential
from azure.storage.filedatalake import DataLakeServiceClient
import pandas as pd
import time
import unicodedata
import json
import os
import re
import requests
import argparse
import gc
import tempfile

# =============================================================================
# CONFIGURACIÓN GLOBAL
# =============================================================================

_AÑO_ACTUAL = datetime.now().year
_AÑO_MINIMO = 2020

# Configuración de workers
MAX_WORKERS_FOTMOB = 3
MAX_WORKERS_TRANSFER = 2
BATCH_SIZE = 50
TIMEOUT_PARTIDO = 30

# Variables globales
_ADLS_CLIENT = None
_CREDENTIAL = None
_ARCHIVOS_PROCESADOS = {}
_EJECUCION_LOG = []
_CACHE_ESTADISTICAS = {}
_CACHE_EXPIRACION = 3600

# =============================================================================
# DETECCIÓN DE ENTORNO Y RUTAS (SOLO PARA LOGS Y TRIGGERS)
# =============================================================================

def get_ruta_base():
    if os.environ.get("GITHUB_ACTIONS") == "true":
        return os.path.join(tempfile.gettempdir(), "liga1_scraping")
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "temp_scraping")

def get_ruta_logs():
    return os.path.join(get_ruta_base(), "logs")

def get_ruta_triggers():
    return os.path.join(get_ruta_base(), "triggers")

os.makedirs(get_ruta_logs(), exist_ok=True)
os.makedirs(get_ruta_triggers(), exist_ok=True)

# =============================================================================
# FUNCIONES DE LOG Y TRIGGERS
# =============================================================================

def registrar_ejecucion(accion, detalles=""):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    registro = {"timestamp": timestamp, "accion": accion, "detalles": detalles}
    _EJECUCION_LOG.append(registro)
    print(f"[{timestamp}] {accion} {detalles}")

def log_info(mensaje):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {mensaje}")

def log_error(mensaje, excepcion=None):
    print(f"[{datetime.now().strftime('%H:%M:%S')}]  {mensaje}")
    if excepcion:
        print(f"   {excepcion}")

def registrar_archivo(nombre_archivo, estado, detalles=""):
    _ARCHIVOS_PROCESADOS[nombre_archivo] = {
        'estado': estado,
        'detalles': detalles,
        'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    print(f"{'' if estado == 'éxito' else ''} {nombre_archivo} - {detalles}")

def generar_reporte_archivos():
    reporte = []
    reporte.append("=" * 70)
    reporte.append(" REPORTE DETALLADO DE ARCHIVOS - SCRAPING LIGA 1")
    reporte.append("=" * 70)
    
    exitosos = 0
    errores = 0
    
    for archivo, info in _ARCHIVOS_PROCESADOS.items():
        estado_char = "" if info['estado'] == "éxito" else ""
        reporte.append(f"{estado_char} {archivo} | Estado: {info['estado']} | Hora: {info['timestamp']}")
        if info['estado'] == "éxito":
            exitosos += 1
        else:
            errores += 1
    
    reporte.append("=" * 70)
    reporte.append(f" RESUMEN: {exitosos} exitosos, {errores} errores")
    reporte.append("=" * 70)
    return "\n".join(reporte)

def guardar_trigger_adf(año_guardado, modo, archivos_generados, estado="completado", adls_client=None):
    try:
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
            "version": "3.0"
        }
        
        trigger_local_path = os.path.join(get_ruta_triggers(), "scraping_completado.json")
        with open(trigger_local_path, 'w', encoding='utf-8') as f:
            json.dump(trigger_data, f, ensure_ascii=False, indent=2)
        
        if adls_client and estado == "completado":
            adls_path = "primera_division/landing/temp/ejecucion/scraping_completado.json"
            file_client = adls_client.get_file_client(adls_path)
            file_client.upload_data(json.dumps(trigger_data, ensure_ascii=False, indent=2), overwrite=True)
        
        registrar_ejecucion("TRIGGER_CREADO", f"ID: {trigger_id}")
        return True
    except Exception as e:
        log_error(f"Error creando trigger: {e}")
        return False

def guardar_log_unico(año_guardado, modo, adls_client=None):
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
        
        log_local_path = os.path.join(get_ruta_logs(), "ultima_ejecucion.json")
        with open(log_local_path, 'w', encoding='utf-8') as f:
            json.dump(log_data, f, ensure_ascii=False, indent=2)
        
        if adls_client:
            adls_path = "primera_division/landing/temp/logs/ultima_ejecucion.json"
            file_client = adls_client.get_file_client(adls_path)
            file_client.upload_data(json.dumps(log_data, ensure_ascii=False, indent=2), overwrite=True)
        
        return True
    except Exception as e:
        log_error(f"Error guardando log: {e}")
        return False

def contar_archivos_generados(año_guardado):
    # Lista de archivos que se suben a ADLS (para el trigger)
    archivos_base = ["partidos", "equipos", "campeones", "tablas_clasificacion", "estadisticas_partidos", "liga1", "plantillas", "estadios", "entrenadores", "estadisticas_jugadores"]
    return [{"nombre": f"{archivo}_{año_guardado}.json"} for archivo in archivos_base]

# =============================================================================
# CONEXIÓN ADLS
# =============================================================================

def conectar_adls_keyvault():
    global _ADLS_CLIENT, _CREDENTIAL
    if _ADLS_CLIENT is not None:
        return _ADLS_CLIENT
    
    try:
        log_info("Conectando a Azure Key Vault...")
        tenant_id = os.environ.get("AZURE_TENANT_ID")
        client_id = os.environ.get("AZURE_CLIENT_ID")
        client_secret = os.environ.get("AZURE_CLIENT_SECRET")
        
        if tenant_id and client_id and client_secret:
            _CREDENTIAL = ClientSecretCredential(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)
        else:
            _CREDENTIAL = InteractiveBrowserCredential()
        
        key_vault_url = "https://kv-liga1-secreto.vault.azure.net/"
        secret_client = SecretClient(vault_url=key_vault_url, credential=_CREDENTIAL)
        
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
# LIMPIEZA DE NOMBRES DE COLUMNA PARA PARQUET / ADF
# =============================================================================

def limpiar_nombre_columna(nombre):
    """
    Limpia el nombre de una columna para que sea compatible con Parquet
    y Azure Data Factory:
    - Reemplaza espacios por '_'
    - Elimina caracteres prohibidos: [, ; { } ( ) \n \t = ]
    - Elimina símbolos como €, %, °
    - Convierte acentos a ASCII
    - Todo a minúsculas
    """
    if not isinstance(nombre, str):
        nombre = str(nombre)
    nombre = nombre.strip()
    nombre = nombre.replace(' ', '_')
    nombre = re.sub(r'[\[,;{}()\n\t=]', '', nombre)
    nombre = re.sub(r'[€%°]', '', nombre)
    # Normalizar acentos (á → a, etc.)
    nombre = unicodedata.normalize('NFKD', nombre).encode('ASCII', 'ignore').decode('ASCII')
    # Reemplazar múltiples guiones bajos por uno solo
    nombre = re.sub(r'_+', '_', nombre)
    nombre = nombre.strip('_')
    nombre = nombre.lower()
    if not nombre:
        nombre = 'columna'
    return nombre

# =============================================================================
# LIMPIEZA DE VALORES MONETARIOS (NUEVO)
# =============================================================================

def limpiar_valor_monetario(valor):
    """
    Convierte valores monetarios a número.
    Formatos soportados:
      Español Transfermarkt: '523 mil €' -> 523000, '21,43 mill. €' -> 21430000
      Inglés Transfermarkt:  '€502k'     -> 502000, '€16.05m'       -> 16050000
    Si no es monetario, devuelve el valor original.
    """
    import re
    if pd.isna(valor) or valor == '' or valor == 'No disponible' or valor is None:
        return valor
    valor_str = str(valor).strip()

    # --- Formato español Transfermarkt: "523 mil €" / "21,43 mill. €" ---
    # Detectar 'mill' antes que 'mil' para evitar falso positivo
    valor_lower = valor_str.lower()
    if 'mill' in valor_lower:
        match = re.search(r'([0-9]+[,.]?[0-9]*)', valor_str)
        if match:
            try:
                num = float(match.group(1).replace(',', '.')) * 1_000_000
                return int(num) if num.is_integer() else num
            except:
                pass
    elif 'mil' in valor_lower:
        match = re.search(r'([0-9]+[,.]?[0-9]*)', valor_str)
        if match:
            try:
                num = float(match.group(1).replace(',', '.')) * 1_000
                return int(num) if num.is_integer() else num
            except:
                pass

    # --- Formato inglés Transfermarkt: "€502k" / "€16.05m" ---
    valor_str = valor_str.replace('€', '').replace(' ', '').lower()
    if valor_str.endswith('k'):
        try:
            num = float(valor_str[:-1]) * 1000
            return int(num) if num.is_integer() else num
        except:
            return valor
    elif valor_str.endswith('m'):
        try:
            num = float(valor_str[:-1]) * 1_000_000
            return int(num) if num.is_integer() else num
        except:
            return valor
    else:
        # Intentar convertir a número directamente
        try:
            # Reemplazar coma decimal por punto si es necesario
            if ',' in valor_str and '.' not in valor_str:
                valor_str = valor_str.replace(',', '.')
            if valor_str.isdigit():
                return int(valor_str)
            else:
                return float(valor_str)
        except:
            return valor

def normalizar_valores_dataframe(df):
    """
    Aplica limpieza de valores monetarios a todas las columnas que
    contengan palabras clave como 'valor', 'precio', 'mercado', 'total', 'medio'.
    También elimina el símbolo % de columnas de porcentaje (opcional).
    """
    df_limpio = df.copy()
    for col in df_limpio.columns:
        if df_limpio[col].dtype == 'object':
            # Detectar si la columna parece tener valores monetarios
            muestra = df_limpio[col].dropna().head(5).astype(str)
            if any('€' in str(v) for v in muestra):
                df_limpio[col] = df_limpio[col].apply(limpiar_valor_monetario)
            # También si el nombre de la columna contiene palabras clave
            elif any(keyword in col for keyword in ['valor', 'precio', 'mercado', 'total', 'medio']):
                df_limpio[col] = df_limpio[col].apply(limpiar_valor_monetario)
            # Opcional: eliminar símbolo % de porcentajes
            elif any('%' in str(v) for v in muestra):
                df_limpio[col] = df_limpio[col].astype(str).str.replace('%', '', regex=False)
    return df_limpio

# =============================================================================
# FUNCIONES DE GUARDADO SOLO EN ADLS (MODIFICADA PARA LIMPIAR VALORES)
# =============================================================================

def guardar_json_adls(datos, año_guardado, tipo_archivo, adls_client):
    nombre_archivo = f"{tipo_archivo}_{año_guardado}.json"
    try:
        adls_path = f"primera_division/landing/{año_guardado}/{nombre_archivo}"
        file_client = adls_client.get_file_client(adls_path)
        file_client.upload_data(json.dumps(datos, ensure_ascii=False, indent=2), overwrite=True)
        registrar_archivo(nombre_archivo, "éxito", f"ADLS: {adls_path}")
        return True
    except Exception as e:
        registrar_archivo(nombre_archivo, "error", str(e))
        return False

def guardar_csv_adls(dataframe, año_guardado, tipo_archivo, adls_client, fuente="Transfermarkt"):
    """
    Guarda un DataFrame como CSV en ADLS, limpiando los nombres de columna
    y normalizando los valores monetarios para evitar errores en Data Factory.
    """
    nombre_archivo = f"{tipo_archivo}_{año_guardado}.csv"
    try:
        if isinstance(dataframe, pd.DataFrame):
            dataframe_con_metadatos = dataframe.copy()
            # 1. Limpiar nombres de columnas
            dataframe_con_metadatos.columns = [limpiar_nombre_columna(col) for col in dataframe_con_metadatos.columns]
            # 2. Normalizar valores monetarios y numéricos
            dataframe_con_metadatos = normalizar_valores_dataframe(dataframe_con_metadatos)
            # 3. Agregar metadatos
            dataframe_con_metadatos['fuente'] = fuente
            dataframe_con_metadatos['temporada'] = año_guardado
            dataframe_con_metadatos['fecha_carga'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            # 4. Generar CSV como bytes UTF-8-SIG para evitar doble-encoding en ADLS SDK
            csv_bytes = dataframe_con_metadatos.to_csv(index=False, encoding="utf-8-sig", sep='|').encode('utf-8-sig')
            adls_path = f"primera_division/landing/{año_guardado}/{nombre_archivo}"
            file_client = adls_client.get_file_client(adls_path)
            file_client.upload_data(csv_bytes, overwrite=True)
            registrar_archivo(nombre_archivo, "éxito", f"ADLS: {adls_path}")
            return True
    except Exception as e:
        registrar_archivo(nombre_archivo, "error", str(e))
        return False

def guardar_reporte_archivos(año_guardado, adls_client=None, adls_conectado=False):
    try:
        reporte = generar_reporte_archivos()
        if adls_conectado and adls_client:
            adls_path = f"primera_division/landing/{año_guardado}/reporte_archivos_{año_guardado}.txt"
            file_client = adls_client.get_file_client(adls_path)
            file_client.upload_data(reporte, overwrite=True)
        return True
    except Exception as e:
        log_error(f"Error guardando reporte: {e}")
        return False

# =============================================================================
# 1. FUNCIONES FOTMOB
# =============================================================================

def normalizar_fecha(fecha_raw, temporada):
    """
    Convierte strings de fecha en distintos formatos al estándar dd-mm-yyyy.

    Maneja tres casos de FotMob:
    - Texto relativo: 'hoy', 'mañana'
    - Formato español: '15 de marzo de 2024'  →  '15-03-2024'
    - Formato ISO o guiones: '2024-03-15' o '15-03-2024'

    Si no puede parsear devuelve fecha_raw sin modificar (no lanza excepción).
    """
    if not fecha_raw or fecha_raw == "N/A":
        return fecha_raw
    
    meses = {
        'enero': '01', 'febrero': '02', 'marzo': '03', 'abril': '04',
        'mayo': '05', 'junio': '06', 'julio': '07', 'agosto': '08',
        'septiembre': '09', 'octubre': '10', 'noviembre': '11', 'diciembre': '12'
    }
    
    fecha_lower = fecha_raw.lower()
    
    if fecha_lower == 'hoy':
        return datetime.now().strftime("%d-%m-%Y")
    if fecha_lower == 'mañana':
        return (datetime.now() + timedelta(days=1)).strftime("%d-%m-%Y")
    
    if ' de ' in fecha_raw:
        try:
            parte_fecha = fecha_raw.split(',')[-1].strip()
            partes = parte_fecha.split(' de ')
            if len(partes) >= 2:
                dia = partes[0].strip().zfill(2)
                mes = meses.get(partes[1].lower(), '00')
                año = partes[2].strip() if len(partes) >= 3 else str(temporada)
                return f"{dia}-{mes}-{año}"
        except:
            pass
    
    if '-' in fecha_raw:
        partes = fecha_raw.split('-')
        if len(partes) == 3:
            if len(partes[0]) == 4:
                return f"{partes[2].zfill(2)}-{partes[1].zfill(2)}-{partes[0]}"
            else:
                return f"{partes[0].zfill(2)}-{partes[1].zfill(2)}-{partes[2]}"
    
    return fecha_raw

def extraer_partidos(temporada):
    """
    Extrae todos los partidos de la Liga 1 para una temporada desde FotMob.

    Navega paginando hasta 30 jornadas y agrupa los partidos por fecha.
    Usa un set de fechas vistas para evitar duplicados entre páginas.

    Args:
        temporada: año de la temporada (ej: 2024). FotMob usa año calendario.

    Returns:
        Lista de dicts con id_partido, equipos, marcador, fecha, url y fase.
    """
    print(f"\n 1. PARTIDOS {temporada}...")
    
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    
    driver = webdriver.Chrome(options=options)
    partidos = []
    fechas_vistas = set()
    
    try:
        for page in range(30):
            url = f"https://www.fotmob.com/es/leagues/131/fixtures/liga-1?season={temporada}&group=by-date&page={page}"
            driver.get(url)
            time.sleep(2)
            
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            secciones = soup.select('section[class*="LeagueMatchesSectionCSS"]')
            
            if not secciones:
                break
            
            for seccion in secciones:
                fecha_tag = seccion.select_one('h3')
                fecha_raw = fecha_tag.text.strip() if fecha_tag else "N/A"
                fecha_limpia = normalizar_fecha(fecha_raw, temporada)
                
                if fecha_raw in fechas_vistas:
                    continue
                fechas_vistas.add(fecha_raw)
                
                for match in seccion.select('a[class*="MatchWrapper"]'):
                    try:
                        local = match.select_one('div[class*="StatusAndHomeTeamWrapper"] span[class*="TeamName"]')
                        visitante = match.select_one('div[class*="AwayTeamAndFollowWrapper"] span[class*="TeamName"]')
                        marcador = match.select_one('span[class*="LSMatchStatusScore"]')
                        marcador = marcador.text.strip() if marcador else "Sin jugar"
                        
                        if local and visitante:
                            href = match.get("href", "")
                            url_partido = f"https://www.fotmob.com{href}" if href else ""
                            
                            partidos.append({
                                "fecha": fecha_limpia,
                                "local": local.text.strip(),
                                "visitante": visitante.text.strip(),
                                "marcador": marcador,
                                "url": url_partido
                            })
                    except:
                        pass
            time.sleep(0.5)
        
        print(f"    {len(partidos)} partidos")
        return partidos
    finally:
        driver.quit()

def extraer_campeones(temporada, año_actual):
    print(f"\n 2. CAMPEONES {temporada}...")
    
    if temporada >= año_actual:
        print(f"   ⏭ Temporada en curso")
        return None
    
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    
    driver = webdriver.Chrome(options=options)
    try:
        driver.get("https://www.fotmob.com/es/leagues/131/seasons/liga-1/teams")
        time.sleep(5)
        
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        trophy_items = soup.find_all('div', class_=re.compile(r'TLTrophyItemCSS'))
        
        for item in trophy_items:
            year_link = item.find('a', href=re.compile(r'season='))
            if not year_link:
                continue
            
            year_text = year_link.text.strip()
            if year_text != str(temporada):
                continue
            
            teams = item.find_all('div', class_=re.compile(r'TeamTextContainer'))
            campeon = None
            subcampeon = None
            
            for team in teams:
                name_span = team.find('span', class_=re.compile(r'TeamName'))
                role_span = team.find('span', class_=re.compile(r'SubText'))
                
                if name_span and role_span:
                    nombre = name_span.text.strip()
                    rol = role_span.text.strip()
                    
                    if rol == 'Campeón':
                        campeon = nombre
                    elif rol == 'Subcampeón':
                        subcampeon = nombre
            
            if campeon:
                print(f"    {campeon} (campeón)")
                return [{'campeon': campeon, 'subcampeon': subcampeon if subcampeon else ''}]
        
        print(f"    No encontrado")
        return []
        
    except Exception as e:
        print(f"    Error: {e}")
        return []
    finally:
        driver.quit()

def extraer_equipos(temporada, partidos=None):
    print(f"\n 3. EQUIPOS {temporada}...")
    
    if temporada == 2025 and partidos:
        equipos = []
        seen = set()
        for partido in partidos:
            local = partido.get('local')
            visitante = partido.get('visitante')
            if local and local not in seen:
                seen.add(local)
                equipos.append({"equipo": local, "url": ""})
            if visitante and visitante not in seen:
                seen.add(visitante)
                equipos.append({"equipo": visitante, "url": ""})
        print(f"    {len(equipos)} equipos (desde partidos)")
        return equipos
    
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    
    driver = webdriver.Chrome(options=options)
    equipos = []
    seen = set()
    
    try:
        if temporada >= datetime.now().year:
            url = "https://www.fotmob.com/es/leagues/131/table/liga-1"
        else:
            url = f"https://www.fotmob.com/es/leagues/131/table/liga-1?season={temporada}"
        
        driver.get(url)
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.CSS_SELECTOR, "body")))
        time.sleep(5)
        
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        for a in soup.find_all('a', href=True):
            href = a.get('href', '')
            if '/teams/' in href and 'overview' in href:
                nombre_span = a.select_one('span.TeamName')
                if nombre_span:
                    nombre = nombre_span.text.strip()
                else:
                    nombre = a.text.strip()
                
                if nombre and len(nombre) > 2 and nombre not in seen and "Perú" not in nombre:
                    if len(nombre) > 30:
                        mitad = len(nombre) // 2
                        primera_mitad = nombre[:mitad]
                        segunda_mitad = nombre[mitad:]
                        if primera_mitad == segunda_mitad:
                            nombre = primera_mitad
                    
                    seen.add(nombre)
                    url_equipo = f"https://www.fotmob.com{href}" if href.startswith('/') else href
                    equipos.append({"equipo": nombre, "url": url_equipo})
        
        print(f"    {len(equipos)} equipos")
        return equipos
    finally:
        driver.quit()

def extraer_tablas_fotmob(temporada):
    """
    Extrae las tablas de clasificación de la Liga 1 desde FotMob.

    Para la temporada en curso usa la URL sin parámetro de año (FotMob
    muestra por defecto la temporada activa). Para temporadas históricas
    agrega ?season={temporada} a la URL.

    Extrae múltiples fases si existen: Apertura, Clausura, General, etc.

    Args:
        temporada: año de la temporada (ej: 2024).

    Returns:
        Lista de dicts con posición, equipo, PJ, PG, PE, PP, GF, GC, Pts por fase.
    """
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    
    driver = webdriver.Chrome(options=options)
    
    try:
        if temporada >= datetime.now().year:
            url = "https://www.fotmob.com/es/leagues/131/table/liga-1"
        else:
            url = f"https://www.fotmob.com/es/leagues/131/table/liga-1?season={temporada}"
        
        driver.get(url)
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.CSS_SELECTOR, "body")))
        time.sleep(5)
        
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        todas_tablas = {}
        
        if temporada >= datetime.now().year:
            filas = soup.select('div[class*="TableRowCSS"]')
            if filas:
                datos_tabla = []
                for fila in filas:
                    try:
                        pos_cell = fila.select_one('div[class*="TablePositionCell"]')
                        posicion = int(pos_cell.text.strip()) if pos_cell and pos_cell.text.strip().isdigit() else 0
                        team_cell = fila.select_one('div[class*="TableTeamCell"] span[class*="TeamName"]')
                        equipo = team_cell.text.strip() if team_cell else ""
                        if posicion == 0 or not equipo:
                            continue
                        celdas = fila.select('div[class*="TableCell"]')
                        if len(celdas) >= 7:
                            pj = int(celdas[2].text.strip()) if celdas[2].text.strip().isdigit() else 0
                            g = int(celdas[3].text.strip()) if celdas[3].text.strip().isdigit() else 0
                            e = int(celdas[4].text.strip()) if celdas[4].text.strip().isdigit() else 0
                            p = int(celdas[5].text.strip()) if celdas[5].text.strip().isdigit() else 0
                            goles_text = celdas[6].text.strip() if len(celdas) > 6 else "0-0"
                            pts = int(celdas[7].text.strip()) if len(celdas) > 7 and celdas[7].text.strip().isdigit() else 0
                            gf, gc = 0, 0
                            if '-' in goles_text:
                                partes = goles_text.split('-')
                                gf = int(partes[0]) if partes[0].isdigit() else 0
                                gc = int(partes[1]) if partes[1].isdigit() else 0
                            datos_tabla.append({
                                "posicion": posicion, "equipo": equipo, "partidos_jugados": pj,
                                "partidos_ganados": g, "partidos_empatados": e, "partidos_perdidos": p,
                                "goles_favor": gf, "goles_contra": gc, "diferencia_goles": gf - gc, "puntos": pts
                            })
                    except:
                        continue
                if datos_tabla:
                    todas_tablas["Apertura"] = datos_tabla
                    print(f"    Apertura: {len(datos_tabla)} equipos")
                    return todas_tablas
        
        secciones = soup.select('section[class*="SubTableCSS"], div[class*="TableWrapper"]')
        for seccion in secciones:
            titulo_elem = seccion.select_one('div[class*="TableName"], h2, div[class*="SubTableHeaderCSS"]')
            if not titulo_elem:
                continue
            titulo_raw = titulo_elem.text.strip()
            titulo = ' '.join(titulo_raw.split())
            titulo = re.sub(r'^\d+\s*-\s*', '', titulo)
            titulo = re.sub(r'^\d+\s+', '', titulo)
            
            fases_validas = ['Apertura', 'Clausura', 'Fase 1', 'Fase 2', 'Clausura - Grupo A', 'Clausura - Grupo B']
            if not any(fase in titulo for fase in fases_validas):
                continue
            if titulo in todas_tablas:
                continue
            
            datos = []
            filas = seccion.select('div[class*="TableRowCSS"]')
            for fila in filas:
                try:
                    pos_cell = fila.select_one('div[class*="TablePositionCell"]')
                    if not pos_cell or not pos_cell.text.strip().isdigit():
                        continue
                    posicion = int(pos_cell.text.strip())
                    team_cell = fila.select_one('div[class*="TableTeamCell"] span[class*="TeamName"]')
                    equipo = team_cell.text.strip() if team_cell else ""
                    if posicion == 0 or not equipo:
                        continue
                    celdas = fila.select('div[class*="TableCell"]')
                    if len(celdas) >= 8:
                        pj = int(celdas[2].text.strip()) if len(celdas) > 2 and celdas[2].text.strip().isdigit() else 0
                        g = int(celdas[3].text.strip()) if len(celdas) > 3 and celdas[3].text.strip().isdigit() else 0
                        e = int(celdas[4].text.strip()) if len(celdas) > 4 and celdas[4].text.strip().isdigit() else 0
                        p = int(celdas[5].text.strip()) if len(celdas) > 5 and celdas[5].text.strip().isdigit() else 0
                        goles_text = celdas[6].text.strip() if len(celdas) > 6 else "0-0"
                        pts = 0
                        if len(celdas) > 7 and celdas[7].text.strip().isdigit():
                            pts = int(celdas[7].text.strip())
                        elif len(celdas) > 8 and celdas[8].text.strip().isdigit():
                            pts = int(celdas[8].text.strip())
                        gf, gc = 0, 0
                        if '-' in goles_text:
                            partes = goles_text.split('-')
                            gf = int(partes[0]) if partes[0].isdigit() else 0
                            gc = int(partes[1]) if partes[1].isdigit() else 0
                        if pj > 0 or pts > 0:
                            datos.append({
                                "posicion": posicion, "equipo": equipo, "partidos_jugados": pj,
                                "partidos_ganados": g, "partidos_empatados": e, "partidos_perdidos": p,
                                "goles_favor": gf, "goles_contra": gc, "diferencia_goles": gf - gc, "puntos": pts
                            })
                except:
                    continue
            if datos and len(datos) >= 5:
                todas_tablas[titulo] = datos
        
        if todas_tablas:
            print(f"    {len(todas_tablas)} tablas encontradas")
        else:
            print(f"    No se encontraron tablas")
        return todas_tablas
        
    except Exception as e:
        print(f"    Error: {e}")
        return {}
    finally:
        driver.quit()

def extraer_tabla_wikipedia_2025():
    print(f"    Extrayendo datos de Wikipedia...")
    
    url = "https://es.wikipedia.org/wiki/Liga_1_2025_(Per%C3%BA)"
    todas_tablas = {}
    
    try:
        response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
        if response.status_code != 200:
            print(f"    Error HTTP {response.status_code}")
            return {}
        
        soup = BeautifulSoup(response.text, 'html.parser')
        tablas_clasificacion = []
        
        for tabla in soup.find_all('table'):
            filas = tabla.find_all('tr')
            if len(filas) < 5:
                continue
            cabeceras = filas[0].find_all(['th', 'td'])
            texto = ' '.join([c.get_text().strip().lower() for c in cabeceras[:4]])
            if 'pos.' in texto and 'equipo' in texto and 'pts.' in texto:
                tablas_clasificacion.append(tabla)
        
        for i, tabla in enumerate(tablas_clasificacion[:2]):
            titulo = "Apertura" if i == 0 else "Clausura"
            datos = []
            filas = tabla.find_all('tr')
            for fila in filas[1:]:
                celdas = fila.find_all(['th', 'td'])
                if len(celdas) >= 9:
                    try:
                        pos_text = re.sub(r'[^0-9]', '', celdas[0].get_text().strip())
                        posicion = int(pos_text) if pos_text.isdigit() else 0
                        if posicion == 0:
                            continue
                        equipo = celdas[1].get_text().strip()
                        equipo = re.sub(r'\s*\([^)]*\)\s*', '', equipo).strip()
                        puntos = int(celdas[2].get_text().strip()) if celdas[2].get_text().strip().isdigit() else 0
                        partidos = int(celdas[3].get_text().strip()) if celdas[3].get_text().strip().isdigit() else 0
                        ganados = int(celdas[4].get_text().strip()) if celdas[4].get_text().strip().isdigit() else 0
                        empatados = int(celdas[5].get_text().strip()) if celdas[5].get_text().strip().isdigit() else 0
                        perdidos = int(celdas[6].get_text().strip()) if celdas[6].get_text().strip().isdigit() else 0
                        gf = int(celdas[7].get_text().strip()) if celdas[7].get_text().strip().isdigit() else 0
                        gc = int(celdas[8].get_text().strip()) if celdas[8].get_text().strip().isdigit() else 0
                        datos.append({
                            "posicion": posicion, "equipo": equipo, "partidos_jugados": partidos,
                            "partidos_ganados": ganados, "partidos_empatados": empatados, "partidos_perdidos": perdidos,
                            "goles_favor": gf, "goles_contra": gc, "diferencia_goles": gf - gc, "puntos": puntos
                        })
                    except:
                        continue
            if datos:
                todas_tablas[titulo] = datos
        
        print(f"    {len(todas_tablas)} tablas encontradas")
        return todas_tablas
    except Exception as e:
        print(f"    Error: {e}")
        return {}

def obtener_tabla_clasificacion(temporada):
    print(f"\n 4. TABLA CLASIFICACIÓN {temporada}...")
    if temporada == 2025:
        print(f"    Usando Wikipedia")
        return extraer_tabla_wikipedia_2025()
    return extraer_tablas_fotmob(temporada)

def extraer_estadisticas_partido_con_reintento(url_partido, max_reintentos=2):
    """
    Extrae las estadísticas detalladas de un partido desde FotMob con reintentos.

    Navega a la pestaña de estadísticas del partido (shots, posesión, pases, etc.).
    FotMob carga el contenido dinámicamente, por lo que se espera a que aparezca
    el elemento de estadísticas antes de parsear.

    Args:
        url_partido: URL del partido en FotMob (puede incluir :tab=stats o #statistics).
        max_reintentos: número máximo de intentos ante fallo de carga (default 2).

    Returns:
        Dict con todas las métricas del partido, o dict vacío si falla tras reintentos.
    """
    for intento in range(max_reintentos):
        driver = None
        try:
            options = Options()
            options.add_argument("--headless=new")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")
            
            driver = webdriver.Chrome(options=options)
            
            # Formato correcto FotMob: #{matchId}:tab=stats (al final, no antes del #)
            if ':tab=stats' in url_partido:
                url_stats = url_partido
            elif '#statistics' in url_partido or '#stats' in url_partido:
                url_stats = url_partido
            elif '/matches/' in url_partido:
                url_stats = url_partido + ':tab=stats'
            else:
                url_stats = url_partido
            
            driver.get(url_stats)
            time.sleep(2)
            
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            stats_container = soup.select_one('div[class*="TopStatsContainer"]')
            
            if not stats_container:
                if intento < max_reintentos - 1:
                    time.sleep(1)
                    continue
                return None
            
            time.sleep(1)
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            
            all_stats_containers = soup.select('div[class*="TopStatsContainer"], div[class*="middle"], div[class*="last"]')
            if not all_stats_containers:
                all_stats_containers = soup.find_all('div', class_=re.compile(r'Stat|Stats'))
            
            estadisticas = {}
            
            for container in all_stats_containers:
                for item in container.select('li[class*="Stat"]'):
                    title_elem = item.select_one('span[class*="StatTitle"]')
                    if not title_elem:
                        continue
                    title = title_elem.text.strip()
                    boxes = item.select('div[class*="StatBox"]')
                    if len(boxes) >= 2:
                        local_val = ' '.join(boxes[0].text.strip().split())
                        visitante_val = ' '.join(boxes[1].text.strip().split())
                        estadisticas[f"{title}_local"] = local_val
                        estadisticas[f"{title}_visitante"] = visitante_val
            
            possession_bar = soup.select_one('div[class*="PossessionDiv"]')
            if possession_bar:
                segments = possession_bar.select('div[class*="PossessionSegment"]')
                if len(segments) >= 2:
                    local_pos = segments[0].text.strip().replace('%', '').strip()
                    visitante_pos = segments[1].text.strip().replace('%', '').strip()
                    estadisticas["Posesión_local"] = f"{local_pos}%"
                    estadisticas["Posesión_visitante"] = f"{visitante_pos}%"
            
            if estadisticas:
                return estadisticas
            
            if intento < max_reintentos - 1:
                time.sleep(1)
                
        except Exception as e:
            if intento < max_reintentos - 1:
                time.sleep(2)
            continue
        finally:
            if driver:
                driver.quit()
    
    return None

def extraer_estadisticas_por_año(temporada, partidos):
    print(f"\n 5. ESTADÍSTICAS {temporada}...")
    
    partidos_con_url = [p for p in partidos if p.get('url')]
    total = len(partidos_con_url)
    
    if total == 0:
        print(f"    No hay partidos con URL")
        return []
    
    print(f"    {total} partidos")
    print(f"    Usando {MAX_WORKERS_FOTMOB} hilos en paralelo...")
    
    resultados = [None] * total
    con_estadisticas = 0
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS_FOTMOB) as executor:
        future_to_index = {}
        for idx, partido in enumerate(partidos_con_url):
            future = executor.submit(extraer_estadisticas_partido_con_reintento, partido['url'])
            future_to_index[future] = idx
        
        completados = 0
        for future in as_completed(future_to_index):
            idx = future_to_index[future]
            partido = partidos_con_url[idx]
            
            stats = future.result()
            
            partido_data = {
                "fecha": partido.get('fecha'),
                "local": partido.get('local'),
                "visitante": partido.get('visitante'),
                "marcador": partido.get('marcador'),
                "url": partido.get('url')
            }
            
            if stats:
                con_estadisticas += 1
                partido_data.update(stats)
            
            resultados[idx] = partido_data
            completados += 1
            
            if completados % 50 == 0 or completados == total:
                print(f"\r    Progreso: {completados}/{total} - {con_estadisticas} con estadísticas", end="", flush=True)
        
        print()
    
    print(f"    {con_estadisticas}/{total} con estadísticas")
    return resultados

# =============================================================================
# 2. FUNCIONES TRANSFERMARKT (COMPLETAS) - MEJORADAS PARA ESTADIOS
# =============================================================================

def manejar_banner(driver):
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
    try:
        selectores_temporada = ['//select[@name="saison_id"]', '//select[contains(@class, "saison")]']
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

        # Mapa de valores de pie en inglés (fallback si TM devuelve en inglés)
        _PIE_EN_ES = {
            'right foot': 'Derecho', 'right': 'Derecho',
            'left foot': 'Izquierdo', 'left': 'Izquierdo',
            'both': 'Ambidiestro', 'both feet': 'Ambidiestro'
        }

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

                    # Capturar id_tm y slug_tm desde el href del jugador
                    id_tm = ""
                    slug_tm = ""
                    try:
                        link_jugador = celdas[1].find_element(By.XPATH, './/a[contains(@href,"/spieler/")]')
                        href_jugador = link_jugador.get_attribute('href')
                        match_tm = re.search(r'/([^/]+)/profil/spieler/(\d+)', href_jugador)
                        if match_tm:
                            slug_tm = match_tm.group(1)
                            id_tm = match_tm.group(2)
                    except Exception:
                        pass

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
                        if len(celdas) > idx:
                            val = celdas[idx].text.strip()
                            if val in ['Derecho', 'Izquierdo', 'Ambidiestro']:
                                pie = val
                                break
                            elif val.lower() in _PIE_EN_ES:
                                pie = _PIE_EN_ES[val.lower()]
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

                    # Extraer verein ID del club desde url_plantilla (usado en filtro de stats)
                    m_verein_plt = re.search(r'/verein/(\d+)', url_plantilla)
                    verein_id_plt = m_verein_plt.group(1) if m_verein_plt else ""

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
                        "Valor Mercado (€)": valor_mercado,
                        "id_tm": id_tm,
                        "slug_tm": slug_tm,
                        "verein_id_tm": verein_id_plt,
                    })
                    jugadores_procesados += 1
            except Exception:
                continue
        
        return datos_plantillas, jugadores_procesados
    except Exception as e:
        return [], 0

def extraer_entrenadores_corregido(driver, club, url_equipo):
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
                                    "Club": club, "Entrenador": nombre, "Fecha Nacimiento": fecha_nacimiento,
                                    "Nacionalidad": nacionalidad, "Comienzo": comienzo, "Salida": salida,
                                    "Tiempo en cargo": tiempo_cargo, "Partidos": partidos, "PPP": ppp
                                })
                        except:
                            salida = celdas[6].text.strip() if len(celdas) > 6 else ""
                            tiempo_cargo = celdas[7].text.strip() if len(celdas) > 7 else ""
                            partidos = celdas[8].text.strip() if len(celdas) > 8 else ""
                            ppp = celdas[9].text.strip() if len(celdas) > 9 else ""
                            datos_entrenadores.append({
                                "Club": club, "Entrenador": nombre, "Fecha Nacimiento": fecha_nacimiento,
                                "Nacionalidad": nacionalidad, "Comienzo": comienzo, "Salida": salida,
                                "Tiempo en cargo": tiempo_cargo, "Partidos": partidos, "PPP": ppp
                            })
            except Exception:
                continue
        return datos_entrenadores
    except Exception as e:
        return []

# =============================================================================
# FUNCIÓN MEJORADA PARA EXTRAER ESTADIO (VERSIÓN QUE FUNCIONA)
# =============================================================================
def extraer_info_estadio_corregido(driver, club, url_equipo):
    try:
        driver.get(url_equipo)
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'data-header__info-box')]"))
        )
        time.sleep(2)

        nombre_estadio = "No disponible"
        capacidad = "No disponible"
        aforo = "No disponible"

        # Método principal - busca el li que contiene "Estadio:" y extrae el contenido
        try:
            contenedor = driver.find_element(By.XPATH, "//li[contains(., 'Estadio:') or contains(., 'Stadium:')]//span[contains(@class, 'data-header__content')]")
            texto = contenedor.text
            try:
                enlace = contenedor.find_element(By.TAG_NAME, "a")
                nombre_estadio = enlace.text.strip()
            except:
                pass
            match = re.search(r'([\d.,]+)\s*(?:Seats|Aforo|aforo|plazas)', texto)
            if match:
                capacidad = match.group(1).replace('.', '').replace(',', '')
                aforo = capacidad
        except Exception as e:
            # Fallback: método original (por si cambia la estructura)
            try:
                elemento = driver.find_element(By.XPATH, '//*[contains(text(), "Estadio:")]')
                texto_completo = elemento.text.strip()
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
                # Último fallback: tabla profilheader
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
                                numeros = re.findall(r'[\d.,]+', valor)
                                if numeros:
                                    capacidad = numeros[-1].replace(',', '.')
                                    aforo = capacidad
                                    nombre_estadio = re.split(r'[\d.,]', valor)[0].strip()
                                break
                except:
                    pass

        return [{"Club": club, "Estadio": nombre_estadio, "Capacidad": capacidad, "Aforo": aforo}]
    except Exception as e:
        log_error(f"Error extrayendo estadio para {club}", e)
        return [{"Club": club, "Estadio": "No disponible", "Capacidad": "No disponible", "Aforo": "No disponible"}]

# =============================================================================
# VERSIÓN MEJORADA DE extraer_estadio_con_reintentos (con más reintentos y refresh)
# =============================================================================
def extraer_estadio_con_reintentos(driver, club, url_equipo, max_reintentos=3):
    for intento in range(max_reintentos + 1):
        try:
            estadio = extraer_info_estadio_corregido(driver, club, url_equipo)
            if estadio and estadio[0]['Estadio'] != "No disponible":
                return estadio
            elif intento < max_reintentos:
                log_info(f"Reintentando ({intento+1}/{max_reintentos}) para {club}")
                time.sleep(3)
                driver.refresh()
                time.sleep(2)
        except Exception as e:
            if intento < max_reintentos:
                time.sleep(3)
    return [{"Club": club, "Estadio": "No disponible", "Capacidad": "No disponible", "Aforo": "No disponible"}]

def _extraer_plantillas_requests(club, url_equipo, año_real):
    """
    Extrae plantilla de un club desde TM usando requests + BeautifulSoup.
    Evita Cloudflare headless al no usar Selenium para páginas estáticas.
    Retorna (lista_de_dicts, n_jugadores).
    """
    _HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "es-PE,es;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Referer": "https://www.transfermarkt.pe/",
    }
    _PIE_EN_ES = {
        'right foot': 'Derecho', 'right': 'Derecho',
        'left foot': 'Izquierdo', 'left': 'Izquierdo',
        'both': 'Ambidiestro', 'both feet': 'Ambidiestro',
    }

    año_tm = año_real - 1  # saison TM (mismo criterio que Selenium)

    # Construir URL del kader con saison_id explícito y vista Ampliado (/plus/1)
    url_kader = re.sub(r'/startseite/', '/kader/', url_equipo.split('?')[0].rstrip('/'))
    if '/saison_id/' not in url_kader:
        url_kader = f"{url_kader}/saison_id/{año_tm}"
    if '/plus/1' not in url_kader:
        url_kader = f"{url_kader}/plus/1"

    try:
        session = requests.Session()
        session.headers.update(_HEADERS)
        resp = session.get(url_kader, timeout=30)

        if resp.status_code != 200:
            log_error(f"[REPAIR_TM][requests] {club}: HTTP {resp.status_code}")
            return [], 0
        if "Just a moment" in resp.text or "cf-browser-verification" in resp.text:
            log_error(f"[REPAIR_TM][requests] {club}: Cloudflare challenge")
            return [], 0

        soup = BeautifulSoup(resp.text, "html.parser")
        tabla = soup.select_one("table.items tbody")
        if not tabla:
            log_error(f"[REPAIR_TM][requests] {club}: sin tabla.items en {url_kader}")
            return [], 0

        datos = []
        for fila in tabla.find_all("tr", class_=["odd", "even"]):
            try:
                celdas = fila.find_all("td", recursive=False)
                if len(celdas) < 5:
                    continue

                # Número de camiseta
                numero = celdas[0].get_text(strip=True) or "Sin Número"

                # Nombre, posición, id_tm, slug_tm, foto_url
                celda_nombre = celdas[1]
                link_j = celda_nombre.find("a", href=re.compile(r"/profil/spieler/\d+"))
                if not link_j:
                    continue
                nombre = link_j.get_text(strip=True)
                href_j = link_j.get("href", "")
                m = re.search(r"/([^/]+)/profil/spieler/(\d+)", href_j)
                slug_tm = m.group(1) if m else ""
                id_tm   = m.group(2) if m else ""

                # Posición (segunda fila del inline-table)
                posicion = ""
                rows_inline = celda_nombre.select("table.inline-table tr")
                if len(rows_inline) > 1:
                    posicion = rows_inline[1].get_text(strip=True)

                # Foto (data-src por lazy-loading de TM)
                foto_url = ""
                img_f = celda_nombre.find("img", class_="bilderrahmen-fixed")
                if img_f:
                    foto_url = img_f.get("data-src") or img_f.get("src", "")
                if not foto_url and id_tm:
                    foto_url = f"https://img.a.transfermarkt.technology/portrait/header/{id_tm}-0.jpg"

                # Nacionalidad (títulos de banderas)
                flags = fila.find_all("img", class_=re.compile(r"flaggen"))
                nacionalidad = ", ".join(
                    img.get("title", "") for img in flags if img.get("title")
                )[:50]

                # Fecha nacimiento (primer texto con formato dd/mm/yyyy)
                fecha_nacimiento = ""
                for td in celdas[2:7]:
                    txt = td.get_text(strip=True)
                    if re.search(r"\d{2}/\d{2}/\d{4}", txt):
                        fecha_nacimiento = txt
                        break

                # Altura (xm donde x es dígito)
                altura = ""
                for td in celdas:
                    txt = td.get_text(strip=True)
                    if re.match(r"\d,\d{2}m", txt):
                        altura = txt
                        break

                # Pie dominante
                pie = ""
                for td in celdas:
                    txt = td.get_text(strip=True)
                    if txt in ("Derecho", "Izquierdo", "Ambidiestro"):
                        pie = txt
                        break
                    elif txt.lower() in _PIE_EN_ES:
                        pie = _PIE_EN_ES[txt.lower()]
                        break

                # Fecha fichaje (segunda fecha encontrada)
                fecha_fichaje = ""
                fechas_vistas = 0
                for td in celdas[2:]:
                    txt = td.get_text(strip=True)
                    if re.search(r"\d{2}/\d{2}/\d{4}", txt):
                        fechas_vistas += 1
                        if fechas_vistas == 2:
                            fecha_fichaje = txt
                            break

                # Valor de mercado (td.rechts)
                valor_mercado = ""
                td_r = fila.find("td", class_="rechts")
                if td_r:
                    valor_mercado = td_r.get_text(strip=True)

                # Extraer verein ID del club desde url_kader (usado en filtro de stats)
                m_verein_kader = re.search(r'/verein/(\d+)', url_kader)
                verein_id_kader = m_verein_kader.group(1) if m_verein_kader else ""

                datos.append({
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
                    "Valor Mercado (€)": valor_mercado,
                    "id_tm": id_tm,
                    "slug_tm": slug_tm,
                    "foto_url": foto_url,
                    "verein_id_tm": verein_id_kader,
                })
            except Exception:
                continue

        # Dedup por id_tm: la vista /plus/1 devuelve 2 filas por jugador
        # (una por Apertura y otra por Clausura). Conservar la que tenga más campos completos.
        deduped = {}
        for d in datos:
            key = d.get("id_tm") or d.get("Jugador", "")
            if key not in deduped:
                deduped[key] = d
            else:
                # Contar campos no vacíos: preferir la fila con más data
                def _score(row):
                    return sum(1 for v in [row.get("Altura",""), row.get("Pie",""), row.get("Fecha Nacimiento","")] if v.strip())
                if _score(d) > _score(deduped[key]):
                    deduped[key] = d
        datos = list(deduped.values())

        return datos, len(datos)

    except Exception as e:
        log_error(f"[REPAIR_TM][requests] {club}: {type(e).__name__} — {str(e)[:120]}")
        return [], 0


def extraer_plantillas_con_reintentos(driver, club, url_equipo, año_real, max_reintentos=2):
    # --- Intento 1: requests (rápido, sin Cloudflare headless) ---
    try:
        plantillas, n = _extraer_plantillas_requests(club, url_equipo, año_real)
        if n > 0:
            log_info(f"[REPAIR_TM][requests] {club}: {n} jugadores obtenidos")
            return plantillas, n
    except Exception as e:
        log_error(f"[REPAIR_TM][requests] {club} excepción: {e}")

    # --- Intento 2: Selenium como fallback ---
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

def extraer_perfil_y_stats_jugador(session, driver, id_tm, slug_tm, año_tm):
    """
    Combina dos fuentes para cada jugador:
    1. foto_url: requests → perfil del jugador (portrait/header, accesible externamente)
    2. estadísticas: Selenium → leistungsdatendetails /wettbewerb/0/liga/1
       La página usa Svelte (JS-rendered), requiere Selenium con WebDriverWait.
       La tabla es div[role="table"] con div[role="row"], NO table.items.

    Estructura Svelte de cada fila (selector [class*="tm-grid__cell"]):
      [0] Temporada | [1] Competición | [2] Partidos | [3] stat1 | [4] stat2
      [5] Amarillas | [6] Am-Roja | [7] Rojas | [8] Minutos
      (el div del logo club tiene clase "grid__cell ac", excluido del selector)

    Returns:
        (foto_url: str, estadisticas: list de dicts por competencia)
    """
    def parse_stat(text):
        t = text.strip().replace("'", "").replace(".", "").replace(",", "").replace("\xa0", "")
        if not t or t == '-':
            return 0
        try:
            return int(t)
        except ValueError:
            return 0

    def parse_float(text):
        t = text.strip().replace("'", "").replace("\xa0", "").replace(",", ".")
        if not t or t == '-':
            return 0.0
        try:
            return float(t)
        except ValueError:
            return 0.0

    foto_url = ""
    estadisticas = []

    _CURL_UA = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                '(KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36')
    _CURL_HDRS = {
        'User-Agent': _CURL_UA,
        'Accept-Language': 'es-PE,es;q=0.9,en;q=0.8',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Referer': 'https://www.transfermarkt.pe/',
    }

    def _es_cf_block(html_lower):
        return ('just a moment' in html_lower
                or 'checking your browser' in html_lower
                or 'cf-browser-verification' in html_lower
                or ('challenge' in html_lower and 'cloudflare' in html_lower))

    def _parse_rows(soup, id_tm, foto_url):
        rows = soup.select('div[role="rowgroup"] div[role="row"]')
        result = []
        for row in rows:
            celdas = row.select('[class*="tm-grid__cell"]')
            if len(celdas) < 3:
                continue
            competencia = celdas[1].get_text(strip=True) if len(celdas) > 1 else ""
            if not competencia or competencia.isdigit() or "/" in competencia:
                continue
            club_tm = ""
            a_verein = row.select_one('a[href*="/verein/"]')
            if a_verein:
                m_v = re.search(r"/verein/(\d+)", a_verein.get("href", ""))
                if m_v:
                    club_tm = m_v.group(1)
            n = len(celdas)

            # n == 15 → portero: grilla TM sin columna penaltis_anotados,
            #           con goles_en_contra [11] y partidos_imbatido [12].
            #           Minutos_Jugados están en [14] (no en [15]).
            # n == 16 → jugador de campo: penaltis_anotados en [13].
            es_portero = (n == 15)

            if es_portero:
                # Layout GK (n=15):
                # [2]=partidos [3]=titular [4]=ppp [5-7]=goles/asis/otro(-)
                # [8]=amarilla [9]=2ªamarilla [10]=roja
                # [11]=entrada_suplente(siempre 0 en GK)
                # [12]=goles_en_contra [13]=partidos_imbatido [14]=minutos_jugados
                result.append({
                    "id_tm": id_tm, "Jugador": "", "Club": "", "ClubTM": club_tm,
                    "Año": "", "foto_url": foto_url, "Competencia": competencia,
                    "Partidos_Jugados":      parse_stat(celdas[2].text)  if n > 2  else 0,
                    "Titularidades":         parse_stat(celdas[3].text)  if n > 3  else 0,
                    "PPP":                   parse_float(celdas[4].text) if n > 4  else 0.0,
                    "Goles":                 0,
                    "Asistencias":           0,
                    "Tarjetas_Amarillas":    parse_stat(celdas[8].text)  if n > 8  else 0,
                    "Tarjeta_Amarilla_Roja": parse_stat(celdas[9].text)  if n > 9  else 0,
                    "Tarjetas_Rojas":        parse_stat(celdas[10].text) if n > 10 else 0,
                    "Entrada_Suplente":      0,
                    "Salida_Suplente":       0,
                    "Penaltis_Anotados":     0,
                    "Goles_En_Contra":       parse_stat(celdas[12].text) if n > 12 else 0,
                    "Partidos_Imbatido":     parse_stat(celdas[13].text) if n > 13 else 0,
                    "Min_Por_Gol":           0,
                    "Minutos_Jugados":       parse_stat(celdas[14].text) if n > 14 else 0,
                    "datos_disponibles": True
                })
            else:
                result.append({
                    "id_tm": id_tm, "Jugador": "", "Club": "", "ClubTM": club_tm,
                    "Año": "", "foto_url": foto_url, "Competencia": competencia,
                    "Partidos_Jugados":      parse_stat(celdas[2].text)  if n > 2  else 0,
                    "Titularidades":         parse_stat(celdas[3].text)  if n > 3  else 0,
                    "PPP":                   parse_float(celdas[4].text) if n > 4  else 0.0,
                    "Goles":                 parse_stat(celdas[5].text)  if n > 5  else 0,
                    "Asistencias":           parse_stat(celdas[6].text)  if n > 6  else 0,
                    "Tarjetas_Amarillas":    parse_stat(celdas[8].text)  if n > 8  else 0,
                    "Tarjeta_Amarilla_Roja": parse_stat(celdas[9].text)  if n > 9  else 0,
                    "Tarjetas_Rojas":        parse_stat(celdas[10].text) if n > 10 else 0,
                    "Entrada_Suplente":      parse_stat(celdas[11].text) if n > 11 else 0,
                    "Salida_Suplente":       parse_stat(celdas[12].text) if n > 12 else 0,
                    "Penaltis_Anotados":     parse_stat(celdas[13].text) if n > 13 else 0,
                    "Goles_En_Contra":       0,
                    "Partidos_Imbatido":     0,
                    "Min_Por_Gol":           parse_stat(celdas[14].text) if n > 14 else 0,
                    "Minutos_Jugados":       parse_stat(celdas[15].text) if n > 15 else 0,
                    "datos_disponibles": True
                })
        return result

    url_plus = (f"https://www.transfermarkt.pe/{slug_tm}/leistungsdatendetails"
                f"/spieler/{id_tm}/saison/{año_tm}/wettbewerb/0/liga/1/plus/1")

    # 1. Perfil → foto_url (curl_cffi primero, fallback a requests)
    try:
        url_perfil = f"https://www.transfermarkt.pe/{slug_tm}/profil/spieler/{id_tm}"
        if _CURL_CFFI_DISPONIBLE:
            resp_p = _curl_requests.get(url_perfil, headers=_CURL_HDRS,
                                        impersonate="chrome110", timeout=15)
        else:
            resp_p = session.get(url_perfil, timeout=15)
        if resp_p.status_code == 200:
            sp = BeautifulSoup(resp_p.text, "html.parser")
            img = sp.select_one("img.data-header__profile-image")
            if img:
                foto_url = img.get("src", "")
    except Exception:
        pass

    # 2. Stats — Capa 1: curl_cffi (sin browser, impersona TLS de Chrome)
    #           Capa 2: Selenium + undetected_chromedriver (pasa JS challenge)
    _curl_ok = False
    if _CURL_CFFI_DISPONIBLE:
        try:
            resp_c = _curl_requests.get(url_plus, headers=_CURL_HDRS,
                                        impersonate="chrome110", timeout=20)
            if resp_c.status_code == 200 and not _es_cf_block(resp_c.text.lower()):
                rows_c = _parse_rows(BeautifulSoup(resp_c.text, "html.parser"),
                                     id_tm, foto_url)
                if rows_c:
                    estadisticas.extend(rows_c)
                    _curl_ok = True
        except Exception:
            pass

    if not _curl_ok:
        try:
            driver.get(url_plus)
            try:
                WebDriverWait(driver, 30).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div[role=\'rowgroup\']"))
                )
            except Exception:
                time.sleep(5)
            page_src = driver.page_source
            if _es_cf_block(page_src.lower()):
                raise RuntimeError("CLOUDFLARE_BLOCKED")
            estadisticas.extend(
                _parse_rows(BeautifulSoup(page_src, "html.parser"), id_tm, foto_url))
        except RuntimeError:
            raise
        except Exception:
            pass

    return foto_url, estadisticas


def extraer_estadisticas_por_club(driver, plantillas_club, año_tm, _stats_cache=None):
    """
    Usa requests (no Selenium) para extraer foto_url y estadísticas de cada jugador.

    - foto_url: obtenida del perfil público (portrait/header, accesible desde Power BI)
    - stats: de leistungsdatendetails con filtro /wettbewerb/0/liga/1 (primera division peruana)

    Actualiza foto_url directamente en cada dict de plantillas_club, lo que se refleja
    automáticamente en datos_plantillas por compartir referencias de objeto Python.

    Args:
        driver: instancia Selenium activa.
        plantillas_club: lista de dicts de jugadores con id_tm y slug_tm.
        año_tm: año TM (ej: 2019 para temporada 2019/20).
        _stats_cache: dict compartido {id_tm: (foto_url, stats)} para evitar
                      re-scrapear jugadores que aparecen en múltiples clubs
                      (ej: transferidos en mitad de temporada). Pasar el MISMO
                      dict en todas las llamadas del año.
    """
    if _stats_cache is None:
        _stats_cache = {}

    # Sesión HTTP compartida con headers de browser real — menos detectable que Selenium
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36',
        'Accept-Language': 'es-PE,es;q=0.9,en;q=0.8',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Referer': 'https://www.transfermarkt.pe/',
        'Connection': 'keep-alive',
    })

    datos_estadisticas = []
    procesados = 0
    sin_id = 0

    for jugador in plantillas_club:
        id_tm = jugador.get("id_tm", "")
        slug_tm = jugador.get("slug_tm", "")

        if not id_tm or not slug_tm:
            sin_id += 1
            jugador["foto_url"] = ""
            continue

        try:
            # Usar cache para evitar re-scrapear jugadores que aparecen en varios clubs
            if id_tm in _stats_cache:
                foto_url, stats = _stats_cache[id_tm]
                log_info(f"      [cache] {jugador.get('Jugador','?')} (id_tm={id_tm}) — reutilizando stats")
            else:
                foto_url, stats = extraer_perfil_y_stats_jugador(session, driver, id_tm, slug_tm, año_tm)
                _stats_cache[id_tm] = (foto_url, stats)
        except RuntimeError as e:
            if "CLOUDFLARE_BLOCKED" in str(e):
                log_error(f"[CLOUDFLARE] Bloqueo detectado en jugador {id_tm}. Abortando — espera 4-6h y reintenta.")
                raise  # propaga hacia arriba para abortar repair_tm completo
            jugador["foto_url"] = ""
            continue
        except Exception:
            jugador["foto_url"] = ""
            continue

        # Propagar foto_url al dict del jugador → se refleja en datos_plantillas
        jugador["foto_url"] = foto_url

        if stats:
            # ClubTM contiene el verein ID (ej: "2734") extraído del href de Titularidades
            # verein_id_tm viene del kader scraping (url_kader contiene /verein/{id}/)
            verein_id_jugador = jugador.get("verein_id_tm", "").strip()
            stats_con_club = [s for s in stats if s.get("ClubTM", "").strip()]
            if stats_con_club:
                if verein_id_jugador:
                    # Filtrar por verein ID (Ampliado view)
                    stats_filtradas = [
                        s for s in stats
                        if s.get("ClubTM", "").strip() == verein_id_jugador
                    ]
                    stats_a_extender = stats_filtradas if stats_filtradas else stats
                else:
                    # Sin verein_id (fallback ADLS): incluir todas las filas
                    stats_a_extender = stats
            else:
                # ClubTM vacío → extracción falló, conservar todo
                stats_a_extender = stats
            # Copiar cada dict antes de mutar: los objetos originales quedan en
            # _stats_cache sin modificar, así el siguiente club que los lea desde
            # cache obtiene dicts limpios sin Club/Jugador sobreescritos.
            stats_copia = [dict(s) for s in stats_a_extender]
            for stat in stats_copia:
                stat["Jugador"] = jugador.get("Jugador", "")
                stat["Club"] = jugador.get("Club", "")
                stat["Año"] = jugador.get("Año", "")
            datos_estadisticas.extend(stats_copia)
        else:
            datos_estadisticas.append({
                "id_tm": id_tm,
                "Jugador": jugador.get("Jugador", ""),
                "Club": jugador.get("Club", ""),
                "Año": jugador.get("Año", ""),
                "foto_url": foto_url,
                "Competencia": "",
                "Partidos_Jugados": 0, "Titularidades": 0, "PPP": 0.0,
                "Goles": 0, "Asistencias": 0,
                "Tarjetas_Amarillas": 0, "Tarjeta_Amarilla_Roja": 0,
                "Tarjetas_Rojas": 0,
                "Entrada_Suplente": 0, "Salida_Suplente": 0,
                "Penaltis_Anotados": 0, "Goles_En_Contra": 0, "Partidos_Imbatido": 0,
                "Min_Por_Gol": 0, "Minutos_Jugados": 0,
                "datos_disponibles": False
            })

        procesados += 1
        time.sleep(1.5)  # 2 requests/jugador × 568 × 1.5s ≈ 28 min/temporada

    log_info(f"    Stats (requests): {procesados} jugadores, {sin_id} sin id_tm")
    return datos_estadisticas


def extraer_entrenadores_con_reintentos(driver, club, url_equipo, max_reintentos=2):
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

def scraping_transfermarkt_por_año(año_usuario):
    """
    Extrae datos de plantillas, valoraciones, estadios y entrenadores desde Transfermarkt.

    IMPORTANTE — desfase de calendarios entre fuentes:
    Transfermarkt nombra sus temporadas como "año_anterior/año_actual" (ej: 2023/24).
    Para obtener la data del año X de entrada, se busca la temporada X-1 en Transfermarkt.
    Ejemplo: año_usuario=2024 → busca temporada "2023" en Transfermarkt (= 2023/24).

    Los archivos se guardan con año_usuario como referencia para alinear con FotMob.

    Args:
        año_usuario: año de la temporada deseada según el calendario de FotMob (ej: 2024).

    Returns:
        Dict con DataFrames/listas de: plantillas, valoraciones, estadios, entrenadores.
    """
    año_guardado = año_usuario + 1
    año_transfermarkt = año_usuario
    
    try:
        options = webdriver.ChromeOptions()
        options.add_argument("--headless=new")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--lang=es-PE,es")

        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        driver.execute_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
            Object.defineProperty(navigator, 'languages', {get: () => ['es-PE', 'es', 'en-US', 'en']});
            window.chrome = {runtime: {}};
        """)

        url_liga = f"https://www.transfermarkt.pe/liga-1-apertura/startseite/wettbewerb/TDeA/plus/?saison_id={año_transfermarkt}"
        driver.get(url_liga)
        time.sleep(12)
        manejar_banner(driver)

        tabla_cargada = False
        for intento in range(2):
            try:
                WebDriverWait(driver, 45).until(
                    EC.presence_of_element_located((By.XPATH, '//table[contains(@class,"items")]//td[2]//a'))
                )
                time.sleep(2)
                tabla_cargada = True
                break
            except:
                if intento == 0:
                    try:
                        page_src = driver.page_source
                        if "Just a moment" in page_src or "checking your browser" in page_src.lower():
                            log_info(f"Cloudflare challenge detectado para año {año_transfermarkt}, reintentando...")
                            driver.refresh()
                            time.sleep(20)
                    except:
                        pass

        if not tabla_cargada:
            try:
                page_src = driver.page_source[:500]
                log_error(f"TM {año_transfermarkt}: tabla NO cargada tras 2 intentos. URL={url_liga}")
                log_error(f"TM {año_transfermarkt}: page_source inicio → {page_src}")
            except Exception as _e:
                log_error(f"TM {año_transfermarkt}: tabla NO cargada y no se pudo leer page_source: {_e}")
            driver.quit()
            return [], [], [], [], []

        filas = driver.find_elements(By.XPATH, '//div[@id="yw1"]//table[contains(@class,"items")]/tbody/tr')
        datos_generales = []
        urls_equipos = []
        
        # Bucle mejorado: filtra SOLO clubes (con escudo y URL que contenga "/verein/")
        for fila in filas:
            try:
                columnas = fila.find_elements(By.XPATH, './/td')
                if len(columnas) >= 7:
                    # Verificar que la fila tenga imagen de escudo (tiny_wappen) -> es club, no jugador
                    tiene_escudo = fila.find_elements(By.XPATH, './/img[contains(@class, "tiny_wappen")]')
                    if not tiene_escudo:
                        continue
                    club = columnas[1].text.strip()
                    if club and len(club) > 2:
                        link_equipo = columnas[1].find_element(By.XPATH, './/a').get_attribute('href')
                        # Asegurar que la URL sea de un club (contiene "/verein/")
                        if "/verein/" in link_equipo:
                            urls_equipos.append((club, link_equipo))
                            datos_generales.append({
                                "Año": año_guardado, "Club": club,
                                "Jugadores en plantilla": columnas[2].text.strip(),
                                "Edad promedio": columnas[3].text.strip(),
                                "Extranjeros": columnas[4].text.strip(),
                                "Valor medio (€)": columnas[5].text.strip(),
                                "Valor total (€)": columnas[6].text.strip()
                            })
            except Exception as e:
                continue

        datos_plantillas = []
        datos_estadios = []
        datos_entrenadores = []
        datos_estadisticas = []

        log_info(f" Procesando {len(urls_equipos)} equipos de Transfermarkt...")

        stats_cache = {}  # cache compartido: evita re-scrapear jugadores transferidos entre clubs
        for i, (club, url_equipo) in enumerate(urls_equipos):
            try:
                plantillas, jugadores_procesados = extraer_plantillas_con_reintentos(driver, club, url_equipo, año_guardado, max_reintentos=1)
                datos_plantillas.extend(plantillas)
                estadio = extraer_estadio_con_reintentos(driver, club, url_equipo, max_reintentos=1)
                datos_estadios.extend(estadio)
                entrenadores = extraer_entrenadores_con_reintentos(driver, club, url_equipo, max_reintentos=1)
                datos_entrenadores.extend(entrenadores)

                # Estadísticas por jugador — usa los id_tm capturados en plantillas
                if plantillas:
                    log_info(f"    Extrayendo stats de {len(plantillas)} jugadores de {club}...")
                    stats_club = extraer_estadisticas_por_club(driver, plantillas, año_transfermarkt, _stats_cache=stats_cache)
                    datos_estadisticas.extend(stats_club)
                    if not stats_club:
                        log_error(f"    SIN STATS: {club} — 0 filas obtenidas (posible bloqueo Cloudflare)")

                if (i + 1) % 5 == 0:
                    log_info(f"    {i + 1}/{len(urls_equipos)} equipos procesados")
            except Exception as e:
                log_error(f"    CLUB FALLIDO: {club} — {type(e).__name__}: {e}")
                continue

        driver.quit()
        log_info(f" Transfermarkt: {len(datos_plantillas)} jugadores, {len(datos_estadios)} estadios, {len(datos_entrenadores)} entrenadores, {len(datos_estadisticas)} filas stats")
        return datos_generales, datos_plantillas, datos_estadios, datos_entrenadores, datos_estadisticas
    except Exception as e:
        log_error(f"Error en scraping Transfermarkt para {año_guardado}", e)
        try:
            driver.quit()
        except:
            pass
        return [], [], [], [], []

# =============================================================================
# PROCESAMIENTO PRINCIPAL POR AÑO (SOLO ADLS)
# =============================================================================

def limpiar_memoria():
    gc.collect()
    time.sleep(1)

def procesar_año_completo(año_usuario, adls_client=None, modo="reproceso"):
    """
    Función principal de orquestación: ejecuta el scraping completo para un año.

    Coordina en secuencia:
    1. Scraping FotMob (partidos, estadísticas, equipos, clasificación, campeones)
    2. Scraping Transfermarkt (plantillas, valoraciones, estadios, entrenadores)
    3. Guardado local de archivos JSON y CSV
    4. Upload a ADLS en la ruta landing/archivos_scraping/{año}/

    Aplica el desfase de calendarios: FotMob usa año_usuario, Transfermarkt usa año_usuario-1.

    Args:
        año_usuario: año a procesar (ej: 2024). Define la carpeta destino en ADLS.
        adls_client: cliente ADLS autenticado. Si es None, solo guarda localmente.
        modo: 'reproceso', 'historico' o 'incremental'.
    """
    global _EJECUCION_LOG
    _EJECUCION_LOG = []

    registrar_ejecucion("INICIANDO_EJECUCION", f"Modo: {modo}, Año entrada: {año_usuario}")

    año_guardado = año_usuario
    año_transfermarkt = año_usuario - 1   # Transfermarkt usa la temporada anterior
    temporada_fotmob = str(año_usuario)

    log_info(f" Procesando año {año_guardado}")
    log_info(f" FotMob: {temporada_fotmob} | Transfermarkt: {año_transfermarkt}")

    adls_conectado = adls_client is not None

    if not adls_conectado:
        log_error(" No hay conexión ADLS, no se guardarán archivos")
        return False

    try:
        # FOTMOB
        partidos = extraer_partidos(año_guardado)
        equipos = extraer_equipos(año_guardado, partidos if año_guardado == 2025 else None)
        campeones = extraer_campeones(año_guardado, _AÑO_ACTUAL)
        tabla = obtener_tabla_clasificacion(año_guardado)
        estadisticas = extraer_estadisticas_por_año(año_guardado, partidos)

        # Guardar FotMob en ADLS
        guardar_json_adls({"fuente": "FotMob", "temporada": año_guardado, "fecha_carga": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "data": partidos}, año_guardado, "partidos", adls_client)
        guardar_json_adls({"fuente": "FotMob", "temporada": año_guardado, "fecha_carga": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "data": equipos}, año_guardado, "equipos", adls_client)
        if campeones:
            guardar_json_adls({"fuente": "FotMob", "temporada": año_guardado, "fecha_carga": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "data": campeones}, año_guardado, "campeones", adls_client)
        guardar_json_adls({"fuente": "FotMob" if año_guardado != 2025 else "Wikipedia", "temporada": año_guardado, "fecha_carga": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "data": tabla}, año_guardado, "tablas_clasificacion", adls_client)
        if estadisticas:
            guardar_json_adls({"fuente": "FotMob", "temporada": año_guardado, "fecha_carga": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "data": estadisticas}, año_guardado, "estadisticas_partidos", adls_client)

        # TRANSFERMARKT
        datos_generales, datos_plantillas, datos_estadios, datos_entrenadores, datos_estadisticas = scraping_transfermarkt_por_año(año_transfermarkt)

        if datos_generales:
            df_general = pd.DataFrame(datos_generales)
            guardar_csv_adls(df_general, año_guardado, "liga1", adls_client, "Transfermarkt")
        else:
            log_info(f" Transfermarkt: sin datos generales para temporada {año_transfermarkt}")

        if datos_plantillas:
            df_plantillas = pd.DataFrame(datos_plantillas)
            guardar_csv_adls(df_plantillas, año_guardado, "plantillas", adls_client, "Transfermarkt")
        else:
            log_info(f" Transfermarkt: sin datos de plantillas para temporada {año_transfermarkt}")

        if datos_estadios:
            df_estadios = pd.DataFrame(datos_estadios)
            guardar_csv_adls(df_estadios, año_guardado, "estadios", adls_client, "Transfermarkt")
        else:
            log_info(f" Transfermarkt: sin datos de estadios para temporada {año_transfermarkt}")

        if datos_entrenadores:
            df_entrenadores = pd.DataFrame(datos_entrenadores)
            guardar_csv_adls(df_entrenadores, año_guardado, "entrenadores", adls_client, "Transfermarkt")
        else:
            log_info(f" Transfermarkt: sin datos de entrenadores para temporada {año_transfermarkt}")

        if datos_estadisticas:
            df_estadisticas = pd.DataFrame(datos_estadisticas)
            guardar_csv_adls(df_estadisticas, año_guardado, "estadisticas_jugadores", adls_client, "Transfermarkt")
        else:
            log_info(f" Transfermarkt: sin datos de estadísticas para temporada {año_transfermarkt}")

        # VALIDACIÓN Y REPORTE
        archivos_generados = contar_archivos_generados(año_guardado)
        guardar_reporte_archivos(año_guardado, adls_client, adls_conectado)

        if modo != "historico":
            guardar_trigger_adf(año_guardado, modo, archivos_generados, "completado", adls_client)
            guardar_log_unico(año_guardado, modo, adls_client)

        log_info(f" Año {año_guardado} completado exitosamente")
        return True

    except Exception as e:
        log_error(f"Error procesando año {año_guardado}", e)
        if modo != "historico":
            guardar_trigger_adf(año_guardado, modo, [], "error", adls_client)
            guardar_log_unico(año_guardado, modo, adls_client)
        return False

# =============================================================================
# REPAIR MODE — rellena estadisticas_jugadores para clubes faltantes en un año
# =============================================================================

def _crear_driver_tm():
    """
    Crea un WebDriver Chrome para scraping de Transfermarkt.
    Usa undetected_chromedriver si está disponible — bypassa la detección de bot
    de Cloudflare parcheando el binario de Chrome, sin los flags típicos de Selenium.
    Fallback a Selenium estándar con evasiones manuales.
    """
    _UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"

    if _UC_DISPONIBLE:
        # Detectar versión de Chrome instalada para que uc descargue el ChromeDriver correcto
        chrome_version = None
        try:
            import subprocess as _sp
            for cmd in (['google-chrome', '--version'], ['chromium-browser', '--version'], ['chromium', '--version']):
                try:
                    out = _sp.run(cmd, capture_output=True, text=True, timeout=5).stdout.strip()
                    if out:
                        chrome_version = int(out.split()[-1].split('.')[0])
                        break
                except Exception:
                    continue
        except Exception:
            pass

        options = uc.ChromeOptions()
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--lang=es-PE,es")
        options.add_argument(f"--user-agent={_UA}")
        driver = uc.Chrome(options=options, version_main=chrome_version)
        log_info(f"[DRIVER] undetected_chromedriver activo (Chrome {chrome_version}) — CF evasion ON")
    else:
        log_info("[DRIVER] undetected_chromedriver no disponible — usando Selenium estándar")
        options = webdriver.ChromeOptions()
        options.add_argument("--headless=new")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)
        options.add_argument(f"--user-agent={_UA}")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--lang=es-PE,es")
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        driver.execute_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
            Object.defineProperty(navigator, 'languages', {get: () => ['es-PE', 'es', 'en-US', 'en']});
            window.chrome = {runtime: {}};
        """)

    driver.set_page_load_timeout(60)
    return driver


def _leer_csv_adls(adls_client, adls_path):
    """Lee un CSV de ADLS y devuelve lista de dicts. Retorna [] si no existe."""
    try:
        file_client = adls_client.get_file_client(adls_path)
        contenido = file_client.download_file().readall().decode('utf-8-sig')
        import io
        reader = pd.read_csv(io.StringIO(contenido), sep='|', dtype=str)
        return reader.to_dict('records')
    except Exception:
        return []


def _obtener_urls_equipos_tm_requests(año_transfermarkt):
    """
    Obtiene la lista de clubes de TM para un año dado usando requests + BeautifulSoup.
    Mucho más rápido y resistente a Cloudflare que Selenium para páginas estáticas.
    Prueba TDeA (Liga 1 Apertura) y PER1 (Liga 1 completa) como fallback.
    Retorna lista de (club_nombre, url_equipo) o [] si falla.
    """
    _HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "es-PE,es;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Referer": "https://www.transfermarkt.pe/",
    }
    urls_candidatas = [
        f"https://www.transfermarkt.pe/liga-1-apertura/startseite/wettbewerb/TDeA/plus/?saison_id={año_transfermarkt}",
        f"https://www.transfermarkt.pe/primera-division/startseite/wettbewerb/PER1/plus/?saison_id={año_transfermarkt}",
    ]

    session = requests.Session()
    session.headers.update(_HEADERS)

    for intento, url_liga in enumerate(urls_candidatas):
        log_info(f"[REPAIR_TM] requests intento {intento+1}/{len(urls_candidatas)}: {url_liga}")
        try:
            resp = session.get(url_liga, timeout=30)
            if resp.status_code != 200:
                log_error(f"[REPAIR_TM] HTTP {resp.status_code} en {url_liga}")
                time.sleep(5)
                continue
            if "Just a moment" in resp.text or "cf-browser-verification" in resp.text:
                log_error(f"[REPAIR_TM] Cloudflare challenge en requests (intento {intento+1})")
                time.sleep(5)
                continue

            soup = BeautifulSoup(resp.text, "html.parser")
            tabla = soup.select_one("#yw1 table.items tbody")
            if not tabla:
                log_error(f"[REPAIR_TM] Sin tabla #yw1 en {url_liga}")
                time.sleep(5)
                continue

            urls_equipos = []
            for fila in tabla.find_all("tr"):
                celdas = fila.find_all("td")
                if len(celdas) < 7:
                    continue
                # Solo filas de club (tienen imagen tiny_wappen)
                if not fila.find("img", class_="tiny_wappen"):
                    continue
                celda_club = celdas[1]
                link_tag = celda_club.find("a", href=lambda h: h and "/verein/" in h)
                if not link_tag:
                    continue
                club = link_tag.get_text(strip=True)
                href = link_tag["href"]
                if not href.startswith("http"):
                    href = "https://www.transfermarkt.pe" + href
                if club and len(club) > 2:
                    urls_equipos.append((club, href))

            if urls_equipos:
                log_info(f"[REPAIR_TM] {len(urls_equipos)} clubes obtenidos vía requests")
                return urls_equipos
            else:
                log_error(f"[REPAIR_TM] Página OK pero sin clubes extraídos en {url_liga}")

        except Exception as e:
            log_error(f"[REPAIR_TM] requests intento {intento+1} fallido: {type(e).__name__} — {str(e)[:120]}")
        time.sleep(5)

    log_error(f"[REPAIR_TM] requests sin clubes para saison={año_transfermarkt}. Fallback a Selenium.")
    return []


def _obtener_urls_equipos_tm(driver, año_transfermarkt):
    """
    Obtiene lista de (club_nombre, url_equipo) para un año TM dado.
    Estrategia:
      1. requests + BeautifulSoup (rápido, sin Cloudflare headless)
      2. Selenium con 3 reintentos y espera creciente (fallback)
    """
    # --- Intento 1: requests (rápido, resistente a Cloudflare) ---
    resultado = _obtener_urls_equipos_tm_requests(año_transfermarkt)
    if resultado:
        return resultado

    # --- Intento 2: Selenium como fallback ---
    log_info(f"[REPAIR_TM] Usando Selenium como fallback para saison={año_transfermarkt}...")
    urls_candidatas = [
        f"https://www.transfermarkt.pe/liga-1-apertura/startseite/wettbewerb/TDeA/plus/?saison_id={año_transfermarkt}",
        f"https://www.transfermarkt.pe/primera-division/startseite/wettbewerb/PER1/plus/?saison_id={año_transfermarkt}",
    ]

    # Timeout mayor para la página de liga (Cloudflare challenge puede tardar >60s)
    driver.set_page_load_timeout(180)

    def _extraer_clubes_del_driver():
        urls_equipos = []
        filas = driver.find_elements(By.XPATH, '//div[@id="yw1"]//table[contains(@class,"items")]/tbody/tr')
        for fila in filas:
            try:
                columnas = fila.find_elements(By.XPATH, './/td')
                if len(columnas) >= 7:
                    tiene_escudo = fila.find_elements(By.XPATH, './/img[contains(@class, "tiny_wappen")]')
                    if not tiene_escudo:
                        continue
                    club = columnas[1].text.strip()
                    if club and len(club) > 2:
                        link = columnas[1].find_element(By.XPATH, './/a').get_attribute('href')
                        if "/verein/" in link:
                            urls_equipos.append((club, link))
            except Exception:
                continue
        return urls_equipos

    for intento in range(3):
        espera_inicial = 15 + (intento * 20)   # 15s → 35s → 55s
        url_liga = urls_candidatas[min(intento, len(urls_candidatas) - 1)]
        log_info(f"[REPAIR_TM] Selenium intento {intento+1}/3, espera={espera_inicial}s: {url_liga}")
        try:
            try:
                driver.get(url_liga)
            except Exception:
                # TimeoutException de page_load: la página puede estar cargada igual
                log_info(f"[REPAIR_TM] page_load timeout — intentando leer contenido actual")
            time.sleep(espera_inicial)
            manejar_banner(driver)

            # Intentar esperar tabla; si no llega, leer lo que haya
            try:
                WebDriverWait(driver, 30).until(
                    EC.presence_of_element_located((By.XPATH, '//table[contains(@class,"items")]//td[2]//a'))
                )
            except Exception:
                log_info(f"[REPAIR_TM] WebDriverWait agotado — leyendo página actual")
            time.sleep(3)

            urls_equipos = _extraer_clubes_del_driver()
            if urls_equipos:
                log_info(f"[REPAIR_TM] Selenium: {len(urls_equipos)} clubes obtenidos")
                driver.set_page_load_timeout(60)  # restaurar para páginas de jugadores
                return urls_equipos
            else:
                log_error(f"[REPAIR_TM] Selenium intento {intento+1}: sin clubes en la página")

        except Exception as e:
            log_error(f"[REPAIR_TM] Selenium intento {intento+1} fallido: {type(e).__name__} — {str(e)[:120]}")

        if intento < 2:
            time.sleep(30)

    driver.set_page_load_timeout(60)  # restaurar aunque falle
    log_error(f"[REPAIR_TM] Sin clubes TM tras requests + Selenium para saison={año_transfermarkt}")
    return []



def reparar_plantillas_y_estadisticas(año_usuario, adls_client, solo_clubes=None, forzar_todos=False, forzar_solo_stats=False):
    """
    Repair mode: re-scrapea TM para los clubes que faltan en ADLS para el año dado.
    Actualiza AMBOS archivos: plantillas_{año}.csv y estadisticas_jugadores_{año}.csv.

    Args:
        año_usuario:      año a reparar (ej: 2025).
        adls_client:      cliente ADLS autenticado.
        solo_clubes:      lista opcional de nombres exactos a forzar (case-insensitive).
                          Si None y forzar_todos=False, detecta automáticamente faltantes.
        forzar_todos:     si True, re-scrapea los 19 clubes aunque ya existan en ADLS
                          (re-scrapea TANTO plantillas COMO estadísticas).
        forzar_solo_stats: si True, reutiliza las plantillas de ADLS tal cual y solo
                          re-scrapea estadísticas para todos los clubes. Útil cuando
                          las plantillas están correctas pero los stats están vacíos.

    Nota TM: saison=año_usuario-1  (ej: 2025 → saison=2024 = "24/25")
    """
    año_guardado      = año_usuario
    año_transfermarkt = año_usuario - 1

    log_info(f"[REPAIR_TM] === año={año_guardado} (TM saison={año_transfermarkt}) ===")

    if adls_client is None:
        log_error("[REPAIR_TM] Se requiere conexión ADLS. Abortando.")
        return False

    # 1. Leer plantillas y estadisticas existentes en ADLS
    adls_path_plant = f"primera_division/landing/{año_guardado}/plantillas_{año_guardado}.csv"
    adls_path_stats = f"primera_division/landing/{año_guardado}/estadisticas_jugadores_{año_guardado}.csv"

    rows_plant_existentes = _leer_csv_adls(adls_client, adls_path_plant)
    rows_stats_existentes = _leer_csv_adls(adls_client, adls_path_stats)

    clubes_con_plantilla = {r['club'].strip().lower() for r in rows_plant_existentes if r.get('club')}
    log_info(f"[REPAIR_TM] Plantillas en ADLS : {len(rows_plant_existentes)} filas / {len(clubes_con_plantilla)} clubes")
    log_info(f"[REPAIR_TM] Estadisticas en ADLS: {len(rows_stats_existentes)} filas")

    # 2. Obtener lista de clubes de TM para ese año
    driver = _crear_driver_tm()
    try:
        # forzar_solo_stats: reutilizar plantillas ADLS directamente, sin ir a TM por la lista
        urls_equipos = [] if forzar_solo_stats else _obtener_urls_equipos_tm(driver, año_transfermarkt)
        if forzar_solo_stats:
            log_info(f"[REPAIR_TM] Modo forzar_solo_stats: reutilizando plantillas ADLS, re-scrapeando solo estadísticas.")
        if not urls_equipos:
            # Fallback: Cloudflare bloquea la página de liga (o forzar_solo_stats activo).
            # Si tenemos plantillas en ADLS, usarlas directamente para re-scrapear SOLO estadísticas.
            if not rows_plant_existentes:
                log_error(f"[REPAIR_TM] Sin clubes TM ni plantillas ADLS para saison={año_transfermarkt}. Abortando.")
                driver.quit()
                return False

            log_info(f"[REPAIR_TM] Cloudflare bloqueó lista TM. Fallback: re-scrapear stats desde plantillas ADLS.")

            # Normalizar keys de las filas ADLS (lowercase) al formato esperado
            # por extraer_estadisticas_por_club (id_tm, slug_tm, Club, Jugador, Año)
            def _norm_row(r):
                return {
                    'id_tm':         r.get('id_tm', ''),
                    'slug_tm':       r.get('slug_tm', ''),
                    'Jugador':       r.get('jugador', r.get('Jugador', '')),
                    'Club':          r.get('club',    r.get('Club', '')),
                    'Año':           r.get('ano',     r.get('Año', '')),
                    'foto_url':      r.get('foto_url', ''),
                    'verein_id_tm':  r.get('verein_id_tm', ''),  # disponible en plantillas scrapeadas con /plus/1
                }

            from collections import defaultdict
            plantillas_por_club = defaultdict(list)
            for row in rows_plant_existentes:
                norm = _norm_row(row)
                if norm['Club']:
                    plantillas_por_club[norm['Club']].append(norm)

            clubes_lista = sorted(plantillas_por_club.keys())
            if solo_clubes:
                forzados = {c.strip().lower() for c in solo_clubes}
                clubes_lista = [c for c in clubes_lista if c.strip().lower() in forzados]
            log_info(f"[REPAIR_TM] Clubes ADLS a procesar ({len(clubes_lista)}): {clubes_lista}")

            nuevas_stats = []
            _stats_cache_adls = {}  # compartido: evita scrapear 2x a jugadores transferidos
            for idx, club in enumerate(clubes_lista):
                plantillas_club = plantillas_por_club[club]
                log_info(f"[REPAIR_TM][stats-adls] [{idx+1}/{len(clubes_lista)}] {club} ({len(plantillas_club)} jugadores)")
                try:
                    stats_club = extraer_estadisticas_por_club(
                        driver, plantillas_club, año_transfermarkt,
                        _stats_cache=_stats_cache_adls)
                    if stats_club:
                        nuevas_stats.extend(stats_club)
                    log_info(f"[REPAIR_TM]   ✓ stats={len(stats_club) if stats_club else 0}")
                except Exception as e:
                    log_error(f"[REPAIR_TM]   ✗ {club}: {type(e).__name__}: {e}")

            driver.quit()

            # Post-proceso: filtrar filas de jugadores transferidos por ClubTM.
            # Las plantillas ADLS no tienen verein_id_tm, así que lo inferimos
            # a partir de los jugadores NO-transferidos (aparecen en un solo club):
            # su ClubTM más frecuente ES el verein_id de ese club.
            if nuevas_stats:
                from collections import Counter as _Counter, defaultdict as _dd
                id_to_rows = _dd(list)
                for s in nuevas_stats:
                    id_to_rows[s.get('id_tm', '')].append(s)

                club_to_verein = {}
                for _id, _rows in id_to_rows.items():
                    clubs_del_jugador = {r.get('Club', '') for r in _rows if r.get('Club', '')}
                    if len(clubs_del_jugador) == 1:
                        _club = next(iter(clubs_del_jugador))
                        _ctm_cnt = _Counter(
                            r.get('ClubTM', '').strip() for r in _rows
                            if r.get('ClubTM', '').strip()
                        )
                        if _ctm_cnt and _club not in club_to_verein:
                            club_to_verein[_club] = _ctm_cnt.most_common(1)[0][0]

                if club_to_verein:
                    antes_filtro = len(nuevas_stats)
                    nuevas_stats = [
                        s for s in nuevas_stats
                        if not s.get('ClubTM', '').strip()                         # sin ClubTM → conservar
                        or not club_to_verein.get(s.get('Club', ''))               # club sin mapeo → conservar
                        or s.get('ClubTM', '').strip() == club_to_verein[s.get('Club', '')]  # match → conservar
                    ]
                    log_info(f"[REPAIR_TM] Filtro ClubTM: {antes_filtro} → {len(nuevas_stats)} filas "
                             f"(map: {len(club_to_verein)} clubs resueltos)")

                # Dedup final por (id_tm, club, competencia)
                seen = set()
                deduped = []
                for s in nuevas_stats:
                    key = (s.get('id_tm',''), s.get('Club',''), s.get('Competencia',''))
                    if key not in seen:
                        seen.add(key)
                        deduped.append(s)
                if len(deduped) < len(nuevas_stats):
                    log_info(f"[REPAIR_TM] Dedup ADLS: {len(nuevas_stats)} → {len(deduped)} filas")
                nuevas_stats = deduped

            # Reemplazar estadísticas (overwrite completo — corrige la inflación)
            ok_plant = True  # plantillas no modificadas
            if nuevas_stats:
                df_nuevas_stats = pd.DataFrame(nuevas_stats)
                df_nuevas_stats.columns = [limpiar_nombre_columna(c) for c in df_nuevas_stats.columns]
                ok_stats = guardar_csv_adls(df_nuevas_stats, año_guardado, "estadisticas_jugadores", adls_client, "Transfermarkt")
                if ok_stats:
                    log_info(f"[REPAIR_TM] ✓ Estadisticas (reemplazadas): {len(df_nuevas_stats)} filas")
                else:
                    log_error(f"[REPAIR_TM] ✗ Error al subir estadisticas_{año_guardado}.csv")
            else:
                log_error("[REPAIR_TM] Sin estadísticas obtenidas en modo fallback ADLS.")
                ok_stats = False
            return ok_stats

        log_info(f"[REPAIR_TM] Clubes en TM ({len(urls_equipos)}): {[c for c,_ in urls_equipos]}")

        # 3. Determinar qué clubes procesar
        if forzar_todos:
            clubes_a_procesar = urls_equipos
            log_info(f"[REPAIR_TM] Modo forzar_todos: re-scrapeando los {len(clubes_a_procesar)} clubes")
        elif solo_clubes:
            forzados = {c.strip().lower() for c in solo_clubes}
            clubes_a_procesar = [(c, u) for c, u in urls_equipos if c.strip().lower() in forzados]
            log_info(f"[REPAIR_TM] Modo forzado ({len(clubes_a_procesar)}): {[c for c,_ in clubes_a_procesar]}")
        else:
            clubes_a_procesar = [(c, u) for c, u in urls_equipos
                                  if c.strip().lower() not in clubes_con_plantilla]
            log_info(f"[REPAIR_TM] Faltantes ({len(clubes_a_procesar)}): {[c for c,_ in clubes_a_procesar]}")

        if not clubes_a_procesar:
            log_info("[REPAIR_TM] Todo completo para este año. Sin acción.")
            driver.quit()
            return True

        # 4. Scrapear clubes faltantes — plantillas + estadísticas, retry x3
        nuevas_plantillas = []
        nuevas_stats      = []
        stats_cache       = {}  # cache compartido entre clubs: evita re-scrapear jugadores transferidos

        for idx, (club, url_equipo) in enumerate(clubes_a_procesar):
            log_info(f"[REPAIR_TM] [{idx+1}/{len(clubes_a_procesar)}] {club}")
            exito = False
            for intento in range(3):
                try:
                    plantillas_club, _ = extraer_plantillas_con_reintentos(
                        driver, club, url_equipo, año_guardado, max_reintentos=2)
                    if not plantillas_club:
                        log_error(f"[REPAIR_TM]   Sin plantillas intento {intento+1}/3")
                        time.sleep(5)
                        continue

                    stats_club = extraer_estadisticas_por_club(
                        driver, plantillas_club, año_transfermarkt, _stats_cache=stats_cache)

                    nuevas_plantillas.extend(plantillas_club)
                    if stats_club:
                        nuevas_stats.extend(stats_club)
                    log_info(f"[REPAIR_TM]   ✓ plantillas={len(plantillas_club)} stats={len(stats_club) if stats_club else 0}")
                    exito = True
                    break

                except Exception as e:
                    espera = 15 * (intento + 1)
                    log_error(f"[REPAIR_TM]   Error intento {intento+1}/3: {type(e).__name__}: {e} — espera {espera}s")
                    time.sleep(espera)

            if not exito:
                log_error(f"[REPAIR_TM]   ✗ {club}: FALLIDO tras 3 intentos")

        driver.quit()

        # Columnas de metadatos que guardar_csv_adls añade — no incluirlas en el
        # DataFrame existente para evitar duplicados tras el concat.
        _META_COLS = ['fuente', 'temporada', 'fecha_carga']

        # 5. Merge plantillas y overwrite en ADLS
        if nuevas_plantillas:
            # Normalizar nombres de columna del nuevo DataFrame ANTES del concat
            # (los datos existentes en ADLS ya tienen columnas limpias: ano, club…
            # los nuevos vienen con nombres crudos: Año, Club… si se mezclan sin
            # limpiar, guardar_csv_adls generaría columnas duplicadas: ano + ano)
            df_nuevas_plant = pd.DataFrame(nuevas_plantillas)
            df_nuevas_plant.columns = [limpiar_nombre_columna(c) for c in df_nuevas_plant.columns]
            df_nuevas_plant['_orden'] = 1  # nuevas filas tienen prioridad
            if rows_plant_existentes and not forzar_todos:
                df_exist_plant = pd.DataFrame(rows_plant_existentes).drop(
                    columns=_META_COLS, errors='ignore')
                df_exist_plant['_orden'] = 0  # filas viejas tienen menor prioridad
                df_plant_final = pd.concat([df_exist_plant, df_nuevas_plant], ignore_index=True)
                # Dedup: por (jugador, ano) quedarse con la fila de mayor _orden (nueva)
                _dedup_cols = [c for c in ['jugador', 'ano'] if c in df_plant_final.columns]
                if _dedup_cols:
                    df_plant_final = (df_plant_final
                        .sort_values('_orden', ascending=False)
                        .drop_duplicates(subset=_dedup_cols, keep='first')
                        .reset_index(drop=True))
            else:
                df_plant_final = df_nuevas_plant  # reemplazo completo
            df_plant_final = df_plant_final.drop(columns=['_orden'], errors='ignore')
            ok_plant = guardar_csv_adls(df_plant_final, año_guardado, "plantillas", adls_client, "Transfermarkt")
            if ok_plant:
                log_info(f"[REPAIR_TM] ✓ Plantillas subidas: {len(df_plant_final)} filas totales")
            else:
                log_error(f"[REPAIR_TM] ✗ Error al subir plantillas_{año_guardado}.csv a ADLS")
        else:
            log_error("[REPAIR_TM] Sin nuevas plantillas obtenidas.")
            ok_plant = False

        # 6. Merge estadísticas y overwrite en ADLS
        # forzar_todos=True → reemplazar completo (no concat) para evitar duplicados
        if nuevas_stats:
            df_nuevas_stats = pd.DataFrame(nuevas_stats)
            df_nuevas_stats.columns = [limpiar_nombre_columna(c) for c in df_nuevas_stats.columns]
            # Dedup por (id_tm, club, competencia): elimina filas duplicadas que pueden
            # ocurrir cuando un jugador aparece en más de un kader (transferidos)
            dedup_cols = [c for c in ['id_tm', 'club', 'competencia'] if c in df_nuevas_stats.columns]
            if dedup_cols:
                antes = len(df_nuevas_stats)
                df_nuevas_stats = df_nuevas_stats.drop_duplicates(subset=dedup_cols, keep='first')
                if len(df_nuevas_stats) < antes:
                    log_info(f"[REPAIR_TM] Dedup stats: {antes} → {len(df_nuevas_stats)} filas ({antes - len(df_nuevas_stats)} duplicados eliminados)")
            if rows_stats_existentes and not forzar_todos:
                df_exist_stats = pd.DataFrame(rows_stats_existentes).drop(
                    columns=_META_COLS, errors='ignore')
                df_nuevas_stats['_orden'] = 1
                df_exist_stats['_orden'] = 0
                df_stats_final = pd.concat([df_exist_stats, df_nuevas_stats], ignore_index=True)
                # Dedup: nuevas filas reemplazan viejas por (id_tm, club, competencia)
                _dedup_s = [c for c in ['id_tm', 'club', 'competencia'] if c in df_stats_final.columns]
                if _dedup_s:
                    df_stats_final = (df_stats_final
                        .sort_values('_orden', ascending=False)
                        .drop_duplicates(subset=_dedup_s, keep='first')
                        .reset_index(drop=True))
                df_stats_final = df_stats_final.drop(columns=['_orden'], errors='ignore')
            else:
                df_stats_final = df_nuevas_stats  # reemplazo completo
            ok_stats = guardar_csv_adls(df_stats_final, año_guardado, "estadisticas_jugadores", adls_client, "Transfermarkt")
            if ok_stats:
                log_info(f"[REPAIR_TM] ✓ Estadisticas subidas: {len(df_stats_final)} filas totales")
            else:
                log_error(f"[REPAIR_TM] ✗ Error al subir estadisticas_jugadores_{año_guardado}.csv a ADLS")
        else:
            log_error("[REPAIR_TM] Sin nuevas estadísticas (puede ser normal en temporadas antiguas).")
            ok_stats = False

        return ok_plant

    except Exception as e:
        log_error(f"[REPAIR_TM] Error general: {e}")
        try:
            driver.quit()
        except Exception:
            pass
        return False


# =============================================================================
# EJECUCIÓN PRINCIPAL
# =============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scraping Liga 1 Perú")
    parser.add_argument(
        "--modo",
        choices=["incremental", "historico", "reproceso", "repair_tm"],
        default=os.environ.get("SCRAPING_MODO", "incremental")
    )
    parser.add_argument("--anio-inicio",   type=int, default=int(os.environ.get("SCRAPING_ANIO_INICIO", "2020")))
    parser.add_argument("--anio-fin",      type=int, default=int(os.environ.get("SCRAPING_ANIO_FIN",    str(_AÑO_ACTUAL))))
    parser.add_argument("--anio-objetivo", type=int, default=None)

    args = parser.parse_args()

    # anio_objetivo: CLI > env var > None
    if args.anio_objetivo is None:
        anio_obj_env = os.environ.get("SCRAPING_ANIO_OBJETIVO", "").strip()
        args.anio_objetivo = int(anio_obj_env) if anio_obj_env else None

    print("=" * 70)
    print(" SCRAPING LIGA 1 PERÚ - FOTMOB + TRANSFERMARKT + WIKIPEDIA")
    print("=" * 70)
    print(f"Año actual : {_AÑO_ACTUAL}")
    print(f"Modo       : {args.modo}")
    print(f"anio_obj   : {args.anio_objetivo}")
    print(f"anio_inicio: {args.anio_inicio}  anio_fin: {args.anio_fin}")
    print("=" * 70)

    adls_client = None
    if os.environ.get("GITHUB_ACTIONS") == "true":
        adls_client = conectar_adls_keyvault()
        if not adls_client:
            log_error(" No se pudo conectar a ADLS. Abortando.")
            exit(1)
    else:
        log_info(" Ejecución local sin ADLS. Solo logs, no se guardan archivos.")

    if args.modo == "incremental":
        procesar_año_completo(_AÑO_ACTUAL, adls_client, "incremental")

    elif args.modo == "reproceso":
        año = args.anio_objetivo or _AÑO_ACTUAL
        procesar_año_completo(año, adls_client, "reproceso")

    elif args.modo == "historico":
        for año in range(args.anio_inicio, args.anio_fin + 1):
            procesar_año_completo(año, adls_client, "historico")
            limpiar_memoria()
            time.sleep(5)

        # ── Fallback: subir desde datasets/{año}/ archivos que no llegaron a ADLS ──
        # Solo aplica en historico porque incremental/reproceso se espera que funcionen.
        # Si el scraping de TM o FotMob falló para algún año, los archivos que tenga
        # el usuario en datasets/{año}/ llenan los huecos en ADLS.
        if adls_client:
            from pathlib import Path
            # datasets/ está en la raíz del repo: 3 niveles arriba de este script
            repo_root     = Path(__file__).parents[3]
            datasets_root = repo_root / "datasets"
            log_info("=== Fallback historico: verificando archivos faltantes en ADLS ===")
            subidos_fb = 0
            for año in range(args.anio_inicio, args.anio_fin + 1):
                anio_dir = datasets_root / str(año)
                if not anio_dir.exists():
                    continue
                for fpath in sorted(anio_dir.iterdir()):
                    if fpath.suffix == ".txt" or not fpath.is_file():
                        continue
                    adls_path = f"primera_division/landing/{año}/{fpath.name}"
                    archivo_en_adls = False
                    try:
                        adls_client.get_file_client(adls_path).get_file_properties()
                        archivo_en_adls = True
                    except Exception:
                        archivo_en_adls = False
                    if not archivo_en_adls:
                        try:
                            data = fpath.read_bytes()
                            adls_client.get_file_client(adls_path).upload_data(data, overwrite=True)
                            log_info(f"  [fallback] {año}/{fpath.name} -> ADLS OK")
                            subidos_fb += 1
                        except Exception as e_up:
                            log_error(f"  [fallback] {año}/{fpath.name} -> error: {e_up}")
            if subidos_fb:
                log_info(f"=== Fallback: {subidos_fb} archivo(s) subidos desde datasets/ a ADLS ===")
            else:
                log_info("=== Fallback: sin huecos — ADLS ya tenia todos los archivos ===")

    elif args.modo == "repair_tm":
        # Repara plantillas + estadisticas_jugadores para clubes faltantes en ADLS.
        clubes_forzar_str = os.environ.get("SCRAPING_REPAIR_CLUBES", "")
        solo_clubes       = [c.strip() for c in clubes_forzar_str.split(",") if c.strip()] or None
        forzar_todos      = os.environ.get("SCRAPING_REPAIR_FORZAR_TODOS", "false").lower() == "true"

        if args.anio_objetivo is not None:
            años_a_reparar = [args.anio_objetivo]
        else:
            años_a_reparar = list(range(args.anio_inicio, args.anio_fin + 1))

        log_info(f" repair_tm años: {años_a_reparar}")
        if solo_clubes:
            log_info(f" Forzando clubes: {solo_clubes}")

        resultados = {}
        for año in años_a_reparar:
            ok = reparar_plantillas_y_estadisticas(
                año, adls_client,
                solo_clubes=solo_clubes,
                forzar_todos=forzar_todos
            )
            resultados[año] = "OK" if ok else "FALLO"
            limpiar_memoria()
            time.sleep(10)

        print("\n--- Resumen repair_tm ---")
        for año, res in resultados.items():
            print(f"  {año}: {res}")

    print("\n" + "=" * 70)
    print(" PROCESO COMPLETADO")
    print("=" * 70)
