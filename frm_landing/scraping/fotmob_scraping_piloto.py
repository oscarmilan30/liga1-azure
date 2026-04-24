# Databricks notebook source
# MAGIC %md
# MAGIC ## **Widgets y configuración inicial**

# COMMAND ----------

# Librerías principales
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

from bs4 import BeautifulSoup
import pandas as pd
import time
import json
import re
from datetime import datetime
from pyspark.sql import SparkSession

# Configurar Spark
spark = SparkSession.builder.appName("FotMobScraper").getOrCreate()

# Widgets de Databricks para pasar parámetros
temporada = dbutils.widgets.get("temporada")
liga_id = dbutils.widgets.get("liga_id")
url_base = dbutils.widgets.get("url_base")
container_temp = dbutils.widgets.get("container_temp")
rutabase = dbutils.widgets.get("rutabase")
liga_nombre = dbutils.widgets.get("liga_nombre")

print(f"Iniciando scraping: Temporada {temporada}, Liga {liga_nombre}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## **IMPORTAR UTILITARIO**

# COMMAND ----------

from util.utils import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## **CONEXION ADLS**

# COMMAND ----------

setup_adls()
if test_conexion_adls():
    print("Todo listo para trabajar con ADLS")
    raiz_landing = get_abfss_path(container_temp)
    print(f"Ruta a trabajar: {raiz_landing}")
else:
    print("Revisar configuracion")

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Setup driver y helper para guardar JSON**

# COMMAND ----------

def setup_driver():
    """Configura Selenium ChromeDriver en modo headless"""
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920x1080")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

    # Define la ruta manual al ejecutable de ChromeDriver
    # Asegúrate de que esta ruta sea la correcta para el archivo que subiste
    chromedriver_path = '/Volumes/adbligaperuana/webscraping/webscraping'

    # Crea el servicio con la ruta explícita
    service = Service(chromedriver_path)
    
    return webdriver.Chrome(service=service, options=options)


def guardar_json(data, ruta_completa):
    """Guarda un diccionario o lista como JSON en ADLS"""
    try:
        df = spark.createDataFrame([data])
        df.write.mode("overwrite").json(ruta_completa)
        print(f"Guardado: {ruta_completa}")
    except Exception as e:
        print(f"Error guardando JSON: {e}")
        raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Scraping Liga Completa**

# COMMAND ----------

def scraping_liga_completa(temporada, liga_id, liga_nombre):
    """Scrapea la liga completa: clasificación, partidos, estadísticas, goleadores, asistentes y equipos"""
    print(f"Scrapeando liga temporada {temporada}...")
    
    url = f"{url_base}/leagues/{liga_id}/overview/{liga_nombre}?season={temporada}"
    datos = {"temporada": temporada, "liga_id": liga_id, "liga_nombre": liga_nombre}

    with setup_driver() as driver:
        driver.get(url)
        time.sleep(5)
        
        datos["clasificacion"] = extraer_clasificacion(driver)
        datos["partidos"] = extraer_partidos_con_stats(driver)
        datos["estadisticas_generales"] = extraer_estadisticas_generales(driver)
        datos["goleadores"] = extraer_goleadores_completo(driver)
        datos["asistentes"] = extraer_asistentes(driver)
        datos["equipos_basicos"] = extraer_equipos_con_ids(driver)

    return datos


def extraer_clasificacion(driver):
    """Extrae la tabla de clasificación de la liga"""
    clasificacion = []
    try:
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div[class*='TableRowCSS']"))
        )
        filas = driver.find_elements(By.CSS_SELECTOR, "div[class*='TableRowCSS']")
        for fila in filas:
            try:
                posicion = fila.find_element(By.CSS_SELECTOR, "span[class*='TableRank']").text
                equipo = fila.find_element(By.CSS_SELECTOR, "span[class*='TeamName']").text
                pts = fila.find_element(By.CSS_SELECTOR, "span[class*='TablePoints']").text
                stats = fila.find_elements(By.CSS_SELECTOR, "span[class*='TableStat']")
                pj, pg, pe, pp, gf, gc, dg = [stat.text for stat in stats[:7]]

                clasificacion.append({
                    "posicion": posicion, "equipo": equipo, "pj": pj, "pg": pg,
                    "pe": pe, "pp": pp, "gf": gf, "gc": gc, "dg": dg, "pts": pts
                })
            except:
                continue
    except Exception as e:
        print(f"Error clasificación: {e}")
    return clasificacion

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Scraping partidos con stats**

# COMMAND ----------

def extraer_partidos_con_stats(driver):
    """Extrae los partidos y estadísticas de la temporada"""
    partidos = []
    try:
        driver.find_element(By.CSS_SELECTOR, "a[href*='partidos']").click()
        time.sleep(3)
        page = 1
        while True:
            partidos_page = extraer_partidos_pagina(driver)
            if not partidos_page:
                break
            partidos.extend(partidos_page)
            
            try:
                next_btn = driver.find_element(By.CSS_SELECTOR, "button[aria-label='Next page']")
                if not next_btn.is_enabled():
                    break
                next_btn.click()
                time.sleep(3)
                page += 1
            except:
                break
    except Exception as e:
        print(f"Error partidos: {e}")
    return partidos


def extraer_partidos_pagina(driver):
    """Extrae los partidos de la página actual"""
    partidos = []
    try:
        partido_elements = driver.find_elements(By.CSS_SELECTOR, "a[class*='MatchWrapper']")
        for partido in partido_elements:
            try:
                local = partido.find_element(By.CSS_SELECTOR, "div[class*='HomeTeam'] span[class*='TeamName']").text
                visitante = partido.find_element(By.CSS_SELECTOR, "div[class*='AwayTeam'] span[class*='TeamName']").text
                resultado = partido.find_element(By.CSS_SELECTOR, "span[class*='MatchScore']").text
                fecha = partido.find_element(By.CSS_SELECTOR, "span[class*='MatchTime']").text

                partido.click()
                time.sleep(2)
                stats = extraer_stats_partido(driver)

                partidos.append({
                    "fecha": fecha, "local": local, "visitante": visitante,
                    "resultado": resultado, "estadisticas": stats
                })

                driver.back()
                time.sleep(2)
            except:
                continue
    except Exception as e:
        print(f"Error página partidos: {e}")
    return partidos


def extraer_stats_partido(driver):
    """Extrae estadísticas específicas del partido"""
    stats = {}
    try:
        driver.find_element(By.CSS_SELECTOR, "a[href*='stats']").click()
        time.sleep(2)
        stat_boxes = driver.find_elements(By.CSS_SELECTOR, "div[class*='StatBox']")
        for box in stat_boxes:
            titulo = box.find_element(By.CSS_SELECTOR, "h3").text.lower()
            valores = box.find_elements(By.CSS_SELECTOR, "span[class*='StatValue']")
            if len(valores) >= 2:
                if 'posesión' in titulo:
                    stats['posesion'] = {'local': valores[0].text, 'visitante': valores[1].text}
                elif 'tiros' in titulo:
                    stats['tiros'] = {'local': valores[0].text, 'visitante': valores[1].text}
                elif 'tarjetas amarillas' in titulo:
                    stats['tarjetas_amarillas'] = {'local': valores[0].text, 'visitante': valores[1].text}
                elif 'tarjetas rojas' in titulo:
                    stats['tarjetas_rojas'] = {'local': valores[0].text, 'visitante': valores[1].text}
                elif 'faltas' in titulo:
                    stats['faltas'] = {'local': valores[0].text, 'visitante': valores[1].text}
                elif 'corners' in titulo:
                    stats['corners'] = {'local': valores[0].text, 'visitante': valores[1].text}
    except Exception as e:
        print(f"Error stats partido: {e}")
    return stats

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Scraping Equipos y Plantilla**

# COMMAND ----------

def extraer_equipos_con_ids(driver):
    """Extrae los equipos básicos con sus IDs"""
    equipos = []
    try:
        filas_equipos = driver.find_elements(By.CSS_SELECTOR, "div[class*='TableRowCSS']")
        for fila in filas_equipos:
            try:
                link_equipo = fila.find_element(By.CSS_SELECTOR, "a[href*='/teams/']")
                href = link_equipo.get_attribute("href")
                equipo_id = href.split('/')[5]
                nombre_equipo = fila.find_element(By.CSS_SELECTOR, "span[class*='TeamName']").text
                equipos.append({"equipo_id": equipo_id, "nombre": nombre_equipo})
            except:
                continue
    except Exception as e:
        print(f"Error equipos: {e}")
    return equipos


def scraping_equipos_completo(equipos_basicos, temporada):
    """Recorre todos los equipos y scrapea plantilla, partidos y estadísticas"""
    print(f"Scrapeando {len(equipos_basicos)} equipos...")
    datos_equipos = {"temporada": temporada, "equipos": []}
    for equipo in equipos_basicos:
        try:
            equipo_data = scraping_equipo_individual(equipo["equipo_id"], equipo["nombre"], temporada)
            datos_equipos["equipos"].append(equipo_data)
            time.sleep(2)
        except Exception as e:
            print(f"Error {equipo['nombre']}: {e}")
            continue
    return datos_equipos


def scraping_equipo_individual(equipo_id, equipo_nombre, temporada):
    """Scrapea un equipo individual: plantilla, estadísticas y entrenador"""
    url = f"{url_base}/teams/{equipo_id}/overview/{equipo_nombre}?season={temporada}"
    with setup_driver() as driver:
        driver.get(url)
        time.sleep(4)
        return {
            "equipo_id": equipo_id,
            "nombre": equipo_nombre,
            "plantilla": extraer_plantilla_completa(driver),
            "estadisticas_equipo": extraer_estadisticas_equipo(driver),
            "partidos_equipo": extraer_partidos_equipo(driver),
            "entrenador": extraer_entrenador(driver)
        }

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Plantilla, goleadores, asistentes, estadísticas y entrenador**

# COMMAND ----------

def extraer_plantilla_completa(driver):
    """Extrae la plantilla completa de un equipo"""
    plantilla = []
    try:
        jugadores = driver.find_elements(By.CSS_SELECTOR, "div[class*='PlayerRow']")
        for jugador in jugadores:
            try:
                try:
                    link_jugador = jugador.find_element(By.CSS_SELECTOR, "a[href*='/players/']")
                    jugador_id = link_jugador.get_attribute("href").split('/')[5]
                except:
                    jugador_id = None

                nombre = jugador.find_element(By.CSS_SELECTOR, "span[class*='PlayerName']").text.strip()
                posicion = jugador.find_element(By.CSS_SELECTOR, "div[class*='Position']").text.strip()

                try:
                    bandera = jugador.find_element(By.CSS_SELECTOR, "img[class*='Flag']")
                    nacionalidad = bandera.get_attribute("alt")
                except:
                    nacionalidad = "Desconocida"

                try:
                    edad = jugador.find_element(By.CSS_SELECTOR, "div[class*='Age']").text.strip()
                except:
                    edad = "N/A"

                plantilla.append({
                    "jugador_id": jugador_id, "nombre": nombre,
                    "posicion": posicion, "nacionalidad": nacionalidad, "edad": edad
                })
            except:
                continue
    except Exception as e:
        print(f"Error plantilla: {e}")
    return plantilla


def extraer_entrenador(driver):
    """Extrae información del entrenador"""
    try:
        info_section = driver.find_element(By.CSS_SELECTOR, "div[class*='TeamHeader']")
        entrenador_element = info_section.find_element(By.XPATH, ".//*[contains(text(), 'Entrenador')]/following-sibling::*")
        nombre_entrenador = entrenador_element.text.strip()

        try:
            bandera_element = entrenador_element.find_element(By.XPATH, ".//preceding-sibling::img[contains(@class, 'Flag')]")
            nacionalidad = bandera_element.get_attribute("alt")
        except:
            nacionalidad = "Desconocida"

        try:
            link_entrenador = entrenador_element.find_element(By.XPATH, ".//a[contains(@href, '/players/')]")
            entrenador_id = link_entrenador.get_attribute("href").split('/')[5]
        except:
            entrenador_id = None

        return {
            "entrenador_id": entrenador_id,
            "nombre": nombre_entrenador,
            "nacionalidad": nacionalidad
        }
    except Exception as e:
        print(f"Error entrenador: {e}")
        return {"entrenador_id": None, "nombre": "Desconocido", "nacionalidad": "Desconocida"}

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Scraping Goleadores, Asistentes y Estadísticas Generales**

# COMMAND ----------

def extraer_goleadores_completo(driver):
    """Extrae la tabla de goleadores"""
    goleadores = []
    try:
        driver.find_element(By.CSS_SELECTOR, "a[href*='estadisticas']").click()
        time.sleep(3)
        secciones = driver.find_elements(By.CSS_SELECTOR, "div[class*='StatsTable']")
        for seccion in secciones:
            titulo = seccion.find_element(By.CSS_SELECTOR, "h3").text.lower()
            if 'goleadores' in titulo:
                filas = seccion.find_elements(By.CSS_SELECTOR, "tr[class*='TableRow']")[1:]
                for fila in filas:
                    try:
                        celdas = fila.find_elements(By.TAG_NAME, "td")
                        try:
                            link_jugador = celdas[1].find_element(By.CSS_SELECTOR, "a[href*='/players/']")
                            jugador_id = link_jugador.get_attribute("href").split('/')[5]
                        except:
                            jugador_id = None
                        goleadores.append({
                            "jugador_id": jugador_id,
                            "nombre": celdas[1].text.strip(),
                            "equipo": celdas[2].text.strip(),
                            "goles": celdas[3].text.strip()
                        })
                    except:
                        continue
    except Exception as e:
        print(f"Error goleadores: {e}")
    return goleadores


def extraer_asistentes(driver):
    """Extrae la tabla de asistencias"""
    asistentes = []
    try:
        secciones = driver.find_elements(By.CSS_SELECTOR, "div[class*='StatsTable']")
        for seccion in secciones:
            titulo = seccion.find_element(By.CSS_SELECTOR, "h3").text.lower()
            if 'asistencias' in titulo:
                filas = seccion.find_elements(By.CSS_SELECTOR, "tr[class*='TableRow']")[1:]
                for fila in filas:
                    try:
                        celdas = fila.find_elements(By.TAG_NAME, "td")
                        try:
                            link_jugador = celdas[1].find_element(By.CSS_SELECTOR, "a[href*='/players/']")
                            jugador_id = link_jugador.get_attribute("href").split('/')[5]
                        except:
                            jugador_id = None
                        asistentes.append({
                            "jugador_id": jugador_id,
                            "nombre": celdas[1].text.strip(),
                            "equipo": celdas[2].text.strip(),
                            "asistencias": celdas[3].text.strip()
                        })
                    except:
                        continue
    except Exception as e:
        print(f"Error asistentes: {e}")
    return asistentes


def extraer_estadisticas_generales(driver):
    """Extrae estadísticas generales de la liga"""
    stats = {}
    try:
        secciones = driver.find_elements(By.CSS_SELECTOR, "div[class*='StatBox']")
        for seccion in secciones:
            try:
                titulo = seccion.find_element(By.CSS_SELECTOR, "h3").text.lower()
                valor = seccion.find_element(By.CSS_SELECTOR, "span[class*='StatValue']").text
                if 'goles' in titulo:
                    stats['goles_totales'] = valor
                elif 'posesión' in titulo:
                    stats['posesion_promedio'] = valor
                elif 'tarjetas amarillas' in titulo:
                    stats['tarjetas_amarillas_totales'] = valor
                elif 'tarjetas rojas' in titulo:
                    stats['tarjetas_rojas_totales'] = valor
                elif 'faltas' in titulo:
                    stats['faltas_totales'] = valor
            except:
                continue
    except Exception as e:
        print(f"Error estadísticas generales: {e}")
    return stats

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Función Principal y Ejecución**

# COMMAND ----------

def main():
    """Función principal que ejecuta todo el scraping de la liga y equipos"""
    print(f"Iniciando scraping temporada {temporada}, liga {liga_nombre}")

    try:
        # Scraping de liga
        datos_liga = scraping_liga_completa(temporada, liga_id, liga_nombre)
        ruta_liga = f"{raiz}{rutabase}/{temporada}/liga_completa.json"
        guardar_json(datos_liga, ruta_liga)
        print(f"Liga completada: {len(datos_liga.get('clasificacion', []))} equipos")

        # Scraping de equipos
        equipos_basicos = datos_liga.get("equipos_basicos", [])
        if equipos_basicos:
            datos_equipos = scraping_equipos_completo(equipos_basicos, temporada)
            ruta_equipos = f"{raiz}{rutabase}/{temporada}/equipos_completo.json"
            guardar_json(datos_equipos, ruta_equipos)
            print(f"Equipos completados: {len(datos_equipos.get('equipos', []))} equipos")

        print(f"Scraping temporada {temporada} COMPLETADO")

    except Exception as e:
        print(f"ERROR en scraping: {e}")
        raise e


# Ejecutar
if __name__ == "__main__":
    main()