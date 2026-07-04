"""
Script para descargar fotos de jugadores Liga 1 y convertirlas a base64.
Ejecutar en Windows con: python descargar_fotos_base64.py
Requiere: pip install requests
"""
import csv
import base64
import time
import os
import requests

# ============================================================
# PASO 1: Edita esta ruta al CSV de jugadores_slots que tienes
# ============================================================
CSV_INPUT = r"C:\Users\milu_\Downloads\jugadores_slots.csv"  # <-- ajusta la ruta
CSV_OUTPUT = r"C:\Users\milu_\Downloads\fotos_base64.csv"

HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/120.0.0.0 Safari/537.36'
    ),
    'Referer': 'https://www.transfermarkt.com/',
    'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
}

DELAY_SECONDS = 0.4   # pausa entre peticiones
TIMEOUT       = 12    # segundos máximo por imagen

def url_to_base64(url: str) -> str | None:
    try:
        resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        if resp.status_code == 200:
            ext = 'png' if url.lower().endswith('.png') else 'jpeg'
            b64 = base64.b64encode(resp.content).decode('utf-8')
            return f"data:image/{ext};base64,{b64}"
        else:
            print(f"  HTTP {resp.status_code}")
            return None
    except Exception as e:
        print(f"  Error: {e}")
        return None

def main():
    # Leer jugadores únicos del CSV
    print("Leyendo CSV...")
    jugadores = {}  # id_jugador -> (jugador, foto_url)
    with open(CSV_INPUT, encoding='utf-8') as f:
        for row in csv.DictReader(f):
            jid = row['id_jugador']
            if jid not in jugadores and row.get('foto_url'):
                jugadores[jid] = (row['jugador'], row['foto_url'])

    print(f"Jugadores únicos con foto: {len(jugadores)}")

    # Descargar y convertir
    results = []
    for i, (jid, (nombre, url)) in enumerate(jugadores.items(), 1):
        print(f"[{i}/{len(jugadores)}] {nombre}...", end=" ")
        b64 = url_to_base64(url)
        if b64:
            print("OK")
            results.append({'id_jugador': jid, 'foto_base64': b64})
        else:
            # Fallback: guardar URL original si no se pudo descargar
            results.append({'id_jugador': jid, 'foto_base64': url})
            print("FALLBACK (URL original)")
        time.sleep(DELAY_SECONDS)

    # Guardar CSV resultado
    with open(CSV_OUTPUT, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['id_jugador', 'foto_base64'])
        writer.writeheader()
        writer.writerows(results)

    ok = sum(1 for r in results if r['foto_base64'].startswith('data:'))
    print(f"\nGuardado: {CSV_OUTPUT}")
    print(f"  Con base64: {ok}")
    print(f"  Con URL (fallback): {len(results) - ok}")
    print("\nSiguiente paso: sube fotos_base64.csv a Da