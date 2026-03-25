import csv
import time
import requests
import re
import os

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

ULTIMO_ESTADO = {}

def obtener_html(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
        return r.text
    except Exception as e:
        print(f"Error: {e}")
        return None

def extraer_buybox(html):
    if not html:
        return None, None

    patron = r'"bestOffer":\{.*?"salePrice":"?(\d+(?:\.\d+)?)"?.*?"sellerName":"(.*?)".*?\}'
    match = re.search(patron, html, re.DOTALL)

    if not match:
        return None, None

    price = match.group(1)
    seller = match.group(2)

    return seller, price
    if not html:
        return None, None

    # Buscar bloque bestOffer
    match = re.search(r'"bestOffer":\{(.*?)\}', html)

    if not match:
        return None, None

    bloque = match.group(1)

    # Seller
    seller_match = re.search(r'"sellerName":"(.*?)"', bloque)
    seller = seller_match.group(1) if seller_match else None

    # Precio
    price_match = re.search(r'"salePrice":"?(\d+(?:\.\d+)?)"?', bloque)
    price = price_match.group(1) if price_match else None

    return seller, price

def alerta(sku, url, seller, price):
    print(f"\n🚨 PERDISTE BUYBOX en {sku}")
    print(f"Nuevo seller: {seller}")
    print(f"Precio: ${price}")
    print(url)
    os.system('say "Perdiste el buybox"')

def monitorear():
    global ULTIMO_ESTADO

    with open("skus.csv", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            sku = row["sku"]
url = row["url"]
tu_seller = row["tu_nombre_seller"]
producto = row["nombre_producto"]
sku_patish = row["sku_patish"]

            print(f"\nRevisando {sku}...")

            html = obtener_html(url)
            if not html:
                continue

            seller, price = extraer_buybox(html)

            if not seller:
                print("No se pudo detectar buybox")
                continue

            print(f"Buybox: {seller} | ${price}")

            if seller.lower() == tu_seller.lower():
                estado_actual = "GANANDO"
            else:
                estado_actual = "PERDIDO"

            estado_anterior = ULTIMO_ESTADO.get(sku)

            print(f"Estado: {estado_actual}")

            if estado_anterior == "GANANDO" and estado_actual == "PERDIDO":
                alerta(sku, url, seller, price)

            ULTIMO_ESTADO[sku] = estado_actual

if __name__ == "__main__":
    print("🔥 Monitor REAL de BuyBox iniciado")
    while True:
        monitorear()
        print("\nEsperando 120 segundos...\n")
        time.sleep(120)
