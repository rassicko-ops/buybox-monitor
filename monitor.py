import csv
import time
import requests
import re
from datetime import datetime

# 🔐 TUS DATOS
TELEGRAM_TOKEN = "TU_TOKEN_NUEVO_AQUI"
CHAT_ID = "2057493748"

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

ULTIMO_ESTADO = {}
ARCHIVO_HISTORIAL = "historial_buybox.csv"


def obtener_html(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
        return r.text
    except Exception as e:
        print(f"Error al abrir URL: {e}")
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


def enviar_telegram(mensaje):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {
            "chat_id": CHAT_ID,
            "text": mensaje
        }
        r = requests.post(url, data=payload, timeout=20)
        print("Telegram status:", r.status_code)
        print("Telegram response:", r.text)
    except Exception as e:
        print(f"Error enviando Telegram: {e}")


def alerta(sku_liverpool, sku_patish, producto, url, seller, price):
    mensaje = f"""🚨 PERDISTE BUYBOX

SKU Liverpool: {sku_liverpool}
SKU PATISH: {sku_patish}
Producto: {producto}
Seller: {seller}
Precio: ${price}

{url}
"""
    print(mensaje)
    enviar_telegram(mensaje)


def inicializar_historial():
    try:
        with open(ARCHIVO_HISTORIAL, "x", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "fecha_hora",
                "sku_liverpool",
                "sku_patish",
                "producto",
                "seller_buybox",
                "precio_buybox",
                "estado",
                "url"
            ])
    except FileExistsError:
        pass


def guardar_historial(sku_liverpool, sku_patish, producto, seller, price, estado, url):
    fecha_hora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with open(ARCHIVO_HISTORIAL, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            fecha_hora,
            sku_liverpool,
            sku_patish,
            producto,
            seller,
            price,
            estado,
            url
        ])


def monitorear():
    global ULTIMO_ESTADO

    with open("skus.csv", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            sku_liverpool = row["sku"]
            url = row["url"]
            tu_seller = row["tu_nombre_seller"]
            producto = row["nombre_producto"]
            sku_patish = row["sku_patish"]

            print(f"\nRevisando {sku_liverpool} - {producto}...")

            html = obtener_html(url)
            if not html:
                continue

            seller, price = extraer_buybox(html)

            if not seller:
                print("No se pudo detectar buybox")
                continue

            print(f"Buybox: {seller} | ${price} | {producto}")

            if seller.lower() == tu_seller.lower():
                estado_actual = "GANANDO"
            else:
                estado_actual = "PERDIDO"

            estado_anterior = ULTIMO_ESTADO.get(sku_liverpool)

            print(f"Estado: {estado_actual}")

            guardar_historial(
                sku_liverpool=sku_liverpool,
                sku_patish=sku_patish,
                producto=producto,
                seller=seller,
                price=price,
                estado=estado_actual,
                url=url
            )

            if estado_actual == "PERDIDO" and estado_anterior == "GANANDO":
                alerta(sku_liverpool, sku_patish, producto, url, seller, price)

            ULTIMO_ESTADO[sku_liverpool] = estado_actual


if __name__ == "__main__":
    print("🔥 Monitor REAL de BuyBox iniciado")
    inicializar_historial()

    while True:
        monitorear()
        print("\nEsperando 120 segundos...\n")
        time.sleep(120)