import csv
import time
import requests
import re
from datetime import datetime
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler

# 🔐 VARIABLES (usa Railway)
import os
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

ULTIMO_ESTADO = {}
ULTIMO_RESUMEN = time.time()

# ==============================
# SERVER PARA RAILWAY
# ==============================
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")

def iniciar_servidor():
    server = HTTPServer(("0.0.0.0", 8080), HealthHandler)
    server.serve_forever()

# ==============================
# FUNCIONES
# ==============================
def obtener_html(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        return r.text
    except:
        return None

def extraer_buybox(html):
    patron = r'"bestOffer":\{.*?"salePrice":"?(\d+(?:\.\d+)?)"?.*?"sellerName":"(.*?)".*?\}'
    match = re.search(patron, html, re.DOTALL)

    if not match:
        return None, None

    return match.group(2), match.group(1)

def enviar_telegram(mensaje):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url, data={"chat_id": CHAT_ID, "text": mensaje}, timeout=10)
    except:
        pass

# ==============================
# ALERTA (INMEDIATA)
# ==============================
def alerta(sku, sku_patish, producto, seller, price):
    mensaje = f"""🚨 PERDISTE BUYBOX

🛍️ {producto}

SKU Liverpool: {sku}
SKU PATISH: {sku_patish}

🏪 Competidor: {seller}
💰 Precio: ${price}

🧠 Acción sugerida: bajar $1
"""
    enviar_telegram(mensaje)

# ==============================
# MONITOREO
# ==============================
def monitorear():
    global ULTIMO_ESTADO, ULTIMO_RESUMEN

    ganando = 0
    perdido = 0
    resumen_lineas = []

    with open("skus.csv", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            sku = row["sku"]
            url = row["url"]
            tu_seller = row["tu_nombre_seller"]
            producto = row["nombre_producto"]
            sku_patish = row["sku_patish"]

            html = obtener_html(url)
            if not html:
                continue

            seller, price = extraer_buybox(html)
            if not seller:
                continue

            if seller.lower() == tu_seller.lower():
                estado = "GANANDO"
                ganando += 1
                emoji = "🟢"
            else:
                estado = "PERDIDO"
                perdido += 1
                emoji = "🔴"

            # ALERTA SOLO SI CAMBIAS A PERDIDO
            estado_anterior = ULTIMO_ESTADO.get(sku)
            if estado == "PERDIDO" and estado_anterior == "GANANDO":
                alerta(sku, sku_patish, producto, seller, price)

            ULTIMO_ESTADO[sku] = estado

            # LINEA RESUMEN
            resumen_lineas.append(
                f"{emoji} {sku} | {seller} | ${price}"
            )

    # ==========================
    # RESUMEN CADA 15 MIN
    # ==========================
    if time.time() - ULTIMO_RESUMEN >= 900:

        fecha = datetime.now().strftime("%H:%M:%S")

        resumen = f"""📊 RESUMEN BUYBOX | {fecha}

🟢 GANANDO: {ganando}
🔴 PERDIDO: {perdido}

""" + "\n".join(resumen_lineas)

        enviar_telegram(resumen)

        ULTIMO_RESUMEN = time.time()

    # LOG MINIMO
    print(f"[{datetime.now().strftime('%H:%M:%S')}] OK | 🟢{ganando} 🔴{perdido}")

# ==============================
# MAIN
# ==============================
if __name__ == "__main__":
    print("🔥 Monitor BuyBox PRO iniciado")

    threading.Thread(target=iniciar_servidor, daemon=True).start()

    while True:
        monitorear()
        time.sleep(120)