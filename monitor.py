import csv
import time
import requests
import re
import os
import threading
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler

# ================================
# CONFIG TELEGRAM (usa variables en Railway)
# ================================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

# ================================
# HEADERS
# ================================
HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

ULTIMO_ESTADO = {}

# ================================
# TELEGRAM
# ================================
def enviar_telegram(mensaje):
    if not TELEGRAM_TOKEN or not CHAT_ID:
        print("⚠️ Telegram no configurado")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"

    try:
        requests.post(url, json={
            "chat_id": CHAT_ID,
            "text": mensaje
        }, timeout=10)
    except Exception as e:
        print(f"Error Telegram: {e}")

# ================================
# HTML
# ================================
def obtener_html(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        return r.text
    except Exception as e:
        print(f"Error HTML: {e}")
        return None

# ================================
# EXTRAER BUYBOX
# ================================
def extraer_buybox(html):
    if not html:
        return None, None

    patron = r'"sellerName":"(.*?)".*?"salePrice":"(\d+)"'
    match = re.search(patron, html)

    if not match:
        return None, None

    seller = match.group(1)
    price = match.group(2)

    return seller, price

# ================================
# MONITOREO
# ================================
def monitorear():

    ganando = 0
    perdiendo = 0
    resumen = []

    with open("skus.csv", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            sku = row["sku"]
            url = row["url"]
            tu_seller = row["tu_nombre_seller"]
            producto = row["nombre_producto"]
            sku_patish = row["sku_patish"]

            html = obtener_html(url)
            seller, price = extraer_buybox(html)

            if not seller:
                continue

            if seller.lower() == tu_seller.lower():
                estado = "GANANDO"
                ganando += 1
            else:
                estado = "PERDIDO"
                perdiendo += 1

            # ALERTA SOLO SI PIERDES
            estado_anterior = ULTIMO_ESTADO.get(sku)

            if estado_anterior == "GANANDO" and estado == "PERDIDO":
                alerta = f"""🚨 PERDISTE BUYBOX

SKU Liverpool: {sku}
SKU PATISH: {sku_patish}
Producto: {producto}
Seller: {seller}
Precio: ${price}

{url}
"""
                enviar_telegram(alerta)

            ULTIMO_ESTADO[sku] = estado

            resumen.append(f"{producto[:25]}... → {estado}")

    # ============================
    # RESUMEN TELEGRAM
    # ============================
    now = datetime.now().strftime("%H:%M:%S")

    mensaje = f"""📊 RESUMEN BUYBOX

🕒 {now}

🟢 Ganando: {ganando}
🔴 Perdidos: {perdiendo}

------------------------
"""

    for r in resumen:
        mensaje += r + "\n"

    enviar_telegram(mensaje)

    print(f"[{now}] OK | 🟢{ganando} 🔴{perdiendo}")

# ================================
# KEEP ALIVE (RAILWAY)
# ================================
PORT = int(os.getenv("PORT", "8080"))

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")

    def log_message(self, format, *args):
        return

def iniciar_servidor():
    server = HTTPServer(("0.0.0.0", PORT), Handler)
    print(f"🌐 Healthcheck activo en puerto {PORT}")
    server.serve_forever()

# ================================
# MAIN
# ================================
if __name__ == "__main__":
    print("🔥 Monitor BuyBox PRO iniciado")

    # levantar servidor para que Railway no mate el proceso
    threading.Thread(target=iniciar_servidor, daemon=True).start()

    enviar_telegram("🚀 Monitor BuyBox iniciado correctamente")

    while True:
        monitorear()
        print("⏳ Esperando 120 segundos...\n")
        time.sleep(120)