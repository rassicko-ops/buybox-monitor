import csv
import time
import requests
import re
import os
import threading
from datetime import datetime, timedelta, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler

# ================================
# CONFIG
# ================================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

ULTIMO_ESTADO = {}
ULTIMO_RESUMEN = 0

# Zona horaria CDMX
CDMX_TZ = timezone(timedelta(hours=-6))

# ================================
# TELEGRAM
# ================================
def enviar_telegram(mensaje):
    if not TELEGRAM_TOKEN or not CHAT_ID:
        print("⚠️ Telegram no configurado")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"

    try:
        requests.post(
            url,
            json={
                "chat_id": CHAT_ID,
                "text": mensaje
            },
            timeout=10
        )
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

    patron = r'"bestOffer":\{.*?"salePrice":"?(\d+(?:\.\d+)?)"?.*?"sellerName":"(.*?)".*?\}'
    match = re.search(patron, html, re.DOTALL)

    if not match:
        return None, None

    price = match.group(1)
    seller = match.group(2)

    return seller, price

# ================================
# HEALTHCHECK RAILWAY
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
# RESUMEN / MONITOREO
# ================================
def monitorear():
    global ULTIMO_ESTADO, ULTIMO_RESUMEN

    ganando = []
    perdiendo = []

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
                ganando.append(f"• {producto[:32]} → ${price}")
            else:
                estado = "PERDIDO"
                perdiendo.append(f"• {producto[:28]} → {seller} → ${price}")

            estado_anterior = ULTIMO_ESTADO.get(sku)

            # ALERTA inmediata solo si cambias de GANANDO a PERDIDO
            if estado_anterior == "GANANDO" and estado == "PERDIDO":
                alerta = f"""🚨 PERDISTE BUYBOX

Producto: {producto}
SKU Liverpool: {sku}
SKU PATISH: {sku_patish}
Seller: {seller}
Precio: ${price}

{url}
"""
                enviar_telegram(alerta)

            ULTIMO_ESTADO[sku] = estado

    # Hora local CDMX
    now_cdmx = datetime.now(CDMX_TZ)
    now_str = now_cdmx.strftime("%H:%M:%S")

    # resumen solo cada 15 minutos = 900 segundos
    if time.time() - ULTIMO_RESUMEN >= 900:
        mensaje = f"""📊 RESUMEN BUYBOX

🕒 {now_str}

🟢 Ganando: {len(ganando)}
🔴 Perdidos: {len(perdiendo)}
"""

        if ganando:
            mensaje += "\n🟢 GANANDO\n" + "\n".join(ganando) + "\n"

        if perdiendo:
            mensaje += "\n🔴 PERDIDO\n" + "\n".join(perdiendo) + "\n"

        enviar_telegram(mensaje)
        ULTIMO_RESUMEN = time.time()

    print(f"[{now_str}] OK | 🟢{len(ganando)} 🔴{len(perdiendo)}")

# ================================
# MAIN
# ================================
if __name__ == "__main__":
    print("🔥 Monitor BuyBox PRO iniciado")
    threading.Thread(target=iniciar_servidor, daemon=True).start()

    # mensaje de arranque solo una vez
    enviar_telegram("🚀 Monitor BuyBox iniciado correctamente")

    while True:
        monitorear()
        print("⏳ Esperando 120 segundos...\n")
        time.sleep(10)