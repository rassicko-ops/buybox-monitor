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

CDMX_TZ = timezone(timedelta(hours=-6))

ULTIMO_ESTADO = {}
ULTIMO_PRECIO = {}
ULTIMO_SELLER = {}

ULTIMO_RESUMEN = 0
ULTIMA_FECHA_CSV = None

CSV_FILE = "historico_buybox.csv"

# ================================
# TELEGRAM
# ================================
def enviar_telegram(mensaje):
    if not TELEGRAM_TOKEN or not CHAT_ID:
        print("⚠️ Falta TELEGRAM_TOKEN o CHAT_ID")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"

    try:
        requests.post(
            url,
            json={"chat_id": CHAT_ID, "text": mensaje},
            timeout=15
        )
    except Exception as e:
        print(f"Error Telegram: {e}")


def enviar_csv_telegram():
    if not TELEGRAM_TOKEN or not CHAT_ID:
        print("⚠️ Falta TELEGRAM_TOKEN o CHAT_ID")
        return

    if not os.path.exists(CSV_FILE):
        print("⚠️ No existe CSV para enviar")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendDocument"

    try:
        with open(CSV_FILE, "rb") as f:
            requests.post(
                url,
                data={"chat_id": CHAT_ID},
                files={"document": f},
                timeout=30
            )
        print("📎 CSV enviado por Telegram")
    except Exception as e:
        print(f"Error enviando CSV: {e}")

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
# HTML / BUYBOX
# ================================
def obtener_html(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        return r.text
    except Exception as e:
        print(f"Error HTML: {e}")
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

# ================================
# CSV HISTÓRICO
# ================================
def inicializar_csv():
    if not os.path.exists(CSV_FILE):
        with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "fecha_hora",
                "sku_liverpool",
                "sku_patish",
                "producto",
                "seller_ganador",
                "precio",
                "estado",
                "tipo_cambio",
                "url"
            ])
        print("📝 CSV histórico inicializado")


def guardar_evento_csv(fecha_hora, sku, sku_patish, producto, seller, price, estado, tipo_cambio, url):
    with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            fecha_hora,
            sku,
            sku_patish,
            producto,
            seller,
            price,
            estado,
            tipo_cambio,
            url
        ])

# ================================
# ALERTA DE PÉRDIDA
# ================================
def enviar_alerta_perdida(producto, sku, sku_patish, seller, price, url):
    mensaje = f"""🚨 PERDISTE BUYBOX

Producto: {producto}
SKU Liverpool: {sku}
SKU PATISH: {sku_patish}
Seller: {seller}
Precio: ${price}

{url}
"""
    enviar_telegram(mensaje)

# ================================
# MONITOREO
# ================================
def monitorear():
    global ULTIMO_RESUMEN, ULTIMA_FECHA_CSV

    ganando = []
    perdiendo = []

    now_cdmx = datetime.now(CDMX_TZ)
    now_str = now_cdmx.strftime("%H:%M:%S")
    fecha_hora = now_cdmx.strftime("%Y-%m-%d %H:%M:%S")

    with open("skus.csv", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        reader.fieldnames = [campo.strip().lower() for campo in reader.fieldnames]

        for row in reader:
            sku = row["sku"].strip()
            url = row["url"].strip()
            tu_seller = row["tu_nombre_seller"].strip()
            producto = row["nombre_producto"].strip()
            sku_patish = row["sku_patish"].strip()

            html = obtener_html(url)
            seller, price = extraer_buybox(html)

            if not seller or not price:
                continue

            if seller.lower() == tu_seller.lower():
                estado = "GANANDO"
                ganando.append(f"• {producto[:30]} → ${price}")
            else:
                estado = "PERDIDO"
                perdiendo.append(f"• {producto[:24]} → {seller} → ${price}")

            estado_anterior = ULTIMO_ESTADO.get(sku)
            precio_anterior = ULTIMO_PRECIO.get(sku)
            seller_anterior = ULTIMO_SELLER.get(sku)

            tipo_cambio = []

            if estado_anterior is None:
                tipo_cambio.append("INICIAL")

            if estado_anterior is not None and estado != estado_anterior:
                tipo_cambio.append("CAMBIO_ESTADO")

            if precio_anterior is not None and str(price) != str(precio_anterior):
                tipo_cambio.append("CAMBIO_PRECIO")

            if seller_anterior is not None and seller != seller_anterior:
                tipo_cambio.append("CAMBIO_SELLER")

            if tipo_cambio:
                guardar_evento_csv(
                    fecha_hora=fecha_hora,
                    sku=sku,
                    sku_patish=sku_patish,
                    producto=producto,
                    seller=seller,
                    price=price,
                    estado=estado,
                    tipo_cambio=" | ".join(tipo_cambio),
                    url=url
                )

            if estado_anterior == "GANANDO" and estado == "PERDIDO":
                enviar_alerta_perdida(producto, sku, sku_patish, seller, price, url)

            ULTIMO_ESTADO[sku] = estado
            ULTIMO_PRECIO[sku] = price
            ULTIMO_SELLER[sku] = seller

    # Resumen cada 15 min
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

    # CSV diario a las 10:00 AM CDMX
    hora_actual = now_cdmx.hour
    minuto_actual = now_cdmx.minute
    fecha_actual = now_cdmx.strftime("%Y-%m-%d")

    if hora_actual == 10 and minuto_actual <= 2:
        if ULTIMA_FECHA_CSV != fecha_actual:
            print("📎 Enviando CSV diario...")
            enviar_csv_telegram()
            ULTIMA_FECHA_CSV = fecha_actual

    print(f"[{now_str}] OK | 🟢{len(ganando)} 🔴{len(perdiendo)}")

# ================================
# MAIN
# ================================
if __name__ == "__main__":
    print("🔥 Monitor BuyBox PRO iniciado")
    inicializar_csv()

    threading.Thread(target=iniciar_servidor, daemon=True).start()

    enviar_telegram("🚀 Monitor BuyBox iniciado correctamente")

    while True:
        monitorear()
        print("⏳ Esperando 120 segundos...\n")
        time.sleep(120)