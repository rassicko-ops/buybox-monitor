import csv
import time
import requests
import re
from datetime import datetime
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler

# 🔐 TUS DATOS
TELEGRAM_TOKEN = "TU_TOKEN_NUEVO_AQUI"
CHAT_ID = "2057493748"

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

ULTIMO_ESTADO = {}
ARCHIVO_HISTORIAL = "historial_buybox.csv"


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(b"OK")

    def log_message(self, format, *args):
        return


def iniciar_servidor():
    server = HTTPServer(("0.0.0.0", 8080), HealthHandler)
    print("🌐 Healthcheck activo en puerto 8080")
    server.serve_forever()


def obtener_html(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
        return r.text
    except Exception as e:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Error al abrir URL: {e}")
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
        requests.post(url, data=payload, timeout=20)
    except Exception as e:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Error enviando Telegram: {e}")


def alerta(sku_liverpool, sku_patish, producto, url, seller, price):
    mensaje = f"""🚨 PERDISTE BUYBOX

Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
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


def imprimir_bloque(fecha_hora, sku_liverpool, sku_patish, producto, seller, price, estado):
    print("\n" + "=" * 60)
    print(f"[{fecha_hora}]")
    print(f"SKU Liverpool: {sku_liverpool}")
    print(f"SKU PATISH: {sku_patish}")
    print(f"Producto: {producto}")
    print(f"Buybox: {seller}")
    print(f"Precio: ${price}")
    print(f"Estado: {estado}")
    print("=" * 60)


def monitorear():
    global ULTIMO_ESTADO

    with open("skus.csv", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            fecha_hora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            sku_liverpool = row["sku"]
            url = row["url"]
            tu_seller = row["tu_nombre_seller"]
            producto = row["nombre_producto"]
            sku_patish = row["sku_patish"]

            html = obtener_html(url)
            if not html:
                continue

            seller, price = extraer_buybox(html)

            if not seller:
                print(f"\n[{fecha_hora}]")
                print(f"SKU Liverpool: {sku_liverpool}")
                print(f"Producto: {producto}")
                print("No se pudo detectar buybox")
                print("-" * 60)
                continue

            if seller.lower() == tu_seller.lower():
                estado_actual = "GANANDO"
            else:
                estado_actual = "PERDIDO"

            imprimir_bloque(
                fecha_hora=fecha_hora,
                sku_liverpool=sku_liverpool,
                sku_patish=sku_patish,
                producto=producto,
                seller=seller,
                price=price,
                estado=estado_actual
            )

            guardar_historial(
                sku_liverpool=sku_liverpool,
                sku_patish=sku_patish,
                producto=producto,
                seller=seller,
                price=price,
                estado=estado_actual,
                url=url
            )

            estado_anterior = ULTIMO_ESTADO.get(sku_liverpool)

            if estado_actual == "PERDIDO" and estado_anterior == "GANANDO":
                alerta(sku_liverpool, sku_patish, producto, url, seller, price)

            ULTIMO_ESTADO[sku_liverpool] = estado_actual


if __name__ == "__main__":
    print("🔥 Monitor REAL de BuyBox iniciado")
    inicializar_historial()

    threading.Thread(target=iniciar_servidor, daemon=True).start()

    while True:
        print("\n" + "#" * 60)
        print(f"⏱️ CICLO INICIADO: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("#" * 60)

        monitorear()

        print(f"\n⏳ Esperando 120 segundos...")
        time.sleep(120)