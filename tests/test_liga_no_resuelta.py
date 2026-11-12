"""Pruebas del fix de falsos "PRODUCTO_NO_EXISTE" (404) del monitor BuyBox.

monitor.py se importa aislado: carpeta de trabajo temporal (no lee el .env real), DATA_DIR
temporal y requests simulado -- ninguna prueba sale a Liverpool ni a Telegram.
Correr con: python3 -m unittest discover -s tests -p "test_*.py"
"""
import importlib
import json
import os
import re
import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from unittest import mock

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TMP = tempfile.mkdtemp()


def cargar_monitor():
    anterior = os.getcwd()
    os.chdir(TMP)
    try:
        os.environ.update({
            "DATA_DIR": TMP, "DISABLE_TELEGRAM": "1", "LIVERPOOL_API_KEY": "",
            "TELEGRAM_TOKEN": "", "CHAT_ID": "",
        })
        if REPO not in sys.path:
            sys.path.insert(0, REPO)
        sys.modules.pop("monitor", None)
        return importlib.import_module("monitor")
    finally:
        os.chdir(anterior)


monitor = cargar_monitor()


class Resp:
    def __init__(self, status=200, text="", url=""):
        self.status_code = status
        self.text = text
        self.url = url


def pagina(sku, ofertas=(("PATISH", "2370", 5000),), ganador="PATISH"):
    """PDP como lo manda Liverpool hoy: JSON dentro de self.__next_f.push + "Vendido por"."""
    lista = ",".join(
        '{"sellerId":"%s","sellerName":"%s","x":1,"priceInfo":{"salePrice":%d}}' % (sid, nombre, precio)
        for nombre, sid, precio in ofertas
    )
    datos = '{"skuId":"%s","offers":[%s],"bestOffer":{"sellerName":"%s"}}' % (sku, lista, ganador)
    return (f"<html><script>self.__next_f.push([1,{json.dumps(datos)}])</script>"
            f'<p>Vendido por:</p><a aria-label="{ganador}">{ganador}</a></html>')


def pagina_otro_producto():
    datos = '{"skuId":"1111111111","offers":[]}'
    return f"<html><script>self.__next_f.push([1,{json.dumps(datos)}])</script></html>"


class FakeLiverpool:
    """Sitio publico simulado: paginas por product_id y redirects del buscador por termino."""

    def __init__(self, paginas=None, busquedas=None):
        self.paginas = paginas or {}
        self.busquedas = busquedas or {}
        self.llamadas = []

    def get(self, url, params=None, headers=None, timeout=None, allow_redirects=True):
        self.llamadas.append(url if not params else f"{url}?s={params.get('s')}")
        if url == "https://www.liverpool.com.mx/tienda":
            destino = self.busquedas.get(params.get("s"))
            if destino:
                return Resp(200, "<html>pdp</html>", f"https://www.liverpool.com.mx/tienda/pdp/algo/{destino}")
            return Resp(200, "<html>resultados</html>", f"https://www.liverpool.com.mx/tienda?s={params.get('s')}")
        m = re.fullmatch(r"https://www\.liverpool\.com\.mx/tienda/pdp/producto/(\d+)\?skuid=(\d+)", url)
        if not m:
            raise AssertionError(f"request inesperado: {url}")
        html = self.paginas.get(m.group(1))
        return Resp(200, html, url) if html is not None else Resp(404, "", url)

    def busquedas_hechas(self):
        return [u for u in self.llamadas if "?s=" in u]


def item(sku_patish="4200986", sku_liverpool="1184988743", product_id=None, vgc="1186306739", ean=""):
    product_id = product_id or sku_liverpool
    return {
        "sku_patish": sku_patish, "offer_sku_raw": sku_patish, "sku_liverpool": sku_liverpool,
        "product_id": product_id, "vgc": vgc, "ean": ean, "producto": "Consola Nintendo Switch",
        "estado_oferta": "ACTIVA", "motivo": "", "precio_base": 5000, "cantidad": 38,
        "url": monitor.liga_pdp(product_id, sku_liverpool), "color": "", "size": "",
    }


class BaseLiga(unittest.TestCase):
    def setUp(self):
        monitor.VGC_OVERRIDES.clear()
        monitor.VGC_RESOLVER_FALLIDOS.clear()
        monitor.LIGA_PRESUPUESTO["restante"] = 5
        self.sitio = FakeLiverpool()
        sin_post = mock.Mock(side_effect=AssertionError("no debe haber POST"))
        self.parches = [
            mock.patch.object(monitor.requests, "get", side_effect=self.sitio.get),
            mock.patch.object(monitor.requests, "post", sin_post),
        ]
        for parche in self.parches:
            parche.start()

    def tearDown(self):
        for parche in self.parches:
            parche.stop()

    def procesar(self, *items):
        return monitor._procesar_grupo_producto(items[0]["product_id"], list(items))


class LigaNoResueltaTests(BaseLiga):
    def test_pagina_normal_sigue_dando_ganando_con_precio(self):
        it = item()
        self.sitio.paginas["1184988743"] = pagina("1184988743")
        r = self.procesar(it)[0]
        self.assertEqual(r["nuevo_estado"], "GANANDO_VERIFICADO")
        self.assertEqual(r["price"], "5000")

    def test_404_de_oferta_activa_ya_no_es_producto_no_existe(self):
        it = item()
        r = self.procesar(it)[0]
        self.assertEqual(r["nuevo_estado"], "LIGA_NO_RESUELTA")
        self.assertIn("Activa en Liverpool", r["error_message"])
        self.assertIn("se reintenta", r["error_message"])
        self.assertIn("4200986", monitor.VGC_RESOLVER_FALLIDOS)

    def test_liga_fallida_no_se_vuelve_a_pedir_hasta_el_reintento(self):
        it = item()
        self.procesar(it)
        llamadas = len(self.sitio.llamadas)
        r = self.procesar(it)[0]
        self.assertEqual(len(self.sitio.llamadas), llamadas, "no debe volver a pedir la liga rota")
        self.assertEqual(r["nuevo_estado"], "LIGA_NO_RESUELTA")
        monitor.VGC_RESOLVER_FALLIDOS["4200986"]["ts"] -= timedelta(minutes=monitor.LIGA_REINTENTO_MINUTOS + 1)
        self.procesar(it)
        self.assertGreater(len(self.sitio.llamadas), llamadas, "pasado el reintento si se vuelve a intentar")

    def test_override_viejo_se_corrige_aunque_ya_exista(self):
        # Antes: si el SKU ya tenia override, nunca se volvia a resolver -> 404 para siempre.
        monitor.VGC_OVERRIDES["4200986"] = "99984773855"
        it = item(product_id="99984773855")
        self.sitio.paginas["1184988743"] = pagina("1184988743")
        r = self.procesar(it)[0]
        self.assertEqual(r["nuevo_estado"], "GANANDO_VERIFICADO")
        self.assertNotIn("4200986", monitor.VGC_OVERRIDES, "el override viejo sobra si sirve el product_sku")
        self.assertEqual(it["url"], monitor.liga_pdp("1184988743", "1184988743"))
        with open(monitor.VGC_OVERRIDES_FILE, encoding="utf-8") as archivo:
            self.assertNotIn("4200986", json.load(archivo))

    def test_resuelve_con_multioffersku(self):
        it = item()
        self.sitio.paginas["1186306739"] = pagina("1184988743")
        r = self.procesar(it)[0]
        self.assertEqual(r["nuevo_estado"], "GANANDO_VERIFICADO")
        self.assertEqual(monitor.VGC_OVERRIDES["4200986"], "1186306739")
        self.assertEqual(it["product_id"], "1186306739")
        self.assertEqual(self.sitio.busquedas_hechas(), [], "si un ID conocido sirve, no se usa el buscador")

    def test_buscador_solo_cuenta_si_la_pagina_trae_este_sku(self):
        it = item()
        self.sitio.busquedas["1184988743"] = "999000111"
        self.sitio.paginas["999000111"] = pagina_otro_producto()
        r = self.procesar(it)[0]
        self.assertEqual(r["nuevo_estado"], "LIGA_NO_RESUELTA")
        self.assertNotIn("4200986", monitor.VGC_OVERRIDES)

    def test_resuelve_por_buscador_con_sku(self):
        it = item()
        self.sitio.busquedas["1184988743"] = "999000333"
        self.sitio.paginas["999000333"] = pagina("1184988743", ofertas=(("TELMOV", "9", 4990), ("PATISH", "2370", 5000)), ganador="TELMOV")
        r = self.procesar(it)[0]
        self.assertEqual(r["nuevo_estado"], "PERDIDO")
        self.assertEqual(monitor.VGC_OVERRIDES["4200986"], "999000333")

    def test_resuelve_por_codigo_de_barras(self):
        it = item(ean="0045496453435")
        self.sitio.busquedas["0045496453435"] = "999000222"
        self.sitio.paginas["999000222"] = pagina("1184988743")
        r = self.procesar(it)[0]
        self.assertEqual(r["nuevo_estado"], "GANANDO_VERIFICADO")
        self.assertEqual(monitor.VGC_OVERRIDES["4200986"], "999000222")

    def test_pagina_de_otro_producto_tambien_dispara_la_correccion(self):
        # Caso S25 Ultra: la pagina abre (200) pero ya no trae nuestro SKU.
        it = item(product_id="1170197151")
        self.sitio.paginas["1170197151"] = pagina_otro_producto()
        self.sitio.paginas["1186306739"] = pagina("1184988743")
        r = self.procesar(it)[0]
        self.assertEqual(r["nuevo_estado"], "GANANDO_VERIFICADO")
        self.assertEqual(it["product_id"], "1186306739")

    def test_pagina_sin_sku_y_sin_solucion_queda_liga_no_resuelta(self):
        it = item(product_id="1170197151")
        self.sitio.paginas["1170197151"] = pagina_otro_producto()
        r = self.procesar(it)[0]
        self.assertEqual(r["nuevo_estado"], "LIGA_NO_RESUELTA")
        self.assertIn("PDP sin este SKU", r["error_message"])

    def test_tope_de_busquedas_por_ciclo(self):
        monitor.LIGA_PRESUPUESTO["restante"] = 1
        a = item(sku_patish="A", sku_liverpool="1000000001", vgc="2000000001")
        b = item(sku_patish="B", sku_liverpool="1000000002", vgc="2000000002")
        resultados = monitor._procesar_grupo_producto("x", [a, b])
        self.assertEqual([r["nuevo_estado"] for r in resultados], ["LIGA_NO_RESUELTA"] * 2)
        self.assertEqual(len(self.sitio.busquedas_hechas()), 1)
        self.assertIn("A", monitor.VGC_RESOLVER_FALLIDOS)
        self.assertNotIn("B", monitor.VGC_RESOLVER_FALLIDOS, "sin turno no se marca: reintenta el siguiente ciclo")


class PanelYApiTests(BaseLiga):
    def test_liga_sin_verificar_no_cuenta_como_no_visible(self):
        items = [
            {"estado": "GANANDO_VERIFICADO", "stock_tuyo": 5},
            {"estado": "LIGA_NO_RESUELTA", "stock_tuyo": 38},
            {"estado": "SIN_DATOS", "stock_tuyo": 2},
        ]
        rv = monitor.calcular_resumen_visibilidad(items)
        self.assertEqual(rv, {"con_stock": 3, "visibles": 1, "no_visibles": 1, "sin_verificar": 1, "no_ofertando": 0})

    def test_product_id_desde_liga(self):
        casos = {
            "https://www.liverpool.com.mx/tienda/pdp/samsung-galaxy-s25/1180928291?skuid=1177033118": "1180928291",
            "https://www.liverpool.com.mx/tienda/pdp/producto/99984773855?skuid=1": "99984773855",
            "https://www.liverpool.com.mx/tienda/pdp/consola-switch/1184988743": "1184988743",
            " 1180928291 ": "1180928291",
            "https://www.liverpool.com.mx/tienda?s=1184988743": "",
            "hola": "",
            None: "",
        }
        for texto, esperado in casos.items():
            with self.subTest(texto=texto):
                self.assertEqual(monitor.product_id_desde_liga(texto), esperado)

    def test_pegar_liga_desde_el_panel(self):
        it = item(sku_patish="4200837/4200808/4200084 Switch 2", sku_liverpool="1219121225")
        monitor.CATALOGO[:] = [it]
        monitor.VGC_RESOLVER_FALLIDOS[it["sku_patish"]] = {"ts": datetime.now(monitor.CDMX_TZ), "url": it["url"], "meta": {}}
        cliente = monitor.app.test_client()
        ruta = "/api/sku/4200837%2F4200808%2F4200084%20Switch%202/liga"
        resp = cliente.post(ruta, json={"url": "https://www.liverpool.com.mx/tienda/pdp/nintendo-switch-2/999700788999?skuid=1219121225"})
        self.assertEqual(resp.status_code, 200, resp.get_json())
        self.assertEqual(monitor.VGC_OVERRIDES[it["sku_patish"]], "999700788999")
        self.assertEqual(it["url"], monitor.liga_pdp("999700788999", "1219121225"))
        self.assertNotIn(it["sku_patish"], monitor.VGC_RESOLVER_FALLIDOS, "liga nueva: se verifica ya, sin esperar")
        self.assertEqual(cliente.post(ruta, json={"url": "hola"}).status_code, 400)
        self.assertEqual(cliente.post("/api/sku/NOEXISTE/liga", json={"url": "1180928291"}).status_code, 404)

    def test_catalogo_api_guarda_codigo_de_barras(self):
        base = {"offer_sku": "4201283-Azul 256GB", "product_sku": "1177033118", "state_code": "ACTIVE",
                "quantity": 36, "multiOfferSku": "1180928291", "product_title": "Galaxy S25+"}
        con = dict(base, product_references=[{"reference": "8806095867113", "reference_type": "Galaxy S25+"}])
        sin = dict(base, product_references=[{"reference": "S25-AZUL", "reference_type": "x"}])
        self.assertEqual(monitor.procesar_catalogo_api([con])[0]["ean"], "8806095867113")
        self.assertEqual(monitor.procesar_catalogo_api([sin])[0]["ean"], "")

    def test_sin_datos_actualiza_el_motivo_pero_no_la_ultima_revision_buena(self):
        it = item()
        monitor.CATALOGO[:] = [it]
        sku = it["sku_patish"]
        monitor.ULTIMO_ESTADO[sku] = "SIN_DATOS"
        monitor.ULTIMO_LAST_CHECKED[sku] = "2026-10-08 11:16:55"
        monitor.ULTIMO_STATUS_CODE[sku] = 404
        monitor.ULTIMO_ERROR_MESSAGE[sku] = "PDP 404"
        resultado = {
            "item": it, "sku_patish": sku, "nuevo_estado": "SIN_DATOS", "seller": "", "price": "",
            "otros": [], "precio_mio": "", "stock_mio": None, "stock_ganador": None,
            "segundo_seller": "", "segundo_precio": "", "color_nuevo": "", "size_nuevo": "",
            "url_nuevo": None, "source": "PDP", "status_code": 503, "error_message": "HTTP 503", "confidence": "",
        }
        with mock.patch.object(monitor, "_procesar_grupo_producto", return_value=[resultado]):
            monitor.monitorear()
        self.assertEqual(monitor.ULTIMO_ERROR_MESSAGE[sku], "HTTP 503")
        self.assertEqual(monitor.ULTIMO_STATUS_CODE[sku], 503)
        self.assertEqual(monitor.ULTIMO_LAST_CHECKED[sku], "2026-10-08 11:16:55")

    def test_monitorear_deja_liga_no_resuelta_con_accion_clara(self):
        it = item()
        monitor.CATALOGO[:] = [it]
        monitor.monitorear()
        fila = monitor.construir_items_estado()[0]
        self.assertEqual(fila["estado"], "LIGA_NO_RESUELTA")
        self.assertIn("pega su liga", fila["accion_recomendada"])
        self.assertEqual(fila["stock_tuyo"], 38)


if __name__ == "__main__":
    unittest.main()
