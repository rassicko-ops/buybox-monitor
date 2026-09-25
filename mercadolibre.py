#!/usr/bin/env python3
"""Integracion Mercado Libre (MLM) -- BuyBox Monitor, preguntas, Full.

Modulo TOTALMENTE separado de monitor.py (que es 100% Liverpool, en
produccion) -- se registra como Blueprint de Flask para no arriesgar nada
de la logica que ya esta corriendo. Cuenta vendedora FABRICA.DIRECTO, sitio
MLM. No comparte tokens con ningun otro sistema (ML rota el refresh_token
en cada renovacion -- solo sirve una vez, compartirlo desconecta al otro).

Variables de entorno requeridas (las pone el usuario, nunca hardcodeadas):
  MELI_CLIENT_ID, MELI_CLIENT_SECRET, MELI_REDIRECT_URI, MELI_SITE_ID (default MLM)
Opcionales:
  ML_BUYBOX_INTERVAL_MIN (default 15), ML_UMBRAL_PRICE_DIFF (default 0.05 = 5%),
  ML_UMBRAL_LOW_STOCK (default 3)
Reusa TELEGRAM_TOKEN / CHAT_ID que ya existen para el monitor de Liverpool --
mismo bot, los mensajes de ML llevan su propio prefijo para no confundirse.
"""
import json
import os
import sqlite3
import threading
import time
from datetime import datetime, timedelta, timezone

import requests
from flask import Blueprint, jsonify, request, redirect

# ================================
# CONFIG
# ================================
MELI_CLIENT_ID = os.getenv("MELI_CLIENT_ID", "").strip()
MELI_CLIENT_SECRET = os.getenv("MELI_CLIENT_SECRET", "").strip()
MELI_REDIRECT_URI = os.getenv("MELI_REDIRECT_URI", "").strip()
MELI_SITE_ID = os.getenv("MELI_SITE_ID", "MLM").strip()
ML_OK = bool(MELI_CLIENT_ID and MELI_CLIENT_SECRET and MELI_REDIRECT_URI)

ML_BUYBOX_INTERVAL_MIN = int(os.getenv("ML_BUYBOX_INTERVAL_MIN", "15"))
ML_UMBRAL_PRICE_DIFF = float(os.getenv("ML_UMBRAL_PRICE_DIFF", "0.05"))
ML_UMBRAL_LOW_STOCK = int(os.getenv("ML_UMBRAL_LOW_STOCK", "3"))

DATA_DIR = os.getenv("DATA_DIR") or os.getenv("RAILWAY_VOLUME_MOUNT_PATH") or "."
ML_DB_FILE = os.path.join(DATA_DIR, "ml_monitor.db")

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

AUTH_BASE = f"https://auth.mercadolibre.com.mx"
API_BASE = "https://api.mercadolibre.com"
TIMEOUT = 15

ml_bp = Blueprint("ml", __name__)

# Overlap protection -- si un ciclo todavia esta corriendo, el siguiente se salta
# en vez de encimarse (los endpoints de Full tienen limites de tasa mas estrictos).
_RUNNING = {"buybox": False, "full": False}


def enviar_telegram_ml(mensaje):
    """Mismo bot/chat que el monitor de Liverpool -- prefijo propio para no confundir."""
    if not TELEGRAM_TOKEN or not CHAT_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": CHAT_ID, "text": "🛒 [ML] " + mensaje, "parse_mode": "HTML"},
            timeout=10,
        )
    except Exception as exc:
        print(f"⚠️ [ML] Error enviando Telegram: {exc}")


# ================================
# DB
# ================================
def _conn():
    con = sqlite3.connect(ML_DB_FILE, timeout=10)
    con.row_factory = sqlite3.Row
    return con


def inicializar_db():
    os.makedirs(DATA_DIR, exist_ok=True)
    with _conn() as con:
        con.execute("""CREATE TABLE IF NOT EXISTS ml_tokens (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            access_token TEXT, refresh_token TEXT, expires_at TEXT,
            user_id TEXT, scope TEXT, updated_at TEXT
        )""")
        con.execute("""CREATE TABLE IF NOT EXISTS ml_items (
            item_id TEXT PRIMARY KEY, title TEXT, price REAL, available_quantity INTEGER,
            status TEXT, logistic_type TEXT, catalog_listing INTEGER, catalog_product_id TEXT,
            seller_sku TEXT, thumbnail TEXT, updated_at TEXT
        )""")
        con.execute("""CREATE TABLE IF NOT EXISTS ml_buybox_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT, item_id TEXT, ts TEXT,
            status TEXT, price_to_win REAL, price_propio REAL,
            winner_item_id TEXT, winner_seller_id TEXT, winner_nickname TEXT,
            winner_price REAL, winner_pais TEXT, visit_share REAL, raw_json TEXT
        )""")
        con.execute("CREATE INDEX IF NOT EXISTS idx_ml_snap_item ON ml_buybox_snapshots(item_id, ts)")
        con.execute("""CREATE TABLE IF NOT EXISTS ml_price_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT, item_id TEXT, ts TEXT, price REAL
        )""")
        con.execute("""CREATE TABLE IF NOT EXISTS ml_alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT, item_id TEXT, tipo TEXT, ts TEXT,
            detalle TEXT, resuelta INTEGER DEFAULT 0
        )""")
        con.execute("""CREATE TABLE IF NOT EXISTS ml_questions (
            question_id TEXT PRIMARY KEY, item_id TEXT, texto TEXT,
            respuesta_sugerida TEXT, respondida INTEGER DEFAULT 0, ts TEXT
        )""")
        con.execute("""CREATE TABLE IF NOT EXISTS ml_full_inventory (
            id INTEGER PRIMARY KEY AUTOINCREMENT, inventory_id TEXT, ts TEXT,
            disponible INTEGER, total INTEGER, raw_json TEXT
        )""")
        con.execute("""CREATE TABLE IF NOT EXISTS ml_full_operations (
            id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, tipo TEXT, raw_json TEXT
        )""")
        con.execute("""CREATE TABLE IF NOT EXISTS ml_full_withdrawals (
            id INTEGER PRIMARY KEY AUTOINCREMENT, periodo TEXT, ts TEXT, raw_json TEXT
        )""")


# ================================
# OAUTH
# ================================
def guardar_tokens(data):
    expires_at = (datetime.now(timezone.utc) + timedelta(seconds=int(data["expires_in"]))).isoformat()
    with _conn() as con:
        con.execute(
            """INSERT INTO ml_tokens (id, access_token, refresh_token, expires_at, user_id, scope, updated_at)
               VALUES (1, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(id) DO UPDATE SET
                 access_token=excluded.access_token, refresh_token=excluded.refresh_token,
                 expires_at=excluded.expires_at, user_id=excluded.user_id,
                 scope=excluded.scope, updated_at=excluded.updated_at""",
            (
                data["access_token"], data["refresh_token"], expires_at,
                str(data.get("user_id", "")), data.get("scope", ""),
                datetime.now(timezone.utc).isoformat(),
            ),
        )


def leer_tokens():
    with _conn() as con:
        row = con.execute("SELECT * FROM ml_tokens WHERE id = 1").fetchone()
    return dict(row) if row else None


def intercambiar_code_por_token(code):
    r = requests.post(
        f"{API_BASE}/oauth/token",
        data={
            "grant_type": "authorization_code",
            "client_id": MELI_CLIENT_ID,
            "client_secret": MELI_CLIENT_SECRET,
            "code": code,
            "redirect_uri": MELI_REDIRECT_URI,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=TIMEOUT,
    )
    r.raise_for_status()
    data = r.json()
    guardar_tokens(data)
    return data


def refrescar_token(refresh_token):
    r = requests.post(
        f"{API_BASE}/oauth/token",
        data={
            "grant_type": "refresh_token",
            "client_id": MELI_CLIENT_ID,
            "client_secret": MELI_CLIENT_SECRET,
            "refresh_token": refresh_token,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=TIMEOUT,
    )
    r.raise_for_status()
    data = r.json()
    guardar_tokens(data)  # ML manda un refresh_token NUEVO -- se guarda de una vez, el viejo ya no sirve
    return data


def obtener_access_token():
    """Regresa un access_token valido, renovando si le quedan <10 min. None si no hay login todavia."""
    tokens = leer_tokens()
    if not tokens:
        return None
    expira = datetime.fromisoformat(tokens["expires_at"])
    if expira - datetime.now(timezone.utc) < timedelta(minutes=10):
        try:
            data = refrescar_token(tokens["refresh_token"])
            return data["access_token"]
        except Exception as exc:
            print(f"⚠️ [ML] Error renovando token: {exc}")
            return None
    return tokens["access_token"]


def ml_get(path, params=None):
    token = obtener_access_token()
    if not token:
        raise RuntimeError("Sin login de Mercado Libre todavia -- entra a /ml/login")
    r = requests.get(
        f"{API_BASE}{path}",
        headers={"Authorization": f"Bearer {token}"},
        params=params or {},
        timeout=TIMEOUT,
    )
    r.raise_for_status()
    return r.json()


def ml_post(path, json_body=None):
    token = obtener_access_token()
    if not token:
        raise RuntimeError("Sin login de Mercado Libre todavia -- entra a /ml/login")
    r = requests.post(
        f"{API_BASE}{path}",
        headers={"Authorization": f"Bearer {token}"},
        json=json_body or {},
        timeout=TIMEOUT,
    )
    r.raise_for_status()
    return r.json() if r.text else {}


def ml_delete(path):
    token = obtener_access_token()
    if not token:
        raise RuntimeError("Sin login de Mercado Libre todavia -- entra a /ml/login")
    r = requests.delete(f"{API_BASE}{path}", headers={"Authorization": f"Bearer {token}"}, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json() if r.text else {}


@ml_bp.route("/ml/login")
def ml_login():
    if not ML_OK:
        return jsonify({"error": "Faltan MELI_CLIENT_ID / MELI_CLIENT_SECRET / MELI_REDIRECT_URI"}), 400
    url = (
        f"{AUTH_BASE}/authorization?response_type=code"
        f"&client_id={MELI_CLIENT_ID}&redirect_uri={MELI_REDIRECT_URI}"
    )
    return redirect(url)


@ml_bp.route("/ml/callback")
def ml_callback():
    code = request.args.get("code")
    if not code:
        return jsonify({"error": request.args.get("error", "sin 'code' en el callback")}), 400
    try:
        data = intercambiar_code_por_token(code)
    except Exception as exc:
        return jsonify({"error": f"Error intercambiando code por token: {exc}"}), 502
    return jsonify({"ok": True, "user_id": data.get("user_id"), "scope": data.get("scope")})


# ================================
# CUENTA / PUBLICACIONES
# ================================
def obtener_cuenta():
    return ml_get("/users/me")


def obtener_publicaciones():
    """Pagina con scroll_id (search_type=scan), luego trae detalle de cada item."""
    tokens = leer_tokens()
    if not tokens or not tokens.get("user_id"):
        raise RuntimeError("Sin user_id -- haz login primero")
    user_id = tokens["user_id"]

    item_ids = []
    scroll_id = None
    while True:
        params = {"search_type": "scan"}
        if scroll_id:
            params["scroll_id"] = scroll_id
        data = ml_get(f"/users/{user_id}/items/search", params=params)
        resultados = data.get("results", [])
        item_ids.extend(resultados)
        scroll_id = data.get("scroll_id")
        if not resultados or not scroll_id:
            break

    items = []
    for item_id in item_ids:
        try:
            it = ml_get(f"/items/{item_id}")
        except Exception as exc:
            print(f"⚠️ [ML] Error trayendo item {item_id}: {exc}")
            continue
        items.append(it)
        time.sleep(0.12)  # ritmo seguro, evita 429 en catalogos grandes
    return items


def guardar_publicaciones(items):
    ahora = datetime.now(timezone.utc).isoformat()
    with _conn() as con:
        for it in items:
            con.execute(
                """INSERT INTO ml_items (item_id, title, price, available_quantity, status,
                     logistic_type, catalog_listing, catalog_product_id, seller_sku, thumbnail, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(item_id) DO UPDATE SET
                     title=excluded.title, price=excluded.price, available_quantity=excluded.available_quantity,
                     status=excluded.status, logistic_type=excluded.logistic_type,
                     catalog_listing=excluded.catalog_listing, catalog_product_id=excluded.catalog_product_id,
                     seller_sku=excluded.seller_sku, thumbnail=excluded.thumbnail, updated_at=excluded.updated_at""",
                (
                    it.get("id"), it.get("title"), it.get("price"), it.get("available_quantity"),
                    it.get("status"),
                    (it.get("shipping") or {}).get("logistic_type"),
                    int(bool(it.get("catalog_listing"))),
                    it.get("catalog_product_id"),
                    next((a.get("value_name") for a in it.get("attributes", []) if a.get("id") == "SELLER_SKU"), None),
                    (it.get("thumbnail") or ""),
                    ahora,
                ),
            )
            con.execute(
                "INSERT INTO ml_price_history (item_id, ts, price) VALUES (?, ?, ?)",
                (it.get("id"), ahora, it.get("price")),
            )


# ================================
# BUYBOX
# ================================
def obtener_price_to_win(item_id):
    return ml_get(f"/items/{item_id}/price_to_win", params={"siteId": MELI_SITE_ID, "version": "v2"})


def obtener_datos_ganador(winner_item_id):
    resultado = {}
    try:
        resultado["item"] = ml_get(f"/items/{winner_item_id}")
    except Exception as exc:
        resultado["item_error"] = str(exc)
    try:
        resultado["marketplace_item"] = ml_get(f"/marketplace/items/{winner_item_id}")
    except Exception as exc:
        resultado["marketplace_item_error"] = str(exc)
    seller_id = (resultado.get("item") or {}).get("seller_id") or (resultado.get("item") or {}).get("seller", {}).get("id")
    if seller_id:
        try:
            vendedor = ml_get(f"/users/{seller_id}")
            resultado["seller"] = vendedor
            pais = (vendedor.get("address") or {}).get("country_id") or vendedor.get("country_id")
            resultado["es_seller_us"] = pais == "US" if pais else None
        except Exception as exc:
            resultado["seller_error"] = str(exc)
    return resultado


def _ultimo_snapshot(item_id):
    with _conn() as con:
        row = con.execute(
            "SELECT * FROM ml_buybox_snapshots WHERE item_id = ? ORDER BY id DESC LIMIT 1", (item_id,)
        ).fetchone()
    return dict(row) if row else None


def _guardar_snapshot(item_id, status, price_to_win, price_propio, ganador, raw):
    ahora = datetime.now(timezone.utc).isoformat()
    with _conn() as con:
        con.execute(
            """INSERT INTO ml_buybox_snapshots
               (item_id, ts, status, price_to_win, price_propio, winner_item_id, winner_seller_id,
                winner_nickname, winner_price, winner_pais, visit_share, raw_json)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                item_id, ahora, status, price_to_win, price_propio,
                ganador.get("winner_item_id"), ganador.get("winner_seller_id"),
                ganador.get("winner_nickname"), ganador.get("winner_price"),
                ganador.get("winner_pais"), ganador.get("visit_share"),
                json.dumps(raw, ensure_ascii=False),
            ),
        )


def _registrar_alerta(item_id, tipo, detalle):
    ahora = datetime.now(timezone.utc).isoformat()
    with _conn() as con:
        con.execute(
            "INSERT INTO ml_alerts (item_id, tipo, ts, detalle) VALUES (?, ?, ?, ?)",
            (item_id, tipo, ahora, detalle),
        )


def _comparar_y_alertar(item, anterior, nuevo_status, price_to_win, price_propio, ganador):
    item_id = item.get("id")
    titulo = (item.get("title") or item_id)[:60]

    if item.get("status") == "paused" and (not anterior or anterior.get("status") != "ITEM_PAUSED"):
        _registrar_alerta(item_id, "ITEM_PAUSED", titulo)
        enviar_telegram_ml(f"⏸️ Publicación pausada: {titulo}")

    estado_anterior = anterior.get("status") if anterior else None
    if estado_anterior and estado_anterior != nuevo_status:
        if estado_anterior == "winning" and nuevo_status in ("losing", "sharing_first_place"):
            _registrar_alerta(item_id, "BUYBOX_LOST", titulo)
            enviar_telegram_ml(f"🔴 Perdiste el BuyBox: {titulo}\nGanador: {ganador.get('winner_nickname','?')} a ${ganador.get('winner_price','?')}")
        elif estado_anterior in ("losing", "sharing_first_place") and nuevo_status == "winning":
            _registrar_alerta(item_id, "BUYBOX_RECOVERED", titulo)
            enviar_telegram_ml(f"🟢 Recuperaste el BuyBox: {titulo}")

    if anterior and anterior.get("winner_price") and price_to_win:
        if anterior["winner_price"] and price_to_win < anterior["winner_price"]:
            _registrar_alerta(item_id, "PRICE_DROP_COMPETITOR", f"{titulo} -- de ${anterior['winner_price']} a ${price_to_win}")

    if price_propio and price_to_win and price_propio > 0:
        diff = abs(price_propio - price_to_win) / price_propio
        if diff >= ML_UMBRAL_PRICE_DIFF:
            _registrar_alerta(item_id, "PRICE_DIFF_HIGH", f"{titulo} -- diferencia {diff*100:.1f}%")

    cantidad = item.get("available_quantity")
    if cantidad is not None and cantidad <= ML_UMBRAL_LOW_STOCK:
        _registrar_alerta(item_id, "LOW_STOCK", f"{titulo} -- quedan {cantidad}")


def ciclo_buybox():
    if _RUNNING["buybox"]:
        print("⏭️ [ML] Ciclo de BuyBox anterior sigue corriendo, se salta este.")
        return
    _RUNNING["buybox"] = True
    try:
        items = obtener_publicaciones()
        guardar_publicaciones(items)
        procesadas = 0
        for item in items:
            item_id = item.get("id")
            if not item.get("catalog_listing"):
                continue  # price_to_win solo aplica a publicaciones de catalogo
            anterior = _ultimo_snapshot(item_id)
            try:
                ptw = obtener_price_to_win(item_id)
            except Exception as exc:
                _registrar_alerta(item_id, "API_ERROR", f"price_to_win: {exc}")
                continue
            status = ptw.get("status")
            price_to_win = ptw.get("price_to_win")
            winner_item_id = (ptw.get("winner") or {}).get("item_id")
            ganador = {"winner_item_id": winner_item_id}
            if winner_item_id:
                datos = obtener_datos_ganador(winner_item_id)
                vendedor = datos.get("seller") or {}
                ganador.update({
                    "winner_nickname": vendedor.get("nickname"),
                    "winner_price": (datos.get("item") or {}).get("price"),
                    "winner_seller_id": vendedor.get("id"),
                    "winner_pais": (vendedor.get("address") or {}).get("country_id"),
                })
            _comparar_y_alertar(item, anterior, status, price_to_win, item.get("price"), ganador)
            _guardar_snapshot(item_id, status, price_to_win, item.get("price"), ganador, ptw)
            procesadas += 1
            time.sleep(0.15)
        print(f"✅ [ML] Ciclo BuyBox: {procesadas} publicaciones de catálogo revisadas.")
    except Exception as exc:
        print(f"💥 [ML] Error en ciclo de BuyBox: {exc}")
    finally:
        _RUNNING["buybox"] = False


# ================================
# PREGUNTAS
# ================================
def obtener_preguntas_sin_responder():
    tokens = leer_tokens()
    if not tokens:
        return []
    data = ml_get("/questions/search", params={"seller_id": tokens["user_id"], "status": "UNANSWERED"})
    return data.get("questions", [])


def sugerir_respuesta(item_id, texto_pregunta):
    """Sugerencia simple basada en titulo/descripcion del item -- no es IA generativa,
    solo arma un borrador con datos reales del producto para que el usuario lo edite."""
    try:
        item = ml_get(f"/items/{item_id}")
        desc = ml_get(f"/items/{item_id}/description")
        return (
            f"Hola, gracias por tu interés en {item.get('title','este producto')}. "
            f"{(desc.get('plain_text') or '')[:200]} "
            f"Quedamos atentos a cualquier duda."
        )
    except Exception:
        return "Hola, gracias por tu pregunta, en breve te confirmamos."


def guardar_pregunta(q, item_id):
    ahora = datetime.now(timezone.utc).isoformat()
    sugerida = sugerir_respuesta(item_id, q.get("text", ""))
    with _conn() as con:
        con.execute(
            """INSERT INTO ml_questions (question_id, item_id, texto, respuesta_sugerida, ts)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(question_id) DO UPDATE SET texto=excluded.texto""",
            (str(q.get("id")), item_id, q.get("text"), sugerida, ahora),
        )


def responder_pregunta(question_id, texto):
    resultado = ml_post("/answers", json_body={"question_id": int(question_id), "text": texto})
    with _conn() as con:
        con.execute("UPDATE ml_questions SET respondida = 1 WHERE question_id = ?", (str(question_id),))
    return resultado


def eliminar_pregunta(question_id):
    return ml_delete(f"/questions/{question_id}")


def bloquear_comprador(buyer_id):
    tokens = leer_tokens()
    return ml_post(f"/users/{tokens['user_id']}/questions_blacklist", json_body={"user_id": buyer_id})


def ciclo_preguntas():
    try:
        preguntas = obtener_preguntas_sin_responder()
        for q in preguntas:
            guardar_pregunta(q, q.get("item_id"))
        print(f"✅ [ML] {len(preguntas)} pregunta(s) sin responder sincronizadas.")
    except Exception as exc:
        print(f"💥 [ML] Error sincronizando preguntas: {exc}")


# ================================
# FULL
# ================================
def obtener_inventario_full(inventory_id):
    return ml_get(f"/inventories/{inventory_id}/stock/fulfillment")


def obtener_operaciones_full():
    return ml_get("/stock/fulfillment/operations/search")


def obtener_retiros_full(periodo):
    return ml_get(f"/billing/integration/periods/key/{periodo}/group/ML/full/details")


def ciclo_full(inventory_ids=None, periodo_actual=None):
    if _RUNNING["full"]:
        print("⏭️ [ML] Ciclo de Full anterior sigue corriendo, se salta este.")
        return
    _RUNNING["full"] = True
    ahora = datetime.now(timezone.utc).isoformat()
    try:
        with _conn() as con:
            for inv_id in (inventory_ids or []):
                try:
                    data = obtener_inventario_full(inv_id)
                    con.execute(
                        "INSERT INTO ml_full_inventory (inventory_id, ts, disponible, total, raw_json) VALUES (?,?,?,?,?)",
                        (inv_id, ahora, data.get("available_quantity"), data.get("total_quantity"), json.dumps(data, ensure_ascii=False)),
                    )
                except Exception as exc:
                    print(f"⚠️ [ML] Error inventario full {inv_id}: {exc}")

            try:
                ops = obtener_operaciones_full()
                con.execute(
                    "INSERT INTO ml_full_operations (ts, tipo, raw_json) VALUES (?, ?, ?)",
                    (ahora, "operations_search", json.dumps(ops, ensure_ascii=False)),
                )
            except Exception as exc:
                print(f"⚠️ [ML] Error operaciones full: {exc}")

            if periodo_actual:
                try:
                    retiros = obtener_retiros_full(periodo_actual)
                    con.execute(
                        "INSERT INTO ml_full_withdrawals (periodo, ts, raw_json) VALUES (?, ?, ?)",
                        (periodo_actual, ahora, json.dumps(retiros, ensure_ascii=False)),
                    )
                except Exception as exc:
                    print(f"⚠️ [ML] Error retiros full: {exc}")
        print("✅ [ML] Ciclo Full completado.")
    except Exception as exc:
        print(f"💥 [ML] Error en ciclo Full: {exc}")
    finally:
        _RUNNING["full"] = False


# ================================
# JOBS (loops de fondo)
# ================================
def loop_ml_buybox():
    while True:
        time.sleep(ML_BUYBOX_INTERVAL_MIN * 60)
        if not ML_OK or not leer_tokens():
            continue
        try:
            ciclo_buybox()
            ciclo_preguntas()
        except Exception as exc:
            print(f"💥 [ML] Error en loop de BuyBox: {exc}")


def loop_ml_full():
    """Corre una vez por hora, en el minuto 17 -- los endpoints de Full tienen
    limites de tasa mas estrictos, se separa del ciclo de BuyBox a proposito."""
    while True:
        ahora = datetime.now()
        objetivo = ahora.replace(minute=17, second=0, microsecond=0)
        if objetivo <= ahora:
            objetivo += timedelta(hours=1)
        time.sleep(max(1, (objetivo - ahora).total_seconds()))
        if not ML_OK or not leer_tokens():
            continue
        try:
            ciclo_full()
        except Exception as exc:
            print(f"💥 [ML] Error en loop de Full: {exc}")


def iniciar_hilos_ml():
    inicializar_db()
    if not ML_OK:
        print("⚠️ [ML] Faltan MELI_CLIENT_ID / MELI_CLIENT_SECRET / MELI_REDIRECT_URI -- integración ML desactivada.")
        return
    threading.Thread(target=loop_ml_buybox, daemon=True).start()
    threading.Thread(target=loop_ml_full, daemon=True).start()
    print(f"🛒 [ML] Monitor de Mercado Libre activo -- BuyBox cada {ML_BUYBOX_INTERVAL_MIN}min, Full cada hora al :17")


# ================================
# RUTAS API
# ================================
@ml_bp.route("/api/ml/estado")
def api_ml_estado():
    tokens = leer_tokens()
    return jsonify({
        "configurado": ML_OK,
        "conectado": bool(tokens),
        "user_id": tokens["user_id"] if tokens else None,
        "scope": tokens["scope"] if tokens else None,
    })


@ml_bp.route("/api/ml/publicaciones")
def api_ml_publicaciones():
    with _conn() as con:
        filas = con.execute("SELECT * FROM ml_items ORDER BY updated_at DESC").fetchall()
    return jsonify([dict(f) for f in filas])


@ml_bp.route("/api/ml/buybox")
def api_ml_buybox():
    with _conn() as con:
        filas = con.execute(
            """SELECT s.* FROM ml_buybox_snapshots s
               INNER JOIN (SELECT item_id, MAX(id) mid FROM ml_buybox_snapshots GROUP BY item_id) ult
               ON s.id = ult.mid ORDER BY s.ts DESC"""
        ).fetchall()
    return jsonify([dict(f) for f in filas])


@ml_bp.route("/api/ml/alertas")
def api_ml_alertas():
    limite = int(request.args.get("limite", 100))
    with _conn() as con:
        filas = con.execute("SELECT * FROM ml_alerts ORDER BY id DESC LIMIT ?", (limite,)).fetchall()
    return jsonify([dict(f) for f in filas])


@ml_bp.route("/api/ml/preguntas")
def api_ml_preguntas():
    with _conn() as con:
        filas = con.execute("SELECT * FROM ml_questions WHERE respondida = 0 ORDER BY ts DESC").fetchall()
    return jsonify([dict(f) for f in filas])


@ml_bp.route("/api/ml/preguntas/<question_id>/responder", methods=["POST"])
def api_ml_responder(question_id):
    texto = (request.get_json(silent=True) or {}).get("texto")
    if not texto:
        return jsonify({"error": "Falta 'texto'"}), 400
    try:
        resultado = responder_pregunta(question_id, texto)
        return jsonify({"ok": True, "resultado": resultado})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 502


@ml_bp.route("/api/ml/preguntas/<question_id>", methods=["DELETE"])
def api_ml_eliminar_pregunta(question_id):
    try:
        eliminar_pregunta(question_id)
        return jsonify({"ok": True})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 502


@ml_bp.route("/api/ml/preguntas/<question_id>/bloquear", methods=["POST"])
def api_ml_bloquear(question_id):
    buyer_id = (request.get_json(silent=True) or {}).get("buyer_id")
    if not buyer_id:
        return jsonify({"error": "Falta 'buyer_id'"}), 400
    try:
        resultado = bloquear_comprador(buyer_id)
        return jsonify({"ok": True, "resultado": resultado})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 502


@ml_bp.route("/api/ml/full/inventario")
def api_ml_full_inventario():
    with _conn() as con:
        filas = con.execute("SELECT * FROM ml_full_inventory ORDER BY id DESC LIMIT 50").fetchall()
    return jsonify([dict(f) for f in filas])


@ml_bp.route("/api/ml/full/operaciones")
def api_ml_full_operaciones():
    with _conn() as con:
        filas = con.execute("SELECT * FROM ml_full_operations ORDER BY id DESC LIMIT 50").fetchall()
    return jsonify([dict(f) for f in filas])


@ml_bp.route("/api/ml/full/retiros")
def api_ml_full_retiros():
    with _conn() as con:
        filas = con.execute("SELECT * FROM ml_full_withdrawals ORDER BY id DESC LIMIT 50").fetchall()
    return jsonify([dict(f) for f in filas])


@ml_bp.route("/api/ml/sync/buybox", methods=["POST"])
def api_ml_forzar_buybox():
    threading.Thread(target=ciclo_buybox, daemon=True).start()
    return jsonify({"ok": True, "encolado": True})


@ml_bp.route("/api/ml/sync/full", methods=["POST"])
def api_ml_forzar_full():
    inventory_ids = (request.get_json(silent=True) or {}).get("inventory_ids", [])
    periodo = (request.get_json(silent=True) or {}).get("periodo")
    threading.Thread(target=ciclo_full, args=(inventory_ids, periodo), daemon=True).start()
    return jsonify({"ok": True, "encolado": True})
