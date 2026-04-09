"""
ventas.py — Módulo de Ventas para Liverpool BuyBox Monitor
Agrega rutas /ventas y /api/ventas/* a la app Flask existente sin tocar BuyBox.
"""
import base64
import calendar
import hashlib
import io
import json
import os
import re
import sqlite3
import threading
import time
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from typing import Optional, Tuple

import pandas as pd
import requests
from flask import Blueprint, jsonify, render_template_string, request

# ─── Config mutable (seteado por init_ventas) ──────────────────────────────
_DATA_DIR: str = "."
_DB_FILE: str = "ventas_monitor.db"
_CDMX_TZ = timezone(timedelta(hours=-6))

CREDITIENDA_SHARE_URL = (
    "https://1drv.ms/x/c/38f7c930c20edbea/"
    "IQB1GeReIzuPSa329bVCAFqPAYFK7lKTDrwb1VXp33Wp-bc?e=M9FfyG"
)
LIVERPOOL_SHARE_URL = (
    "https://1drv.ms/x/c/0db300c27ec53cb3/"
    "IQCzPMV-wgCzIIANhAAAAAAAAfkEc_5TCgOXUwlaASmmPzY?e=lETFJd"
)

_HEADERS_HTTP = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "es-MX,es;q=0.9",
}

ventas_bp = Blueprint("ventas", __name__)


def init_ventas(data_dir: str, cdmx_tz=None):
    """Llamar desde monitor.py después de crear la app Flask."""
    global _DATA_DIR, _DB_FILE, _CDMX_TZ
    _DATA_DIR = data_dir or "."
    if cdmx_tz is not None:
        _CDMX_TZ = cdmx_tz
    _DB_FILE = os.path.join(_DATA_DIR, "ventas_monitor.db")
    os.makedirs(_DATA_DIR, exist_ok=True)
    _init_db()
    print(f"💰 Ventas DB: {_DB_FILE}")
    # Auto-sync en background cada 6 horas
    t = threading.Thread(target=_loop_auto_sync, daemon=True)
    t.start()
    print("💰 Ventas auto-sync: cada 6 horas")


# ═══════════════════════════════════════════════════════════════════════════
# UTILIDADES
# ═══════════════════════════════════════════════════════════════════════════

def _normalizar_col(valor) -> str:
    return re.sub(r"\s+", " ", str(valor).strip().lower())


def _limpiar_str(valor) -> str:
    if valor is None:
        return ""
    texto = str(valor).strip()
    return "" if texto.lower() in ("nan", "none", "") else texto


def _parse_fecha(valor) -> Optional[date]:
    if valor is None:
        return None
    # Filtrar pd.NaT y otros nulos de pandas sin importar pandas globalmente
    try:
        import pandas as _pd
        if _pd.isnull(valor):
            return None
    except Exception:
        pass
    if isinstance(valor, datetime):
        try:
            return valor.date()
        except Exception:
            return None
    if isinstance(valor, date):
        return valor
    texto = _limpiar_str(valor)
    if not texto or texto.lower() in ("nat", "nan", "none"):
        return None
    for fmt in ("%d/%m/%y", "%d/%m/%Y", "%Y-%m-%d", "%m/%d/%Y", "%d-%m-%Y", "%d-%m-%y"):
        try:
            return datetime.strptime(texto, fmt).date()
        except ValueError:
            continue
    return None


def _parse_precio(valor) -> Optional[float]:
    if valor is None:
        return None
    if isinstance(valor, bool):
        return None
    if isinstance(valor, (int, float)):
        try:
            v = float(valor)
            return None if (v != v) else v  # NaN check
        except Exception:
            return None
    texto = _limpiar_str(valor)
    if not texto:
        return None
    texto = texto.replace("$", "").replace(",", "").strip()
    try:
        return float(texto)
    except ValueError:
        return None


def _normalizar_sku(sku_raw: str) -> Tuple[str, int]:
    """
    Extrae (sku_base, multiplicador) de strings como:
      4201814/4200087          → ('4201814', 1)
      4201815_ iphone 13 Rosa  → ('4201815', 1)
      4201180--S25 Verde       → ('4201180', 1)
      2x4201017/2x4200085      → ('4201017', 2)
    """
    texto = _limpiar_str(sku_raw)
    if not texto:
        return "", 1

    # Detectar multiplicador tipo "2x" al inicio
    mult_match = re.match(r"^(\d+)[xX]", texto)
    multiplicador = int(mult_match.group(1)) if mult_match else 1

    # Primer bloque de 7 dígitos
    m = re.search(r"\b(\d{7})\b", texto)
    if m:
        return m.group(1), multiplicador

    # Fallback: primer bloque de 5+ dígitos
    m = re.search(r"(\d{5,})", texto)
    if m:
        return m.group(1), multiplicador

    return texto[:50], multiplicador


def _raw_hash(rec: dict) -> str:
    key = "|".join([
        str(rec.get("channel", "")),
        str(rec.get("source_sheet", "")),
        str(rec.get("event_type", "")),
        str(rec.get("fecha", "")),
        str(rec.get("remision", "")),
        str(rec.get("pedido_externo", "")),
        str(rec.get("sku_raw", "")),
        str(rec.get("cantidad", "")),
        str(rec.get("precio_unitario", "")),
    ])
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:32]


# ═══════════════════════════════════════════════════════════════════════════
# BASE DE DATOS
# ═══════════════════════════════════════════════════════════════════════════

def _init_db():
    with _get_db() as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS ventas_eventos (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            source_file     TEXT,
            source_sheet    TEXT,
            channel         TEXT,
            event_type      TEXT,
            fecha           DATE,
            pedido_externo  TEXT,
            pedido_interno  TEXT,
            remision        TEXT,
            pedido_shopify  TEXT,
            pedido_simco    TEXT,
            realizo         TEXT,
            cliente         TEXT,
            sku_raw         TEXT,
            sku_normalizado TEXT,
            sku_base        TEXT,
            multiplicador   INTEGER DEFAULT 1,
            descripcion     TEXT,
            cantidad        REAL,
            precio_unitario REAL,
            monto_bruto     REAL,
            comision        REAL,
            monto_neto      REAL,
            ajuste          TEXT,
            guia            TEXT,
            raw_hash        TEXT UNIQUE,
            synced_at       DATETIME
        )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_fecha   ON ventas_eventos(fecha)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_channel ON ventas_eventos(channel)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sku     ON ventas_eventos(sku_normalizado)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_evtype  ON ventas_eventos(event_type)")


@contextmanager
def _get_db():
    conn = sqlite3.connect(_DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _insert_records(records: list, source_file: str) -> dict:
    synced_at = datetime.now(_CDMX_TZ).strftime("%Y-%m-%d %H:%M:%S")
    insertados = 0
    duplicados = 0
    errores = 0

    with _get_db() as conn:
        for rec in records:
            h = _raw_hash(rec)
            fecha_val = rec.get("fecha")
            fecha_str = fecha_val.isoformat() if isinstance(fecha_val, date) else None

            try:
                conn.execute("""
                INSERT INTO ventas_eventos (
                    source_file, source_sheet, channel, event_type, fecha,
                    pedido_externo, pedido_interno, remision, pedido_shopify, pedido_simco,
                    realizo, cliente, sku_raw, sku_normalizado, sku_base, multiplicador,
                    descripcion, cantidad, precio_unitario, monto_bruto, comision, monto_neto,
                    ajuste, guia, raw_hash, synced_at
                ) VALUES (
                    :source_file, :source_sheet, :channel, :event_type, :fecha,
                    :pedido_externo, :pedido_interno, :remision, :pedido_shopify, :pedido_simco,
                    :realizo, :cliente, :sku_raw, :sku_normalizado, :sku_base, :multiplicador,
                    :descripcion, :cantidad, :precio_unitario, :monto_bruto, :comision, :monto_neto,
                    :ajuste, :guia, :raw_hash, :synced_at
                )
                """, {
                    **rec,
                    "source_file": source_file,
                    "raw_hash": h,
                    "synced_at": synced_at,
                    "fecha": fecha_str,
                    # Asegura que tipos complejos no pasen al driver
                    "precio_unitario": rec.get("precio_unitario"),
                    "monto_bruto": rec.get("monto_bruto"),
                    "comision": rec.get("comision"),
                    "monto_neto": rec.get("monto_neto"),
                    "cantidad": rec.get("cantidad"),
                    "multiplicador": rec.get("multiplicador", 1),
                })
                insertados += 1
            except sqlite3.IntegrityError:
                duplicados += 1
            except Exception as exc:
                errores += 1
                print(f"  DB error en registro {h[:8]}: {exc}")

    return {"insertados": insertados, "duplicados": duplicados, "errores_db": errores}


# ═══════════════════════════════════════════════════════════════════════════
# AUTO-SYNC BACKGROUND
# ═══════════════════════════════════════════════════════════════════════════

SYNC_INTERVAL_HORAS = int(os.getenv("VENTAS_SYNC_HORAS", "6"))
_ultimo_auto_sync: float = 0.0


def _loop_auto_sync():
    """Hilo daemon: sincroniza ventas cada SYNC_INTERVAL_HORAS horas."""
    global _ultimo_auto_sync
    # Primer sync: espera 60s para que la app termine de arrancar
    time.sleep(60)
    while True:
        ahora = time.time()
        if ahora - _ultimo_auto_sync >= SYNC_INTERVAL_HORAS * 3600:
            print(f"💰 Auto-sync ventas iniciado ({SYNC_INTERVAL_HORAS}h interval)...")
            try:
                r = sincronizar_ventas()
                cred = r.get("creditienda", {})
                liv  = r.get("liverpool",   {})
                print(f"💰 Auto-sync OK — CT: {cred.get('insertados',0)} nuevos | LV: {liv.get('insertados',0)} nuevos")
            except Exception as exc:
                print(f"💰 Auto-sync error: {exc}")
            _ultimo_auto_sync = time.time()
        time.sleep(300)  # revisa cada 5 minutos


# ═══════════════════════════════════════════════════════════════════════════
# ONEDRIVE — RESOLUCIÓN Y DESCARGA
# ═══════════════════════════════════════════════════════════════════════════

def _resolve_onedrive(share_url: str, env_override: str = None) -> Optional[str]:
    """
    Resuelve share link de OneDrive a URL de descarga directa que NO expira.

    Método principal (funciona desde cualquier servidor incluido Railway):
      1. Seguir el redirect de 1drv.ms → obtener URL de onedrive.live.com
      2. Agregar ?download=1&e={authkey} → descarga directa estable

    Fallback: scraping HTML (solo funciona desde IPs no bloqueadas por MS).
    """
    if env_override:
        return env_override

    # ── Método principal: redirect + download=1 ───────────────────────────
    try:
        m_e = re.search(r"[?&]e=([^&]+)", share_url)
        authkey = m_e.group(1) if m_e else ""

        r1 = requests.get(share_url, headers=_HEADERS_HTTP, timeout=20, allow_redirects=False)
        loc = r1.headers.get("Location", "")

        if loc and "onedrive.live.com" in loc:
            # Quitar query string existente y agregar download=1
            base = loc.split("?")[0]
            dl_url = f"{base}?e={authkey}&download=1"
            # Verificar que devuelve un Excel
            head = requests.head(dl_url, headers=_HEADERS_HTTP, timeout=15, allow_redirects=True)
            ct = head.headers.get("Content-Type", "")
            if head.status_code == 200 and ("spreadsheet" in ct or "excel" in ct or "octet" in ct):
                print(f"  OneDrive → OK (redirect+download)")
                return dl_url
            # A veces el HEAD no trae Content-Type pero el GET sí funciona
            if head.status_code == 200:
                print(f"  OneDrive → OK (redirect+download, sin CT)")
                return dl_url

        print(f"  OneDrive redirect: {r1.status_code} → {loc[:80]}")
    except Exception as exc:
        print(f"  OneDrive redirect error: {exc}")

    # ── Fallback: scraping HTML ───────────────────────────────────────────
    try:
        resp = requests.get(share_url, headers=_HEADERS_HTTP, timeout=30, allow_redirects=True)
        if resp.status_code == 200:
            for pat in [r'"FileGetUrl"\s*:\s*"([^"]+)"', r'"downloadUrl"\s*:\s*"([^"]+)"']:
                m = re.search(pat, resp.text)
                if m:
                    url = m.group(1).replace("\\u0026", "&").replace("\\/", "/")
                    print(f"  OneDrive scraping → OK")
                    return url
        print(f"  OneDrive: todos los métodos fallaron (HTTP {resp.status_code})")
    except Exception as exc:
        print(f"  OneDrive scraping error: {exc}")

    return None


def _download_excel(url: str) -> Optional[bytes]:
    try:
        resp = requests.get(url, headers=_HEADERS_HTTP, timeout=90, allow_redirects=True)
        if resp.status_code == 200:
            # Verificar que es un Excel real
            ct = resp.headers.get("Content-Type", "")
            if len(resp.content) < 1000:
                print(f"  Descarga: respuesta muy pequeña ({len(resp.content)} bytes), posible error")
                return None
            return resp.content
        print(f"  Descarga HTTP {resp.status_code}")
        return None
    except Exception as exc:
        print(f"  Descarga error: {exc}")
        return None


# ═══════════════════════════════════════════════════════════════════════════
# PARSER HELPERS
# ═══════════════════════════════════════════════════════════════════════════

def _read_sheet(xf: pd.ExcelFile, sheet_name: str, required_cols: list) -> Optional[pd.DataFrame]:
    """
    Lee la hoja detectando automáticamente la fila de encabezado.
    Prueba header=0 primero; si no encuentra las columnas requeridas
    escanea las primeras 15 filas.
    """
    try:
        df = xf.parse(sheet_name, header=0)
        df.columns = [_normalizar_col(c) for c in df.columns]
        if all(c in df.columns for c in required_cols):
            return df
    except Exception:
        pass

    # Buscar fila de encabezado
    try:
        df_raw = xf.parse(sheet_name, header=None)
        for i in range(min(20, len(df_raw))):
            row_vals = [_normalizar_col(v) for v in df_raw.iloc[i].values]
            if all(any(req in rv for rv in row_vals) for req in required_cols):
                df2 = xf.parse(sheet_name, header=i)
                df2.columns = [_normalizar_col(c) for c in df2.columns]
                return df2
    except Exception as exc:
        print(f"  Error detectando header en '{sheet_name}': {exc}")

    # Devuelve con header=0 como fallback (columnas normalizadas)
    try:
        df = xf.parse(sheet_name, header=0)
        df.columns = [_normalizar_col(c) for c in df.columns]
        return df
    except Exception:
        return None


# ═══════════════════════════════════════════════════════════════════════════
# PARSER CREDITIENDA
# ═══════════════════════════════════════════════════════════════════════════

def _parse_creditienda(excel_bytes: bytes, source_file: str) -> list:
    """
    Hoja PEDIDOS      → event_type = 'venta'
    Hoja DEVOLUCIONES → event_type = 'devolucion'
    """
    records = []
    try:
        xf = pd.ExcelFile(io.BytesIO(excel_bytes))
    except Exception as exc:
        print(f"  Error abriendo Creditienda Excel: {exc}")
        return []

    for sheet_name in xf.sheet_names:
        upper = sheet_name.strip().upper()
        if "DEVOLUCION" in upper:
            event_type = "devolucion"
        elif "PEDIDO" in upper:
            event_type = "venta"
        else:
            continue

        try:
            df = _read_sheet(xf, sheet_name, ["sku"])
            if df is None or df.empty:
                continue

            for _, row in df.iterrows():
                sku_raw = _limpiar_str(row.get("sku", ""))
                if not sku_raw:
                    continue

                fecha = _parse_fecha(row.get("fecha"))
                if not fecha:
                    continue

                sku_base, mult = _normalizar_sku(sku_raw)
                cantidad_raw = _parse_precio(row.get("cantidad")) or 1.0
                cantidad = cantidad_raw * mult
                precio = _parse_precio(row.get("precio creditienda"))
                monto = round(precio * cantidad, 2) if precio is not None else None

                records.append({
                    "channel":         "creditienda",
                    "source_sheet":    sheet_name,
                    "event_type":      event_type,
                    "fecha":           fecha,
                    "remision":        _limpiar_str(row.get("remision", "")),
                    "pedido_externo":  _limpiar_str(row.get("pedido", "")),
                    "pedido_interno":  "",
                    "pedido_shopify":  _limpiar_str(row.get("pedido shopify", "")),
                    "pedido_simco":    _limpiar_str(row.get("pedido simco", "")),
                    "realizo":         _limpiar_str(row.get("realizo", "")),
                    "cliente":         _limpiar_str(row.get("nombre", "")),
                    "sku_raw":         sku_raw,
                    "sku_normalizado": sku_base,
                    "sku_base":        sku_base,
                    "multiplicador":   mult,
                    "descripcion":     _limpiar_str(row.get("descripcion", "")),
                    "cantidad":        cantidad,
                    "precio_unitario": precio,
                    "monto_bruto":     monto,
                    "comision":        None,
                    "monto_neto":      monto,
                    "ajuste":          _limpiar_str(row.get("ajuste", "")),
                    "guia":            _limpiar_str(row.get("guia", "")),
                })

        except Exception as exc:
            print(f"  Error en hoja Creditienda '{sheet_name}': {exc}")

    print(f"  Creditienda: {len(records)} registros parseados")
    return records


# ═══════════════════════════════════════════════════════════════════════════
# PARSER LIVERPOOL
# ═══════════════════════════════════════════════════════════════════════════

def _parse_remision_bloque(texto: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[date]]:
    """
    Parsea celda REMISION tipo:
      'Remision 13526\nPedido 12055\nAngélica \n08/01/26'
    Devuelve (remision, pedido_interno, realizo, fecha).
    También maneja REMISION simple (solo número).
    """
    lineas = [l.strip() for l in texto.replace("\\n", "\n").split("\n") if l.strip()]

    remision     = None
    pedido       = None
    realizo      = None
    fecha        = None
    nombre_visto = False

    for linea in lineas:
        l_up = linea.upper()
        if l_up.startswith("REMISION") or l_up.startswith("REMISIÓN"):
            nums = re.findall(r"\d+", linea)
            remision = nums[0] if nums else linea
        elif l_up.startswith("PEDIDO"):
            nums = re.findall(r"\d+", linea)
            pedido = nums[0] if nums else linea
        elif re.match(r"^\d{1,2}/\d{1,2}/\d{2,4}$", linea):
            fecha = _parse_fecha(linea)
        elif not nombre_visto and not re.match(r"^\d", linea) and len(linea) > 1:
            realizo = linea
            nombre_visto = True

    # Si no hay prefijo "Remision", el texto podría ser solo un número
    if remision is None:
        nums = re.findall(r"\d+", texto.split("\n")[0])
        if nums:
            remision = nums[0]

    return remision, pedido, realizo, fecha


def _parse_liverpool_sheet(df: pd.DataFrame, sheet_name: str, event_type: str) -> list:
    """
    Parsea una hoja de Liverpool.
    Soporta DOS formatos:
      A) REMISION con bloque multi-línea ("Remision 13526\\nPedido 12055\\nAngélica\\n08/01/26")
      B) Columnas separadas REMISION + PEDIDO + REALIZO + FECHA (nuevo formato)
    En ambos casos hace fill-down del contexto de bloque para filas sin REMISION.
    """
    records = []

    cur_remision = None
    cur_pedido   = None
    cur_realizo  = None
    cur_fecha    = None

    # Detectar si existe columna FECHA separada
    tiene_col_fecha   = "fecha"   in df.columns
    tiene_col_realizo = "realizo" in df.columns
    tiene_col_pedido  = "pedido"  in df.columns

    for _, row in df.iterrows():
        remision_raw = _limpiar_str(row.get("remision", ""))

        # ── Columnas separadas (Formato B) ───────────────────────────────
        fecha_col   = _parse_fecha(row.get("fecha"))    if tiene_col_fecha   else None
        realizo_col = _limpiar_str(row.get("realizo", "")) if tiene_col_realizo else ""
        pedido_col  = _limpiar_str(row.get("pedido",  "")) if tiene_col_pedido  else ""

        if remision_raw:
            # Intentar parsear bloque (Formato A)
            r, p, re_, f = _parse_remision_bloque(remision_raw)
            cur_remision = r or remision_raw

            # Fecha: primero del bloque, luego columna FECHA
            cur_fecha  = f or fecha_col
            # Pedido: del bloque o columna separada
            cur_pedido = p or pedido_col or ""
            # Realizo: del bloque o columna separada
            cur_realizo = re_ or realizo_col or ""

        elif fecha_col is not None:
            # Fila sin REMISION pero con FECHA explícita → nueva cabecera en Formato B
            cur_fecha = fecha_col
            if pedido_col:  cur_pedido  = pedido_col
            if realizo_col: cur_realizo = realizo_col
        # else: fill-down del bloque anterior (cur_* sin cambios)

        sku_raw = _limpiar_str(row.get("sku", ""))
        if not sku_raw:
            continue
        if cur_fecha is None:
            continue

        sku_base, mult = _normalizar_sku(sku_raw)
        cantidad_raw   = _parse_precio(row.get("cantidad")) or 1.0
        cantidad       = cantidad_raw * mult

        precio      = _parse_precio(row.get("precio liverpool"))
        total_sai   = _parse_precio(row.get("total sai"))
        sin_iva_sai = _parse_precio(row.get("sin iva sai"))
        comision    = _parse_precio(row.get("comision"))

        if total_sai is not None:
            monto_bruto = total_sai
        elif precio is not None:
            monto_bruto = round(precio * cantidad, 2)
        else:
            monto_bruto = None

        # PEDIDO.1 = segundo campo PEDIDO (pandas auto-renombra duplicados)
        pedido_ext = _limpiar_str(
            row.get("pedido.1", "") or row.get("pedido 1", "") or pedido_col
        )

        records.append({
            "channel":         "liverpool",
            "source_sheet":    sheet_name,
            "event_type":      event_type,
            "fecha":           cur_fecha,
            "remision":        cur_remision or "",
            "pedido_externo":  pedido_ext,
            "pedido_interno":  cur_pedido or "",
            "pedido_shopify":  "",
            "pedido_simco":    "",
            "realizo":         cur_realizo or "",
            "cliente":         "",
            "sku_raw":         sku_raw,
            "sku_normalizado": sku_base,
            "sku_base":        sku_base,
            "multiplicador":   mult,
            "descripcion":     _limpiar_str(row.get("descripcion", "")),
            "cantidad":        cantidad,
            "precio_unitario": precio,
            "monto_bruto":     monto_bruto,
            "comision":        comision,
            "monto_neto":      sin_iva_sai,
            "ajuste":          "",
            "guia":            _limpiar_str(row.get("guia", "")),
        })

    return records


def _parse_liverpool(excel_bytes: bytes, source_file: str) -> list:
    """
    Hoja 'LIVERPOOL 2026'         → event_type = 'venta'
    Hoja 'DEVOLUCIONES LIVERPOOL' → event_type = 'devolucion'
    """
    records = []
    try:
        xf = pd.ExcelFile(io.BytesIO(excel_bytes))
    except Exception as exc:
        print(f"  Error abriendo Liverpool Excel: {exc}")
        return []

    sheet_map = {}
    for s in xf.sheet_names:
        up = s.strip().upper()
        if "DEVOLUCION" in up and "LIVERPOOL" in up:
            sheet_map[s] = "devolucion"
        elif "LIVERPOOL" in up and "DEVOLUCION" not in up and "MASTER" not in up:
            # Captura: LIVERPOOL 2026, LIVERPOOL ABR 2026, Liverpool 2025, etc.
            sheet_map[s] = "venta"

    if not sheet_map:
        print(f"  Liverpool: no se encontraron hojas esperadas. Hojas disponibles: {xf.sheet_names}")

    for sheet_name, event_type in sheet_map.items():
        try:
            df = _read_sheet(xf, sheet_name, ["sku", "cantidad"])
            if df is None or df.empty:
                continue
            sheet_records = _parse_liverpool_sheet(df, sheet_name, event_type)
            records.extend(sheet_records)
            print(f"  Liverpool '{sheet_name}': {len(sheet_records)} registros")
        except Exception as exc:
            print(f"  Error en hoja Liverpool '{sheet_name}': {exc}")

    return records


# ═══════════════════════════════════════════════════════════════════════════
# SINCRONIZACIÓN
# ═══════════════════════════════════════════════════════════════════════════

def sincronizar_ventas(cred_url_override: str = None, liv_url_override: str = None) -> dict:
    """
    Descarga ambos Excel, parsea e inserta en SQLite. Idempotente por raw_hash.
    Si se pasan cred_url_override / liv_url_override se usan directamente
    (el script local sync_ventas.py los resuelve y los envía al servidor).
    Fallback: env vars CREDITIENDA_FILEGETURL / LIVERPOOL_FILEGETURL.
    Último fallback: intenta resolver el share link desde el propio servidor.
    """
    resultado = {
        "ok":          False,
        "creditienda": {},
        "liverpool":   {},
        "errores":     [],
    }

    # ── Creditienda ──────────────────────────────────────────────────────
    cred_url = (
        cred_url_override
        or os.getenv("CREDITIENDA_FILEGETURL", "").strip()
        or _resolve_onedrive(CREDITIENDA_SHARE_URL)
    )
    if cred_url:
        print("  Descargando Creditienda...")
        cred_bytes = _download_excel(cred_url)
        if cred_bytes:
            recs = _parse_creditienda(cred_bytes, "creditienda.xlsx")
            stats = _insert_records(recs, "creditienda.xlsx")
            resultado["creditienda"] = {"ok": True, "registros_parseados": len(recs), **stats}
        else:
            msg = "No se pudo descargar Creditienda"
            resultado["creditienda"] = {"ok": False, "error": msg}
            resultado["errores"].append(msg)
    else:
        msg = "No se pudo resolver URL de Creditienda"
        resultado["creditienda"] = {"ok": False, "error": msg}
        resultado["errores"].append(msg)

    # ── Liverpool ─────────────────────────────────────────────────────────
    liv_url = (
        liv_url_override
        or os.getenv("LIVERPOOL_FILEGETURL", "").strip()
        or _resolve_onedrive(LIVERPOOL_SHARE_URL)
    )
    if liv_url:
        print("  Descargando Liverpool...")
        liv_bytes = _download_excel(liv_url)
        if liv_bytes:
            recs = _parse_liverpool(liv_bytes, "liverpool.xlsx")
            stats = _insert_records(recs, "liverpool.xlsx")
            resultado["liverpool"] = {"ok": True, "registros_parseados": len(recs), **stats}
        else:
            msg = "No se pudo descargar Liverpool"
            resultado["liverpool"] = {"ok": False, "error": msg}
            resultado["errores"].append(msg)
    else:
        msg = "No se pudo resolver URL de Liverpool"
        resultado["liverpool"] = {"ok": False, "error": msg}
        resultado["errores"].append(msg)

    resultado["ok"] = (
        resultado["creditienda"].get("ok", False)
        or resultado["liverpool"].get("ok", False)
    )
    return resultado


# ═══════════════════════════════════════════════════════════════════════════
# HELPERS DE PERÍODO
# ═══════════════════════════════════════════════════════════════════════════

def _hoy() -> date:
    return datetime.now(_CDMX_TZ).date()


def _rango_hoy() -> Tuple[date, date]:
    d = _hoy()
    return d, d


def _rango_semana() -> Tuple[date, date]:
    d = _hoy()
    inicio = d - timedelta(days=d.weekday())   # lunes
    fin    = inicio + timedelta(days=6)
    return inicio, fin


def _rango_quincena() -> Tuple[date, date]:
    d = _hoy()
    if d.day <= 15:
        return d.replace(day=1), d.replace(day=15)
    ultimo = calendar.monthrange(d.year, d.month)[1]
    return d.replace(day=16), d.replace(day=ultimo)


def _rango_mes() -> Tuple[date, date]:
    d = _hoy()
    ultimo = calendar.monthrange(d.year, d.month)[1]
    return d.replace(day=1), d.replace(day=ultimo)


def _rango_personalizado(desde_str: str, hasta_str: str) -> Tuple[date, date]:
    try:
        return date.fromisoformat(desde_str), date.fromisoformat(hasta_str)
    except Exception:
        return _hoy(), _hoy()


_RANGOS = {
    "hoy":       _rango_hoy,
    "semana":    _rango_semana,
    "quincena":  _rango_quincena,
    "mes":       _rango_mes,
}


def _get_rango(periodo: str, desde_str: str = "", hasta_str: str = "") -> Tuple[date, date]:
    if desde_str and hasta_str:
        return _rango_personalizado(desde_str, hasta_str)
    return _RANGOS.get(periodo, _rango_mes)()


# ═══════════════════════════════════════════════════════════════════════════
# QUERIES
# ═══════════════════════════════════════════════════════════════════════════

def _kpis_rango(desde: date, hasta: date, canal: str = None) -> dict:
    params = {"desde": desde.isoformat(), "hasta": hasta.isoformat()}
    canal_filter = ""
    if canal:
        canal_filter = "AND channel = :canal"
        params["canal"] = canal

    with _get_db() as conn:
        rows = conn.execute(f"""
            SELECT event_type, channel,
                   COALESCE(SUM(cantidad), 0)    AS piezas,
                   COALESCE(SUM(monto_bruto), 0) AS monto
            FROM ventas_eventos
            WHERE fecha BETWEEN :desde AND :hasta
              AND fecha IS NOT NULL
              {canal_filter}
            GROUP BY event_type, channel
        """, params).fetchall()

    v_piezas = v_monto = d_piezas = d_monto = 0.0
    por_canal: dict = {}

    for row in rows:
        ch     = row["channel"] or "desconocido"
        piezas = row["piezas"] or 0.0
        monto  = row["monto"]  or 0.0

        if ch not in por_canal:
            por_canal[ch] = {"piezas": 0.0, "monto": 0.0}

        if row["event_type"] == "venta":
            v_piezas += piezas
            v_monto  += monto
            por_canal[ch]["piezas"] += piezas
            por_canal[ch]["monto"]  += monto
        else:
            d_piezas += piezas
            d_monto  += monto

    neto_piezas = v_piezas - d_piezas
    neto_monto  = v_monto  - d_monto
    ticket      = round(v_monto / v_piezas, 2) if v_piezas else 0.0

    return {
        "desde":               desde.isoformat(),
        "hasta":               hasta.isoformat(),
        "ventas_piezas":       round(v_piezas,   2),
        "ventas_monto":        round(v_monto,     2),
        "devoluciones_piezas": round(d_piezas,   2),
        "devoluciones_monto":  round(d_monto,     2),
        "neto_piezas":         round(neto_piezas, 2),
        "neto_monto":          round(neto_monto,  2),
        "ticket_promedio":     ticket,
        "por_canal":           {k: {kk: round(vv, 2) for kk, vv in v.items()} for k, v in por_canal.items()},
    }


# ═══════════════════════════════════════════════════════════════════════════
# RUTAS FLASK
# ═══════════════════════════════════════════════════════════════════════════

@ventas_bp.route("/ventas")
def ventas_panel():
    return render_template_string(HTML_VENTAS)


@ventas_bp.route("/api/ventas/sync", methods=["POST"])
def api_ventas_sync():
    try:
        body = request.get_json(silent=True) or {}
        cred_url = body.get("creditienda_url", "").strip() or None
        liv_url  = body.get("liverpool_url",  "").strip() or None
        result = sincronizar_ventas(cred_url, liv_url)
        return jsonify(result)
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@ventas_bp.route("/api/ventas/resumen")
def api_ventas_resumen():
    canal     = request.args.get("canal",  "").strip().lower() or None
    desde_str = request.args.get("desde", "").strip()
    hasta_str = request.args.get("hasta", "").strip()

    if desde_str and hasta_str:
        periodos = {"personalizado": _rango_personalizado(desde_str, hasta_str)}
    else:
        periodos = {k: fn() for k, fn in _RANGOS.items()}

    resultado = {nombre: _kpis_rango(d, h, canal) for nombre, (d, h) in periodos.items()}
    return jsonify(resultado)


@ventas_bp.route("/api/ventas/por-sku")
def api_ventas_por_sku():
    canal     = request.args.get("canal",   "").strip().lower() or None
    periodo   = request.args.get("periodo", "mes").strip()
    desde_str = request.args.get("desde",  "").strip()
    hasta_str = request.args.get("hasta",  "").strip()
    q         = request.args.get("q",      "").strip().lower()

    desde, hasta = _get_rango(periodo, desde_str, hasta_str)

    params = {"desde": desde.isoformat(), "hasta": hasta.isoformat()}
    canal_filter = ""
    if canal:
        canal_filter = "AND channel = :canal"
        params["canal"] = canal

    with _get_db() as conn:
        rows = conn.execute(f"""
            SELECT sku_normalizado, descripcion, channel,
                   SUM(CASE WHEN event_type='venta' THEN cantidad    ELSE -cantidad    END) AS piezas,
                   SUM(CASE WHEN event_type='venta' THEN monto_bruto ELSE -monto_bruto END) AS monto
            FROM ventas_eventos
            WHERE fecha BETWEEN :desde AND :hasta
              AND fecha IS NOT NULL
              AND sku_normalizado != ''
              {canal_filter}
            GROUP BY sku_normalizado, descripcion, channel
            ORDER BY piezas DESC
        """, params).fetchall()

    # Pivot por SKU
    skus: dict = {}
    for row in rows:
        sku    = row["sku_normalizado"]
        desc   = (row["descripcion"] or "").strip()
        piezas = round(row["piezas"] or 0.0, 2)
        monto  = round(row["monto"]  or 0.0, 2)

        # Filtro de búsqueda
        if q and q not in sku and q not in desc.lower():
            continue

        if sku not in skus:
            skus[sku] = {
                "sku_normalizado":     sku,
                "descripcion":         desc,
                "piezas_total":        0.0,
                "piezas_liverpool":    0.0,
                "piezas_creditienda":  0.0,
                "monto_liverpool":     0.0,
                "monto_creditienda":   0.0,
                "monto_total":         0.0,
            }
        else:
            if desc and not skus[sku]["descripcion"]:
                skus[sku]["descripcion"] = desc

        skus[sku]["piezas_total"] += piezas
        skus[sku]["monto_total"]  += monto

        if row["channel"] == "liverpool":
            skus[sku]["piezas_liverpool"]   += piezas
            skus[sku]["monto_liverpool"]    += monto
        elif row["channel"] == "creditienda":
            skus[sku]["piezas_creditienda"] += piezas
            skus[sku]["monto_creditienda"]  += monto

    items = sorted(skus.values(), key=lambda x: x["piezas_total"], reverse=True)
    # Redondear
    for item in items:
        for k in ("piezas_total","piezas_liverpool","piezas_creditienda","monto_liverpool","monto_creditienda","monto_total"):
            item[k] = round(item[k], 2)

    return jsonify({"desde": desde.isoformat(), "hasta": hasta.isoformat(), "items": items})


@ventas_bp.route("/api/ventas/calendario")
def api_ventas_calendario():
    try:
        year  = int(request.args.get("year",  _hoy().year))
        month = int(request.args.get("month", _hoy().month))
    except ValueError:
        year, month = _hoy().year, _hoy().month

    canal     = request.args.get("canal", "").strip().lower() or None
    desde     = date(year, month, 1)
    hasta     = date(year, month, calendar.monthrange(year, month)[1])

    params = {"desde": desde.isoformat(), "hasta": hasta.isoformat()}
    canal_filter = ""
    if canal:
        canal_filter = "AND channel = :canal"
        params["canal"] = canal

    with _get_db() as conn:
        rows = conn.execute(f"""
            SELECT fecha,
                   SUM(CASE WHEN event_type='venta' THEN cantidad    ELSE 0 END) AS piezas,
                   SUM(CASE WHEN event_type='venta' THEN monto_bruto ELSE 0 END) AS monto
            FROM ventas_eventos
            WHERE fecha BETWEEN :desde AND :hasta
              AND fecha IS NOT NULL
              {canal_filter}
            GROUP BY fecha
        """, params).fetchall()

    dias = {
        row["fecha"]: {
            "piezas": round(row["piezas"] or 0, 2),
            "monto":  round(row["monto"]  or 0, 2),
        }
        for row in rows
    }
    return jsonify({"year": year, "month": month, "dias": dias})


@ventas_bp.route("/api/ventas/top-skus")
def api_ventas_top_skus():
    periodo   = request.args.get("periodo", "mes")
    canal     = request.args.get("canal",   "").strip().lower() or None
    limit_val = min(int(request.args.get("limit", "10")), 50)
    desde, hasta = _get_rango(periodo)

    params = {"desde": desde.isoformat(), "hasta": hasta.isoformat(), "limit": limit_val}
    canal_filter = ""
    if canal:
        canal_filter = "AND channel = :canal"
        params["canal"] = canal

    with _get_db() as conn:
        rows = conn.execute(f"""
            SELECT sku_normalizado, descripcion,
                   SUM(CASE WHEN event_type='venta' THEN cantidad    ELSE -cantidad    END) AS piezas,
                   SUM(CASE WHEN event_type='venta' THEN monto_bruto ELSE -monto_bruto END) AS monto
            FROM ventas_eventos
            WHERE fecha BETWEEN :desde AND :hasta
              AND fecha IS NOT NULL
              AND sku_normalizado != ''
              {canal_filter}
            GROUP BY sku_normalizado
            ORDER BY piezas DESC
            LIMIT :limit
        """, params).fetchall()

    return jsonify([dict(r) for r in rows])


@ventas_bp.route("/api/ventas/debug/fuentes")
def api_ventas_debug():
    cred_env = os.getenv("CREDITIENDA_FILEGETURL", "")
    liv_env  = os.getenv("LIVERPOOL_FILEGETURL",  "")

    with _get_db() as conn:
        total     = conn.execute("SELECT COUNT(*) AS n FROM ventas_eventos").fetchone()["n"]
        por_fuente = conn.execute("""
            SELECT channel, event_type, COUNT(*) AS n, MAX(fecha) AS fecha_max,
                   MAX(synced_at) AS ultimo_sync
            FROM ventas_eventos
            GROUP BY channel, event_type
        """).fetchall()
        ultima = conn.execute("SELECT MAX(synced_at) AS ts FROM ventas_eventos").fetchone()["ts"]

    return jsonify({
        "db_file":          _DB_FILE,
        "total_registros":  total,
        "ultimo_sync":      ultima,
        "env_vars": {
            "CREDITIENDA_FILEGETURL": "configurado" if cred_env else "no configurado",
            "LIVERPOOL_FILEGETURL":   "configurado" if liv_env  else "no configurado",
        },
        "por_fuente": [dict(r) for r in por_fuente],
    })


# ═══════════════════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════════════════
# HTML — PANEL VENTAS  (diseño inspirado en ML Ventas en Vivo)
# ═══════════════════════════════════════════════════════════════════════════


# ═══════════════════════════════════════════════════════════════════════════
# HTML — PANEL VENTAS
# ═══════════════════════════════════════════════════════════════════════════

HTML_VENTAS = """<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Ventas · PATISH</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.3/dist/chart.umd.min.js"></script>
<style>
*{box-sizing:border-box;margin:0;padding:0}
:root{
  --green:#147a54;--green2:#0f5f43;--green-light:#e6f4ed;
  --blue:#1a6fa8;--blue-light:#e8f2fa;
  --orange:#cb6f2d;--orange-light:#fdf0e6;
  --red:#c94f5d;--red-light:#fdeef0;
  --text:#1a2026;--muted:#6b7280;--border:#e5e7eb;
  --bg:#f9fafb;--card:#fff;
  --shadow:0 1px 3px rgba(0,0,0,.08),0 4px 16px rgba(0,0,0,.05);
}
body{background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;min-height:100vh}

/* Header */
.hdr{background:#fff;border-bottom:1px solid var(--border);padding:12px 28px;display:flex;align-items:center;gap:12px;position:sticky;top:0;z-index:30;box-shadow:0 1px 4px rgba(0,0,0,.06)}
.hdr-logo{font-size:1.05rem;font-weight:800;color:var(--text)}
.hdr-logo span{color:var(--green)}
.nav-pill{font-size:.73rem;padding:6px 14px;border-radius:999px;border:1px solid var(--border);background:transparent;color:var(--muted);text-decoration:none;transition:all .15s}
.nav-pill:hover{border-color:var(--green);color:var(--green)}
.nav-pill.on{background:var(--green);border-color:var(--green);color:#fff}
.hdr-r{margin-left:auto;display:flex;align-items:center;gap:10px}
.sync-ts{font-size:.68rem;color:var(--muted)}
.btn-sync{font-size:.73rem;font-weight:600;padding:8px 18px;border-radius:999px;border:none;background:var(--green);color:#fff;cursor:pointer;display:flex;align-items:center;gap:6px;transition:all .15s}
.btn-sync:hover{background:var(--green2);transform:translateY(-1px);box-shadow:0 4px 12px rgba(20,122,84,.3)}
.btn-sync:disabled{opacity:.55;cursor:not-allowed;transform:none;box-shadow:none}
.btn-sync svg{width:14px;height:14px;flex-shrink:0}

/* Hero */
.hero{background:linear-gradient(135deg,#0d5c3d 0%,#147a54 50%,#1a9463 100%);padding:32px 28px 48px;position:relative;overflow:hidden}
.hero::before{content:'';position:absolute;inset:0;background:radial-gradient(ellipse at 80% 50%,rgba(255,255,255,.08),transparent 60%)}
.hero::after{content:'';position:absolute;bottom:-1px;left:0;right:0;height:32px;background:var(--bg);clip-path:ellipse(55% 100% at 50% 100%)}
.hero-in{max-width:1400px;margin:0 auto;position:relative}
.hero-badge{display:inline-flex;align-items:center;gap:6px;background:rgba(255,255,255,.15);border:1px solid rgba(255,255,255,.25);border-radius:999px;padding:5px 14px;font-size:.7rem;color:rgba(255,255,255,.9);margin-bottom:14px}
.live-dot{width:7px;height:7px;background:#4ade80;border-radius:50%;animation:blink 2s infinite}
@keyframes blink{0%,100%{opacity:1}50%{opacity:.25}}
.hero-num{font-size:clamp(2.2rem,5.5vw,3.8rem);font-weight:800;color:#fff;letter-spacing:-.03em;line-height:1;margin-bottom:5px}
.hero-sub{font-size:.82rem;color:rgba(255,255,255,.7);margin-bottom:22px}

/* Period tabs */
.ptabs{display:flex;gap:7px;flex-wrap:wrap;align-items:center}
.ptab{font-size:.74rem;font-weight:600;padding:8px 18px;border-radius:999px;border:1.5px solid rgba(255,255,255,.3);background:rgba(255,255,255,.12);color:rgba(255,255,255,.85);cursor:pointer;transition:all .15s;backdrop-filter:blur(4px)}
.ptab:hover{background:rgba(255,255,255,.22);border-color:rgba(255,255,255,.5)}
.ptab.on{background:#fff;border-color:#fff;color:var(--green);box-shadow:0 2px 12px rgba(0,0,0,.15)}
.ptabs-sep{width:1px;height:20px;background:rgba(255,255,255,.25);margin:0 4px}

/* Date range inputs in hero */
.date-row{display:flex;align-items:center;gap:6px}
.date-inp{background:rgba(255,255,255,.15);border:1.5px solid rgba(255,255,255,.3);border-radius:12px;padding:7px 11px;font-size:.72rem;color:#fff;cursor:pointer;transition:all .15s;width:130px}
.date-inp::-webkit-calendar-picker-indicator{filter:invert(1);opacity:.7}
.date-inp:focus{outline:none;border-color:rgba(255,255,255,.7);background:rgba(255,255,255,.22)}
.date-sep{font-size:.7rem;color:rgba(255,255,255,.6)}

/* Main */
.main{max-width:1400px;margin:0 auto;padding:24px 28px 48px}

/* Grid 3 cols */
.g3{display:grid;grid-template-columns:270px 1fr 280px;gap:18px;margin-bottom:20px}
@media(max-width:1100px){.g3{grid-template-columns:1fr 1fr}}
@media(max-width:680px){.g3{grid-template-columns:1fr}}

/* Cards */
.card{background:var(--card);border:1px solid var(--border);border-radius:18px;padding:20px;box-shadow:var(--shadow)}
.card-ttl{font-size:.73rem;font-weight:700;text-transform:uppercase;letter-spacing:.08em;color:var(--muted);margin-bottom:16px}

/* Métricas */
.mgrid{display:grid;grid-template-columns:1fr 1fr;gap:14px}
.mi{display:flex;flex-direction:column;gap:3px}
.mi-icon{width:32px;height:32px;border-radius:9px;display:flex;align-items:center;justify-content:center;margin-bottom:5px;font-size:.9rem}
.g{background:var(--green-light);color:var(--green)}
.b{background:var(--blue-light);color:var(--blue)}
.o{background:var(--orange-light);color:var(--orange)}
.r{background:var(--red-light);color:var(--red)}
.mi-lbl{font-size:.65rem;color:var(--muted)}
.mi-val{font-size:1.3rem;font-weight:800;letter-spacing:-.02em;line-height:1.1}
.mi-val.g{color:var(--green);background:none}
.mi-val.b{color:var(--blue);background:none}
.mi-val.o{color:var(--orange);background:none}
.mi-val.r{color:var(--red);background:none}

/* Chart */
.chart-wrap{position:relative;height:190px}

/* Top SKUs */
.rank-list{display:flex;flex-direction:column;gap:8px}
.rank-row{display:flex;align-items:flex-start;gap:10px;padding:9px 10px;border-radius:12px;border:1px solid transparent;transition:background .12s;cursor:default}
.rank-row:hover{background:var(--bg)}
.rank-row.r1{background:var(--green-light);border-color:rgba(20,122,84,.15)}
.rn{font-size:.75rem;font-weight:800;color:var(--muted);min-width:16px;padding-top:1px}
.r1 .rn{color:var(--green)}
.ri{flex:1;min-width:0}
.ri-name{font-size:.7rem;font-weight:600;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.ri-sku{font-size:.6rem;color:var(--muted);margin-top:1px}
.rr{text-align:right;flex-shrink:0}
.rr-monto{font-size:.76rem;font-weight:700}
.rr-pzas{font-size:.6rem;color:var(--muted)}

/* Calendario */
.cal-wrap{background:var(--card);border:1px solid var(--border);border-radius:18px;padding:20px;box-shadow:var(--shadow);margin-bottom:20px}
.cal-nav{display:flex;align-items:center;gap:10px;margin-bottom:14px}
.cal-title{font-size:.9rem;font-weight:700;flex:1}
.cal-btn{background:var(--bg);border:1px solid var(--border);border-radius:999px;padding:5px 14px;font-size:.7rem;cursor:pointer;transition:all .12s}
.cal-btn:hover{border-color:var(--green);color:var(--green)}
.cal-grid{display:grid;grid-template-columns:repeat(7,1fr);gap:3px}
.dn{text-align:center;font-size:.58rem;color:var(--muted);padding:4px 0;text-transform:uppercase;letter-spacing:.06em;font-weight:600}
.dc{border-radius:8px;min-height:62px;padding:5px 6px;transition:transform .1s,box-shadow .1s;position:relative}
.dc.vacio{background:transparent}
.dc.sin{background:rgba(229,231,235,.45)}
.dc.con{cursor:pointer}
.dc.con:hover{transform:scale(1.05);box-shadow:0 4px 12px rgba(0,0,0,.12);z-index:2}
.dc.sel{outline:2px solid var(--green);outline-offset:1px;z-index:3}
.dc.hoy{outline:2px solid rgba(20,122,84,.4);outline-offset:1px}
.dc-day{font-size:.65rem;font-weight:700;margin-bottom:2px}
.dc-pzas{font-size:.55rem;opacity:.75}
.dc-monto{font-size:.6rem;font-weight:700}

/* Tabla */
.tbl-hdr{display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin-bottom:12px}
.tbl-ttl{font-size:.85rem;font-weight:700;flex:1}
.badge{font-size:.67rem;background:var(--bg);border:1px solid var(--border);color:var(--muted);padding:3px 10px;border-radius:999px}
.filts{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:12px}
select,input[type=text]{font-size:.73rem;padding:8px 12px;border:1px solid var(--border);border-radius:10px;background:#fff;color:var(--text);outline:none;transition:border-color .15s}
select:focus,input[type=text]:focus{border-color:var(--green);box-shadow:0 0 0 3px rgba(20,122,84,.1)}
input[type=text]{min-width:180px}
.tw{overflow:auto;border-radius:12px;border:1px solid var(--border)}
table{width:100%;border-collapse:collapse;font-size:.76rem}
th{text-align:left;font-size:.62rem;font-weight:700;text-transform:uppercase;letter-spacing:.07em;color:var(--muted);padding:10px 13px;border-bottom:1px solid var(--border);white-space:nowrap;background:#fafafa;cursor:pointer;user-select:none}
th:hover{color:var(--green)}
td{padding:11px 13px;border-bottom:1px solid var(--border);vertical-align:middle}
tbody tr:last-child td{border-bottom:none}
tbody tr:hover td{background:#f7faf8}
.num{text-align:right;font-variant-numeric:tabular-nums;font-weight:600}
.sku-tag{font-size:.67rem;background:var(--blue-light);color:var(--blue);border-radius:6px;padding:3px 8px;font-weight:600}
.lv{color:var(--blue);font-weight:600}
.ct{color:var(--orange);font-weight:600}
.tot{color:var(--green);font-weight:700}
.empty{text-align:center;color:var(--muted);padding:32px;font-size:.78rem}

/* Msg */
.msg{padding:10px 16px;border-radius:12px;font-size:.73rem;margin-bottom:12px;display:none}
.msg-ok{background:var(--green-light);color:var(--green);border:1px solid rgba(20,122,84,.2)}
.msg-err{background:var(--red-light);color:var(--red);border:1px solid rgba(201,79,93,.2)}
</style>
</head>
<body>

<!-- Header -->
<div class="hdr">
  <div class="hdr-logo">PATISH <span>●</span></div>
  <a href="/" class="nav-pill">BuyBox</a>
  <a href="/ventas" class="nav-pill on">Ventas</a>
  <div class="hdr-r">
    <span class="sync-ts" id="sync-ts">–</span>
    <button class="btn-sync" id="btn-sync" onclick="sincronizar()">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><path d="M1 4v6h6M23 20v-6h-6"/><path d="M20.49 9A9 9 0 0 0 5.64 5.64L1 10m22 4-4.64 4.36A9 9 0 0 1 3.51 15"/></svg>
      Sincronizar
    </button>
  </div>
</div>

<!-- Hero -->
<div class="hero">
  <div class="hero-in">
    <div class="hero-badge"><span class="live-dot"></span><span id="hero-rango">–</span></div>
    <div class="hero-num" id="hero-num">$ –</div>
    <div class="hero-sub" id="hero-sub">– unidades · –</div>
    <div class="ptabs">
      <button class="ptab on" data-p="hoy"      onclick="setPer(this,'hoy')">Hoy</button>
      <button class="ptab"    data-p="ayer"     onclick="setPer(this,'ayer')">Ayer</button>
      <button class="ptab"    data-p="semana"   onclick="setPer(this,'semana')">Semana</button>
      <button class="ptab"    data-p="quincena" onclick="setPer(this,'quincena')">Quincena</button>
      <button class="ptab"    data-p="mes"      onclick="setPer(this,'mes')">Mes</button>
      <div class="ptabs-sep"></div>
      <div class="date-row">
        <input class="date-inp" type="date" id="inp-desde" onchange="setRango()">
        <span class="date-sep">–</span>
        <input class="date-inp" type="date" id="inp-hasta" onchange="setRango()">
      </div>
    </div>
  </div>
</div>

<!-- Main -->
<div class="main">
  <div class="msg" id="msg-sync"></div>

  <!-- 3 columnas -->
  <div class="g3">
    <!-- Métricas -->
    <div class="card">
      <div class="card-ttl">Métricas clave</div>
      <div class="mgrid">
        <div class="mi"><div class="mi-icon g">📦</div><div class="mi-lbl">Unidades</div><div class="mi-val g" id="m-pzas">–</div></div>
        <div class="mi"><div class="mi-icon g">💰</div><div class="mi-lbl">Ticket prom.</div><div class="mi-val" id="m-tkt">–</div></div>
        <div class="mi"><div class="mi-icon b">🏪</div><div class="mi-lbl">Liverpool</div><div class="mi-val b" id="m-lv">–</div></div>
        <div class="mi"><div class="mi-icon o">🛒</div><div class="mi-lbl">Creditienda</div><div class="mi-val o" id="m-ct">–</div></div>
        <div class="mi"><div class="mi-icon r">↩</div><div class="mi-lbl">Devoluciones</div><div class="mi-val r" id="m-dev">–</div></div>
        <div class="mi"><div class="mi-icon g">📈</div><div class="mi-lbl">Monto neto</div><div class="mi-val g" id="m-neto">–</div></div>
      </div>
    </div>

    <!-- Gráfica -->
    <div class="card">
      <div class="card-ttl" id="chart-ttl">Tendencia diaria</div>
      <div class="chart-wrap"><canvas id="vchart"></canvas></div>
    </div>

    <!-- Top SKUs -->
    <div class="card">
      <div class="card-ttl">Más vendidos</div>
      <div class="rank-list" id="top-list"><div style="color:var(--muted);font-size:.74rem">Cargando…</div></div>
    </div>
  </div>

  <!-- Calendario clickeable -->
  <div class="cal-wrap">
    <div class="cal-nav">
      <button class="cal-btn" onclick="calMes(-1)">‹</button>
      <span class="cal-title" id="cal-ttl">–</span>
      <button class="cal-btn" onclick="calMes(1)">›</button>
      <span style="margin-left:auto;font-size:.68rem;color:var(--muted)" id="cal-sel-lbl"></span>
      <button class="cal-btn" onclick="limpiarCalSel()" id="cal-clear" style="display:none">✕ Quitar filtro</button>
    </div>
    <div class="cal-grid" id="cal-grid">
      <div class="dn">Lun</div><div class="dn">Mar</div><div class="dn">Mié</div>
      <div class="dn">Jue</div><div class="dn">Vie</div><div class="dn">Sáb</div><div class="dn">Dom</div>
    </div>
  </div>

  <!-- Tabla -->
  <div class="card">
    <div class="tbl-hdr">
      <span class="tbl-ttl">Detalle por SKU</span>
      <span class="badge" id="sku-cnt">–</span>
    </div>
    <div class="filts">
      <select id="sel-canal" onchange="cargarTabla()">
        <option value="">Todos los canales</option>
        <option value="liverpool">Liverpool</option>
        <option value="creditienda">Creditienda</option>
      </select>
      <input type="text" id="inp-q" placeholder="Buscar SKU o descripción…" oninput="cargarTabla()">
    </div>
    <div class="tw">
      <table>
        <thead><tr>
          <th onclick="sortBy('sku_normalizado')">SKU</th>
          <th onclick="sortBy('descripcion')">Descripción</th>
          <th class="num" onclick="sortBy('piezas_total')">Piezas ↕</th>
          <th class="num" onclick="sortBy('piezas_liverpool')">Pzas LV</th>
          <th class="num" onclick="sortBy('piezas_creditienda')">Pzas CT</th>
          <th class="num" onclick="sortBy('monto_liverpool')">$ Liverpool</th>
          <th class="num" onclick="sortBy('monto_creditienda')">$ CT</th>
          <th class="num" onclick="sortBy('monto_total')">$ Total</th>
        </tr></thead>
        <tbody id="tbody-sku"><tr><td colspan="8" class="empty">Cargando…</td></tr></tbody>
      </table>
    </div>
  </div>

</div>

<script>
// ── Estado ────────────────────────────────────────────────────────────────
let desde = '', hasta = '';
let skuData = [], sortField = 'piezas_total', sortDir = 'desc';
let chart = null;
let calY, calM, calDias = {}, calSelDate = null;

const MESES = ['Enero','Febrero','Marzo','Abril','Mayo','Junio',
               'Julio','Agosto','Septiembre','Octubre','Noviembre','Diciembre'];

// ── Utilidades ────────────────────────────────────────────────────────────
function hoyStr(){
  const d=new Date();
  return `${d.getFullYear()}-${p2(d.getMonth()+1)}-${p2(d.getDate())}`;
}
function p2(n){return String(n).padStart(2,'0')}
function addDays(str,n){
  const d=new Date(str+'T12:00:00');d.setDate(d.getDate()+n);
  return `${d.getFullYear()}-${p2(d.getMonth()+1)}-${p2(d.getDate())}`;
}
function fmt$(n){
  if(n==null||isNaN(n))return'–';
  const abs=Math.abs(n);
  if(abs>=1000000)return'$'+(n/1000000).toLocaleString('es-MX',{minimumFractionDigits:1,maximumFractionDigits:2})+' M';
  return'$'+Math.round(n).toLocaleString('es-MX');
}
function fmtN(n){if(n==null||isNaN(n))return'–';return Number(n).toLocaleString('es-MX',{minimumFractionDigits:0,maximumFractionDigits:1});}
function esc(s){return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');}
function qs(o){return Object.entries(o).filter(([,v])=>v!=null&&v!=='').map(([k,v])=>`${k}=${encodeURIComponent(v)}`).join('&');}

function rangoLabel(d1,d2){
  if(d1===d2){
    const h=hoyStr(),y=addDays(h,-1);
    if(d1===h)return'Hoy · '+fmtFecha(d1);
    if(d1===y)return'Ayer · '+fmtFecha(d1);
    return fmtFecha(d1);
  }
  return fmtFecha(d1)+' – '+fmtFecha(d2);
}
function fmtFecha(s){
  if(!s)return'–';
  const [y,m,d]=s.split('-');
  return `${parseInt(d)} ${MESES[parseInt(m)-1].slice(0,3)} ${y}`;
}

// ── Períodos ──────────────────────────────────────────────────────────────
function getMonday(dateStr){
  const d=new Date(dateStr+'T12:00:00');
  const dow=d.getDay()||7;
  d.setDate(d.getDate()-dow+1);
  return `${d.getFullYear()}-${p2(d.getMonth()+1)}-${p2(d.getDate())}`;
}
function calcRango(p){
  const h=hoyStr();
  if(p==='hoy')    return[h,h];
  if(p==='ayer')   return[addDays(h,-1),addDays(h,-1)];
  if(p==='semana'){const lun=getMonday(h);return[lun,addDays(lun,6)];}
  if(p==='quincena'){
    const [y,m,d]=h.split('-').map(Number);
    if(d<=15)return[`${y}-${p2(m)}-01`,`${y}-${p2(m)}-15`];
    const ult=new Date(y,m,0).getDate();
    return[`${y}-${p2(m)}-16`,`${y}-${p2(m)}-${ult}`];
  }
  if(p==='mes'){
    const [y,m]=h.split('-').map(Number);
    const ult=new Date(y,m,0).getDate();
    return[`${y}-${p2(m)}-01`,`${y}-${p2(m)}-${ult}`];
  }
  return[h,h];
}

function setPer(btn,p){
  document.querySelectorAll('.ptab').forEach(b=>b.classList.remove('on'));
  btn.classList.add('on');
  calSelDate=null;
  const [d1,d2]=calcRango(p);
  desde=d1;hasta=d2;
  document.getElementById('inp-desde').value=d1;
  document.getElementById('inp-hasta').value=d2;
  cargarTodo();
}

function setRango(){
  const d1=document.getElementById('inp-desde').value;
  const d2=document.getElementById('inp-hasta').value;
  if(!d1||!d2)return;
  document.querySelectorAll('.ptab').forEach(b=>b.classList.remove('on'));
  calSelDate=null;
  desde=d1;hasta=d2;
  cargarTodo();
}

// ── Init ──────────────────────────────────────────────────────────────────
(function init(){
  const h=hoyStr();
  document.getElementById('inp-desde').value=h;
  document.getElementById('inp-hasta').value=h;
  desde=h;hasta=h;
  const d=new Date(h+'T12:00:00');
  calY=d.getFullYear();calM=d.getMonth()+1;
  cargarTodo();
  cargarCalendario();
  // Info último sync
  fetch('/api/ventas/debug/fuentes').then(r=>r.json()).then(d=>{
    if(d.ultimo_sync){
      const ts=new Date(d.ultimo_sync.replace(' ','T'));
      document.getElementById('sync-ts').textContent='Sync: '+ts.toLocaleString('es-MX',{day:'numeric',month:'short',hour:'2-digit',minute:'2-digit'});
    }
  }).catch(()=>{});
})();

function cargarTodo(){
  cargarResumen();
  cargarChart();
  cargarTopSkus();
  cargarTabla();
}

// ── Resumen ───────────────────────────────────────────────────────────────
async function cargarResumen(){
  try{
    const r=await fetch('/api/ventas/resumen?'+qs({desde,hasta}));
    const d=await r.json();
    const p=d.personalizado||Object.values(d)[0];if(!p)return;

    document.getElementById('hero-rango').textContent=rangoLabel(desde,hasta);
    document.getElementById('hero-num').textContent=fmt$(p.neto_monto);
    document.getElementById('hero-sub').textContent=fmtN(p.neto_piezas)+' unidades vendidas';

    document.getElementById('m-pzas').textContent=fmtN(p.ventas_piezas);
    document.getElementById('m-tkt').textContent=fmt$(p.ticket_promedio);
    document.getElementById('m-neto').textContent=fmt$(p.neto_monto);
    document.getElementById('m-dev').textContent=fmtN(p.devoluciones_piezas);

    const lv=p.por_canal?.liverpool||{};
    const ct=p.por_canal?.creditienda||{};
    document.getElementById('m-lv').textContent=fmtN(lv.piezas||0)+' · '+fmt$(lv.monto||0);
    document.getElementById('m-ct').textContent=fmtN(ct.piezas||0)+' · '+fmt$(ct.monto||0);
  }catch(e){console.error(e);}
}

// ── Gráfica ───────────────────────────────────────────────────────────────
async function cargarChart(){
  // Si el rango es ≤ 31 días muestra días; si > 31 muestra meses
  const d1=new Date(desde+'T12:00:00'),d2=new Date(hasta+'T12:00:00');
  const dias=Math.round((d2-d1)/(86400*1000))+1;
  const y=d1.getFullYear(),m=d1.getMonth()+1;

  try{
    const r=await fetch(`/api/ventas/calendario?year=${y}&month=${m}`);
    const d=await r.json();
    const diasObj=d.dias||{};

    const labels=[],dataM=[],dataP=[];
    for(let i=0;i<Math.min(dias,62);i++){
      const key=addDays(desde,i);
      const info=diasObj[key]||{monto:0,piezas:0};
      const [,mm,dd]=key.split('-');
      labels.push(parseInt(dd)+'/'+parseInt(mm));
      dataM.push(info.monto||0);
      dataP.push(info.piezas||0);
    }

    document.getElementById('chart-ttl').textContent=
      'Tendencia · '+fmtFecha(desde)+(desde!==hasta?' – '+fmtFecha(hasta):'');

    const ctx=document.getElementById('vchart').getContext('2d');
    if(chart)chart.destroy();
    chart=new Chart(ctx,{
      type:'bar',
      data:{labels,datasets:[
        {label:'Monto',data:dataM,backgroundColor:'rgba(20,122,84,.2)',borderColor:'rgba(20,122,84,.8)',borderWidth:1.5,borderRadius:4,yAxisID:'y'},
        {label:'Piezas',data:dataP,type:'line',borderColor:'rgba(26,111,168,.7)',backgroundColor:'transparent',borderWidth:2,pointRadius:dias<=14?3:1,tension:.3,yAxisID:'y1'},
      ]},
      options:{
        responsive:true,maintainAspectRatio:false,
        interaction:{mode:'index',intersect:false},
        plugins:{legend:{display:false},tooltip:{callbacks:{
          label:c=>c.datasetIndex===0?' $'+Math.round(c.raw).toLocaleString('es-MX'):' '+c.raw+' pzas'
        }}},
        scales:{
          x:{grid:{display:false},ticks:{font:{size:9},color:'#9ca3af',maxTicksLimit:16}},
          y:{position:'left',grid:{color:'rgba(0,0,0,.04)'},ticks:{font:{size:9},color:'#9ca3af',callback:v=>v>=1000?'$'+(v/1000).toFixed(0)+'k':'$'+v}},
          y1:{position:'right',grid:{display:false},ticks:{font:{size:9},color:'rgba(26,111,168,.6)'},display:dataP.some(v=>v>0)},
        }
      }
    });
  }catch(e){console.error(e);}
}

// ── Top SKUs ──────────────────────────────────────────────────────────────
async function cargarTopSkus(){
  try{
    const r=await fetch('/api/ventas/top-skus?'+qs({desde,hasta,limit:7}));
    const items=await r.json();
    const el=document.getElementById('top-list');
    if(!items.length){el.innerHTML='<div style="color:var(--muted);font-size:.73rem">Sin ventas en este período</div>';return;}
    el.innerHTML=items.map((it,i)=>`
    <div class="rank-row ${i===0?'r1':''}">
      <div class="rn">${i+1}</div>
      <div class="ri"><div class="ri-name">${esc(it.descripcion||it.sku_normalizado)}</div><div class="ri-sku">${esc(it.sku_normalizado)}</div></div>
      <div class="rr"><div class="rr-monto">${fmt$(it.monto)}</div><div class="rr-pzas">${fmtN(it.piezas)} pzas</div></div>
    </div>`).join('');
  }catch(e){console.error(e);}
}

// ── Top SKUs con rango personalizado ─────────────────────────────────────
// (override del endpoint para usar desde/hasta en lugar de periodo)
const _origFetch=window.fetch;
// No necesitamos override, ya pasamos desde/hasta directamente ↑

// ── Tabla SKU ─────────────────────────────────────────────────────────────
async function cargarTabla(){
  const canal=document.getElementById('sel-canal').value;
  const q=document.getElementById('inp-q').value.trim();
  try{
    const r=await fetch('/api/ventas/por-sku?'+qs({desde,hasta,canal,q}));
    const d=await r.json();
    skuData=d.items||[];
    renderTabla();
  }catch(e){console.error(e);}
}

function sortBy(f){
  sortField===f?sortDir=sortDir==='asc'?'desc':'asc':(sortField=f,sortDir='desc');
  renderTabla();
}

function renderTabla(){
  const items=[...skuData].sort((a,b)=>{
    let va=a[sortField],vb=b[sortField];
    if(typeof va==='number')return sortDir==='asc'?va-vb:vb-va;
    va=String(va||'').toLowerCase();vb=String(vb||'').toLowerCase();
    return sortDir==='asc'?va.localeCompare(vb,'es'):vb.localeCompare(va,'es');
  });
  document.getElementById('sku-cnt').textContent=items.length+' SKUs';
  const tbody=document.getElementById('tbody-sku');
  if(!items.length){tbody.innerHTML='<tr><td colspan="8" class="empty">Sin datos para este filtro</td></tr>';return;}
  tbody.innerHTML=items.map(r=>`<tr>
    <td><span class="sku-tag">${esc(r.sku_normalizado)}</span></td>
    <td style="max-width:250px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:.71rem" title="${esc(r.descripcion)}">${esc(r.descripcion||'–')}</td>
    <td class="num"><strong>${fmtN(r.piezas_total)}</strong></td>
    <td class="num lv">${fmtN(r.piezas_liverpool)||'–'}</td>
    <td class="num ct">${fmtN(r.piezas_creditienda)||'–'}</td>
    <td class="num lv">${fmt$(r.monto_liverpool)}</td>
    <td class="num ct">${fmt$(r.monto_creditienda)}</td>
    <td class="num tot">${fmt$(r.monto_total)}</td>
  </tr>`).join('');
}

// ── Calendario clickeable ─────────────────────────────────────────────────
function calMes(delta){
  calM+=delta;
  if(calM<1){calM=12;calY--;}
  if(calM>12){calM=1;calY++;}
  cargarCalendario();
}

async function cargarCalendario(){
  const canal=document.getElementById('sel-canal').value;
  document.getElementById('cal-ttl').textContent=MESES[calM-1]+' '+calY;
  try{
    const r=await fetch(`/api/ventas/calendario?year=${calY}&month=${calM}`+(canal?'&canal='+canal:''));
    const d=await r.json();
    calDias=d.dias||{};
    renderCal();
  }catch(e){console.error(e);}
}

function renderCal(){
  const grid=document.getElementById('cal-grid');
  while(grid.children.length>7)grid.removeChild(grid.lastChild);

  const montos=Object.values(calDias).map(v=>v.monto||0);
  const maxM=montos.length?Math.max(...montos):0;

  const first=new Date(calY,calM-1,1).getDay();
  const offset=first===0?6:first-1;
  const daysInMonth=new Date(calY,calM,0).getDate();
  const hoy=hoyStr();

  for(let i=0;i<offset;i++){const c=document.createElement('div');c.className='dc vacio';grid.appendChild(c);}

  for(let day=1;day<=daysInMonth;day++){
    const key=`${calY}-${p2(calM)}-${p2(day)}`;
    const info=calDias[key];
    const c=document.createElement('div');
    const isHoy=key===hoy,isSel=calSelDate===key;

    if(info){
      const ratio=Math.min(info.monto/maxM,1);
      const alpha=0.1+ratio*0.6;
      c.style.background=`rgba(20,122,84,${alpha.toFixed(2)})`;
      const light=ratio<0.5;
      c.innerHTML=`<div class="dc-day" style="color:${light?'var(--green2)':'#fff'}">${day}</div>`
        +`<div class="dc-pzas" style="color:${light?'var(--muted)':'rgba(255,255,255,.75)'}">${fmtN(info.piezas)} pz</div>`
        +`<div class="dc-monto" style="color:${light?'var(--green)':'#fff'}">${fmt$(info.monto)}</div>`;
      c.className='dc con'+(isSel?' sel':'')+(isHoy?' hoy':'');
      c.onclick=()=>selDia(key,c);
    } else {
      c.innerHTML=`<div class="dc-day" style="color:var(--muted)">${day}</div>`;
      c.className='dc sin'+(isHoy?' hoy':'');
      // días sin venta también son clickeables (para ver si hay algo en tabla)
      c.style.cursor='pointer';
      c.onclick=()=>selDia(key,c);
    }
    grid.appendChild(c);
  }
}

function selDia(dateStr, cell){
  calSelDate=dateStr;
  desde=dateStr;hasta=dateStr;

  // Actualizar date inputs
  document.getElementById('inp-desde').value=dateStr;
  document.getElementById('inp-hasta').value=dateStr;

  // Quitar active de period tabs
  document.querySelectorAll('.ptab').forEach(b=>b.classList.remove('on'));

  // Mostrar botón "quitar filtro"
  document.getElementById('cal-clear').style.display='';
  document.getElementById('cal-sel-lbl').textContent='Mostrando: '+fmtFecha(dateStr);

  renderCal();
  cargarTodo();
}

function limpiarCalSel(){
  calSelDate=null;
  document.getElementById('cal-clear').style.display='none';
  document.getElementById('cal-sel-lbl').textContent='';
  // Volver a hoy
  document.querySelector('[data-p="hoy"]').click();
}

// ── Sincronizar ───────────────────────────────────────────────────────────
async function sincronizar(){
  const btn=document.getElementById('btn-sync');
  const msg=document.getElementById('msg-sync');
  btn.disabled=true;
  btn.innerHTML=`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" style="width:14px;height:14px;animation:spin 1s linear infinite"><path d="M1 4v6h6M23 20v-6h-6"/><path d="M20.49 9A9 9 0 0 0 5.64 5.64L1 10m22 4-4.64 4.36A9 9 0 0 1 3.51 15"/></svg> Sincronizando…`;
  msg.style.display='none';

  if(!document.getElementById('spin-kf')){
    const s=document.createElement('style');s.id='spin-kf';
    s.textContent='@keyframes spin{to{transform:rotate(360deg)}}';
    document.head.appendChild(s);
  }
  try{
    const r=await fetch('/api/ventas/sync',{method:'POST'});
    const d=await r.json();
    const cred=d.creditienda||{},liv=d.liverpool||{};
    let txt='';
    if(cred.ok)txt+=`✓ Creditienda: ${cred.registros_parseados} registros, ${cred.insertados} nuevos.  `;
    else txt+=`✗ Creditienda: ${cred.error||'error'}.  `;
    if(liv.ok)txt+=`✓ Liverpool: ${liv.registros_parseados} registros, ${liv.insertados} nuevos.`;
    else txt+=`✗ Liverpool: ${liv.error||'error'}.`;
    msg.className='msg '+(d.ok?'msg-ok':'msg-err');
    msg.textContent=txt;msg.style.display='block';
    if(d.ok){
      const ts=new Date();
      document.getElementById('sync-ts').textContent='Sync: '+ts.toLocaleString('es-MX',{day:'numeric',month:'short',hour:'2-digit',minute:'2-digit'});
      cargarTodo();cargarCalendario();
    }
  }catch(e){
    msg.className='msg msg-err';msg.textContent='Error: '+e.message;msg.style.display='block';
  }finally{
    btn.disabled=false;
    btn.innerHTML=`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" style="width:14px;height:14px"><path d="M1 4v6h6M23 20v-6h-6"/><path d="M20.49 9A9 9 0 0 0 5.64 5.64L1 10m22 4-4.64 4.36A9 9 0 0 1 3.51 15"/></svg> Sincronizar`;
  }
}
</script>
</body>
</html>"""
