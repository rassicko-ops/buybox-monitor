import csv
import base64
import html
import io
import json
import os
import re
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from functools import cmp_to_key

import pandas as pd
import requests
from flask import Flask, jsonify, render_template_string, request, send_file


def cargar_env_local(path=".env"):
    """Carga variables locales simples sin agregar dependencia python-dotenv."""
    if not os.path.exists(path):
        return
    try:
        with open(path, encoding="utf-8") as archivo:
            for linea in archivo:
                linea = linea.strip()
                if not linea or linea.startswith("#") or "=" not in linea:
                    continue
                key, value = linea.split("=", 1)
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if key and key not in os.environ:
                    os.environ[key] = value
    except Exception as exc:
        print(f"⚠️ No se pudo cargar .env local: {exc}")


cargar_env_local()

# ================================
# CONFIG
# ================================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
MY_SELLER = os.getenv("MY_SELLER", "PATISH").strip()
MY_SELLER_ID = os.getenv("MY_SELLER_ID", "2370").strip()
DATA_DIR = os.getenv("DATA_DIR") or os.getenv("RAILWAY_VOLUME_MOUNT_PATH") or "."
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "3"))
CSV_MAX_DIAS = int(os.getenv("CSV_MAX_DIAS", "30"))
DISABLE_TELEGRAM = os.getenv("DISABLE_TELEGRAM", "").strip().lower() in {"1", "true", "yes", "si"}
RUN_ONCE = os.getenv("RUN_ONCE", "").strip().lower() in {"1", "true", "yes", "si"}
STALE_MINUTES = int(os.getenv("STALE_MINUTES", "30"))
CATALOGO_EXCEL_URL = os.getenv("CATALOGO_EXCEL_URL", "").strip()
CATALOGO_AUTH_BEARER = os.getenv("CATALOGO_AUTH_BEARER", "").strip()
CATALOGO_SYNC_INTERVAL_HOURS = float(os.getenv("CATALOGO_SYNC_INTERVAL_HOURS", "24"))
CATALOGO_SYNC_ON_START = os.getenv("CATALOGO_SYNC_ON_START", "").strip().lower() in {"1", "true", "yes", "si"}
CATALOGO_TOKEN_ALERT_INTERVAL_HOURS = float(os.getenv("CATALOGO_TOKEN_ALERT_INTERVAL_HOURS", "6"))
PANEL_SECRET = os.getenv("PANEL_SECRET", "").strip()
REPRICER_STEP = float(os.getenv("REPRICER_STEP", "1"))

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "es-MX,es;q=0.9",
}

CDMX_TZ = timezone(timedelta(hours=-6))

# Sesión HTTP con cookies de Akamai (se renueva automáticamente si expira)
_SESSION = requests.Session()
_SESSION.headers.update({
    "User-Agent": HEADERS["User-Agent"],
    "Accept-Language": "es-MX,es;q=0.9",
})
_SESSION_LOCK = threading.Lock()
_SESSION_COOKIES_AT = 0
_SESSION_COOKIE_TTL = 1800  # renovar cada 30 min


def _asegurar_cookies():
    """Obtiene cookies de Akamai si no existen o expiraron."""
    global _SESSION_COOKIES_AT
    now = time.time()
    with _SESSION_LOCK:
        if now - _SESSION_COOKIES_AT < _SESSION_COOKIE_TTL:
            return
        try:
            _SESSION.get("https://www.liverpool.com.mx/tienda/", timeout=15)
            _SESSION_COOKIES_AT = now
            print("🍪 Cookies Akamai renovadas")
        except Exception as exc:
            print(f"⚠️ No se pudieron obtener cookies: {exc}")

ULTIMO_ESTADO = {}
ULTIMO_PRECIO = {}
ULTIMO_SELLER = {}
ULTIMO_PRECIO_PATISH = {}
ULTIMO_STOCK_PATISH = {}
ULTIMO_LAST_CHECKED = {}
ULTIMO_SOURCE = {}
ULTIMO_STATUS_CODE = {}
ULTIMO_ERROR_MESSAGE = {}
ULTIMO_CONFIDENCE = {}
ULTIMO_STOCK_GANADOR = {}
ULTIMO_SEGUNDO_SELLER = {}
ULTIMO_SEGUNDO_PRECIO = {}
ULTIMO_REPRICE_SUGERIDO = {}
ULTIMO_REPRICE_MOTIVO = {}
RESUMEN_VGC = {}
PRECIOS_MINIMOS = {}
VENTAS_CACHE = {"ts": 0, "dias": None, "data": {}}
CATALOGO_SYNC_LOCK = threading.Lock()
CATALOGO_SYNC_STATE = {
    "last_sync_at": "",
    "last_attempt_at": "",
    "last_error_at": "",
    "source": "",
    "ok": False,
    "error": "",
    "error_type": "",
    "total": 0,
    "activas": 0,
    "inactivas_stock": 0,
    "bloqueadas": 0,
}
CATALOGO_TOKEN_ALERT_LAST_TS = 0

CATALOGO = []

ULTIMO_RESUMEN = 0
ULTIMA_FECHA_CSV = None
ULTIMO_UPDATE_ID = 0

BOOTSTRAP_SKUS_FILE = "skus.csv"
CSV_FILE = os.path.join(DATA_DIR, "historico_buybox.csv")
SKUS_FILE = os.path.join(DATA_DIR, "skus.csv")
CATALOGO_FILE = os.path.join(DATA_DIR, "catalogo_activo.json")
ESTADO_FILE = os.path.join(DATA_DIR, "estado_persistido.json")
CATALOGO_SYNC_EXCEL_FILE = os.path.join(DATA_DIR, "catalogo_sync_ultimo.xlsx")
PRECIOS_MINIMOS_FILE = os.path.join(DATA_DIR, "precios_minimos.json")
VENTAS_DB_FILE = os.path.join(DATA_DIR, "ventas_monitor.db")
CATALOGO_TOKEN_FILE = os.path.join(DATA_DIR, "catalogo_auth.json")
PORT = int(os.getenv("PORT", "8080"))

app = Flask(__name__)

# ================================
# HTML PANEL
# ================================
HTML_PANEL = """<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>BuyBox Monitor · PATISH</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap" rel="stylesheet">
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:#f0f4f8;--card:#fff;--border:#e2e8f0;--text:#0f172a;--muted:#64748b;
  --primary:#00c896;--primary-dark:#00a87e;--primary-light:rgba(0,200,150,.1);
  --accent:#6366f1;--accent-light:rgba(99,102,241,.1);
  --red:#f43f5e;--red-light:#ffe4e8;
  --yellow:#f59e0b;--yellow-light:#fef9c3;
  --orange:#f97316;--orange-light:#ffedd5;
  --shadow-sm:0 1px 4px rgba(0,0,0,.06),0 1px 2px rgba(0,0,0,.04);
  --shadow:0 8px 24px rgba(0,0,0,.10),0 2px 6px rgba(0,0,0,.06);
  --shadow-glow-g:0 0 0 1px rgba(0,200,150,.2),0 4px 20px rgba(0,200,150,.15);
  --shadow-glow-r:0 0 0 1px rgba(244,63,94,.2),0 4px 20px rgba(244,63,94,.12);
  --r:14px;--rf:9999px;
}
body{font-family:'Inter',-apple-system,BlinkMacSystemFont,sans-serif;background:var(--bg);color:var(--text);min-height:100vh;font-size:14px;line-height:1.5;background-image:radial-gradient(circle,#cbd5e1 1px,transparent 1px);background-size:28px 28px}

/* HEADER */
header{background:linear-gradient(135deg,#0f172a 0%,#1e293b 100%);border-bottom:1px solid rgba(255,255,255,.06);padding:0 32px;height:64px;display:flex;align-items:center;gap:20px;position:sticky;top:0;z-index:50;box-shadow:0 4px 24px rgba(0,0,0,.25)}
.logo{display:flex;align-items:center;gap:12px}
.logo-icon{width:36px;height:36px;background:linear-gradient(135deg,#00c896 0%,#6366f1 100%);border-radius:10px;display:flex;align-items:center;justify-content:center;color:#fff;font-size:18px;font-weight:800;flex-shrink:0;box-shadow:0 2px 12px rgba(0,200,150,.4)}
.logo-name{font-size:15px;font-weight:700;color:#f1f5f9;letter-spacing:-.3px}
.logo-sub{font-size:11px;color:#94a3b8;font-weight:400}
.h-space{flex:1}
.live-pill{display:flex;align-items:center;gap:7px;padding:6px 14px;background:rgba(0,200,150,.15);border:1px solid rgba(0,200,150,.3);border-radius:var(--rf);font-size:12px;font-weight:700;color:#00c896;letter-spacing:.02em}
.live-dot{width:7px;height:7px;background:#00c896;border-radius:50%;box-shadow:0 0 6px rgba(0,200,150,.8);animation:pulse 2s ease-in-out infinite}
@keyframes pulse{0%,100%{opacity:1;transform:scale(1)}50%{opacity:.4;transform:scale(.85)}}

/* PAGE */
.page{max-width:1440px;margin:0 auto;padding:28px 32px 52px}

/* STAT CARDS */
.stats{display:grid;grid-template-columns:repeat(6,1fr);gap:14px;margin-bottom:24px}
.sc{background:var(--card);border-radius:var(--r);border:1px solid var(--border);padding:22px 18px;box-shadow:var(--shadow-sm);position:relative;overflow:hidden;transition:transform .18s,box-shadow .18s}
.sc:hover{transform:translateY(-3px)}
.sc.g:hover{box-shadow:var(--shadow-glow-g)}
.sc.r:hover{box-shadow:var(--shadow-glow-r)}
.sc.y:hover,.sc.o:hover,.sc.gr:hover,.sc.d:hover{box-shadow:var(--shadow)}
.sc::before{content:'';position:absolute;top:0;left:0;right:0;height:3px;border-radius:var(--r) var(--r) 0 0}
.sc::after{content:'';position:absolute;bottom:0;right:0;width:64px;height:64px;border-radius:50%;opacity:.06;pointer-events:none}
.sc.g::before{background:linear-gradient(90deg,#00c896,#6366f1)}.sc.g::after{background:#00c896}
.sc.r::before{background:linear-gradient(90deg,#f43f5e,#fb923c)}.sc.r::after{background:#f43f5e}
.sc.y::before{background:linear-gradient(90deg,#f59e0b,#fbbf24)}.sc.y::after{background:#f59e0b}
.sc.o::before{background:linear-gradient(90deg,#f97316,#f59e0b)}.sc.o::after{background:#f97316}
.sc.gr::before{background:#94a3b8}.sc.d::before{background:linear-gradient(90deg,#334155,#475569)}
.sc-lbl{font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.08em;color:var(--muted);margin-bottom:10px}
.sc-val{font-size:36px;font-weight:900;line-height:1;letter-spacing:-2px}
.sc.g .sc-val{color:var(--primary)}
.sc.r .sc-val{color:var(--red)}
.sc.y .sc-val{color:var(--yellow)}
.sc.o .sc-val{color:var(--orange)}
.sc.gr .sc-val{color:#64748b}
.sc.d .sc-val{color:#334155}

/* CARD */
.card{background:var(--card);border-radius:var(--r);border:1px solid var(--border);box-shadow:var(--shadow-sm);margin-bottom:16px}
.ch{padding:16px 20px;border-bottom:1px solid var(--border);display:flex;align-items:center;gap:12px}
.ct{font-size:14px;font-weight:700;color:var(--text)}
.cs{font-size:12px;color:var(--muted);margin-top:1px}
.cb{padding:20px}

/* UPLOAD */
.urow{display:flex;align-items:center;gap:12px;flex-wrap:wrap}
.flabel{display:inline-flex;align-items:center;gap:6px;padding:7px 16px;background:#f8fafc;border:1px solid var(--border);border-radius:var(--rf);font-size:13px;font-weight:500;color:var(--text);cursor:pointer;transition:all .15s}
.flabel:hover{background:#e2e8f0;border-color:#cbd5e1}
.fname{font-size:12px;color:var(--muted)}
input[type=file]{display:none}
.umsg{font-size:13px;margin-top:10px;min-height:18px}
.umsg.ok{color:var(--primary-dark)}
.umsg.err{color:var(--red)}
.sync-meta{font-size:12px;color:var(--muted);margin-top:8px}

/* BUTTONS */
.btn{display:inline-flex;align-items:center;gap:6px;padding:8px 18px;border-radius:var(--rf);font-family:inherit;font-size:13px;font-weight:600;cursor:pointer;border:none;transition:all .15s;text-decoration:none;white-space:nowrap}
.btn-p{background:linear-gradient(135deg,var(--primary) 0%,var(--accent) 100%);color:#fff;box-shadow:0 2px 8px rgba(0,200,150,.35)}
.btn-p:hover{filter:brightness(1.08);box-shadow:0 4px 16px rgba(0,200,150,.45);transform:translateY(-1px)}
.btn-s{background:#fff;color:var(--text);border:1px solid var(--border)}
.btn-s:hover{background:#f8fafc;border-color:#cbd5e1}

/* FILTERS */
.frow{display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-bottom:12px}
.fbtn{padding:6px 14px;border-radius:var(--rf);border:1px solid var(--border);background:#fff;font-family:inherit;font-size:12px;font-weight:500;color:var(--muted);cursor:pointer;transition:all .15s}
.fbtn:hover{border-color:var(--primary);color:var(--primary);background:var(--primary-light)}
.fbtn.active{background:linear-gradient(135deg,var(--primary),var(--accent));color:#fff;border-color:transparent;box-shadow:0 2px 10px rgba(0,200,150,.3)}
.legend{display:flex;gap:8px;flex-wrap:wrap;align-items:center;margin:-4px 0 12px;color:var(--muted);font-size:11px}
.legend-item{display:inline-flex;align-items:center;gap:5px;background:#f8fafc;border:1px solid var(--border);border-radius:999px;padding:4px 9px}
.legend strong{color:var(--text)}

/* TOOLBAR */
.toolbar{display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin-bottom:16px}
.sw{flex:1;min-width:220px;max-width:380px;position:relative}
.si{position:absolute;left:11px;top:50%;transform:translateY(-50%);color:var(--muted);font-size:13px;pointer-events:none}
.sinput{width:100%;padding:8px 12px 8px 32px;background:#fff;border:1px solid var(--border);border-radius:var(--rf);font-family:inherit;font-size:13px;color:var(--text);transition:border-color .15s,box-shadow .15s}
.sinput:focus{outline:none;border-color:var(--primary);box-shadow:0 0 0 3px rgba(0,200,150,.12)}
.scount{font-size:12px;color:var(--muted);white-space:nowrap}
.insights{display:grid;grid-template-columns:1fr 1fr 1.6fr;gap:14px;margin-bottom:16px}
.insight-box{background:#fff;border:1px solid var(--border);border-radius:var(--r);box-shadow:var(--shadow-sm);padding:16px 18px}
.insight-click{cursor:pointer;transition:transform .15s,box-shadow .15s,border-color .15s;border-left:3px solid var(--primary)}
.insight-click:hover{transform:translateY(-2px);box-shadow:var(--shadow);border-color:var(--primary)}
.insight-title{font-size:11px;font-weight:800;text-transform:uppercase;letter-spacing:.07em;color:var(--muted);margin-bottom:10px}
.insight-row{display:flex;justify-content:space-between;gap:10px;border-top:1px solid #f1f5f9;padding:8px 0;font-size:13px}
.insight-row:first-of-type{border-top:none}
.insight-row strong{font-weight:800;color:var(--text)}
.insight-link{cursor:pointer;border-radius:8px;padding-left:6px;padding-right:6px}
.insight-link:hover{background:var(--primary-light);color:var(--primary-dark)}
.prio{display:inline-flex;align-items:center;border-radius:999px;padding:2px 8px;font-size:10px;font-weight:900;margin-top:5px}
.prio-alta{background:#ffe4e8;color:#be123c}.prio-media{background:#fef3c7;color:#92400e}.prio-baja{background:#e0f2fe;color:#075985}
.diag-btn{border:1px solid var(--border);background:#fff;border-radius:999px;padding:4px 9px;font-size:11px;font-weight:700;color:var(--muted);cursor:pointer}
.diag-btn:hover{border-color:var(--primary);color:var(--primary-dark);background:var(--primary-light)}
.diag-panel{background:#f8fafc;border:1px solid var(--border);border-radius:12px;padding:14px 16px;margin:4px 0}
.diag-grid{display:grid;grid-template-columns:repeat(4,minmax(140px,1fr));gap:10px;margin-top:10px}
.diag-card{background:#fff;border:1px solid var(--border);border-radius:10px;padding:9px}
.diag-label{font-size:10px;text-transform:uppercase;letter-spacing:.06em;color:var(--muted);font-weight:800}
.diag-value{font-size:13px;color:var(--text);font-weight:700;margin-top:2px;word-break:break-word}
.min-input{width:110px;border:1px solid var(--border);border-radius:8px;padding:5px 7px;font:inherit;font-size:12px}
.hist-list{margin-top:10px;font-size:12px;color:var(--muted)}

/* TABLE */
.tw{overflow:auto;border-top:1px solid var(--border)}
table{width:100%;border-collapse:collapse;font-size:13px}
thead th{padding:10px 14px;font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.07em;color:var(--muted);background:#f8fafc;border-bottom:1px solid var(--border);white-space:nowrap;position:sticky;top:0;z-index:6}
.cfrow th{background:#fff;padding:6px 8px;border-bottom:1px solid var(--border);position:sticky;top:37px;z-index:5}
.cf{width:100%;min-width:80px;padding:5px 8px;font-family:inherit;font-size:11px;background:#f8fafc;border:1px solid var(--border);border-radius:6px;color:var(--text);transition:border-color .15s}
.cf:focus{outline:none;border-color:var(--primary);background:#fff}
tbody td{padding:11px 14px;border-bottom:1px solid #f1f5f9;vertical-align:middle}
tbody tr:last-child td{border-bottom:none}
tbody tr:hover td{background:rgba(0,200,150,.04)}
.prod-cell{min-width:360px;max-width:560px;white-space:normal;line-height:1.35}
.sku-cell{font-size:11px;color:var(--muted);min-width:110px;cursor:pointer;font-family:'SF Mono','Fira Code',monospace}
.sku-cell:hover{color:var(--primary-dark);text-decoration:underline dotted}
#copy-toast{position:fixed;bottom:24px;left:50%;transform:translateX(-50%) translateY(20px);background:linear-gradient(135deg,#0f172a,#1e293b);color:#fff;padding:8px 20px;border-radius:999px;font-size:12px;font-weight:600;opacity:0;transition:opacity .2s,transform .2s;pointer-events:none;z-index:9999;border:1px solid rgba(255,255,255,.08);box-shadow:0 8px 24px rgba(0,0,0,.3)}
#copy-toast.show{opacity:1;transform:translateX(-50%) translateY(0)}
.sortable{cursor:pointer;user-select:none}
.sortable:hover{color:var(--primary)}
.sarr{font-size:10px;color:#cbd5e1;margin-left:3px}
.sarr.active{color:var(--primary)}

/* BADGES */
.badge{display:inline-flex;align-items:center;padding:3px 10px;border-radius:var(--rf);font-size:11px;font-weight:700;white-space:nowrap}
.bg{background:rgba(0,200,150,.12);color:#00795a;border:1px solid rgba(0,200,150,.2)}
.br{background:#ffe4e8;color:#be123c;border:1px solid rgba(244,63,94,.2)}
.by{background:#fef9c3;color:#854d0e}
.bo{background:#ffedd5;color:#9a3412}
.bgr{background:#f1f5f9;color:#475569}

/* MISC */
a.lnk{color:var(--muted);font-size:12px;text-decoration:none}
a.lnk:hover{color:var(--primary)}
.dp{color:var(--primary-dark);font-size:12px;font-weight:600}
.dn{color:var(--red);font-size:12px;font-weight:600}
.ts{text-align:right;font-size:12px;color:var(--muted);padding:10px 20px}
.chip{display:inline-flex;align-items:center;padding:1px 8px;background:#f1f5f9;border-radius:var(--rf);font-size:11px;color:var(--muted);font-weight:600;margin-left:6px}
.vgc-chip{display:inline-flex;align-items:center;margin-top:5px;padding:2px 8px;border:1px solid #f59e0b;background:#fffbeb;color:#92400e;border-radius:var(--rf);font-size:10px;font-weight:700;cursor:pointer}
.vgc-chip:hover{background:#fef3c7}
.src-chip{display:inline-flex;align-items:center;margin-top:5px;padding:2px 7px;border-radius:999px;font-size:9px;font-weight:800;letter-spacing:.02em}
.src-ok{background:#e0f2fe;color:#075985}.src-warn{background:#fef3c7;color:#92400e}.src-bad{background:#ffe4e8;color:#be123c}.src-muted{background:#f1f5f9;color:#64748b}
.vgc-detail{background:#fffbeb;border:1px solid #fde68a;border-radius:12px;padding:14px 16px;margin:4px 0;color:#78350f}
.vgc-detail h4{font-size:13px;margin-bottom:6px;color:#78350f}
.vgc-detail .muted{font-size:12px;color:#92400e;margin-bottom:10px}
.vgc-tools{display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin:10px 0}
.vgc-sort{border:1px solid #f59e0b;background:#fff7ed;color:#92400e;border-radius:999px;padding:4px 10px;font-size:10px;font-weight:800;cursor:pointer}
.vgc-sort.active{background:#f59e0b;color:white}
.vgc-grid{display:grid;grid-template-columns:minmax(760px,1.65fr) minmax(360px,.8fr);gap:12px}
.vgc-box{background:#fff;border:1px solid #fde68a;border-radius:10px;overflow:hidden}
.vgc-box-title{font-size:12px;font-weight:800;padding:8px 10px;background:#fef3c7;color:#78350f}
.vgc-mini{width:100%;font-size:12px}
.vgc-mini th,.vgc-mini td{padding:7px 9px;border-bottom:1px solid #fef3c7;text-align:left;white-space:normal}
.vgc-mini th{font-size:10px;color:#92400e;background:#fffbeb}
.vgc-mini tr:last-child td{border-bottom:none}
.vgc-sim{min-width:170px;white-space:normal}
@media(max-width:900px){.vgc-grid{grid-template-columns:1fr}}
@media(max-width:900px){.insights,.diag-grid{grid-template-columns:1fr}}
@media(max-width:1100px){.stats{grid-template-columns:repeat(3,1fr)}}
@media(max-width:640px){.stats{grid-template-columns:repeat(2,1fr)}.page{padding:18px 16px 32px}header{padding:0 16px}}
</style>
</head>
<body>
<div id="copy-toast"></div>

<header>
  <div class="logo">
    <div class="logo-icon">⚡</div>
    <div>
      <div class="logo-name">BuyBox Monitor</div>
      <div class="logo-sub">Liverpool Marketplace · Tiempo real</div>
    </div>
  </div>
  <div class="h-space"></div>
  <div class="live-pill"><div class="live-dot"></div>En vivo</div>
</header>

<div class="page">

  <div class="stats">
    <div class="sc g"><div class="sc-lbl">Ganando</div><div class="sc-val" id="sg">—</div></div>
    <div class="sc r"><div class="sc-lbl">Perdiendo</div><div class="sc-val" id="sp">—</div></div>
    <div class="sc y"><div class="sc-lbl">No prendida</div><div class="sc-val" id="snp">—</div></div>
    <div class="sc o"><div class="sc-lbl">Bloqueadas</div><div class="sc-val" id="sb">—</div></div>
    <div class="sc gr"><div class="sc-lbl">Sin stock</div><div class="sc-val" id="ss">—</div></div>
    <div class="sc d"><div class="sc-lbl">Total</div><div class="sc-val" id="st">—</div></div>
  </div>

  <div class="insights">
    <div class="insight-box insight-click" onclick="irAOportunidades()" title="Ver oportunidades en la tabla">
      <div class="insight-title">Oportunidades</div>
      <div class="insight-row"><span>Recuperables</span><strong id="io-total">—</strong></div>
      <div class="insight-row"><span>Hacer ahora</span><strong id="io-alta">—</strong></div>
      <div class="insight-row"><span>Con ventas 30d</span><strong id="io-ventas">—</strong></div>
      <div class="insight-row"><span>Con precio mínimo</span><strong id="io-minimo">—</strong></div>
    </div>
    <div class="insight-box">
      <div class="insight-title">Para actuar hoy</div>
      <div id="acciones-box"><div class="insight-row"><span>Cargando...</span><strong>—</strong></div></div>
    </div>
    <div class="insight-box">
      <div class="insight-title">Competidores que más presionan</div>
      <div id="competidores-box"><div class="insight-row"><span>Cargando...</span><strong>—</strong></div></div>
    </div>
  </div>

  <div class="card">
    <div class="ch">
      <div>
        <div class="ct">Actualizar catálogo</div>
        <div class="cs">Sube el reporte de ofertas semanal de Liverpool (.xlsx)</div>
      </div>
    </div>
    <div class="cb">
      <div class="urow">
        <label class="flabel" for="excel-input">📂 Elegir archivo</label>
        <input type="file" id="excel-input" accept=".xlsx,.xls">
        <span class="fname" id="file-name-lbl">Ningún archivo seleccionado</span>
        <button class="btn btn-p" onclick="subirCatalogo()">Actualizar catálogo</button>
        <button class="btn btn-s" onclick="syncCatalogoUrl()">Actualizar desde URL</button>
        <a class="btn btn-s" href="/api/catalogo/sync/download" target="_blank" rel="noreferrer">Descargar último auto-sync</a>
      </div>
      <div class="urow" style="margin-top:10px">
        <label class="flabel" for="minimos-input">Subir precios mínimos</label>
        <input type="file" id="minimos-input" accept=".xlsx,.xls,.csv">
        <span class="fname" id="minimos-file-lbl">Ningún archivo seleccionado</span>
        <button class="btn btn-s" onclick="subirPreciosMinimos()">Cargar mínimos</button>
      </div>
      <div class="umsg" id="msg-upload"></div>
      <div class="umsg" id="msg-minimos"></div>
      <div class="sync-meta" id="catalog-sync-status">Catálogo automático: cargando estado...</div>
      <div style="margin-top:8px"><a href="/admin/token" target="_blank" rel="noreferrer" style="font-size:12px;color:#0ea5e9;text-decoration:none">🔑 Renovar token Liverpool</a></div>
    </div>
  </div>

  <div class="card" id="estado-card">
    <div class="ch">
      <div class="ct">Estado actual <span class="chip" id="count-visible">—</span></div>
    </div>
    <div class="cb" style="padding-bottom:12px">
      <div class="frow">
        <button class="fbtn state-btn active" data-filter="TODOS" onclick="setFiltro('TODOS',this)">Todos</button>
        <button class="fbtn state-btn" data-filter="OPORTUNIDADES" onclick="setFiltro('OPORTUNIDADES',this)">💰 Oportunidades</button>
        <button class="fbtn state-btn" data-filter="GANANDO_VERIFICADO" onclick="setFiltro('GANANDO_VERIFICADO',this)">🟢 Ganando</button>
        <button class="fbtn state-btn" data-filter="GANANDO_API_NO_VISIBLE" onclick="setFiltro('GANANDO_API_NO_VISIBLE',this)">⚠️ Inconsistente</button>
        <button class="fbtn state-btn" data-filter="PERDIDO" onclick="setFiltro('PERDIDO',this)">🔴 Perdiendo</button>
        <button class="fbtn state-btn" data-filter="NO_PRENDIDA" onclick="setFiltro('NO_PRENDIDA',this)">🟡 No prendida</button>
        <button class="fbtn state-btn" data-filter="SKU_INVALIDO" onclick="setFiltro('SKU_INVALIDO',this)">⛔ SKU inválido</button>
        <button class="fbtn state-btn" data-filter="PRODUCTO_NO_EXISTE" onclick="setFiltro('PRODUCTO_NO_EXISTE',this)">404</button>
        <button class="fbtn state-btn" data-filter="BLOQUEADA" onclick="setFiltro('BLOQUEADA',this)">🟠 Bloqueadas</button>
        <button class="fbtn state-btn" data-filter="INACTIVA_STOCK" onclick="setFiltro('INACTIVA_STOCK',this)">⚫ Sin stock</button>
        <button class="fbtn state-btn" data-filter="SIN_DATOS" onclick="setFiltro('SIN_DATOS',this)">Sin datos</button>
      </div>
      <div class="frow">
        <span class="scount">Confianza:</span>
        <button class="fbtn confidence-btn active" onclick="setConfianza('TODAS',this)">Todas</button>
        <button class="fbtn confidence-btn" onclick="setConfianza('PDP',this)">PDP verificado</button>
        <button class="fbtn confidence-btn" onclick="setConfianza('CONFLICTO',this)">Fuentes en conflicto</button>
        <button class="fbtn confidence-btn" onclick="setConfianza('API',this)">API no PDP</button>
      </div>
      <div class="legend">
        <span class="legend-item"><strong>PDP</strong> página pública que ve el cliente.</span>
        <span class="legend-item"><strong>API</strong> dato auxiliar de Liverpool, puede no coincidir.</span>
        <span class="legend-item"><strong>Conflicto</strong> PDP y API dicen diferente; manda PDP.</span>
      </div>
      <div class="toolbar">
        <div class="sw">
          <span class="si">🔍</span>
          <input id="sku-search" class="sinput" type="search" placeholder="Buscar producto, SKU, VGC…" oninput="setBusqueda(this.value)">
        </div>
        <span class="scount" id="search-status">—</span>
        <button type="button" class="btn btn-s" onclick="clearColumnFilters()">Limpiar filtros</button>
        <a id="download-link" class="btn btn-p" href="/api/exportar?estado=TODOS" target="_blank" rel="noreferrer">⬇ Exportar Excel</a>
        <a id="actions-link" class="btn btn-s" href="/api/exportar/acciones" target="_blank" rel="noreferrer">Acciones sugeridas</a>
      </div>
    </div>

    <div class="tw">
      <table>
        <thead>
          <tr>
            <th class="sortable" onclick="toggleSort('producto')">Producto <span class="sarr" id="sort-producto">↕</span></th>
            <th class="sortable" onclick="toggleSort('sku_patish')">SKU PATISH <span class="sarr" id="sort-sku_patish">↕</span></th>
            <th class="sortable" onclick="toggleSort('sku_liverpool')">SKU Liverpool <span class="sarr" id="sort-sku_liverpool">↕</span></th>
            <th class="sortable" onclick="toggleSort('vgc')">VGC <span class="sarr" id="sort-vgc">↕</span></th>
            <th class="sortable" onclick="toggleSort('estado')">Estado <span class="sarr" id="sort-estado">↕</span></th>
            <th class="sortable" onclick="toggleSort('seller_buybox')">Seller BuyBox <span class="sarr" id="sort-seller_buybox">↕</span></th>
            <th class="sortable" onclick="toggleSort('precio_liverpool')">Precio Liverpool <span class="sarr" id="sort-precio_liverpool">↕</span></th>
            <th class="sortable" onclick="toggleSort('precio_tuyo')">Tu precio <span class="sarr" id="sort-precio_tuyo">↕</span></th>
            <th class="sortable" onclick="toggleSort('stock_tuyo')">Stock <span class="sarr" id="sort-stock_tuyo">↕</span></th>
            <th class="sortable" onclick="toggleSort('diferencia')">Diferencia <span class="sarr" id="sort-diferencia">↕</span></th>
            <th>URL</th>
          </tr>
          <tr class="cfrow">
            <th><input class="cf" data-col-filter="producto" type="search" placeholder="Filtrar…" oninput="setColumnFilter('producto',this.value)"></th>
            <th><input class="cf" data-col-filter="sku_patish" type="search" placeholder="Filtrar…" oninput="setColumnFilter('sku_patish',this.value)"></th>
            <th><input class="cf" data-col-filter="sku_liverpool" type="search" placeholder="Filtrar…" oninput="setColumnFilter('sku_liverpool',this.value)"></th>
            <th><input class="cf" data-col-filter="vgc" type="search" placeholder="Filtrar…" oninput="setColumnFilter('vgc',this.value)"></th>
            <th>
              <select class="cf" data-col-filter="estado" onchange="setColumnFilter('estado',this.value)">
                <option value="">Todos</option>
                <option value="GANANDO">Ganando</option>
                <option value="GANANDO_VERIFICADO">Ganando verificado</option>
                <option value="GANANDO_API_NO_VISIBLE">Inconsistente</option>
                <option value="PERDIDO">Perdido</option>
                <option value="NO_PRENDIDA">No prendida</option>
                <option value="SKU_INVALIDO">SKU inválido</option>
                <option value="PRODUCTO_NO_EXISTE">Producto no existe</option>
                <option value="SIN_DATOS_STALE">Sin datos stale</option>
                <option value="BLOQUEADA">Bloqueada</option>
                <option value="INACTIVA_STOCK">Sin stock</option>
                <option value="SIN_DATOS">Sin datos</option>
              </select>
            </th>
            <th><input class="cf" data-col-filter="seller_buybox" type="search" placeholder="Filtrar…" oninput="setColumnFilter('seller_buybox',this.value)"></th>
            <th><input class="cf" data-col-filter="precio_liverpool" type="search" placeholder=">15000" oninput="setColumnFilter('precio_liverpool',this.value)"></th>
            <th><input class="cf" data-col-filter="precio_tuyo" type="search" placeholder=">15000" oninput="setColumnFilter('precio_tuyo',this.value)"></th>
            <th><input class="cf" data-col-filter="stock_tuyo" type="search" placeholder=">0" oninput="setColumnFilter('stock_tuyo',this.value)"></th>
            <th><input class="cf" data-col-filter="diferencia" type="search" placeholder="1000-2000" oninput="setColumnFilter('diferencia',this.value)"></th>
            <th></th>
          </tr>
        </thead>
        <tbody id="tbody-estado">
          <tr><td colspan="11" style="text-align:center;padding:40px;color:var(--muted)">Cargando…</td></tr>
        </tbody>
      </table>
    </div>
    <div class="ts" id="refresh-ts">—</div>
  </div>

</div>

<script>
let _copyToastTimer=null;
function copiarCelda(texto){
  if(!texto||texto==='-')return;
  navigator.clipboard.writeText(texto).then(()=>{
    const t=document.getElementById('copy-toast');
    t.textContent=`Copiado: ${texto}`;
    t.classList.add('show');
    clearTimeout(_copyToastTimer);
    _copyToastTimer=setTimeout(()=>t.classList.remove('show'),1800);
  }).catch(()=>{});
}
const DEFAULT_COLUMN_FILTERS={
  producto:'',sku_patish:'',sku_liverpool:'',vgc:'',
  estado:'',seller_buybox:'',precio_liverpool:'',precio_tuyo:'',stock_tuyo:'',diferencia:'',url:'',
};
const NUMERIC_SORT_FIELDS=new Set(['precio_liverpool','precio_tuyo','stock_tuyo','diferencia']);
const STATE_SORT_ORDER={GANANDO_VERIFICADO:0,GANANDO:0,GANANDO_API_NO_VISIBLE:1,PERDIDO:2,NO_PRENDIDA:3,SKU_INVALIDO:4,PRODUCTO_NO_EXISTE:5,VGC_INVALIDO:6,BLOQUEADA:7,INACTIVA_STOCK:8,SIN_DATOS_STALE:9,SIN_DATOS:10};

let filtroActual='TODOS',confianzaActual='TODAS',accionActual='TODAS',busquedaActual='',ordenActual='producto_asc',todosItems=[],columnFilters={...DEFAULT_COLUMN_FILTERS},expandedVgc='',expandedDiag='',vgcSort='precio_asc',historialCache={};

document.getElementById('excel-input').addEventListener('change',function(){
  document.getElementById('file-name-lbl').textContent=this.files[0]?.name||'Ningún archivo';
});
document.getElementById('minimos-input').addEventListener('change',function(){
  document.getElementById('minimos-file-lbl').textContent=this.files[0]?.name||'Ningún archivo';
});

async function subirCatalogo(){
  const input=document.getElementById('excel-input');
  const msg=document.getElementById('msg-upload');
  if(!input.files[0]){msg.className='umsg err';msg.textContent='Selecciona el archivo primero.';return;}
  const fd=new FormData();fd.append('file',input.files[0]);
  msg.className='umsg';msg.textContent='Procesando...';
  const resp=await fetch('/api/catalogo',{method:'POST',body:fd});
  const d=await resp.json();
  if(d.ok){
    msg.className='umsg ok';
    msg.textContent=`Catálogo actualizado: ${d.total} variantes, ${d.activas} activas, ${d.inactivas_stock} sin stock, ${d.bloqueadas} bloqueadas.`;
    cargarEstado();
  }else{
    msg.className='umsg err';
    msg.textContent='Error: '+(d.error||'No se pudo procesar el archivo');
  }
}

async function subirPreciosMinimos(){
  const input=document.getElementById('minimos-input');
  const msg=document.getElementById('msg-minimos');
  if(!input.files[0]){msg.className='umsg err';msg.textContent='Selecciona archivo de mínimos primero.';return;}
  const fd=new FormData();fd.append('file',input.files[0]);
  msg.className='umsg';msg.textContent='Cargando precios mínimos...';
  const resp=await fetch('/api/precios-minimos/carga',{method:'POST',body:fd});
  const d=await resp.json();
  if(d.ok){
    msg.className='umsg ok';
    msg.textContent=`Precios mínimos cargados: ${d.cargados}. Omitidos: ${d.omitidos}.`;
    cargarEstado();
  }else{
    msg.className='umsg err';
    msg.textContent='Error mínimos: '+(d.error||'No se pudo cargar el archivo');
  }
}

async function syncCatalogoUrl(){
  const msg=document.getElementById('msg-upload');
  msg.className='umsg';msg.textContent='Descargando catálogo desde URL...';
  try{
    const resp=await fetch('/api/catalogo/sync',{method:'POST'});
    const d=await resp.json();
    if(d.ok){
      msg.className='umsg ok';
      msg.textContent=`Catálogo auto actualizado: ${d.total} variantes, ${d.activas} activas, ${d.inactivas_stock} sin stock, ${d.bloqueadas} bloqueadas.`;
      cargarEstado();
      cargarCatalogoSyncStatus();
    }else{
      msg.className='umsg err';
      msg.textContent='Error sync: '+(d.error||'No se pudo actualizar desde URL');
      cargarCatalogoSyncStatus();
    }
  }catch(e){
    msg.className='umsg err';msg.textContent='Error sync: '+e;
  }
}

async function cargarCatalogoSyncStatus(){
  try{
    const resp=await fetch('/api/catalogo/sync/status');
    const d=await resp.json();
    const el=document.getElementById('catalog-sync-status');
    const cfg=d.configured?'configurada':'sin CATALOGO_EXCEL_URL';
    const auth=d.configured?(d.auth_configured?' · token configurado':' · falta token'):'';
    const last=d.last_sync_at?` · última OK: ${d.last_sync_at}`:' · sin sync automático aún';
    let err='';
    if(d.error){
      const tipos={token:'token vencido/no autorizado',red_dns:'fallo temporal de red/DNS',archivo:'respuesta no fue Excel',otro:'error'};
      err=` · último intento falló${d.last_error_at?` (${d.last_error_at})`:''}: ${tipos[d.error_type]||'error'} · ${d.error}`;
    }
    el.textContent=`Catálogo automático: ${cfg}${auth}${last}${err}`;
  }catch(e){}
}

function setFiltro(f,btn){
  filtroActual=f;
  document.querySelectorAll('.state-btn').forEach(b=>b.classList.remove('active'));
  if(btn) btn.classList.add('active');
  const fallback=document.querySelector(`.state-btn[data-filter="${f}"]`);
  if(!btn&&fallback) fallback.classList.add('active');
  actualizarDescarga();
  renderTabla();
}

function setConfianza(f,btn){
  confianzaActual=f;
  document.querySelectorAll('.confidence-btn').forEach(b=>b.classList.remove('active'));
  btn.classList.add('active');
  actualizarResumenFiltros();
  actualizarDescarga();
  renderTabla();
}

function setBusqueda(value){
  busquedaActual=(value||'').trim().toLowerCase();
  actualizarResumenFiltros();
  actualizarDescarga();
  renderTabla();
}

function scrollTabla(){
  document.getElementById('estado-card')?.scrollIntoView({behavior:'smooth',block:'start'});
}

function irAOportunidades(){
  columnFilters={...DEFAULT_COLUMN_FILTERS};
  document.querySelectorAll('[data-col-filter]').forEach(input=>{input.value=''});
  document.getElementById('sku-search').value='';
  busquedaActual='';
  accionActual='TODAS';
  setFiltro('OPORTUNIDADES');
  actualizarResumenFiltros();
  scrollTabla();
}

function irAAccion(grupo){
  columnFilters={...DEFAULT_COLUMN_FILTERS};
  document.querySelectorAll('[data-col-filter]').forEach(input=>{input.value=''});
  document.getElementById('sku-search').value='';
  busquedaActual='';
  accionActual=grupo;
  setFiltro('OPORTUNIDADES');
  actualizarResumenFiltros();
  scrollTabla();
}

function irACompetidor(seller){
  columnFilters={...DEFAULT_COLUMN_FILTERS,seller_buybox:seller};
  document.querySelectorAll('[data-col-filter]').forEach(input=>{input.value=columnFilters[input.dataset.colFilter]||''});
  document.getElementById('sku-search').value='';
  busquedaActual='';
  accionActual='TODAS';
  setFiltro('TODOS');
  actualizarResumenFiltros();
  actualizarDescarga();
  renderTabla();
  scrollTabla();
}

function setOrden(value){
  ordenActual=value||'producto_asc';
  actualizarIndicadoresOrden();
  actualizarDescarga();
  renderTabla();
}

function obtenerOrdenActual(){
  const match=String(ordenActual||'producto_asc').match(/^(.*)_(asc|desc)$/);
  if(!match) return {field:'producto',direction:'asc'};
  return {field:match[1]||'producto',direction:match[2]||'asc'};
}

function toggleSort(field){
  const actual=obtenerOrdenActual();
  const dir=actual.field===field && actual.direction==='asc'?'desc':'asc';
  setOrden(`${field}_${dir}`);
}

function actualizarIndicadoresOrden(){
  document.querySelectorAll('.sarr').forEach(icon=>{icon.textContent='↕';icon.classList.remove('active')});
  const actual=obtenerOrdenActual();
  const icon=document.getElementById(`sort-${actual.field}`);
  if(!icon) return;
  icon.textContent=actual.direction==='asc'?'↑':'↓';
  icon.classList.add('active');
}

function setColumnFilter(key,value){
  columnFilters[key]=(value||'').trim();
  actualizarResumenFiltros();
  actualizarDescarga();
  renderTabla();
}

function clearColumnFilters(){
  columnFilters={...DEFAULT_COLUMN_FILTERS};
  document.querySelectorAll('[data-col-filter]').forEach(input=>{input.value=''});
  actualizarResumenFiltros();
  actualizarDescarga();
  renderTabla();
}

function actualizarResumenFiltros(){
  const status=document.getElementById('search-status');
  const activos=Object.values(columnFilters).filter(Boolean).length;
  const piezas=[];
  if(busquedaActual) piezas.push(`Búsqueda: "${busquedaActual}"`);
  if(activos) piezas.push(`${activos} filtro(s) activo(s)`);
  if(confianzaActual!=='TODAS') piezas.push(`Confianza: ${confianzaActual}`);
  if(accionActual!=='TODAS') piezas.push(`Acción: ${accionActual.replaceAll('_',' ')}`);
  if(piezas.length){status.textContent=piezas.join(' · ');return;}
  status.textContent='';
}

function actualizarDescarga(){
  const link=document.getElementById('download-link');
  const params=new URLSearchParams();
  params.set('estado',filtroActual);
  if(confianzaActual!=='TODAS') params.set('confianza',confianzaActual);
  if(accionActual!=='TODAS') params.set('accion',accionActual);
  if(busquedaActual) params.set('q',busquedaActual);
  for(const [key,value] of Object.entries(columnFilters)){
    if(value) params.set(`f_${key}`,value);
  }
  params.set('sort',ordenActual);
  link.href='/api/exportar?'+params.toString();
  const titulo=filtroActual==='TODOS'?'Todo':filtroActual.replaceAll('_',' ');
  link.textContent='⬇ '+titulo;
  const actions=document.getElementById('actions-link');
  if(actions){
    const ap=new URLSearchParams();
    if(confianzaActual!=='TODAS') ap.set('confianza',confianzaActual);
    if(accionActual!=='TODAS') ap.set('accion',accionActual);
    actions.href='/api/exportar/acciones'+(ap.toString()?`?${ap.toString()}`:'');
  }
}

function escapeHtml(value){
  return String(value??'').replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('"','&quot;').replaceAll("'","&#39;");
}

function numeroSeguro(value){
  const n=parseFloat(String(value??'').replaceAll('$','').replaceAll(',',''));
  return Number.isFinite(n)?n:null;
}

function diferenciaNumero(item){
  const a=numeroSeguro(item.precio_liverpool),b=numeroSeguro(item.precio_tuyo);
  if(a===null||b===null) return null;
  return a-b;
}

function confianzaBucket(item){
  const confidence=String(item.confidence||'').toLowerCase();
  const source=String(item.source||'').toUpperCase();
  if(confidence.includes('conflict')) return 'CONFLICTO';
  if(source==='PDP') return 'PDP';
  if(source==='OFFERSLISTING'||source==='ALLOFFERS') return 'API';
  return 'OTRO';
}

function esOportunidad(item){
  return item.estado==='PERDIDO'
    && !!item.reprice_sugerido
    && (numeroSeguro(item.stock_tuyo)??0)>0
    && confianzaBucket(item)!=='API';
}

function pasaFiltroConfianza(item){
  return confianzaActual==='TODAS'||confianzaBucket(item)===confianzaActual;
}

function pasaFiltroAccion(item){
  return accionActual==='TODAS'||String(item.accion_grupo||'')===accionActual;
}

function prioridadBadge(item){
  const p=String(item.oportunidad_prioridad||'').toLowerCase();
  if(!p) return '';
  return `<span class="prio prio-${p}">Prioridad ${escapeHtml(item.oportunidad_prioridad)}</span>`;
}

function toggleDiagnostico(skuPatish){
  expandedDiag=expandedDiag===String(skuPatish)?'':String(skuPatish);
  if(expandedDiag && !historialCache[expandedDiag]){
    fetch(`/api/sku/${encodeURIComponent(expandedDiag)}/historial`)
      .then(r=>r.json())
      .then(d=>{historialCache[expandedDiag]=d.historial||[];renderTabla();})
      .catch(()=>{historialCache[expandedDiag]=[];renderTabla();});
  }
  renderTabla();
}

async function guardarPrecioMinimo(skuPatish){
  const input=document.getElementById(`min-${domIdSku(skuPatish)}`);
  const precio=input?input.value:'';
  const resp=await fetch('/api/precio-minimo',{
    method:'POST',
    headers:{'Content-Type':'application/json'},
    body:JSON.stringify({sku_patish:skuPatish,precio_minimo:precio})
  });
  const d=await resp.json();
  if(!d.ok){alert(d.error||'No se pudo guardar precio mínimo');return;}
  await cargarEstado();
  expandedDiag=String(skuPatish);
}

function domIdSku(value){
  return String(value||'').replace(/[^a-zA-Z0-9_-]/g,'_');
}

function detalleDiagnosticoHtml(item){
  const hist=historialCache[String(item.sku_patish)]||[];
  const histHtml=hist.length
    ? hist.slice(0,8).map(h=>`<div>${escapeHtml(h.fecha_hora||'-')} · ${escapeHtml(h.estado||'-')} · ${escapeHtml(h.seller||'-')} · ${h.precio?money(h.precio):'-'} · ${escapeHtml(h.tipo_cambio||'')}</div>`).join('')
    : '<div>Cargando historial o sin eventos previos.</div>';
  return `<tr class="diag-row"><td colspan="11">
    <div class="diag-panel">
      <div style="display:flex;justify-content:space-between;gap:12px;align-items:flex-start">
        <div>
          <strong>Diagnóstico SKU ${escapeHtml(item.sku_patish)}</strong>
          <div class="muted" style="font-size:12px">Fuente actual: ${fuenteBadge(item)} ${item.error_message?` · ${escapeHtml(item.error_message)}`:''}</div>
        </div>
        <div>
          <input id="min-${domIdSku(item.sku_patish)}" class="min-input" type="number" step="1" placeholder="Precio mín." value="${escapeHtml(item.precio_minimo||'')}">
          <button class="diag-btn" onclick="guardarPrecioMinimo('${escapeHtml(item.sku_patish)}')">Guardar mínimo</button>
        </div>
      </div>
      <div class="diag-grid">
        <div class="diag-card"><div class="diag-label">PDP / ganador</div><div class="diag-value">${escapeHtml(item.seller_buybox||'-')} · ${money(item.precio_liverpool)}</div></div>
        <div class="diag-card"><div class="diag-label">Tu oferta</div><div class="diag-value">${money(item.precio_tuyo)} · stock ${escapeHtml(item.stock_tuyo??'-')}</div></div>
        <div class="diag-card"><div class="diag-label">Ventas 30d</div><div class="diag-value">${formatearNumero(item.ventas_30d_piezas)} pzas<br><span style="font-size:11px;color:var(--muted)">${money(item.ventas_30d_monto)} · ${escapeHtml(item.ventas_ultima_fecha||'sin fecha')}</span></div></div>
        <div class="diag-card"><div class="diag-label">Repricing</div><div class="diag-value">${item.reprice_sugerido?money(item.reprice_sugerido):'-'}<br><span style="font-size:11px;color:var(--muted)">${escapeHtml(item.reprice_motivo||item.accion_recomendada||'')}</span></div></div>
        <div class="diag-card"><div class="diag-label">Acción</div><div class="diag-value">${escapeHtml(item.accion_recomendada||'-')}</div></div>
      </div>
      <div class="hist-list"><strong>Historial reciente</strong>${histHtml}</div>
    </div>
  </td></tr>`;
}

function textoSeguro(value){return String(value??'').trim().toLowerCase()}

function valorOrdenable(item,field){
  if(field==='diferencia') return diferenciaNumero(item);
  if(field==='estado') return STATE_SORT_ORDER[String(item.estado||'').trim()]??999;
  if(NUMERIC_SORT_FIELDS.has(field)) return numeroSeguro(item[field]);
  return textoSeguro(item[field]);
}

function ordenarItems(items){
  const copia=[...items];
  const actual=obtenerOrdenActual();
  const factor=actual.direction==='desc'?-1:1;
  return copia.sort((a,b)=>{
    const av=valorOrdenable(a,actual.field),bv=valorOrdenable(b,actual.field);
    const av0=av===null||av===undefined||av==='',bv0=bv===null||bv===undefined||bv==='';
    if(av0&&!bv0) return 1;
    if(!av0&&bv0) return -1;
    if(!av0&&!bv0){
      if(typeof av==='number'&&typeof bv==='number'){if(av!==bv) return (av-bv)*factor;}
      else{const c=String(av).localeCompare(String(bv),'es',{sensitivity:'base',numeric:true});if(c!==0) return c*factor;}
    }
    const pc=textoSeguro(a.producto).localeCompare(textoSeguro(b.producto),'es',{sensitivity:'base',numeric:true});
    if(pc!==0) return pc;
    return textoSeguro(a.sku_patish).localeCompare(textoSeguro(b.sku_patish),'es');
  });
}

function coincideTexto(value,filtro){
  if(!filtro) return true;
  return String(value??'').toLowerCase().includes(String(filtro).toLowerCase());
}

function coincideNumerico(value,filtro){
  const c=String(filtro||'').replaceAll(' ','');
  if(!c) return true;
  const n=numeroSeguro(value);
  if(n===null) return false;
  let m=c.match(/^(-?\\d+(?:\\.\\d+)?)\\-(-?\\d+(?:\\.\\d+)?)$/);
  if(m){const a=parseFloat(m[1]),b=parseFloat(m[2]);return n>=Math.min(a,b)&&n<=Math.max(a,b);}
  m=c.match(/^(>=|<=|>|<|=)?(-?\\d+(?:\\.\\d+)?)$/);
  if(!m) return false;
  const op=m[1]||'=',t=parseFloat(m[2]);
  if(op==='>') return n>t;if(op==='<') return n<t;
  if(op==='>=') return n>=t;if(op==='<=') return n<=t;
  return n===t;
}

function aplicarFiltrosColumna(items){
  return items.filter(item=>{
    if(columnFilters.estado&&String(item.estado||'')!==columnFilters.estado) return false;
    for(const k of ['producto','sku_patish','sku_liverpool','vgc','seller_buybox','url']){
      if(!coincideTexto(item[k],columnFilters[k])) return false;
    }
    for(const k of ['precio_liverpool','precio_tuyo','stock_tuyo']){
      if(!coincideNumerico(item[k],columnFilters[k])) return false;
    }
    if(!coincideNumerico(diferenciaNumero(item),columnFilters.diferencia)) return false;
    return true;
  });
}

function estadoBadge(estado){
  const map={
    GANANDO:['bg','GANANDO'],
    GANANDO_VERIFICADO:['bg','GANANDO VERIFICADO'],
    GANANDO_API_NO_VISIBLE:['by','API NO VISIBLE'],
    PERDIDO:['br','PERDIDO'],
    NO_PRENDIDA:['by','NO PRENDIDA'],
    SKU_INVALIDO:['br','SKU INVALIDO'],
    PRODUCTO_NO_EXISTE:['br','PRODUCTO NO EXISTE'],
    VGC_INVALIDO:['br','VGC INVALIDO'],
    BLOQUEADA:['bo','BLOQUEADA'],
    INACTIVA_STOCK:['bgr','SIN STOCK'],
    SIN_DATOS_STALE:['bgr','SIN DATOS STALE'],
    SIN_DATOS:['bgr','SIN DATOS'],
  };
  const [cls,txt]=map[estado]||['bgr',estado||'—'];
  return `<span class="badge ${cls}">${txt}</span>`;
}

function fuenteBadge(item){
  const source=String(item.source||'').toUpperCase();
  const confidence=String(item.confidence||'').toLowerCase();
  const error=String(item.error_message||'');
  if(confidence.includes('conflict')){
    return `<span class="src-chip src-bad" title="${escapeHtml(error)}">FUENTES EN CONFLICTO</span>`;
  }
  if(source==='PDP'){
    return `<span class="src-chip src-ok" title="Validado contra la página pública que ve el comprador">PDP VERIFICADO</span>`;
  }
  if(source==='OFFERSLISTING'||source==='ALLOFFERS'){
    return `<span class="src-chip src-warn" title="API auxiliar; puede no coincidir con el PDP público">API NO PDP</span>`;
  }
  return source?`<span class="src-chip src-muted">${escapeHtml(source)}</span>`:'';
}

function setVgcSort(sort){
  vgcSort=sort;
  renderTabla();
}

function toggleDetalleVgc(skuLiverpool){
  const item=todosItems.find(x=>String(x.sku_liverpool||'')===String(skuLiverpool));
  if(!item){return;}
  const key=String(item.product_id||item.vgc||'');
  expandedVgc=expandedVgc===key?'':key;
  renderTabla();
}

function money(value){
  const n=numeroSeguro(value);
  return n===null?'-':'$'+n.toLocaleString('es-MX',{maximumFractionDigits:0});
}

function formatearNumero(value){
  const n=numeroSeguro(value);
  return n===null?'0':n.toLocaleString('es-MX',{maximumFractionDigits:2});
}

function ordenarVgc(items){
  const copia=[...items];
  const [field,dir]=String(vgcSort||'precio_asc').split('_');
  const factor=dir==='desc'?-1:1;
  return copia.sort((a,b)=>{
    let av,bv;
    if(field==='precio'){av=numeroSeguro(a.precio_ganador??a.precio);bv=numeroSeguro(b.precio_ganador??b.precio);}
    else if(field==='stock'){av=numeroSeguro(a.stock_tuyo??a.stock);bv=numeroSeguro(b.stock_tuyo??b.stock);}
    else if(field==='estado'){av=STATE_SORT_ORDER[String(a.estado_monitor||a.estado_oferta||'')]??999;bv=STATE_SORT_ORDER[String(b.estado_monitor||b.estado_oferta||'')]??999;}
    else{av=textoSeguro(a.sku_liverpool);bv=textoSeguro(b.sku_liverpool);}
    const av0=av===null||av===undefined||av==='',bv0=bv===null||bv===undefined||bv==='';
    if(av0&&!bv0) return 1;if(!av0&&bv0) return -1;
    if(typeof av==='number'&&typeof bv==='number'&&av!==bv) return (av-bv)*factor;
    const c=String(av).localeCompare(String(bv),'es',{sensitivity:'base',numeric:true});
    return c*factor;
  });
}

function vgcSortButton(sort,label){
  return `<button type="button" class="vgc-sort ${vgcSort===sort?'active':''}" onclick="setVgcSort('${sort}')">${escapeHtml(label)}</button>`;
}

function renderInsights(data){
  const items=data.items||[];
  const oportunidades=items.filter(esOportunidad);
  document.getElementById('io-total').textContent=data.oportunidades??oportunidades.length;
  const hacerAhora=oportunidades.filter(i=>i.accion_grupo==='HACER_AHORA');
  const faltaMin=oportunidades.filter(i=>i.accion_grupo==='FALTA_MINIMO');
  document.getElementById('io-alta').textContent=hacerAhora.length;
  document.getElementById('io-ventas').textContent=oportunidades.filter(i=>(numeroSeguro(i.ventas_30d_piezas)||0)>0).length;
  document.getElementById('io-minimo').textContent=oportunidades.filter(i=>!!i.precio_minimo).length;
  const acciones=document.getElementById('acciones-box');
  if(acciones){
    acciones.innerHTML=[
      `<div class="insight-row insight-link" onclick="irAAccion('HACER_AHORA')"><span>Hacer ahora</span><strong>${hacerAhora.length}</strong></div>`,
      `<div class="insight-row insight-link" onclick="irAAccion('FALTA_MINIMO')"><span>Falta precio mínimo</span><strong>${faltaMin.length}</strong></div>`,
      `<div class="insight-row insight-link" onclick="irAAccion('NO_TOCAR')"><span>No tocar / revisar</span><strong>${oportunidades.filter(i=>i.accion_grupo==='NO_TOCAR').length}</strong></div>`
    ].join('');
  }
  const comps=data.competidores||[];
  const box=document.getElementById('competidores-box');
  box.innerHTML=comps.length
    ? comps.slice(0,5).map(c=>`<div class="insight-row insight-link" onclick="irACompetidor('${escapeHtml(c.seller)}')" title="Ver SKUs ganados por ${escapeHtml(c.seller)}"><span>${escapeHtml(c.seller)} · ${escapeHtml(c.skus)} SKUs</span><strong>${escapeHtml(c.oportunidades)} ops</strong></div>`).join('')
    : '<div class="insight-row"><span>Sin competidores detectados</span><strong>—</strong></div>';
}

function detalleVgcHtml(item){
  const d=item.vgc_detalle||{};
  const key=String(item.product_id||item.vgc||'');
  const estadosGrupo=todosItems.filter(x=>String(x.product_id||x.vgc||'')===key);
  const ventasPiezas=estadosGrupo.reduce((s,x)=>s+(numeroSeguro(x.ventas_30d_piezas)||0),0);
  const ventasMonto=estadosGrupo.reduce((s,x)=>s+(numeroSeguro(x.ventas_30d_monto)||0),0);
  const sellersDominio={};
  estadosGrupo.forEach(x=>{const seller=x.seller_buybox||'Sin seller';sellersDominio[seller]=(sellersDominio[seller]||0)+1;});
  const sellerDominante=Object.entries(sellersDominio).sort((a,b)=>b[1]-a[1])[0]||['-',0];
  let recomendacionVgc='Cobertura completa en catálogo.';
  if((d.variantes_faltantes||0)>0){
    recomendacionVgc=`Te faltan ${d.variantes_faltantes} variante(s); revisar si conviene agregarlas antes de bajar precio.`;
  }else if(ventasPiezas>0 && estadosGrupo.some(x=>x.estado==='PERDIDO')){
    recomendacionVgc='VGC con ventas recientes y pérdidas activas; priorizar recuperación de buybox.';
  }else if(!ventasPiezas){
    recomendacionVgc='Sin ventas recientes; revisar antes de competir agresivamente por precio.';
  }
  const miasBase=(d.mias_detalle&&d.mias_detalle.length)
    ? d.mias_detalle
    : estadosGrupo.map(x=>({sku_liverpool:x.sku_liverpool,sku_patish:x.sku_patish,estado_oferta:x.estado,stock:x.stock_tuyo,seller:x.seller_buybox,precio:x.precio_liverpool}));
  const mias=ordenarVgc(miasBase.map(v=>{
    const estado=estadosGrupo.find(x=>String(x.sku_liverpool||'')===String(v.sku_liverpool||''));
    return {
      ...v,
      estado_monitor:estado?.estado||'',
      seller_ganador:estado?.seller_buybox||v.seller||'',
      precio_ganador:estado?.precio_liverpool||v.precio||'',
      precio_tuyo:estado?.precio_tuyo||'',
      stock_tuyo:estado?.stock_tuyo,
      stock_ganador:estado?.stock_ganador,
      segundo_seller:estado?.segundo_seller||'',
      segundo_precio:estado?.segundo_precio||'',
      reprice_sugerido:estado?.reprice_sugerido||'',
      reprice_motivo:estado?.reprice_motivo||'',
      source:estado?.source||'',
      confidence:estado?.confidence||'',
      error_message:estado?.error_message||''
    };
  }));
  const faltantes=ordenarVgc(d.faltantes||[]);
  const rowsMias=mias.length?mias.map(v=>`<tr>
    <td>${escapeHtml(v.sku_liverpool||'-')}</td>
    <td>${escapeHtml(v.sku_patish||'-')}</td>
    <td>${escapeHtml(v.estado_monitor||v.estado_oferta||'-')}<br>${fuenteBadge(v)}</td>
    <td>${escapeHtml(v.seller_ganador||'-')}</td>
    <td>${money(v.precio_ganador)}</td>
    <td>${money(v.precio_tuyo)}</td>
    <td>${v.stock_ganador===0||v.stock_ganador?escapeHtml(v.stock_ganador):'-'}</td>
    <td>${v.stock_tuyo===0||v.stock_tuyo?escapeHtml(v.stock_tuyo):(v.stock===0||v.stock?escapeHtml(v.stock):'-')}</td>
    <td class="vgc-sim">${v.reprice_sugerido?money(v.reprice_sugerido):'-'}${v.reprice_motivo?`<div style="font-size:10px;color:#92400e">${escapeHtml(v.reprice_motivo)}</div>`:''}</td>
  </tr>`).join(''):'<tr><td colspan="9">No hay variantes tuyas detectadas en este VGC.</td></tr>';
  const rowsFaltantes=faltantes.length?faltantes.map(v=>`<tr>
    <td>${escapeHtml(v.sku_liverpool||'-')}</td>
    <td>${escapeHtml(v.seller||'-')}</td>
    <td>${money(v.precio)}</td>
    <td>${v.stock===0||v.stock?escapeHtml(v.stock):'-'}</td>
    <td>${escapeHtml(v.sellers_count??'-')}</td>
  </tr>`).join(''):'<tr><td colspan="5">No se detectaron variantes faltantes.</td></tr>';
  return `<tr class="vgc-row"><td colspan="11">
    <div class="vgc-detail">
      <h4>VGC ${escapeHtml(d.product_id||item.product_id||item.vgc||'-')} · ${escapeHtml(d.alerta_texto||'Detalle de variantes')}</h4>
      <div class="muted">Tus SKUs en este VGC: ${escapeHtml(String(estadosGrupo.length||d.variantes_mias||'-'))}. Cobertura pública: ${escapeHtml(String(d.variantes_mias??'-'))}/${escapeHtml(String(d.total_variantes_liverpool??'-'))}. Faltantes: ${escapeHtml(String(d.variantes_faltantes??0))}.</div>
      <div class="legend" style="margin:0 0 10px">
        <span class="legend-item"><strong>Ventas 30d</strong> ${escapeHtml(formatearNumero(ventasPiezas))} pzas · ${money(ventasMonto)}</span>
        <span class="legend-item"><strong>Seller dominante</strong> ${escapeHtml(sellerDominante[0])} (${escapeHtml(sellerDominante[1])} SKU)</span>
        <span class="legend-item"><strong>Recomendación</strong> ${escapeHtml(recomendacionVgc)}</span>
      </div>
      <div class="vgc-tools">
        <span class="muted" style="margin:0">Ordenar:</span>
        ${vgcSortButton('precio_asc','Precio ↑')}
        ${vgcSortButton('precio_desc','Precio ↓')}
        ${vgcSortButton('estado_asc','Estado')}
        ${vgcSortButton('stock_desc','Stock ↓')}
        ${vgcSortButton('sku_asc','SKU')}
      </div>
      <div class="vgc-grid">
        <div class="vgc-box">
          <div class="vgc-box-title">Tus variantes</div>
          <table class="vgc-mini"><thead><tr><th>SKU Liverpool</th><th>SKU PATISH</th><th>Estado</th><th>Seller ganador</th><th>Precio ganador</th><th>Tu precio</th><th>Stock ganador</th><th>Stock tuyo</th><th>Simulación</th></tr></thead><tbody>${rowsMias}</tbody></table>
        </div>
        <div class="vgc-box">
          <div class="vgc-box-title">Variantes que no tienes</div>
          <table class="vgc-mini"><thead><tr><th>SKU Liverpool</th><th>Seller ganador</th><th>Precio</th><th>Stock visible</th><th>Sellers</th></tr></thead><tbody>${rowsFaltantes}</tbody></table>
        </div>
      </div>
    </div>
  </td></tr>`;
}

function renderTabla(){
  let items=filtroActual==='TODOS'
    ? todosItems
    : filtroActual==='OPORTUNIDADES'
      ? todosItems.filter(esOportunidad)
      : todosItems.filter(i=>i.estado===filtroActual);
  items=items.filter(pasaFiltroConfianza);
  items=items.filter(pasaFiltroAccion);
  if(busquedaActual){
    items=items.filter(i=>{
      const t=busquedaActual;
      return String(i.producto||'').toLowerCase().includes(t)
        ||String(i.sku_liverpool||'').toLowerCase().includes(t)
        ||String(i.sku_patish||'').toLowerCase().includes(t)
        ||String(i.vgc||'').toLowerCase().includes(t);
    });
  }
  items=aplicarFiltrosColumna(items);
  items=ordenarItems(items);
  const gruposFiltro=new Map();
  if(filtroActual!=='TODOS'){
    items.forEach(item=>{
      const key=String(item.product_id||item.vgc||item.sku_liverpool||'');
      if(!gruposFiltro.has(key)) gruposFiltro.set(key,[]);
      gruposFiltro.get(key).push(item);
    });
    items=Array.from(gruposFiltro.values()).map(grupo=>{
      const base={...grupo[0]};
      base._grupo_vgc=grupo;
      base._grupo_count=grupo.length;
      base._grupo_estados=grupo.reduce((acc,item)=>{acc[item.estado]=(acc[item.estado]||0)+1;return acc;},{});
      return base;
    });
  }
  actualizarIndicadoresOrden();
  document.getElementById('count-visible').textContent=items.length;
  const tbody=document.getElementById('tbody-estado');
  if(!items.length){
    tbody.innerHTML='<tr><td colspan="11" style="text-align:center;padding:40px;color:var(--muted)">Sin resultados para este filtro</td></tr>';
    return;
  }
  const expandedMostrados=new Set();
  tbody.innerHTML=items.map(p=>{
    let diff='';
    if(p.precio_liverpool&&p.precio_tuyo){
      const d=parseFloat(p.precio_liverpool)-parseFloat(p.precio_tuyo);
      if(!isNaN(d)) diff=d<0?`<span class="dn">-$${Math.abs(d).toFixed(0)}</span>`:d>0?`<span class="dp">+$${d.toFixed(0)}</span>`:'<span style="color:var(--muted)">igual</span>';
    }
    const stock=(p.stock_tuyo===0||p.stock_tuyo)?escapeHtml(String(p.stock_tuyo)):'-';
    const vgcKey=String(p.product_id||p.vgc||'');
    const groupInfo=p._grupo_count>1?`<span class="chip">${p._grupo_count} SKUs en este filtro</span>`:'';
    const debeMostrarChip=p.vgc_alerta||p._grupo_count>1;
    const chipTexto=p.vgc_alerta_texto||(p._grupo_count>1?`${p._grupo_count} SKUs del VGC`:'Ver VGC');
    const vgcChip=debeMostrarChip
      ? `<button type="button" class="vgc-chip" onclick="toggleDetalleVgc('${escapeHtml(p.sku_liverpool)}')">${expandedVgc===vgcKey?'Ocultar':'Ver'} ${escapeHtml(chipTexto)}</button>`
      : '';
    const prioridad=prioridadBadge(p);
    const row=`<tr>
      <td class="prod-cell" title="${escapeHtml(p.producto)}">${escapeHtml(p.producto)} ${groupInfo}<br>${vgcChip} ${prioridad}</td>
      <td class="sku-cell" title="Click para copiar" onclick="copiarCelda('${escapeHtml(p.sku_patish)}')">${escapeHtml(p.sku_patish)}</td>
      <td class="sku-cell" title="Click para copiar" onclick="copiarCelda('${escapeHtml(p.sku_liverpool)}')">${escapeHtml(p.sku_liverpool)}</td>
      <td class="sku-cell" title="Click para copiar" onclick="copiarCelda('${escapeHtml(p.vgc||'')}')">${escapeHtml(p.vgc||'-')}</td>
      <td>${estadoBadge(p.estado)}<br>${fuenteBadge(p)}</td>
      <td>${escapeHtml(p.seller_buybox||'-')}</td>
      <td>${p.precio_liverpool?'$'+escapeHtml(String(p.precio_liverpool)):'-'}</td>
      <td>${p.precio_tuyo?'$'+escapeHtml(String(p.precio_tuyo)):'-'}</td>
      <td>${stock}</td>
      <td>${diff||'-'}${p.accion_recomendada?`<div style="font-size:10px;color:var(--muted)">${escapeHtml(p.accion_recomendada)}</div>`:''}</td>
      <td>${p.url?`<a class="lnk" href="${escapeHtml(p.url)}" target="_blank" rel="noreferrer">ver</a>`:'-'}<br><button type="button" class="diag-btn" onclick="toggleDiagnostico('${escapeHtml(p.sku_patish)}')">${expandedDiag===String(p.sku_patish)?'Ocultar':'Diagnóstico'}</button></td>
    </tr>`;
    let extra='';
    if(expandedVgc===vgcKey&&!expandedMostrados.has(vgcKey)){
      expandedMostrados.add(vgcKey);
      extra+=detalleVgcHtml(p);
    }
    if(expandedDiag===String(p.sku_patish)){
      extra+=detalleDiagnosticoHtml(p);
    }
    return row+extra;
  }).join('');
}

async function cargarEstado(){
  try{
    const resp=await fetch('/api/estado');
    const d=await resp.json();
    document.getElementById('sg').textContent=d.ganando;
    document.getElementById('sp').textContent=d.perdidos;
    document.getElementById('snp').textContent=d.no_prendida;
    document.getElementById('sb').textContent=d.bloqueadas;
    document.getElementById('ss').textContent=d.sin_stock;
    document.getElementById('st').textContent=d.total;
    todosItems=d.items||[];
    renderInsights(d);
    renderTabla();
    document.getElementById('refresh-ts').textContent='Actualizado: '+new Date().toLocaleTimeString('es-MX');
  }catch(e){console.error('Error cargando estado',e)}
}

cargarEstado();
cargarCatalogoSyncStatus();
actualizarDescarga();
setInterval(cargarEstado,30000);
setInterval(cargarCatalogoSyncStatus,60000);
</script>
</body>
</html>"""


# ================================
# HELPERS
# ================================

def normalizar_columna(valor):
    return re.sub(r"\s+", " ", str(valor).strip().lower())


def limpiar_texto(valor):
    if valor is None or pd.isna(valor):
        return ""
    texto = str(valor).strip()
    return "" if texto.lower() == "nan" else texto


def normalizar_identificador(valor):
    texto = limpiar_texto(valor)
    if not texto:
        return ""
    if re.fullmatch(r"\d+\.0+", texto):
        return texto.split(".", 1)[0]
    return texto


def normalizar_precio(valor):
    if valor is None or pd.isna(valor):
        return None
    if isinstance(valor, (int, float)):
        return float(valor)
    texto = str(valor).strip()
    if not texto or texto.lower() == "nan":
        return None
    texto = texto.replace("$", "").replace(",", "")
    try:
        return float(texto)
    except ValueError:
        return None


def normalizar_entero(valor):
    if valor is None or pd.isna(valor):
        return None
    if isinstance(valor, bool):
        return int(valor)
    if isinstance(valor, int):
        return valor
    if isinstance(valor, float):
        return int(valor)
    texto = str(valor).strip()
    if not texto or texto.lower() == "nan":
        return None
    texto = texto.replace(",", "")
    try:
        return int(float(texto))
    except ValueError:
        return None


def formatear_precio(valor):
    if valor in (None, ""):
        return ""
    try:
        numero = float(valor)
    except (TypeError, ValueError):
        return str(valor).strip()
    if numero.is_integer():
        return str(int(numero))
    return f"{numero:.2f}".rstrip("0").rstrip(".")


def formatear_money(valor):
    precio = formatear_precio(valor)
    return f"${precio}" if precio else "-"


def escapar(valor):
    return html.escape(str(valor), quote=False)


def es_seller_mio(seller, seller_id):
    seller_texto = limpiar_texto(seller).lower()
    seller_id_texto = normalizar_identificador(seller_id)
    return seller_texto == MY_SELLER.lower() or (seller_id_texto and seller_id_texto == MY_SELLER_ID)


ESTADOS_GANANDO_VERIFICADO = {"GANANDO", "GANANDO_VERIFICADO"}
ESTADOS_BUENOS_TRANSITORIOS = {"GANANDO", "GANANDO_VERIFICADO", "PERDIDO", "NO_PRENDIDA", "GANANDO_API_NO_VISIBLE"}
ESTADOS_INVALIDOS = {"SKU_INVALIDO", "PRODUCTO_NO_EXISTE", "VGC_INVALIDO"}
ESTADOS_NO_GANAN = {"GANANDO_API_NO_VISIBLE", "SKU_INVALIDO", "PRODUCTO_NO_EXISTE", "SIN_DATOS_STALE", "VGC_INVALIDO"}


def normalizar_estado_persistido(estado):
    """Los estados viejos GANANDO cuentan como verificados sólo hasta volver a revisar."""
    return "GANANDO_VERIFICADO" if estado == "GANANDO" else estado


def es_estado_ganador_verificado(estado):
    return normalizar_estado_persistido(estado) == "GANANDO_VERIFICADO"


def es_error_sku_invalido(mensaje):
    return "sku id no es válido" in limpiar_texto(mensaje).lower() or "sku id no es valido" in limpiar_texto(mensaje).lower()


def obtener_precio_actual(datos):
    if not isinstance(datos, dict):
        return ""
    for llave in ("promoPrice", "salePrice", "listPrice", "sortPrice"):
        precio = formatear_precio(datos.get(llave))
        if precio:
            return precio
    return ""


def obtener_stock_actual(datos):
    if not isinstance(datos, dict):
        return None
    for llave in ("stock", "availableQuantity", "quantity"):
        stock = normalizar_entero(datos.get(llave))
        if stock is not None:
            return stock
    return None


def resumir_oferta(oferta):
    if not isinstance(oferta, dict):
        return {}
    return {
        "seller": limpiar_texto(oferta.get("sellerName")),
        "sellerId": normalizar_identificador(oferta.get("sellerId")),
        "precio": obtener_precio_actual(oferta),
        "stock": obtener_stock_actual(oferta),
    }


def clasificar_estado_oferta(estado_oferta, motivo, cantidad):
    motivo_texto = limpiar_texto(motivo).lower()
    estado_texto = limpiar_texto(estado_oferta).upper()
    try:
        cantidad_num = int(cantidad)
    except (TypeError, ValueError):
        cantidad_num = 0

    if "restricción de oferta" in motivo_texto:
        return "BLOQUEADA"
    if cantidad_num <= 0:
        return "INACTIVA_STOCK"
    if estado_texto == "ACTIVA":
        return "ACTIVA"
    return "INACTIVA_STOCK"


def guardar_catalogo_persistido(items):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(CATALOGO_FILE, "w", encoding="utf-8") as archivo:
        json.dump(items, archivo, ensure_ascii=False, indent=2)


def cargar_catalogo_persistido():
    global CATALOGO
    if not os.path.exists(CATALOGO_FILE):
        return False
    try:
        with open(CATALOGO_FILE, encoding="utf-8") as archivo:
            CATALOGO = json.load(archivo)
        CATALOGO = [
            item for item in CATALOGO
            if limpiar_texto(item.get("sku_patish", "")) and not limpiar_texto(item.get("sku_patish", "")).startswith("Eliminado")
        ]
        for item in CATALOGO:
            item["estado_oferta"] = clasificar_estado_oferta(
                item.get("estado_oferta", ""),
                item.get("motivo", ""),
                item.get("cantidad", 0),
            )
        print(f"📦 {len(CATALOGO)} items cargados desde {CATALOGO_FILE}")
        return True
    except Exception as exc:
        print(f"⚠️ No se pudo cargar {CATALOGO_FILE}: {exc}")
        return False


def cargar_precios_minimos():
    if not os.path.exists(PRECIOS_MINIMOS_FILE):
        return False
    try:
        with open(PRECIOS_MINIMOS_FILE, encoding="utf-8") as archivo:
            data = json.load(archivo)
        PRECIOS_MINIMOS.clear()
        if isinstance(data, dict):
            for sku, precio in data.items():
                precio_num = normalizar_precio(precio)
                if precio_num is not None:
                    PRECIOS_MINIMOS[limpiar_texto(sku)] = precio_num
        return True
    except Exception as exc:
        print(f"⚠️ No se pudieron cargar precios mínimos: {exc}")
        return False


def guardar_precios_minimos():
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(PRECIOS_MINIMOS_FILE, "w", encoding="utf-8") as archivo:
        json.dump(PRECIOS_MINIMOS, archivo, ensure_ascii=False, indent=2)


def sku_base_desde_patish(sku):
    match = re.search(r"\b(\d{7})\b", limpiar_texto(sku))
    return match.group(1) if match else limpiar_texto(sku)


def ventas_por_sku(dias=30):
    ahora = time.time()
    if VENTAS_CACHE["dias"] == dias and ahora - VENTAS_CACHE["ts"] < 60:
        return VENTAS_CACHE["data"]
    if not os.path.exists(VENTAS_DB_FILE):
        VENTAS_CACHE.update({"ts": ahora, "dias": dias, "data": {}})
        return {}
    desde = (datetime.now(CDMX_TZ).date() - timedelta(days=dias)).isoformat()
    data = {}
    try:
        conn = sqlite3.connect(VENTAS_DB_FILE)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT sku_normalizado,
                   SUM(CASE WHEN event_type='venta' THEN cantidad ELSE -cantidad END) AS piezas,
                   SUM(CASE WHEN event_type='venta' THEN monto_bruto ELSE -monto_bruto END) AS monto,
                   MAX(fecha) AS ultima_fecha
            FROM ventas_eventos
            WHERE fecha >= ?
              AND fecha IS NOT NULL
              AND sku_normalizado != ''
            GROUP BY sku_normalizado
            """,
            (desde,),
        ).fetchall()
        conn.close()
        for row in rows:
            sku = limpiar_texto(row["sku_normalizado"])
            data[sku] = {
                "piezas": round(row["piezas"] or 0, 2),
                "monto": round(row["monto"] or 0, 2),
                "ultima_fecha": row["ultima_fecha"] or "",
            }
    except Exception as exc:
        print(f"⚠️ No se pudieron leer ventas: {exc}")
        data = {}
    VENTAS_CACHE.update({"ts": ahora, "dias": dias, "data": data})
    return data


def procesar_excel_catalogo(excel_bytes):
    if excel_bytes[:2] == b"PK":
        df = pd.read_excel(io.BytesIO(excel_bytes), engine="openpyxl")
    elif excel_bytes[:4] == b"\xd0\xcf\x11\xe0":
        df = pd.read_excel(io.BytesIO(excel_bytes), engine="xlrd")
    else:
        snippet = excel_bytes[:250].decode("utf-8", errors="replace").replace("\n", " ")
        raise RuntimeError(f"El archivo descargado no parece Excel. Primeros bytes: {snippet}")
    df.columns = [normalizar_columna(columna) for columna in df.columns]

    nuevos = []
    for _, row in df.iterrows():
        sku_oferta = normalizar_identificador(row.get("sku de oferta"))
        sku_producto = normalizar_identificador(row.get("sku de producto"))
        vgc = normalizar_identificador(row.get("vgc"))
        producto = limpiar_texto(row.get("producto"))
        estado_oferta = limpiar_texto(row.get("estado de oferta")).upper()
        motivo = limpiar_texto(row.get("motivo oferta inactiva"))
        precio_base = normalizar_precio(row.get("precio base"))

        cantidad_raw = row.get("cantidad")
        cantidad = 0
        if cantidad_raw is not None and not pd.isna(cantidad_raw):
            try:
                cantidad = int(float(cantidad_raw))
            except (TypeError, ValueError):
                cantidad = 0

        if sku_oferta.startswith("Eliminado_") or not sku_oferta or not sku_producto:
            continue

        estado_inicial = clasificar_estado_oferta(estado_oferta, motivo, cantidad)

        vgc_limpio = re.sub(r"[^0-9]", "", vgc)
        product_id = vgc_limpio if len(vgc_limpio) >= 8 else sku_producto
        url = f"https://www.liverpool.com.mx/tienda/pdp/producto/{product_id}?skuid={sku_producto}"

        nuevos.append(
            {
                "sku_patish": sku_oferta,
                "sku_liverpool": sku_producto,
                "product_id": product_id,
                "vgc": vgc,
                "producto": producto,
                "estado_oferta": estado_inicial,
                "motivo": motivo,
                "precio_base": precio_base,
                "cantidad": cantidad,
                "url": url,
                "color": "",
                "size": "",
            }
        )

    return nuevos


def aplicar_catalogo_nuevo(nuevos, source="manual"):
    global CATALOGO
    CATALOGO = nuevos
    guardar_catalogo_persistido(nuevos)
    guardar_skus_csv(nuevos)
    activas = sum(1 for item in nuevos if item["estado_oferta"] == "ACTIVA")
    inactivas_stock = sum(1 for item in nuevos if item["estado_oferta"] == "INACTIVA_STOCK")
    bloqueadas = sum(1 for item in nuevos if item["estado_oferta"] == "BLOQUEADA")
    now = datetime.now(CDMX_TZ).strftime("%Y-%m-%d %H:%M:%S")
    CATALOGO_SYNC_STATE.update({
        "last_sync_at": now,
        "source": source,
        "ok": True,
        "error": "",
        "error_type": "",
        "last_error_at": "",
        "total": len(nuevos),
        "activas": activas,
        "inactivas_stock": inactivas_stock,
        "bloqueadas": bloqueadas,
    })
    return {
        "total": len(nuevos),
        "activas": activas,
        "inactivas_stock": inactivas_stock,
        "bloqueadas": bloqueadas,
    }


def leer_token_persistido():
    try:
        with open(CATALOGO_TOKEN_FILE, "r") as f:
            data = json.load(f)
        return data.get("bearer", "").strip()
    except Exception:
        return ""


def guardar_token_persistido(bearer):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(CATALOGO_TOKEN_FILE, "w") as f:
        json.dump({"bearer": bearer, "guardado_at": datetime.now(CDMX_TZ).strftime("%Y-%m-%d %H:%M:%S")}, f)


def descargar_catalogo_excel():
    if not CATALOGO_EXCEL_URL:
        raise RuntimeError("Falta CATALOGO_EXCEL_URL")
    headers = {
        **HEADERS,
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://marketplace.liverpool.com.mx",
        "Referer": "https://marketplace.liverpool.com.mx/",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    token = leer_token_persistido() or CATALOGO_AUTH_BEARER
    if token:
        headers["Authorization"] = token if token.lower().startswith("bearer ") else f"Bearer {token}"
    respuesta = requests.get(CATALOGO_EXCEL_URL, headers=headers, timeout=90, allow_redirects=True)
    respuesta.raise_for_status()
    content_type = limpiar_texto(respuesta.headers.get("content-type", "")).lower()
    contenido = respuesta.content
    if contenido[:2] == b"PK" or contenido[:4] == b"\xd0\xcf\x11\xe0":
        return contenido

    texto_base64 = contenido.strip()
    if texto_base64.startswith(b"UEsD"):
        try:
            decoded = base64.b64decode(texto_base64, validate=True)
            if decoded[:2] == b"PK" or decoded[:4] == b"\xd0\xcf\x11\xe0":
                return decoded
        except Exception:
            pass

    # Algunos exports responden JSON con URL firmada o contenido base64.
    data = None
    if "json" in content_type or contenido[:1] in (b"{", b"["):
        try:
            data = respuesta.json()
        except Exception:
            data = None

    def walk_json(obj):
        if isinstance(obj, dict):
            for key, value in obj.items():
                yield key, value
                yield from walk_json(value)
        elif isinstance(obj, list):
            for value in obj:
                yield from walk_json(value)

    if data is not None:
        for key, value in walk_json(data):
            key_l = str(key).lower()
            if isinstance(value, str) and value.startswith("http") and any(p in key_l for p in ("url", "link", "download", "file")):
                r2 = requests.get(value, headers=headers, timeout=90, allow_redirects=True)
                r2.raise_for_status()
                if r2.content[:2] == b"PK" or r2.content[:4] == b"\xd0\xcf\x11\xe0":
                    return r2.content
            if isinstance(value, str) and any(p in key_l for p in ("base64", "content", "file")):
                try:
                    decoded = base64.b64decode(value, validate=True)
                    if decoded[:2] == b"PK" or decoded[:4] == b"\xd0\xcf\x11\xe0":
                        return decoded
                except Exception:
                    pass

    debug_path = os.path.join(DATA_DIR, "catalogo_sync_debug.bin")
    try:
        with open(debug_path, "wb") as f:
            f.write(contenido[:5000])
    except Exception:
        pass
    snippet = contenido[:300].decode("utf-8", errors="replace").replace("\n", " ")
    raise RuntimeError(f"La descarga no parece Excel ({content_type or 'sin content-type'}). Debug: {debug_path}. Inicio: {snippet}")


def es_error_token_catalogo_expirado(exc):
    response = getattr(exc, "response", None)
    if response is not None and response.status_code in (401, 403):
        return True
    texto = str(exc).lower()
    return "401" in texto or "unauthorized" in texto or "token" in texto and "expir" in texto


def tipo_error_catalogo(exc):
    if es_error_token_catalogo_expirado(exc):
        return "token"
    if isinstance(exc, (requests.exceptions.ConnectionError, requests.exceptions.Timeout)):
        return "red_dns"
    texto = str(exc).lower()
    if any(p in texto for p in ("nameresolutionerror", "failed to resolve", "temporary failure", "connectionpool", "max retries", "timed out", "timeout")):
        return "red_dns"
    if any(p in texto for p in ("no parece excel", "file is not a zip", "excel file format")):
        return "archivo"
    return "otro"


def alertar_token_catalogo_expirado(exc):
    global CATALOGO_TOKEN_ALERT_LAST_TS
    ahora = time.time()
    intervalo = CATALOGO_TOKEN_ALERT_INTERVAL_HOURS * 3600
    if CATALOGO_TOKEN_ALERT_LAST_TS and ahora - CATALOGO_TOKEN_ALERT_LAST_TS < intervalo:
        return
    CATALOGO_TOKEN_ALERT_LAST_TS = ahora
    panel_link = " Renuévalo en el panel: /admin/token" if PANEL_SECRET else " Renueva CATALOGO_AUTH_BEARER en Railway."
    enviar_telegram(
        "⚠️ <b>Token de Liverpool vencido</b>\n\n"
        "No pude actualizar el catálogo automático desde Marketplace.\n"
        f"{panel_link}\n\n"
        f"Error: {escapar(str(exc)[:220])}"
    )


def sync_catalogo_desde_url(force=False):
    global CATALOGO_TOKEN_ALERT_LAST_TS
    if not CATALOGO_EXCEL_URL:
        return {"ok": False, "error": "Falta CATALOGO_EXCEL_URL", "configured": False}
    if not force and CATALOGO_SYNC_STATE.get("last_sync_at"):
        try:
            last = datetime.strptime(CATALOGO_SYNC_STATE["last_sync_at"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=CDMX_TZ)
            if datetime.now(CDMX_TZ) - last < timedelta(hours=CATALOGO_SYNC_INTERVAL_HOURS):
                return {"ok": True, "skipped": True, **CATALOGO_SYNC_STATE}
        except Exception:
            pass
    if not CATALOGO_SYNC_LOCK.acquire(blocking=False):
        return {"ok": False, "error": "Sync de catálogo ya está corriendo", "configured": True}
    try:
        CATALOGO_SYNC_STATE["last_attempt_at"] = datetime.now(CDMX_TZ).strftime("%Y-%m-%d %H:%M:%S")
        excel_bytes = descargar_catalogo_excel()
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(CATALOGO_SYNC_EXCEL_FILE, "wb") as f:
            f.write(excel_bytes)
        nuevos = procesar_excel_catalogo(excel_bytes)
        resumen = aplicar_catalogo_nuevo(nuevos, source="url")
        CATALOGO_TOKEN_ALERT_LAST_TS = 0
        print(f"📦 Catálogo auto-sync OK: {resumen['total']} variantes ({resumen['activas']} activas)")
        return {"ok": True, "configured": True, **resumen, **CATALOGO_SYNC_STATE}
    except Exception as exc:
        error_type = tipo_error_catalogo(exc)
        error_at = datetime.now(CDMX_TZ).strftime("%Y-%m-%d %H:%M:%S")
        CATALOGO_SYNC_STATE.update({
            "ok": False,
            "error": str(exc),
            "error_type": error_type,
            "last_error_at": error_at,
            "source": "url",
        })
        print(f"⚠️ Catálogo auto-sync error: {exc}")
        if error_type == "token":
            alertar_token_catalogo_expirado(exc)
        return {"ok": False, "configured": True, "error": str(exc), **CATALOGO_SYNC_STATE}
    finally:
        CATALOGO_SYNC_LOCK.release()


def construir_items_estado():
    items = []
    ventas_30d = ventas_por_sku(30)
    for variante in CATALOGO:
        sku = variante["sku_patish"]
        sku_base = sku_base_desde_patish(sku)
        ventas_info = ventas_30d.get(sku_base, {})
        product_id = normalizar_identificador(variante.get("product_id", ""))
        resumen_vgc = RESUMEN_VGC.get(product_id, {}) or RESUMEN_VGC.get(normalizar_identificador(variante.get("vgc", "")), {})
        estado_monitor = normalizar_estado_persistido(ULTIMO_ESTADO.get(sku, ""))
        if not estado_monitor:
            estado_final = variante["estado_oferta"] if variante["estado_oferta"] != "ACTIVA" else "SIN_DATOS"
        else:
            estado_final = estado_monitor
        precio_tuyo = ULTIMO_PRECIO_PATISH[sku] if sku in ULTIMO_PRECIO_PATISH else ""
        stock_tuyo = ULTIMO_STOCK_PATISH[sku] if sku in ULTIMO_STOCK_PATISH else normalizar_entero(variante.get("cantidad"))
        precio_minimo = PRECIOS_MINIMOS.get(sku)
        item_estado = {
                "sku_patish": sku,
                "sku_base": sku_base,
                "sku_liverpool": variante["sku_liverpool"],
                "product_id": product_id,
                "vgc": variante.get("vgc", ""),
                "producto": variante["producto"],
                "color": variante.get("color", ""),
                "size": variante.get("size", ""),
                "estado": estado_final,
                "seller_buybox": ULTIMO_SELLER.get(sku, ""),
                "precio_liverpool": ULTIMO_PRECIO.get(sku, ""),
                "precio_tuyo": precio_tuyo,
                "precio_minimo": formatear_precio(precio_minimo) if precio_minimo is not None else "",
                "stock_tuyo": stock_tuyo,
                "ventas_30d_piezas": ventas_info.get("piezas", 0),
                "ventas_30d_monto": ventas_info.get("monto", 0),
                "ventas_ultima_fecha": ventas_info.get("ultima_fecha", ""),
                "last_checked": ULTIMO_LAST_CHECKED.get(sku, ""),
                "source": ULTIMO_SOURCE.get(sku, ""),
                "status_code": ULTIMO_STATUS_CODE.get(sku, ""),
                "error_message": ULTIMO_ERROR_MESSAGE.get(sku, ""),
                "confidence": ULTIMO_CONFIDENCE.get(sku, ""),
                "stock_ganador": ULTIMO_STOCK_GANADOR.get(sku, ""),
                "segundo_seller": ULTIMO_SEGUNDO_SELLER.get(sku, ""),
                "segundo_precio": ULTIMO_SEGUNDO_PRECIO.get(sku, ""),
                "reprice_sugerido": ULTIMO_REPRICE_SUGERIDO.get(sku, ""),
                "reprice_motivo": ULTIMO_REPRICE_MOTIVO.get(sku, ""),
                "vgc_alerta": bool(resumen_vgc.get("alerta")),
                "vgc_alerta_texto": resumen_vgc.get("alerta_texto", ""),
                "vgc_detalle_texto": resumen_vgc.get("detalle_texto", ""),
                "vgc_detalle": resumen_vgc,
                "url": variante["url"],
        }
        prioridad, score = prioridad_oportunidad_item(item_estado)
        item_estado["oportunidad_prioridad"] = prioridad
        item_estado["oportunidad_score"] = score
        item_estado["accion_recomendada"] = accion_recomendada_item(item_estado)
        item_estado["accion_grupo"] = accion_grupo_item(item_estado)
        items.append(item_estado)
    return items


COLUMNAS_FILTRO_TEXTO = (
    "producto", "sku_patish", "sku_liverpool", "vgc", "seller_buybox", "url",
)
COLUMNAS_FILTRO_NUMERICO = ("precio_liverpool", "precio_tuyo", "stock_tuyo", "diferencia")


def obtener_filtros_columna_request():
    filtros = {}
    for campo in (*COLUMNAS_FILTRO_TEXTO, "estado", *COLUMNAS_FILTRO_NUMERICO):
        valor = request.args.get(f"f_{campo}", "").strip()
        if valor:
            filtros[campo] = valor
    return filtros


def confianza_bucket_item(item):
    confidence = limpiar_texto(item.get("confidence", "")).lower()
    source = limpiar_texto(item.get("source", "")).upper()
    if "conflict" in confidence:
        return "CONFLICTO"
    if source == "PDP":
        return "PDP"
    if source in {"OFFERSLISTING", "ALLOFFERS"}:
        return "API"
    return "OTRO"


def es_oportunidad_item(item):
    return (
        item.get("estado") == "PERDIDO"
        and bool(item.get("reprice_sugerido"))
        and (normalizar_entero(item.get("stock_tuyo")) or 0) > 0
        and confianza_bucket_item(item) != "API"
    )


def prioridad_oportunidad_item(item):
    if not es_oportunidad_item(item):
        return "", 0
    ganador = normalizar_precio(item.get("precio_liverpool"))
    mio = normalizar_precio(item.get("precio_tuyo"))
    gap = (mio - ganador) if ganador is not None and mio is not None else None
    score = 40
    if gap is not None:
        if gap <= 50:
            score += 40
        elif gap <= 300:
            score += 25
        else:
            score += 10
    stock_ganador = normalizar_entero(item.get("stock_ganador"))
    if stock_ganador is not None and stock_ganador <= 5:
        score += 15
    ventas = normalizar_precio(item.get("ventas_30d_piezas")) or 0
    if ventas >= 5:
        score += 15
    elif ventas > 0:
        score += 7
    else:
        score -= 5
    if confianza_bucket_item(item) == "PDP":
        score += 10
    elif confianza_bucket_item(item) == "CONFLICTO":
        score -= 5
    if score >= 85:
        return "ALTA", score
    if score >= 65:
        return "MEDIA", score
    return "BAJA", score


def accion_recomendada_item(item):
    estado = item.get("estado", "")
    if estado == "PERDIDO" and item.get("reprice_sugerido"):
        ventas = normalizar_precio(item.get("ventas_30d_piezas")) or 0
        minimo = normalizar_precio(item.get("precio_minimo"))
        sugerido = normalizar_precio(item.get("reprice_sugerido"))
        ventas_txt = f" · {formatear_precio(ventas)} ventas 30d" if ventas else " · sin ventas 30d"
        if minimo is None:
            return f"Simular {formatear_money(sugerido)}; falta precio mínimo{ventas_txt}"
        if sugerido is not None and sugerido < minimo:
            return f"No bajar: sugerido {formatear_money(sugerido)} queda bajo mínimo {formatear_money(minimo)}"
        return f"Bajar a {formatear_money(sugerido)}{ventas_txt}"
    if estado == "PERDIDO":
        return "Revisar motivo de pérdida"
    if estado == "NO_PRENDIDA":
        return "Revisar si la oferta está publicada"
    if estado == "BLOQUEADA":
        return "Resolver bloqueo Liverpool"
    if estado == "INACTIVA_STOCK":
        return "Agregar stock"
    if estado in ESTADOS_INVALIDOS:
        return "Validar SKU/PDP"
    return ""


def accion_grupo_item(item):
    if not es_oportunidad_item(item):
        return ""
    if confianza_bucket_item(item) == "CONFLICTO":
        return "NO_TOCAR"
    minimo = normalizar_precio(item.get("precio_minimo"))
    sugerido = normalizar_precio(item.get("reprice_sugerido"))
    if minimo is None:
        return "FALTA_MINIMO"
    if sugerido is None or sugerido < minimo:
        return "NO_TOCAR"
    return "HACER_AHORA"


def coincide_filtro_texto(valor, filtro):
    if not filtro:
        return True
    return filtro.strip().lower() in str(valor or "").lower()


def coincide_filtro_numerico(valor, filtro):
    criterio = (filtro or "").replace(" ", "")
    if not criterio:
        return True
    numero = normalizar_precio(valor)
    if numero is None:
        return False
    rango = re.fullmatch(r"(-?\d+(?:\.\d+)?)\-(-?\d+(?:\.\d+)?)", criterio)
    if rango:
        minimo = float(rango.group(1))
        maximo = float(rango.group(2))
        if minimo > maximo:
            minimo, maximo = maximo, minimo
        return minimo <= numero <= maximo
    comparacion = re.fullmatch(r"(>=|<=|>|<|=)?(-?\d+(?:\.\d+)?)", criterio)
    if not comparacion:
        return False
    operador = comparacion.group(1) or "="
    objetivo = float(comparacion.group(2))
    if operador == ">":
        return numero > objetivo
    if operador == "<":
        return numero < objetivo
    if operador == ">=":
        return numero >= objetivo
    if operador == "<=":
        return numero <= objetivo
    return numero == objetivo


def aplicar_filtros_columna(items, filtros_columna):
    if not filtros_columna:
        return items
    filtrados = []
    for item in items:
        if filtros_columna.get("estado") and item.get("estado", "") != filtros_columna["estado"]:
            continue
        coincide = True
        for campo in COLUMNAS_FILTRO_TEXTO:
            if not coincide_filtro_texto(item.get(campo, ""), filtros_columna.get(campo, "")):
                coincide = False
                break
        if not coincide:
            continue
        for campo in COLUMNAS_FILTRO_NUMERICO:
            valor = calcular_diferencia_item(item) if campo == "diferencia" else item.get(campo)
            if not coincide_filtro_numerico(valor, filtros_columna.get(campo, "")):
                coincide = False
                break
        if coincide:
            filtrados.append(item)
    return filtrados


def filtrar_items_estado(items, estado, busqueda, filtros_columna=None, confianza="TODAS", accion="TODAS"):
    filtrados = items
    if estado and estado != "TODOS":
        if estado == "OPORTUNIDADES":
            filtrados = [item for item in filtrados if es_oportunidad_item(item)]
        else:
            filtrados = [item for item in filtrados if item["estado"] == estado]
    confianza = limpiar_texto(confianza).upper() or "TODAS"
    if confianza != "TODAS":
        filtrados = [item for item in filtrados if confianza_bucket_item(item) == confianza]
    accion = limpiar_texto(accion).upper() or "TODAS"
    if accion != "TODAS":
        filtrados = [item for item in filtrados if item.get("accion_grupo") == accion]
    if busqueda:
        termino = busqueda.strip().lower()
        filtrados = [
            item for item in filtrados
            if termino in str(item.get("producto", "")).lower()
            or termino in str(item.get("sku_liverpool", "")).lower()
            or termino in str(item.get("sku_patish", "")).lower()
            or termino in str(item.get("vgc", "")).lower()
        ]
    filtrados = aplicar_filtros_columna(filtrados, filtros_columna or {})
    return filtrados


def calcular_diferencia_item(item):
    precio_liverpool = normalizar_precio(item.get("precio_liverpool"))
    precio_tuyo = normalizar_precio(item.get("precio_tuyo"))
    if precio_liverpool is None or precio_tuyo is None:
        return None
    return precio_liverpool - precio_tuyo


ESTADO_ORDEN = {
    "GANANDO_VERIFICADO": 0, "GANANDO": 0, "GANANDO_API_NO_VISIBLE": 1,
    "PERDIDO": 2, "NO_PRENDIDA": 3, "SKU_INVALIDO": 4,
    "PRODUCTO_NO_EXISTE": 5, "VGC_INVALIDO": 6,
    "BLOQUEADA": 7, "INACTIVA_STOCK": 8, "SIN_DATOS_STALE": 9, "SIN_DATOS": 10,
}

SORT_ALIASES = {"stock_desc": "stock_tuyo_desc", "stock_asc": "stock_tuyo_asc"}

SORTABLE_ITEM_FIELDS = {
    "producto", "sku_patish", "sku_liverpool", "vgc",
    "estado", "seller_buybox", "precio_liverpool", "precio_tuyo", "stock_tuyo", "diferencia", "url",
}


def parsear_orden_items(orden):
    orden_normalizado = SORT_ALIASES.get(limpiar_texto(orden).lower(), limpiar_texto(orden).lower()) or "producto_asc"
    match = re.fullmatch(r"(.+)_(asc|desc)", orden_normalizado)
    if not match:
        return "producto", "asc"
    campo, direccion = match.groups()
    if campo not in SORTABLE_ITEM_FIELDS:
        return "producto", "asc"
    return campo, direccion


def comparar_texto_sort(a, b):
    texto_a = limpiar_texto(a).lower()
    texto_b = limpiar_texto(b).lower()
    if texto_a < texto_b:
        return -1
    if texto_a > texto_b:
        return 1
    return 0


def valor_sort_item(item, campo):
    if campo == "diferencia":
        return calcular_diferencia_item(item)
    if campo == "estado":
        return ESTADO_ORDEN.get(limpiar_texto(item.get("estado", "")).upper(), 999)
    if campo in {"precio_liverpool", "precio_tuyo"}:
        return normalizar_precio(item.get(campo))
    if campo == "stock_tuyo":
        return normalizar_entero(item.get(campo))
    return limpiar_texto(item.get(campo, "")).lower()


def ordenar_items_estado(items, orden):
    campo, direccion = parsear_orden_items(orden)

    def comparar_items(a, b):
        valor_a = valor_sort_item(a, campo)
        valor_b = valor_sort_item(b, campo)
        a_vacio = valor_a in (None, "")
        b_vacio = valor_b in (None, "")
        if a_vacio and not b_vacio:
            return 1
        if not a_vacio and b_vacio:
            return -1
        resultado = 0
        if not a_vacio and not b_vacio:
            if isinstance(valor_a, (int, float)) and isinstance(valor_b, (int, float)):
                if valor_a < valor_b:
                    resultado = -1
                elif valor_a > valor_b:
                    resultado = 1
            else:
                resultado = comparar_texto_sort(valor_a, valor_b)
        if direccion == "desc":
            resultado *= -1
        if resultado != 0:
            return resultado
        resultado = comparar_texto_sort(a.get("producto", ""), b.get("producto", ""))
        if resultado != 0:
            return resultado
        return comparar_texto_sort(a.get("sku_patish", ""), b.get("sku_patish", ""))

    return sorted(items, key=cmp_to_key(comparar_items))


def resumen_competidores(items):
    resumen = {}
    for item in items:
        seller = limpiar_texto(item.get("seller_buybox", ""))
        if not seller or es_seller_mio(seller, ""):
            continue
        datos = resumen.setdefault(seller, {
            "seller": seller,
            "skus": 0,
            "oportunidades": 0,
            "diferencia_total": 0.0,
            "diferencia_count": 0,
            "stock_visible_total": 0,
            "stock_visible_count": 0,
        })
        datos["skus"] += 1
        if es_oportunidad_item(item):
            datos["oportunidades"] += 1
        ganador = normalizar_precio(item.get("precio_liverpool"))
        mio = normalizar_precio(item.get("precio_tuyo"))
        if ganador is not None and mio is not None:
            datos["diferencia_total"] += mio - ganador
            datos["diferencia_count"] += 1
        stock = normalizar_entero(item.get("stock_ganador"))
        if stock is not None:
            datos["stock_visible_total"] += stock
            datos["stock_visible_count"] += 1

    salida = []
    for datos in resumen.values():
        dif_prom = datos["diferencia_total"] / datos["diferencia_count"] if datos["diferencia_count"] else None
        stock_prom = datos["stock_visible_total"] / datos["stock_visible_count"] if datos["stock_visible_count"] else None
        salida.append({
            "seller": datos["seller"],
            "skus": datos["skus"],
            "oportunidades": datos["oportunidades"],
            "diferencia_promedio": formatear_precio(dif_prom) if dif_prom is not None else "",
            "stock_visible_promedio": int(round(stock_prom)) if stock_prom is not None else "",
        })
    return sorted(salida, key=lambda x: (x["oportunidades"], x["skus"]), reverse=True)[:8]


def historial_sku(sku_patish, limite=20):
    if not os.path.exists(CSV_FILE):
        return []
    eventos = []
    try:
        with open(CSV_FILE, newline="", encoding="utf-8-sig") as archivo:
            reader = csv.DictReader(archivo)
            for row in reader:
                if limpiar_texto(row.get("sku_patish", "")) == limpiar_texto(sku_patish):
                    eventos.append({
                        "fecha_hora": row.get("fecha_hora", ""),
                        "estado": row.get("estado", ""),
                        "seller": row.get("seller_buybox", ""),
                        "precio": row.get("precio", ""),
                        "tipo_cambio": row.get("tipo_cambio", ""),
                    })
    except Exception as exc:
        return [{"fecha_hora": "", "estado": "ERROR", "seller": "", "precio": "", "tipo_cambio": str(exc)}]
    return eventos[-limite:][::-1]


# ================================
# RUTAS FLASK
# ================================

@app.route("/")
def panel():
    return render_template_string(HTML_PANEL)


@app.route("/health")
def health():
    return jsonify({"ok": True})


@app.route("/api/catalogo", methods=["POST"])
def api_catalogo_post():
    if "file" not in request.files:
        return jsonify({"ok": False, "error": "No se recibio archivo"}), 400
    archivo = request.files["file"]
    if not archivo.filename:
        return jsonify({"ok": False, "error": "Archivo vacio"}), 400
    try:
        excel_bytes = archivo.read()
        nuevos = procesar_excel_catalogo(excel_bytes)
        resumen = aplicar_catalogo_nuevo(nuevos, source="manual_upload")
        activas = resumen["activas"]
        inactivas_stock = resumen["inactivas_stock"]
        bloqueadas = resumen["bloqueadas"]
        return jsonify({"ok": True, **resumen})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/api/catalogo/sync", methods=["POST"])
def api_catalogo_sync():
    resultado = sync_catalogo_desde_url(force=True)
    status_code = 200 if resultado.get("ok") else 400
    return jsonify(resultado), status_code


@app.route("/api/catalogo/sync/status")
def api_catalogo_sync_status():
    return jsonify({
        "configured": bool(CATALOGO_EXCEL_URL),
        "auth_configured": bool(leer_token_persistido() or CATALOGO_AUTH_BEARER),
        "token_source": "persistido" if leer_token_persistido() else ("env" if CATALOGO_AUTH_BEARER else "ninguno"),
        "interval_hours": CATALOGO_SYNC_INTERVAL_HOURS,
        "download_available": os.path.exists(CATALOGO_SYNC_EXCEL_FILE),
        **CATALOGO_SYNC_STATE,
    })


@app.route("/api/catalogo/sync/download")
def api_catalogo_sync_download():
    if not os.path.exists(CATALOGO_SYNC_EXCEL_FILE):
        return jsonify({"ok": False, "error": "No hay archivo de auto-sync descargado todavía"}), 404
    return send_file(
        CATALOGO_SYNC_EXCEL_FILE,
        as_attachment=True,
        download_name=f"catalogo_sync_{datetime.now(CDMX_TZ).strftime('%Y%m%d_%H%M%S')}.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@app.route("/api/estado")
def api_estado():
    items = construir_items_estado()
    return jsonify({
        "ganando": sum(1 for item in items if es_estado_ganador_verificado(item["estado"])),
        "perdidos": sum(1 for item in items if item["estado"] == "PERDIDO"),
        "oportunidades": sum(1 for item in items if es_oportunidad_item(item)),
        "no_prendida": sum(1 for item in items if item["estado"] == "NO_PRENDIDA"),
        "ganando_api_no_visible": sum(1 for item in items if item["estado"] == "GANANDO_API_NO_VISIBLE"),
        "sku_invalido": sum(1 for item in items if item["estado"] == "SKU_INVALIDO"),
        "producto_no_existe": sum(1 for item in items if item["estado"] == "PRODUCTO_NO_EXISTE"),
        "sin_datos_stale": sum(1 for item in items if item["estado"] == "SIN_DATOS_STALE"),
        "bloqueadas": sum(1 for item in items if item["estado"] == "BLOQUEADA"),
        "sin_stock": sum(1 for item in items if item["estado"] == "INACTIVA_STOCK"),
        "total": len(items),
        "competidores": resumen_competidores(items),
        "items": items,
    })


@app.route("/api/sku/<sku_patish>/historial")
def api_sku_historial(sku_patish):
    items = construir_items_estado()
    item = next((i for i in items if limpiar_texto(i.get("sku_patish", "")) == limpiar_texto(sku_patish)), None)
    return jsonify({
        "ok": bool(item),
        "item": item or {},
        "historial": historial_sku(sku_patish),
    })


@app.route("/api/precio-minimo", methods=["POST"])
def api_precio_minimo():
    data = request.get_json(silent=True) or {}
    sku = limpiar_texto(data.get("sku_patish", ""))
    precio = normalizar_precio(data.get("precio_minimo"))
    if not sku:
        return jsonify({"ok": False, "error": "Falta sku_patish"}), 400
    if precio is None:
        PRECIOS_MINIMOS.pop(sku, None)
    else:
        PRECIOS_MINIMOS[sku] = precio
    guardar_precios_minimos()
    return jsonify({"ok": True, "sku_patish": sku, "precio_minimo": formatear_precio(PRECIOS_MINIMOS.get(sku, ""))})


@app.route("/api/precios-minimos/carga", methods=["POST"])
def api_precios_minimos_carga():
    if "file" not in request.files:
        return jsonify({"ok": False, "error": "No se recibio archivo"}), 400
    archivo = request.files["file"]
    if not archivo.filename:
        return jsonify({"ok": False, "error": "Archivo vacio"}), 400
    try:
        contenido = archivo.read()
        nombre = archivo.filename.lower()
        if nombre.endswith(".csv"):
            df = pd.read_csv(io.BytesIO(contenido))
        elif contenido[:2] == b"PK":
            df = pd.read_excel(io.BytesIO(contenido), engine="openpyxl")
        elif contenido[:4] == b"\xd0\xcf\x11\xe0":
            df = pd.read_excel(io.BytesIO(contenido), engine="xlrd")
        else:
            return jsonify({"ok": False, "error": "Formato no soportado. Usa .xlsx, .xls o .csv"}), 400
        df.columns = [normalizar_columna(columna) for columna in df.columns]
        sku_col = next((c for c in ("sku patish", "sku_patish", "sku de oferta", "sku oferta", "sku") if c in df.columns), None)
        precio_col = next((c for c in ("precio minimo", "precio mínimo", "precio_minimo", "minimo", "mínimo", "precio min") if c in df.columns), None)
        if not sku_col or not precio_col:
            return jsonify({"ok": False, "error": "El archivo necesita columnas SKU PATISH y Precio minimo"}), 400
        cargados = 0
        omitidos = 0
        for _, row in df.iterrows():
            sku = limpiar_texto(row.get(sku_col, ""))
            precio = normalizar_precio(row.get(precio_col))
            if not sku or precio is None:
                omitidos += 1
                continue
            PRECIOS_MINIMOS[sku] = precio
            cargados += 1
        guardar_precios_minimos()
        return jsonify({"ok": True, "cargados": cargados, "omitidos": omitidos})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/api/vgc/<vgc>")
def api_vgc(vgc):
    key = normalizar_identificador(vgc)
    return jsonify({"ok": True, "vgc": key, "resumen": RESUMEN_VGC.get(key, {})})


@app.route("/api/exportar")
def api_exportar():
    estado = request.args.get("estado", "TODOS").strip().upper() or "TODOS"
    confianza = request.args.get("confianza", "TODAS").strip().upper() or "TODAS"
    accion = request.args.get("accion", "TODAS").strip().upper() or "TODAS"
    busqueda = request.args.get("q", "").strip()
    orden = request.args.get("sort", "producto_asc").strip()
    filtros_columna = obtener_filtros_columna_request()
    items = construir_items_estado()
    filtrados = filtrar_items_estado(items, estado, busqueda, filtros_columna, confianza, accion)
    filtrados = ordenar_items_estado(filtrados, orden)
    columnas = ["estado", "sku_patish", "sku_liverpool", "vgc", "producto",
                "seller_buybox", "precio_liverpool", "precio_tuyo", "stock_tuyo",
                "ventas_30d_piezas", "ventas_30d_monto", "ventas_ultima_fecha",
                "precio_minimo", "oportunidad_prioridad", "accion_recomendada",
                "stock_ganador", "segundo_seller", "segundo_precio", "reprice_sugerido",
                "reprice_motivo", "last_checked", "source", "status_code", "error_message", "confidence", "url"]
    df = pd.DataFrame(filtrados, columns=columnas)
    if not df.empty:
        df["diferencia"] = [
            formatear_precio(calcular_diferencia_item(item)) if calcular_diferencia_item(item) is not None else ""
            for item in filtrados
        ]
    df = df.rename(columns={
        "estado": "Estado", "sku_patish": "SKU PATISH", "sku_liverpool": "SKU Liverpool",
        "vgc": "VGC", "producto": "Producto",
        "seller_buybox": "Seller BuyBox", "precio_liverpool": "Precio Liverpool",
        "precio_tuyo": "Tu Precio", "stock_tuyo": "Stock Tuyo", "diferencia": "Diferencia",
        "ventas_30d_piezas": "Ventas 30d Piezas", "ventas_30d_monto": "Ventas 30d Monto",
        "ventas_ultima_fecha": "Ultima Venta",
        "precio_minimo": "Precio Minimo", "oportunidad_prioridad": "Prioridad",
        "accion_recomendada": "Accion Recomendada",
        "stock_ganador": "Stock Ganador", "segundo_seller": "Segundo Seller",
        "segundo_precio": "Segundo Precio", "reprice_sugerido": "Precio Sugerido",
        "reprice_motivo": "Motivo Repricing",
        "last_checked": "Last Checked", "source": "Fuente", "status_code": "Status Code",
        "error_message": "Error", "confidence": "Confianza", "url": "URL",
    })
    salida = BytesIO()
    nombre_estado = estado.lower()
    if busqueda or filtros_columna:
        nombre_estado += "_filtrado"
    nombre_archivo = f"buybox_{nombre_estado}_{datetime.now(CDMX_TZ).strftime('%Y%m%d_%H%M%S')}.xlsx"
    with pd.ExcelWriter(salida, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="buybox", index=False)
        hoja = writer.sheets["buybox"]
        hoja.freeze_panes = "A2"
        for columna in hoja.columns:
            letra = columna[0].column_letter
            largo = max(len(str(celda.value or "")) for celda in columna)
            hoja.column_dimensions[letra].width = min(max(largo + 2, 12), 48)
    salida.seek(0)
    return send_file(salida, as_attachment=True, download_name=nombre_archivo,
                     mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


@app.route("/api/exportar/acciones")
def api_exportar_acciones():
    confianza = request.args.get("confianza", "TODAS").strip().upper() or "TODAS"
    accion = request.args.get("accion", "TODAS").strip().upper() or "TODAS"
    items = construir_items_estado()
    filtrados = filtrar_items_estado(items, "OPORTUNIDADES", "", {}, confianza, accion)
    orden_accion = {"HACER_AHORA": 0, "FALTA_MINIMO": 1, "NO_TOCAR": 2}
    filtrados = sorted(
        filtrados,
        key=lambda item: (
            orden_accion.get(item.get("accion_grupo", ""), 9),
            -(normalizar_entero(item.get("oportunidad_score")) or 0),
            limpiar_texto(item.get("producto", "")).lower(),
        ),
    )
    columnas = [
        "accion_grupo", "oportunidad_prioridad", "accion_recomendada",
        "sku_patish", "sku_liverpool", "vgc", "producto",
        "seller_buybox", "precio_liverpool", "precio_tuyo", "reprice_sugerido",
        "precio_minimo", "ventas_30d_piezas", "ventas_30d_monto", "ventas_ultima_fecha",
        "stock_tuyo", "stock_ganador", "segundo_seller",
        "segundo_precio", "source", "confidence", "error_message", "url",
    ]
    df = pd.DataFrame(filtrados, columns=columnas)
    if not df.empty:
        df["diferencia"] = [
            formatear_precio((normalizar_precio(item.get("precio_tuyo")) or 0) - (normalizar_precio(item.get("precio_liverpool")) or 0))
            for item in filtrados
        ]
    df = df.rename(columns={
        "accion_grupo": "Grupo Accion", "oportunidad_prioridad": "Prioridad",
        "accion_recomendada": "Accion Recomendada", "sku_patish": "SKU PATISH",
        "sku_liverpool": "SKU Liverpool", "vgc": "VGC", "producto": "Producto",
        "seller_buybox": "Seller Ganador", "precio_liverpool": "Precio Ganador",
        "precio_tuyo": "Tu Precio", "reprice_sugerido": "Precio Sugerido",
        "precio_minimo": "Precio Minimo", "stock_tuyo": "Stock Tuyo",
        "ventas_30d_piezas": "Ventas 30d Piezas", "ventas_30d_monto": "Ventas 30d Monto",
        "ventas_ultima_fecha": "Ultima Venta",
        "stock_ganador": "Stock Ganador", "segundo_seller": "Segundo Seller",
        "segundo_precio": "Segundo Precio", "source": "Fuente",
        "confidence": "Confianza", "error_message": "Notas", "url": "URL",
        "diferencia": "Diferencia",
    })
    salida = BytesIO()
    nombre_archivo = f"acciones_sugeridas_{datetime.now(CDMX_TZ).strftime('%Y%m%d_%H%M%S')}.xlsx"
    with pd.ExcelWriter(salida, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="acciones", index=False)
        hoja = writer.sheets["acciones"]
        hoja.freeze_panes = "A2"
        for columna in hoja.columns:
            letra = columna[0].column_letter
            largo = max(len(str(celda.value or "")) for celda in columna)
            hoja.column_dimensions[letra].width = min(max(largo + 2, 12), 52)
    salida.seek(0)
    return send_file(salida, as_attachment=True, download_name=nombre_archivo,
                     mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


HTML_ADMIN_TOKEN = """<!DOCTYPE html>
<html lang="es">
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Renovar token Liverpool</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f1f5f9;min-height:100vh;display:flex;align-items:center;justify-content:center;padding:20px}
.card{background:#fff;border-radius:16px;box-shadow:0 4px 24px rgba(0,0,0,.08);padding:32px;max-width:520px;width:100%}
h1{font-size:18px;font-weight:700;color:#0f172a;margin-bottom:6px}
p{font-size:13px;color:#64748b;margin-bottom:20px;line-height:1.5}
label{font-size:12px;font-weight:600;color:#374151;display:block;margin-bottom:6px}
textarea{width:100%;min-height:130px;padding:10px 12px;border:1px solid #e2e8f0;border-radius:8px;font-size:12px;font-family:monospace;resize:vertical;outline:none}
textarea:focus{border-color:#0ea5e9;box-shadow:0 0 0 3px rgba(14,165,233,.15)}
.row{display:flex;gap:10px;margin-top:16px;align-items:center}
input[type=password]{flex:1;padding:9px 12px;border:1px solid #e2e8f0;border-radius:8px;font-size:13px;outline:none}
input[type=password]:focus{border-color:#0ea5e9;box-shadow:0 0 0 3px rgba(14,165,233,.15)}
button{background:#0ea5e9;color:#fff;border:none;border-radius:8px;padding:10px 22px;font-size:14px;font-weight:600;cursor:pointer}
button:hover{background:#0284c7}
.msg{margin-top:16px;padding:10px 14px;border-radius:8px;font-size:13px;display:none}
.msg.ok{background:#f0fdf4;color:#166534;border:1px solid #bbf7d0;display:block}
.msg.err{background:#fef2f2;color:#991b1b;border:1px solid #fecaca;display:block}
.hint{font-size:11px;color:#94a3b8;margin-top:8px}
.current{margin-top:20px;padding:10px 14px;background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;font-size:12px;color:#475569}
</style></head>
<body>
<div class="card">
  <h1>🔑 Renovar token Liverpool</h1>
  <p>Pega aquí el Bearer token que copiaste de DevTools en Marketplace Liverpool. El monitor lo usará de inmediato sin necesidad de reiniciar.</p>
  <label>Nuevo Bearer token</label>
  <textarea id="token" placeholder="eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9..."></textarea>
  <p class="hint">Puedes pegar el token completo con o sin el prefijo "Bearer ".</p>
  <div class="row">
    <input type="password" id="secret" placeholder="PANEL_SECRET">
    <button onclick="guardar()">Guardar</button>
  </div>
  <div id="msg" class="msg"></div>
  <div class="current">{{ estado_actual }}</div>
</div>
<script>
async function guardar(){
  const token=document.getElementById('token').value.trim();
  const secret=document.getElementById('secret').value.trim();
  const msg=document.getElementById('msg');
  msg.className='msg';msg.textContent='';
  if(!token){msg.className='msg err';msg.textContent='Pega el token primero.';return;}
  if(!secret){msg.className='msg err';msg.textContent='Ingresa el PANEL_SECRET.';return;}
  try{
    const r=await fetch('/admin/token',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({bearer:token,secret})});
    const d=await r.json();
    if(d.ok){msg.className='msg ok';msg.textContent='Token guardado. El próximo sync usará este token.';}
    else{msg.className='msg err';msg.textContent=d.error||'Error desconocido.';}
  }catch(e){msg.className='msg err';msg.textContent='Error de red: '+e;}
}
</script>
</body></html>"""


@app.route("/admin/token", methods=["GET"])
def admin_token_get():
    if not PANEL_SECRET:
        return "PANEL_SECRET no configurado en Railway. Agrega esa variable de entorno primero.", 503
    token_actual = leer_token_persistido()
    if token_actual:
        preview = token_actual[:16] + "..." + token_actual[-8:]
        estado = f"Token persistido activo: {preview}"
    elif CATALOGO_AUTH_BEARER:
        estado = "Usando token de variable de entorno CATALOGO_AUTH_BEARER (sin token persistido guardado aún)"
    else:
        estado = "Sin token configurado — el sync del catálogo fallará con 401"
    html = HTML_ADMIN_TOKEN.replace("{{ estado_actual }}", estado)
    return html


@app.route("/admin/token", methods=["POST"])
def admin_token_post():
    if not PANEL_SECRET:
        return jsonify({"ok": False, "error": "PANEL_SECRET no configurado en Railway"}), 503
    try:
        data = request.get_json(force=True) or {}
    except Exception:
        return jsonify({"ok": False, "error": "JSON inválido"}), 400
    if data.get("secret", "") != PANEL_SECRET:
        return jsonify({"ok": False, "error": "PANEL_SECRET incorrecto"}), 403
    bearer = str(data.get("bearer", "")).strip()
    if not bearer:
        return jsonify({"ok": False, "error": "Token vacío"}), 400
    try:
        guardar_token_persistido(bearer)
        return jsonify({"ok": True})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/status")
def status():
    return jsonify({
        "ganando": sum(1 for valor in ULTIMO_ESTADO.values() if es_estado_ganador_verificado(valor)),
        "perdidos": sum(1 for valor in ULTIMO_ESTADO.values() if valor == "PERDIDO"),
        "no_prendida": sum(1 for valor in ULTIMO_ESTADO.values() if valor == "NO_PRENDIDA"),
        "ganando_api_no_visible": sum(1 for valor in ULTIMO_ESTADO.values() if valor == "GANANDO_API_NO_VISIBLE"),
        "sku_invalido": sum(1 for valor in ULTIMO_ESTADO.values() if valor == "SKU_INVALIDO"),
        "producto_no_existe": sum(1 for valor in ULTIMO_ESTADO.values() if valor == "PRODUCTO_NO_EXISTE"),
        "sin_datos_stale": sum(1 for valor in ULTIMO_ESTADO.values() if valor == "SIN_DATOS_STALE"),
        "total": len(CATALOGO),
        "ts": datetime.now(CDMX_TZ).strftime("%Y-%m-%d %H:%M:%S"),
    })


# ================================
# CSV
# ================================

def guardar_skus_csv(items):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(SKUS_FILE, "w", newline="", encoding="utf-8-sig") as archivo:
        writer = csv.DictWriter(archivo, fieldnames=["sku", "url", "tu_nombre_seller", "nombre_producto", "sku_patish", "vgc"])
        writer.writeheader()
        for item in items:
            writer.writerow({
                "sku": item["sku_liverpool"], "url": item["url"],
                "tu_nombre_seller": MY_SELLER, "nombre_producto": item["producto"],
                "sku_patish": item["sku_patish"], "vgc": item.get("vgc", ""),
            })


def inicializar_csv():
    os.makedirs(DATA_DIR, exist_ok=True)
    if not os.path.exists(CSV_FILE):
        with open(CSV_FILE, "w", newline="", encoding="utf-8") as archivo:
            writer = csv.writer(archivo)
            writer.writerow(["fecha_hora", "sku_patish", "sku_liverpool", "producto", "color", "size",
                             "seller_ganador", "precio", "estado", "tipo_cambio", "url", "sellers_alternativos"])
        print("📝 CSV historico inicializado")


def guardar_evento_csv(fecha_hora, item, seller, price, estado, tipo_cambio, alternativos=None):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(CSV_FILE, "a", newline="", encoding="utf-8") as archivo:
        writer = csv.writer(archivo)
        writer.writerow([
            fecha_hora, item["sku_patish"], item["sku_liverpool"], item["producto"],
            item.get("color", ""), item.get("size", ""), seller, price, estado, tipo_cambio,
            item["url"], json.dumps(alternativos or [], ensure_ascii=False),
        ])


def rotar_csv():
    """Elimina registros del CSV mas antiguos que CSV_MAX_DIAS dias."""
    if not os.path.exists(CSV_FILE):
        return
    try:
        limite = datetime.now(CDMX_TZ) - timedelta(days=CSV_MAX_DIAS)
        filas_ok = []
        encabezado = None
        with open(CSV_FILE, newline="", encoding="utf-8") as f:
            for i, row in enumerate(csv.reader(f)):
                if i == 0:
                    encabezado = row
                    continue
                if not row:
                    continue
                try:
                    fecha = datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S").replace(tzinfo=CDMX_TZ)
                    if fecha >= limite:
                        filas_ok.append(row)
                except Exception:
                    filas_ok.append(row)
        if encabezado:
            with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(encabezado)
                writer.writerows(filas_ok)
            print(f"🗑️ CSV rotado: {len(filas_ok)} registros conservados (ultimos {CSV_MAX_DIAS} dias)")
    except Exception as exc:
        print(f"⚠️ Error rotando CSV: {exc}")


# ================================
# ESTADO PERSISTIDO
# ================================

def guardar_estado_persistido():
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(ESTADO_FILE, "w", encoding="utf-8") as f:
            json.dump({
                "ULTIMO_ESTADO": ULTIMO_ESTADO,
                "ULTIMO_PRECIO": ULTIMO_PRECIO,
                "ULTIMO_SELLER": ULTIMO_SELLER,
                "ULTIMO_PRECIO_PATISH": ULTIMO_PRECIO_PATISH,
                "ULTIMO_STOCK_PATISH": ULTIMO_STOCK_PATISH,
                "ULTIMO_LAST_CHECKED": ULTIMO_LAST_CHECKED,
                "ULTIMO_SOURCE": ULTIMO_SOURCE,
                "ULTIMO_STATUS_CODE": ULTIMO_STATUS_CODE,
                "ULTIMO_ERROR_MESSAGE": ULTIMO_ERROR_MESSAGE,
                "ULTIMO_CONFIDENCE": ULTIMO_CONFIDENCE,
                "ULTIMO_STOCK_GANADOR": ULTIMO_STOCK_GANADOR,
                "ULTIMO_SEGUNDO_SELLER": ULTIMO_SEGUNDO_SELLER,
                "ULTIMO_SEGUNDO_PRECIO": ULTIMO_SEGUNDO_PRECIO,
                "ULTIMO_REPRICE_SUGERIDO": ULTIMO_REPRICE_SUGERIDO,
                "ULTIMO_REPRICE_MOTIVO": ULTIMO_REPRICE_MOTIVO,
                "RESUMEN_VGC": RESUMEN_VGC,
            }, f, ensure_ascii=False)
    except Exception as exc:
        print(f"⚠️ No se pudo guardar estado: {exc}")


def cargar_estado_persistido_monitor():
    if not os.path.exists(ESTADO_FILE):
        return False
    try:
        with open(ESTADO_FILE, encoding="utf-8") as f:
            estado = json.load(f)
        ULTIMO_ESTADO.update({
            sku: normalizar_estado_persistido(valor)
            for sku, valor in estado.get("ULTIMO_ESTADO", {}).items()
        })
        ULTIMO_PRECIO.update(estado.get("ULTIMO_PRECIO", {}))
        ULTIMO_SELLER.update(estado.get("ULTIMO_SELLER", {}))
        ULTIMO_PRECIO_PATISH.update(estado.get("ULTIMO_PRECIO_PATISH", {}))
        ULTIMO_STOCK_PATISH.update(estado.get("ULTIMO_STOCK_PATISH", {}))
        ULTIMO_LAST_CHECKED.update(estado.get("ULTIMO_LAST_CHECKED", {}))
        ULTIMO_SOURCE.update(estado.get("ULTIMO_SOURCE", {}))
        ULTIMO_STATUS_CODE.update(estado.get("ULTIMO_STATUS_CODE", {}))
        ULTIMO_ERROR_MESSAGE.update(estado.get("ULTIMO_ERROR_MESSAGE", {}))
        ULTIMO_CONFIDENCE.update(estado.get("ULTIMO_CONFIDENCE", {}))
        ULTIMO_STOCK_GANADOR.update(estado.get("ULTIMO_STOCK_GANADOR", {}))
        ULTIMO_SEGUNDO_SELLER.update(estado.get("ULTIMO_SEGUNDO_SELLER", {}))
        ULTIMO_SEGUNDO_PRECIO.update(estado.get("ULTIMO_SEGUNDO_PRECIO", {}))
        ULTIMO_REPRICE_SUGERIDO.update(estado.get("ULTIMO_REPRICE_SUGERIDO", {}))
        ULTIMO_REPRICE_MOTIVO.update(estado.get("ULTIMO_REPRICE_MOTIVO", {}))
        RESUMEN_VGC.update(estado.get("RESUMEN_VGC", {}))
        print(f"💾 Estado previo cargado: {len(ULTIMO_ESTADO)} SKUs (sin alertas falsas al reiniciar)")
        return True
    except Exception as exc:
        print(f"⚠️ No se pudo cargar estado previo: {exc}")
        return False


# ================================
# HTTP (con retry)
# ================================

def obtener_html(url, reintentos=3):
    for intento in range(reintentos):
        try:
            respuesta = requests.get(url, headers=HEADERS, timeout=20)
            if respuesta.status_code == 200:
                return respuesta.text
            if respuesta.status_code in (429, 503) and intento < reintentos - 1:
                espera = 2 ** intento
                print(f"  HTTP {respuesta.status_code} — reintentando en {espera}s: {url[:60]}")
                time.sleep(espera)
                continue
            print(f"  HTTP {respuesta.status_code}: {url[:70]}")
            return None
        except requests.exceptions.Timeout:
            if intento < reintentos - 1:
                time.sleep(2 ** intento)
                continue
            print(f"  Timeout: {url[:70]}")
            return None
        except Exception as exc:
            print(f"  Error HTTP: {exc}")
            return None
    return None


# ================================
# SCRAPING
# ================================

def extraer_next_data(html_text):
    if not html_text:
        return None
    match = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html_text, re.DOTALL)
    if not match:
        return None
    try:
        return json.loads(match.group(1))
    except Exception:
        return None


def find_deep(objeto, llave, resultados=None):
    if resultados is None:
        resultados = []
    if not isinstance(objeto, (dict, list)):
        return resultados
    if isinstance(objeto, dict):
        if llave in objeto:
            resultados.append(objeto[llave])
        for valor in objeto.values():
            find_deep(valor, llave, resultados)
    else:
        for valor in objeto:
            find_deep(valor, llave, resultados)
    return resultados


def extraer_variantes(data):
    if not data:
        return []
    variants_blocks = find_deep(data, "variants")
    if not variants_blocks:
        return []
    biggest = sorted(variants_blocks, key=lambda block: len(block) if isinstance(block, list) else 0, reverse=True)[0]
    if not isinstance(biggest, list):
        return []
    resultado = []
    for variante in biggest:
        if not isinstance(variante, dict):
            continue
        offers_obj = variante.get("offers", {})
        prices_obj = variante.get("prices", {})
        offers_arr = []
        best_offer = None
        if isinstance(offers_obj, dict):
            offers_arr = offers_obj.get("offers", [])
            if not isinstance(offers_arr, list):
                offers_arr = []
            best_offer = offers_obj.get("bestOffer")
        ofertas_resumidas = [resumir_oferta(oferta) for oferta in offers_arr if isinstance(oferta, dict)]
        ganador = None
        if isinstance(best_offer, dict):
            ganador = resumir_oferta(best_offer)
        elif ofertas_resumidas:
            ganador = ofertas_resumidas[0]
        otros = []
        for oferta in ofertas_resumidas:
            if not oferta.get("seller"):
                continue
            if ganador and oferta.get("seller") == ganador.get("seller") and oferta.get("sellerId") == ganador.get("sellerId"):
                continue
            otros.append({"seller": oferta.get("seller", ""), "precio": oferta.get("precio", "")})
            if len(otros) >= 4:
                break
        resultado.append({
            "skuId": normalizar_identificador(variante.get("skuId")),
            "color": limpiar_texto(variante.get("color")),
            "size": limpiar_texto(variante.get("size")),
            "hasValidOnlineInventory": str(variante.get("hasValidOnlineInventory", "false")).lower(),
            "sellersCount": variante.get("sellersCount", 0),
            "precio_liverpool": obtener_precio_actual(prices_obj),
            "stock_liverpool": obtener_stock_actual(variante.get("inventory", {})),
            "buybox": ganador,
            "offers": ofertas_resumidas,
            "otros_sellers": otros,
        })
    return resultado


def extraer_buybox_legacy(html_text):
    if not html_text:
        return None, None, []
    seller = None
    price = None
    patrones = [
        r'"bestOffer"\s*:\s*\{[^{}]*?"salePrice"\s*:\s*"?(\d+(?:\.\d+)?)"?[^{}]*?"sellerName"\s*:\s*"([^"]+)"',
        r'"bestOffer"\s*:\s*\{[^{}]*?"sellerName"\s*:\s*"([^"]+)"[^{}]*?"salePrice"\s*:\s*"?(\d+(?:\.\d+)?)"?',
    ]
    for patron in patrones:
        match = re.search(patron, html_text, re.DOTALL)
        if not match:
            continue
        if "salePrice" in patron.split("sellerName")[0]:
            price = match.group(1)
            seller = match.group(2)
        else:
            seller = match.group(1)
            price = match.group(2)
        break
    if not seller:
        return None, None, []
    alternativos = []
    vistos = {seller}
    for match in re.finditer(
        r'"sellerName"\s*:\s*"([^"]+)"[^}]{0,200}?"salePrice"\s*:\s*"?(\d+(?:\.\d+)?)"?', html_text
    ):
        seller_alt = match.group(1)
        if seller_alt in vistos:
            continue
        vistos.add(seller_alt)
        alternativos.append({"seller": seller_alt, "precio": match.group(2)})
        if len(alternativos) >= 4:
            break
    return seller, price, alternativos


# ================================
# TELEGRAM
# ================================

def enviar_telegram(mensaje):
    if DISABLE_TELEGRAM:
        return
    if not TELEGRAM_TOKEN or not CHAT_ID:
        print("⚠️ Falta TELEGRAM_TOKEN o CHAT_ID")
        return
    try:
        respuesta = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": CHAT_ID, "text": mensaje, "parse_mode": "HTML"},
            timeout=15,
        )
        if not respuesta.ok:
            print(f"Telegram {respuesta.status_code}: {respuesta.text[:200]}")
    except Exception as exc:
        print(f"Error Telegram: {exc}")


def enviar_telegram_a(chat_id, mensaje):
    if DISABLE_TELEGRAM:
        return
    if not TELEGRAM_TOKEN:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": chat_id, "text": mensaje, "parse_mode": "HTML"},
            timeout=15,
        )
    except Exception as exc:
        print(f"Error Telegram: {exc}")


def enviar_csv_telegram():
    if DISABLE_TELEGRAM:
        return
    if not TELEGRAM_TOKEN or not CHAT_ID or not os.path.exists(CSV_FILE):
        return
    try:
        with open(CSV_FILE, "rb") as archivo:
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendDocument",
                data={"chat_id": CHAT_ID, "caption": "📊 Historico BuyBox diario"},
                files={"document": (CSV_FILE, archivo, "text/csv")},
                timeout=30,
            )
        print("📎 CSV enviado")
    except Exception as exc:
        print(f"Error CSV: {exc}")


# ================================
# COMANDOS TELEGRAM
# ================================

def procesar_comandos():
    global ULTIMO_UPDATE_ID
    if DISABLE_TELEGRAM:
        return
    if not TELEGRAM_TOKEN:
        return
    try:
        respuesta = requests.get(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates",
            params={"offset": ULTIMO_UPDATE_ID + 1, "timeout": 2},
            timeout=10,
        )
        if not respuesta.ok:
            return
        for update in respuesta.json().get("result", []):
            ULTIMO_UPDATE_ID = update["update_id"]
            mensaje = update.get("message", {})
            texto = mensaje.get("text", "").strip()
            chat_id = str(mensaje.get("chat", {}).get("id", ""))
            if not texto.startswith("/"):
                continue
            comando = texto.split()[0].lower()
            if comando == "/reporte":
                enviar_telegram_a(chat_id, generar_reporte_historico())
            elif comando == "/estado":
                enviar_telegram_a(chat_id, generar_estado_actual())
            elif comando == "/bloqueadas":
                enviar_telegram_a(chat_id, generar_reporte_bloqueadas())
            elif comando == "/ayuda":
                enviar_telegram_a(chat_id, "📋 <b>Comandos disponibles</b>\n\n/estado - Resumen en tiempo real\n/bloqueadas - Ofertas bloqueadas por Liverpool\n/reporte - Analisis de patrones\n/ayuda - Ayuda")
    except Exception as exc:
        print(f"Error comandos: {exc}")


def generar_estado_actual():
    if not CATALOGO:
        return "⚠️ Catalogo vacio. Sube el Excel de Liverpool desde el panel web."
    ganando = [item for item in CATALOGO if es_estado_ganador_verificado(ULTIMO_ESTADO.get(item["sku_patish"]))]
    perdiendo = [item for item in CATALOGO if ULTIMO_ESTADO.get(item["sku_patish"]) == "PERDIDO"]
    no_prendidas = [item for item in CATALOGO if ULTIMO_ESTADO.get(item["sku_patish"]) == "NO_PRENDIDA"]
    inconsistentes = [item for item in CATALOGO if ULTIMO_ESTADO.get(item["sku_patish"]) == "GANANDO_API_NO_VISIBLE"]
    invalidos = [item for item in CATALOGO if ULTIMO_ESTADO.get(item["sku_patish"]) in ESTADOS_INVALIDOS]
    bloqueadas = [item for item in CATALOGO if item["estado_oferta"] == "BLOQUEADA"]
    now_str = datetime.now(CDMX_TZ).strftime("%H:%M:%S")
    lineas = [
        f"📡 <b>ESTADO ACTUAL</b> - {now_str}", "",
        f"🟢 Ganando: {len(ganando)}", f"🔴 Perdiendo: {len(perdiendo)}",
        f"🟡 No prendida: {len(no_prendidas)}", f"⚠️ Inconsistentes: {len(inconsistentes)}",
        f"⛔ Invalidos/no existe: {len(invalidos)}", f"🟠 Bloqueadas Liverpool: {len(bloqueadas)}",
        f"📦 Total catalogo: {len(CATALOGO)}",
    ]
    if ganando:
        lineas += ["", "🟢 <b>GANANDO</b>"]
        for item in ganando[:10]:
            variante = " ".join(p for p in [item.get("color", ""), item.get("size", "")] if p).strip()
            sufijo = f" {escapar(variante)}" if variante else ""
            lineas.append(f"• {escapar(item['producto'][:22])}{sufijo} → ${escapar(ULTIMO_PRECIO.get(item['sku_patish'], '?'))}")
    if perdiendo:
        lineas += ["", "🔴 <b>PERDIENDO</b>"]
        for item in perdiendo[:10]:
            variante = " ".join(p for p in [item.get("color", ""), item.get("size", "")] if p).strip()
            sufijo = f" {escapar(variante)}" if variante else ""
            lineas.append(f"• {escapar(item['producto'][:18])}{sufijo} → {escapar(ULTIMO_SELLER.get(item['sku_patish'], '?'))} → ${escapar(ULTIMO_PRECIO.get(item['sku_patish'], '?'))}")
    if no_prendidas:
        lineas += ["", "🟡 <b>NO PRENDIDA</b>"]
        for item in no_prendidas[:10]:
            variante = " ".join(p for p in [item.get("color", ""), item.get("size", "")] if p).strip()
            sufijo = f" {escapar(variante)}" if variante else ""
            lineas.append(f"• {escapar(item['producto'][:18])}{sufijo} → {escapar(ULTIMO_SELLER.get(item['sku_patish'], '?'))} → ${escapar(ULTIMO_PRECIO.get(item['sku_patish'], '?'))}")
    return "\n".join(lineas)


def generar_reporte_bloqueadas():
    bloqueadas = [item for item in CATALOGO if item["estado_oferta"] == "BLOQUEADA"]
    if not bloqueadas:
        return "✅ No hay ofertas bloqueadas por Liverpool."
    lineas = [f"🟠 <b>BLOQUEADAS POR LIVERPOOL</b> ({len(bloqueadas)})", ""]
    for item in bloqueadas[:20]:
        lineas.append(f"• {escapar(item['producto'][:35])}\n  SKU: {escapar(item['sku_patish'])} | {escapar(item.get('motivo', '?'))}")
    if len(bloqueadas) > 20:
        lineas += ["", f"...y {len(bloqueadas) - 20} mas."]
    return "\n".join(lineas)


def generar_reporte_historico():
    if not os.path.exists(CSV_FILE):
        return "⚠️ No hay historico todavia."
    sellers_count = defaultdict(int)
    perdidas_por_hora = defaultdict(int)
    total_cambios = 0
    total_perdidas = 0
    try:
        with open(CSV_FILE, newline="", encoding="utf-8") as archivo:
            for row in csv.DictReader(archivo):
                tipo = row.get("tipo_cambio", "")
                total_cambios += 1
                if "CAMBIO_ESTADO" in tipo and row.get("estado") == "PERDIDO":
                    total_perdidas += 1
                    sellers_count[row.get("seller_ganador", "")] += 1
                    try:
                        perdidas_por_hora[int(row.get("fecha_hora", "")[11:13])] += 1
                    except Exception:
                        pass
        if not total_cambios:
            return "📊 Historico vacio aun."
        top = sorted(sellers_count.items(), key=lambda item: item[1], reverse=True)[:5]
        hora_pico = max(perdidas_por_hora, key=perdidas_por_hora.get) if perdidas_por_hora else None
        lineas = ["📊 <b>REPORTE DE PATRONES</b>", "", f"📝 Eventos: {total_cambios}", f"🔴 Perdidas de BuyBox: {total_perdidas}"]
        if top:
            lineas += ["", "🏆 <b>Sellers que mas te ganan:</b>"]
            for seller, conteo in top:
                sufijo = "es" if conteo > 1 else ""
                lineas.append(f"• {escapar(seller)} → {conteo} vez{sufijo}")
        if hora_pico is not None:
            lineas += ["", f"⏰ <b>Hora pico:</b> {hora_pico:02d}:00 hrs"]
        return "\n".join(lineas)
    except Exception as exc:
        return f"⚠️ Error: {escapar(exc)}"


# ================================
# ALERTAS
# ================================

def construir_alerta_perdida(item, resultado):
    sku_patish = item["sku_patish"]
    seller = resultado.get("seller", "")
    price = resultado.get("price", "")
    precio_mio = resultado.get("precio_mio", "")
    stock_mio = resultado.get("stock_mio", "")
    reprice_sugerido, reprice_motivo = calcular_reprice_sugerido(
        resultado.get("nuevo_estado", ""), price, precio_mio, stock_mio
    )
    ventas_info = ventas_por_sku(30).get(sku_base_desde_patish(sku_patish), {})
    precio_minimo = PRECIOS_MINIMOS.get(sku_patish)
    item_accion = {
        "estado": resultado.get("nuevo_estado", ""),
        "reprice_sugerido": reprice_sugerido,
        "precio_minimo": formatear_precio(precio_minimo) if precio_minimo is not None else "",
        "ventas_30d_piezas": ventas_info.get("piezas", 0),
    }
    return {
        "sku_patish": sku_patish,
        "sku_liverpool": item.get("sku_liverpool", ""),
        "producto": item.get("producto", ""),
        "seller": seller,
        "precio_ganador": price,
        "precio_mio": precio_mio,
        "stock_mio": stock_mio,
        "reprice_sugerido": reprice_sugerido,
        "reprice_motivo": reprice_motivo,
        "accion": accion_recomendada_item(item_accion),
        "ventas_30d": ventas_info.get("piezas", 0),
        "source": resultado.get("source", ""),
        "confidence": resultado.get("confidence", ""),
        "nota": resultado.get("error_message", ""),
        "url": item.get("url", ""),
    }


def enviar_alerta_perdidas(perdidas):
    if not perdidas:
        return
    total = len(perdidas)
    lineas = [
        f"<b>PERDISTE BUYBOX</b> ({total} SKU{'s' if total != 1 else ''})",
        "",
    ]
    for perdida in perdidas[:20]:
        lineas.extend([
            f"<b>{escapar(perdida['sku_patish'])}</b> | Liverpool {escapar(perdida['sku_liverpool'])}",
            f"{escapar(perdida['producto'][:90])}",
            f"Gana: <b>{escapar(perdida['seller'] or '-')}</b> a {escapar(formatear_money(perdida['precio_ganador']))}",
            f"Tu precio: {escapar(formatear_money(perdida['precio_mio']))} | Stock: {escapar(perdida['stock_mio'] if perdida['stock_mio'] not in (None, '') else '-')}",
            f"Ventas 30d: {escapar(formatear_precio(perdida['ventas_30d']) or '0')}",
        ])
        if perdida.get("reprice_sugerido"):
            lineas.append(f"Sugerido: {escapar(formatear_money(perdida['reprice_sugerido']))}")
        if perdida.get("accion"):
            lineas.append(f"Accion: {escapar(perdida['accion'])}")
        fuente = perdida.get("source") or "-"
        confianza = perdida.get("confidence") or "-"
        lineas.append(f"Fuente: {escapar(fuente)} | {escapar(confianza)}")
        if perdida.get("nota"):
            lineas.append(f"Nota: {escapar(perdida['nota'][:160])}")
        if perdida.get("url"):
            lineas.append(escapar(perdida["url"]))
        lineas.append("")
    if total > 20:
        lineas.append(f"...y {total - 20} SKUs mas. Revisa el panel para verlos todos.")
    enviar_telegram("\n".join(lineas).strip())


def limpiar_estado_item(sku, estado):
    ULTIMO_ESTADO[sku] = estado
    ULTIMO_PRECIO[sku] = ""
    ULTIMO_SELLER[sku] = ""
    ULTIMO_PRECIO_PATISH.pop(sku, None)
    ULTIMO_STOCK_PATISH.pop(sku, None)
    ULTIMO_LAST_CHECKED[sku] = datetime.now(CDMX_TZ).strftime("%Y-%m-%d %H:%M:%S")
    ULTIMO_SOURCE[sku] = "catalogo"
    ULTIMO_STATUS_CODE[sku] = ""
    ULTIMO_ERROR_MESSAGE[sku] = ""
    ULTIMO_CONFIDENCE[sku] = "catalogo"
    ULTIMO_STOCK_GANADOR.pop(sku, None)
    ULTIMO_SEGUNDO_SELLER.pop(sku, None)
    ULTIMO_SEGUNDO_PRECIO.pop(sku, None)
    ULTIMO_REPRICE_SUGERIDO.pop(sku, None)
    ULTIMO_REPRICE_MOTIVO.pop(sku, None)


# ================================
# PROCESAMIENTO PARALELO
# ================================

def _api_get(url):
    """GET autenticado con sesión de cookies. Un reintento si 403 (renueva cookies)."""
    _asegurar_cookies()
    hdrs = {
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://www.liverpool.com.mx/tienda/",
        "Origin": "https://www.liverpool.com.mx",
    }
    for intento in range(2):
        try:
            r = _SESSION.get(url, headers=hdrs, timeout=15)
            if r.status_code == 200:
                return r
            if r.status_code == 403 and intento == 0:
                global _SESSION_COOKIES_AT
                _SESSION_COOKIES_AT = 0
                _asegurar_cookies()
                time.sleep(1)
                continue
            return r
        except Exception as exc:
            if intento == 0:
                time.sleep(1)
                continue
            print(f"  Error HTTP: {exc}")
    return None


def _meta_api(source, status_code=None, error_message="", confidence=""):
    return {
        "source": source,
        "status_code": status_code or "",
        "error_message": limpiar_texto(error_message),
        "confidence": confidence,
    }


def _json_error_message(data):
    if not isinstance(data, dict):
        return ""
    return limpiar_texto(data.get("errorMessage") or data.get("message") or data.get("error"))


def _fetch_alloffers_api(product_id):
    """Llama a la API de Liverpool para obtener el mejor seller por SKU del producto."""
    url = f"https://www.liverpool.com.mx/tienda/browse/marketplace/products/allOffers?productId={product_id}"
    r = _api_get(url)
    if not r or r.status_code != 200:
        if r:
            print(f"  HTTP {r.status_code} allOffers {product_id}")
            return {}, _meta_api("allOffers", r.status_code, f"HTTP {r.status_code}")
        return {}, _meta_api("allOffers", error_message="sin respuesta")
    try:
        data = r.json()
        error_message = _json_error_message(data)
        if error_message:
            return {}, _meta_api("allOffers", r.status_code, error_message)
        result = {}
        for offer in data.get("skuOffers", []):
            sku_id = normalizar_identificador(str(offer.get("skuId", "")))
            if sku_id:
                result[sku_id] = offer
        return result, _meta_api("allOffers", r.status_code, confidence="api_ok")
    except Exception as exc:
        print(f"  Error parsing allOffers {product_id}: {exc}")
        return {}, _meta_api("allOffers", r.status_code, f"json_error: {exc}")


def _fetch_offers_listing(sku_liverpool):
    """Obtiene todos los sellers activos para un SKU (para distinguir PERDIDO vs NO_PRENDIDA)."""
    url = f"https://www.liverpool.com.mx/tienda/browse/marketplace/skus/offersListing?skuId={sku_liverpool}"
    r = _api_get(url)
    if not r or r.status_code != 200:
        if r:
            return [], _meta_api("offersListing", r.status_code, f"HTTP {r.status_code}")
        return [], _meta_api("offersListing", error_message="sin respuesta")
    try:
        data = r.json()
        error_message = _json_error_message(data)
        if error_message:
            return [], _meta_api("offersListing", r.status_code, error_message)
        return data.get("sellersOfferDetails", []), _meta_api("offersListing", r.status_code, confidence="api_ok")
    except Exception as exc:
        return [], _meta_api("offersListing", r.status_code, f"json_error: {exc}")


def _fetch_pdp_variantes(url):
    """Lee el PDP publico; es la fuente final de buybox que ve el comprador."""
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        if r.status_code == 404:
            return {}, _meta_api("PDP", 404, "PDP 404", "pdp_404")
        if r.status_code != 200:
            return {}, _meta_api("PDP", r.status_code, f"HTTP {r.status_code}")
        data = extraer_next_data(r.text)
        variantes = extraer_variantes(data)
        mapa = {
            normalizar_identificador(variante.get("skuId", "")): variante
            for variante in variantes
            if normalizar_identificador(variante.get("skuId", ""))
        }
        if mapa:
            return mapa, _meta_api("PDP", r.status_code, confidence="pdp_bestoffer")
        seller, price, otros = extraer_buybox_legacy(r.text)
        sku_match = re.search(r"[?&]skuid=(\d+)", url)
        if seller and sku_match:
            sku_id = normalizar_identificador(sku_match.group(1))
            return {
                sku_id: {
                    "skuId": sku_id,
                    "buybox": {"seller": seller, "sellerId": "", "precio": formatear_precio(price), "stock": None},
                    "offers": [{"seller": seller, "sellerId": "", "precio": formatear_precio(price), "stock": None}],
                    "otros_sellers": otros,
                    "sellersCount": len(otros) + 1,
                }
            }, _meta_api("PDP", r.status_code, confidence="pdp_legacy")
        return {}, _meta_api("PDP", r.status_code, "PDP sin variantes/bestOffer")
    except Exception as exc:
        return {}, _meta_api("PDP", error_message=f"pdp_error: {exc}")


def _resumen_sellers_pdp(variante_pdp):
    buybox = variante_pdp.get("buybox") if isinstance(variante_pdp, dict) else {}
    offers = variante_pdp.get("offers", []) if isinstance(variante_pdp, dict) else []
    if not isinstance(buybox, dict):
        buybox = {}
    if not isinstance(offers, list):
        offers = []

    seller_ganador = limpiar_texto(buybox.get("seller", ""))
    seller_id_ganador = normalizar_identificador(str(buybox.get("sellerId", "")))
    precio_ganador = formatear_precio(buybox.get("precio", ""))
    stock_ganador = buybox.get("stock")
    patish_es_primero = es_seller_mio(seller_ganador, seller_id_ganador)
    tiene_mi_oferta = False
    precio_mio = ""
    stock_mio = None
    otros = []
    segundo_seller = ""
    segundo_precio = ""

    for idx, oferta in enumerate(offers):
        if not isinstance(oferta, dict):
            continue
        s_name = limpiar_texto(oferta.get("seller", ""))
        s_id = normalizar_identificador(str(oferta.get("sellerId", "")))
        s_price = formatear_precio(oferta.get("precio", ""))
        s_stock = oferta.get("stock")
        if idx == 1:
            segundo_seller = s_name
            segundo_precio = s_price
        if es_seller_mio(s_name, s_id):
            tiene_mi_oferta = True
            precio_mio = s_price
            stock_mio = s_stock
        else:
            otros.append({"seller": s_name, "precio": s_price, "stock": s_stock})

    return {
        "seller_ganador": seller_ganador,
        "seller_id_ganador": seller_id_ganador,
        "precio_ganador": precio_ganador,
        "stock_ganador": stock_ganador,
        "segundo_seller": segundo_seller,
        "segundo_precio": segundo_precio,
        "tiene_mi_oferta": tiene_mi_oferta,
        "patish_es_primero": patish_es_primero,
        "precio_mio": precio_mio,
        "stock_mio": stock_mio,
        "otros": otros[:4],
    }


def _descripcion_conflicto_pdp_api(resumen_pdp, offer_data):
    if not isinstance(offer_data, dict):
        return ""
    api_seller = limpiar_texto(offer_data.get("bestSeller", ""))
    api_seller_id = normalizar_identificador(str(offer_data.get("sellerId", "")))
    api_price = formatear_precio(offer_data.get("bestSalePrice") or offer_data.get("bestPromoPrice") or "")
    pdp_seller = resumen_pdp.get("seller_ganador", "")
    pdp_seller_id = resumen_pdp.get("seller_id_ganador", "")
    pdp_price = resumen_pdp.get("precio_ganador", "")
    seller_conflict = False
    if api_seller_id and pdp_seller_id:
        seller_conflict = api_seller_id != pdp_seller_id
    elif api_seller and pdp_seller:
        seller_conflict = limpiar_texto(api_seller).lower() != limpiar_texto(pdp_seller).lower()
    price_conflict = bool(api_price and pdp_price and api_price != pdp_price)
    if not seller_conflict and not price_conflict:
        return ""
    return f"PDP manda: {pdp_seller or '-'} ${pdp_price or '-'}; allOffers decía: {api_seller or '-'} ${api_price or '-'}"


def _fetch_pdp_status(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code == 404:
            return _meta_api("PDP", 404, "PDP 404", "pdp_404")
        if r.status_code != 200:
            return _meta_api("PDP", r.status_code, f"HTTP {r.status_code}")
        return _meta_api("PDP", 200, "", "pdp_ok")
    except Exception as exc:
        return _meta_api("PDP", error_message=f"pdp_error: {exc}")


def _resolver_estado_invalido(item, meta_api):
    """Distingue SKU inválido de PDP inexistente cuando Liverpool ya reportó error fuerte."""
    if not es_error_sku_invalido(meta_api.get("error_message", "")):
        return None, meta_api
    pdp_meta = _fetch_pdp_status(item["url"])
    if pdp_meta.get("status_code") == 404:
        return "PRODUCTO_NO_EXISTE", pdp_meta
    return "SKU_INVALIDO", meta_api


def _resumen_sellers_listing(all_sellers):
    otros = []
    precio_mio = ""
    stock_mio = None
    tiene_mi_oferta = False
    patish_es_primero = False
    seller_ganador = ""
    seller_id_ganador = ""
    precio_ganador = ""
    stock_ganador = None
    segundo_seller = ""
    segundo_precio = ""

    if all_sellers:
        primero = all_sellers[0]
        seller_ganador = limpiar_texto(primero.get("sellerName", ""))
        seller_id_ganador = normalizar_identificador(str(primero.get("sellerId", "")))
        precio_ganador = formatear_precio(primero.get("salePrice", ""))
        stock_ganador = obtener_stock_actual(primero)
        patish_es_primero = es_seller_mio(seller_ganador, seller_id_ganador)
        if len(all_sellers) > 1:
            segundo = all_sellers[1]
            segundo_seller = limpiar_texto(segundo.get("sellerName", ""))
            segundo_precio = formatear_precio(segundo.get("salePrice", ""))

    for s in all_sellers:
        s_name = limpiar_texto(s.get("sellerName", ""))
        s_id = normalizar_identificador(str(s.get("sellerId", "")))
        s_price = formatear_precio(s.get("salePrice", ""))
        if es_seller_mio(s_name, s_id):
            tiene_mi_oferta = True
            precio_mio = s_price
            stock_mio = obtener_stock_actual(s)
        else:
            otros.append({"seller": s_name, "precio": s_price, "stock": obtener_stock_actual(s)})

    return {
        "seller_ganador": seller_ganador,
        "seller_id_ganador": seller_id_ganador,
        "precio_ganador": precio_ganador,
        "stock_ganador": stock_ganador,
        "segundo_seller": segundo_seller,
        "segundo_precio": segundo_precio,
        "tiene_mi_oferta": tiene_mi_oferta,
        "patish_es_primero": patish_es_primero,
        "precio_mio": precio_mio,
        "stock_mio": stock_mio,
        "otros": otros[:4],
    }


def _precio_numero_oferta(oferta):
    return normalizar_precio(oferta.get("bestSalePrice") or oferta.get("bestPromoPrice") or oferta.get("salePrice"))


def calcular_reprice_sugerido(nuevo_estado, precio_ganador, precio_mio, stock_mio):
    if nuevo_estado != "PERDIDO":
        return "", ""
    ganador = normalizar_precio(precio_ganador)
    mio = normalizar_precio(precio_mio)
    stock = normalizar_entero(stock_mio)
    if ganador is None:
        return "", "sin precio ganador"
    if mio is None:
        return "", "sin precio tuyo"
    if stock is not None and stock <= 0:
        return "", "sin stock tuyo"
    sugerido = max(0, ganador - REPRICER_STEP)
    if sugerido >= mio:
        return "", "ya estás igual o más barato; revisar razón de pérdida"
    return formatear_precio(sugerido), f"Simulación: bajar ${mio - sugerido:.0f} para quedar ${REPRICER_STEP:.0f} abajo"


def _resumen_vgc(product_id, items_grupo, alloffers_mapa):
    items_catalogo = [
        item for item in CATALOGO
        if normalizar_identificador(item.get("product_id", "")) == product_id
        or normalizar_identificador(item.get("vgc", "")) == product_id
    ] or items_grupo
    skus_mios = {normalizar_identificador(item.get("sku_liverpool", "")) for item in items_catalogo}
    items_por_sku = {normalizar_identificador(item.get("sku_liverpool", "")): item for item in items_catalogo}
    ofertas = []
    for sku_id, oferta in alloffers_mapa.items():
        if not isinstance(oferta, dict):
            continue
        precio = _precio_numero_oferta(oferta)
        ofertas.append({
            "sku_liverpool": sku_id,
            "seller": limpiar_texto(oferta.get("bestSeller", "")),
            "seller_id": normalizar_identificador(str(oferta.get("sellerId", ""))),
            "precio": precio,
            "precio_texto": formatear_precio(precio) if precio is not None else "",
            "stock": obtener_stock_actual(oferta),
            "sellers_count": oferta.get("sellersCount", 0),
            "es_mio": sku_id in skus_mios,
        })

    if not ofertas:
        return {
            "product_id": product_id,
            "alerta": False,
            "total_variantes_liverpool": 0,
            "variantes_mias": len(items_catalogo),
            "variantes_faltantes": 0,
        }

    faltantes = [oferta for oferta in ofertas if not oferta["es_mio"]]
    mias = [oferta for oferta in ofertas if oferta["es_mio"]]
    mias_detalle = []
    for sku_id in sorted(skus_mios):
        item = items_por_sku.get(sku_id, {})
        oferta_publica = next((oferta for oferta in mias if oferta["sku_liverpool"] == sku_id), None)
        precio_publico = oferta_publica.get("precio") if oferta_publica else None
        mias_detalle.append({
            "sku_liverpool": sku_id,
            "sku_patish": limpiar_texto(item.get("sku_patish", "")),
            "estado_oferta": limpiar_texto(item.get("estado_oferta", "")),
            "stock": normalizar_entero(item.get("cantidad")),
            "seller": oferta_publica.get("seller", "") if oferta_publica else "",
            "precio": precio_publico,
            "precio_texto": formatear_precio(precio_publico) if precio_publico is not None else "",
        })
    faltantes_ordenadas = sorted(
        faltantes,
        key=lambda oferta: oferta["precio"] if oferta["precio"] is not None else float("inf"),
    )
    mias_con_precio = [oferta for oferta in mias if oferta["precio"] is not None]
    precio_minimo_mio = min((oferta["precio"] for oferta in mias_con_precio), default=None)
    faltante_barata = faltantes_ordenadas[0] if faltantes_ordenadas else None

    detalle = [
        f"VGC {product_id}",
        f"Cobertura: {len(mias)}/{len(ofertas)} variantes",
        "",
    ]
    if mias_detalle:
        detalle.append("Tus variantes:")
        for oferta in mias_detalle:
            precio = f"${oferta['precio_texto']}" if oferta["precio_texto"] else "-"
            detalle.append(f"- {oferta['sku_liverpool']} · {oferta['sku_patish']} · {oferta['estado_oferta']} · {oferta['seller'] or '-'} · {precio}")
        detalle.append("")
    if faltantes_ordenadas:
        detalle.append("Variantes que no tienes:")
        for oferta in faltantes_ordenadas[:12]:
            precio = f"${oferta['precio_texto']}" if oferta["precio_texto"] else "-"
            detalle.append(f"- {oferta['sku_liverpool']} · {oferta['seller'] or '-'} · {precio}")
        if len(faltantes_ordenadas) > 12:
            detalle.append(f"...y {len(faltantes_ordenadas) - 12} más")

    diferencia = None
    if faltante_barata and faltante_barata["precio"] is not None and precio_minimo_mio is not None:
        diferencia = faltante_barata["precio"] - precio_minimo_mio

    alerta = bool(faltantes)
    alerta_texto = f"+{len(faltantes)} variantes" if alerta else ""
    if faltante_barata and faltante_barata["precio_texto"]:
        alerta_texto += f" · desde ${faltante_barata['precio_texto']}"

    return {
        "product_id": product_id,
        "alerta": alerta,
        "alerta_texto": alerta_texto,
        "detalle_texto": "\n".join(detalle),
        "total_variantes_liverpool": len(ofertas),
        "variantes_mias": len(items_catalogo),
        "variantes_faltantes": len(faltantes),
        "precio_minimo_mio": precio_minimo_mio,
        "sku_variante_faltante_mas_barata": faltante_barata["sku_liverpool"] if faltante_barata else "",
        "seller_variante_faltante_mas_barata": faltante_barata["seller"] if faltante_barata else "",
        "precio_variante_faltante_mas_barata": faltante_barata["precio"] if faltante_barata else None,
        "diferencia_vs_mi_precio": diferencia,
        "mias_detalle": mias_detalle,
        "faltantes": faltantes_ordenadas[:20],
    }


def _procesar_grupo_producto(product_id, items_grupo):
    """Obtiene datos de buybox via API y procesa cada variante. Thread-safe."""
    alloffers_mapa, alloffers_meta = _fetch_alloffers_api(product_id)
    pdp_mapa, pdp_meta = _fetch_pdp_variantes(items_grupo[0]["url"]) if items_grupo else ({}, _meta_api("PDP", error_message="sin items"))
    RESUMEN_VGC[product_id] = _resumen_vgc(product_id, items_grupo, alloffers_mapa)

    resultados = []
    for item in items_grupo:
        sku_liverpool = normalizar_identificador(item["sku_liverpool"])
        offer_data = alloffers_mapa.get(sku_liverpool)
        pdp_variante = pdp_mapa.get(sku_liverpool)
        base_result = {
            "item": item, "sku_patish": item["sku_patish"],
            "otros": [], "precio_mio": "", "stock_mio": None,
            "stock_ganador": None, "segundo_seller": "", "segundo_precio": "",
            "color_nuevo": "", "size_nuevo": "", "url_nuevo": None,
            "source": alloffers_meta.get("source", "allOffers"),
            "status_code": alloffers_meta.get("status_code", ""),
            "error_message": alloffers_meta.get("error_message", ""),
            "confidence": alloffers_meta.get("confidence", ""),
        }

        if pdp_variante:
            resumen_pdp = _resumen_sellers_pdp(pdp_variante)
            conflicto_pdp_api = _descripcion_conflicto_pdp_api(resumen_pdp, offer_data)
            if resumen_pdp["patish_es_primero"]:
                nuevo_estado = "GANANDO_VERIFICADO"
            elif resumen_pdp["tiene_mi_oferta"]:
                nuevo_estado = "PERDIDO"
            elif pdp_variante.get("sellersCount", 0) or resumen_pdp["seller_ganador"]:
                nuevo_estado = "NO_PRENDIDA"
            else:
                nuevo_estado = "SIN_DATOS"
            resultados.append({
                **base_result,
                "nuevo_estado": nuevo_estado,
                "seller": resumen_pdp["seller_ganador"],
                "price": resumen_pdp["precio_ganador"],
                "otros": resumen_pdp["otros"],
                "precio_mio": resumen_pdp["precio_mio"],
                "stock_mio": resumen_pdp["stock_mio"],
                "stock_ganador": resumen_pdp["stock_ganador"],
                "segundo_seller": resumen_pdp["segundo_seller"],
                "segundo_precio": resumen_pdp["segundo_precio"],
                "source": "PDP",
                "status_code": pdp_meta.get("status_code", ""),
                "error_message": conflicto_pdp_api or pdp_meta.get("error_message", ""),
                "confidence": "pdp_conflict_api" if conflicto_pdp_api else pdp_meta.get("confidence", "pdp_bestoffer"),
            })
            continue

        if pdp_meta.get("status_code") == 404:
            resultados.append({
                **base_result,
                "nuevo_estado": "PRODUCTO_NO_EXISTE",
                "seller": "",
                "price": "",
                "source": pdp_meta.get("source", ""),
                "status_code": pdp_meta.get("status_code", ""),
                "error_message": pdp_meta.get("error_message", ""),
                "confidence": "pdp_404",
            })
            continue

        if not offer_data:
            all_sellers, listing_meta = _fetch_offers_listing(sku_liverpool)
            estado_invalido, meta_invalida = _resolver_estado_invalido(item, listing_meta if listing_meta.get("error_message") else alloffers_meta)
            if estado_invalido:
                resultados.append({
                    **base_result,
                    "nuevo_estado": estado_invalido,
                    "seller": "",
                    "price": "",
                    "source": meta_invalida.get("source", ""),
                    "status_code": meta_invalida.get("status_code", ""),
                    "error_message": meta_invalida.get("error_message", ""),
                    "confidence": "invalidado",
                })
                continue
            if all_sellers:
                resumen_listing = _resumen_sellers_listing(all_sellers)
                if resumen_listing["patish_es_primero"]:
                    nuevo_estado = "GANANDO_VERIFICADO"
                elif resumen_listing["tiene_mi_oferta"]:
                    nuevo_estado = "PERDIDO"
                else:
                    nuevo_estado = "NO_PRENDIDA"
                resultados.append({
                    **base_result,
                    "nuevo_estado": nuevo_estado,
                    "seller": resumen_listing["seller_ganador"],
                    "price": resumen_listing["precio_ganador"],
                    "otros": resumen_listing["otros"],
                    "precio_mio": resumen_listing["precio_mio"],
                    "stock_mio": resumen_listing["stock_mio"],
                    "stock_ganador": resumen_listing["stock_ganador"],
                    "segundo_seller": resumen_listing["segundo_seller"],
                    "segundo_precio": resumen_listing["segundo_precio"],
                    "source": "offersListing",
                    "status_code": listing_meta.get("status_code", ""),
                    "error_message": listing_meta.get("error_message", ""),
                    "confidence": "verified_listing",
                })
                continue
            pdp_meta = _fetch_pdp_status(item["url"])
            if pdp_meta.get("status_code") == 404:
                resultados.append({
                    **base_result,
                    "nuevo_estado": "PRODUCTO_NO_EXISTE",
                    "seller": "",
                    "price": "",
                    "source": pdp_meta.get("source", ""),
                    "status_code": pdp_meta.get("status_code", ""),
                    "error_message": pdp_meta.get("error_message", ""),
                    "confidence": "pdp_404",
                })
                continue
            resultados.append({
                **base_result,
                "nuevo_estado": "SIN_DATOS", "seller": "", "price": "",
            })
            continue

        best_seller = limpiar_texto(offer_data.get("bestSeller", ""))
        seller_id = normalizar_identificador(str(offer_data.get("sellerId", "")))
        price = formatear_precio(offer_data.get("bestSalePrice", ""))
        sellers_count = offer_data.get("sellersCount", 0)
        es_mio = es_seller_mio(best_seller, seller_id)
        all_sellers, listing_meta = _fetch_offers_listing(sku_liverpool)
        estado_invalido, meta_invalida = _resolver_estado_invalido(item, listing_meta)
        if estado_invalido:
            resultados.append({
                **base_result,
                "nuevo_estado": estado_invalido,
                "seller": "",
                "price": "",
                "source": meta_invalida.get("source", ""),
                "status_code": meta_invalida.get("status_code", ""),
                "error_message": meta_invalida.get("error_message", ""),
                "confidence": "invalidado",
            })
            continue
        resumen_listing = _resumen_sellers_listing(all_sellers)

        if es_mio:
            if resumen_listing["patish_es_primero"]:
                nuevo_estado = "GANANDO_VERIFICADO"
                price = resumen_listing["precio_ganador"] or price
                best_seller = resumen_listing["seller_ganador"] or best_seller
                precio_mio = resumen_listing["precio_mio"] or price
                stock_mio = resumen_listing["stock_mio"]
                otros = resumen_listing["otros"]
                confidence = "verified_listing"
            else:
                nuevo_estado = "GANANDO_API_NO_VISIBLE"
                precio_mio = resumen_listing["precio_mio"] or price
                stock_mio = resumen_listing["stock_mio"]
                otros = resumen_listing["otros"]
                confidence = "alloffers_only"
        else:
            otros = resumen_listing["otros"]
            precio_mio = resumen_listing["precio_mio"]
            stock_mio = resumen_listing["stock_mio"]
            if resumen_listing["seller_ganador"]:
                best_seller = resumen_listing["seller_ganador"]
                price = resumen_listing["precio_ganador"]
            if resumen_listing["tiene_mi_oferta"]:
                nuevo_estado = "PERDIDO"
            elif sellers_count > 0:
                nuevo_estado = "NO_PRENDIDA"
            else:
                nuevo_estado = "PERDIDO"
            confidence = "verified_listing" if all_sellers else "alloffers_only"

        resultados.append({
            **base_result,
            "nuevo_estado": nuevo_estado, "seller": best_seller, "price": price,
            "otros": otros[:4], "precio_mio": precio_mio, "stock_mio": stock_mio,
            "stock_ganador": resumen_listing.get("stock_ganador"),
            "segundo_seller": resumen_listing.get("segundo_seller", ""),
            "segundo_precio": resumen_listing.get("segundo_precio", ""),
            "color_nuevo": "", "size_nuevo": "", "url_nuevo": None,
            "source": "offersListing" if all_sellers else "allOffers",
            "status_code": listing_meta.get("status_code", alloffers_meta.get("status_code", "")),
            "error_message": listing_meta.get("error_message", ""),
            "confidence": confidence,
        })

    return resultados


# ================================
# MONITOR PRINCIPAL
# ================================

def monitorear():
    global ULTIMO_RESUMEN, ULTIMA_FECHA_CSV

    ganando = []
    inconsistentes = []
    invalidos = []
    stale = []
    perdiendo = []
    no_prendidas = []
    perdidas_alerta = []
    now_cdmx = datetime.now(CDMX_TZ)
    now_str = now_cdmx.strftime("%H:%M:%S")
    fecha_hora = now_cdmx.strftime("%Y-%m-%d %H:%M:%S")

    if not CATALOGO:
        print(f"[{now_str}] ⚠️ Catalogo vacio - sube el Excel desde el panel web")
        return

    # Marcar items no activos
    for item in CATALOGO:
        sku = item["sku_patish"]
        if item["estado_oferta"] == "BLOQUEADA":
            limpiar_estado_item(sku, "BLOQUEADA")
        elif item["estado_oferta"] == "INACTIVA_STOCK":
            limpiar_estado_item(sku, "INACTIVA_STOCK")

    # Agrupar activos por product_id
    productos_agrupados = defaultdict(list)
    for item in CATALOGO:
        if item["estado_oferta"] == "ACTIVA":
            productos_agrupados[item["product_id"]].append(item)

    # Procesar en paralelo con pausa entre lotes para no saturar el API
    LOTE_SIZE = MAX_WORKERS
    PAUSA_ENTRE_LOTES = float(os.getenv("PAUSA_LOTES", "1.0"))
    grupos_lista = list(productos_agrupados.items())
    todos_resultados = []
    for i in range(0, len(grupos_lista), LOTE_SIZE):
        lote = grupos_lista[i:i + LOTE_SIZE]
        with ThreadPoolExecutor(max_workers=len(lote)) as executor:
            futures = {
                executor.submit(_procesar_grupo_producto, pid, grupo): pid
                for pid, grupo in lote
            }
            for future in as_completed(futures):
                pid = futures[future]
                try:
                    todos_resultados.extend(future.result())
                except Exception as exc:
                    print(f"  💥 Error procesando {pid}: {exc}")
        if i + LOTE_SIZE < len(grupos_lista):
            time.sleep(PAUSA_ENTRE_LOTES)

    # Aplicar resultados secuencialmente
    for r in todos_resultados:
        item = r["item"]
        sku_patish = r["sku_patish"]
        nuevo_estado = r["nuevo_estado"]
        seller = r["seller"]
        price = r["price"]
        otros = r["otros"]

        if r["color_nuevo"]:
            item["color"] = r["color_nuevo"]
        if r["size_nuevo"]:
            item["size"] = r["size_nuevo"]
        if r["url_nuevo"]:
            item["url"] = r["url_nuevo"]

        variante = " ".join(p for p in [item.get("color", ""), item.get("size", "")] if p).strip()

        if nuevo_estado == "GANANDO_VERIFICADO":
            ganando.append(f"• {item['producto'][:22]} {variante} → ${price}".strip())
        elif nuevo_estado == "GANANDO_API_NO_VISIBLE":
            inconsistentes.append(f"• {item['producto'][:18]} {variante} → API:{seller} → ${price}".strip())
        elif nuevo_estado == "PERDIDO":
            perdiendo.append(f"• {item['producto'][:18]} {variante} → {seller} → ${price}".strip())
        elif nuevo_estado == "NO_PRENDIDA":
            no_prendidas.append(f"• {item['producto'][:18]} {variante} → {seller} → ${price}".strip())
        elif nuevo_estado in ESTADOS_INVALIDOS:
            invalidos.append(f"• {item['producto'][:18]} {variante} → {nuevo_estado}".strip())
        elif nuevo_estado == "SIN_DATOS_STALE":
            stale.append(f"• {item['producto'][:18]} {variante}".strip())

        estado_anterior = ULTIMO_ESTADO.get(sku_patish)
        precio_anterior = ULTIMO_PRECIO.get(sku_patish)
        seller_anterior = ULTIMO_SELLER.get(sku_patish)

        tipo_cambio = []
        if estado_anterior is None:
            tipo_cambio.append("INICIAL")
        else:
            if nuevo_estado != estado_anterior:
                tipo_cambio.append("CAMBIO_ESTADO")
            if str(price) != str(precio_anterior):
                tipo_cambio.append("CAMBIO_PRECIO")
            if seller != seller_anterior:
                tipo_cambio.append("CAMBIO_SELLER")

        if tipo_cambio:
            guardar_evento_csv(fecha_hora, item, seller, price, nuevo_estado, " | ".join(tipo_cambio), otros)

        if es_estado_ganador_verificado(estado_anterior) and nuevo_estado == "PERDIDO":
            perdidas_alerta.append(construir_alerta_perdida(item, r))

        # Fallos transitorios no borran el estado previo de inmediato; invalidez real sí.
        estado_bueno = ULTIMO_ESTADO.get(sku_patish) in ESTADOS_BUENOS_TRANSITORIOS
        if nuevo_estado == "SIN_DATOS" and estado_bueno:
            checked_raw = ULTIMO_LAST_CHECKED.get(sku_patish, "")
            try:
                checked_dt = datetime.strptime(checked_raw, "%Y-%m-%d %H:%M:%S").replace(tzinfo=CDMX_TZ)
                if now_cdmx - checked_dt >= timedelta(minutes=STALE_MINUTES):
                    ULTIMO_ESTADO[sku_patish] = "SIN_DATOS_STALE"
            except Exception:
                ULTIMO_ESTADO[sku_patish] = "SIN_DATOS_STALE"
        else:
            ULTIMO_ESTADO[sku_patish] = nuevo_estado
        if nuevo_estado != "SIN_DATOS":
            reprice_sugerido, reprice_motivo = calcular_reprice_sugerido(
                nuevo_estado, price, r["precio_mio"], r["stock_mio"]
            )
            ULTIMO_PRECIO[sku_patish] = price
            ULTIMO_SELLER[sku_patish] = seller
            ULTIMO_PRECIO_PATISH[sku_patish] = r["precio_mio"]
            ULTIMO_STOCK_PATISH[sku_patish] = r["stock_mio"]
            ULTIMO_STOCK_GANADOR[sku_patish] = r.get("stock_ganador")
            ULTIMO_SEGUNDO_SELLER[sku_patish] = r.get("segundo_seller", "")
            ULTIMO_SEGUNDO_PRECIO[sku_patish] = r.get("segundo_precio", "")
            ULTIMO_REPRICE_SUGERIDO[sku_patish] = reprice_sugerido
            ULTIMO_REPRICE_MOTIVO[sku_patish] = reprice_motivo
            ULTIMO_LAST_CHECKED[sku_patish] = fecha_hora
            ULTIMO_SOURCE[sku_patish] = r.get("source", "")
            ULTIMO_STATUS_CODE[sku_patish] = r.get("status_code", "")
            ULTIMO_ERROR_MESSAGE[sku_patish] = r.get("error_message", "")
            ULTIMO_CONFIDENCE[sku_patish] = r.get("confidence", "")

    items_actuales = construir_items_estado()
    conteos_actuales = defaultdict(int)
    for item in items_actuales:
        conteos_actuales[item.get("estado", "SIN_DATOS")] += 1

    enviar_alerta_perdidas(perdidas_alerta)

    activas = sum(1 for item in CATALOGO if item["estado_oferta"] == "ACTIVA")
    print(
        f"[{now_str}] ✅ 🟢{conteos_actuales.get('GANANDO_VERIFICADO', 0)}"
        f" 🔴{conteos_actuales.get('PERDIDO', 0)} 🟡{conteos_actuales.get('NO_PRENDIDA', 0)}"
        f" ⚠️{conteos_actuales.get('GANANDO_API_NO_VISIBLE', 0)}"
        f" ⛔{conteos_actuales.get('SKU_INVALIDO', 0) + conteos_actuales.get('PRODUCTO_NO_EXISTE', 0)}"
        f" 🧊{conteos_actuales.get('SIN_DATOS_STALE', 0)}"
        f" / {activas} activas / {len(CATALOGO)} total"
    )


def loop_monitor():
    ciclo = 0
    while True:
        try:
            sync_catalogo_desde_url(force=False)
            procesar_comandos()
            monitorear()
            guardar_estado_persistido()
            ciclo += 1
            if ciclo % 48 == 0:  # cada ~1.6 horas
                rotar_csv()
        except Exception as exc:
            print(f"💥 Error en ciclo: {exc}")
        time.sleep(120)


def cargar_catalogo_compatibilidad():
    global CATALOGO
    source_file = SKUS_FILE if os.path.exists(SKUS_FILE) else BOOTSTRAP_SKUS_FILE
    if not os.path.exists(source_file):
        return
    try:
        with open(source_file, newline="", encoding="utf-8-sig") as archivo:
            reader = csv.DictReader(archivo)
            if reader.fieldnames:
                reader.fieldnames = [normalizar_columna(campo) for campo in reader.fieldnames]
            for row in reader:
                sku_liv = normalizar_identificador(row.get("sku", ""))
                sku_patish = limpiar_texto(row.get("sku_patish", ""))
                url = limpiar_texto(row.get("url", ""))
                if not sku_liv or not url or not sku_patish or sku_patish.startswith("Eliminado"):
                    continue
                product_id = url.split("/")[-1].split("?")[0]
                CATALOGO.append({
                    "sku_patish": sku_patish,
                    "sku_liverpool": sku_liv,
                    "product_id": product_id,
                    "vgc": normalizar_identificador(row.get("vgc", "")),
                    "producto": limpiar_texto(row.get("nombre_producto", "")),
                    "estado_oferta": "ACTIVA",
                    "motivo": "",
                    "precio_base": None,
                    "cantidad": 0,
                    "url": url,
                    "color": "",
                    "size": "",
                })
        print(f"📂 {len(CATALOGO)} items cargados desde {source_file} (modo compatibilidad)")
        print("💡 Sube el Excel de Liverpool desde el panel web para activar estados bloqueada/sin stock")
    except Exception as exc:
        print(f"⚠️ No se pudo cargar {source_file}: {exc}")


def imprimir_resumen_local():
    items = construir_items_estado()
    conteos = defaultdict(int)
    for item in items:
        conteos[item.get("estado", "SIN_DATOS")] += 1
    print("\n===== RESUMEN RUN_ONCE =====")
    print(f"Total SKUs: {len(items)}")
    for estado in [
        "GANANDO_VERIFICADO", "GANANDO_API_NO_VISIBLE", "PERDIDO", "NO_PRENDIDA",
        "SKU_INVALIDO", "PRODUCTO_NO_EXISTE", "VGC_INVALIDO", "SIN_DATOS",
        "SIN_DATOS_STALE", "BLOQUEADA", "INACTIVA_STOCK",
    ]:
        print(f"{estado}: {conteos.get(estado, 0)}")
    vgc_alertas = [resumen for resumen in RESUMEN_VGC.values() if resumen.get("alerta")]
    print(f"VGC con variantes faltantes: {len(vgc_alertas)}")
    for vgc in ("1170197151", "1170196309"):
        relacionados = [item for item in items if str(item.get("vgc", "")) == vgc or str(item.get("sku_liverpool", "")) == vgc]
        print(f"\nVGC {vgc}:")
        if not relacionados:
            print("  sin SKUs en catálogo")
            continue
        for item in relacionados:
            print(
                f"  {item['sku_liverpool']} | {item['sku_patish']} | {item['estado']} | "
                f"{item.get('seller_buybox') or '-'} | {item.get('precio_liverpool') or '-'} | "
                f"{item.get('source') or '-'} | {item.get('error_message') or '-'}"
            )
        resumen = RESUMEN_VGC.get(vgc, {})
        if resumen:
            print(f"  Resumen VGC: {resumen.get('alerta_texto') or 'sin faltantes'}")


if __name__ == "__main__":
    print("🔥 Monitor BuyBox v4 iniciado")
    inicializar_csv()

    if not cargar_catalogo_persistido():
        cargar_catalogo_compatibilidad()

    cargar_precios_minimos()
    cargar_estado_persistido_monitor()

    if CATALOGO_SYNC_ON_START:
        sync_catalogo_desde_url(force=True)

    if RUN_ONCE:
        sync_catalogo_desde_url(force=False)
        monitorear()
        guardar_estado_persistido()
        imprimir_resumen_local()
        raise SystemExit(0)

    threading.Thread(target=loop_monitor, daemon=True).start()

    app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)
