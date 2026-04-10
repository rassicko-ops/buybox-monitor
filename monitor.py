import csv
import html
import io
import json
import os
import re
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

CATALOGO = []

ULTIMO_RESUMEN = 0
ULTIMA_FECHA_CSV = None
ULTIMO_UPDATE_ID = 0

BOOTSTRAP_SKUS_FILE = "skus.csv"
CSV_FILE = os.path.join(DATA_DIR, "historico_buybox.csv")
SKUS_FILE = os.path.join(DATA_DIR, "skus.csv")
CATALOGO_FILE = os.path.join(DATA_DIR, "catalogo_activo.json")
ESTADO_FILE = os.path.join(DATA_DIR, "estado_persistido.json")
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
  --bg:#f1f5f9;--card:#fff;--border:#e2e8f0;--text:#0f172a;--muted:#64748b;
  --primary:#10b981;--primary-dark:#059669;--primary-light:#d1fae5;
  --red:#ef4444;--red-light:#fee2e2;
  --yellow:#f59e0b;--yellow-light:#fef9c3;
  --orange:#f97316;--orange-light:#ffedd5;
  --shadow-sm:0 1px 3px rgba(0,0,0,.06),0 1px 2px rgba(0,0,0,.04);
  --shadow:0 4px 6px -1px rgba(0,0,0,.07),0 2px 4px -1px rgba(0,0,0,.04);
  --r:12px;--rf:9999px;
}
body{font-family:'Inter',-apple-system,BlinkMacSystemFont,sans-serif;background:var(--bg);color:var(--text);min-height:100vh;font-size:14px;line-height:1.5}

/* HEADER */
header{background:#fff;border-bottom:1px solid var(--border);padding:0 32px;height:60px;display:flex;align-items:center;gap:20px;position:sticky;top:0;z-index:50;box-shadow:var(--shadow-sm)}
.logo{display:flex;align-items:center;gap:10px}
.logo-icon{width:34px;height:34px;background:var(--primary);border-radius:8px;display:flex;align-items:center;justify-content:center;color:#fff;font-size:17px;font-weight:800;flex-shrink:0}
.logo-name{font-size:15px;font-weight:700;color:var(--text);letter-spacing:-.3px}
.logo-sub{font-size:11px;color:var(--muted);font-weight:400}
.h-space{flex:1}
.live-pill{display:flex;align-items:center;gap:6px;padding:5px 13px;background:var(--primary-light);border-radius:var(--rf);font-size:12px;font-weight:600;color:var(--primary-dark)}
.live-dot{width:7px;height:7px;background:var(--primary);border-radius:50%;animation:pulse 2s ease-in-out infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.25}}

/* PAGE */
.page{max-width:1440px;margin:0 auto;padding:28px 32px 52px}

/* STAT CARDS */
.stats{display:grid;grid-template-columns:repeat(6,1fr);gap:12px;margin-bottom:24px}
.sc{background:var(--card);border-radius:var(--r);border:1px solid var(--border);padding:20px 18px;box-shadow:var(--shadow-sm);position:relative;overflow:hidden;transition:transform .15s,box-shadow .15s}
.sc:hover{transform:translateY(-2px);box-shadow:var(--shadow)}
.sc::before{content:'';position:absolute;top:0;left:0;right:0;height:3px;border-radius:var(--r) var(--r) 0 0}
.sc.g::before{background:var(--primary)}
.sc.r::before{background:var(--red)}
.sc.y::before{background:var(--yellow)}
.sc.o::before{background:var(--orange)}
.sc.gr::before{background:#94a3b8}
.sc.d::before{background:#334155}
.sc-lbl{font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:.06em;color:var(--muted);margin-bottom:8px}
.sc-val{font-size:30px;font-weight:800;line-height:1;letter-spacing:-1px}
.sc.g .sc-val{color:var(--primary)}
.sc.r .sc-val{color:var(--red)}
.sc.y .sc-val{color:var(--yellow)}
.sc.o .sc-val{color:var(--orange)}
.sc.gr .sc-val{color:#64748b}
.sc.d .sc-val{color:#334155}

/* CARD */
.card{background:var(--card);border-radius:var(--r);border:1px solid var(--border);box-shadow:var(--shadow-sm);margin-bottom:16px}
.ch{padding:16px 20px;border-bottom:1px solid var(--border);display:flex;align-items:center;gap:12px}
.ct{font-size:14px;font-weight:600;color:var(--text)}
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

/* BUTTONS */
.btn{display:inline-flex;align-items:center;gap:6px;padding:8px 18px;border-radius:var(--rf);font-family:inherit;font-size:13px;font-weight:600;cursor:pointer;border:none;transition:all .15s;text-decoration:none;white-space:nowrap}
.btn-p{background:var(--primary);color:#fff;box-shadow:0 1px 3px rgba(16,185,129,.3)}
.btn-p:hover{background:var(--primary-dark);box-shadow:0 4px 12px rgba(16,185,129,.35);transform:translateY(-1px)}
.btn-s{background:#fff;color:var(--text);border:1px solid var(--border)}
.btn-s:hover{background:#f8fafc;border-color:#cbd5e1}

/* FILTERS */
.frow{display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-bottom:12px}
.fbtn{padding:6px 14px;border-radius:var(--rf);border:1px solid var(--border);background:#fff;font-family:inherit;font-size:12px;font-weight:500;color:var(--muted);cursor:pointer;transition:all .15s}
.fbtn:hover{border-color:var(--primary);color:var(--primary);background:var(--primary-light)}
.fbtn.active{background:var(--primary);color:#fff;border-color:var(--primary);box-shadow:0 2px 8px rgba(16,185,129,.25)}

/* TOOLBAR */
.toolbar{display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin-bottom:16px}
.sw{flex:1;min-width:220px;max-width:380px;position:relative}
.si{position:absolute;left:11px;top:50%;transform:translateY(-50%);color:var(--muted);font-size:13px;pointer-events:none}
.sinput{width:100%;padding:8px 12px 8px 32px;background:#fff;border:1px solid var(--border);border-radius:var(--rf);font-family:inherit;font-size:13px;color:var(--text);transition:border-color .15s,box-shadow .15s}
.sinput:focus{outline:none;border-color:var(--primary);box-shadow:0 0 0 3px rgba(16,185,129,.12)}
.scount{font-size:12px;color:var(--muted);white-space:nowrap}

/* TABLE */
.tw{overflow:auto;border-top:1px solid var(--border)}
table{width:100%;border-collapse:collapse;font-size:13px}
thead th{padding:10px 14px;font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:.05em;color:var(--muted);background:#f8fafc;border-bottom:1px solid var(--border);white-space:nowrap;position:sticky;top:0;z-index:6}
.cfrow th{background:#fff;padding:6px 8px;border-bottom:1px solid var(--border);position:sticky;top:37px;z-index:5}
.cf{width:100%;min-width:80px;padding:5px 8px;font-family:inherit;font-size:11px;background:#f8fafc;border:1px solid var(--border);border-radius:6px;color:var(--text);transition:border-color .15s}
.cf:focus{outline:none;border-color:var(--primary);background:#fff}
tbody td{padding:11px 14px;border-bottom:1px solid #f1f5f9;vertical-align:middle}
tbody tr:last-child td{border-bottom:none}
tbody tr:hover td{background:#f0fdf9}
.sortable{cursor:pointer;user-select:none}
.sortable:hover{color:var(--primary-dark)}
.sarr{font-size:10px;color:#cbd5e1;margin-left:3px}
.sarr.active{color:var(--primary)}

/* BADGES */
.badge{display:inline-flex;align-items:center;padding:3px 10px;border-radius:var(--rf);font-size:11px;font-weight:600;white-space:nowrap}
.bg{background:#dcfce7;color:#166534}
.br{background:#fee2e2;color:#991b1b}
.by{background:#fef9c3;color:#854d0e}
.bo{background:#ffedd5;color:#9a3412}
.bgr{background:#f1f5f9;color:#475569}

/* MISC */
a.lnk{color:var(--muted);font-size:12px;text-decoration:none}
a.lnk:hover{color:var(--primary)}
.dp{color:var(--primary-dark);font-size:12px;font-weight:500}
.dn{color:var(--red);font-size:12px;font-weight:500}
.ts{text-align:right;font-size:12px;color:var(--muted);padding:10px 20px}
.chip{display:inline-flex;align-items:center;padding:1px 8px;background:#f1f5f9;border-radius:var(--rf);font-size:11px;color:var(--muted);font-weight:500;margin-left:6px}

@media(max-width:1100px){.stats{grid-template-columns:repeat(3,1fr)}}
@media(max-width:640px){.stats{grid-template-columns:repeat(2,1fr)}.page{padding:18px 16px 32px}header{padding:0 16px}}
</style>
</head>
<body>

<header>
  <div class="logo">
    <div class="logo-icon">B</div>
    <div>
      <div class="logo-name">BuyBox Monitor</div>
      <div class="logo-sub">PATISH · Liverpool MX</div>
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
      </div>
      <div class="umsg" id="msg-upload"></div>
    </div>
  </div>

  <div class="card">
    <div class="ch">
      <div class="ct">Estado actual <span class="chip" id="count-visible">—</span></div>
    </div>
    <div class="cb" style="padding-bottom:12px">
      <div class="frow">
        <button class="fbtn active" onclick="setFiltro('TODOS',this)">Todos</button>
        <button class="fbtn" onclick="setFiltro('GANANDO',this)">🟢 Ganando</button>
        <button class="fbtn" onclick="setFiltro('PERDIDO',this)">🔴 Perdiendo</button>
        <button class="fbtn" onclick="setFiltro('NO_PRENDIDA',this)">🟡 No prendida</button>
        <button class="fbtn" onclick="setFiltro('BLOQUEADA',this)">🟠 Bloqueadas</button>
        <button class="fbtn" onclick="setFiltro('INACTIVA_STOCK',this)">⚫ Sin stock</button>
        <button class="fbtn" onclick="setFiltro('SIN_DATOS',this)">Sin datos</button>
      </div>
      <div class="toolbar">
        <div class="sw">
          <span class="si">🔍</span>
          <input id="sku-search" class="sinput" type="search" placeholder="Buscar producto, SKU, VGC…" oninput="setBusqueda(this.value)">
        </div>
        <span class="scount" id="search-status">—</span>
        <button type="button" class="btn btn-s" onclick="clearColumnFilters()">Limpiar filtros</button>
        <a id="download-link" class="btn btn-p" href="/api/exportar?estado=TODOS" target="_blank" rel="noreferrer">⬇ Exportar Excel</a>
      </div>
    </div>

    <div class="tw">
      <table>
        <thead>
          <tr>
            <th class="sortable" onclick="toggleSort('producto')">Producto <span class="sarr" id="sort-producto">↕</span></th>
            <th class="sortable" onclick="toggleSort('color')">Color <span class="sarr" id="sort-color">↕</span></th>
            <th class="sortable" onclick="toggleSort('size')">Tamaño <span class="sarr" id="sort-size">↕</span></th>
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
            <th><input class="cf" data-col-filter="color" type="search" placeholder="Filtrar…" oninput="setColumnFilter('color',this.value)"></th>
            <th><input class="cf" data-col-filter="size" type="search" placeholder="Filtrar…" oninput="setColumnFilter('size',this.value)"></th>
            <th><input class="cf" data-col-filter="sku_patish" type="search" placeholder="Filtrar…" oninput="setColumnFilter('sku_patish',this.value)"></th>
            <th><input class="cf" data-col-filter="sku_liverpool" type="search" placeholder="Filtrar…" oninput="setColumnFilter('sku_liverpool',this.value)"></th>
            <th><input class="cf" data-col-filter="vgc" type="search" placeholder="Filtrar…" oninput="setColumnFilter('vgc',this.value)"></th>
            <th>
              <select class="cf" data-col-filter="estado" onchange="setColumnFilter('estado',this.value)">
                <option value="">Todos</option>
                <option value="GANANDO">Ganando</option>
                <option value="PERDIDO">Perdido</option>
                <option value="NO_PRENDIDA">No prendida</option>
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
          <tr><td colspan="13" style="text-align:center;padding:40px;color:var(--muted)">Cargando…</td></tr>
        </tbody>
      </table>
    </div>
    <div class="ts" id="refresh-ts">—</div>
  </div>

</div>

<script>
const DEFAULT_COLUMN_FILTERS={
  producto:'',color:'',size:'',sku_patish:'',sku_liverpool:'',vgc:'',
  estado:'',seller_buybox:'',precio_liverpool:'',precio_tuyo:'',stock_tuyo:'',diferencia:'',url:'',
};
const NUMERIC_SORT_FIELDS=new Set(['precio_liverpool','precio_tuyo','stock_tuyo','diferencia']);
const STATE_SORT_ORDER={GANANDO:0,PERDIDO:1,NO_PRENDIDA:2,BLOQUEADA:3,INACTIVA_STOCK:4,SIN_DATOS:5};

let filtroActual='TODOS',busquedaActual='',ordenActual='producto_asc',todosItems=[],columnFilters={...DEFAULT_COLUMN_FILTERS};

document.getElementById('excel-input').addEventListener('change',function(){
  document.getElementById('file-name-lbl').textContent=this.files[0]?.name||'Ningún archivo';
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

function setFiltro(f,btn){
  filtroActual=f;
  document.querySelectorAll('.fbtn').forEach(b=>b.classList.remove('active'));
  btn.classList.add('active');
  actualizarDescarga();
  renderTabla();
}

function setBusqueda(value){
  busquedaActual=(value||'').trim().toLowerCase();
  actualizarResumenFiltros();
  actualizarDescarga();
  renderTabla();
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
  if(busquedaActual && activos){status.textContent=`"${busquedaActual}" · ${activos} filtro(s) activo(s)`;return;}
  if(busquedaActual){status.textContent=`Búsqueda: "${busquedaActual}"`;return;}
  if(activos){status.textContent=`${activos} filtro(s) activo(s)`;return;}
  status.textContent='';
}

function actualizarDescarga(){
  const link=document.getElementById('download-link');
  const params=new URLSearchParams();
  params.set('estado',filtroActual);
  if(busquedaActual) params.set('q',busquedaActual);
  for(const [key,value] of Object.entries(columnFilters)){
    if(value) params.set(`f_${key}`,value);
  }
  params.set('sort',ordenActual);
  link.href='/api/exportar?'+params.toString();
  const titulo=filtroActual==='TODOS'?'Todo':filtroActual.replaceAll('_',' ');
  link.textContent='⬇ '+titulo;
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
    for(const k of ['producto','color','size','sku_patish','sku_liverpool','vgc','seller_buybox','url']){
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
    PERDIDO:['br','PERDIDO'],
    NO_PRENDIDA:['by','NO PRENDIDA'],
    BLOQUEADA:['bo','BLOQUEADA'],
    INACTIVA_STOCK:['bgr','SIN STOCK'],
    SIN_DATOS:['bgr','SIN DATOS'],
  };
  const [cls,txt]=map[estado]||['bgr',estado||'—'];
  return `<span class="badge ${cls}">${txt}</span>`;
}

function renderTabla(){
  let items=filtroActual==='TODOS'?todosItems:todosItems.filter(i=>i.estado===filtroActual);
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
  actualizarIndicadoresOrden();
  document.getElementById('count-visible').textContent=items.length;
  const tbody=document.getElementById('tbody-estado');
  if(!items.length){
    tbody.innerHTML='<tr><td colspan="13" style="text-align:center;padding:40px;color:var(--muted)">Sin resultados para este filtro</td></tr>';
    return;
  }
  tbody.innerHTML=items.map(p=>{
    let diff='';
    if(p.precio_liverpool&&p.precio_tuyo){
      const d=parseFloat(p.precio_liverpool)-parseFloat(p.precio_tuyo);
      if(!isNaN(d)) diff=d<0?`<span class="dn">-$${Math.abs(d).toFixed(0)}</span>`:d>0?`<span class="dp">+$${d.toFixed(0)}</span>`:'<span style="color:var(--muted)">igual</span>';
    }
    const stock=(p.stock_tuyo===0||p.stock_tuyo)?escapeHtml(String(p.stock_tuyo)):'-';
    return `<tr>
      <td style="max-width:190px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${escapeHtml(p.producto)}">${escapeHtml(p.producto)}</td>
      <td>${escapeHtml(p.color||'-')}</td>
      <td>${escapeHtml(p.size||'-')}</td>
      <td style="font-size:11px;color:var(--muted)">${escapeHtml(p.sku_patish)}</td>
      <td style="font-size:11px;color:var(--muted)">${escapeHtml(p.sku_liverpool)}</td>
      <td style="font-size:11px;color:var(--muted)">${escapeHtml(p.vgc||'-')}</td>
      <td>${estadoBadge(p.estado)}</td>
      <td>${escapeHtml(p.seller_buybox||'-')}</td>
      <td>${p.precio_liverpool?'$'+escapeHtml(String(p.precio_liverpool)):'-'}</td>
      <td>${p.precio_tuyo?'$'+escapeHtml(String(p.precio_tuyo)):'-'}</td>
      <td>${stock}</td>
      <td>${diff||'-'}</td>
      <td>${p.url?`<a class="lnk" href="${escapeHtml(p.url)}" target="_blank" rel="noreferrer">ver</a>`:'-'}</td>
    </tr>`;
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
    renderTabla();
    document.getElementById('refresh-ts').textContent='Actualizado: '+new Date().toLocaleTimeString('es-MX');
  }catch(e){console.error('Error cargando estado',e)}
}

cargarEstado();
actualizarDescarga();
setInterval(cargarEstado,30000);
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


def escapar(valor):
    return html.escape(str(valor), quote=False)


def es_seller_mio(seller, seller_id):
    seller_texto = limpiar_texto(seller).lower()
    seller_id_texto = normalizar_identificador(seller_id)
    return seller_texto == MY_SELLER.lower() or (seller_id_texto and seller_id_texto == MY_SELLER_ID)


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


def procesar_excel_catalogo(excel_bytes):
    df = pd.read_excel(io.BytesIO(excel_bytes))
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


def construir_items_estado():
    items = []
    for variante in CATALOGO:
        sku = variante["sku_patish"]
        estado_monitor = ULTIMO_ESTADO.get(sku, "")
        if not estado_monitor:
            estado_final = variante["estado_oferta"] if variante["estado_oferta"] != "ACTIVA" else "SIN_DATOS"
        else:
            estado_final = estado_monitor
        precio_tuyo = ULTIMO_PRECIO_PATISH[sku] if sku in ULTIMO_PRECIO_PATISH else ""
        stock_tuyo = ULTIMO_STOCK_PATISH[sku] if sku in ULTIMO_STOCK_PATISH else normalizar_entero(variante.get("cantidad"))
        items.append(
            {
                "sku_patish": sku,
                "sku_liverpool": variante["sku_liverpool"],
                "vgc": variante.get("vgc", ""),
                "producto": variante["producto"],
                "color": variante.get("color", ""),
                "size": variante.get("size", ""),
                "estado": estado_final,
                "seller_buybox": ULTIMO_SELLER.get(sku, ""),
                "precio_liverpool": ULTIMO_PRECIO.get(sku, ""),
                "precio_tuyo": precio_tuyo,
                "stock_tuyo": stock_tuyo,
                "url": variante["url"],
            }
        )
    return items


COLUMNAS_FILTRO_TEXTO = (
    "producto", "color", "size", "sku_patish", "sku_liverpool", "vgc", "seller_buybox", "url",
)
COLUMNAS_FILTRO_NUMERICO = ("precio_liverpool", "precio_tuyo", "stock_tuyo", "diferencia")


def obtener_filtros_columna_request():
    filtros = {}
    for campo in (*COLUMNAS_FILTRO_TEXTO, "estado", *COLUMNAS_FILTRO_NUMERICO):
        valor = request.args.get(f"f_{campo}", "").strip()
        if valor:
            filtros[campo] = valor
    return filtros


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


def filtrar_items_estado(items, estado, busqueda, filtros_columna=None):
    filtrados = items
    if estado and estado != "TODOS":
        filtrados = [item for item in filtrados if item["estado"] == estado]
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
    "GANANDO": 0, "PERDIDO": 1, "NO_PRENDIDA": 2,
    "BLOQUEADA": 3, "INACTIVA_STOCK": 4, "SIN_DATOS": 5,
}

SORT_ALIASES = {"stock_desc": "stock_tuyo_desc", "stock_asc": "stock_tuyo_asc"}

SORTABLE_ITEM_FIELDS = {
    "producto", "color", "size", "sku_patish", "sku_liverpool", "vgc",
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
        global CATALOGO
        CATALOGO = nuevos
        guardar_catalogo_persistido(nuevos)
        guardar_skus_csv(nuevos)
        activas = sum(1 for item in nuevos if item["estado_oferta"] == "ACTIVA")
        inactivas_stock = sum(1 for item in nuevos if item["estado_oferta"] == "INACTIVA_STOCK")
        bloqueadas = sum(1 for item in nuevos if item["estado_oferta"] == "BLOQUEADA")
        if bloqueadas > 0:
            lista = [item for item in nuevos if item["estado_oferta"] == "BLOQUEADA"]
            mensaje = ["🚨 <b>OFERTAS BLOQUEADAS POR LIVERPOOL</b>", "", f"{bloqueadas} variantes bloqueadas:", ""]
            for item in lista[:10]:
                mensaje.append(f"• {escapar(item['producto'][:40])}\n  SKU: {escapar(item['sku_patish'])} | {escapar(item['motivo'])}")
                mensaje.append("")
            if bloqueadas > 10:
                mensaje.append(f"...y {bloqueadas - 10} mas. Ver panel web.")
            enviar_telegram("\n".join(mensaje))
        return jsonify({"ok": True, "total": len(nuevos), "activas": activas, "inactivas_stock": inactivas_stock, "bloqueadas": bloqueadas})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/api/estado")
def api_estado():
    items = construir_items_estado()
    return jsonify({
        "ganando": sum(1 for item in items if item["estado"] == "GANANDO"),
        "perdidos": sum(1 for item in items if item["estado"] == "PERDIDO"),
        "no_prendida": sum(1 for item in items if item["estado"] == "NO_PRENDIDA"),
        "bloqueadas": sum(1 for item in items if item["estado"] == "BLOQUEADA"),
        "sin_stock": sum(1 for item in items if item["estado"] == "INACTIVA_STOCK"),
        "total": len(items),
        "items": items,
    })


@app.route("/api/exportar")
def api_exportar():
    estado = request.args.get("estado", "TODOS").strip().upper() or "TODOS"
    busqueda = request.args.get("q", "").strip()
    orden = request.args.get("sort", "producto_asc").strip()
    filtros_columna = obtener_filtros_columna_request()
    items = construir_items_estado()
    filtrados = filtrar_items_estado(items, estado, busqueda, filtros_columna)
    filtrados = ordenar_items_estado(filtrados, orden)
    columnas = ["estado", "sku_patish", "sku_liverpool", "vgc", "producto", "color", "size",
                "seller_buybox", "precio_liverpool", "precio_tuyo", "stock_tuyo", "url"]
    df = pd.DataFrame(filtrados, columns=columnas)
    if not df.empty:
        df["diferencia"] = [
            formatear_precio(calcular_diferencia_item(item)) if calcular_diferencia_item(item) is not None else ""
            for item in filtrados
        ]
    df = df.rename(columns={
        "estado": "Estado", "sku_patish": "SKU PATISH", "sku_liverpool": "SKU Liverpool",
        "vgc": "VGC", "producto": "Producto", "color": "Color", "size": "Tamano",
        "seller_buybox": "Seller BuyBox", "precio_liverpool": "Precio Liverpool",
        "precio_tuyo": "Tu Precio", "stock_tuyo": "Stock Tuyo", "diferencia": "Diferencia", "url": "URL",
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


@app.route("/status")
def status():
    return jsonify({
        "ganando": sum(1 for valor in ULTIMO_ESTADO.values() if valor == "GANANDO"),
        "perdidos": sum(1 for valor in ULTIMO_ESTADO.values() if valor == "PERDIDO"),
        "no_prendida": sum(1 for valor in ULTIMO_ESTADO.values() if valor == "NO_PRENDIDA"),
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
            }, f, ensure_ascii=False)
    except Exception as exc:
        print(f"⚠️ No se pudo guardar estado: {exc}")


def cargar_estado_persistido_monitor():
    if not os.path.exists(ESTADO_FILE):
        return False
    try:
        with open(ESTADO_FILE, encoding="utf-8") as f:
            estado = json.load(f)
        ULTIMO_ESTADO.update(estado.get("ULTIMO_ESTADO", {}))
        ULTIMO_PRECIO.update(estado.get("ULTIMO_PRECIO", {}))
        ULTIMO_SELLER.update(estado.get("ULTIMO_SELLER", {}))
        ULTIMO_PRECIO_PATISH.update(estado.get("ULTIMO_PRECIO_PATISH", {}))
        ULTIMO_STOCK_PATISH.update(estado.get("ULTIMO_STOCK_PATISH", {}))
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
    ganando = [item for item in CATALOGO if ULTIMO_ESTADO.get(item["sku_patish"]) == "GANANDO"]
    perdiendo = [item for item in CATALOGO if ULTIMO_ESTADO.get(item["sku_patish"]) == "PERDIDO"]
    no_prendidas = [item for item in CATALOGO if ULTIMO_ESTADO.get(item["sku_patish"]) == "NO_PRENDIDA"]
    bloqueadas = [item for item in CATALOGO if item["estado_oferta"] == "BLOQUEADA"]
    now_str = datetime.now(CDMX_TZ).strftime("%H:%M:%S")
    lineas = [
        f"📡 <b>ESTADO ACTUAL</b> - {now_str}", "",
        f"🟢 Ganando: {len(ganando)}", f"🔴 Perdiendo: {len(perdiendo)}",
        f"🟡 No prendida: {len(no_prendidas)}", f"🟠 Bloqueadas Liverpool: {len(bloqueadas)}",
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

def enviar_alerta_perdida(item, seller, price, otros):
    diff_str = ""
    precio_anterior = ULTIMO_PRECIO.get(item["sku_patish"])
    if precio_anterior:
        try:
            diff = float(price) - float(precio_anterior)
            sentido = "mas barato" if diff < 0 else "mas caro"
            diff_str = f"\n💸 Diferencia: ${abs(diff):.2f} ({sentido})"
        except Exception:
            pass
    alt_str = ""
    if otros:
        alt_str = "\n\n👥 <b>Otros oferentes:</b>"
        for alternativo in otros[:3]:
            alt_str += f"\n  • {escapar(alternativo.get('seller', ''))} → ${escapar(alternativo.get('precio', ''))}"
    variante = " ".join(p for p in [item.get("color", ""), item.get("size", "")] if p).strip()
    enviar_telegram(
        "🚨 <b>PERDISTE BUYBOX</b>\n\n"
        f"Producto: {escapar(item['producto'])}\n"
        f"Variante: {escapar(variante or '-')}\n"
        f"SKU PATISH: {escapar(item['sku_patish'])}\n"
        f"SKU Liverpool: {escapar(item['sku_liverpool'])}\n"
        f"Seller: <b>{escapar(seller)}</b>\n"
        f"Precio: ${escapar(price)}{diff_str}{alt_str}\n\n"
        f"{escapar(item['url'])}"
    )


def limpiar_estado_item(sku, estado):
    ULTIMO_ESTADO[sku] = estado
    ULTIMO_PRECIO[sku] = ""
    ULTIMO_SELLER[sku] = ""
    ULTIMO_PRECIO_PATISH.pop(sku, None)
    ULTIMO_STOCK_PATISH.pop(sku, None)


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


def _fetch_alloffers_api(product_id):
    """Llama a la API de Liverpool para obtener el mejor seller por SKU del producto."""
    url = f"https://www.liverpool.com.mx/tienda/browse/marketplace/products/allOffers?productId={product_id}"
    r = _api_get(url)
    if not r or r.status_code != 200:
        if r:
            print(f"  HTTP {r.status_code} allOffers {product_id}")
        return {}
    try:
        data = r.json()
        result = {}
        for offer in data.get("skuOffers", []):
            sku_id = normalizar_identificador(str(offer.get("skuId", "")))
            if sku_id:
                result[sku_id] = offer
        return result
    except Exception as exc:
        print(f"  Error parsing allOffers {product_id}: {exc}")
        return {}


def _fetch_offers_listing(sku_liverpool):
    """Obtiene todos los sellers activos para un SKU (para distinguir PERDIDO vs NO_PRENDIDA)."""
    url = f"https://www.liverpool.com.mx/tienda/browse/marketplace/skus/offersListing?skuId={sku_liverpool}"
    r = _api_get(url)
    if not r or r.status_code != 200:
        return []
    try:
        return r.json().get("sellersOfferDetails", [])
    except Exception:
        return []


def _procesar_grupo_producto(product_id, items_grupo):
    """Obtiene datos de buybox via API y procesa cada variante. Thread-safe."""
    alloffers_mapa = _fetch_alloffers_api(product_id)

    resultados = []
    for item in items_grupo:
        sku_liverpool = normalizar_identificador(item["sku_liverpool"])
        offer_data = alloffers_mapa.get(sku_liverpool)

        if not offer_data:
            resultados.append({
                "item": item, "sku_patish": item["sku_patish"],
                "nuevo_estado": "SIN_DATOS", "seller": "", "price": "",
                "otros": [], "precio_mio": "", "stock_mio": None,
                "color_nuevo": "", "size_nuevo": "", "url_nuevo": None,
            })
            continue

        best_seller = limpiar_texto(offer_data.get("bestSeller", ""))
        seller_id = normalizar_identificador(str(offer_data.get("sellerId", "")))
        price = formatear_precio(offer_data.get("bestSalePrice", ""))
        sellers_count = offer_data.get("sellersCount", 0)
        es_mio = es_seller_mio(best_seller, seller_id)

        if es_mio:
            nuevo_estado = "GANANDO"
            precio_mio = price
            stock_mio = normalizar_entero(item.get("cantidad"))
            otros = []
        else:
            otros = []
            precio_mio = ""
            stock_mio = None
            tiene_mi_oferta = False

            if sellers_count > 1:
                all_sellers = _fetch_offers_listing(sku_liverpool)
                for s in all_sellers:
                    s_name = limpiar_texto(s.get("sellerName", ""))
                    s_id = normalizar_identificador(str(s.get("sellerId", "")))
                    if es_seller_mio(s_name, s_id):
                        tiene_mi_oferta = True
                        precio_mio = formatear_precio(s.get("salePrice", ""))
                    else:
                        otros.append({"seller": s_name, "precio": formatear_precio(s.get("salePrice", ""))})

            if tiene_mi_oferta:
                nuevo_estado = "PERDIDO"
            elif sellers_count > 0:
                nuevo_estado = "NO_PRENDIDA"
            else:
                nuevo_estado = "PERDIDO"

        resultados.append({
            "item": item, "sku_patish": item["sku_patish"],
            "nuevo_estado": nuevo_estado, "seller": best_seller, "price": price,
            "otros": otros[:4], "precio_mio": precio_mio, "stock_mio": stock_mio,
            "color_nuevo": "", "size_nuevo": "", "url_nuevo": None,
        })

    return resultados


# ================================
# MONITOR PRINCIPAL
# ================================

def monitorear():
    global ULTIMO_RESUMEN, ULTIMA_FECHA_CSV

    ganando = []
    perdiendo = []
    no_prendidas = []
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

        if nuevo_estado == "GANANDO":
            ganando.append(f"• {item['producto'][:22]} {variante} → ${price}".strip())
        elif nuevo_estado == "PERDIDO":
            perdiendo.append(f"• {item['producto'][:18]} {variante} → {seller} → ${price}".strip())
        elif nuevo_estado == "NO_PRENDIDA":
            no_prendidas.append(f"• {item['producto'][:18]} {variante} → {seller} → ${price}".strip())

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

        if estado_anterior == "GANANDO" and nuevo_estado in {"PERDIDO", "NO_PRENDIDA"}:
            enviar_alerta_perdida(item, seller, price, otros)

        if estado_anterior in {"PERDIDO", "NO_PRENDIDA"} and nuevo_estado == "GANANDO":
            enviar_telegram(
                "✅ <b>RECUPERASTE BUYBOX</b>\n\n"
                f"Producto: {escapar(item['producto'])}\n"
                f"Variante: {escapar(variante or '-')}\n"
                f"SKU PATISH: {escapar(sku_patish)} | Precio: ${escapar(price)}\n\n"
                f"{escapar(item['url'])}"
            )

        # No sobreescribir estado bueno con SIN_DATOS (fallo de API transitorio)
        estado_bueno = ULTIMO_ESTADO.get(sku_patish) in ("GANANDO", "PERDIDO", "NO_PRENDIDA")
        if nuevo_estado != "SIN_DATOS" or not estado_bueno:
            ULTIMO_ESTADO[sku_patish] = nuevo_estado
        if nuevo_estado != "SIN_DATOS":
            ULTIMO_PRECIO[sku_patish] = price
            ULTIMO_SELLER[sku_patish] = seller
            ULTIMO_PRECIO_PATISH[sku_patish] = r["precio_mio"]
            ULTIMO_STOCK_PATISH[sku_patish] = r["stock_mio"]

    # Resumen cada 15 min
    if time.time() - ULTIMO_RESUMEN >= 900:
        bloqueadas = sum(1 for item in CATALOGO if item["estado_oferta"] == "BLOQUEADA")
        mensaje = (
            "📊 <b>RESUMEN BUYBOX</b>\n\n"
            f"🕒 {now_str}\n\n"
            f"🟢 Ganando: {len(ganando)}\n"
            f"🔴 Perdiendo: {len(perdiendo)}\n"
            f"🟡 No prendida: {len(no_prendidas)}\n"
            f"🟠 Bloqueadas Liverpool: {bloqueadas}\n"
        )
        if ganando:
            mensaje += "\n🟢 <b>GANANDO</b>\n" + "\n".join(escapar(item) for item in ganando[:10]) + "\n"
        if perdiendo:
            mensaje += "\n🔴 <b>PERDIENDO</b>\n" + "\n".join(escapar(item) for item in perdiendo[:10]) + "\n"
        if no_prendidas:
            mensaje += "\n🟡 <b>NO PRENDIDA</b>\n" + "\n".join(escapar(item) for item in no_prendidas[:10]) + "\n"
        if bloqueadas:
            mensaje += "\n⚠️ Usa /bloqueadas para ver detalles."
        enviar_telegram(mensaje)
        ULTIMO_RESUMEN = time.time()

    # CSV diario a las 10am
    if now_cdmx.hour == 10 and now_cdmx.minute <= 2:
        fecha_actual = now_cdmx.strftime("%Y-%m-%d")
        if ULTIMA_FECHA_CSV != fecha_actual:
            enviar_csv_telegram()
            ULTIMA_FECHA_CSV = fecha_actual

    activas = sum(1 for item in CATALOGO if item["estado_oferta"] == "ACTIVA")
    print(
        f"[{now_str}] ✅ 🟢{len(ganando)} 🔴{len(perdiendo)} 🟡{len(no_prendidas)}"
        f" / {activas} activas / {len(CATALOGO)} total"
    )


def loop_monitor():
    ciclo = 0
    while True:
        try:
            procesar_comandos()
            monitorear()
            guardar_estado_persistido()
            ciclo += 1
            if ciclo % 48 == 0:  # cada ~1.6 horas
                rotar_csv()
        except Exception as exc:
            print(f"💥 Error en ciclo: {exc}")
            enviar_telegram(f"⚠️ Error en ciclo: {escapar(exc)}")
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
                url = limpiar_texto(row.get("url", ""))
                if not sku_liv or not url:
                    continue
                product_id = url.split("/")[-1].split("?")[0]
                CATALOGO.append({
                    "sku_patish": limpiar_texto(row.get("sku_patish", "")),
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


if __name__ == "__main__":
    print("🔥 Monitor BuyBox v4 iniciado")
    inicializar_csv()

    if not cargar_catalogo_persistido():
        cargar_catalogo_compatibilidad()

    cargar_estado_persistido_monitor()

    threading.Thread(target=loop_monitor, daemon=True).start()

    enviar_telegram(
        "🚀 <b>Monitor BuyBox v4 iniciado</b>\n\n"
        "⚡ Requests en paralelo\n"
        "💾 Estado persistido entre reinicios\n"
        "🔄 CSV auto-rotado cada 30 dias\n"
        "🔁 Retry automatico en errores HTTP\n\n"
        "Comandos:\n"
        "/estado · /bloqueadas · /reporte · /ayuda"
    )

    app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)
