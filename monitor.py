import csv
import html
import io
import json
import os
import re
import threading
import time
from io import BytesIO
from collections import defaultdict
from datetime import datetime, timedelta, timezone

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

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "es-MX,es;q=0.9",
}

CDMX_TZ = timezone(timedelta(hours=-6))

ULTIMO_ESTADO = {}
ULTIMO_PRECIO = {}
ULTIMO_SELLER = {}

CATALOGO = []

ULTIMO_RESUMEN = 0
ULTIMA_FECHA_CSV = None
ULTIMO_UPDATE_ID = 0

BOOTSTRAP_SKUS_FILE = "skus.csv"
CSV_FILE = os.path.join(DATA_DIR, "historico_buybox.csv")
SKUS_FILE = os.path.join(DATA_DIR, "skus.csv")
CATALOGO_FILE = os.path.join(DATA_DIR, "catalogo_activo.json")
PORT = int(os.getenv("PORT", "8080"))

app = Flask(__name__)

HTML_PANEL = """<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>BuyBox Monitor v3 - PATISH</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=Syne:wght@400;700;800&display=swap');
  :root{--bg:#0a0a0f;--surface:#13131a;--border:#1e1e2e;--accent:#00ff88;--red:#ff4466;--yellow:#ffbb00;--orange:#ff8800;--text:#e0e0e0;--muted:#555;--card:#16161f}
  *{box-sizing:border-box;margin:0;padding:0}
  body{background:var(--bg);color:var(--text);font-family:'Space Mono',monospace;min-height:100vh}
  header{border-bottom:1px solid var(--border);padding:18px 32px;display:flex;align-items:center;gap:14px;background:var(--surface)}
  .dot{width:9px;height:9px;background:var(--accent);border-radius:50%;animation:pulse 2s infinite}
  @keyframes pulse{0%,100%{opacity:1}50%{opacity:.2}}
  header h1{font-family:'Syne',sans-serif;font-weight:800;font-size:1.1rem;letter-spacing:.06em;color:#fff}
  header .tag{margin-left:auto;font-size:.65rem;color:var(--muted);border:1px solid var(--border);padding:3px 10px;border-radius:3px}
  .stats{display:flex;gap:1px;background:var(--border);border-bottom:1px solid var(--border)}
  .stat{flex:1;background:var(--surface);padding:16px 20px;text-align:center}
  .stat .num{font-family:'Syne',sans-serif;font-size:2rem;font-weight:800;line-height:1}
  .stat .lbl{font-size:.6rem;color:var(--muted);margin-top:5px;text-transform:uppercase;letter-spacing:.1em}
  .stat.g .num{color:var(--accent)}.stat.r .num{color:var(--red)}.stat.y .num{color:var(--yellow)}.stat.o .num{color:var(--orange)}.stat.gr .num{color:var(--muted)}
  main{padding:22px 32px;max-width:1500px;margin:0 auto}
  .sec{font-family:'Syne',sans-serif;font-size:.7rem;font-weight:700;letter-spacing:.14em;text-transform:uppercase;color:var(--muted);margin:28px 0 10px;display:flex;align-items:center;gap:12px}
  .sec-count{background:var(--border);color:var(--muted);padding:2px 8px;border-radius:3px;font-size:.65rem}
  .upload-bar{background:var(--card);border:1px solid var(--border);border-radius:5px;padding:18px 22px;margin-bottom:20px}
  .upload-bar h3{font-family:'Syne',sans-serif;font-size:.85rem;margin-bottom:6px;color:#fff}
  .upload-bar p{font-size:.68rem;color:var(--muted);margin-bottom:14px}
  .upload-row{display:flex;align-items:center;gap:12px;flex-wrap:wrap}
  .file-label{padding:8px 18px;border-radius:3px;border:1px solid var(--border);color:var(--text);font-family:'Space Mono',monospace;font-size:.72rem;cursor:pointer;transition:border-color .2s}
  .file-label:hover{border-color:var(--accent)}
  .file-name{font-size:.68rem;color:var(--muted)}
  input[type=file]{display:none}
  .filters{display:flex;gap:8px;margin-bottom:12px;flex-wrap:wrap}
  .filter-btn{padding:5px 14px;border-radius:3px;border:1px solid var(--border);background:transparent;color:var(--muted);font-family:'Space Mono',monospace;font-size:.68rem;cursor:pointer;transition:all .15s}
  .filter-btn:hover{border-color:var(--accent);color:var(--accent)}
  .filter-btn.active{background:var(--accent);color:#000;border-color:var(--accent)}
  .search-row{display:flex;gap:12px;align-items:center;flex-wrap:wrap;margin:0 0 14px}
  .search-input{min-width:280px;flex:1;max-width:460px;background:var(--card);color:var(--text);border:1px solid var(--border);border-radius:3px;padding:9px 12px;font-family:'Space Mono',monospace;font-size:.72rem}
  .search-input:focus{outline:none;border-color:var(--accent)}
  .search-help{font-size:.65rem;color:var(--muted)}
  .download-btn{display:inline-flex;align-items:center;justify-content:center;text-decoration:none}
  .tw{overflow-x:auto}
  table{width:100%;border-collapse:collapse;font-size:.74rem}
  th{text-align:left;font-size:.6rem;text-transform:uppercase;letter-spacing:.09em;color:var(--muted);padding:9px 12px;border-bottom:1px solid var(--border);white-space:nowrap}
  td{padding:10px 12px;border-bottom:1px solid var(--border);vertical-align:middle}
  tr:hover td{background:var(--card)}
  .badge{display:inline-block;padding:2px 7px;border-radius:3px;font-size:.6rem;font-weight:700;letter-spacing:.06em;white-space:nowrap}
  .bw{background:rgba(0,255,136,.1);color:var(--accent);border:1px solid rgba(0,255,136,.2)}
  .bl{background:rgba(255,68,102,.1);color:var(--red);border:1px solid rgba(255,68,102,.2)}
  .bo{background:rgba(255,136,0,.1);color:var(--orange);border:1px solid rgba(255,136,0,.2)}
  .bn{background:rgba(100,100,100,.1);color:var(--muted);border:1px solid var(--border)}
  a.lnk{color:var(--muted);text-decoration:none;font-size:.62rem} a.lnk:hover{color:var(--accent)}
  .diff-neg{color:var(--red);font-size:.68rem}
  .diff-pos{color:var(--accent);font-size:.68rem}
  .ts{font-size:.62rem;color:var(--muted);text-align:right;margin-top:10px}
  .btn{padding:9px 20px;border-radius:3px;border:none;font-family:'Space Mono',monospace;font-size:.72rem;font-weight:700;cursor:pointer;transition:opacity .2s}
  .btn:hover{opacity:.75}
  .bp{background:var(--accent);color:#000}
  .msg{margin-top:8px;font-size:.72rem;min-height:1em}
  .ok{color:var(--accent)}.err{color:var(--red)}
</style>
</head>
<body>
<header>
  <div class="dot"></div>
  <h1>BUYBOX MONITOR v3</h1>
  <span class="tag">PATISH · Liverpool MX · variantes</span>
</header>

<div class="stats">
  <div class="stat g"><div class="num" id="sg">-</div><div class="lbl">Ganando BuyBox</div></div>
  <div class="stat r"><div class="num" id="sp">-</div><div class="lbl">Perdiendo</div></div>
  <div class="stat o"><div class="num" id="sb">-</div><div class="lbl">Bloqueadas</div></div>
  <div class="stat gr"><div class="num" id="ss">-</div><div class="lbl">Sin stock</div></div>
  <div class="stat y"><div class="num" id="st">-</div><div class="lbl">Total variantes</div></div>
</div>

<main>

  <div class="sec">Actualizar catalogo</div>
  <div class="upload-bar">
    <h3>Subir reporte de ofertas de Liverpool</h3>
    <p>Descarga el Excel semanal de Liverpool y subelo aqui. El sistema actualiza el catalogo activo, lo deja guardado para reinicios y vuelve a monitorear con ese corte.</p>
    <div class="upload-row">
      <label class="file-label" for="excel-input">Elegir archivo</label>
      <input type="file" id="excel-input" accept=".xlsx,.xls">
      <span class="file-name" id="file-name-lbl">Ningun archivo seleccionado</span>
      <button class="btn bp" onclick="subirCatalogo()">Actualizar catalogo</button>
    </div>
    <div class="msg" id="msg-upload"></div>
  </div>

  <div class="sec">Estado actual <span class="sec-count" id="count-visible">-</span></div>
  <div class="filters">
    <button class="filter-btn active" onclick="setFiltro('TODOS',this)">Todos</button>
    <button class="filter-btn" onclick="setFiltro('GANANDO',this)">Ganando</button>
    <button class="filter-btn" onclick="setFiltro('PERDIDO',this)">Perdiendo</button>
    <button class="filter-btn" onclick="setFiltro('BLOQUEADA',this)">Bloqueadas</button>
    <button class="filter-btn" onclick="setFiltro('INACTIVA_STOCK',this)">Sin stock</button>
    <button class="filter-btn" onclick="setFiltro('SIN_DATOS',this)">Sin datos</button>
  </div>
  <div class="search-row">
    <input
      id="sku-search"
      class="search-input"
      type="search"
      placeholder="Buscar por SKU Liverpool o SKU PATISH"
      oninput="setBusqueda(this.value)"
    >
    <div class="search-help" id="search-status">Sin busqueda activa</div>
    <a id="download-link" class="btn bp download-btn" href="/api/exportar?estado=TODOS" target="_blank" rel="noreferrer">
      Descargar Excel de Todos
    </a>
  </div>

  <div class="tw">
    <table>
      <thead>
        <tr>
          <th>Producto</th><th>Color</th><th>Tamano</th><th>SKU PATISH</th><th>SKU Liverpool</th><th>VGC</th>
          <th>Estado</th><th>Seller BuyBox</th><th>Precio BuyBox</th><th>Tu precio</th><th>Diferencia</th><th>URL</th>
        </tr>
      </thead>
      <tbody id="tbody-estado">
        <tr><td colspan="12" style="color:var(--muted);text-align:center;padding:32px">Cargando...</td></tr>
      </tbody>
    </table>
  </div>
  <div class="ts" id="refresh-ts">-</div>

</main>

<script>
let filtroActual='TODOS', busquedaActual='', todosItems=[];

document.getElementById('excel-input').addEventListener('change',function(){
  document.getElementById('file-name-lbl').textContent=this.files[0]?.name||'Ningun archivo';
});

async function subirCatalogo(){
  const input=document.getElementById('excel-input');
  const msg=document.getElementById('msg-upload');
  if(!input.files[0]){msg.className='msg err';msg.textContent='Selecciona el archivo primero.';return;}
  const fd=new FormData();fd.append('file',input.files[0]);
  msg.className='msg';msg.textContent='Procesando...';
  const resp=await fetch('/api/catalogo',{method:'POST',body:fd});
  const d=await resp.json();
  if(d.ok){
    msg.className='msg ok';
    msg.textContent=`Catalogo actualizado: ${d.total} variantes, ${d.activas} activas, ${d.inactivas_stock} sin stock, ${d.bloqueadas} bloqueadas.`;
    cargarEstado();
  }else{
    msg.className='msg err';
    msg.textContent='Error: '+(d.error||'No se pudo procesar el archivo');
  }
}

function setFiltro(f,btn){
  filtroActual=f;
  document.querySelectorAll('.filter-btn').forEach(b=>b.classList.remove('active'));
  btn.classList.add('active');
  actualizarDescarga();
  renderTabla();
}

function setBusqueda(value){
  busquedaActual=(value||'').trim().toLowerCase();
  const status=document.getElementById('search-status');
  status.textContent=busquedaActual
    ? `Busqueda activa: ${value.trim()}`
    : 'Sin busqueda activa';
  actualizarDescarga();
  renderTabla();
}

function actualizarDescarga(){
  const link=document.getElementById('download-link');
  const params=new URLSearchParams();
  params.set('estado',filtroActual);
  if(busquedaActual){
    params.set('q',busquedaActual);
  }
  link.href='/api/exportar?'+params.toString();
  const titulo=filtroActual==='TODOS'?'Todos':filtroActual.replaceAll('_',' ');
  link.textContent='Descargar Excel de '+titulo;
}

function escapeHtml(value){
  return String(value ?? '')
    .replaceAll('&','&amp;')
    .replaceAll('<','&lt;')
    .replaceAll('>','&gt;')
    .replaceAll('"','&quot;')
    .replaceAll("'","&#39;");
}

function renderTabla(){
  let items=filtroActual==='TODOS'?todosItems:todosItems.filter(i=>i.estado===filtroActual);
  if(busquedaActual){
    items=items.filter(i=>{
      const skuLiverpool=String(i.sku_liverpool||'').toLowerCase();
      const skuPatish=String(i.sku_patish||'').toLowerCase();
      const vgc=String(i.vgc||'').toLowerCase();
      return skuLiverpool.includes(busquedaActual) || skuPatish.includes(busquedaActual) || vgc.includes(busquedaActual);
    });
  }
  document.getElementById('count-visible').textContent=items.length;
  const tbody=document.getElementById('tbody-estado');
  if(!items.length){
    tbody.innerHTML='<tr><td colspan="12" style="color:var(--muted);text-align:center;padding:28px">Sin datos para este filtro</td></tr>';
    return;
  }
  tbody.innerHTML=items.map(p=>{
    const bc=p.estado==='GANANDO'?'bw':p.estado==='PERDIDO'?'bl':p.estado==='BLOQUEADA'?'bo':'bn';
    const bt=p.estado==='GANANDO'?'GANANDO':p.estado==='PERDIDO'?'PERDIDO':p.estado==='BLOQUEADA'?'BLOQUEADA':p.estado==='SIN_DATOS'?'SIN DATOS':'SIN STOCK';
    let diff='';
    if(p.precio_buybox&&p.precio_patish){
      const d=parseFloat(p.precio_buybox)-parseFloat(p.precio_patish);
      if(!isNaN(d))diff=d<0?`<span class="diff-neg">-$${Math.abs(d).toFixed(2)}</span>`:d>0?`<span class="diff-pos">+$${d.toFixed(2)}</span>`:'<span style="color:var(--muted)">igual</span>';
    }
    return `<tr>
      <td style="max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${escapeHtml(p.producto)}">${escapeHtml(p.producto)}</td>
      <td>${escapeHtml(p.color||'-')}</td><td>${escapeHtml(p.size||'-')}</td>
      <td style="font-size:.65rem">${escapeHtml(p.sku_patish)}</td>
      <td style="font-size:.65rem">${escapeHtml(p.sku_liverpool)}</td>
      <td style="font-size:.65rem">${escapeHtml(p.vgc||'-')}</td>
      <td><span class="badge ${bc}">${escapeHtml(bt)}</span></td>
      <td>${escapeHtml(p.seller_buybox||'-')}</td>
      <td>${p.precio_buybox?'$'+escapeHtml(String(p.precio_buybox)):'-'}</td>
      <td>${p.precio_patish?'$'+escapeHtml(String(p.precio_patish)):'-'}</td>
      <td>${diff||'-'}</td>
      <td>${p.url?`<a class="lnk" href="${escapeHtml(p.url)}" target="_blank" rel="noreferrer">ver</a>`:'-'}</td>
    </tr>`;
  }).join('');
}

async function cargarEstado(){
  const resp=await fetch('/api/estado');
  const d=await resp.json();
  document.getElementById('sg').textContent=d.ganando;
  document.getElementById('sp').textContent=d.perdidos;
  document.getElementById('sb').textContent=d.bloqueadas;
  document.getElementById('ss').textContent=d.sin_stock;
  document.getElementById('st').textContent=d.total;
  todosItems=d.items||[];
  renderTabla();
  document.getElementById('refresh-ts').textContent='Actualizado: '+new Date().toLocaleTimeString('es-MX');
}

cargarEstado();
actualizarDescarga();
setInterval(cargarEstado,30000);
</script>
</body>
</html>"""


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
                "precio_buybox": ULTIMO_PRECIO.get(sku, ""),
                "precio_patish": formatear_precio(variante.get("precio_base")),
                "url": variante["url"],
            }
        )
    return items


def filtrar_items_estado(items, estado, busqueda):
    filtrados = items
    if estado and estado != "TODOS":
        filtrados = [item for item in filtrados if item["estado"] == estado]
    if busqueda:
        termino = busqueda.strip().lower()
        filtrados = [
            item
            for item in filtrados
            if termino in str(item.get("sku_liverpool", "")).lower()
            or termino in str(item.get("sku_patish", "")).lower()
            or termino in str(item.get("vgc", "")).lower()
        ]
    return filtrados


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
            mensaje = [
                "🚨 <b>OFERTAS BLOQUEADAS POR LIVERPOOL</b>",
                "",
                f"{bloqueadas} variantes bloqueadas en tu catalogo:",
                "",
            ]
            for item in lista[:10]:
                mensaje.append(
                    f"• {escapar(item['producto'][:40])}\n  SKU: {escapar(item['sku_patish'])} | {escapar(item['motivo'])}"
                )
                mensaje.append("")
            if bloqueadas > 10:
                mensaje.append(f"...y {bloqueadas - 10} mas. Ver panel web.")
            enviar_telegram("\n".join(mensaje))

        return jsonify(
            {
                "ok": True,
                "total": len(nuevos),
                "activas": activas,
                "inactivas_stock": inactivas_stock,
                "bloqueadas": bloqueadas,
            }
        )
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/api/estado")
def api_estado():
    items = construir_items_estado()
    return jsonify(
        {
            "ganando": sum(1 for item in items if item["estado"] == "GANANDO"),
            "perdidos": sum(1 for item in items if item["estado"] == "PERDIDO"),
            "bloqueadas": sum(1 for item in items if item["estado"] == "BLOQUEADA"),
            "sin_stock": sum(1 for item in items if item["estado"] == "INACTIVA_STOCK"),
            "total": len(items),
            "items": items,
        }
    )


@app.route("/api/exportar")
def api_exportar():
    estado = request.args.get("estado", "TODOS").strip().upper() or "TODOS"
    busqueda = request.args.get("q", "").strip()

    items = construir_items_estado()
    filtrados = filtrar_items_estado(items, estado, busqueda)

    columnas = [
        "estado",
        "sku_patish",
        "sku_liverpool",
        "vgc",
        "producto",
        "color",
        "size",
        "seller_buybox",
        "precio_buybox",
        "precio_patish",
        "url",
    ]
    df = pd.DataFrame(filtrados, columns=columnas)
    df = df.rename(
        columns={
            "estado": "Estado",
            "sku_patish": "SKU PATISH",
            "sku_liverpool": "SKU Liverpool",
            "vgc": "VGC",
            "producto": "Producto",
            "color": "Color",
            "size": "Tamano",
            "seller_buybox": "Seller BuyBox",
            "precio_buybox": "Precio BuyBox",
            "precio_patish": "Tu Precio",
            "url": "URL",
        }
    )

    salida = BytesIO()
    nombre_estado = estado.lower()
    if busqueda:
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
    return send_file(
        salida,
        as_attachment=True,
        download_name=nombre_archivo,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@app.route("/status")
def status():
    return jsonify(
        {
            "ganando": sum(1 for valor in ULTIMO_ESTADO.values() if valor == "GANANDO"),
            "perdidos": sum(1 for valor in ULTIMO_ESTADO.values() if valor == "PERDIDO"),
            "total": len(CATALOGO),
            "ts": datetime.now(CDMX_TZ).strftime("%Y-%m-%d %H:%M:%S"),
        }
    )


def guardar_skus_csv(items):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(SKUS_FILE, "w", newline="", encoding="utf-8-sig") as archivo:
        writer = csv.DictWriter(
            archivo,
            fieldnames=["sku", "url", "tu_nombre_seller", "nombre_producto", "sku_patish"],
        )
        writer.writeheader()
        for item in items:
            writer.writerow(
                {
                    "sku": item["sku_liverpool"],
                    "url": item["url"],
                    "tu_nombre_seller": MY_SELLER,
                    "nombre_producto": item["producto"],
                    "sku_patish": item["sku_patish"],
                }
            )


def inicializar_csv():
    os.makedirs(DATA_DIR, exist_ok=True)
    if not os.path.exists(CSV_FILE):
        with open(CSV_FILE, "w", newline="", encoding="utf-8") as archivo:
            writer = csv.writer(archivo)
            writer.writerow(
                [
                    "fecha_hora",
                    "sku_patish",
                    "sku_liverpool",
                    "producto",
                    "color",
                    "size",
                    "seller_ganador",
                    "precio",
                    "estado",
                    "tipo_cambio",
                    "url",
                    "sellers_alternativos",
                ]
            )
        print("📝 CSV historico inicializado")


def guardar_evento_csv(fecha_hora, item, seller, price, estado, tipo_cambio, alternativos=None):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(CSV_FILE, "a", newline="", encoding="utf-8") as archivo:
        writer = csv.writer(archivo)
        writer.writerow(
            [
                fecha_hora,
                item["sku_patish"],
                item["sku_liverpool"],
                item["producto"],
                item.get("color", ""),
                item.get("size", ""),
                seller,
                price,
                estado,
                tipo_cambio,
                item["url"],
                json.dumps(alternativos or [], ensure_ascii=False),
            ]
        )


def obtener_html(url):
    try:
        respuesta = requests.get(url, headers=HEADERS, timeout=20)
        if respuesta.status_code == 200:
            return respuesta.text
        print(f"  HTTP {respuesta.status_code}: {url[:70]}")
        return None
    except Exception as exc:
        print(f"  Error HTTP: {exc}")
        return None


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

    biggest = sorted(
        variants_blocks,
        key=lambda block: len(block) if isinstance(block, list) else 0,
        reverse=True,
    )[0]
    if not isinstance(biggest, list):
        return []

    resultado = []
    for variante in biggest:
        if not isinstance(variante, dict):
            continue

        offers_obj = variante.get("offers", {})
        offers_arr = []
        best_offer = None

        if isinstance(offers_obj, dict):
            offers_arr = offers_obj.get("offers", [])
            if not isinstance(offers_arr, list):
                offers_arr = []
            best_offer = offers_obj.get("bestOffer")

        ganador = None
        if isinstance(best_offer, dict):
            ganador = {
                "seller": limpiar_texto(best_offer.get("sellerName")),
                "sellerId": normalizar_identificador(best_offer.get("sellerId")),
                "precio": formatear_precio(best_offer.get("salePrice")),
            }
        elif offers_arr:
            oferta = offers_arr[0]
            if isinstance(oferta, dict):
                ganador = {
                    "seller": limpiar_texto(oferta.get("sellerName")),
                    "sellerId": normalizar_identificador(oferta.get("sellerId")),
                    "precio": formatear_precio(oferta.get("salePrice")),
                }

        otros = []
        for oferta in offers_arr[1:]:
            if not isinstance(oferta, dict):
                continue
            otros.append(
                {
                    "seller": limpiar_texto(oferta.get("sellerName")),
                    "precio": formatear_precio(oferta.get("salePrice")),
                }
            )
            if len(otros) >= 4:
                break

        resultado.append(
            {
                "skuId": normalizar_identificador(variante.get("skuId")),
                "color": limpiar_texto(variante.get("color")),
                "size": limpiar_texto(variante.get("size")),
                "hasValidOnlineInventory": str(variante.get("hasValidOnlineInventory", "false")).lower(),
                "sellersCount": variante.get("sellersCount", 0),
                "buybox": ganador,
                "otros_sellers": otros,
            }
        )

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
        r'"sellerName"\s*:\s*"([^"]+)"[^}]{0,200}?"salePrice"\s*:\s*"?(\d+(?:\.\d+)?)"?',
        html_text,
    ):
        seller_alt = match.group(1)
        if seller_alt in vistos:
            continue
        vistos.add(seller_alt)
        alternativos.append({"seller": seller_alt, "precio": match.group(2)})
        if len(alternativos) >= 4:
            break

    return seller, price, alternativos


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
                enviar_telegram_a(
                    chat_id,
                    "📋 <b>Comandos disponibles</b>\n\n"
                    "/estado - Resumen en tiempo real\n"
                    "/bloqueadas - Ofertas bloqueadas por Liverpool\n"
                    "/reporte - Analisis de patrones\n"
                    "/ayuda - Ayuda",
                )
    except Exception as exc:
        print(f"Error comandos: {exc}")


def generar_estado_actual():
    if not CATALOGO:
        return "⚠️ Catalogo vacio. Sube el Excel de Liverpool desde el panel web."

    ganando = [item for item in CATALOGO if ULTIMO_ESTADO.get(item["sku_patish"]) == "GANANDO"]
    perdiendo = [item for item in CATALOGO if ULTIMO_ESTADO.get(item["sku_patish"]) == "PERDIDO"]
    bloqueadas = [item for item in CATALOGO if item["estado_oferta"] == "BLOQUEADA"]
    now_str = datetime.now(CDMX_TZ).strftime("%H:%M:%S")

    lineas = [
        f"📡 <b>ESTADO ACTUAL</b> - {now_str}",
        "",
        f"🟢 Ganando: {len(ganando)}",
        f"🔴 Perdiendo: {len(perdiendo)}",
        f"🟠 Bloqueadas Liverpool: {len(bloqueadas)}",
        f"📦 Total catalogo: {len(CATALOGO)}",
    ]

    if ganando:
        lineas.append("")
        lineas.append("🟢 <b>GANANDO</b>")
        for item in ganando[:10]:
            variante = " ".join(parte for parte in [item.get("color", ""), item.get("size", "")] if parte).strip()
            sufijo = f" {escapar(variante)}" if variante else ""
            lineas.append(
                f"• {escapar(item['producto'][:22])}{sufijo} → ${escapar(ULTIMO_PRECIO.get(item['sku_patish'], '?'))}"
            )

    if perdiendo:
        lineas.append("")
        lineas.append("🔴 <b>PERDIENDO</b>")
        for item in perdiendo[:10]:
            variante = " ".join(parte for parte in [item.get("color", ""), item.get("size", "")] if parte).strip()
            sufijo = f" {escapar(variante)}" if variante else ""
            lineas.append(
                f"• {escapar(item['producto'][:18])}{sufijo} → {escapar(ULTIMO_SELLER.get(item['sku_patish'], '?'))} → ${escapar(ULTIMO_PRECIO.get(item['sku_patish'], '?'))}"
            )

    return "\n".join(lineas)


def generar_reporte_bloqueadas():
    bloqueadas = [item for item in CATALOGO if item["estado_oferta"] == "BLOQUEADA"]
    if not bloqueadas:
        return "✅ No hay ofertas bloqueadas por Liverpool."

    lineas = [f"🟠 <b>BLOQUEADAS POR LIVERPOOL</b> ({len(bloqueadas)})", ""]
    for item in bloqueadas[:20]:
        lineas.append(
            f"• {escapar(item['producto'][:35])}\n  SKU: {escapar(item['sku_patish'])} | {escapar(item.get('motivo', '?'))}"
        )
    if len(bloqueadas) > 20:
        lineas.append("")
        lineas.append(f"...y {len(bloqueadas) - 20} mas.")
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

        lineas = [
            "📊 <b>REPORTE DE PATRONES</b>",
            "",
            f"📝 Eventos: {total_cambios}",
            f"🔴 Perdidas de BuyBox: {total_perdidas}",
        ]

        if top:
            lineas.append("")
            lineas.append("🏆 <b>Sellers que mas te ganan:</b>")
            for seller, conteo in top:
                sufijo = "es" if conteo > 1 else ""
                lineas.append(f"• {escapar(seller)} → {conteo} vez{sufijo}")

        if hora_pico is not None:
            lineas.append("")
            lineas.append(f"⏰ <b>Hora pico:</b> {hora_pico:02d}:00 hrs")

        return "\n".join(lineas)
    except Exception as exc:
        return f"⚠️ Error: {escapar(exc)}"


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
            alt_str += (
                f"\n  • {escapar(alternativo.get('seller', ''))}"
                f" → ${escapar(alternativo.get('precio', ''))}"
            )

    variante = " ".join(
        parte for parte in [item.get("color", ""), item.get("size", "")] if parte
    ).strip()

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


def monitorear():
    global ULTIMO_RESUMEN, ULTIMA_FECHA_CSV

    ganando = []
    perdiendo = []
    now_cdmx = datetime.now(CDMX_TZ)
    now_str = now_cdmx.strftime("%H:%M:%S")
    fecha_hora = now_cdmx.strftime("%Y-%m-%d %H:%M:%S")

    if not CATALOGO:
        print(f"[{now_str}] ⚠️ Catalogo vacio - sube el Excel desde el panel web")
        return

    for item in CATALOGO:
        sku = item["sku_patish"]
        if item["estado_oferta"] == "BLOQUEADA":
            limpiar_estado_item(sku, "BLOQUEADA")
        elif item["estado_oferta"] == "INACTIVA_STOCK":
            limpiar_estado_item(sku, "INACTIVA_STOCK")

    productos_agrupados = defaultdict(list)
    for item in CATALOGO:
        if item["estado_oferta"] == "ACTIVA":
            productos_agrupados[item["product_id"]].append(item)

    html_cache = {}
    for product_id, items_grupo in productos_agrupados.items():
        url_base = items_grupo[0]["url"]
        html_text = html_cache.get(product_id)
        if html_text is None:
            html_text = obtener_html(url_base)
            html_cache[product_id] = html_text

        data = extraer_next_data(html_text)
        variantes = extraer_variantes(data)
        mapa = {variante["skuId"]: variante for variante in variantes if variante.get("skuId")}

        if data and variantes:
            slugs = [slug for slug in find_deep(data, "slug") if isinstance(slug, str) and slug]
            slug = slugs[0] if slugs else None
            for item in items_grupo:
                variante_data = mapa.get(normalizar_identificador(item["sku_liverpool"]), {})
                if variante_data.get("color"):
                    item["color"] = variante_data["color"]
                if variante_data.get("size"):
                    item["size"] = variante_data["size"]
                if slug:
                    item["url"] = (
                        f"https://www.liverpool.com.mx/tienda/pdp/{slug}/{product_id}"
                        f"?skuid={item['sku_liverpool']}"
                    )

        for item in items_grupo:
            sku_patish = item["sku_patish"]
            sku_liverpool = normalizar_identificador(item["sku_liverpool"])

            variante_data = mapa.get(sku_liverpool)
            if not variante_data:
                seller_legacy, price_legacy, alternativos_legacy = extraer_buybox_legacy(html_text)
                if not seller_legacy:
                    print(f"  ⚠️ Sin BuyBox: {item['producto'][:35]}")
                    limpiar_estado_item(sku_patish, "SIN_DATOS")
                    continue
                variante_data = {
                    "buybox": {
                        "seller": seller_legacy,
                        "sellerId": "",
                        "precio": formatear_precio(price_legacy),
                    },
                    "otros_sellers": [
                        {
                            "seller": alternativo["seller"],
                            "precio": formatear_precio(alternativo["precio"]),
                        }
                        for alternativo in alternativos_legacy
                    ],
                    "hasValidOnlineInventory": "true",
                }

            buybox = variante_data.get("buybox")
            otros = variante_data.get("otros_sellers", [])

            if not buybox or not buybox.get("seller"):
                limpiar_estado_item(sku_patish, "INACTIVA_STOCK")
                continue

            seller = limpiar_texto(buybox.get("seller"))
            price = formatear_precio(buybox.get("precio"))
            seller_id = normalizar_identificador(buybox.get("sellerId"))
            es_mio = seller.lower() == MY_SELLER.lower() or (seller_id and seller_id == MY_SELLER_ID)
            nuevo_estado = "GANANDO" if es_mio else "PERDIDO"

            variante = " ".join(
                parte for parte in [item.get("color", ""), item.get("size", "")] if parte
            ).strip()
            if nuevo_estado == "GANANDO":
                ganando.append(f"• {item['producto'][:22]} {variante} → ${price}".strip())
            else:
                perdiendo.append(f"• {item['producto'][:18]} {variante} → {seller} → ${price}".strip())

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
                guardar_evento_csv(
                    fecha_hora,
                    item,
                    seller,
                    price,
                    nuevo_estado,
                    " | ".join(tipo_cambio),
                    otros,
                )

            if estado_anterior == "GANANDO" and nuevo_estado == "PERDIDO":
                enviar_alerta_perdida(item, seller, price, otros)

            if estado_anterior == "PERDIDO" and nuevo_estado == "GANANDO":
                enviar_telegram(
                    "✅ <b>RECUPERASTE BUYBOX</b>\n\n"
                    f"Producto: {escapar(item['producto'])}\n"
                    f"Variante: {escapar(variante or '-')}\n"
                    f"SKU PATISH: {escapar(sku_patish)} | Precio: ${escapar(price)}\n\n"
                    f"{escapar(item['url'])}"
                )

            ULTIMO_ESTADO[sku_patish] = nuevo_estado
            ULTIMO_PRECIO[sku_patish] = price
            ULTIMO_SELLER[sku_patish] = seller

    if time.time() - ULTIMO_RESUMEN >= 900:
        bloqueadas = sum(1 for item in CATALOGO if item["estado_oferta"] == "BLOQUEADA")
        mensaje = (
            "📊 <b>RESUMEN BUYBOX</b>\n\n"
            f"🕒 {now_str}\n\n"
            f"🟢 Ganando: {len(ganando)}\n"
            f"🔴 Perdiendo: {len(perdiendo)}\n"
            f"🟠 Bloqueadas Liverpool: {bloqueadas}\n"
        )
        if ganando:
            mensaje += "\n🟢 <b>GANANDO</b>\n" + "\n".join(escapar(item) for item in ganando[:10]) + "\n"
        if perdiendo:
            mensaje += "\n🔴 <b>PERDIENDO</b>\n" + "\n".join(escapar(item) for item in perdiendo[:10]) + "\n"
        if bloqueadas:
            mensaje += "\n⚠️ Usa /bloqueadas para ver detalles."
        enviar_telegram(mensaje)
        ULTIMO_RESUMEN = time.time()

    if now_cdmx.hour == 10 and now_cdmx.minute <= 2:
        fecha_actual = now_cdmx.strftime("%Y-%m-%d")
        if ULTIMA_FECHA_CSV != fecha_actual:
            enviar_csv_telegram()
            ULTIMA_FECHA_CSV = fecha_actual

    activas = sum(1 for item in CATALOGO if item["estado_oferta"] == "ACTIVA")
    print(f"[{now_str}] ✅ 🟢{len(ganando)} 🔴{len(perdiendo)} / {activas} activas / {len(CATALOGO)} total")


def loop_monitor():
    while True:
        try:
            procesar_comandos()
            monitorear()
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
                CATALOGO.append(
                    {
                        "sku_patish": limpiar_texto(row.get("sku_patish", "")),
                        "sku_liverpool": sku_liv,
                        "product_id": product_id,
                        "vgc": "",
                        "producto": limpiar_texto(row.get("nombre_producto", "")),
                        "estado_oferta": "ACTIVA",
                        "motivo": "",
                        "precio_base": None,
                        "cantidad": 0,
                        "url": url,
                        "color": "",
                        "size": "",
                    }
                )
        print(f"📂 {len(CATALOGO)} items cargados desde {source_file} (modo compatibilidad)")
        print("💡 Sube el Excel de Liverpool desde el panel web para activar estados bloqueada/sin stock")
    except Exception as exc:
        print(f"⚠️ No se pudo cargar {source_file}: {exc}")


if __name__ == "__main__":
    print("🔥 Monitor BuyBox PRO v3 iniciado")
    inicializar_csv()

    if not cargar_catalogo_persistido():
        cargar_catalogo_compatibilidad()

    threading.Thread(target=loop_monitor, daemon=True).start()

    enviar_telegram(
        "🚀 <b>Monitor BuyBox v3 iniciado</b>\n\n"
        "📌 Novedades:\n"
        "• Variantes por color/capacidad con menos requests\n"
        "• Detecta bloqueadas por Liverpool vs sin stock\n"
        "• Panel web para subir el Excel semanal\n\n"
        "Comandos:\n"
        "/estado · /bloqueadas · /reporte · /ayuda"
    )

    app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)
