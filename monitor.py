import csv
import time
import requests
import re
import os
import json
import threading
import io
from datetime import datetime, timedelta, timezone
from collections import defaultdict

import pandas as pd
from flask import Flask, request, jsonify, render_template_string

# ================================
# CONFIG
# ================================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID        = os.getenv("CHAT_ID")
MY_SELLER      = "PATISH"
MY_SELLER_ID   = "2370"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "es-MX,es;q=0.9",
}

CDMX_TZ = timezone(timedelta(hours=-6))

ULTIMO_ESTADO   = {}
ULTIMO_PRECIO   = {}
ULTIMO_SELLER   = {}

CATALOGO = []

ULTIMO_RESUMEN   = 0
ULTIMA_FECHA_CSV = None

CSV_FILE  = "historico_buybox.csv"
SKUS_FILE = "skus.csv"
PORT      = int(os.getenv("PORT", "8080"))

# ================================
# FLASK APP
# ================================
app = Flask(__name__)

HTML_PANEL = """<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>BuyBox Monitor v3 — PATISH</title>
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
  <div class="stat g"><div class="num" id="sg">—</div><div class="lbl">Ganando BuyBox</div></div>
  <div class="stat r"><div class="num" id="sp">—</div><div class="lbl">Perdiendo</div></div>
  <div class="stat o"><div class="num" id="sb">—</div><div class="lbl">Bloqueadas</div></div>
  <div class="stat gr"><div class="num" id="ss">—</div><div class="lbl">Sin stock</div></div>
  <div class="stat y"><div class="num" id="st">—</div><div class="lbl">Total variantes</div></div>
</div>

<main>

  <div class="sec">Actualizar catálogo</div>
  <div class="upload-bar">
    <h3>Subir reporte de ofertas — cada jueves</h3>
    <p>Descarga el "Reporte_de_ofertas_actuales.xlsx" de Liverpool y súbelo aquí. El sistema detecta automáticamente variantes activas, sin stock y bloqueadas por Liverpool.</p>
    <div class="upload-row">
      <label class="file-label" for="excel-input">📂 Elegir archivo</label>
      <input type="file" id="excel-input" accept=".xlsx,.xls">
      <span class="file-name" id="file-name-lbl">Ningún archivo seleccionado</span>
      <button class="btn bp" onclick="subirCatalogo()">Actualizar catálogo</button>
    </div>
    <div class="msg" id="msg-upload"></div>
  </div>

  <div class="sec">Estado actual <span class="sec-count" id="count-visible">—</span></div>
  <div class="filters">
    <button class="filter-btn active" onclick="setFiltro('TODOS',this)">Todos</button>
    <button class="filter-btn" onclick="setFiltro('GANANDO',this)">🟢 Ganando</button>
    <button class="filter-btn" onclick="setFiltro('PERDIDO',this)">🔴 Perdiendo</button>
    <button class="filter-btn" onclick="setFiltro('BLOQUEADA',this)">🟠 Bloqueadas</button>
    <button class="filter-btn" onclick="setFiltro('INACTIVA_STOCK',this)">⚫ Sin stock</button>
  </div>

  <div class="tw">
    <table>
      <thead>
        <tr>
          <th>Producto</th><th>Color</th><th>Tamaño</th><th>SKU PATISH</th><th>SKU Liverpool</th>
          <th>Estado</th><th>Seller BuyBox</th><th>Precio BuyBox</th><th>Tu precio</th><th>Diferencia</th><th>URL</th>
        </tr>
      </thead>
      <tbody id="tbody-estado">
        <tr><td colspan="11" style="color:var(--muted);text-align:center;padding:32px">Cargando…</td></tr>
      </tbody>
    </table>
  </div>
  <div class="ts" id="refresh-ts">—</div>

</main>

<script>
let filtroActual='TODOS', todosItems=[];

document.getElementById('excel-input').addEventListener('change',function(){
  document.getElementById('file-name-lbl').textContent=this.files[0]?.name||'Ningún archivo';
});

async function subirCatalogo(){
  const input=document.getElementById('excel-input');
  const msg=document.getElementById('msg-upload');
  if(!input.files[0]){msg.className='msg err';msg.textContent='⚠ Selecciona el archivo primero.';return;}
  const fd=new FormData();fd.append('file',input.files[0]);
  msg.className='msg';msg.textContent='⏳ Procesando…';
  const d=await(await fetch('/api/catalogo',{method:'POST',body:fd})).json();
  if(d.ok){
    msg.className='msg ok';
    msg.textContent=`✅ ${d.total} variantes cargadas — ${d.activas} activas · ${d.inactivas_stock} sin stock · ${d.bloqueadas} bloqueadas por Liverpool`;
    cargarEstado();
  }else{msg.className='msg err';msg.textContent='❌ '+(d.error||'Error');}
}

function setFiltro(f,btn){
  filtroActual=f;
  document.querySelectorAll('.filter-btn').forEach(b=>b.classList.remove('active'));
  btn.classList.add('active');
  renderTabla();
}

function renderTabla(){
  const items=filtroActual==='TODOS'?todosItems:todosItems.filter(i=>i.estado===filtroActual);
  document.getElementById('count-visible').textContent=items.length;
  const tbody=document.getElementById('tbody-estado');
  if(!items.length){tbody.innerHTML='<tr><td colspan="11" style="color:var(--muted);text-align:center;padding:28px">Sin datos para este filtro</td></tr>';return;}
  tbody.innerHTML=items.map(p=>{
    const bc=p.estado==='GANANDO'?'bw':p.estado==='PERDIDO'?'bl':p.estado==='BLOQUEADA'?'bo':'bn';
    const bt=p.estado==='GANANDO'?'GANANDO':p.estado==='PERDIDO'?'PERDIDO':p.estado==='BLOQUEADA'?'BLOQUEADA ⚠':'SIN STOCK';
    let diff='';
    if(p.precio_buybox&&p.precio_patish){
      const d=parseFloat(p.precio_buybox)-parseFloat(p.precio_patish);
      if(!isNaN(d))diff=d<0?`<span class="diff-neg">-$${Math.abs(d).toFixed(0)}</span>`:d>0?`<span class="diff-pos">+$${d.toFixed(0)}</span>`:'<span style="color:var(--muted)">igual</span>';
    }
    return `<tr>
      <td style="max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${p.producto}">${p.producto}</td>
      <td>${p.color||'—'}</td><td>${p.size||'—'}</td>
      <td style="font-size:.65rem">${p.sku_patish}</td>
      <td style="font-size:.65rem">${p.sku_liverpool}</td>
      <td><span class="badge ${bc}">${bt}</span></td>
      <td>${p.seller_buybox||'—'}</td>
      <td>${p.precio_buybox?'$'+p.precio_buybox:'—'}</td>
      <td>${p.precio_patish?'$'+p.precio_patish:'—'}</td>
      <td>${diff||'—'}</td>
      <td>${p.url?`<a class="lnk" href="${p.url}" target="_blank">↗ ver</a>`:'—'}</td>
    </tr>`;
  }).join('');
}

async function cargarEstado(){
  const d=await(await fetch('/api/estado')).json();
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
setInterval(cargarEstado,30000);
</script>
</body>
</html>"""


@app.route("/")
def panel():
    return render_template_string(HTML_PANEL)


@app.route("/api/catalogo", methods=["POST"])
def api_catalogo_post():
    if 'file' not in request.files:
        return jsonify({"ok": False, "error": "No se recibió archivo"}), 400
    f = request.files['file']
    try:
        df = pd.read_excel(io.BytesIO(f.read()))
        df.columns = [c.strip() for c in df.columns]

        nuevos = []
        for _, row in df.iterrows():
            sku_oferta    = str(row.get('SKU de oferta', '')).strip()
            sku_producto  = str(row.get('SKU de producto', '')).strip()
            vgc           = str(row.get('VGC', '')).strip() if pd.notna(row.get('VGC')) else ''
            producto      = str(row.get('Producto', '')).strip()
            estado_oferta = str(row.get('Estado de oferta', '')).strip().upper()
            motivo_raw    = row.get('Motivo oferta inactiva ')
            motivo        = str(motivo_raw).strip() if pd.notna(motivo_raw) else ''
            precio_base   = row.get('Precio base')
            cantidad_raw  = row.get('Cantidad ')
            cantidad      = int(cantidad_raw) if pd.notna(cantidad_raw) else 0

            if sku_oferta.startswith('Eliminado_') or not sku_oferta or not sku_producto:
                continue

            # Clasificar estado
            if 'Restricción de oferta' in motivo:
                estado_inicial = 'BLOQUEADA'
            elif estado_oferta == 'ACTIVA':
                estado_inicial = 'ACTIVA'
            else:
                estado_inicial = 'INACTIVA_STOCK'

            # product_id para la URL: VGC si es numérico largo, si no el sku_producto
            vgc_limpio = re.sub(r'[^0-9]', '', vgc)
            product_id = vgc_limpio if len(vgc_limpio) >= 8 else sku_producto
            url = f"https://www.liverpool.com.mx/tienda/pdp/producto/{product_id}?skuid={sku_producto}"

            nuevos.append({
                'sku_patish':    sku_oferta,
                'sku_liverpool': sku_producto,
                'product_id':    product_id,
                'vgc':           vgc,
                'producto':      producto,
                'estado_oferta': estado_inicial,
                'motivo':        motivo,
                'precio_base':   float(precio_base) if pd.notna(precio_base) else None,
                'cantidad':      cantidad,
                'url':           url,
                'color':         '',
                'size':          '',
            })

        global CATALOGO
        CATALOGO = nuevos
        guardar_skus_csv(nuevos)

        activas          = sum(1 for n in nuevos if n['estado_oferta'] == 'ACTIVA')
        inactivas_stock  = sum(1 for n in nuevos if n['estado_oferta'] == 'INACTIVA_STOCK')
        bloqueadas       = sum(1 for n in nuevos if n['estado_oferta'] == 'BLOQUEADA')

        if bloqueadas > 0:
            lista = [n for n in nuevos if n['estado_oferta'] == 'BLOQUEADA']
            msg   = f"🚨 <b>OFERTAS BLOQUEADAS POR LIVERPOOL</b>\n\n{bloqueadas} variantes bloqueadas en tu catálogo:\n\n"
            for n in lista[:10]:
                msg += f"• {n['producto'][:40]}\n  SKU: {n['sku_patish']} | {n['motivo']}\n\n"
            if bloqueadas > 10:
                msg += f"...y {bloqueadas-10} más. Ver panel web."
            enviar_telegram(msg)

        return jsonify({"ok": True, "total": len(nuevos), "activas": activas,
                        "inactivas_stock": inactivas_stock, "bloqueadas": bloqueadas})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/estado")
def api_estado():
    items = []
    for v in CATALOGO:
        sku = v['sku_patish']
        estado_monitor = ULTIMO_ESTADO.get(sku, '')
        if not estado_monitor:
            estado_final = v['estado_oferta'] if v['estado_oferta'] != 'ACTIVA' else 'SIN_DATOS'
        else:
            estado_final = estado_monitor
        items.append({
            'sku_patish':    sku,
            'sku_liverpool': v['sku_liverpool'],
            'producto':      v['producto'],
            'color':         v.get('color', ''),
            'size':          v.get('size', ''),
            'estado':        estado_final,
            'seller_buybox': ULTIMO_SELLER.get(sku, ''),
            'precio_buybox': ULTIMO_PRECIO.get(sku, ''),
            'precio_patish': v.get('precio_base'),
            'url':           v['url'],
        })
    return jsonify({
        "ganando":   sum(1 for i in items if i['estado'] == 'GANANDO'),
        "perdidos":  sum(1 for i in items if i['estado'] == 'PERDIDO'),
        "bloqueadas":sum(1 for i in items if i['estado'] == 'BLOQUEADA'),
        "sin_stock": sum(1 for i in items if i['estado'] == 'INACTIVA_STOCK'),
        "total":     len(items),
        "items":     items,
    })


@app.route("/status")
def status():
    return jsonify({
        "ganando":  sum(1 for v in ULTIMO_ESTADO.values() if v == 'GANANDO'),
        "perdidos": sum(1 for v in ULTIMO_ESTADO.values() if v == 'PERDIDO'),
        "total":    len(CATALOGO),
        "ts":       datetime.now(CDMX_TZ).strftime("%Y-%m-%d %H:%M:%S"),
    })


# ================================
# CSV HELPERS
# ================================
def guardar_skus_csv(items):
    with open(SKUS_FILE, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=['sku', 'url', 'tu_nombre_seller', 'nombre_producto', 'sku_patish'])
        w.writeheader()
        for i in items:
            w.writerow({'sku': i['sku_liverpool'], 'url': i['url'],
                        'tu_nombre_seller': MY_SELLER, 'nombre_producto': i['producto'],
                        'sku_patish': i['sku_patish']})


def inicializar_csv():
    if not os.path.exists(CSV_FILE):
        with open(CSV_FILE, 'w', newline='', encoding='utf-8') as f:
            csv.writer(f).writerow([
                'fecha_hora', 'sku_patish', 'sku_liverpool', 'producto',
                'color', 'size', 'seller_ganador', 'precio', 'estado',
                'tipo_cambio', 'url', 'sellers_alternativos',
            ])
        print("📝 CSV histórico inicializado")


def guardar_evento_csv(fecha_hora, item, seller, price, estado, tipo_cambio, alternativos=None):
    with open(CSV_FILE, 'a', newline='', encoding='utf-8') as f:
        csv.writer(f).writerow([
            fecha_hora, item['sku_patish'], item['sku_liverpool'],
            item['producto'], item.get('color',''), item.get('size',''),
            seller, price, estado, tipo_cambio, item['url'],
            json.dumps(alternativos or [], ensure_ascii=False),
        ])


# ================================
# EXTRACCIÓN BUYBOX
# ================================
def obtener_html(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        if r.status_code == 200:
            return r.text
        print(f"  HTTP {r.status_code}: {url[:70]}")
        return None
    except Exception as e:
        print(f"  Error HTTP: {e}")
        return None


def extraer_next_data(html):
    if not html:
        return None
    m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
    if not m:
        return None
    try:
        return json.loads(m.group(1))
    except Exception:
        return None


def find_deep(obj, key, results=None):
    if results is None:
        results = []
    if not obj or not isinstance(obj, (dict, list)):
        return results
    if isinstance(obj, dict):
        if key in obj:
            results.append(obj[key])
        for v in obj.values():
            find_deep(v, key, results)
    elif isinstance(obj, list):
        for v in obj:
            find_deep(v, key, results)
    return results


def extraer_variantes(data):
    if not data:
        return []
    variants_blocks = find_deep(data, 'variants')
    if not variants_blocks:
        return []
    biggest = sorted(variants_blocks, key=lambda x: len(x) if isinstance(x, list) else 0, reverse=True)[0]
    if not isinstance(biggest, list):
        return []

    resultado = []
    for v in biggest:
        if not isinstance(v, dict):
            continue
        offers_obj = v.get('offers', {})
        offers_arr = []
        best_offer = None
        if isinstance(offers_obj, dict):
            offers_arr = offers_obj.get('offers', [])
            if not isinstance(offers_arr, list):
                offers_arr = []
            best_offer = offers_obj.get('bestOffer')

        ganador = None
        if best_offer and isinstance(best_offer, dict):
            ganador = {'seller': best_offer.get('sellerName',''),
                       'sellerId': best_offer.get('sellerId',''),
                       'precio': best_offer.get('salePrice','')}
        elif offers_arr:
            o = offers_arr[0]
            ganador = {'seller': o.get('sellerName',''),
                       'sellerId': o.get('sellerId',''),
                       'precio': o.get('salePrice','')}

        otros = [{'seller': o.get('sellerName',''), 'precio': o.get('salePrice','')}
                 for o in offers_arr[1:] if isinstance(o, dict)][:4]

        resultado.append({
            'skuId':                   str(v.get('skuId', '')),
            'color':                   v.get('color', ''),
            'size':                    v.get('size', ''),
            'hasValidOnlineInventory': str(v.get('hasValidOnlineInventory', 'false')).lower(),
            'sellersCount':            v.get('sellersCount', 0),
            'buybox':                  ganador,
            'otros_sellers':           otros,
        })
    return resultado


def extraer_buybox_legacy(html):
    """Fallback regex para productos sin variantes en __NEXT_DATA__."""
    if not html:
        return None, None, []
    seller = price = None
    for patron in [
        r'"bestOffer"\s*:\s*\{[^{}]*?"salePrice"\s*:\s*"?(\d+(?:\.\d+)?)"?[^{}]*?"sellerName"\s*:\s*"([^"]+)"',
        r'"bestOffer"\s*:\s*\{[^{}]*?"sellerName"\s*:\s*"([^"]+)"[^{}]*?"salePrice"\s*:\s*"?(\d+(?:\.\d+)?)"?',
    ]:
        m = re.search(patron, html, re.DOTALL)
        if m:
            price, seller = (m.group(1), m.group(2)) if 'salePrice' in patron.split('sellerName')[0] else (m.group(2), m.group(1))
            break
    if not seller:
        return None, None, []
    alternativos = []
    seen = {seller}
    for m in re.finditer(r'"sellerName"\s*:\s*"([^"]+)"[^}]{0,200}?"salePrice"\s*:\s*"?(\d+(?:\.\d+)?)"?', html):
        if m.group(1) not in seen:
            seen.add(m.group(1))
            alternativos.append({"seller": m.group(1), "precio": m.group(2)})
        if len(alternativos) >= 4:
            break
    return seller, price, alternativos


# ================================
# TELEGRAM
# ================================
def enviar_telegram(mensaje):
    if not TELEGRAM_TOKEN or not CHAT_ID:
        return
    try:
        r = requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                          json={"chat_id": CHAT_ID, "text": mensaje, "parse_mode": "HTML"}, timeout=15)
        if not r.ok:
            print(f"Telegram {r.status_code}: {r.text[:100]}")
    except Exception as e:
        print(f"Error Telegram: {e}")


def enviar_telegram_a(chat_id, mensaje):
    if not TELEGRAM_TOKEN:
        return
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                      json={"chat_id": chat_id, "text": mensaje, "parse_mode": "HTML"}, timeout=15)
    except Exception as e:
        print(f"Error Telegram: {e}")


def enviar_csv_telegram():
    if not TELEGRAM_TOKEN or not CHAT_ID or not os.path.exists(CSV_FILE):
        return
    try:
        with open(CSV_FILE, 'rb') as f:
            requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendDocument",
                          data={"chat_id": CHAT_ID, "caption": "📊 Histórico BuyBox diario"},
                          files={"document": (CSV_FILE, f, "text/csv")}, timeout=30)
        print("📎 CSV enviado")
    except Exception as e:
        print(f"Error CSV: {e}")


# ================================
# COMANDOS TELEGRAM
# ================================
_ultimo_update_id = 0

def procesar_comandos():
    global _ultimo_update_id
    if not TELEGRAM_TOKEN:
        return
    try:
        r = requests.get(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates",
                         params={"offset": _ultimo_update_id + 1, "timeout": 2}, timeout=10)
        if not r.ok:
            return
        for update in r.json().get("result", []):
            _ultimo_update_id = update["update_id"]
            msg     = update.get("message", {})
            texto   = msg.get("text", "").strip()
            chat_id = str(msg.get("chat", {}).get("id", ""))
            if not texto.startswith("/"):
                continue
            cmd = texto.split()[0].lower()
            if cmd == "/reporte":
                enviar_telegram_a(chat_id, generar_reporte_historico())
            elif cmd == "/estado":
                enviar_telegram_a(chat_id, generar_estado_actual())
            elif cmd == "/bloqueadas":
                enviar_telegram_a(chat_id, generar_reporte_bloqueadas())
            elif cmd == "/ayuda":
                enviar_telegram_a(chat_id,
                    "📋 <b>Comandos disponibles</b>\n\n"
                    "/estado — Resumen en tiempo real\n"
                    "/bloqueadas — Ofertas bloqueadas por Liverpool\n"
                    "/reporte — Análisis de patrones\n"
                    "/ayuda — Ayuda")
    except Exception as e:
        print(f"Error comandos: {e}")


# ================================
# REPORTES
# ================================
def generar_estado_actual():
    if not CATALOGO:
        return "⚠️ Catálogo vacío. Sube el Excel de Liverpool desde el panel web."
    ganando   = [v for v in CATALOGO if ULTIMO_ESTADO.get(v['sku_patish']) == 'GANANDO']
    perdiendo = [v for v in CATALOGO if ULTIMO_ESTADO.get(v['sku_patish']) == 'PERDIDO']
    bloqueadas= [v for v in CATALOGO if v['estado_oferta'] == 'BLOQUEADA']
    now = datetime.now(CDMX_TZ).strftime("%H:%M:%S")
    lineas = [f"📡 <b>ESTADO ACTUAL</b> — {now}\n",
              f"🟢 Ganando: {len(ganando)}",
              f"🔴 Perdiendo: {len(perdiendo)}",
              f"🟠 Bloqueadas Liverpool: {len(bloqueadas)}",
              f"📦 Total catálogo: {len(CATALOGO)}"]
    if ganando:
        lineas.append("\n🟢 <b>GANANDO</b>")
        for v in ganando[:10]:
            lineas.append(f"• {v['producto'][:22]} {v.get('color','')} {v.get('size','')} → ${ULTIMO_PRECIO.get(v['sku_patish'],'?')}")
    if perdiendo:
        lineas.append("\n🔴 <b>PERDIENDO</b>")
        for v in perdiendo[:10]:
            lineas.append(f"• {v['producto'][:18]} {v.get('color','')} → {ULTIMO_SELLER.get(v['sku_patish'],'?')} → ${ULTIMO_PRECIO.get(v['sku_patish'],'?')}")
    return "\n".join(lineas)


def generar_reporte_bloqueadas():
    bloqueadas = [v for v in CATALOGO if v['estado_oferta'] == 'BLOQUEADA']
    if not bloqueadas:
        return "✅ No hay ofertas bloqueadas por Liverpool."
    lineas = [f"🟠 <b>BLOQUEADAS POR LIVERPOOL</b> ({len(bloqueadas)})\n"]
    for v in bloqueadas[:20]:
        lineas.append(f"• {v['producto'][:35]}\n  SKU: {v['sku_patish']} | {v.get('motivo','?')}")
    if len(bloqueadas) > 20:
        lineas.append(f"\n...y {len(bloqueadas)-20} más.")
    return "\n".join(lineas)


def generar_reporte_historico():
    if not os.path.exists(CSV_FILE):
        return "⚠️ No hay histórico todavía."
    sellers_count = defaultdict(int)
    perdidas_por_hora = defaultdict(int)
    total_cambios = total_perdidas = 0
    try:
        with open(CSV_FILE, newline='', encoding='utf-8') as f:
            for row in csv.DictReader(f):
                tipo = row.get("tipo_cambio", "")
                total_cambios += 1
                if "CAMBIO_ESTADO" in tipo and row.get("estado") == "PERDIDO":
                    total_perdidas += 1
                    sellers_count[row.get("seller_ganador","")] += 1
                    try:
                        perdidas_por_hora[int(row.get("fecha_hora","")[11:13])] += 1
                    except Exception:
                        pass
        if not total_cambios:
            return "📊 Histórico vacío aún."
        top    = sorted(sellers_count.items(), key=lambda x: x[1], reverse=True)[:5]
        h_pico = max(perdidas_por_hora, key=perdidas_por_hora.get) if perdidas_por_hora else None
        lineas = ["📊 <b>REPORTE DE PATRONES</b>\n",
                  f"📝 Eventos: {total_cambios}",
                  f"🔴 Pérdidas de BuyBox: {total_perdidas}\n"]
        if top:
            lineas.append("🏆 <b>Sellers que más te ganan:</b>")
            for s, c in top:
                lineas.append(f"  • {s} → {c} vez{'es' if c>1 else ''}")
        if h_pico is not None:
            lineas.append(f"\n⏰ <b>Hora pico:</b> {h_pico:02d}:00 hrs")
        return "\n".join(lineas)
    except Exception as e:
        return f"⚠️ Error: {e}"


# ================================
# ALERTAS
# ================================
def enviar_alerta_perdida(item, seller, price, otros):
    diff_str = ""
    precio_ant = ULTIMO_PRECIO.get(item['sku_patish'])
    if precio_ant:
        try:
            diff = float(price) - float(precio_ant)
            diff_str = f"\n💸 Diferencia: ${abs(diff):.0f} ({'más barato' if diff<0 else 'más caro'})"
        except Exception:
            pass
    alt_str = ""
    if otros:
        alt_str = "\n\n👥 <b>Otros oferentes:</b>"
        for a in otros[:3]:
            alt_str += f"\n  • {a['seller']} → ${a['precio']}"
    variante = f"{item.get('color','')} {item.get('size','')}".strip()
    enviar_telegram(
        f"🚨 <b>PERDISTE BUYBOX</b>\n\n"
        f"Producto: {item['producto']}\n"
        f"Variante: {variante or '—'}\n"
        f"SKU PATISH: {item['sku_patish']}\n"
        f"SKU Liverpool: {item['sku_liverpool']}\n"
        f"Seller: <b>{seller}</b>\n"
        f"Precio: ${price}{diff_str}{alt_str}\n\n"
        f"{item['url']}"
    )


# ================================
# CICLO DE MONITOREO
# ================================
_html_cache = {}

def monitorear():
    global ULTIMO_RESUMEN, ULTIMA_FECHA_CSV, _html_cache

    ganando = perdiendo = []
    ganando   = []
    perdiendo = []
    now_cdmx   = datetime.now(CDMX_TZ)
    now_str    = now_cdmx.strftime("%H:%M:%S")
    fecha_hora = now_cdmx.strftime("%Y-%m-%d %H:%M:%S")

    if not CATALOGO:
        print(f"[{now_str}] ⚠️  Catálogo vacío — sube el Excel desde el panel web")
        return

    _html_cache = {}

    # Registrar bloqueadas e inactivas en estado en memoria
    for item in CATALOGO:
        sku = item['sku_patish']
        if item['estado_oferta'] == 'BLOQUEADA':
            ULTIMO_ESTADO[sku] = 'BLOQUEADA'
            continue
        if item['estado_oferta'] == 'INACTIVA_STOCK':
            ULTIMO_ESTADO[sku] = 'INACTIVA_STOCK'
            continue

    # Agrupar activas por product_id — 1 request por producto base
    productos_agrupados = defaultdict(list)
    for item in CATALOGO:
        if item['estado_oferta'] == 'ACTIVA':
            productos_agrupados[item['product_id']].append(item)

    for product_id, items_grupo in productos_agrupados.items():
        url_base = items_grupo[0]['url']
        html = _html_cache.get(product_id) or obtener_html(url_base)
        if html:
            _html_cache[product_id] = html

        data      = extraer_next_data(html)
        variantes = extraer_variantes(data)

        # Mapa skuId -> datos de variante
        mapa = {v['skuId']: v for v in variantes}

        # Actualizar color/size y URL con slug si están disponibles
        if data and variantes:
            slugs = find_deep(data, 'slug')
            slug  = slugs[0] if slugs else None
            for item in items_grupo:
                vd = mapa.get(item['sku_liverpool'], {})
                if vd.get('color'):
                    item['color'] = vd['color']
                if vd.get('size'):
                    item['size'] = vd['size']
                if slug:
                    item['url'] = f"https://www.liverpool.com.mx/tienda/pdp/{slug}/{product_id}?skuid={item['sku_liverpool']}"

        for item in items_grupo:
            sku     = item['sku_patish']
            sku_liv = item['sku_liverpool']

            variante_data = mapa.get(sku_liv)

            if not variante_data:
                # Fallback legacy para productos sin variantes claras
                seller_l, price_l, alt_l = extraer_buybox_legacy(html)
                if not seller_l:
                    print(f"  ⚠️  Sin BuyBox: {item['producto'][:35]}")
                    continue
                variante_data = {
                    'buybox': {'seller': seller_l, 'sellerId': '', 'precio': price_l},
                    'otros_sellers': [{'seller': a['seller'], 'precio': a['precio']} for a in alt_l],
                    'hasValidOnlineInventory': 'true',
                }

            buybox = variante_data.get('buybox')
            otros  = variante_data.get('otros_sellers', [])

            if not buybox or not buybox.get('seller'):
                ULTIMO_ESTADO[sku] = 'INACTIVA_STOCK'
                continue

            seller = buybox['seller']
            price  = buybox['precio']
            es_mio = (seller.strip().lower() == MY_SELLER.lower() or
                      buybox.get('sellerId') == MY_SELLER_ID)

            nuevo_estado = 'GANANDO' if es_mio else 'PERDIDO'
            variante     = f"{item.get('color','')} {item.get('size','')}".strip()

            if nuevo_estado == 'GANANDO':
                ganando.append(f"• {item['producto'][:22]} {variante} → ${price}")
            else:
                perdiendo.append(f"• {item['producto'][:18]} {variante} → {seller} → ${price}")

            estado_ant = ULTIMO_ESTADO.get(sku)
            precio_ant = ULTIMO_PRECIO.get(sku)
            seller_ant = ULTIMO_SELLER.get(sku)

            tipo_cambio = []
            if estado_ant is None:
                tipo_cambio.append("INICIAL")
            else:
                if nuevo_estado != estado_ant:
                    tipo_cambio.append("CAMBIO_ESTADO")
                if str(price) != str(precio_ant):
                    tipo_cambio.append("CAMBIO_PRECIO")
                if seller != seller_ant:
                    tipo_cambio.append("CAMBIO_SELLER")

            if tipo_cambio:
                guardar_evento_csv(fecha_hora, item, seller, price, nuevo_estado,
                                   " | ".join(tipo_cambio), otros)

            if estado_ant == 'GANANDO' and nuevo_estado == 'PERDIDO':
                enviar_alerta_perdida(item, seller, price, otros)

            if estado_ant == 'PERDIDO' and nuevo_estado == 'GANANDO':
                enviar_telegram(
                    f"✅ <b>RECUPERASTE BUYBOX</b>\n\n"
                    f"Producto: {item['producto']}\n"
                    f"Variante: {variante or '—'}\n"
                    f"SKU PATISH: {sku} | Precio: ${price}\n\n{item['url']}"
                )

            ULTIMO_ESTADO[sku] = nuevo_estado
            ULTIMO_PRECIO[sku] = price
            ULTIMO_SELLER[sku] = seller

    # ── Resumen cada 15 min ──
    if time.time() - ULTIMO_RESUMEN >= 900:
        bloq = sum(1 for v in CATALOGO if v['estado_oferta'] == 'BLOQUEADA')
        msg  = (f"📊 <b>RESUMEN BUYBOX</b>\n\n🕒 {now_str}\n\n"
                f"🟢 Ganando: {len(ganando)}\n"
                f"🔴 Perdiendo: {len(perdiendo)}\n"
                f"🟠 Bloqueadas Liverpool: {bloq}\n")
        if ganando:
            msg += "\n🟢 <b>GANANDO</b>\n" + "\n".join(ganando[:10]) + "\n"
        if perdiendo:
            msg += "\n🔴 <b>PERDIENDO</b>\n" + "\n".join(perdiendo[:10]) + "\n"
        if bloq:
            msg += "\n⚠️ Usa /bloqueadas para ver detalles."
        enviar_telegram(msg)
        ULTIMO_RESUMEN = time.time()

    # ── CSV diario 10:00 AM CDMX ──
    if now_cdmx.hour == 10 and now_cdmx.minute <= 2:
        fecha_actual = now_cdmx.strftime("%Y-%m-%d")
        if ULTIMA_FECHA_CSV != fecha_actual:
            enviar_csv_telegram()
            ULTIMA_FECHA_CSV = fecha_actual

    activas_n = len([v for v in CATALOGO if v['estado_oferta'] == 'ACTIVA'])
    print(f"[{now_str}] ✅  🟢{len(ganando)} 🔴{len(perdiendo)} / {activas_n} activas / {len(CATALOGO)} total")


# ================================
# LOOP EN THREAD
# ================================
def loop_monitor():
    while True:
        try:
            procesar_comandos()
            monitorear()
        except Exception as e:
            print(f"💥 Error: {e}")
            enviar_telegram(f"⚠️ Error en ciclo: {e}")
        time.sleep(120)


# ================================
# MAIN
# ================================
if __name__ == "__main__":
    print("🔥 Monitor BuyBox PRO v3 iniciado")
    inicializar_csv()

    # Compatibilidad: cargar skus.csv anterior si existe y no hay catálogo
    if os.path.exists(SKUS_FILE) and not CATALOGO:
        try:
            with open(SKUS_FILE, newline='', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                if reader.fieldnames:
                    reader.fieldnames = [c.strip().lower() for c in reader.fieldnames]
                for row in reader:
                    sku_liv = row.get('sku','').strip()
                    url     = row.get('url','').strip()
                    if sku_liv and url:
                        pid = url.split('/')[-1].split('?')[0]
                        CATALOGO.append({
                            'sku_patish':    row.get('sku_patish','').strip(),
                            'sku_liverpool': sku_liv,
                            'product_id':    pid,
                            'vgc':           '',
                            'producto':      row.get('nombre_producto','').strip(),
                            'estado_oferta': 'ACTIVA',
                            'motivo':        '',
                            'precio_base':   None,
                            'cantidad':      0,
                            'url':           url,
                            'color':         '',
                            'size':          '',
                        })
            print(f"📂 {len(CATALOGO)} items cargados desde skus.csv (modo compatibilidad)")
            print("💡 Sube el Excel de Liverpool desde el panel web para activar todas las funciones v3")
        except Exception as e:
            print(f"⚠️  No se pudo cargar skus.csv: {e}")

    threading.Thread(target=loop_monitor, daemon=True).start()

    enviar_telegram(
        "🚀 <b>Monitor BuyBox v3 iniciado</b>\n\n"
        "📌 Novedades:\n"
        "• Variantes por color/capacidad — 1 request por producto\n"
        "• Detecta bloqueadas por Liverpool vs sin stock\n"
        "• Sube tu Excel del jueves desde el panel web\n\n"
        "Comandos:\n"
        "/estado · /bloqueadas · /reporte · /ayuda"
    )

    app.run(host="0.0.0.0", port=PORT, debug=False)
