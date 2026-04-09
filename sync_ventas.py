#!/usr/bin/env python3
"""
sync_ventas.py — Resuelve URLs de OneDrive en tu Mac y dispara el sync en Railway.

Uso:
  python3 sync_ventas.py                        # usa RAILWAY_URL del env o pregunta
  RAILWAY_URL=https://tu-app.railway.app python3 sync_ventas.py

Solo necesitas correr esto cuando quieras actualizar los datos de ventas.
No necesitas tocar Railway ni env vars.
"""
import os
import re
import sys
import json
import requests

# ── Configuración ─────────────────────────────────────────────────────────
RAILWAY_URL = os.getenv("RAILWAY_URL", "").strip().rstrip("/")

CREDITIENDA_SHARE = (
    "https://1drv.ms/x/c/38f7c930c20edbea/"
    "IQB1GeReIzuPSa329bVCAFqPAYFK7lKTDrwb1VXp33Wp-bc?e=M9FfyG"
)
LIVERPOOL_SHARE = (
    "https://1drv.ms/x/c/0db300c27ec53cb3/"
    "IQCzPMV-wgCzIIANhAAAAAAAAfkEc_5TCgOXUwlaASmmPzY?e=lETFJd"
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "es-MX,es;q=0.9",
}


# ── Helpers ───────────────────────────────────────────────────────────────
def resolve_onedrive(share_url: str, nombre: str) -> str | None:
    print(f"  Resolviendo {nombre}...", end=" ", flush=True)
    try:
        resp = requests.get(share_url, headers=HEADERS, timeout=30, allow_redirects=True)
        if resp.status_code != 200:
            print(f"HTTP {resp.status_code}")
            return None
        for pat in [r'"FileGetUrl"\s*:\s*"([^"]+)"', r'"downloadUrl"\s*:\s*"([^"]+)"']:
            m = re.search(pat, resp.text)
            if m:
                url = m.group(1).replace("\\u0026", "&").replace("\\/", "/")
                print("OK")
                return url
        print("no encontrada en HTML")
        return None
    except Exception as e:
        print(f"error: {e}")
        return None


def get_railway_url() -> str:
    url = RAILWAY_URL
    if not url:
        url = input("URL de tu app en Railway (ej: https://mi-app.railway.app): ").strip().rstrip("/")
    if not url.startswith("http"):
        print("URL inválida.")
        sys.exit(1)
    return url


# ── Main ──────────────────────────────────────────────────────────────────
def main():
    print("=" * 55)
    print("  PATISH · Sync Ventas")
    print("=" * 55)

    # 1. Resolver URLs de OneDrive localmente (funciona en Mac)
    print("\n[1/3] Resolviendo URLs de OneDrive...")
    cred_url = resolve_onedrive(CREDITIENDA_SHARE, "Creditienda")
    liv_url  = resolve_onedrive(LIVERPOOL_SHARE,   "Liverpool")

    if not cred_url and not liv_url:
        print("\nNo se pudo resolver ningún Excel. Verifica tu conexión.")
        sys.exit(1)

    # 2. Obtener URL de Railway
    railway = get_railway_url()
    endpoint = f"{railway}/api/ventas/sync"

    # 3. Llamar al servidor con las URLs resueltas
    print(f"\n[2/3] Llamando a {endpoint}...")
    payload = {}
    if cred_url: payload["creditienda_url"] = cred_url
    if liv_url:  payload["liverpool_url"]   = liv_url

    try:
        resp = requests.post(endpoint, json=payload, timeout=120)
        resp.raise_for_status()
        d = resp.json()
    except requests.exceptions.ConnectionError:
        print(f"\nNo se pudo conectar a {railway}. ¿Está corriendo la app?")
        sys.exit(1)
    except Exception as e:
        print(f"\nError llamando al servidor: {e}")
        sys.exit(1)

    # 4. Mostrar resultado
    print("\n[3/3] Resultado:")
    cred = d.get("creditienda", {})
    liv  = d.get("liverpool",   {})

    if cred.get("ok"):
        print(f"  ✓ Creditienda: {cred['registros_parseados']:,} registros, {cred['insertados']:,} nuevos, {cred['duplicados']:,} ya existían")
    else:
        print(f"  ✗ Creditienda: {cred.get('error', 'error desconocido')}")

    if liv.get("ok"):
        print(f"  ✓ Liverpool:   {liv['registros_parseados']:,} registros, {liv['insertados']:,} nuevos, {liv['duplicados']:,} ya existían")
    else:
        print(f"  ✗ Liverpool:   {liv.get('error', 'error desconocido')}")

    if d.get("ok"):
        print(f"\n  Abre {railway}/ventas para ver el dashboard.")
    else:
        print("\n  Ninguna fuente se sincronizó correctamente.")
        sys.exit(1)


if __name__ == "__main__":
    main()
