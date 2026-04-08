"""
CRM / Cartera module — Daily Sistema de Gestión
Fuentes: Siigo (clientes + facturas), Excel (datos contacto), Google Sheets (movimientos bancarios)
"""

import os
import json
import re
import io
import requests
from datetime import date, datetime, timedelta
from typing import Optional

from fastapi import APIRouter, HTTPException, UploadFile, File, BackgroundTasks
from pydantic import BaseModel

from database import get_conn
from siigo import get_token, _headers as siigo_headers, SIIGO_BASE, _paginate

router = APIRouter(prefix="/crm")

# In-memory status for long-running sync jobs (single Railway instance)
_sync_jobs: dict = {
    "siigo": {"running": False, "ok": None, "msg": "", "step": "", "result": None}
}

# ═══════════════════════════════════════════════════════════════
# CONFIG helpers
# ═══════════════════════════════════════════════════════════════

def _get_config(key: str, default: str = "") -> str:
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT valor FROM config_planta WHERE parametro = %s", (key,))
        row = cur.fetchone()
        cur.close(); conn.close()
        return row[0] if row else default
    except Exception:
        return default

def _set_config(key: str, value: str, desc: str = ""):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO config_planta (parametro, valor, descripcion)
        VALUES (%s, %s, %s)
        ON CONFLICT (parametro) DO UPDATE SET valor = EXCLUDED.valor
    """, (key, value, desc))
    conn.commit()
    cur.close(); conn.close()

def _modo_prueba() -> bool:
    return _get_config("crm_modo_prueba", "true").lower() in ("true", "1", "yes")

# ═══════════════════════════════════════════════════════════════
# GOOGLE SHEETS helper
# ═══════════════════════════════════════════════════════════════

SHEET_ID = "1TPNSQWHHZbGJNrDwVbiSXmWRnQNQNRX97ciSWjfEXwU"

# Tabs: banco -> (tab_name, banco_code)
SHEET_TABS = [
    ("BDB2024",         "BDB"),
    ("BDB2025",         "BDB"),
    ("BANCOLOMBIA 2025","BANCOLOMBIA"),
    ("BANCOLOMBIA 2024","BANCOLOMBIA"),
]

def _get_sheets_service():
    """Build Google Sheets service from env var or local credentials file."""
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")
    if creds_json:
        info = json.loads(creds_json)
    else:
        # Fall back to local file (dev)
        paths = [
            os.path.join(os.path.dirname(__file__), "credentials.json"),
            "/Users/cav/Proyectos/trackmeat/credentials.json",
        ]
        for p in paths:
            if os.path.exists(p):
                with open(p) as f:
                    info = json.load(f)
                break
        else:
            raise RuntimeError("Google credentials not found. Set GOOGLE_CREDENTIALS_JSON env var.")

    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    creds = service_account.Credentials.from_service_account_info(info, scopes=scopes)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def _clean_valor_sheet(v) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().replace("$", "").replace(",", "").replace(".", "").strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        try:
            # Try interpreting dots as thousands separators
            s2 = re.sub(r"[^0-9]", "", str(v).strip())
            return float(s2) if s2 else None
        except ValueError:
            return None


def _parse_date_sheet(v) -> Optional[date]:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    s = str(v).strip()
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%m/%d/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    return None


def _fetch_sheet_tab(service, tab_name: str) -> list:
    """Fetch all rows from a sheet tab. Returns list of row lists."""
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=SHEET_ID,
            range=f"{tab_name}!A1:J5000"
        ).execute()
        return result.get("values", [])
    except Exception as e:
        print(f"Error fetching tab {tab_name}: {e}")
        return []

# ═══════════════════════════════════════════════════════════════
# SIIGO CUSTOMERS — fetch all
# ═══════════════════════════════════════════════════════════════

def _fetch_siigo_customers() -> list:
    """Fetch all customers from Siigo, paginated."""
    all_results = []
    page = 1
    while True:
        resp = requests.get(
            f"{SIIGO_BASE}/customers",
            headers=siigo_headers(),
            params={"page": page, "page_size": 100},
            timeout=30
        )
        if resp.status_code == 429:
            import time; time.sleep(3)
            continue
        resp.raise_for_status()
        data = resp.json()
        results = data.get("results", [])
        all_results.extend(results)
        total = data.get("pagination", {}).get("total_results", 0)
        if len(all_results) >= total or not results:
            break
        page += 1
    return all_results


def _fetch_siigo_invoices_all() -> list:
    """Fetch all invoices from Siigo (last 3 years). Uses date chunking to avoid limits."""
    import time
    today = date.today()
    start = date(today.year - 3, 1, 1)
    all_invoices = []
    # Chunk by month to avoid hitting API limits
    cursor = start
    while cursor <= today:
        month_end = (cursor.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
        month_end = min(month_end, today)
        try:
            chunk = _paginate("/invoices", {
                "date_start": cursor.isoformat(),
                "date_end": month_end.isoformat(),
            }, max_pages=20)
            all_invoices.extend(chunk)
            time.sleep(0.2)
        except Exception as e:
            print(f"Error fetching invoices {cursor} - {month_end}: {e}")
        cursor = month_end + timedelta(days=1)
    return all_invoices

# ═══════════════════════════════════════════════════════════════
# ENDPOINTS — CONFIG
# ═══════════════════════════════════════════════════════════════

@router.get("/config")
def get_crm_config():
    modo_prueba = _modo_prueba()
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT tipo, fecha, registros, nuevos, actualizados FROM crm_sync_log ORDER BY fecha DESC LIMIT 1")
    last = cur.fetchone()
    cur.execute("SELECT COUNT(*) FROM crm_clientes")
    n_clientes = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM crm_facturas")
    n_facturas = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM movimientos_bancarios")
    n_movimientos = cur.fetchone()[0]
    cur.close(); conn.close()
    return {
        "modo_prueba": modo_prueba,
        "last_sync": {"tipo": last[0], "fecha": last[1].isoformat() if last else None, "registros": last[2]} if last else None,
        "stats": {"clientes": n_clientes, "facturas": n_facturas, "movimientos": n_movimientos}
    }


class ConfigIn(BaseModel):
    modo_prueba: bool

@router.put("/config")
def set_crm_config(data: ConfigIn):
    _set_config("crm_modo_prueba", str(data.modo_prueba).lower(), "Modo prueba CRM — no enviar a Siigo")
    return {"ok": True, "modo_prueba": data.modo_prueba}


# ═══════════════════════════════════════════════════════════════
# ENDPOINTS — RESET
# ═══════════════════════════════════════════════════════════════

@router.post("/reset")
def reset_crm():
    """Elimina todos los datos del CRM para empezar de cero."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM movimientos_bancarios")
    cur.execute("DELETE FROM crm_facturas")
    cur.execute("DELETE FROM crm_clientes")
    cur.execute("DELETE FROM crm_sync_log")
    conn.commit()
    cur.close(); conn.close()
    return {"ok": True, "mensaje": "Datos CRM eliminados. Listo para sincronizar desde Siigo."}


# ═══════════════════════════════════════════════════════════════
# ENDPOINTS — SYNC
# ═══════════════════════════════════════════════════════════════

def _parse_customers(customers: list) -> list:
    rows = []
    for c in customers:
        sid = str(c.get("id", ""))
        if not sid:
            continue
        ident = str(c.get("identification", "")).strip() or None
        names = c.get("name", [])
        nombre = (" ".join(names) if isinstance(names, list) else str(names)).strip() or "(Sin nombre)"
        phones = c.get("phones", [])
        telefono = phones[0].get("number", "") if phones else None
        contacts = c.get("contacts", [])
        email = (contacts[0].get("email", "") if contacts else "") or None
        address_obj = c.get("address", {}) or {}
        direccion = address_obj.get("address", "") or None
        ciudad_obj = address_obj.get("city", {})
        ciudad = ciudad_obj.get("city_name", "") if ciudad_obj else None
        rows.append((sid, ident, nombre, telefono, email, direccion, ciudad))
    return rows


def _parse_invoices(invoices: list) -> list:
    rows = []
    for inv in invoices:
        if inv.get("annulled"):
            continue
        siigo_id = str(inv.get("id", ""))
        if not siigo_id:
            continue
        prefix = inv.get("prefix", "") or ""
        number = inv.get("number", 0) or 0
        fecha_str = (inv.get("date") or "")[:10]
        try:
            fecha = datetime.strptime(fecha_str, "%Y-%m-%d").date() if fecha_str else date.today()
        except ValueError:
            fecha = date.today()
        total = float(inv.get("total", 0) or 0)
        balance = float(inv.get("balance", 0) or 0)
        estado = "pendiente" if balance > 0 else "pagado"
        cust = inv.get("customer", {}) or {}
        cedula = str(cust.get("identification", "") or "").strip() or None
        cust_names = cust.get("name", [])
        cli_nombre = (" ".join(cust_names) if isinstance(cust_names, list) else str(cust_names)).strip() or "(Sin nombre)"
        siigo_cust_id = str(cust.get("id", "") or "") or None
        rows.append((siigo_id, number, prefix, cli_nombre, cedula, fecha, total, balance, estado, siigo_cust_id))
    return rows


def _run_sync_siigo():
    """Tarea en background: sincroniza clientes y facturas de Siigo usando upsert batch."""
    job = _sync_jobs["siigo"]
    job["running"] = True
    job["ok"] = None
    job["msg"] = ""
    job["result"] = None

    try:
        job["step"] = "Obteniendo clientes de Siigo..."
        customers = _fetch_siigo_customers()
    except Exception as e:
        job["running"] = False; job["ok"] = False
        job["msg"] = f"Error obteniendo clientes: {str(e)}"
        return

    job["step"] = f"Guardando {len(customers)} clientes..."
    cli_rows = _parse_customers(customers)

    conn = get_conn()
    cur = conn.cursor()

    # Batch upsert clientes — un solo query
    if cli_rows:
        from psycopg2.extras import execute_values
        execute_values(cur, """
            INSERT INTO crm_clientes (siigo_id, cedula, nombre, telefono, email, direccion, ciudad)
            VALUES %s
            ON CONFLICT (siigo_id) DO UPDATE SET
              cedula = EXCLUDED.cedula,
              nombre = EXCLUDED.nombre,
              telefono = EXCLUDED.telefono,
              email = EXCLUDED.email,
              direccion = EXCLUDED.direccion,
              ciudad = EXCLUDED.ciudad,
              actualizado_en = NOW()
        """, cli_rows)
        conn.commit()

    # Count nuevos vs actualizados
    cur.execute("SELECT COUNT(*) FROM crm_clientes")
    cli_total = cur.fetchone()[0]
    cli_new = max(0, cli_total - max(0, cli_total - len(cli_rows)))

    try:
        job["step"] = "Obteniendo facturas de Siigo (últimos 3 años)..."
        invoices = _fetch_siigo_invoices_all()
    except Exception as e:
        conn.close()
        job["running"] = False; job["ok"] = False
        job["msg"] = f"Error obteniendo facturas: {str(e)}"
        return

    job["step"] = f"Guardando {len(invoices)} facturas..."
    inv_rows = _parse_invoices(invoices)

    # Build siigo_id -> internal id map for cliente lookup
    cur.execute("SELECT siigo_id, id FROM crm_clientes")
    cust_map = {row[0]: row[1] for row in cur.fetchall()}

    # Resolve cliente_id for each invoice
    final_rows = []
    for r in inv_rows:
        siigo_id, number, prefix, cli_nombre, cedula, fecha, total, balance, estado, siigo_cust_id = r
        cliente_id = cust_map.get(siigo_cust_id) if siigo_cust_id else None
        final_rows.append((siigo_id, number, prefix, cliente_id, cli_nombre, cedula, fecha, total, balance, estado))

    if final_rows:
        execute_values(cur, """
            INSERT INTO crm_facturas
              (siigo_invoice_id, numero, prefix, cliente_id, cliente_nombre, cliente_cedula,
               fecha, total, balance, estado_pago)
            VALUES %s
            ON CONFLICT (siigo_invoice_id) DO UPDATE SET
              total = EXCLUDED.total,
              balance = EXCLUDED.balance,
              cliente_id = EXCLUDED.cliente_id,
              cliente_nombre = EXCLUDED.cliente_nombre,
              cliente_cedula = EXCLUDED.cliente_cedula,
              sync_at = NOW()
        """, final_rows)
        conn.commit()

    cur.execute("SELECT COUNT(*) FROM crm_facturas")
    inv_total = cur.fetchone()[0]

    cur.execute("""
        INSERT INTO crm_sync_log (tipo, registros, nuevos, actualizados, detalle)
        VALUES ('siigo_full', %s, %s, %s, %s)
    """, (len(cli_rows) + len(final_rows), len(cli_rows), len(final_rows),
          f"Clientes: {len(cli_rows)} | Facturas: {len(final_rows)}"))
    conn.commit()
    cur.close(); conn.close()

    job["running"] = False
    job["ok"] = True
    job["msg"] = f"{len(cli_rows)} clientes y {len(final_rows)} facturas sincronizados."
    job["step"] = "Completado"
    job["result"] = {
        "ok": True,
        "clientes": {"total": len(cli_rows)},
        "facturas": {"total": len(final_rows)}
    }


@router.post("/sync/siigo")
def sync_siigo(background_tasks: BackgroundTasks):
    """Inicia sync de Siigo en background y retorna inmediatamente."""
    job = _sync_jobs["siigo"]
    if job["running"]:
        return {"started": False, "running": True, "msg": "Sync ya en curso"}
    background_tasks.add_task(_run_sync_siigo)
    return {"started": True, "running": True, "msg": "Sync iniciado"}


@router.get("/sync/siigo/status")
def sync_siigo_status():
    """Retorna el estado actual del sync de Siigo."""
    return _sync_jobs["siigo"]


@router.post("/sync/excel")
async def sync_excel(file: UploadFile = File(...)):
    """Importa BASE DATOS y CLIENTES SIN COMPRA del Excel para enriquecer contactos."""
    import openpyxl

    def _cs(v):
        if v is None: return None
        s = str(v).strip(); return s or None

    def _cp(v):
        if v is None: return None
        s = re.sub(r"\.0$", "", str(v).strip())
        s = re.sub(r"[^0-9+]", "", s)
        return s or None

    def _cc(v):
        if v is None: return None
        s = re.sub(r"\.0$", "", str(v).strip())
        s = re.sub(r"[^0-9a-zA-Z-]", "", s)
        return s or None

    content = await file.read()
    wb = openpyxl.load_workbook(io.BytesIO(content), read_only=True, data_only=True)
    conn = get_conn()
    cur = conn.cursor()
    ins = upd = 0

    # ── BASE DATOS ──
    for sheet_name in [' BASE DATOS', 'BASE DATOS']:
        if sheet_name in wb.sheetnames:
            ws = wb[sheet_name]
            for row in ws.iter_rows(min_row=2, values_only=True):
                nombre = _cs(row[0])
                if not nombre: continue
                direccion = _cs(row[1])
                apto      = _cs(row[2])
                zona      = _cs(row[4])
                telefono  = _cp(row[5])
                cedula    = _cc(row[6])
                email     = _cs(row[7])

                # Try match by cedula first
                if cedula:
                    cur.execute("SELECT id FROM crm_clientes WHERE cedula = %s LIMIT 1", (cedula,))
                    ex = cur.fetchone()
                    if ex:
                        cur.execute("""
                            UPDATE crm_clientes SET
                              direccion=COALESCE(crm_clientes.direccion,%s),
                              telefono=COALESCE(crm_clientes.telefono,%s),
                              email=COALESCE(crm_clientes.email,%s),
                              actualizado_en=NOW()
                            WHERE id=%s
                        """, (direccion, telefono, email, ex[0]))
                        upd += 1; continue

                # Try match by name
                cur.execute("SELECT id FROM crm_clientes WHERE nombre ILIKE %s LIMIT 1", (nombre,))
                ex = cur.fetchone()
                if ex:
                    cur.execute("""
                        UPDATE crm_clientes SET
                          direccion=COALESCE(crm_clientes.direccion,%s),
                          telefono=COALESCE(crm_clientes.telefono,%s),
                          cedula=COALESCE(crm_clientes.cedula,%s),
                          email=COALESCE(crm_clientes.email,%s),
                          actualizado_en=NOW()
                        WHERE id=%s
                    """, (direccion, telefono, cedula, email, ex[0]))
                    upd += 1; continue

                # New client from Excel
                cur.execute("""
                    INSERT INTO crm_clientes (nombre, direccion, telefono, cedula, email)
                    VALUES (%s,%s,%s,%s,%s)
                """, (nombre, direccion, telefono, cedula, email))
                ins += 1
            conn.commit()
            break

    # ── CLIENTES SIN COMPRA ──
    for sheet_name in ['CLIENTES SIN COMPRA ', 'CLIENTES SIN COMPRA']:
        if sheet_name in wb.sheetnames:
            ws = wb[sheet_name]
            for row in ws.iter_rows(min_row=2, values_only=True):
                nombre = _cs(row[0])
                if not nombre: continue
                cur.execute("SELECT id FROM crm_clientes WHERE nombre ILIKE %s LIMIT 1", (nombre,))
                if cur.fetchone(): continue
                direccion = _cs(row[1])
                apto      = _cs(row[2])
                zona      = _cs(row[4]) if len(row) > 4 else None
                telefono  = _cp(row[5]) if len(row) > 5 else None
                cur.execute("""
                    INSERT INTO crm_clientes (nombre, direccion, telefono)
                    VALUES (%s,%s,%s)
                """, (nombre, direccion, telefono))
                ins += 1
            conn.commit()
            break

    cur.execute("""
        INSERT INTO crm_sync_log (tipo, registros, nuevos, actualizados, detalle)
        VALUES ('excel', %s, %s, %s, %s)
    """, (ins + upd, ins, upd, "Importación Excel BASE DATOS + CLIENTES SIN COMPRA"))
    conn.commit()
    cur.close(); conn.close()
    return {"ok": True, "nuevos": ins, "actualizados": upd}


@router.post("/sync/bancos")
def sync_bancos():
    """Sincroniza movimientos bancarios desde Google Sheets."""
    try:
        service = _get_sheets_service()
    except Exception as e:
        raise HTTPException(500, f"Error conectando Google Sheets: {str(e)}")

    conn = get_conn()
    cur = conn.cursor()
    ins = skip = 0

    for tab_name, banco in SHEET_TABS:
        rows = _fetch_sheet_tab(service, tab_name)
        if not rows or len(rows) < 2:
            continue

        # Detect column layout from header
        header = [str(h).strip().upper() for h in rows[0]]

        # BDB layout: Fecha(0), Transacción(1), Débitos(2), Créditos(3), ESTADO(4), RC(5), CLIENTE(6), VALOR_CLI(7)
        # BANCOLOMBIA: FECHA(0), CONCEPTO(1), VALOR BANCO(2), ESTADO(3), RC(4), CLIENTE(5)
        is_bdb = banco == "BDB"

        for row_idx, row in enumerate(rows[1:], start=2):
            # Extend row to avoid index errors
            row = list(row) + [""] * 10

            if is_bdb:
                fecha_raw = row[0]
                desc_raw  = row[1]
                debito    = _clean_valor_sheet(row[2])
                credito   = _clean_valor_sheet(row[3])
                estado    = str(row[4]).strip() if row[4] else None
                rc_sheet  = str(row[5]).strip() if row[5] else None
                cli_sheet = str(row[6]).strip() if row[6] else None
                # For BDB, we care about credits (ingresos)
                valor = credito if credito and credito > 0 else (-(debito) if debito and debito > 0 else None)
            else:
                # Bancolombia
                fecha_raw = row[0]
                desc_raw  = row[1]
                valor_raw = _clean_valor_sheet(row[2])
                estado    = str(row[3]).strip() if row[3] else None
                rc_sheet  = str(row[4]).strip() if row[4] else None
                cli_sheet = str(row[5]).strip() if row[5] else None
                valor = valor_raw

            fecha = _parse_date_sheet(fecha_raw)
            desc  = str(desc_raw).strip() if desc_raw else None

            if not fecha or valor is None:
                skip += 1
                continue

            # Skip zero or negative (debits) — we only want credits/ingresos
            if valor <= 0:
                skip += 1
                continue

            # Avoid duplicates by (banco, fecha, valor, desc)
            cur.execute("""
                SELECT id FROM movimientos_bancarios
                WHERE banco=%s AND fecha=%s AND valor=%s AND (descripcion=%s OR (descripcion IS NULL AND %s IS NULL))
                LIMIT 1
            """, (banco, fecha, valor, desc, desc))
            if cur.fetchone():
                skip += 1
                continue

            # Auto-detect conciliado
            conciliado = estado and "CONCILI" in estado.upper()

            cur.execute("""
                INSERT INTO movimientos_bancarios
                  (banco, fecha, descripcion, valor, estado, rc_sheet, cliente_sheet,
                   conciliado, sheet_row, sheet_tab)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (banco, fecha, desc, valor, estado, rc_sheet or None, cli_sheet or None,
                  conciliado, row_idx, tab_name))
            ins += 1

    conn.commit()
    cur.execute("""
        INSERT INTO crm_sync_log (tipo, registros, nuevos, actualizados, detalle)
        VALUES ('bancos', %s, %s, 0, %s)
    """, (ins, ins, f"Movimientos bancarios: {ins} nuevos, {skip} ignorados"))
    conn.commit()
    cur.close(); conn.close()
    return {"ok": True, "nuevos": ins, "ignorados": skip}


# ═══════════════════════════════════════════════════════════════
# ENDPOINTS — FACTURAS (Activos / Históricos)
# ═══════════════════════════════════════════════════════════════

@router.get("/activos")
def get_activos(q: Optional[str] = None):
    """Clientes con al menos una factura en los últimos 6 meses, con sus facturas."""
    corte = (date.today() - timedelta(days=180)).isoformat()
    return _get_clientes_con_facturas(corte_desde=corte, q=q)


@router.get("/historicos")
def get_historicos(q: Optional[str] = None):
    """Clientes con facturas más antiguas de 6 meses (sin facturas recientes)."""
    corte = (date.today() - timedelta(days=180)).isoformat()
    return _get_clientes_con_facturas(corte_hasta=corte, q=q)


def _get_clientes_con_facturas(corte_desde: str = None, corte_hasta: str = None, q: str = None) -> list:
    conn = get_conn()
    cur = conn.cursor()

    # Get client IDs that match date criteria
    if corte_desde:
        cur.execute("""
            SELECT DISTINCT cliente_id FROM crm_facturas
            WHERE fecha >= %s AND cliente_id IS NOT NULL
        """, (corte_desde,))
    elif corte_hasta:
        cur.execute("""
            SELECT DISTINCT cliente_id FROM crm_facturas f
            WHERE cliente_id IS NOT NULL
            AND NOT EXISTS (
                SELECT 1 FROM crm_facturas f2
                WHERE f2.cliente_id = f.cliente_id AND f2.fecha >= %s
            )
        """, (corte_hasta,))
    else:
        cur.execute("SELECT DISTINCT cliente_id FROM crm_facturas WHERE cliente_id IS NOT NULL")

    ids = [r[0] for r in cur.fetchall()]
    if not ids:
        cur.close(); conn.close()
        return []

    # Apply name filter
    q_filter = ""
    params = [tuple(ids)]
    if q:
        q_filter = "AND c.nombre ILIKE %s"
        params.append(f"%{q}%")

    cur.execute(f"""
        SELECT c.id, c.siigo_id, c.cedula, c.nombre, c.telefono, c.email,
               c.direccion, c.ciudad, c.origen_canal, c.notas,
               COUNT(f.id) AS num_facturas,
               SUM(f.total) AS total_ventas,
               SUM(f.balance) AS total_pendiente,
               MAX(f.fecha) AS ultima_factura
        FROM crm_clientes c
        JOIN crm_facturas f ON f.cliente_id = c.id
        WHERE c.id = ANY(%s) {q_filter}
        GROUP BY c.id
        ORDER BY ultima_factura DESC
    """, params)

    clients = []
    for row in cur.fetchall():
        cid = row[0]
        # Get invoices for this client
        cur.execute("""
            SELECT id, siigo_invoice_id, numero, prefix, fecha, total, balance,
                   estado_pago, medio_pago, cuenta_debito, origen_canal,
                   movimiento_id, rc_siigo_id, rc_numero, rc_modo_prueba
            FROM crm_facturas
            WHERE cliente_id = %s
            ORDER BY fecha DESC
        """, (cid,))
        facturas = []
        for f in cur.fetchall():
            facturas.append({
                "id": f[0], "siigo_invoice_id": f[1],
                "numero": f[2], "prefix": f[3],
                "factura": f"{f[3]}-{f[2]}" if f[3] else str(f[2]),
                "fecha": f[4].isoformat() if f[4] else None,
                "total": float(f[5] or 0), "balance": float(f[6] or 0),
                "estado_pago": f[7], "medio_pago": f[8],
                "cuenta_debito": f[9], "origen_canal": f[10],
                "movimiento_id": f[11], "rc_siigo_id": f[12],
                "rc_numero": f[13], "rc_modo_prueba": f[14],
            })
        clients.append({
            "id": row[0], "siigo_id": row[1], "cedula": row[2], "nombre": row[3],
            "telefono": row[4], "email": row[5], "direccion": row[6], "ciudad": row[7],
            "origen_canal": row[8], "notas": row[9],
            "num_facturas": row[10], "total_ventas": float(row[11] or 0),
            "total_pendiente": float(row[12] or 0),
            "ultima_factura": row[13].isoformat() if row[13] else None,
            "facturas": facturas,
        })

    cur.close(); conn.close()
    return clients


@router.get("/clientes/{cliente_id}")
def get_cliente(cliente_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, siigo_id, cedula, nombre, telefono, email, direccion, ciudad, origen_canal, notas
        FROM crm_clientes WHERE id = %s
    """, (cliente_id,))
    row = cur.fetchone()
    if not row:
        cur.close(); conn.close()
        raise HTTPException(404, "Cliente no encontrado")

    cur.execute("""
        SELECT id, siigo_invoice_id, numero, prefix, fecha, total, balance,
               estado_pago, medio_pago, cuenta_debito, origen_canal,
               movimiento_id, rc_siigo_id, rc_numero, rc_modo_prueba
        FROM crm_facturas WHERE cliente_id = %s ORDER BY fecha DESC
    """, (cliente_id,))
    facturas = []
    for f in cur.fetchall():
        facturas.append({
            "id": f[0], "siigo_invoice_id": f[1],
            "numero": f[2], "prefix": f[3],
            "factura": f"{f[3]}-{f[2]}" if f[3] else str(f[2]),
            "fecha": f[4].isoformat() if f[4] else None,
            "total": float(f[5] or 0), "balance": float(f[6] or 0),
            "estado_pago": f[7], "medio_pago": f[8],
            "cuenta_debito": f[9], "origen_canal": f[10],
            "movimiento_id": f[11], "rc_siigo_id": f[12],
            "rc_numero": f[13], "rc_modo_prueba": f[14],
        })
    cur.close(); conn.close()
    return {
        "id": row[0], "siigo_id": row[1], "cedula": row[2], "nombre": row[3],
        "telefono": row[4], "email": row[5], "direccion": row[6], "ciudad": row[7],
        "origen_canal": row[8], "notas": row[9],
        "facturas": facturas,
    }


class FacturaUpdate(BaseModel):
    estado_pago: Optional[str] = None
    medio_pago: Optional[str] = None
    cuenta_debito: Optional[str] = None
    origen_canal: Optional[str] = None

@router.put("/facturas/{factura_id}")
def update_factura(factura_id: int, data: FacturaUpdate):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id FROM crm_facturas WHERE id = %s", (factura_id,))
    if not cur.fetchone():
        cur.close(); conn.close()
        raise HTTPException(404, "Factura no encontrada")

    sets = []
    vals = []
    if data.estado_pago is not None:
        sets.append("estado_pago=%s"); vals.append(data.estado_pago)
    if data.medio_pago is not None:
        sets.append("medio_pago=%s"); vals.append(data.medio_pago)
    if data.cuenta_debito is not None:
        sets.append("cuenta_debito=%s"); vals.append(data.cuenta_debito)
    if data.origen_canal is not None:
        sets.append("origen_canal=%s"); vals.append(data.origen_canal)

    if sets:
        vals.append(factura_id)
        cur.execute(f"UPDATE crm_facturas SET {', '.join(sets)} WHERE id=%s", vals)
        conn.commit()

    cur.close(); conn.close()
    return {"ok": True}


# ═══════════════════════════════════════════════════════════════
# ENDPOINTS — RECIBO DE CAJA (RC)
# ═══════════════════════════════════════════════════════════════

# Debit account codes for RC in Siigo
CUENTAS_RC = {
    "Efectivo":          "11050501",
    "Banco de Bogota":   "11100501",
    "BDB":               "11100501",
    "Bancolombia":       "11200502",
    "Link":              "11100501",  # Link de pago cae en BDB
}

def _cuenta_for_medio(medio: str) -> str:
    if not medio:
        return "11050501"
    m = medio.strip()
    for k, v in CUENTAS_RC.items():
        if k.upper() in m.upper():
            return v
    return "11050501"


def _crear_rc_en_siigo(factura: dict, modo_prueba: bool) -> dict:
    """
    Crea un Recibo de Caja en Siigo para la factura dada.
    Si modo_prueba=True, simula y retorna respuesta falsa.
    Retorna dict con {ok, rc_id, rc_numero, simulado}
    """
    if modo_prueba:
        return {
            "ok": True,
            "simulado": True,
            "rc_id": f"PRUEBA-{factura['id']}",
            "rc_numero": f"RC-PRUEBA-{factura['id']}",
            "mensaje": "Modo prueba: RC no creado en Siigo"
        }

    # Build RC payload
    # We need siigo_id from crm_clientes to get customer data
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT c.siigo_id, c.cedula, c.nombre
        FROM crm_clientes c
        JOIN crm_facturas f ON f.cliente_id = c.id
        WHERE f.id = %s
    """, (factura["id"],))
    cli = cur.fetchone()
    cur.close(); conn.close()

    if not cli:
        return {"ok": False, "error": "Cliente no encontrado para la factura"}

    siigo_cust_id = cli[0]
    cedula = cli[1] or ""
    nombre = cli[2] or ""
    cuenta = factura.get("cuenta_debito") or _cuenta_for_medio(factura.get("medio_pago", ""))
    monto = float(factura["balance"]) if float(factura.get("balance", 0)) > 0 else float(factura["total"])
    factura_ref = f"{factura.get('prefix','')}-{factura.get('numero','')}"

    payload = {
        "document": {"id": 3619},
        "date": date.today().isoformat(),
        "customer": {
            "person_type": "Person",
            "id_type": {"code": "13"},
            "identification": cedula,
            "name": [nombre]
        },
        "stamp": {"send": False},
        "observations": f"Pago factura {factura_ref}",
        "items": [
            {
                "account": {"code": cuenta},
                "value": monto,
                "description": f"Recibo pago {factura_ref}",
                "taxes": []
            },
            {
                "account": {"code": "130505"},
                "value": -monto,
                "description": f"Abono {factura_ref}",
                "invoice": {"id": factura["siigo_invoice_id"]},
                "taxes": []
            }
        ]
    }

    try:
        resp = requests.post(
            f"{SIIGO_BASE}/vouchers",
            headers=siigo_headers(),
            json=payload,
            timeout=30
        )
        resp.raise_for_status()
        data = resp.json()
        rc_id = str(data.get("id", ""))
        prefix_rc = data.get("prefix", "RC")
        number_rc = data.get("number", "")
        rc_numero = f"{prefix_rc}-{number_rc}" if number_rc else rc_id
        return {"ok": True, "simulado": False, "rc_id": rc_id, "rc_numero": rc_numero}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@router.post("/facturas/{factura_id}/generar-rc")
def generar_rc(factura_id: int):
    """Genera Recibo de Caja para una factura específica."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, siigo_invoice_id, numero, prefix, total, balance,
               estado_pago, medio_pago, cuenta_debito, rc_siigo_id
        FROM crm_facturas WHERE id = %s
    """, (factura_id,))
    row = cur.fetchone()
    if not row:
        cur.close(); conn.close()
        raise HTTPException(404, "Factura no encontrada")

    if row[9]:  # rc_siigo_id already set
        cur.close(); conn.close()
        return {"ok": False, "mensaje": "Ya tiene Recibo de Caja generado"}

    factura = {
        "id": row[0], "siigo_invoice_id": row[1], "numero": row[2], "prefix": row[3],
        "total": row[4], "balance": row[5], "estado_pago": row[6],
        "medio_pago": row[7], "cuenta_debito": row[8]
    }
    modo = _modo_prueba()
    result = _crear_rc_en_siigo(factura, modo)

    if result["ok"]:
        cur.execute("""
            UPDATE crm_facturas
            SET rc_siigo_id=%s, rc_numero=%s, rc_modo_prueba=%s, estado_pago='pagado'
            WHERE id=%s
        """, (result["rc_id"], result["rc_numero"], result["simulado"], factura_id))
        conn.commit()

    cur.close(); conn.close()
    return result


@router.post("/generar-rc-masivo")
def generar_rc_masivo():
    """Genera RC para todas las facturas marcadas como pagado sin RC aún."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, siigo_invoice_id, numero, prefix, total, balance,
               estado_pago, medio_pago, cuenta_debito, rc_siigo_id
        FROM crm_facturas
        WHERE estado_pago = 'pagado' AND rc_siigo_id IS NULL
    """)
    facturas = cur.fetchall()
    cur.close(); conn.close()

    modo = _modo_prueba()
    ok_count = 0
    errors = []

    for row in facturas:
        factura = {
            "id": row[0], "siigo_invoice_id": row[1], "numero": row[2], "prefix": row[3],
            "total": row[4], "balance": row[5], "estado_pago": row[6],
            "medio_pago": row[7], "cuenta_debito": row[8]
        }
        result = _crear_rc_en_siigo(factura, modo)
        if result["ok"]:
            conn2 = get_conn()
            cur2 = conn2.cursor()
            cur2.execute("""
                UPDATE crm_facturas
                SET rc_siigo_id=%s, rc_numero=%s, rc_modo_prueba=%s
                WHERE id=%s
            """, (result["rc_id"], result["rc_numero"], result["simulado"], row[0]))
            conn2.commit()
            cur2.close(); conn2.close()
            ok_count += 1
        else:
            errors.append({"factura_id": row[0], "error": result.get("error", "Error desconocido")})

    return {
        "ok": True,
        "procesadas": len(facturas),
        "exitosas": ok_count,
        "errores": errors,
        "modo_prueba": modo
    }


# ═══════════════════════════════════════════════════════════════
# ENDPOINTS — CONCILIACIÓN BANCARIA
# ═══════════════════════════════════════════════════════════════

@router.get("/conciliacion")
def get_conciliacion(banco: Optional[str] = None, solo_pendientes: bool = True):
    """Retorna movimientos bancarios no conciliados + facturas pendientes para matching."""
    conn = get_conn()
    cur = conn.cursor()

    # Movimientos sin conciliar (créditos/ingresos)
    banco_filter = "AND banco = %s" if banco else ""
    pendiente_filter = "AND conciliado = FALSE" if solo_pendientes else ""
    params = []
    if banco:
        params.append(banco)

    cur.execute(f"""
        SELECT id, banco, fecha, descripcion, valor, estado,
               rc_sheet, cliente_sheet, conciliado, factura_id, sheet_tab
        FROM movimientos_bancarios
        WHERE valor > 0 {banco_filter} {pendiente_filter}
        ORDER BY fecha DESC
        LIMIT 500
    """, params)

    movimientos = []
    for r in cur.fetchall():
        movimientos.append({
            "id": r[0], "banco": r[1],
            "fecha": r[2].isoformat() if r[2] else None,
            "descripcion": r[3], "valor": float(r[4]),
            "estado": r[5], "rc_sheet": r[6], "cliente_sheet": r[7],
            "conciliado": r[8], "factura_id": r[9], "sheet_tab": r[10]
        })

    # Facturas pendientes de pago
    cur.execute("""
        SELECT f.id, f.siigo_invoice_id, f.numero, f.prefix, f.fecha,
               f.total, f.balance, f.estado_pago, f.medio_pago,
               f.cliente_nombre, f.cliente_cedula
        FROM crm_facturas f
        WHERE f.estado_pago = 'pendiente' AND f.balance > 0
        ORDER BY f.fecha DESC
        LIMIT 500
    """)
    facturas_pendientes = []
    for r in cur.fetchall():
        facturas_pendientes.append({
            "id": r[0], "siigo_invoice_id": r[1], "numero": r[2], "prefix": r[3],
            "factura": f"{r[3]}-{r[2]}" if r[3] else str(r[2]),
            "fecha": r[4].isoformat() if r[4] else None,
            "total": float(r[5]), "balance": float(r[6]),
            "estado_pago": r[7], "medio_pago": r[8],
            "cliente_nombre": r[9], "cliente_cedula": r[10]
        })

    cur.close(); conn.close()
    return {"movimientos": movimientos, "facturas_pendientes": facturas_pendientes}


class ConciliarIn(BaseModel):
    movimiento_id: int
    factura_id: int
    medio_pago: Optional[str] = None
    cuenta_debito: Optional[str] = None

@router.post("/conciliar")
def conciliar(data: ConciliarIn):
    """Vincula un movimiento bancario con una factura y la marca como pagada."""
    conn = get_conn()
    cur = conn.cursor()

    # Verify both exist
    cur.execute("SELECT id, valor FROM movimientos_bancarios WHERE id = %s", (data.movimiento_id,))
    mov = cur.fetchone()
    if not mov:
        cur.close(); conn.close()
        raise HTTPException(404, "Movimiento no encontrado")

    cur.execute("SELECT id FROM crm_facturas WHERE id = %s", (data.factura_id,))
    if not cur.fetchone():
        cur.close(); conn.close()
        raise HTTPException(404, "Factura no encontrada")

    # Auto-detect banco and set cuenta_debito if not provided
    cur.execute("SELECT banco FROM movimientos_bancarios WHERE id = %s", (data.movimiento_id,))
    banco = cur.fetchone()[0]
    cuenta = data.cuenta_debito
    if not cuenta:
        if banco == "BDB":
            cuenta = "11100501"
        elif banco == "BANCOLOMBIA":
            cuenta = "11200502"
        else:
            cuenta = "11050501"

    medio = data.medio_pago or (banco if banco else "Efectivo")

    cur.execute("""
        UPDATE movimientos_bancarios
        SET conciliado=TRUE, factura_id=%s
        WHERE id=%s
    """, (data.factura_id, data.movimiento_id))

    cur.execute("""
        UPDATE crm_facturas
        SET estado_pago='pagado', movimiento_id=%s,
            medio_pago=COALESCE(medio_pago,%s),
            cuenta_debito=COALESCE(cuenta_debito,%s)
        WHERE id=%s
    """, (data.movimiento_id, medio, cuenta, data.factura_id))

    conn.commit()
    cur.close(); conn.close()
    return {"ok": True}


# ═══════════════════════════════════════════════════════════════
# ENDPOINTS — SYNC LOG
# ═══════════════════════════════════════════════════════════════

@router.get("/sync/log")
def get_sync_log():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT tipo, fecha, registros, nuevos, actualizados, detalle
        FROM crm_sync_log ORDER BY fecha DESC LIMIT 50
    """)
    rows = cur.fetchall()
    cur.close(); conn.close()
    return [
        {"tipo": r[0], "fecha": r[1].isoformat() if r[1] else None,
         "registros": r[2], "nuevos": r[3], "actualizados": r[4], "detalle": r[5]}
        for r in rows
    ]
