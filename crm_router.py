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
    """
    Parsea valores del Sheet colombiano: punto=miles, coma=decimal.
    Ej: "406.080,00" -> 406080.0  |  "1.234.567,50" -> 1234567.5  |  "1.500.000" -> 1500000.0
    Google Sheets API entrega números como float directamente — esos pasan sin modificar.
    """
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().replace("$", "").replace("\xa0", "").strip()
    if not s:
        return None
    if "," in s and "." in s:
        # Formato colombiano clásico: "1.234.567,50" → quitar puntos, coma→punto
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        parts = s.split(",")
        if len(parts) == 2 and len(parts[1]) == 3 and parts[1].isdigit():
            s = s.replace(",", "")  # coma como miles: "1,500,000"
        else:
            s = s.replace(",", ".")  # coma como decimal: "406080,00"
    elif "." in s:
        parts = s.split(".")
        if len(parts) > 2:
            # Múltiples puntos = separadores de miles: "1.500.000"
            s = s.replace(".", "")
        elif len(parts) == 2 and len(parts[1]) == 3 and parts[1].isdigit():
            s = s.replace(".", "")  # un punto como miles: "406.080"
        # Si no (ej: "406.08"), dejar como decimal anglosajón
    try:
        return float(s)
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
            range=f"{tab_name}!A:J"  # Sin límite de filas
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


@router.post("/fix/conciliados-estado")
def fix_conciliados_estado():
    """Marca como conciliados los movimientos cuyo estado indica que ya tienen medio de pago en Siigo."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE movimientos_bancarios
        SET conciliado = TRUE
        WHERE conciliado = FALSE
          AND (
            estado ILIKE '%MEDIO DE PAGO%' OR
            estado ILIKE '%QUEDO CON MEDIO%'
          )
    """)
    marcados = cur.rowcount
    conn.commit()
    cur.close(); conn.close()
    return {"ok": True, "marcados": marcados}


@router.post("/reset/bancos")
def reset_bancos():
    """Elimina solo los movimientos bancarios, conservando clientes y facturas."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM movimientos_bancarios")
    # También limpiar movimiento_id en facturas para no dejar referencias huérfanas
    cur.execute("UPDATE crm_facturas SET movimiento_id = NULL WHERE movimiento_id IS NOT NULL")
    conn.commit()
    cur.close(); conn.close()
    return {"ok": True}


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
        nombre = (" ".join(n for n in names if n) if isinstance(names, list) else str(names or "")).strip() or "(Sin nombre)"
        phones = c.get("phones", [])
        telefono = phones[0].get("number", "") if phones else None
        contacts = c.get("contacts", [])
        email = (contacts[0].get("email", "") if contacts else "") or None
        address_obj = c.get("address", {}) or {}
        direccion = address_obj.get("address", "") or None
        ciudad_obj = address_obj.get("city", {})
        ciudad = ciudad_obj.get("city_name", "") if ciudad_obj else None
        # Tipo de documento y persona para RC
        id_type_obj = c.get("id_type", {}) or {}
        id_type_code = str(id_type_obj.get("code", "13")) or "13"
        person_type = c.get("person_type", "Person") or "Person"
        rows.append((sid, ident, nombre, telefono, email, direccion, ciudad, id_type_code, person_type))
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
        cli_nombre = (" ".join(n for n in cust_names if n) if isinstance(cust_names, list) else str(cust_names or "")).strip() or "(Sin nombre)"
        siigo_cust_id = str(cust.get("id", "") or "") or None
        # Extraer medio de pago desde Siigo (payments[0].name si existe)
        payments = inv.get("payments") or []
        medio_pago_siigo = payments[0].get("name", "").strip() if payments else None
        rows.append((siigo_id, number, prefix, cli_nombre, cedula, fecha, total, balance, estado, siigo_cust_id, medio_pago_siigo))
    return rows


def _run_sync_siigo():
    """Tarea en background: sincroniza clientes y facturas de Siigo usando upsert batch."""
    job = _sync_jobs["siigo"]
    job["running"] = True
    job["ok"] = None
    job["msg"] = ""
    job["result"] = None
    try:
        _run_sync_siigo_inner(job)
    except Exception as e:
        job["running"] = False
        job["ok"] = False
        job["msg"] = f"Error inesperado: {str(e)}"
        job["step"] = "Error"


def _run_sync_siigo_inner(job):

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
            INSERT INTO crm_clientes (siigo_id, cedula, nombre, telefono, email, direccion, ciudad, id_type_code, person_type)
            VALUES %s
            ON CONFLICT (siigo_id) DO UPDATE SET
              cedula = EXCLUDED.cedula,
              nombre = EXCLUDED.nombre,
              telefono = EXCLUDED.telefono,
              email = EXCLUDED.email,
              direccion = EXCLUDED.direccion,
              ciudad = EXCLUDED.ciudad,
              id_type_code = EXCLUDED.id_type_code,
              person_type = EXCLUDED.person_type,
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
        siigo_id, number, prefix, cli_nombre, cedula, fecha, total, balance, estado, siigo_cust_id, medio_pago_siigo = r
        cliente_id = cust_map.get(siigo_cust_id) if siigo_cust_id else None
        final_rows.append((siigo_id, number, prefix, cliente_id, cli_nombre, cedula, fecha, total, balance, estado, medio_pago_siigo))

    if final_rows:
        execute_values(cur, """
            INSERT INTO crm_facturas
              (siigo_invoice_id, numero, prefix, cliente_id, cliente_nombre, cliente_cedula,
               fecha, total, balance, estado_pago, medio_pago)
            VALUES %s
            ON CONFLICT (siigo_invoice_id) DO UPDATE SET
              total = EXCLUDED.total,
              balance = EXCLUDED.balance,
              cliente_id = EXCLUDED.cliente_id,
              cliente_nombre = EXCLUDED.cliente_nombre,
              cliente_cedula = EXCLUDED.cliente_cedula,
              -- Si Siigo dice balance=0 → pagado (Siigo es fuente de verdad)
              -- Si Siigo dice balance>0 → solo actualizar si era pendiente (no revertir pagos manuales)
              estado_pago = CASE
                WHEN EXCLUDED.balance = 0 THEN 'pagado'
                ELSE crm_facturas.estado_pago
              END,
              -- Solo poner medio_pago de Siigo si no está ya definido manualmente
              medio_pago = COALESCE(crm_facturas.medio_pago, EXCLUDED.medio_pago),
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


@router.post("/sync/siigo/reset")
def sync_siigo_reset():
    """Resetea el estado del job si quedó bloqueado."""
    _sync_jobs["siigo"] = {"running": False, "ok": None, "msg": "", "step": "", "result": None}
    return {"ok": True}


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

    # Asegurar índice único por posición en sheet para upsert
    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_mov_sheet_pos
        ON movimientos_bancarios (sheet_tab, sheet_row)
    """)
    conn.commit()

    ins = upd = skip = 0

    for tab_name, banco in SHEET_TABS:
        rows = _fetch_sheet_tab(service, tab_name)
        if not rows or len(rows) < 2:
            continue

        is_bdb = banco == "BDB"

        for row_idx, row in enumerate(rows[1:], start=2):
            row = list(row) + [""] * 10

            if is_bdb:
                fecha_raw = row[0]
                desc_raw  = row[1]
                debito    = _clean_valor_sheet(row[2])
                credito   = _clean_valor_sheet(row[3])
                estado    = str(row[4]).strip() if row[4] else None
                rc_sheet  = str(row[5]).strip() if row[5] else None
                cli_sheet = str(row[6]).strip() if row[6] else None
                valor = credito if credito and credito > 0 else (-(debito) if debito and debito > 0 else None)
            else:
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

            if valor <= 0:
                skip += 1
                continue

            # Auto-detect conciliado por estado del sheet
            conciliado_sheet = bool(estado and (
                "CONCILI" in estado.upper() or
                "MEDIO DE PAGO" in estado.upper() or
                "QUEDO CON MEDIO" in estado.upper()
            ))

            # Upsert por (sheet_tab, sheet_row) — siempre refleja el estado actual del sheet
            # No pisar conciliado=TRUE si ya fue conciliado manualmente en la app
            cur.execute("""
                INSERT INTO movimientos_bancarios
                  (banco, fecha, descripcion, valor, estado, rc_sheet, cliente_sheet,
                   conciliado, sheet_row, sheet_tab)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (sheet_tab, sheet_row) DO UPDATE SET
                  fecha        = EXCLUDED.fecha,
                  descripcion  = EXCLUDED.descripcion,
                  valor        = EXCLUDED.valor,
                  estado       = EXCLUDED.estado,
                  rc_sheet     = EXCLUDED.rc_sheet,
                  cliente_sheet= EXCLUDED.cliente_sheet,
                  conciliado   = CASE
                    WHEN movimientos_bancarios.conciliado = TRUE THEN TRUE
                    ELSE EXCLUDED.conciliado
                  END
            """, (banco, fecha, desc, valor, estado, rc_sheet or None, cli_sheet or None,
                  conciliado_sheet, row_idx, tab_name))

            if cur.statusmessage == "INSERT 0 1":
                ins += 1
            else:
                upd += 1

    # Marcar como conciliados los registros existentes con estado "MEDIO DE PAGO"
    cur.execute("""
        UPDATE movimientos_bancarios
        SET conciliado = TRUE
        WHERE conciliado = FALSE
          AND (
            estado ILIKE '%MEDIO DE PAGO%' OR
            estado ILIKE '%QUEDO CON MEDIO%'
          )
    """)
    marcados = cur.rowcount

    conn.commit()
    cur.execute("""
        INSERT INTO crm_sync_log (tipo, registros, nuevos, actualizados, detalle)
        VALUES ('bancos', %s, %s, %s, %s)
    """, (ins + upd + marcados, ins, upd + marcados,
          f"Movimientos bancarios: {ins} nuevos, {upd} actualizados, {skip} ignorados, {marcados} marcados conciliados por estado"))
    conn.commit()
    cur.close(); conn.close()
    return {"ok": True, "nuevos": ins, "actualizados": upd, "ignorados": skip, "marcados_conciliados": marcados}


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
    params = [list(ids)]
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
        SELECT c.siigo_id, c.cedula, c.nombre, c.id_type_code, c.person_type
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
    id_type_code = cli[3] or "13"
    person_type = cli[4] or "Person"
    cuenta = factura.get("cuenta_debito") or _cuenta_for_medio(factura.get("medio_pago", ""))
    monto = float(factura["balance"]) if float(factura.get("balance", 0)) > 0 else float(factura["total"])
    factura_ref = f"{factura.get('prefix','')}-{factura.get('numero','')}"

    payload = {
        "document": {"id": 3619},
        "date": date.today().isoformat(),
        "customer": {
            "person_type": person_type,
            "id_type": {"code": id_type_code},
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
               COALESCE(NULLIF(TRIM(c.nombre), ''), NULLIF(f.cliente_nombre, '(Sin nombre)'), f.cliente_nombre) AS cliente_nombre,
               COALESCE(c.cedula, f.cliente_cedula) AS cliente_cedula
        FROM crm_facturas f
        LEFT JOIN crm_clientes c ON c.id = f.cliente_id
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


@router.get("/sugerencias/{factura_id}")
def get_sugerencias(factura_id: int):
    """Retorna movimientos sugeridos para una factura, rankeados por proximidad de monto y fecha."""
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT id, siigo_invoice_id, balance, total, fecha, cliente_nombre
        FROM crm_facturas WHERE id = %s
    """, (factura_id,))
    row = cur.fetchone()
    if not row:
        cur.close(); conn.close()
        raise HTTPException(404, "Factura no encontrada")

    monto_ref = float(row[2]) if row[2] else float(row[3])  # balance si existe, si no total
    fecha_ref = row[4]

    # Traer movimientos no conciliados
    cur.execute("""
        SELECT id, banco, fecha, descripcion, valor, estado, rc_sheet, cliente_sheet, sheet_tab
        FROM movimientos_bancarios
        WHERE conciliado = FALSE AND valor > 0
        ORDER BY fecha DESC
        LIMIT 1000
    """)
    movimientos = cur.fetchall()
    cur.close(); conn.close()

    scored = []
    for m in movimientos:
        mov_valor = float(m[4])
        mov_fecha = m[2]

        # Score de monto: 100% si coincide exacto, cae linealmente hasta 0% con diferencia >50%
        if monto_ref > 0:
            diff_pct = abs(mov_valor - monto_ref) / monto_ref
        else:
            diff_pct = 1.0
        score_monto = max(0.0, 1.0 - diff_pct * 2)  # 50% diff → score 0

        # Score de fecha: 100% mismo día, cae a 0% con >60 días de diferencia
        if fecha_ref and mov_fecha:
            dias = abs((mov_fecha - fecha_ref).days)
            score_fecha = max(0.0, 1.0 - dias / 60)
        else:
            score_fecha = 0.0

        score_total = round(score_monto * 0.75 + score_fecha * 0.25, 3)

        if score_total < 0.1:
            continue

        diff_abs = round(mov_valor - monto_ref, 0)
        scored.append({
            "id": m[0], "banco": m[1],
            "fecha": m[2].isoformat() if m[2] else None,
            "descripcion": m[3], "valor": mov_valor,
            "estado": m[4], "rc_sheet": m[6], "cliente_sheet": m[7], "sheet_tab": m[8],
            "score": score_total,
            "score_monto": round(score_monto, 3),
            "score_fecha": round(score_fecha, 3),
            "diff_monto": diff_abs,
            "diff_pct": round(diff_pct * 100, 2),   # porcentaje real de diferencia de monto
            "match_exacto": diff_pct < 0.001,         # realmente exacto: < 0.1%
            "match_cercano": 0.001 <= diff_pct <= 0.01  # cercano: entre 0.1% y 1%
        })

    scored.sort(key=lambda x: -x["score"])
    return {"factura_id": factura_id, "monto_ref": monto_ref, "sugerencias": scored[:8]}


@router.post("/auto-conciliar")
def auto_conciliar():
    """Empareja automáticamente facturas con movimientos de monto exacto (±1%) y fecha ±30 días."""
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT id, balance, total, fecha FROM crm_facturas
        WHERE estado_pago = 'pendiente' AND movimiento_id IS NULL
    """)
    facturas = cur.fetchall()

    cur.execute("""
        SELECT id, valor, fecha, banco FROM movimientos_bancarios
        WHERE conciliado = FALSE AND valor > 0
    """)
    movimientos = cur.fetchall()

    vinculados = 0
    usados = set()

    for fac in facturas:
        fid, balance, total, fecha_fac = fac
        monto = float(balance) if balance else float(total)

        mejor = None
        mejor_dias = 9999
        for mov in movimientos:
            mid, valor, fecha_mov, banco = mov
            if mid in usados:
                continue
            valor = float(valor)
            if monto == 0:
                continue
            diff_pct = abs(valor - monto) / monto
            if diff_pct > 0.01:  # solo ±1%
                continue
            dias = abs((fecha_mov - fecha_fac).days) if fecha_fac and fecha_mov else 9999
            if dias > 30:
                continue
            if dias < mejor_dias:
                mejor = mov
                mejor_dias = dias

        if mejor:
            mid, valor, fecha_mov, banco = mejor
            cuenta = "11100501" if banco == "BDB" else ("11200502" if banco == "BANCOLOMBIA" else "11050501")
            cur.execute("""
                UPDATE movimientos_bancarios SET conciliado=TRUE, factura_id=%s WHERE id=%s
            """, (fid, mid))
            cur.execute("""
                UPDATE crm_facturas SET estado_pago='pagado', movimiento_id=%s,
                  medio_pago=COALESCE(medio_pago,%s), cuenta_debito=COALESCE(cuenta_debito,%s)
                WHERE id=%s
            """, (mid, banco, cuenta, fid))
            usados.add(mid)
            vinculados += 1

    conn.commit()
    cur.close(); conn.close()
    return {"ok": True, "vinculados": vinculados}


# ═══════════════════════════════════════════════════════════════
# ENDPOINTS — SYNC LOG
# ═══════════════════════════════════════════════════════════════

@router.get("/debug/factura-siigo")
def debug_factura_siigo():
    """Muestra los primeros campos crudos de una factura reciente de Siigo para diagnóstico."""
    try:
        from siigo import fetch_invoices
        from datetime import date, timedelta
        hoy = date.today()
        facturas = fetch_invoices((hoy - timedelta(days=30)).isoformat(), hoy.isoformat())
        if not facturas:
            return {"msg": "Sin facturas en los últimos 30 días"}
        sample = facturas[0]
        # Solo devolver campos relevantes, no todo el objeto
        return {
            "keys": list(sample.keys()),
            "payments": sample.get("payments"),
            "balance": sample.get("balance"),
            "total": sample.get("total"),
        }
    except Exception as e:
        return {"error": str(e)}


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


# ═══════════════════════════════════════════════════════════════
# TAREA 1 — CONCILIACIÓN EXTENDIDA
# ═══════════════════════════════════════════════════════════════

@router.get("/conciliados")
def get_conciliados(q: Optional[str] = None, filtro_rc: Optional[str] = None):
    """Facturas ya pagadas con info del cliente y movimiento bancario vinculado."""
    conn = get_conn()
    cur = conn.cursor()

    q_filter = ""
    params: list = []
    if q:
        q_filter = "AND (c.nombre ILIKE %s OR CAST(f.numero AS TEXT) ILIKE %s)"
        params.extend([f"%{q}%", f"%{q}%"])
    if filtro_rc == "con_rc":
        q_filter += " AND f.rc_siigo_id IS NOT NULL"
    elif filtro_rc == "sin_rc":
        q_filter += " AND f.rc_siigo_id IS NULL"

    cur.execute(f"""
        SELECT f.id, f.siigo_invoice_id, f.numero, f.prefix, f.fecha,
               f.total, f.balance, f.estado_pago, f.medio_pago, f.cuenta_debito,
               f.movimiento_id, f.rc_siigo_id, f.rc_numero, f.rc_modo_prueba,
               c.nombre AS cliente_nombre, c.cedula AS cliente_cedula,
               m.banco, m.fecha AS mov_fecha, m.valor AS mov_valor, m.descripcion AS mov_desc
        FROM crm_facturas f
        LEFT JOIN crm_clientes c ON c.id = f.cliente_id
        LEFT JOIN movimientos_bancarios m ON m.id = f.movimiento_id
        WHERE f.estado_pago = 'pagado' {q_filter}
        ORDER BY f.fecha DESC
        LIMIT 500
    """, params)

    cols = [
        "id", "siigo_invoice_id", "numero", "prefix", "fecha",
        "total", "balance", "estado_pago", "medio_pago", "cuenta_debito",
        "movimiento_id", "rc_siigo_id", "rc_numero", "rc_modo_prueba",
        "cliente_nombre", "cliente_cedula",
        "banco", "mov_fecha", "mov_valor", "mov_desc"
    ]
    result = []
    for row in cur.fetchall():
        d = dict(zip(cols, row))
        if d["fecha"]:
            d["fecha"] = d["fecha"].isoformat()
        if d["mov_fecha"]:
            d["mov_fecha"] = d["mov_fecha"].isoformat()
        if d["total"] is not None:
            d["total"] = float(d["total"])
        if d["balance"] is not None:
            d["balance"] = float(d["balance"])
        if d["mov_valor"] is not None:
            d["mov_valor"] = float(d["mov_valor"])
        # Campo combinado factura
        d["factura"] = f"{d['prefix']}-{d['numero']}" if d.get("prefix") else str(d.get("numero", ""))
        # Fecha de pago: usar mov_fecha si existe, si no fecha de la factura
        d["fecha_pago"] = d["mov_fecha"] or d["fecha"]
        # Registrado en: banco del movimiento, medio_pago manual, o "Siigo" si se pagó directo allá
        d["banco_display"] = d.get("banco") or d.get("medio_pago") or "Siigo"
        result.append(d)

    cur.close(); conn.close()
    return result


@router.post("/facturas/{factura_id}/efectivo")
def pagar_efectivo(factura_id: int):
    """Marca una factura como pagada en efectivo (sin movimiento bancario)."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id, estado_pago FROM crm_facturas WHERE id = %s", (factura_id,))
    row = cur.fetchone()
    if not row:
        cur.close(); conn.close()
        raise HTTPException(404, "Factura no encontrada")
    if row[1] == "pagado":
        cur.close(); conn.close()
        raise HTTPException(400, "La factura ya está pagada")

    cur.execute("""
        UPDATE crm_facturas
        SET estado_pago='pagado', medio_pago='Efectivo', cuenta_debito='11050501'
        WHERE id=%s
    """, (factura_id,))
    conn.commit()
    cur.close(); conn.close()
    return {"ok": True, "factura_id": factura_id}


@router.post("/facturas/{factura_id}/revertir")
def revertir_factura(factura_id: int):
    """Revierte una conciliación: vuelve la factura a pendiente y desliga el movimiento bancario."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, estado_pago, movimiento_id, rc_siigo_id, rc_modo_prueba
        FROM crm_facturas WHERE id = %s
    """, (factura_id,))
    row = cur.fetchone()
    if not row:
        cur.close(); conn.close()
        raise HTTPException(404, "Factura no encontrada")

    _, estado, mov_id, rc_id, rc_prueba = row

    if rc_id and not rc_prueba:
        cur.close(); conn.close()
        raise HTTPException(400, "No se puede revertir: ya tiene un RC real generado en Siigo. Anúlalo primero en Siigo.")

    # Desligar movimiento bancario
    if mov_id:
        cur.execute("""
            UPDATE movimientos_bancarios
            SET conciliado = FALSE, factura_id = NULL
            WHERE id = %s
        """, (mov_id,))

    # Revertir factura a pendiente
    cur.execute("""
        UPDATE crm_facturas
        SET estado_pago = 'pendiente',
            medio_pago = NULL,
            cuenta_debito = NULL,
            movimiento_id = NULL,
            rc_siigo_id = NULL,
            rc_numero = NULL,
            rc_modo_prueba = TRUE
        WHERE id = %s
    """, (factura_id,))

    conn.commit()
    cur.close(); conn.close()
    return {"ok": True}


class EditarFacturaIn(BaseModel):
    medio_pago: Optional[str] = None
    cuenta_debito: Optional[str] = None
    origen_canal: Optional[str] = None

@router.put("/facturas/{factura_id}/editar")
def editar_factura_conciliada(factura_id: int, data: EditarFacturaIn):
    """Edita campos de una factura conciliada sin revertir el pago."""
    conn = get_conn()
    cur = conn.cursor()
    sets, vals = [], []
    if data.medio_pago is not None:
        sets.append("medio_pago=%s"); vals.append(data.medio_pago)
        # Auto-actualizar cuenta_debito si no viene explícita
        if data.cuenta_debito is None:
            sets.append("cuenta_debito=%s"); vals.append(_cuenta_for_medio(data.medio_pago))
    if data.cuenta_debito is not None:
        sets.append("cuenta_debito=%s"); vals.append(data.cuenta_debito)
    if data.origen_canal is not None:
        sets.append("origen_canal=%s"); vals.append(data.origen_canal)
    if not sets:
        cur.close(); conn.close()
        return {"ok": True}
    vals.append(factura_id)
    cur.execute(f"UPDATE crm_facturas SET {', '.join(sets)} WHERE id=%s", vals)
    conn.commit()
    cur.close(); conn.close()
    return {"ok": True}


class RcMasivoIn(BaseModel):
    factura_ids: list

@router.post("/rc-masivo-seleccion")
def rc_masivo_seleccion(data: RcMasivoIn):
    """Genera RC en Siigo para las facturas indicadas (pagadas y sin RC)."""
    if not data.factura_ids:
        return {"ok": True, "exitosas": 0, "errores": []}

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, siigo_invoice_id, numero, prefix, total, balance,
               estado_pago, medio_pago, cuenta_debito, rc_siigo_id
        FROM crm_facturas
        WHERE id = ANY(%s) AND estado_pago = 'pagado' AND rc_siigo_id IS NULL
    """, (list(data.factura_ids),))
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

    return {"ok": True, "exitosas": ok_count, "errores": errors}


# ═══════════════════════════════════════════════════════════════
# TAREA 3 — ENDPOINTS CRM (dashboard, clientes, seguimientos, prospectos)
# ═══════════════════════════════════════════════════════════════

@router.get("/dashboard-crm")
def dashboard_crm():
    """Métricas generales del CRM."""
    conn = get_conn()
    cur = conn.cursor()
    hoy = date.today()
    hace_30 = (hoy - timedelta(days=30)).isoformat()
    hace_90 = (hoy - timedelta(days=90)).isoformat()

    # Clasificación por última compra (usa columna ultima_compra de crm_clientes si existe,
    # sino la calcula desde crm_facturas)
    cur.execute("""
        SELECT
          COUNT(*) FILTER (WHERE uc >= %s) AS activos,
          COUNT(*) FILTER (WHERE uc < %s AND uc >= %s) AS en_riesgo,
          COUNT(*) FILTER (WHERE uc < %s OR uc IS NULL) AS inactivos
        FROM (
          SELECT c.id,
                 COALESCE(c.ultima_compra, MAX(f.fecha)) AS uc
          FROM crm_clientes c
          LEFT JOIN crm_facturas f ON f.cliente_id = c.id
          GROUP BY c.id, c.ultima_compra
        ) sub
    """, (hace_90, hace_90, hace_30, hace_90))
    row = cur.fetchone()
    total_activos, total_riesgo, total_inactivos = (row[0] or 0), (row[1] or 0), (row[2] or 0)

    # Ticket promedio últimos 90 días
    cur.execute("""
        SELECT AVG(total) FROM crm_facturas
        WHERE fecha >= %s AND total > 0
    """, (hace_90,))
    ticket_row = cur.fetchone()
    ticket_promedio = float(ticket_row[0] or 0)

    # Top 10 clientes por total_compras
    cur.execute("""
        SELECT c.nombre,
               COALESCE(c.total_compras, SUM(f.total)) AS total,
               COALESCE(c.num_facturas, COUNT(f.id)) AS num_facturas,
               COALESCE(c.ultima_compra, MAX(f.fecha)) AS ultima_compra,
               c.segmento
        FROM crm_clientes c
        LEFT JOIN crm_facturas f ON f.cliente_id = c.id
        GROUP BY c.id
        ORDER BY total DESC NULLS LAST
        LIMIT 10
    """)
    top_clientes = [
        {
            "nombre": r[0],
            "total": float(r[1] or 0),
            "total_compras": float(r[1] or 0),
            "num_facturas": r[2] or 0,
            "ultima_compra": r[3].isoformat() if r[3] else None,
            "segmento": r[4]
        }
        for r in cur.fetchall()
    ]

    # Por segmento
    cur.execute("""
        SELECT segmento, COUNT(*) AS count,
               COALESCE(SUM(total_compras), 0) AS total_ventas
        FROM crm_clientes
        GROUP BY segmento
        ORDER BY count DESC
    """)
    por_segmento = [
        {"segmento": r[0], "count": r[1], "total_ventas": float(r[2] or 0)}
        for r in cur.fetchall()
    ]

    # Por canal
    cur.execute("""
        SELECT canal_adquisicion, COUNT(*) AS count
        FROM crm_clientes
        GROUP BY canal_adquisicion
        ORDER BY count DESC
    """)
    por_canal = [{"canal": r[0], "count": r[1]} for r in cur.fetchall()]

    # Clientes en riesgo (sin compra 30-90 días)
    cur.execute("""
        SELECT c.id, c.nombre, c.telefono,
               COALESCE(c.ultima_compra, MAX(f.fecha)) AS uc,
               COALESCE(c.total_compras, SUM(f.total)) AS total
        FROM crm_clientes c
        LEFT JOIN crm_facturas f ON f.cliente_id = c.id
        GROUP BY c.id
        HAVING COALESCE(c.ultima_compra, MAX(f.fecha)) < %s
           AND COALESCE(c.ultima_compra, MAX(f.fecha)) >= %s
        ORDER BY uc ASC
        LIMIT 20
    """, (hace_30, hace_90))
    clientes_en_riesgo = []
    for r in cur.fetchall():
        uc = r[3]
        dias = (hoy - uc).days if uc else None
        clientes_en_riesgo.append({
            "id": r[0], "nombre": r[1], "telefono": r[2],
            "ultima_compra": uc.isoformat() if uc else None,
            "dias_sin_compra": dias,
            "total_historico": float(r[4] or 0),
            "total_compras": float(r[4] or 0),
        })

    cur.close(); conn.close()
    return {
        "total_clientes_activos": total_activos,
        "total_clientes_riesgo": total_riesgo,
        "total_clientes_inactivos": total_inactivos,
        "ticket_promedio": round(ticket_promedio, 2),
        "top_clientes": top_clientes,
        "por_segmento": por_segmento,
        "por_canal": por_canal,
        "clientes_en_riesgo": clientes_en_riesgo,
    }


@router.get("/clientes-crm")
def get_clientes_crm(
    q: Optional[str] = None,
    segmento: Optional[str] = None,
    estado: Optional[str] = None,
    page: int = 1,
    page_size: int = 50,
    limit: Optional[int] = None,
    offset: Optional[int] = None,
):
    """Lista de clientes CRM con filtros y paginación."""
    conn = get_conn()
    cur = conn.cursor()

    conditions = ["1=1"]
    params: list = []
    if q:
        conditions.append("(c.nombre ILIKE %s OR c.cedula ILIKE %s OR c.telefono ILIKE %s)")
        params.extend([f"%{q}%", f"%{q}%", f"%{q}%"])
    if segmento:
        conditions.append("c.segmento = %s")
        params.append(segmento)
    if estado:
        conditions.append("c.estado_cliente = %s")
        params.append(estado)

    where = " AND ".join(conditions)
    # Soportar limit/offset directo (frontend) o page/page_size
    if limit is not None:
        _limit = limit
        _offset = offset if offset is not None else 0
    else:
        _limit = page_size
        _offset = (page - 1) * page_size

    cur.execute(f"SELECT COUNT(*) FROM crm_clientes c WHERE {where}", params)
    total_count = cur.fetchone()[0]

    cur.execute(f"""
        SELECT c.id, c.siigo_id, c.cedula, c.nombre, c.telefono, c.email,
               c.ciudad, c.segmento, c.canal_adquisicion, c.estado_cliente,
               c.ultima_compra, c.total_compras, c.num_facturas,
               c.responsable, c.cupo_credito, c.dias_credito, c.notas_crm
        FROM crm_clientes c
        WHERE {where}
        ORDER BY c.nombre ASC
        LIMIT %s OFFSET %s
    """, params + [_limit, _offset])

    cols = [
        "id", "siigo_id", "cedula", "nombre", "telefono", "email",
        "ciudad", "segmento", "canal_adquisicion", "estado_cliente",
        "ultima_compra", "total_compras", "num_facturas",
        "responsable", "cupo_credito", "dias_credito", "notas_crm"
    ]
    clientes = []
    for row in cur.fetchall():
        d = dict(zip(cols, row))
        if d["ultima_compra"]:
            d["ultima_compra"] = d["ultima_compra"].isoformat()
        if d["total_compras"] is not None:
            d["total_compras"] = float(d["total_compras"])
        if d["cupo_credito"] is not None:
            d["cupo_credito"] = float(d["cupo_credito"])
        # Aliases para compatibilidad frontend
        d["estado"] = d.get("estado_cliente")
        d["origen_canal"] = d.get("canal_adquisicion")
        d["total_ventas"] = d.get("total_compras")
        clientes.append(d)

    cur.close(); conn.close()
    return {
        "items": clientes,
        "total": total_count,
        "clientes": clientes,
        "total_count": total_count,
        "page": page,
        "page_size": page_size,
    }


@router.get("/clientes-crm/{cliente_id}")
def get_cliente_crm(cliente_id: int):
    """Ficha completa del cliente CRM."""
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT id, siigo_id, cedula, nombre, telefono, email, direccion, ciudad,
               segmento, canal_adquisicion, estado_cliente, ultima_compra,
               total_compras, num_facturas, responsable, cupo_credito, dias_credito,
               notas_crm, origen_canal, notas, activo
        FROM crm_clientes WHERE id = %s
    """, (cliente_id,))
    row = cur.fetchone()
    if not row:
        cur.close(); conn.close()
        raise HTTPException(404, "Cliente no encontrado")

    cols = [
        "id", "siigo_id", "cedula", "nombre", "telefono", "email", "direccion", "ciudad",
        "segmento", "canal_adquisicion", "estado_cliente", "ultima_compra",
        "total_compras", "num_facturas", "responsable", "cupo_credito", "dias_credito",
        "notas_crm", "origen_canal", "notas", "activo"
    ]
    cliente = dict(zip(cols, row))
    if cliente["ultima_compra"]:
        cliente["ultima_compra"] = cliente["ultima_compra"].isoformat()
    if cliente["total_compras"] is not None:
        cliente["total_compras"] = float(cliente["total_compras"])
    if cliente["cupo_credito"] is not None:
        cliente["cupo_credito"] = float(cliente["cupo_credito"])

    # Últimas 20 facturas
    cur.execute("""
        SELECT id, siigo_invoice_id, numero, prefix, fecha, total, balance,
               estado_pago, medio_pago, rc_numero
        FROM crm_facturas WHERE cliente_id = %s
        ORDER BY fecha DESC LIMIT 20
    """, (cliente_id,))
    facturas = []
    for f in cur.fetchall():
        facturas.append({
            "id": f[0], "siigo_invoice_id": f[1], "numero": f[2], "prefix": f[3],
            "factura": f"{f[3]}-{f[2]}" if f[3] else str(f[2]),
            "fecha": f[4].isoformat() if f[4] else None,
            "total": float(f[5] or 0), "balance": float(f[6] or 0),
            "estado_pago": f[7], "medio_pago": f[8], "rc_numero": f[9],
        })

    # Últimos 10 seguimientos
    cur.execute("""
        SELECT id, tipo, descripcion, resultado, fecha, responsable,
               proxima_accion, proxima_fecha
        FROM crm_seguimientos WHERE cliente_id = %s
        ORDER BY fecha DESC LIMIT 10
    """, (cliente_id,))
    seguimientos = []
    for s in cur.fetchall():
        seguimientos.append({
            "id": s[0], "tipo": s[1], "descripcion": s[2], "resultado": s[3],
            "fecha": s[4].isoformat() if s[4] else None,
            "responsable": s[5], "proxima_accion": s[6],
            "proxima_fecha": s[7].isoformat() if s[7] else None,
        })

    # Resumen calculado
    cur.execute("""
        SELECT SUM(total), COUNT(*), MAX(fecha)
        FROM crm_facturas WHERE cliente_id = %s
    """, (cliente_id,))
    res = cur.fetchone()
    total_compras_calc = float(res[0] or 0)
    num_facturas_calc = res[1] or 0
    ultima_compra_calc = res[2]
    dias_sin_compra = (date.today() - ultima_compra_calc).days if ultima_compra_calc else None

    # Aliases para compatibilidad con frontend
    cliente["estado"] = cliente.get("estado_cliente")
    cliente["origen_canal"] = cliente.get("canal_adquisicion")

    cur.close(); conn.close()
    return {
        **cliente,
        "facturas": facturas,
        "seguimientos": seguimientos,
        "resumen": {
            "total_compras": total_compras_calc,
            "num_facturas": num_facturas_calc,
            "ultima_compra": ultima_compra_calc.isoformat() if ultima_compra_calc else None,
            "dias_sin_compra": dias_sin_compra,
        }
    }


class ClienteCrmUpdate(BaseModel):
    segmento: Optional[str] = None
    canal_adquisicion: Optional[str] = None
    estado_cliente: Optional[str] = None
    responsable: Optional[str] = None
    cupo_credito: Optional[float] = None
    dias_credito: Optional[int] = None
    notas_crm: Optional[str] = None

@router.put("/clientes-crm/{cliente_id}")
def update_cliente_crm(cliente_id: int, data: ClienteCrmUpdate):
    """Actualiza campos CRM de un cliente."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id FROM crm_clientes WHERE id = %s", (cliente_id,))
    if not cur.fetchone():
        cur.close(); conn.close()
        raise HTTPException(404, "Cliente no encontrado")

    sets = []
    vals = []
    for field in ("segmento", "canal_adquisicion", "estado_cliente", "responsable",
                  "cupo_credito", "dias_credito", "notas_crm"):
        v = getattr(data, field)
        if v is not None:
            sets.append(f"{field}=%s"); vals.append(v)

    if sets:
        sets.append("actualizado_en=NOW()")
        vals.append(cliente_id)
        cur.execute(f"UPDATE crm_clientes SET {', '.join(sets)} WHERE id=%s", vals)
        conn.commit()

    cur.close(); conn.close()
    return {"ok": True}


class SeguimientoIn(BaseModel):
    cliente_id: int
    tipo: str
    descripcion: str
    resultado: Optional[str] = None
    responsable: Optional[str] = None
    proxima_accion: Optional[str] = None
    proxima_fecha: Optional[str] = None

@router.post("/seguimientos")
def crear_seguimiento(data: SeguimientoIn):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id FROM crm_clientes WHERE id = %s", (data.cliente_id,))
    if not cur.fetchone():
        cur.close(); conn.close()
        raise HTTPException(404, "Cliente no encontrado")

    proxima_fecha = None
    if data.proxima_fecha:
        try:
            proxima_fecha = datetime.strptime(data.proxima_fecha, "%Y-%m-%d").date()
        except ValueError:
            pass

    cur.execute("""
        INSERT INTO crm_seguimientos
          (cliente_id, tipo, descripcion, resultado, responsable, proxima_accion, proxima_fecha)
        VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING id
    """, (data.cliente_id, data.tipo, data.descripcion, data.resultado,
          data.responsable, data.proxima_accion, proxima_fecha))
    new_id = cur.fetchone()[0]
    conn.commit()
    cur.close(); conn.close()
    return {"ok": True, "id": new_id}


@router.get("/prospectos")
def get_prospectos(estado: Optional[str] = None):
    conn = get_conn()
    cur = conn.cursor()
    where = "WHERE estado = %s" if estado else ""
    params = [estado] if estado else []
    cur.execute(f"""
        SELECT id, nombre, empresa, telefono, email, ciudad, segmento, canal,
               estado, responsable, notas, fecha_contacto, fecha_seguimiento,
               valor_potencial, convertido_cliente_id, creado_en
        FROM crm_prospectos {where}
        ORDER BY creado_en DESC
    """, params)
    cols = [
        "id", "nombre", "empresa", "telefono", "email", "ciudad", "segmento", "canal",
        "estado", "responsable", "notas", "fecha_contacto", "fecha_seguimiento",
        "valor_potencial", "convertido_cliente_id", "creado_en"
    ]
    result = []
    for row in cur.fetchall():
        d = dict(zip(cols, row))
        for f in ("fecha_contacto", "fecha_seguimiento"):
            if d[f]:
                d[f] = d[f].isoformat()
        if d["creado_en"]:
            d["creado_en"] = d["creado_en"].isoformat()
        if d["valor_potencial"] is not None:
            d["valor_potencial"] = float(d["valor_potencial"])
        result.append(d)
    cur.close(); conn.close()
    return result


class ProspectoIn(BaseModel):
    nombre: str
    empresa: Optional[str] = None
    telefono: Optional[str] = None
    email: Optional[str] = None
    direccion: Optional[str] = None
    ciudad: Optional[str] = None
    segmento: Optional[str] = None
    canal: Optional[str] = None
    estado: Optional[str] = "contactado"
    responsable: Optional[str] = None
    notas: Optional[str] = None
    fecha_contacto: Optional[str] = None
    fecha_seguimiento: Optional[str] = None
    valor_potencial: Optional[float] = 0

@router.get("/prospectos/{prospecto_id}")
def get_prospecto(prospecto_id: int):
    """Retorna un prospecto por ID."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, nombre, empresa, telefono, email, ciudad, direccion, segmento, canal,
               estado, responsable, notas, fecha_contacto, fecha_seguimiento,
               valor_potencial, convertido_cliente_id, creado_en
        FROM crm_prospectos WHERE id = %s
    """, (prospecto_id,))
    row = cur.fetchone()
    cur.close(); conn.close()
    if not row:
        raise HTTPException(404, "Prospecto no encontrado")
    cols = [
        "id", "nombre", "empresa", "telefono", "email", "ciudad", "direccion", "segmento", "canal",
        "estado", "responsable", "notas", "fecha_contacto", "fecha_seguimiento",
        "valor_potencial", "convertido_cliente_id", "creado_en"
    ]
    d = dict(zip(cols, row))
    for f in ("fecha_contacto", "fecha_seguimiento"):
        if d[f]:
            d[f] = d[f].isoformat()
    if d["creado_en"]:
        d["creado_en"] = d["creado_en"].isoformat()
    if d["valor_potencial"] is not None:
        d["valor_potencial"] = float(d["valor_potencial"])
    return d


@router.post("/prospectos")
def crear_prospecto(data: ProspectoIn):
    conn = get_conn()
    cur = conn.cursor()

    def _parse_d(s):
        if not s:
            return None
        try:
            return datetime.strptime(s, "%Y-%m-%d").date()
        except ValueError:
            return None

    cur.execute("""
        INSERT INTO crm_prospectos
          (nombre, empresa, telefono, email, direccion, ciudad, segmento, canal,
           estado, responsable, notas, fecha_contacto, fecha_seguimiento, valor_potencial)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id
    """, (
        data.nombre, data.empresa, data.telefono, data.email, data.direccion,
        data.ciudad, data.segmento, data.canal, data.estado or "contactado",
        data.responsable, data.notas,
        _parse_d(data.fecha_contacto) or date.today(),
        _parse_d(data.fecha_seguimiento), data.valor_potencial or 0
    ))
    new_id = cur.fetchone()[0]
    conn.commit()
    cur.close(); conn.close()
    return {"ok": True, "id": new_id}


class ProspectoUpdate(BaseModel):
    nombre: Optional[str] = None
    empresa: Optional[str] = None
    telefono: Optional[str] = None
    email: Optional[str] = None
    direccion: Optional[str] = None
    ciudad: Optional[str] = None
    segmento: Optional[str] = None
    canal: Optional[str] = None
    estado: Optional[str] = None
    responsable: Optional[str] = None
    notas: Optional[str] = None
    fecha_contacto: Optional[str] = None
    fecha_seguimiento: Optional[str] = None
    valor_potencial: Optional[float] = None

@router.put("/prospectos/{prospecto_id}")
def update_prospecto(prospecto_id: int, data: ProspectoUpdate):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, nombre, empresa, telefono, email, direccion, ciudad, segmento
        FROM crm_prospectos WHERE id = %s
    """, (prospecto_id,))
    pro = cur.fetchone()
    if not pro:
        cur.close(); conn.close()
        raise HTTPException(404, "Prospecto no encontrado")

    def _parse_d(s):
        if not s:
            return None
        try:
            return datetime.strptime(s, "%Y-%m-%d").date()
        except ValueError:
            return None

    sets = []
    vals = []
    for field in ("nombre", "empresa", "telefono", "email", "direccion", "ciudad",
                  "segmento", "canal", "estado", "responsable", "notas", "valor_potencial"):
        v = getattr(data, field)
        if v is not None:
            sets.append(f"{field}=%s"); vals.append(v)
    for field in ("fecha_contacto", "fecha_seguimiento"):
        v = getattr(data, field)
        if v is not None:
            parsed = _parse_d(v)
            sets.append(f"{field}=%s"); vals.append(parsed)
    if sets:
        sets.append("actualizado_en=NOW()")
        vals.append(prospecto_id)
        cur.execute(f"UPDATE crm_prospectos SET {', '.join(sets)} WHERE id=%s", vals)

    # Si se convierte, crear cliente en crm_clientes
    if data.estado == "convertido":
        cur.execute("SELECT nombre, empresa, telefono, email, direccion, ciudad, segmento FROM crm_prospectos WHERE id=%s", (prospecto_id,))
        p = cur.fetchone()
        if p:
            nombre_cli = p[0]
            cur.execute("""
                INSERT INTO crm_clientes (nombre, telefono, email, direccion, ciudad, segmento)
                VALUES (%s,%s,%s,%s,%s,%s) RETURNING id
            """, (nombre_cli, p[2], p[3], p[4], p[5], p[6]))
            cli_id = cur.fetchone()[0]
            cur.execute("""
                UPDATE crm_prospectos SET convertido_cliente_id=%s WHERE id=%s
            """, (cli_id, prospecto_id))

    conn.commit()
    cur.close(); conn.close()
    return {"ok": True}


@router.get("/clientes-crm/{cliente_id}/actualizar-stats")
def actualizar_stats_cliente(cliente_id: int):
    """Recalcula ultima_compra, total_compras y num_facturas desde crm_facturas."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id FROM crm_clientes WHERE id = %s", (cliente_id,))
    if not cur.fetchone():
        cur.close(); conn.close()
        raise HTTPException(404, "Cliente no encontrado")

    cur.execute("""
        SELECT MAX(fecha), SUM(total), COUNT(*)
        FROM crm_facturas WHERE cliente_id = %s
    """, (cliente_id,))
    row = cur.fetchone()
    ultima_compra = row[0]
    total_compras = float(row[1] or 0)
    num_facturas = row[2] or 0

    cur.execute("""
        UPDATE crm_clientes
        SET ultima_compra=%s, total_compras=%s, num_facturas=%s, actualizado_en=NOW()
        WHERE id=%s
    """, (ultima_compra, total_compras, num_facturas, cliente_id))
    conn.commit()
    cur.close(); conn.close()
    return {
        "ok": True,
        "cliente_id": cliente_id,
        "ultima_compra": ultima_compra.isoformat() if ultima_compra else None,
        "total_compras": total_compras,
        "num_facturas": num_facturas,
    }
