"""Siigo accounting sync — journals → trial balance → financial statements."""

import json
from collections import defaultdict
from datetime import datetime
from database import get_conn
from siigo import get_token, _headers, _paginate, SIIGO_BASE
import requests

# PUC classification for Colombian chart of accounts
PUC_CLASES = {
    '1': ('Activo', 'debit'),
    '2': ('Pasivo', 'credit'),
    '3': ('Patrimonio', 'credit'),
    '4': ('Ingresos', 'credit'),
    '5': ('Gastos', 'debit'),
    '6': ('Costo de Ventas', 'debit'),
    '7': ('Costos de Producción', 'debit'),
    '8': ('Cuentas de Orden Deudoras', 'debit'),
    '9': ('Cuentas de Orden Acreedoras', 'credit'),
}

PUC_GRUPOS = {
    '11': 'Efectivo y equivalentes', '12': 'Inversiones', '13': 'Deudores',
    '14': 'Inventarios', '15': 'Propiedad planta y equipo', '16': 'Intangibles',
    '17': 'Diferidos', '18': 'Otros activos',
    '21': 'Obligaciones financieras', '22': 'Proveedores', '23': 'Cuentas por pagar',
    '24': 'Impuestos por pagar', '25': 'Obligaciones laborales', '26': 'Pasivos estimados',
    '27': 'Diferidos pasivo', '28': 'Otros pasivos', '29': 'Bonos y papeles comerciales',
    '31': 'Capital social', '32': 'Superávit de capital', '33': 'Reservas',
    '34': 'Revalorización del patrimonio', '36': 'Resultados del ejercicio',
    '37': 'Resultados de ejercicios anteriores', '38': 'Superávit por valorizaciones',
    '41': 'Ingresos operacionales', '42': 'Ingresos no operacionales',
    '51': 'Gastos operacionales de administración', '52': 'Gastos operacionales de ventas',
    '53': 'Gastos no operacionales', '54': 'Impuesto de renta',
    '61': 'Costo de ventas', '62': 'Compras', '71': 'Costos de producción',
    '81': 'Derechos contingentes', '82': 'Deudoras fiscales', '83': 'Deudoras de control',
    '91': 'Responsabilidades contingentes', '92': 'Acreedoras fiscales',
    '93': 'Acreedoras de control',
}


def classify_account(code: str):
    """Classify a PUC account code."""
    clase_code = code[0] if code else '0'
    grupo_code = code[:2] if len(code) >= 2 else code
    clase_info = PUC_CLASES.get(clase_code, ('Otra', 'debit'))
    grupo_name = PUC_GRUPOS.get(grupo_code, '')
    return {
        'clase': clase_info[0],
        'naturaleza': clase_info[1],
        'grupo_puc': grupo_name
    }


def sync_journals():
    """
    Download accounting data from Siigo.
    Strategy:
    - Journals + Vouchers = real accounting entries with PUC codes (the source of truth)
    - Invoices = revenue detail (used for sales analytics, NOT for accounting entries —
      the journals/vouchers already contain the accounting impact of invoices)
    - Purchases = cost detail (same — already reflected in journals)
    """
    conn = get_conn()
    cur = conn.cursor()

    # Clear old synthetic entries (INV_, PUR_, CN_ prefixed) from previous bad sync
    cur.execute("DELETE FROM siigo_journals WHERE id LIKE 'INV_%' OR id LIKE 'PUR_%' OR id LIKE 'CN_%'")

    cuentas_seen = {}
    total = 0

    def process_doc(doc, source):
        nonlocal total
        jid = f"{source}_{doc['id']}"
        fecha = doc.get('date', '')[:10]
        items = doc.get('items', [])
        if not fecha or not items:
            return
        # Only items with account codes (real accounting entries)
        normalized = [item for item in items if 'account' in item and 'code' in item.get('account', {})]
        if not normalized:
            return
        cur.execute("""
            INSERT INTO siigo_journals (id, name, fecha, items, synced_at)
            VALUES (%s, %s, %s, %s, NOW())
            ON CONFLICT (id) DO UPDATE SET name=%s, fecha=%s, items=%s, synced_at=NOW()
        """, (jid, doc.get('name', ''), fecha, json.dumps(normalized),
              doc.get('name', ''), fecha, json.dumps(normalized)))
        for item in normalized:
            code = item['account']['code']
            if code not in cuentas_seen:
                cuentas_seen[code] = classify_account(code)
        total += 1

    # 1. Journals (manual entries, adjustments, closing entries)
    for doc in _paginate("/journals"):
        process_doc(doc, "JRN")

    # 2. Vouchers (cash receipts — have real account codes)
    for doc in _paginate("/vouchers"):
        process_doc(doc, "VCH")

    # Upsert accounts
    for code, info in cuentas_seen.items():
        cur.execute("""
            INSERT INTO siigo_cuentas (codigo, clase, grupo_puc, naturaleza)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (codigo) DO UPDATE SET clase=%s, grupo_puc=%s, naturaleza=%s
        """, (code, info['clase'], info['grupo_puc'], info['naturaleza'],
              info['clase'], info['grupo_puc'], info['naturaleza']))

    # Rebuild monthly balances
    _rebuild_saldos_mensuales(cur)

    # Log
    cur.execute("INSERT INTO sync_log (tipo, registros, detalle) VALUES ('full_sync', %s, %s)",
                (total, f"{len(cuentas_seen)} cuentas"))
    conn.commit()
    cur.close()
    conn.close()
    return {"registros": total, "cuentas": len(cuentas_seen)}


def _rebuild_saldos_mensuales(cur):
    """Rebuild monthly balances from all stored journals."""
    cur.execute("DELETE FROM saldos_mensuales")

    cur.execute("SELECT fecha, items FROM siigo_journals ORDER BY fecha")
    saldos = defaultdict(lambda: defaultdict(lambda: {'debito': 0, 'credito': 0}))

    for fecha, items_json in cur.fetchall():
        if not fecha:
            continue
        anio = fecha.year
        mes = fecha.month
        items = json.loads(items_json) if isinstance(items_json, str) else items_json
        for item in items:
            code = item['account']['code']
            val = float(item['value'])
            mov = item['account']['movement']
            key = (code, anio, mes)
            if mov == 'Debit':
                saldos[code][(anio, mes)]['debito'] += val
            else:
                saldos[code][(anio, mes)]['credito'] += val

    for code, periodos in saldos.items():
        for (anio, mes), vals in periodos.items():
            debito = round(vals['debito'], 2)
            credito = round(vals['credito'], 2)
            # Get naturaleza
            info = classify_account(code)
            if info['naturaleza'] == 'debit':
                saldo = debito - credito
            else:
                saldo = credito - debito
            cur.execute("""
                INSERT INTO saldos_mensuales (cuenta, anio, mes, debito, credito, saldo)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (code, anio, mes, debito, credito, saldo))


def get_balance_prueba(anio: int, mes: int):
    """Get trial balance for a specific month (movements of that month only)."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT s.cuenta, COALESCE(c.nombre, s.cuenta), c.clase, c.grupo_puc, c.naturaleza,
               s.debito, s.credito, s.saldo
        FROM saldos_mensuales s
        LEFT JOIN siigo_cuentas c ON c.codigo = s.cuenta
        WHERE s.anio = %s AND s.mes = %s
        ORDER BY s.cuenta
    """, (anio, mes))
    rows = cur.fetchall()
    cur.close(); conn.close()

    result = []
    total_debito = 0
    total_credito = 0
    for r in rows:
        total_debito += float(r[5])
        total_credito += float(r[6])
        result.append({
            "cuenta": r[0], "nombre": r[1] or r[0], "clase": r[2],
            "grupo_puc": r[3], "naturaleza": r[4],
            "debito": float(r[5]), "credito": float(r[6]), "saldo": float(r[7])
        })
    return {
        "anio": anio, "mes": mes,
        "cuentas": result,
        "total_debito": round(total_debito, 2),
        "total_credito": round(total_credito, 2)
    }


def get_estado_resultados(anio: int, mes_inicio: int = 1, mes_fin: int = 12):
    """
    P&L combining:
    - Revenue from Siigo invoices (accurate monthly)
    - Costs/expenses from journal saldos (5xxx, 6xxx)
    """
    from siigo import fetch_invoices

    # 1. Revenue from invoices
    start = f"{anio}-{mes_inicio:02d}-01"
    last_day = 28 if mes_fin == 2 else 30 if mes_fin in (4,6,9,11) else 31
    end = f"{anio}-{mes_fin:02d}-{last_day}"
    invoices = fetch_invoices(start, end)
    total_ingresos = sum(float(inv.get('total', 0)) for inv in invoices if not inv.get('annulled'))

    # 2. Costs and expenses from accounting saldos
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT s.cuenta, COALESCE(c.nombre, s.cuenta), c.clase, c.grupo_puc,
               SUM(s.debito), SUM(s.credito), SUM(s.saldo)
        FROM saldos_mensuales s
        LEFT JOIN siigo_cuentas c ON c.codigo = s.cuenta
        WHERE s.anio = %s AND s.mes BETWEEN %s AND %s
          AND s.cuenta LIKE ANY(ARRAY['5%%','6%%','7%%'])
        GROUP BY s.cuenta, c.nombre, c.clase, c.grupo_puc
        ORDER BY s.cuenta
    """, (anio, mes_inicio, mes_fin))
    rows = cur.fetchall()
    cur.close(); conn.close()

    costos = []
    gastos_admin = []
    gastos_ventas = []
    gastos_no_op = []

    for r in rows:
        item = {"cuenta": r[0], "nombre": r[1] or r[0], "clase": r[2],
                "grupo_puc": r[3], "saldo": abs(float(r[6]))}
        prefix = r[0][:2]
        if prefix in ('61', '62'):
            costos.append(item)
        elif prefix == '51':
            gastos_admin.append(item)
        elif prefix == '52':
            gastos_ventas.append(item)
        elif prefix in ('53', '54', '71'):
            gastos_no_op.append(item)

    total_costos = sum(i['saldo'] for i in costos)
    total_gastos_admin = sum(i['saldo'] for i in gastos_admin)
    total_gastos_ventas = sum(i['saldo'] for i in gastos_ventas)
    total_gastos_no_op = sum(i['saldo'] for i in gastos_no_op)
    utilidad_bruta = total_ingresos - total_costos
    utilidad_operacional = utilidad_bruta - total_gastos_admin - total_gastos_ventas
    utilidad_neta = utilidad_operacional - total_gastos_no_op

    ingresos_items = [{"cuenta": "Facturas Siigo", "nombre": "Ingresos por ventas (facturas)",
                       "clase": "Ingresos", "grupo_puc": "Ingresos operacionales",
                       "saldo": round(total_ingresos, 2)}]

    return {
        "anio": anio, "mes_inicio": mes_inicio, "mes_fin": mes_fin,
        "ingresos": {"items": ingresos_items, "total": round(total_ingresos, 2)},
        "costos_ventas": {"items": costos, "total": round(total_costos, 2)},
        "utilidad_bruta": round(utilidad_bruta, 2),
        "margen_bruto_pct": round(utilidad_bruta / total_ingresos * 100, 1) if total_ingresos > 0 else 0,
        "gastos_admin": {"items": gastos_admin, "total": round(total_gastos_admin, 2)},
        "gastos_ventas": {"items": gastos_ventas, "total": round(total_gastos_ventas, 2)},
        "utilidad_operacional": round(utilidad_operacional, 2),
        "margen_operacional_pct": round(utilidad_operacional / total_ingresos * 100, 1) if total_ingresos > 0 else 0,
        "gastos_no_operacionales": {"items": gastos_no_op, "total": round(total_gastos_no_op, 2)},
        "utilidad_neta": round(utilidad_neta, 2),
        "margen_neto_pct": round(utilidad_neta / total_ingresos * 100, 1) if total_ingresos > 0 else 0,
    }


def get_indicadores(anio: int, mes: int):
    """Key financial indicators combining invoices (revenue) + journals (balance/expenses)."""
    from siigo import fetch_invoices

    # Revenue from invoices (YTD)
    start = f"{anio}-01-01"
    last_day = 28 if mes == 2 else 30 if mes in (4,6,9,11) else 31
    end = f"{anio}-{mes:02d}-{last_day}"
    invoices = fetch_invoices(start, end)
    ingresos = sum(float(inv.get('total', 0)) for inv in invoices if not inv.get('annulled'))

    # Balance and expenses from journal saldos
    conn = get_conn()
    cur = conn.cursor()

    # Cumulative balance sheet accounts (1xxx, 2xxx, 3xxx)
    cur.execute("""
        SELECT s.cuenta, SUM(s.debito) as total_d, SUM(s.credito) as total_c
        FROM saldos_mensuales s
        WHERE (s.anio < %s) OR (s.anio = %s AND s.mes <= %s)
        GROUP BY s.cuenta
    """, (anio, anio, mes))
    raw = {r[0]: {"debito": float(r[1]), "credito": float(r[2])} for r in cur.fetchall()}

    # Expenses YTD from saldos
    cur.execute("""
        SELECT SUM(CASE WHEN s.cuenta LIKE '6%%' THEN ABS(s.saldo) ELSE 0 END),
               SUM(CASE WHEN s.cuenta LIKE '5%%' THEN ABS(s.saldo) ELSE 0 END)
        FROM saldos_mensuales s
        WHERE s.anio = %s AND s.mes <= %s
    """, (anio, mes))
    row = cur.fetchone()
    costos = float(row[0] or 0)
    gastos = float(row[1] or 0)
    cur.close(); conn.close()

    # Calculate balance from raw debits/credits
    def balance_grupo(prefixes):
        total = 0
        for code, vals in raw.items():
            if any(code.startswith(p) for p in prefixes):
                info = classify_account(code)
                if info['naturaleza'] == 'debit':
                    total += vals['debito'] - vals['credito']
                else:
                    total += vals['credito'] - vals['debito']
        return total

    activo_corriente = balance_grupo(['11', '12', '13', '14'])
    activo_total = balance_grupo(['1'])
    pasivo_corriente = balance_grupo(['21', '22', '23', '24', '25'])
    pasivo_total = balance_grupo(['2'])
    patrimonio = balance_grupo(['3'])

    utilidad_bruta = ingresos - costos
    utilidad_operacional = utilidad_bruta - gastos

    return {
        "anio": anio, "mes": mes,
        "balance": {
            "activo_corriente": round(activo_corriente, 2),
            "pasivo_corriente": round(pasivo_corriente, 2),
            "activo_total": round(activo_total, 2),
            "pasivo_total": round(pasivo_total, 2),
            "patrimonio": round(patrimonio, 2),
        },
        "resultados": {
            "ingresos": round(ingresos, 2),
            "costos": round(costos, 2),
            "gastos": round(gastos, 2),
            "utilidad_bruta": round(utilidad_bruta, 2),
            "utilidad_operacional": round(utilidad_operacional, 2),
            "ebitda": round(utilidad_operacional, 2),
        },
        "ratios": {
            "liquidez": round(activo_corriente / pasivo_corriente, 2) if pasivo_corriente > 0 else 0,
            "endeudamiento_pct": round(pasivo_total / activo_total * 100, 1) if activo_total > 0 else 0,
            "margen_bruto_pct": round(utilidad_bruta / ingresos * 100, 1) if ingresos > 0 else 0,
            "margen_operacional_pct": round(utilidad_operacional / ingresos * 100, 1) if ingresos > 0 else 0,
            "margen_neto_pct": round(utilidad_operacional / ingresos * 100, 1) if ingresos > 0 else 0,
            "roe_pct": round(utilidad_operacional / patrimonio * 100, 1) if patrimonio > 0 else 0,
            "roa_pct": round(utilidad_operacional / activo_total * 100, 1) if activo_total > 0 else 0,
        }
    }


def get_tendencia_mensual_from_invoices(anio: int):
    """Monthly P&L trend built from invoice/purchase data (more accurate monthly view)."""
    from siigo import fetch_invoices
    import time

    meses = {}
    for mes in range(1, 13):
        meses[mes] = {"ingresos": 0, "costos": 0, "gastos": 0}

    # Invoices = revenue
    start = f"{anio}-01-01"
    end = f"{anio}-12-31"
    invoices = fetch_invoices(start, end)
    for inv in invoices:
        if inv.get('annulled'):
            continue
        fecha = inv.get('date', '')
        if not fecha:
            continue
        mes = int(fecha[5:7])
        meses[mes]["ingresos"] += float(inv.get('total', 0))

    # Gastos from journals (real accounting entries for expenses - 5xxx)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT s.mes, SUM(s.saldo)
        FROM saldos_mensuales s
        WHERE s.anio = %s AND s.cuenta LIKE '5%%'
        GROUP BY s.mes
    """, (anio,))
    for row in cur.fetchall():
        meses[row[0]]["gastos"] = abs(float(row[1]))

    # Costos from saldos (6xxx) — but only if they are spread monthly
    # If not, use purchases as proxy
    cur.execute("""
        SELECT s.mes, SUM(s.saldo)
        FROM saldos_mensuales s
        WHERE s.anio = %s AND s.cuenta LIKE '6%%'
        GROUP BY s.mes
    """, (anio,))
    has_monthly_costs = False
    for row in cur.fetchall():
        val = abs(float(row[1]))
        if val > 0:
            meses[row[0]]["costos"] = val
            has_monthly_costs = True

    cur.close()
    conn.close()

    result = []
    for mes in range(1, 13):
        m = meses[mes]
        if m["ingresos"] == 0 and m["costos"] == 0 and m["gastos"] == 0:
            continue
        utilidad_bruta = m["ingresos"] - m["costos"]
        utilidad_neta = utilidad_bruta - m["gastos"]
        result.append({
            "mes": mes,
            "ingresos": round(m["ingresos"], 2),
            "costos": round(m["costos"], 2),
            "gastos": round(m["gastos"], 2),
            "utilidad_bruta": round(utilidad_bruta, 2),
            "utilidad_neta": round(utilidad_neta, 2),
            "margen_bruto_pct": round(utilidad_bruta / m["ingresos"] * 100, 1) if m["ingresos"] > 0 else 0,
            "margen_neto_pct": round(utilidad_neta / m["ingresos"] * 100, 1) if m["ingresos"] > 0 else 0,
        })
    return {"anio": anio, "meses": result}


def get_tendencia_mensual(anio: int):
    """Monthly P&L trend for a full year."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT s.mes,
               SUM(CASE WHEN s.cuenta LIKE '4%%' THEN s.saldo ELSE 0 END) as ingresos,
               SUM(CASE WHEN s.cuenta LIKE '6%%' THEN s.saldo ELSE 0 END) as costos,
               SUM(CASE WHEN s.cuenta LIKE '5%%' THEN s.saldo ELSE 0 END) as gastos
        FROM saldos_mensuales s
        WHERE s.anio = %s
        GROUP BY s.mes ORDER BY s.mes
    """, (anio,))
    rows = cur.fetchall()
    cur.close(); conn.close()

    meses = []
    for r in rows:
        ingresos = abs(float(r[1]))
        costos = abs(float(r[2]))
        gastos = abs(float(r[3]))
        utilidad_bruta = ingresos - costos
        utilidad_neta = utilidad_bruta - gastos
        meses.append({
            "mes": r[0],
            "ingresos": round(ingresos, 2),
            "costos": round(costos, 2),
            "gastos": round(gastos, 2),
            "utilidad_bruta": round(utilidad_bruta, 2),
            "utilidad_neta": round(utilidad_neta, 2),
            "margen_bruto_pct": round(utilidad_bruta / ingresos * 100, 1) if ingresos > 0 else 0,
            "margen_neto_pct": round(utilidad_neta / ingresos * 100, 1) if ingresos > 0 else 0,
        })
    return {"anio": anio, "meses": meses}


def get_presupuesto_vs_real(anio: int, mes: int):
    """Compare budget vs actual for a given month."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT p.cuenta, p.cuenta_nombre, p.monto AS presupuesto,
               COALESCE(s.saldo, 0) AS real,
               c.clase, c.grupo_puc
        FROM presupuestos p
        LEFT JOIN saldos_mensuales s ON s.cuenta = p.cuenta AND s.anio = p.anio AND s.mes = p.mes
        LEFT JOIN siigo_cuentas c ON c.codigo = p.cuenta
        WHERE p.anio = %s AND p.mes = %s
        ORDER BY p.cuenta
    """, (anio, mes))
    rows = cur.fetchall()
    cur.close(); conn.close()

    result = []
    for r in rows:
        presupuesto = float(r[2])
        real = abs(float(r[3]))
        desviacion = real - presupuesto
        desviacion_pct = (desviacion / presupuesto * 100) if presupuesto != 0 else 0
        # For expenses: over budget is bad. For revenue: under budget is bad.
        es_gasto = (r[4] or '').startswith('Gasto') or (r[4] or '').startswith('Costo')
        alerta = ''
        if es_gasto and desviacion_pct > 10:
            alerta = 'sobre_presupuesto'
        elif not es_gasto and desviacion_pct < -10:
            alerta = 'bajo_presupuesto'
        result.append({
            "cuenta": r[0], "nombre": r[1] or r[0],
            "presupuesto": round(presupuesto, 2), "real": round(real, 2),
            "desviacion": round(desviacion, 2),
            "desviacion_pct": round(desviacion_pct, 1),
            "clase": r[4], "grupo_puc": r[5], "alerta": alerta
        })
    return {"anio": anio, "mes": mes, "items": result}
