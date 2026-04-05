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
    """Download all journals from Siigo and store in local DB."""
    all_journals = _paginate("/journals")

    conn = get_conn()
    cur = conn.cursor()
    cuentas_seen = {}
    count = 0

    for j in all_journals:
        jid = j['id']
        fecha = j.get('date', '')[:10]
        items = j.get('items', [])

        # Upsert journal
        cur.execute("""
            INSERT INTO siigo_journals (id, name, fecha, items, synced_at)
            VALUES (%s, %s, %s, %s, NOW())
            ON CONFLICT (id) DO UPDATE SET name=%s, fecha=%s, items=%s, synced_at=NOW()
        """, (jid, j.get('name', ''), fecha, json.dumps(items),
              j.get('name', ''), fecha, json.dumps(items)))

        # Track accounts
        for item in items:
            code = item['account']['code']
            if code not in cuentas_seen:
                info = classify_account(code)
                cuentas_seen[code] = info
        count += 1

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

    # Log sync
    cur.execute("""
        INSERT INTO sync_log (tipo, registros, detalle)
        VALUES ('journals', %s, %s)
    """, (count, f"{len(cuentas_seen)} cuentas"))

    conn.commit()
    cur.close()
    conn.close()
    return {"journals": count, "cuentas": len(cuentas_seen)}


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
    """P&L from accumulated monthly balances (accounts 4xxx, 5xxx, 6xxx, 7xxx)."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT s.cuenta, COALESCE(c.nombre, s.cuenta), c.clase, c.grupo_puc,
               SUM(s.debito), SUM(s.credito), SUM(s.saldo)
        FROM saldos_mensuales s
        LEFT JOIN siigo_cuentas c ON c.codigo = s.cuenta
        WHERE s.anio = %s AND s.mes BETWEEN %s AND %s
          AND s.cuenta LIKE ANY(ARRAY['4%%','5%%','6%%','7%%'])
        GROUP BY s.cuenta, c.nombre, c.clase, c.grupo_puc
        ORDER BY s.cuenta
    """, (anio, mes_inicio, mes_fin))
    rows = cur.fetchall()
    cur.close(); conn.close()

    ingresos = []
    costos = []
    gastos_admin = []
    gastos_ventas = []
    gastos_no_op = []
    otros = []

    for r in rows:
        item = {"cuenta": r[0], "nombre": r[1] or r[0], "clase": r[2],
                "grupo_puc": r[3], "saldo": abs(float(r[6]))}
        prefix = r[0][:2]
        if prefix in ('41', '42'):
            ingresos.append(item)
        elif prefix in ('61', '62'):
            costos.append(item)
        elif prefix == '51':
            gastos_admin.append(item)
        elif prefix == '52':
            gastos_ventas.append(item)
        elif prefix in ('53', '54'):
            gastos_no_op.append(item)
        else:
            otros.append(item)

    total_ingresos = sum(i['saldo'] for i in ingresos)
    total_costos = sum(i['saldo'] for i in costos)
    total_gastos_admin = sum(i['saldo'] for i in gastos_admin)
    total_gastos_ventas = sum(i['saldo'] for i in gastos_ventas)
    total_gastos_no_op = sum(i['saldo'] for i in gastos_no_op)
    utilidad_bruta = total_ingresos - total_costos
    utilidad_operacional = utilidad_bruta - total_gastos_admin - total_gastos_ventas
    utilidad_neta = utilidad_operacional - total_gastos_no_op

    return {
        "anio": anio, "mes_inicio": mes_inicio, "mes_fin": mes_fin,
        "ingresos": {"items": ingresos, "total": round(total_ingresos, 2)},
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
    """Key financial indicators for a given month."""
    conn = get_conn()
    cur = conn.cursor()

    # Get cumulative balances up to this month
    cur.execute("""
        SELECT s.cuenta, SUM(s.saldo) as saldo_acum
        FROM saldos_mensuales s
        WHERE (s.anio < %s) OR (s.anio = %s AND s.mes <= %s)
        GROUP BY s.cuenta
    """, (anio, anio, mes))
    saldos = {r[0]: float(r[1]) for r in cur.fetchall()}
    cur.close(); conn.close()

    # Aggregate by PUC group (first 2 digits)
    grupos = defaultdict(float)
    for code, saldo in saldos.items():
        grupos[code[:2]] += saldo
        grupos[code[:1]] += saldo  # Also class level

    # Balance indicators
    activo_corriente = sum(v for k, v in grupos.items() if k in ('11', '12', '13', '14'))
    pasivo_corriente = sum(v for k, v in grupos.items() if k in ('21', '22', '23', '24', '25'))
    activo_total = grupos.get('1', 0)
    pasivo_total = grupos.get('2', 0)
    patrimonio = grupos.get('3', 0)

    # P&L indicators (current year only)
    ingresos = abs(sum(v for k, v in saldos.items() if k.startswith('4')))
    costos = abs(sum(v for k, v in saldos.items() if k.startswith('6')))
    gastos = abs(sum(v for k, v in saldos.items() if k.startswith('5')))

    utilidad_bruta = ingresos - costos
    utilidad_operacional = utilidad_bruta - gastos
    ebitda = utilidad_operacional  # Simplified (no D&A separation available)

    return {
        "anio": anio, "mes": mes,
        "balance": {
            "activo_corriente": round(activo_corriente, 2),
            "pasivo_corriente": round(abs(pasivo_corriente), 2),
            "activo_total": round(activo_total, 2),
            "pasivo_total": round(abs(pasivo_total), 2),
            "patrimonio": round(abs(patrimonio), 2),
        },
        "resultados": {
            "ingresos": round(ingresos, 2),
            "costos": round(costos, 2),
            "gastos": round(gastos, 2),
            "utilidad_bruta": round(utilidad_bruta, 2),
            "utilidad_operacional": round(utilidad_operacional, 2),
            "ebitda": round(ebitda, 2),
        },
        "ratios": {
            "liquidez": round(activo_corriente / abs(pasivo_corriente), 2) if pasivo_corriente != 0 else 0,
            "endeudamiento_pct": round(abs(pasivo_total) / activo_total * 100, 1) if activo_total > 0 else 0,
            "margen_bruto_pct": round(utilidad_bruta / ingresos * 100, 1) if ingresos > 0 else 0,
            "margen_operacional_pct": round(utilidad_operacional / ingresos * 100, 1) if ingresos > 0 else 0,
            "margen_neto_pct": round((utilidad_operacional) / ingresos * 100, 1) if ingresos > 0 else 0,
            "roe_pct": round(utilidad_operacional / abs(patrimonio) * 100, 1) if patrimonio != 0 else 0,
            "roa_pct": round(utilidad_operacional / activo_total * 100, 1) if activo_total > 0 else 0,
        }
    }


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
