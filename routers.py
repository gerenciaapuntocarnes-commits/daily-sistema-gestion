from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, List
from datetime import date, datetime
import uuid
import psycopg2
from database import get_conn

router = APIRouter()

# ═══════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════

def _gen_lote_produccion(new_id: int) -> str:
    return f"LOT-{datetime.now().strftime('%Y%m%d')}-{new_id:06d}"

def _gen_lote_item(new_id: int) -> str:
    return f"LMP-{datetime.now().strftime('%Y%m%d')}-{new_id:06d}"

def _gen_numero_remision(new_id: int) -> str:
    return f"REM-{datetime.now().strftime('%Y%m%d')}-{new_id:06d}"

# ═══════════════════════════════════════════════════════════════
# MATERIAS PRIMAS
# ═══════════════════════════════════════════════════════════════

class MPIn(BaseModel):
    nombre: str
    codigo: Optional[str] = None
    unidad: str = "kg"
    categoria: str = "General"

class CompraIn(BaseModel):
    mp_id: int
    fecha: date
    proveedor: Optional[str] = None
    cantidad: float
    precio_unit: float
    factura: Optional[str] = None
    notas: Optional[str] = None

@router.get("/materias-primas/check-codigo")
def check_codigo(codigo: str):
    """Verifica en tiempo real si un código ya existe."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT id FROM materias_primas WHERE codigo=%s AND activo=TRUE",
        (codigo.strip(),)
    )
    existe = cur.fetchone() is not None
    cur.close(); conn.close()
    return {"existe": existe}

@router.get("/materias-primas")
def listar_mp():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT mp.id, mp.codigo, mp.nombre, mp.unidad, mp.categoria, mp.activo,
               c.precio_unit AS precio_actual,
               c.fecha       AS ultima_compra,
               c.proveedor   AS ultimo_proveedor
        FROM materias_primas mp
        LEFT JOIN LATERAL (
            SELECT precio_unit, fecha, proveedor
            FROM compras_mp
            WHERE mp_id = mp.id
            ORDER BY fecha DESC, id DESC
            LIMIT 1
        ) c ON TRUE
        WHERE mp.activo = TRUE
        ORDER BY mp.categoria, mp.nombre
    """)
    rows = cur.fetchall()
    cur.close(); conn.close()
    return [{"id": r[0], "codigo": r[1], "nombre": r[2], "unidad": r[3],
             "categoria": r[4], "activo": r[5],
             "precio_actual": float(r[6]) if r[6] else 0,
             "ultima_compra": str(r[7]) if r[7] else None,
             "ultimo_proveedor": r[8]} for r in rows]

@router.post("/materias-primas")
def crear_mp(data: MPIn):
    conn = get_conn()
    cur = conn.cursor()
    codigo = data.codigo.strip() if data.codigo and data.codigo.strip() else None
    try:
        cur.execute(
            "INSERT INTO materias_primas (codigo, nombre, unidad, categoria) VALUES (%s,%s,%s,%s) RETURNING id",
            (codigo, data.nombre, data.unidad, data.categoria)
        )
        new_id = cur.fetchone()[0]
        conn.commit()
    except psycopg2.errors.UniqueViolation:
        conn.rollback()
        raise HTTPException(409, f"El código '{codigo}' ya está en uso por otra materia prima")
    finally:
        cur.close(); conn.close()
    return {"id": new_id, "ok": True}

@router.put("/materias-primas/{mp_id}")
def editar_mp(mp_id: int, data: MPIn):
    conn = get_conn()
    cur = conn.cursor()
    codigo = data.codigo.strip() if data.codigo and data.codigo.strip() else None
    try:
        cur.execute(
            "UPDATE materias_primas SET codigo=%s, nombre=%s, unidad=%s, categoria=%s WHERE id=%s",
            (codigo, data.nombre, data.unidad, data.categoria, mp_id)
        )
        conn.commit()
    except psycopg2.errors.UniqueViolation:
        conn.rollback()
        raise HTTPException(409, f"El código '{codigo}' ya está en uso por otra materia prima")
    finally:
        cur.close(); conn.close()
    return {"ok": True}

@router.delete("/materias-primas/{mp_id}")
def eliminar_mp(mp_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE materias_primas SET activo=FALSE WHERE id=%s", (mp_id,))
    conn.commit(); cur.close(); conn.close()
    return {"ok": True}

@router.get("/compras-mp")
def listar_compras(mp_id: Optional[int] = None, limit: int = 100):
    conn = get_conn()
    cur = conn.cursor()
    if mp_id:
        cur.execute("""
            SELECT c.id, c.fecha, mp.nombre, mp.unidad, c.proveedor,
                   c.cantidad, c.precio_unit, c.cantidad*c.precio_unit AS total,
                   c.factura, c.notas
            FROM compras_mp c JOIN materias_primas mp ON mp.id=c.mp_id
            WHERE c.mp_id=%s ORDER BY c.fecha DESC LIMIT %s
        """, (mp_id, limit))
    else:
        cur.execute("""
            SELECT c.id, c.fecha, mp.nombre, mp.unidad, c.proveedor,
                   c.cantidad, c.precio_unit, c.cantidad*c.precio_unit AS total,
                   c.factura, c.notas
            FROM compras_mp c JOIN materias_primas mp ON mp.id=c.mp_id
            ORDER BY c.fecha DESC LIMIT %s
        """, (limit,))
    rows = cur.fetchall()
    cur.close(); conn.close()
    return [{"id": r[0], "fecha": str(r[1]), "mp_nombre": r[2], "unidad": r[3],
             "proveedor": r[4], "cantidad": float(r[5]), "precio_unit": float(r[6]),
             "total": float(r[7]), "factura": r[8], "notas": r[9]} for r in rows]

@router.post("/compras-mp")
def registrar_compra(data: CompraIn):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO compras_mp (mp_id, fecha, proveedor, cantidad, precio_unit, factura, notas)
        VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING id
    """, (data.mp_id, data.fecha, data.proveedor, data.cantidad,
          data.precio_unit, data.factura, data.notas))
    new_id = cur.fetchone()[0]
    conn.commit()
    # Auto-update inventory
    try:
        cur.execute("""
            INSERT INTO inventario (mp_id, cantidad_actual, updated_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (mp_id) DO UPDATE SET cantidad_actual = inventario.cantidad_actual + %s, updated_at = NOW()
        """, (data.mp_id, data.cantidad, data.cantidad))
        conn.commit()
    except:
        pass
    cur.close(); conn.close()
    return {"id": new_id, "ok": True}

@router.delete("/compras-mp/{compra_id}")
def eliminar_compra(compra_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM compras_mp WHERE id=%s", (compra_id,))
    conn.commit(); cur.close(); conn.close()
    return {"ok": True}

# ═══════════════════════════════════════════════════════════════
# RECETAS
# ═══════════════════════════════════════════════════════════════

class RecetaIn(BaseModel):
    nombre: str
    categoria: str = "General"
    descripcion: Optional[str] = None
    porciones: float = 1
    precio_venta: float = 0

class IngredienteIn(BaseModel):
    mp_id: int
    cantidad: float

@router.get("/recetas")
def listar_recetas():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT r.id, r.nombre, r.categoria, r.descripcion, r.porciones, r.precio_venta, r.activo
        FROM recetas r WHERE r.activo=TRUE ORDER BY r.categoria, r.nombre
    """)
    recetas = cur.fetchall()
    result = []
    for r in recetas:
        cur.execute("""
            SELECT COALESCE(SUM(ri.cantidad * COALESCE(ult.precio_unit,0)), 0)
            FROM receta_ingredientes ri
            LEFT JOIN LATERAL (
                SELECT precio_unit FROM compras_mp
                WHERE mp_id=ri.mp_id ORDER BY fecha DESC, id DESC LIMIT 1
            ) ult ON TRUE
            WHERE ri.receta_id=%s
        """, (r[0],))
        costo = float(cur.fetchone()[0])
        porciones = float(r[4]) if r[4] else 1
        precio_venta = float(r[5]) if r[5] else 0
        costo_porcion = costo / porciones if porciones > 0 else 0
        margen = precio_venta - costo_porcion
        margen_pct = (margen / precio_venta * 100) if precio_venta > 0 else 0
        result.append({
            "id": r[0], "nombre": r[1], "categoria": r[2], "descripcion": r[3],
            "porciones": porciones, "precio_venta": precio_venta, "activo": r[6],
            "costo_total": costo, "costo_porcion": costo_porcion,
            "margen": margen, "margen_pct": margen_pct
        })
    cur.close(); conn.close()
    return result

@router.get("/recetas/{receta_id}")
def detalle_receta(receta_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id,nombre,categoria,descripcion,porciones,precio_venta FROM recetas WHERE id=%s", (receta_id,))
    r = cur.fetchone()
    if not r:
        raise HTTPException(404, "Receta no encontrada")
    cur.execute("""
        SELECT ri.id, mp.id, mp.nombre, mp.unidad, ri.cantidad,
               COALESCE(ult.precio_unit,0) AS precio_unit,
               ri.cantidad * COALESCE(ult.precio_unit,0) AS costo_linea
        FROM receta_ingredientes ri
        JOIN materias_primas mp ON mp.id=ri.mp_id
        LEFT JOIN LATERAL (
            SELECT precio_unit FROM compras_mp
            WHERE mp_id=ri.mp_id ORDER BY fecha DESC, id DESC LIMIT 1
        ) ult ON TRUE
        WHERE ri.receta_id=%s
    """, (receta_id,))
    ingredientes = [{"id": i[0], "mp_id": i[1], "mp_nombre": i[2], "unidad": i[3],
                     "cantidad": float(i[4]), "precio_unit": float(i[5]),
                     "costo_linea": float(i[6])} for i in cur.fetchall()]
    cur.close(); conn.close()
    porciones = float(r[4]) if r[4] else 1
    precio_venta = float(r[5]) if r[5] else 0
    costo_total = sum(i["costo_linea"] for i in ingredientes)
    costo_porcion = costo_total / porciones if porciones > 0 else 0
    return {
        "id": r[0], "nombre": r[1], "categoria": r[2], "descripcion": r[3],
        "porciones": porciones, "precio_venta": precio_venta,
        "ingredientes": ingredientes, "costo_total": costo_total,
        "costo_porcion": costo_porcion,
        "margen": precio_venta - costo_porcion,
        "margen_pct": ((precio_venta - costo_porcion) / precio_venta * 100) if precio_venta > 0 else 0
    }

@router.post("/recetas")
def crear_receta(data: RecetaIn):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO recetas (nombre, categoria, descripcion, porciones, precio_venta)
        VALUES (%s,%s,%s,%s,%s) RETURNING id
    """, (data.nombre, data.categoria, data.descripcion, data.porciones, data.precio_venta))
    new_id = cur.fetchone()[0]
    conn.commit(); cur.close(); conn.close()
    return {"id": new_id, "ok": True}

@router.put("/recetas/{receta_id}")
def editar_receta(receta_id: int, data: RecetaIn):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE recetas SET nombre=%s, categoria=%s, descripcion=%s,
               porciones=%s, precio_venta=%s WHERE id=%s
    """, (data.nombre, data.categoria, data.descripcion,
          data.porciones, data.precio_venta, receta_id))
    conn.commit(); cur.close(); conn.close()
    return {"ok": True}

@router.delete("/recetas/{receta_id}")
def eliminar_receta(receta_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE recetas SET activo=FALSE WHERE id=%s", (receta_id,))
    conn.commit(); cur.close(); conn.close()
    return {"ok": True}

@router.post("/recetas/{receta_id}/ingredientes")
def agregar_ingrediente(receta_id: int, data: IngredienteIn):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO receta_ingredientes (receta_id, mp_id, cantidad)
        VALUES (%s,%s,%s) RETURNING id
    """, (receta_id, data.mp_id, data.cantidad))
    new_id = cur.fetchone()[0]
    conn.commit(); cur.close(); conn.close()
    return {"id": new_id, "ok": True}

@router.delete("/receta-ingredientes/{ing_id}")
def eliminar_ingrediente(ing_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM receta_ingredientes WHERE id=%s", (ing_id,))
    conn.commit(); cur.close(); conn.close()
    return {"ok": True}

# ═══════════════════════════════════════════════════════════════
# PRODUCCIÓN  — lote generado automáticamente por el servidor
# ═══════════════════════════════════════════════════════════════

class ProduccionIn(BaseModel):
    fecha: date
    receta_id: int
    porciones_planeadas: float = 0
    porciones: float
    operario: Optional[str] = None
    notas: Optional[str] = None

@router.get("/produccion")
def listar_produccion(limit: int = 200):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT p.id, p.fecha, r.nombre AS receta, r.categoria,
               p.porciones_planeadas, p.porciones, p.lote, p.operario, p.notas,
               COALESCE(costo.val,0) * p.porciones AS costo_total
        FROM produccion p
        LEFT JOIN recetas r ON r.id=p.receta_id
        LEFT JOIN LATERAL (
            SELECT SUM(ri.cantidad * COALESCE(ult.precio_unit,0)) / NULLIF(r.porciones,0) AS val
            FROM receta_ingredientes ri
            LEFT JOIN LATERAL (
                SELECT precio_unit FROM compras_mp
                WHERE mp_id=ri.mp_id ORDER BY fecha DESC, id DESC LIMIT 1
            ) ult ON TRUE
            WHERE ri.receta_id=p.receta_id
        ) costo ON TRUE
        ORDER BY p.fecha DESC, p.id DESC LIMIT %s
    """, (limit,))
    rows = cur.fetchall()
    cur.close(); conn.close()
    return [{"id": r[0], "fecha": str(r[1]), "receta": r[2], "categoria": r[3],
             "porciones_planeadas": float(r[4]) if r[4] else 0,
             "porciones": float(r[5]), "lote": r[6], "operario": r[7],
             "notas": r[8], "costo_total": float(r[9]) if r[9] else 0} for r in rows]

@router.post("/produccion")
def registrar_produccion(data: ProduccionIn):
    conn = get_conn()
    cur = conn.cursor()
    # Insertar con lote temporal único
    temp_lote = f"TEMP-{uuid.uuid4()}"
    cur.execute("""
        INSERT INTO produccion (fecha, receta_id, porciones_planeadas, porciones, lote, operario, notas)
        VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING id
    """, (data.fecha, data.receta_id, data.porciones_planeadas, data.porciones,
          temp_lote, data.operario, data.notas))
    new_id = cur.fetchone()[0]
    # Generar lote definitivo con el ID real
    lote = _gen_lote_produccion(new_id)
    cur.execute("UPDATE produccion SET lote=%s WHERE id=%s", (lote, new_id))
    conn.commit(); cur.close(); conn.close()
    return {"id": new_id, "lote": lote, "ok": True}

@router.delete("/produccion/{prod_id}")
def eliminar_produccion(prod_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM produccion WHERE id=%s", (prod_id,))
    conn.commit(); cur.close(); conn.close()
    return {"ok": True}

# ═══════════════════════════════════════════════════════════════
# GASTOS
# ═══════════════════════════════════════════════════════════════

class GastoIn(BaseModel):
    fecha: date
    tipo: str = "caja_menor"
    categoria: str = "General"
    descripcion: str
    monto: float
    responsable: Optional[str] = None
    comprobante: Optional[str] = None
    notas: Optional[str] = None

@router.get("/gastos")
def listar_gastos(tipo: Optional[str] = None, limit: int = 200):
    conn = get_conn()
    cur = conn.cursor()
    if tipo:
        cur.execute("""
            SELECT id, fecha, tipo, categoria, descripcion, monto, responsable, comprobante, notas
            FROM gastos WHERE tipo=%s ORDER BY fecha DESC, id DESC LIMIT %s
        """, (tipo, limit))
    else:
        cur.execute("""
            SELECT id, fecha, tipo, categoria, descripcion, monto, responsable, comprobante, notas
            FROM gastos ORDER BY fecha DESC, id DESC LIMIT %s
        """, (limit,))
    rows = cur.fetchall()
    cur.close(); conn.close()
    return [{"id": r[0], "fecha": str(r[1]), "tipo": r[2], "categoria": r[3],
             "descripcion": r[4], "monto": float(r[5]), "responsable": r[6],
             "comprobante": r[7], "notas": r[8]} for r in rows]

@router.post("/gastos")
def registrar_gasto(data: GastoIn):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO gastos (fecha, tipo, categoria, descripcion, monto, responsable, comprobante, notas)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id
    """, (data.fecha, data.tipo, data.categoria, data.descripcion,
          data.monto, data.responsable, data.comprobante, data.notas))
    new_id = cur.fetchone()[0]
    conn.commit(); cur.close(); conn.close()
    return {"id": new_id, "ok": True}

@router.delete("/gastos/{gasto_id}")
def eliminar_gasto(gasto_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM gastos WHERE id=%s", (gasto_id,))
    conn.commit(); cur.close(); conn.close()
    return {"ok": True}

# ═══════════════════════════════════════════════════════════════
# PROCEDIMIENTOS
# ═══════════════════════════════════════════════════════════════

class ProcedimientoIn(BaseModel):
    nombre: str
    categoria: str = "General"
    descripcion: Optional[str] = None
    pasos: Optional[str] = None
    responsable: Optional[str] = None
    frecuencia: str = "Cada vez"

@router.get("/procedimientos")
def listar_procedimientos():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, nombre, categoria, descripcion, pasos, responsable, frecuencia, activo
        FROM procedimientos WHERE activo=TRUE ORDER BY categoria, nombre
    """)
    rows = cur.fetchall()
    cur.close(); conn.close()
    return [{"id": r[0], "nombre": r[1], "categoria": r[2], "descripcion": r[3],
             "pasos": r[4], "responsable": r[5], "frecuencia": r[6], "activo": r[7]} for r in rows]

@router.post("/procedimientos")
def crear_procedimiento(data: ProcedimientoIn):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO procedimientos (nombre, categoria, descripcion, pasos, responsable, frecuencia)
        VALUES (%s,%s,%s,%s,%s,%s) RETURNING id
    """, (data.nombre, data.categoria, data.descripcion,
          data.pasos, data.responsable, data.frecuencia))
    new_id = cur.fetchone()[0]
    conn.commit(); cur.close(); conn.close()
    return {"id": new_id, "ok": True}

@router.put("/procedimientos/{proc_id}")
def editar_procedimiento(proc_id: int, data: ProcedimientoIn):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE procedimientos SET nombre=%s, categoria=%s, descripcion=%s,
               pasos=%s, responsable=%s, frecuencia=%s WHERE id=%s
    """, (data.nombre, data.categoria, data.descripcion,
          data.pasos, data.responsable, data.frecuencia, proc_id))
    conn.commit(); cur.close(); conn.close()
    return {"ok": True}

@router.delete("/procedimientos/{proc_id}")
def eliminar_procedimiento(proc_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE procedimientos SET activo=FALSE WHERE id=%s", (proc_id,))
    conn.commit(); cur.close(); conn.close()
    return {"ok": True}

# ═══════════════════════════════════════════════════════════════
# REGISTROS SANITARIOS
# ═══════════════════════════════════════════════════════════════

class RegistroSanitarioIn(BaseModel):
    fecha: date
    tipo: str = "Control temperatura"
    descripcion: str
    resultado: str = "Aprobado"
    operario: Optional[str] = None
    observaciones: Optional[str] = None
    proxima_revision: Optional[date] = None

@router.get("/registros-sanitarios")
def listar_registros(limit: int = 200):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, fecha, tipo, descripcion, resultado, operario, observaciones, proxima_revision
        FROM registros_sanitarios ORDER BY fecha DESC, id DESC LIMIT %s
    """, (limit,))
    rows = cur.fetchall()
    cur.close(); conn.close()
    return [{"id": r[0], "fecha": str(r[1]), "tipo": r[2], "descripcion": r[3],
             "resultado": r[4], "operario": r[5], "observaciones": r[6],
             "proxima_revision": str(r[7]) if r[7] else None} for r in rows]

@router.post("/registros-sanitarios")
def crear_registro(data: RegistroSanitarioIn):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO registros_sanitarios
            (fecha, tipo, descripcion, resultado, operario, observaciones, proxima_revision)
        VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING id
    """, (data.fecha, data.tipo, data.descripcion, data.resultado,
          data.operario, data.observaciones, data.proxima_revision))
    new_id = cur.fetchone()[0]
    conn.commit(); cur.close(); conn.close()
    return {"id": new_id, "ok": True}

@router.delete("/registros-sanitarios/{reg_id}")
def eliminar_registro(reg_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM registros_sanitarios WHERE id=%s", (reg_id,))
    conn.commit(); cur.close(); conn.close()
    return {"ok": True}

# ═══════════════════════════════════════════════════════════════
# REMISIONES — flujo de aprobación
# ═══════════════════════════════════════════════════════════════

class RemisionItemIn(BaseModel):
    mp_id: int
    mp_nombre: str
    cantidad: float
    precio_unit: float

class RemisionIn(BaseModel):
    fecha: date
    proveedor: str
    operario: str
    notas: Optional[str] = None
    foto: str          # base64 obligatorio
    items: List[RemisionItemIn]

class AprobarIn(BaseModel):
    aprobado_por: str

class RechazarIn(BaseModel):
    rechazado_por: str
    motivo: str

@router.get("/remisiones")
def listar_remisiones(estado: Optional[str] = None, limit: int = 200):
    conn = get_conn()
    cur = conn.cursor()
    # Detect available columns
    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='remisiones'")
    rem_cols = {r[0] for r in cur.fetchall()}
    has_proveedor = 'proveedor' in rem_cols
    has_operario = 'operario' in rem_cols

    prov_col = "proveedor" if has_proveedor else "NULL"
    oper_col = "operario" if has_operario else "NULL"
    base_q = f"""
        SELECT id, numero,
               COALESCE(fecha, creado_en::date) as fecha,
               {prov_col} as proveedor, {oper_col} as operario,
               notas, estado, aprobado_por, rechazo_motivo, creado_en
        FROM remisiones
    """
    if estado:
        cur.execute(base_q + " WHERE estado=%s ORDER BY creado_en DESC LIMIT %s", (estado, limit))
    else:
        cur.execute(base_q + " ORDER BY creado_en DESC LIMIT %s", (limit,))
    rows = cur.fetchall()

    # Detect remision_items columns
    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='remision_items'")
    ri_cols = {r[0] for r in cur.fetchall()}
    has_lote = 'lote' in ri_cols
    has_mp_nombre = 'mp_nombre' in ri_cols
    has_precio = 'precio_unit' in ri_cols

    result = []
    for r in rows:
        lote_col = "lote" if has_lote else "NULL"
        nombre_col = "mp_nombre" if has_mp_nombre else "NULL"
        precio_col = "precio_unit" if has_precio else "0"
        cur.execute(f"""
            SELECT {nombre_col}, cantidad, {precio_col}, {lote_col}
            FROM remision_items WHERE remision_id=%s ORDER BY id
        """, (r[0],))
        items = [{"mp_nombre": i[0] or '', "cantidad": float(i[1]),
                  "precio_unit": float(i[2]) if i[2] else 0,
                  "lote": i[3]} for i in cur.fetchall()]
        result.append({
            "id": r[0], "numero": r[1], "fecha": str(r[2]) if r[2] else '',
            "proveedor": r[3] or '', "operario": r[4] or '', "notas": r[5],
            "estado": r[6], "aprobado_por": r[7], "rechazo_motivo": r[8],
            "creado_en": str(r[9]), "items": items
        })
    cur.close(); conn.close()
    return result

@router.get("/remisiones/{remision_id}/estado")
def estado_remision(remision_id: int):
    """Endpoint de polling para el operario."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT estado, aprobado_por, rechazo_motivo
        FROM remisiones WHERE id=%s
    """, (remision_id,))
    r = cur.fetchone()
    cur.close(); conn.close()
    if not r:
        raise HTTPException(404, "Remisión no encontrada")
    return {"estado": r[0], "aprobado_por": r[1], "rechazo_motivo": r[2]}

@router.get("/remisiones/{remision_id}/foto")
def foto_remision(remision_id: int):
    """Devuelve la foto de una remisión."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT foto FROM remisiones WHERE id=%s", (remision_id,))
    r = cur.fetchone()
    cur.close(); conn.close()
    if not r:
        raise HTTPException(404, "Remisión no encontrada")
    return {"foto": r[0]}

@router.post("/remisiones")
def crear_remision(data: RemisionIn):
    if not data.items:
        raise HTTPException(400, "Debe agregar al menos un ítem")
    if not data.foto:
        raise HTTPException(400, "La foto de la remisión es obligatoria")

    conn = get_conn()
    cur = conn.cursor()

    # 1. Insertar remisión con número temporal único
    temp_numero = f"TEMP-{uuid.uuid4()}"
    cur.execute("""
        INSERT INTO remisiones (numero, fecha, proveedor, operario, notas, foto, estado)
        VALUES (%s, %s, %s, %s, %s, %s, 'pendiente') RETURNING id
    """, (temp_numero, data.fecha, data.proveedor, data.operario,
          data.notas, data.foto))
    remision_id = cur.fetchone()[0]

    # 2. Generar número definitivo con el ID real
    numero = _gen_numero_remision(remision_id)
    cur.execute("UPDATE remisiones SET numero=%s WHERE id=%s", (numero, remision_id))

    # 3. Insertar ítems con lote generado automáticamente
    for item in data.items:
        temp_lote = f"TEMP-{uuid.uuid4()}"
        cur.execute("""
            INSERT INTO remision_items (remision_id, mp_id, mp_nombre, cantidad, precio_unit, lote)
            VALUES (%s, %s, %s, %s, %s, %s) RETURNING id
        """, (remision_id, item.mp_id, item.mp_nombre,
              item.cantidad, item.precio_unit, temp_lote))
        item_id = cur.fetchone()[0]
        lote_item = _gen_lote_item(item_id)
        cur.execute("UPDATE remision_items SET lote=%s WHERE id=%s", (lote_item, item_id))

    conn.commit(); cur.close(); conn.close()
    return {"id": remision_id, "numero": numero, "estado": "pendiente", "ok": True}

@router.post("/remisiones/{remision_id}/aprobar")
def aprobar_remision(remision_id: int, data: AprobarIn):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE remisiones
        SET estado='aprobada', aprobado_por=%s, actualizado_en=NOW()
        WHERE id=%s AND estado='pendiente'
        RETURNING id
    """, (data.aprobado_por, remision_id))
    if not cur.fetchone():
        conn.rollback(); cur.close(); conn.close()
        raise HTTPException(400, "La remisión no existe o ya fue procesada")
    conn.commit(); cur.close(); conn.close()
    return {"ok": True, "estado": "aprobada"}

@router.post("/remisiones/{remision_id}/rechazar")
def rechazar_remision(remision_id: int, data: RechazarIn):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE remisiones
        SET estado='rechazada', aprobado_por=%s, rechazo_motivo=%s, actualizado_en=NOW()
        WHERE id=%s AND estado='pendiente'
        RETURNING id
    """, (data.rechazado_por, data.motivo, remision_id))
    if not cur.fetchone():
        conn.rollback(); cur.close(); conn.close()
        raise HTTPException(400, "La remisión no existe o ya fue procesada")
    conn.commit(); cur.close(); conn.close()
    return {"ok": True, "estado": "rechazada"}

@router.delete("/remisiones/{remision_id}")
def eliminar_remision(remision_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM remisiones WHERE id=%s", (remision_id,))
    conn.commit(); cur.close(); conn.close()
    return {"ok": True}

# ═══════════════════════════════════════════════════════════════
# DASHBOARD
# ═══════════════════════════════════════════════════════════════

@router.get("/dashboard")
def dashboard():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM materias_primas WHERE activo=TRUE")
    total_mp = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM recetas WHERE activo=TRUE")
    total_recetas = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*), COALESCE(SUM(porciones),0) FROM produccion WHERE fecha=CURRENT_DATE")
    r = cur.fetchone()
    prod_hoy_reg, prod_hoy_porciones = r[0], float(r[1])

    cur.execute("SELECT COALESCE(SUM(monto),0) FROM gastos WHERE DATE_TRUNC('month',fecha)=DATE_TRUNC('month',CURRENT_DATE)")
    gastos_mes = float(cur.fetchone()[0])

    cur.execute("SELECT COALESCE(SUM(monto),0) FROM gastos WHERE tipo='caja_menor' AND fecha=CURRENT_DATE")
    caja_hoy = float(cur.fetchone()[0])

    cur.execute("""
        SELECT COUNT(*) FROM registros_sanitarios
        WHERE proxima_revision <= CURRENT_DATE + INTERVAL '7 days'
          AND proxima_revision >= CURRENT_DATE
    """)
    alertas_sanitarias = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM remisiones WHERE estado='pendiente'")
    remisiones_pendientes = cur.fetchone()[0]

    # Stock bajo / critico / agotado count
    cur.execute("""
        SELECT COUNT(*) FROM inventario inv
        JOIN materias_primas mp ON mp.id = inv.mp_id AND mp.activo = TRUE
        WHERE inv.stock_minimo > 0
          AND inv.cantidad_actual <= inv.stock_minimo
    """)
    stock_bajo = cur.fetchone()[0]

    # Items con stock bajo (para mini-tabla dashboard)
    cur.execute("""
        SELECT mp.nombre, mp.unidad, inv.cantidad_actual, inv.stock_minimo,
               lc.proveedor AS ultimo_proveedor
        FROM inventario inv
        JOIN materias_primas mp ON mp.id = inv.mp_id AND mp.activo = TRUE
        LEFT JOIN LATERAL (
            SELECT proveedor FROM compras_mp WHERE mp_id = mp.id ORDER BY fecha DESC, id DESC LIMIT 1
        ) lc ON TRUE
        WHERE inv.stock_minimo > 0 AND inv.cantidad_actual <= inv.stock_minimo
        ORDER BY (inv.cantidad_actual / NULLIF(inv.stock_minimo, 0)) ASC
        LIMIT 8
    """)
    items_stock_bajo = [{"nombre": r[0], "unidad": r[1],
                         "cantidad_actual": float(r[2]), "stock_minimo": float(r[3]),
                         "proveedor": r[4]} for r in cur.fetchall()]

    cur.execute("""
        SELECT p.fecha, r.nombre, p.porciones_planeadas, p.porciones, p.operario
        FROM produccion p LEFT JOIN recetas r ON r.id=p.receta_id
        ORDER BY p.fecha DESC, p.id DESC LIMIT 5
    """)
    ultima_produccion = [{"fecha": str(r[0]), "receta": r[1],
                          "porciones_planeadas": float(r[2]) if r[2] else 0,
                          "porciones": float(r[3]), "operario": r[4]}
                         for r in cur.fetchall()]

    cur.close(); conn.close()
    return {
        "total_mp": total_mp,
        "total_recetas": total_recetas,
        "produccion_hoy": {"registros": prod_hoy_reg, "porciones": prod_hoy_porciones},
        "gastos_mes": gastos_mes,
        "caja_hoy": caja_hoy,
        "alertas_sanitarias": alertas_sanitarias,
        "remisiones_pendientes": remisiones_pendientes,
        "stock_bajo": stock_bajo,
        "items_stock_bajo": items_stock_bajo,
        "ultima_produccion": ultima_produccion
    }

@router.get("/dashboard/charts")
def dashboard_charts():
    """Data for dashboard charts: production trend, expense breakdown, monthly costs, recipe margins."""
    conn = get_conn()
    cur = conn.cursor()

    # Production daily last 30 days
    cur.execute("""
        SELECT fecha, COALESCE(SUM(porciones),0)
        FROM produccion
        WHERE fecha >= CURRENT_DATE - INTERVAL '30 days'
        GROUP BY fecha ORDER BY fecha
    """)
    produccion_30d = [{"fecha": str(r[0]), "porciones": float(r[1])} for r in cur.fetchall()]

    # Expenses by category current month
    cur.execute("""
        SELECT categoria, COALESCE(SUM(monto),0) as total
        FROM gastos
        WHERE DATE_TRUNC('month',fecha)=DATE_TRUNC('month',CURRENT_DATE)
        GROUP BY categoria ORDER BY total DESC
    """)
    gastos_categorias = [{"categoria": r[0], "total": float(r[1])} for r in cur.fetchall()]

    # Monthly expenses last 6 months
    cur.execute("""
        SELECT DATE_TRUNC('month', fecha) as mes, COALESCE(SUM(monto),0) as total
        FROM gastos
        WHERE fecha >= CURRENT_DATE - INTERVAL '6 months'
        GROUP BY mes ORDER BY mes
    """)
    gastos_mensuales = [{"mes": r[0].strftime('%Y-%m'), "total": float(r[1])} for r in cur.fetchall()]

    # Top 5 recipes by margin
    cur.execute("""
        SELECT r.nombre, r.precio_venta,
               COALESCE(SUM(ri.cantidad * COALESCE(lc.precio_unit, 0)), 0) / NULLIF(r.porciones, 0) as costo_porcion
        FROM recetas r
        LEFT JOIN receta_ingredientes ri ON ri.receta_id = r.id
        LEFT JOIN LATERAL (
            SELECT precio_unit FROM compras_mp WHERE mp_id = ri.mp_id ORDER BY fecha DESC, id DESC LIMIT 1
        ) lc ON TRUE
        WHERE r.activo = TRUE AND r.precio_venta > 0
        GROUP BY r.id, r.nombre, r.precio_venta, r.porciones
        ORDER BY (r.precio_venta - COALESCE(SUM(ri.cantidad * COALESCE(lc.precio_unit, 0)), 0) / NULLIF(r.porciones, 0)) DESC
        LIMIT 5
    """)
    top_recetas = []
    for r in cur.fetchall():
        costo = float(r[2]) if r[2] else 0
        venta = float(r[1])
        margen_pct = ((venta - costo) / venta * 100) if venta > 0 else 0
        top_recetas.append({"nombre": r[0], "precio_venta": venta, "costo_porcion": costo, "margen_pct": round(margen_pct, 1)})

    # KPI comparisons (current month vs previous month)
    cur.execute("""
        SELECT
            COALESCE(SUM(CASE WHEN DATE_TRUNC('month',fecha)=DATE_TRUNC('month',CURRENT_DATE) THEN porciones END),0) as prod_mes,
            COALESCE(SUM(CASE WHEN DATE_TRUNC('month',fecha)=DATE_TRUNC('month',CURRENT_DATE - INTERVAL '1 month') THEN porciones END),0) as prod_mes_ant
        FROM produccion
        WHERE fecha >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')
    """)
    r = cur.fetchone()
    prod_mes = float(r[0]); prod_mes_ant = float(r[1])

    cur.execute("""
        SELECT
            COALESCE(SUM(CASE WHEN DATE_TRUNC('month',fecha)=DATE_TRUNC('month',CURRENT_DATE) THEN monto END),0) as gastos_mes,
            COALESCE(SUM(CASE WHEN DATE_TRUNC('month',fecha)=DATE_TRUNC('month',CURRENT_DATE - INTERVAL '1 month') THEN monto END),0) as gastos_mes_ant
        FROM gastos
        WHERE fecha >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')
    """)
    r = cur.fetchone()
    gastos_mes = float(r[0]); gastos_mes_ant = float(r[1])

    # Production last 7 days for sparkline
    cur.execute("""
        SELECT d::date, COALESCE(SUM(p.porciones),0)
        FROM generate_series(CURRENT_DATE - INTERVAL '6 days', CURRENT_DATE, '1 day') d
        LEFT JOIN produccion p ON p.fecha = d::date
        GROUP BY d::date ORDER BY d::date
    """)
    sparkline_prod = [float(r[1]) for r in cur.fetchall()]

    # Expenses last 7 days for sparkline
    cur.execute("""
        SELECT d::date, COALESCE(SUM(g.monto),0)
        FROM generate_series(CURRENT_DATE - INTERVAL '6 days', CURRENT_DATE, '1 day') d
        LEFT JOIN gastos g ON g.fecha = d::date
        GROUP BY d::date ORDER BY d::date
    """)
    sparkline_gastos = [float(r[1]) for r in cur.fetchall()]

    cur.close(); conn.close()
    return {
        "produccion_30d": produccion_30d,
        "gastos_categorias": gastos_categorias,
        "gastos_mensuales": gastos_mensuales,
        "top_recetas": top_recetas,
        "comparaciones": {
            "prod_mes": prod_mes, "prod_mes_ant": prod_mes_ant,
            "gastos_mes": gastos_mes, "gastos_mes_ant": gastos_mes_ant
        },
        "sparklines": {
            "produccion": sparkline_prod,
            "gastos": sparkline_gastos
        }
    }

# ═══════════════════════════════════════════════════════════════
# INVENTARIO
# ═══════════════════════════════════════════════════════════════

class InventarioAjusteIn(BaseModel):
    mp_id: int
    stock_minimo: float = 0

@router.get("/inventario")
def listar_inventario():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT mp.id, mp.codigo, mp.nombre, mp.unidad, mp.categoria,
               COALESCE(inv.cantidad_actual, 0) as cantidad_actual,
               COALESCE(inv.stock_minimo, 0) as stock_minimo,
               inv.updated_at
        FROM materias_primas mp
        LEFT JOIN inventario inv ON inv.mp_id = mp.id
        WHERE mp.activo = TRUE
        ORDER BY mp.categoria, mp.nombre
    """)
    rows = cur.fetchall()
    cur.close(); conn.close()
    result = []
    for r in rows:
        cant = float(r[5])
        minimo = float(r[6])
        if minimo > 0:
            status = 'ok' if cant > minimo * 1.5 else ('bajo' if cant > minimo else ('critico' if cant > 0 else 'agotado'))
        else:
            status = 'ok' if cant > 0 else 'sin_stock'
        result.append({
            "id": r[0], "codigo": r[1], "nombre": r[2], "unidad": r[3], "categoria": r[4],
            "cantidad_actual": cant, "stock_minimo": minimo,
            "updated_at": str(r[7]) if r[7] else None,
            "status": status
        })
    return result

@router.post("/inventario/ajuste")
def ajustar_inventario(data: InventarioAjusteIn):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO inventario (mp_id, cantidad_actual, stock_minimo, updated_at)
        VALUES (%s, 0, %s, NOW())
        ON CONFLICT (mp_id) DO UPDATE SET stock_minimo=%s, updated_at=NOW()
    """, (data.mp_id, data.stock_minimo, data.stock_minimo))
    conn.commit(); cur.close(); conn.close()
    return {"ok": True}

@router.post("/inventario/entrada")
def inventario_entrada(mp_id: int, cantidad: float):
    """Adds stock (called after purchase registration)"""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO inventario (mp_id, cantidad_actual, updated_at)
        VALUES (%s, %s, NOW())
        ON CONFLICT (mp_id) DO UPDATE SET cantidad_actual = inventario.cantidad_actual + %s, updated_at = NOW()
    """, (mp_id, cantidad, cantidad))
    conn.commit(); cur.close(); conn.close()
    return {"ok": True}

@router.put("/inventario/{mp_id}/stock-minimo")
def update_stock_minimo(mp_id: int, data: InventarioAjusteIn):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO inventario (mp_id, cantidad_actual, stock_minimo, updated_at)
        VALUES (%s, 0, %s, NOW())
        ON CONFLICT (mp_id) DO UPDATE SET stock_minimo=%s, updated_at=NOW()
    """, (mp_id, data.stock_minimo, data.stock_minimo))
    conn.commit(); cur.close(); conn.close()
    return {"ok": True}

# ═══════════════════════════════════════════════════════════════
# RECETA — Escalar (calculadora de ingredientes)
# ═══════════════════════════════════════════════════════════════

@router.get("/recetas/{receta_id}/escalar")
def escalar_receta(receta_id: int, porciones: float = 1):
    """Calcula ingredientes necesarios para N porciones."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT porciones FROM recetas WHERE id=%s", (receta_id,))
    r = cur.fetchone()
    if not r:
        raise HTTPException(404, "Receta no encontrada")
    porciones_base = float(r[0]) if r[0] else 1
    factor = porciones / porciones_base if porciones_base > 0 else 1

    cur.execute("""
        SELECT mp.nombre, mp.unidad, ri.cantidad,
               COALESCE(ult.precio_unit, 0) AS precio_unit,
               COALESCE(inv.cantidad_actual, 0) AS stock
        FROM receta_ingredientes ri
        JOIN materias_primas mp ON mp.id = ri.mp_id
        LEFT JOIN LATERAL (
            SELECT precio_unit FROM compras_mp WHERE mp_id = ri.mp_id ORDER BY fecha DESC, id DESC LIMIT 1
        ) ult ON TRUE
        LEFT JOIN inventario inv ON inv.mp_id = ri.mp_id
        WHERE ri.receta_id = %s
    """, (receta_id,))
    ingredientes = []
    for i in cur.fetchall():
        cant_necesaria = float(i[2]) * factor
        precio = float(i[3])
        stock = float(i[4])
        ingredientes.append({
            "nombre": i[0], "unidad": i[1],
            "cantidad_base": float(i[2]),
            "cantidad_necesaria": round(cant_necesaria, 4),
            "costo": round(cant_necesaria * precio, 2),
            "stock_disponible": stock,
            "alcanza": stock >= cant_necesaria
        })
    cur.close(); conn.close()
    costo_total = sum(i["costo"] for i in ingredientes)
    return {
        "porciones_solicitadas": porciones,
        "factor": round(factor, 4),
        "ingredientes": ingredientes,
        "costo_total": costo_total,
        "costo_porcion": round(costo_total / porciones, 2) if porciones > 0 else 0
    }

# ═══════════════════════════════════════════════════════════════
# TRAZABILIDAD — Rastreo de lotes
# ═══════════════════════════════════════════════════════════════

@router.get("/trazabilidad/produccion/{lote}")
def trazar_produccion(lote: str):
    """Traza un lote de producción hasta sus materias primas y proveedores."""
    conn = get_conn()
    cur = conn.cursor()
    # Buscar producción por lote
    cur.execute("""
        SELECT p.id, p.fecha, p.porciones, p.operario, p.notas,
               r.nombre AS receta, r.id AS receta_id
        FROM produccion p
        LEFT JOIN recetas r ON r.id = p.receta_id
        WHERE p.lote = %s
    """, (lote,))
    prod = cur.fetchone()
    if not prod:
        raise HTTPException(404, "Lote de producción no encontrado")

    # Ingredientes de la receta con sus últimas compras
    cur.execute("""
        SELECT mp.id, mp.nombre, mp.unidad, ri.cantidad,
               c.fecha AS compra_fecha, c.proveedor, c.precio_unit
        FROM receta_ingredientes ri
        JOIN materias_primas mp ON mp.id = ri.mp_id
        LEFT JOIN LATERAL (
            SELECT fecha, proveedor, precio_unit FROM compras_mp
            WHERE mp_id = ri.mp_id AND fecha <= %s
            ORDER BY fecha DESC, id DESC LIMIT 1
        ) c ON TRUE
        WHERE ri.receta_id = %s
    """, (prod[1], prod[6]))
    ingredientes_base = cur.fetchall()

    # Try to get lote info from remision_items (column may not exist in older DBs)
    lote_info = {}
    try:
        mp_ids = [i[0] for i in ingredientes_base]
        if mp_ids:
            cur.execute("""
                SELECT ri2.mp_id, ri2.lote, rem.numero
                FROM remision_items ri2
                LEFT JOIN remisiones rem ON rem.id = ri2.remision_id
                WHERE ri2.mp_id = ANY(%s)
                ORDER BY ri2.id DESC
            """, (mp_ids,))
            for row in cur.fetchall():
                if row[0] not in lote_info:
                    lote_info[row[0]] = {"lote_mp": row[1], "remision": row[2]}
    except Exception:
        conn.rollback()

    ingredientes = []
    for i in ingredientes_base:
        li = lote_info.get(i[0], {})
        ingredientes.append({
            "mp_nombre": i[1], "unidad": i[2], "cantidad": float(i[3]),
            "compra_fecha": str(i[4]) if i[4] else None,
            "proveedor": i[5], "precio_unit": float(i[6]) if i[6] else 0,
            "lote_mp": li.get("lote_mp"), "remision": li.get("remision"),
            "rem_proveedor": None
        })
    cur.close(); conn.close()
    return {
        "lote": lote,
        "fecha": str(prod[1]),
        "receta": prod[5],
        "porciones": float(prod[2]),
        "operario": prod[3],
        "notas": prod[4],
        "ingredientes": ingredientes
    }

@router.get("/trazabilidad/buscar")
def buscar_lotes(q: str = ""):
    """Busca lotes de producción por texto."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT p.lote, p.fecha, r.nombre, p.porciones, p.operario
        FROM produccion p LEFT JOIN recetas r ON r.id = p.receta_id
        WHERE p.lote ILIKE %s OR r.nombre ILIKE %s
        ORDER BY p.fecha DESC LIMIT 20
    """, (f"%{q}%", f"%{q}%"))
    rows = cur.fetchall()
    cur.close(); conn.close()
    return [{"lote": r[0], "fecha": str(r[1]), "receta": r[2],
             "porciones": float(r[3]), "operario": r[4]} for r in rows]

# ═══════════════════════════════════════════════════════════════
# ÓRDENES DE COMPRA
# ═══════════════════════════════════════════════════════════════

class OrdenItemIn(BaseModel):
    mp_id: int
    mp_nombre: str
    cantidad: float
    precio_est: float = 0
    notas: Optional[str] = None

class OrdenCompraIn(BaseModel):
    fecha: date
    proveedor: str
    notas: Optional[str] = None
    creado_por: Optional[str] = None
    items: List[OrdenItemIn]

def _gen_numero_orden(new_id: int) -> str:
    return f"OC-{datetime.now().strftime('%Y%m%d')}-{new_id:06d}"

@router.get("/ordenes-compra")
def listar_ordenes(estado: Optional[str] = None, limit: int = 100):
    conn = get_conn()
    cur = conn.cursor()
    if estado:
        cur.execute("""
            SELECT id, numero, fecha, proveedor, estado, notas, creado_por, creado_en
            FROM ordenes_compra WHERE estado=%s ORDER BY creado_en DESC LIMIT %s
        """, (estado, limit))
    else:
        cur.execute("""
            SELECT id, numero, fecha, proveedor, estado, notas, creado_por, creado_en
            FROM ordenes_compra ORDER BY creado_en DESC LIMIT %s
        """, (limit,))
    rows = cur.fetchall()
    result = []
    for r in rows:
        cur.execute("SELECT mp_nombre, cantidad, precio_est FROM orden_items WHERE orden_id=%s ORDER BY id", (r[0],))
        items = [{"mp_nombre": i[0], "cantidad": float(i[1]), "precio_est": float(i[2]) if i[2] else 0} for i in cur.fetchall()]
        total = sum(i["cantidad"] * i["precio_est"] for i in items)
        result.append({"id": r[0], "numero": r[1], "fecha": str(r[2]), "proveedor": r[3],
                       "estado": r[4], "notas": r[5], "creado_por": r[6],
                       "creado_en": str(r[7]), "items": items, "total": total})
    cur.close(); conn.close()
    return result

@router.post("/ordenes-compra")
def crear_orden(data: OrdenCompraIn):
    if not data.items:
        raise HTTPException(400, "Debe agregar al menos un ítem")
    conn = get_conn()
    cur = conn.cursor()
    temp = f"TEMP-{uuid.uuid4()}"
    cur.execute("""
        INSERT INTO ordenes_compra (numero, fecha, proveedor, notas, creado_por)
        VALUES (%s,%s,%s,%s,%s) RETURNING id
    """, (temp, data.fecha, data.proveedor, data.notas, data.creado_por))
    orden_id = cur.fetchone()[0]
    numero = _gen_numero_orden(orden_id)
    cur.execute("UPDATE ordenes_compra SET numero=%s WHERE id=%s", (numero, orden_id))
    for item in data.items:
        cur.execute("""
            INSERT INTO orden_items (orden_id, mp_id, mp_nombre, cantidad, precio_est, notas)
            VALUES (%s,%s,%s,%s,%s,%s)
        """, (orden_id, item.mp_id, item.mp_nombre, item.cantidad, item.precio_est, item.notas))
    conn.commit(); cur.close(); conn.close()
    return {"id": orden_id, "numero": numero, "ok": True}

@router.put("/ordenes-compra/{orden_id}/estado")
def cambiar_estado_orden(orden_id: int, estado: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE ordenes_compra SET estado=%s, actualizado_en=NOW() WHERE id=%s", (estado, orden_id))
    conn.commit(); cur.close(); conn.close()
    return {"ok": True}

@router.delete("/ordenes-compra/{orden_id}")
def eliminar_orden(orden_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM ordenes_compra WHERE id=%s", (orden_id,))
    conn.commit(); cur.close(); conn.close()
    return {"ok": True}

@router.get("/ordenes-compra/generar-desde-inventario")
def generar_orden_desde_inventario():
    """Sugiere items para orden de compra basado en stock bajo."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT mp.id, mp.nombre, mp.unidad, inv.cantidad_actual, inv.stock_minimo,
               COALESCE(lc.precio_unit, 0) AS precio_est,
               COALESCE(lc.proveedor, '') AS proveedor
        FROM inventario inv
        JOIN materias_primas mp ON mp.id = inv.mp_id AND mp.activo = TRUE
        LEFT JOIN LATERAL (
            SELECT precio_unit, proveedor FROM compras_mp WHERE mp_id = mp.id ORDER BY fecha DESC, id DESC LIMIT 1
        ) lc ON TRUE
        WHERE inv.stock_minimo > 0 AND inv.cantidad_actual <= inv.stock_minimo
        ORDER BY mp.nombre
    """)
    rows = cur.fetchall()
    cur.close(); conn.close()
    # Agrupar por proveedor
    por_proveedor = {}
    for r in rows:
        prov = r[6] or 'Sin proveedor'
        faltante = float(r[4]) * 2 - float(r[3])  # Pedir hasta 2x el mínimo
        if faltante < 0: faltante = float(r[4])
        item = {"mp_id": r[0], "mp_nombre": r[1], "unidad": r[2],
                "stock_actual": float(r[3]), "stock_minimo": float(r[4]),
                "cantidad_sugerida": round(faltante, 2),
                "precio_est": float(r[5])}
        if prov not in por_proveedor:
            por_proveedor[prov] = []
        por_proveedor[prov].append(item)
    return por_proveedor

# ═══════════════════════════════════════════════════════════════
# INVIMA — Programas Sanitarios
# ═══════════════════════════════════════════════════════════════

class InvimaProgramaIn(BaseModel):
    nombre: str
    codigo: Optional[str] = None
    descripcion: Optional[str] = None
    responsable: Optional[str] = None
    frecuencia: str = "Mensual"

class InvimaRegistroIn(BaseModel):
    programa_id: int
    fecha: date
    descripcion: str
    resultado: str = "Conforme"
    responsable: Optional[str] = None
    observaciones: Optional[str] = None
    proxima_revision: Optional[date] = None

@router.get("/invima/programas")
def listar_invima_programas():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT p.id, p.codigo, p.nombre, p.descripcion, p.responsable, p.frecuencia, p.activo,
               (SELECT COUNT(*) FROM invima_registros WHERE programa_id=p.id) AS total_registros,
               (SELECT MAX(fecha) FROM invima_registros WHERE programa_id=p.id) AS ultimo_registro,
               (SELECT MIN(proxima_revision) FROM invima_registros
                WHERE programa_id=p.id AND proxima_revision >= CURRENT_DATE) AS prox_revision
        FROM invima_programas p WHERE p.activo=TRUE
        ORDER BY p.codigo, p.nombre
    """)
    rows = cur.fetchall()
    cur.close(); conn.close()
    return [{"id": r[0], "codigo": r[1], "nombre": r[2], "descripcion": r[3],
             "responsable": r[4], "frecuencia": r[5], "activo": r[6],
             "total_registros": r[7],
             "ultimo_registro": str(r[8]) if r[8] else None,
             "prox_revision": str(r[9]) if r[9] else None} for r in rows]

@router.post("/invima/programas")
def crear_invima_programa(data: InvimaProgramaIn):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO invima_programas (codigo, nombre, descripcion, responsable, frecuencia)
        VALUES (%s,%s,%s,%s,%s) RETURNING id
    """, (data.codigo, data.nombre, data.descripcion, data.responsable, data.frecuencia))
    new_id = cur.fetchone()[0]
    conn.commit(); cur.close(); conn.close()
    return {"id": new_id, "ok": True}

@router.put("/invima/programas/{prog_id}")
def editar_invima_programa(prog_id: int, data: InvimaProgramaIn):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE invima_programas SET codigo=%s, nombre=%s, descripcion=%s,
               responsable=%s, frecuencia=%s WHERE id=%s
    """, (data.codigo, data.nombre, data.descripcion, data.responsable, data.frecuencia, prog_id))
    conn.commit(); cur.close(); conn.close()
    return {"ok": True}

@router.delete("/invima/programas/{prog_id}")
def eliminar_invima_programa(prog_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE invima_programas SET activo=FALSE WHERE id=%s", (prog_id,))
    conn.commit(); cur.close(); conn.close()
    return {"ok": True}

@router.get("/invima/registros")
def listar_invima_registros(programa_id: Optional[int] = None, limit: int = 100):
    conn = get_conn()
    cur = conn.cursor()
    if programa_id:
        cur.execute("""
            SELECT r.id, r.fecha, p.nombre AS programa, p.codigo, r.descripcion,
                   r.resultado, r.responsable, r.observaciones, r.proxima_revision
            FROM invima_registros r JOIN invima_programas p ON p.id=r.programa_id
            WHERE r.programa_id=%s ORDER BY r.fecha DESC LIMIT %s
        """, (programa_id, limit))
    else:
        cur.execute("""
            SELECT r.id, r.fecha, p.nombre AS programa, p.codigo, r.descripcion,
                   r.resultado, r.responsable, r.observaciones, r.proxima_revision
            FROM invima_registros r JOIN invima_programas p ON p.id=r.programa_id
            ORDER BY r.fecha DESC LIMIT %s
        """, (limit,))
    rows = cur.fetchall()
    cur.close(); conn.close()
    return [{"id": r[0], "fecha": str(r[1]), "programa": r[2], "codigo": r[3],
             "descripcion": r[4], "resultado": r[5], "responsable": r[6],
             "observaciones": r[7],
             "proxima_revision": str(r[8]) if r[8] else None} for r in rows]

@router.post("/invima/registros")
def crear_invima_registro(data: InvimaRegistroIn):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO invima_registros (programa_id, fecha, descripcion, resultado, responsable, observaciones, proxima_revision)
        VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING id
    """, (data.programa_id, data.fecha, data.descripcion, data.resultado,
          data.responsable, data.observaciones, data.proxima_revision))
    new_id = cur.fetchone()[0]
    conn.commit(); cur.close(); conn.close()
    return {"id": new_id, "ok": True}

@router.delete("/invima/registros/{reg_id}")
def eliminar_invima_registro(reg_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM invima_registros WHERE id=%s", (reg_id,))
    conn.commit(); cur.close(); conn.close()
    return {"ok": True}

# ═══════════════════════════════════════════════════════════════
# PROYECCIONES DE DEMANDA
# ═══════════════════════════════════════════════════════════════

@router.get("/proyecciones")
def proyecciones(dias: int = 30):
    """Forecast simple basado en producción histórica por receta."""
    conn = get_conn()
    cur = conn.cursor()
    # Producción por receta últimos 90 días (para calcular promedio)
    cur.execute("""
        SELECT r.id, r.nombre, r.categoria,
               COUNT(p.id) AS veces_producido,
               COALESCE(SUM(p.porciones), 0) AS total_porciones,
               COALESCE(AVG(p.porciones), 0) AS promedio_porciones,
               MAX(p.fecha) AS ultima_produccion,
               COALESCE(SUM(ri.cantidad * COALESCE(lc.precio_unit, 0)), 0) / NULLIF(r.porciones, 0) AS costo_porcion
        FROM recetas r
        LEFT JOIN produccion p ON p.receta_id = r.id AND p.fecha >= CURRENT_DATE - INTERVAL '90 days'
        LEFT JOIN receta_ingredientes ri ON ri.receta_id = r.id
        LEFT JOIN LATERAL (
            SELECT precio_unit FROM compras_mp WHERE mp_id = ri.mp_id ORDER BY fecha DESC, id DESC LIMIT 1
        ) lc ON TRUE
        WHERE r.activo = TRUE
        GROUP BY r.id, r.nombre, r.categoria, r.porciones
        HAVING COUNT(p.id) > 0
        ORDER BY total_porciones DESC
    """)
    rows = cur.fetchall()

    # Producción por semana últimas 12 semanas para tendencia
    cur.execute("""
        SELECT DATE_TRUNC('week', fecha) AS semana, COALESCE(SUM(porciones), 0)
        FROM produccion
        WHERE fecha >= CURRENT_DATE - INTERVAL '12 weeks'
        GROUP BY semana ORDER BY semana
    """)
    tendencia_semanal = [{"semana": r[0].strftime('%Y-%m-%d'), "porciones": float(r[1])} for r in cur.fetchall()]

    cur.close(); conn.close()

    recetas_forecast = []
    for r in rows:
        veces = r[3]
        total = float(r[4])
        promedio = float(r[5])
        # Proyección simple: (promedio diario) * días
        promedio_diario = total / 90.0
        proyeccion = round(promedio_diario * dias, 1)
        recetas_forecast.append({
            "id": r[0], "nombre": r[1], "categoria": r[2],
            "veces_90d": veces, "total_90d": total,
            "promedio_produccion": round(promedio, 1),
            "ultima_produccion": str(r[6]) if r[6] else None,
            "costo_porcion": round(float(r[7]), 2) if r[7] else 0,
            "proyeccion_porciones": proyeccion,
            "proyeccion_costo": round(proyeccion * (float(r[7]) if r[7] else 0), 2)
        })

    return {
        "dias_proyeccion": dias,
        "recetas": recetas_forecast,
        "tendencia_semanal": tendencia_semanal
    }

# ═══════════════════════════════════════════════════════════════
# SIIGO — Productos y Ventas
# ═══════════════════════════════════════════════════════════════

@router.get("/siigo/productos")
def siigo_productos(tipo: str = "terminado"):
    """tipo: terminado | mp | todos"""
    try:
        from siigo import fetch_products
        return fetch_products(tipo=tipo)
    except Exception as e:
        raise HTTPException(500, f"Error Siigo: {str(e)}")

@router.get("/siigo/ventas-semana")
def siigo_ventas_semana(semanas: int = 8):
    try:
        from siigo import sales_by_product_weekly
        return sales_by_product_weekly(semanas)
    except Exception as e:
        raise HTTPException(500, f"Error Siigo: {str(e)}")

# ═══════════════════════════════════════════════════════════════
# PRODUCTO ↔ RECETA (ligado Siigo → Daily)
# ═══════════════════════════════════════════════════════════════

class ProductoRecetaIn(BaseModel):
    siigo_code: str
    siigo_name: str
    siigo_group: Optional[str] = None
    receta_id: Optional[int] = None

@router.get("/producto-receta")
def listar_producto_receta():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT pr.id, pr.siigo_code, pr.siigo_name, pr.siigo_group, pr.receta_id,
               r.nombre AS receta_nombre, pr.activo
        FROM producto_receta pr
        LEFT JOIN recetas r ON r.id = pr.receta_id
        WHERE pr.activo = TRUE
        ORDER BY pr.siigo_group, pr.siigo_name
    """)
    rows = cur.fetchall()
    cur.close(); conn.close()
    return [{"id": r[0], "siigo_code": r[1], "siigo_name": r[2], "siigo_group": r[3],
             "receta_id": r[4], "receta_nombre": r[5], "activo": r[6]} for r in rows]

@router.post("/producto-receta")
def crear_producto_receta(data: ProductoRecetaIn):
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO producto_receta (siigo_code, siigo_name, siigo_group, receta_id)
            VALUES (%s,%s,%s,%s)
            ON CONFLICT (siigo_code) WHERE siigo_code IS NOT NULL
            DO UPDATE SET siigo_name=%s, siigo_group=%s, receta_id=%s
            RETURNING id
        """, (data.siigo_code, data.siigo_name, data.siigo_group, data.receta_id,
              data.siigo_name, data.siigo_group, data.receta_id))
        new_id = cur.fetchone()[0]
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise HTTPException(500, str(e))
    finally:
        cur.close(); conn.close()
    return {"id": new_id, "ok": True}

@router.put("/producto-receta/{pr_id}/receta")
def ligar_receta(pr_id: int, receta_id: Optional[int] = None):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE producto_receta SET receta_id=%s WHERE id=%s", (receta_id, pr_id))
    conn.commit(); cur.close(); conn.close()
    return {"ok": True}

@router.post("/producto-receta/sync")
def sync_productos_siigo():
    """Sincroniza productos terminados de Siigo con la tabla producto_receta."""
    try:
        from siigo import fetch_products
        productos = fetch_products(tipo="terminado")
    except Exception as e:
        raise HTTPException(500, f"Error Siigo: {str(e)}")
    conn = get_conn()
    cur = conn.cursor()
    synced = 0
    for p in productos:
        cur.execute("""
            INSERT INTO producto_receta (siigo_code, siigo_name, siigo_group)
            VALUES (%s, %s, %s)
            ON CONFLICT (siigo_code) WHERE siigo_code IS NOT NULL
            DO UPDATE SET siigo_name=%s, siigo_group=%s
        """, (p["code"], p["name"], p["group"], p["name"], p["group"]))
        synced += 1
    conn.commit(); cur.close(); conn.close()
    return {"synced": synced, "ok": True}

# ═══════════════════════════════════════════════════════════════
# REGLAS DE PRODUCCIÓN (condicionales)
# ═══════════════════════════════════════════════════════════════

class ReglaIn(BaseModel):
    tipo: str          # dias_recepcion, vida_util, no_fin_semana, lead_time, stock_seguridad, produccion_dia, capacidad_max
    entidad: Optional[str] = None   # nombre de MP, proveedor, receta, o 'planta'
    entidad_id: Optional[int] = None
    parametro: str
    valor: str
    descripcion: Optional[str] = None

@router.get("/reglas")
def listar_reglas():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, tipo, entidad, entidad_id, parametro, valor, descripcion, activo
        FROM reglas_produccion WHERE activo=TRUE ORDER BY tipo, entidad
    """)
    rows = cur.fetchall()
    cur.close(); conn.close()
    return [{"id": r[0], "tipo": r[1], "entidad": r[2], "entidad_id": r[3],
             "parametro": r[4], "valor": r[5], "descripcion": r[6], "activo": r[7]} for r in rows]

@router.post("/reglas")
def crear_regla(data: ReglaIn):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO reglas_produccion (tipo, entidad, entidad_id, parametro, valor, descripcion)
        VALUES (%s,%s,%s,%s,%s,%s) RETURNING id
    """, (data.tipo, data.entidad, data.entidad_id, data.parametro, data.valor, data.descripcion))
    new_id = cur.fetchone()[0]
    conn.commit(); cur.close(); conn.close()
    return {"id": new_id, "ok": True}

@router.put("/reglas/{regla_id}")
def editar_regla(regla_id: int, data: ReglaIn):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE reglas_produccion SET tipo=%s, entidad=%s, entidad_id=%s,
               parametro=%s, valor=%s, descripcion=%s WHERE id=%s
    """, (data.tipo, data.entidad, data.entidad_id, data.parametro, data.valor, data.descripcion, regla_id))
    conn.commit(); cur.close(); conn.close()
    return {"ok": True}

@router.delete("/reglas/{regla_id}")
def eliminar_regla(regla_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE reglas_produccion SET activo=FALSE WHERE id=%s", (regla_id,))
    conn.commit(); cur.close(); conn.close()
    return {"ok": True}

# ═══════════════════════════════════════════════════════════════
# PLAN DE PRODUCCIÓN SEMANAL (motor de planificación)
# ═══════════════════════════════════════════════════════════════

@router.get("/plan-produccion")
def plan_produccion(semanas_historico: int = 4):
    """
    Genera plan de producción semanal basado en:
    1. Ventas reales de Siigo (últimas N semanas) → demanda promedio
    2. Producto ↔ Receta → ingredientes necesarios
    3. Reglas/condicionales → restricciones de compra y producción
    """
    # 1. Obtener ventas por producto
    try:
        from siigo import sales_by_product_weekly
        ventas = sales_by_product_weekly(semanas_historico)
    except Exception as e:
        raise HTTPException(500, f"Error Siigo: {str(e)}")

    conn = get_conn()
    cur = conn.cursor()

    # 2. Obtener ligados producto ↔ receta
    cur.execute("""
        SELECT pr.siigo_code, pr.siigo_name, pr.receta_id, r.nombre, r.porciones
        FROM producto_receta pr
        LEFT JOIN recetas r ON r.id = pr.receta_id
        WHERE pr.activo = TRUE AND pr.receta_id IS NOT NULL
    """)
    ligados = {r[0]: {"siigo_name": r[1], "receta_id": r[2], "receta_nombre": r[3],
                       "porciones_base": float(r[4]) if r[4] else 1} for r in cur.fetchall()}

    # 3. Obtener reglas
    cur.execute("SELECT tipo, entidad, parametro, valor FROM reglas_produccion WHERE activo=TRUE")
    reglas_raw = cur.fetchall()
    reglas = {}
    for tipo, entidad, param, valor in reglas_raw:
        key = f"{tipo}:{entidad or 'global'}"
        reglas[key] = {"tipo": tipo, "entidad": entidad, "parametro": param, "valor": valor}

    # Capacidad máxima diaria
    cap_max = float(reglas.get("capacidad_max:planta", {}).get("valor", "9999"))

    # 4. Calcular demanda promedio semanal por producto
    num_weeks = len(ventas.get("weeks", [])) or 1
    plan_productos = []
    total_mp_necesaria = {}  # mp_id -> {nombre, unidad, cantidad_total, proveedores, reglas}

    for prod_venta in ventas.get("products", []):
        code = prod_venta["code"]
        if code not in ligados:
            continue
        lig = ligados[code]
        avg_qty_semana = prod_venta["total_qty"] / num_weeks
        avg_revenue = prod_venta["total_revenue"] / num_weeks

        # Factor: porciones que rinde la receta vs unidades vendidas
        # Si la receta rinde 10 porciones y se venden 30/semana → 3 batches
        porciones_base = lig["porciones_base"]
        batches_semana = avg_qty_semana / porciones_base if porciones_base > 0 else avg_qty_semana

        # Obtener ingredientes de la receta
        cur.execute("""
            SELECT ri.mp_id, mp.nombre, mp.unidad, ri.cantidad,
                   COALESCE(lc.precio_unit, 0) AS precio,
                   COALESCE(lc.proveedor, '') AS proveedor
            FROM receta_ingredientes ri
            JOIN materias_primas mp ON mp.id = ri.mp_id
            LEFT JOIN LATERAL (
                SELECT precio_unit, proveedor FROM compras_mp WHERE mp_id=ri.mp_id ORDER BY fecha DESC, id DESC LIMIT 1
            ) lc ON TRUE
            WHERE ri.receta_id = %s
        """, (lig["receta_id"],))
        ingredientes = []
        for ing in cur.fetchall():
            mp_id, mp_nombre, mp_unidad, cant_base, precio, proveedor = ing
            cant_necesaria = float(cant_base) * batches_semana

            # Aplicar reglas de esta MP
            mp_reglas = []
            dias_recepcion = reglas.get(f"dias_recepcion:{mp_nombre}", {}).get("valor", "")
            vida_util = reglas.get(f"vida_util:{mp_nombre}", {}).get("valor", "")
            no_finde = reglas.get(f"no_fin_semana:{mp_nombre}", {}).get("valor", "")
            lead_time = reglas.get(f"dias_entrega:{proveedor}", {}).get("valor", "") if proveedor else ""

            if dias_recepcion:
                mp_reglas.append(f"Solo llega: {dias_recepcion}")
            if vida_util:
                mp_reglas.append(f"Vida util: {vida_util} dias")
            if no_finde:
                mp_reglas.append("No pedir para fin de semana")
            if lead_time:
                mp_reglas.append(f"Lead time: {lead_time} dias ({proveedor})")

            ingredientes.append({
                "mp_id": mp_id, "mp_nombre": mp_nombre, "unidad": mp_unidad,
                "cantidad_semanal": round(cant_necesaria, 2),
                "costo_semanal": round(cant_necesaria * float(precio), 2),
                "proveedor": proveedor,
                "reglas": mp_reglas
            })

            # Acumular MP total
            if mp_id not in total_mp_necesaria:
                total_mp_necesaria[mp_id] = {"nombre": mp_nombre, "unidad": mp_unidad,
                                              "cantidad": 0, "costo": 0, "proveedor": proveedor,
                                              "reglas": mp_reglas}
            total_mp_necesaria[mp_id]["cantidad"] += cant_necesaria
            total_mp_necesaria[mp_id]["costo"] += cant_necesaria * float(precio)

        plan_productos.append({
            "siigo_code": code,
            "producto": prod_venta["name"],
            "receta": lig["receta_nombre"],
            "venta_semanal_promedio": round(avg_qty_semana, 1),
            "revenue_semanal": round(avg_revenue, 2),
            "batches_semana": round(batches_semana, 2),
            "porciones_producir": round(avg_qty_semana, 0),
            "ingredientes": ingredientes
        })

    # 5. Plan de compras: agrupar MP necesaria con reglas
    cur.execute("""
        SELECT mp.id, COALESCE(inv.cantidad_actual, 0)
        FROM materias_primas mp LEFT JOIN inventario inv ON inv.mp_id = mp.id
        WHERE mp.activo = TRUE
    """)
    stock_actual = {r[0]: float(r[1]) for r in cur.fetchall()}

    plan_compras = []
    for mp_id, mp_data in sorted(total_mp_necesaria.items(), key=lambda x: -x[1]["costo"]):
        stock = stock_actual.get(mp_id, 0)
        necesita = round(mp_data["cantidad"], 2)
        a_pedir = max(0, round(necesita - stock, 2))
        plan_compras.append({
            "mp_id": mp_id,
            "nombre": mp_data["nombre"],
            "unidad": mp_data["unidad"],
            "necesario_semanal": necesita,
            "stock_actual": stock,
            "a_pedir": a_pedir,
            "costo_estimado": round(mp_data["costo"], 2),
            "proveedor": mp_data["proveedor"],
            "reglas": mp_data["reglas"]
        })

    cur.close(); conn.close()
    return {
        "semanas_analizadas": num_weeks,
        "plan_produccion": plan_productos,
        "plan_compras": plan_compras,
        "capacidad_diaria": cap_max,
        "total_costo_semanal": round(sum(m["costo_estimado"] for m in plan_compras), 2)
    }
