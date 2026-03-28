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
    conn.commit(); cur.close(); conn.close()
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
    if estado:
        cur.execute("""
            SELECT id, numero, fecha, proveedor, operario, notas, estado,
                   aprobado_por, rechazo_motivo, creado_en
            FROM remisiones WHERE estado=%s
            ORDER BY creado_en DESC LIMIT %s
        """, (estado, limit))
    else:
        cur.execute("""
            SELECT id, numero, fecha, proveedor, operario, notas, estado,
                   aprobado_por, rechazo_motivo, creado_en
            FROM remisiones ORDER BY creado_en DESC LIMIT %s
        """, (limit,))
    rows = cur.fetchall()

    result = []
    for r in rows:
        cur.execute("""
            SELECT mp_nombre, cantidad, precio_unit, lote
            FROM remision_items WHERE remision_id=%s ORDER BY id
        """, (r[0],))
        items = [{"mp_nombre": i[0], "cantidad": float(i[1]),
                  "precio_unit": float(i[2]) if i[2] else 0,
                  "lote": i[3]} for i in cur.fetchall()]
        result.append({
            "id": r[0], "numero": r[1], "fecha": str(r[2]),
            "proveedor": r[3], "operario": r[4], "notas": r[5],
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
        "ultima_produccion": ultima_produccion
    }
