import os
import psycopg2

def get_conn():
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL no configurada")
    return psycopg2.connect(url)

def _create_tables():
    conn = get_conn()
    cur = conn.cursor()

    # ── Materias Primas ──────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS materias_primas (
            id        SERIAL PRIMARY KEY,
            nombre    TEXT NOT NULL,
            unidad    TEXT NOT NULL DEFAULT 'kg',
            categoria TEXT NOT NULL DEFAULT 'General',
            activo    BOOLEAN NOT NULL DEFAULT TRUE,
            creado_en TIMESTAMP DEFAULT NOW()
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS compras_mp (
            id           SERIAL PRIMARY KEY,
            mp_id        INTEGER REFERENCES materias_primas(id) ON DELETE CASCADE,
            fecha        DATE NOT NULL DEFAULT CURRENT_DATE,
            proveedor    TEXT,
            cantidad     NUMERIC(12,4) NOT NULL,
            precio_unit  NUMERIC(12,2) NOT NULL,
            factura      TEXT,
            notas        TEXT,
            creado_en    TIMESTAMP DEFAULT NOW()
        )
    """)

    # ── Recetas ──────────────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS recetas (
            id          SERIAL PRIMARY KEY,
            nombre      TEXT NOT NULL,
            categoria   TEXT NOT NULL DEFAULT 'General',
            descripcion TEXT,
            porciones   NUMERIC(8,2) NOT NULL DEFAULT 1,
            precio_venta NUMERIC(12,2) DEFAULT 0,
            activo      BOOLEAN NOT NULL DEFAULT TRUE,
            creado_en   TIMESTAMP DEFAULT NOW()
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS receta_ingredientes (
            id        SERIAL PRIMARY KEY,
            receta_id INTEGER REFERENCES recetas(id) ON DELETE CASCADE,
            mp_id     INTEGER REFERENCES materias_primas(id) ON DELETE CASCADE,
            cantidad  NUMERIC(12,4) NOT NULL
        )
    """)

    # ── Producción ───────────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS produccion (
            id                 SERIAL PRIMARY KEY,
            fecha              DATE NOT NULL DEFAULT CURRENT_DATE,
            receta_id          INTEGER REFERENCES recetas(id) ON DELETE SET NULL,
            porciones_planeadas NUMERIC(10,2) DEFAULT 0,
            porciones          NUMERIC(10,2) NOT NULL,
            lote               TEXT,
            operario           TEXT,
            notas              TEXT,
            creado_en          TIMESTAMP DEFAULT NOW()
        )
    """)

    # ── Gastos ───────────────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS gastos (
            id          SERIAL PRIMARY KEY,
            fecha       DATE NOT NULL DEFAULT CURRENT_DATE,
            tipo        TEXT NOT NULL DEFAULT 'caja_menor',
            categoria   TEXT NOT NULL DEFAULT 'General',
            descripcion TEXT NOT NULL,
            monto       NUMERIC(12,2) NOT NULL,
            responsable TEXT,
            comprobante TEXT,
            notas       TEXT,
            creado_en   TIMESTAMP DEFAULT NOW()
        )
    """)

    # ── Procedimientos (SOPs) ────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS procedimientos (
            id          SERIAL PRIMARY KEY,
            nombre      TEXT NOT NULL,
            categoria   TEXT NOT NULL DEFAULT 'General',
            descripcion TEXT,
            pasos       TEXT,
            responsable TEXT,
            frecuencia  TEXT DEFAULT 'Cada vez',
            activo      BOOLEAN NOT NULL DEFAULT TRUE,
            creado_en   TIMESTAMP DEFAULT NOW()
        )
    """)

    # ── Registros Sanitarios ─────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS registros_sanitarios (
            id               SERIAL PRIMARY KEY,
            fecha            DATE NOT NULL DEFAULT CURRENT_DATE,
            tipo             TEXT NOT NULL DEFAULT 'Control temperatura',
            descripcion      TEXT NOT NULL,
            resultado        TEXT NOT NULL DEFAULT 'Aprobado',
            operario         TEXT,
            observaciones    TEXT,
            proxima_revision DATE,
            creado_en        TIMESTAMP DEFAULT NOW()
        )
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Base de datos Daily Sistema Gestión inicializada")

def init_db():
    try:
        _create_tables()
    except Exception as e:
        print(f"⚠️  Sin base de datos: {e}. Corriendo en modo preview.")
