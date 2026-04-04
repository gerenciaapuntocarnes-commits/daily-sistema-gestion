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
            codigo    TEXT,
            nombre    TEXT NOT NULL,
            unidad    TEXT NOT NULL DEFAULT 'kg',
            categoria TEXT NOT NULL DEFAULT 'General',
            activo    BOOLEAN NOT NULL DEFAULT TRUE,
            creado_en TIMESTAMP DEFAULT NOW()
        )
    """)
    # Agregar columna codigo si la tabla ya existía sin ella
    cur.execute("""
        ALTER TABLE materias_primas
        ADD COLUMN IF NOT EXISTS codigo TEXT
    """)
    # Índice único en codigo (ignora NULLs automáticamente en PostgreSQL)
    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_mp_codigo
        ON materias_primas(codigo) WHERE codigo IS NOT NULL
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

    # Campos de configuración de producción en recetas
    for col, defn in [
        ('batch_maximo',       'NUMERIC(10,2) DEFAULT 0'),
        ('tiempo_batch_min',   'INTEGER DEFAULT 0'),
        ('vida_util_dias',     'INTEGER DEFAULT 30'),
        ('costo_mano_obra',    'NUMERIC(12,2) DEFAULT 0'),
        ('costo_servicios',    'NUMERIC(12,2) DEFAULT 0'),
    ]:
        cur.execute(f"ALTER TABLE recetas ADD COLUMN IF NOT EXISTS {col} {defn}")

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
    # Índice único en lote de producción (ignora NULLs)
    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_prod_lote
        ON produccion(lote) WHERE lote IS NOT NULL
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

    # ── Remisiones ───────────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS remisiones (
            id             SERIAL PRIMARY KEY,
            numero         TEXT UNIQUE NOT NULL,
            fecha          DATE NOT NULL DEFAULT CURRENT_DATE,
            proveedor      TEXT NOT NULL,
            operario       TEXT NOT NULL,
            notas          TEXT,
            foto           TEXT NOT NULL,
            estado         TEXT NOT NULL DEFAULT 'pendiente',
            aprobado_por   TEXT,
            rechazo_motivo TEXT,
            creado_en      TIMESTAMP DEFAULT NOW(),
            actualizado_en TIMESTAMP DEFAULT NOW()
        )
    """)

    # Migraciones — columnas que pueden faltar en tablas existentes
    migrations = [
        ('fecha', 'remisiones', "DATE NOT NULL DEFAULT CURRENT_DATE"),
        ('actualizado_en', 'remisiones', "TIMESTAMP DEFAULT NOW()"),
        ('proveedor', 'remisiones', "TEXT NOT NULL DEFAULT ''"),
        ('operario', 'remisiones', "TEXT NOT NULL DEFAULT ''"),
        ('notas', 'remisiones', "TEXT"),
        ('foto', 'remisiones', "TEXT NOT NULL DEFAULT ''"),
        ('estado', 'remisiones', "TEXT NOT NULL DEFAULT 'pendiente'"),
        ('aprobado_por', 'remisiones', "TEXT"),
        ('rechazo_motivo', 'remisiones', "TEXT"),
        ('lote', 'remision_items', "TEXT"),
        ('mp_nombre', 'remision_items', "TEXT"),
        ('precio_unit', 'remision_items', "NUMERIC(12,2)"),
    ]
    for col, tbl, default in migrations:
        try:
            cur.execute(f"ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS {col} {default}")
        except Exception:
            conn.rollback()

    # ── Ítems de Remisión ────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS remision_items (
            id          SERIAL PRIMARY KEY,
            remision_id INTEGER REFERENCES remisiones(id) ON DELETE CASCADE,
            mp_id       INTEGER REFERENCES materias_primas(id),
            mp_nombre   TEXT,
            cantidad    NUMERIC(12,4) NOT NULL,
            precio_unit NUMERIC(12,2),
            lote        TEXT UNIQUE NOT NULL,
            creado_en   TIMESTAMP DEFAULT NOW()
        )
    """)

    # ── Inventario ──────────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS inventario (
            id             SERIAL PRIMARY KEY,
            mp_id          INTEGER REFERENCES materias_primas(id) ON DELETE CASCADE UNIQUE,
            cantidad_actual NUMERIC(12,4) NOT NULL DEFAULT 0,
            stock_minimo   NUMERIC(12,4) NOT NULL DEFAULT 0,
            updated_at     TIMESTAMP DEFAULT NOW()
        )
    """)

    # ── Órdenes de Compra ────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ordenes_compra (
            id             SERIAL PRIMARY KEY,
            numero         TEXT UNIQUE NOT NULL,
            fecha          DATE NOT NULL DEFAULT CURRENT_DATE,
            proveedor      TEXT NOT NULL,
            estado         TEXT NOT NULL DEFAULT 'borrador',
            notas          TEXT,
            creado_por     TEXT,
            creado_en      TIMESTAMP DEFAULT NOW(),
            actualizado_en TIMESTAMP DEFAULT NOW()
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS orden_items (
            id        SERIAL PRIMARY KEY,
            orden_id  INTEGER REFERENCES ordenes_compra(id) ON DELETE CASCADE,
            mp_id     INTEGER REFERENCES materias_primas(id),
            mp_nombre TEXT,
            cantidad  NUMERIC(12,4) NOT NULL,
            precio_est NUMERIC(12,2) DEFAULT 0,
            notas     TEXT
        )
    """)

    # ── Configuración de Planta ────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS config_planta (
            id            SERIAL PRIMARY KEY,
            parametro     TEXT UNIQUE NOT NULL,
            valor         TEXT NOT NULL,
            descripcion   TEXT
        )
    """)
    # Seed defaults if empty
    cur.execute("SELECT COUNT(*) FROM config_planta")
    if cur.fetchone()[0] == 0:
        for param, val, desc in [
            ('horas_productivas_dia', '8', 'Horas de produccion disponibles por dia'),
            ('dias_produccion', 'lunes,martes,miercoles,jueves,viernes', 'Dias de la semana en que se produce'),
            ('factor_stock_minimo', '2', 'Multiplicador de venta semanal para stock minimo (2 = 2 semanas)'),
        ]:
            cur.execute("INSERT INTO config_planta (parametro, valor, descripcion) VALUES (%s,%s,%s)", (param, val, desc))

    # ── Producto Siigo ↔ Receta (ligado) ───────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS producto_receta (
            id             SERIAL PRIMARY KEY,
            siigo_code     TEXT NOT NULL,
            siigo_name     TEXT NOT NULL,
            siigo_group    TEXT,
            precio_venta   NUMERIC(12,2) DEFAULT 0,
            receta_id      INTEGER REFERENCES recetas(id) ON DELETE SET NULL,
            activo         BOOLEAN DEFAULT TRUE,
            creado_en      TIMESTAMP DEFAULT NOW()
        )
    """)
    # Migration for existing table
    cur.execute("ALTER TABLE producto_receta ADD COLUMN IF NOT EXISTS precio_venta NUMERIC(12,2) DEFAULT 0")
    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_prod_receta_siigo
        ON producto_receta(siigo_code) WHERE siigo_code IS NOT NULL
    """)

    # ── Reglas de Producción (condicionales) ─────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS reglas_produccion (
            id          SERIAL PRIMARY KEY,
            tipo        TEXT NOT NULL,
            entidad     TEXT,
            entidad_id  INTEGER,
            parametro   TEXT NOT NULL,
            valor       TEXT NOT NULL,
            descripcion TEXT,
            activo      BOOLEAN DEFAULT TRUE,
            creado_en   TIMESTAMP DEFAULT NOW()
        )
    """)
    # Tipos de regla:
    # 'dias_entrega'    entidad='proveedor'  parametro='lead_time_dias'   valor='2'
    # 'dias_recepcion'  entidad='mp'         parametro='dias_disponibles' valor='martes,jueves'
    # 'vida_util'       entidad='mp'         parametro='dias_vida_util'   valor='3'
    # 'no_fin_semana'   entidad='mp'         parametro='no_pedir_finde'   valor='true'
    # 'stock_seguridad' entidad='mp'         parametro='stock_seguridad'  valor='5'
    # 'produccion_dia'  entidad='receta'     parametro='solo_dias'        valor='lunes,miercoles'
    # 'capacidad_max'   entidad='planta'     parametro='porciones_dia'    valor='200'

    # ── INVIMA Programas Sanitarios ──────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS invima_programas (
            id          SERIAL PRIMARY KEY,
            nombre      TEXT NOT NULL,
            codigo      TEXT,
            descripcion TEXT,
            responsable TEXT,
            frecuencia  TEXT DEFAULT 'Mensual',
            activo      BOOLEAN DEFAULT TRUE,
            creado_en   TIMESTAMP DEFAULT NOW()
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS invima_registros (
            id               SERIAL PRIMARY KEY,
            programa_id      INTEGER REFERENCES invima_programas(id) ON DELETE CASCADE,
            fecha            DATE NOT NULL DEFAULT CURRENT_DATE,
            descripcion      TEXT NOT NULL,
            resultado        TEXT DEFAULT 'Conforme',
            responsable      TEXT,
            observaciones    TEXT,
            proxima_revision DATE,
            creado_en        TIMESTAMP DEFAULT NOW()
        )
    """)

    # ── Seed INVIMA programs if empty ──────────────────────────
    cur.execute("SELECT COUNT(*) FROM invima_programas")
    if cur.fetchone()[0] == 0:
        programas_invima = [
            ('L&D', 'Limpieza y Desinfeccion', 'Protocolos de limpieza de superficies, equipos e instalaciones', 'Diario'),
            ('MIP', 'Manejo Integrado de Plagas', 'Control y prevencion de plagas en la planta', 'Mensual'),
            ('RSL', 'Manejo de Residuos Solidos y Liquidos', 'Disposicion adecuada de residuos generados', 'Diario'),
            ('AGU', 'Abastecimiento y Calidad de Agua', 'Control de potabilidad y abastecimiento de agua', 'Semanal'),
            ('CAP', 'Capacitacion del Personal', 'Plan de formacion en BPM e higiene para todo el personal', 'Mensual'),
            ('TRZ', 'Plan de Trazabilidad', 'Rastreo de productos desde materia prima hasta producto final', 'Cada lote'),
            ('CAL', 'Calibracion de Equipos e Instrumentos', 'Verificacion y calibracion de balanzas, termometros, etc.', 'Mensual'),
            ('MNT', 'Mantenimiento de Instalaciones y Equipos', 'Plan preventivo y correctivo de mantenimiento', 'Mensual'),
            ('PRV', 'Control de Proveedores', 'Evaluacion y seguimiento de proveedores de MP', 'Trimestral'),
            ('ALM', 'Almacenamiento', 'Control de condiciones de almacenamiento (temp, humedad, PEPS)', 'Diario'),
            ('DST', 'Distribucion y Transporte', 'Control de cadena de frio y condiciones de transporte', 'Cada despacho'),
            ('RCL', 'Plan de Recall / Recuperacion de Producto', 'Procedimiento para retiro de producto del mercado', 'Anual'),
            ('HPE', 'Higiene y Proteccion del Personal', 'Control de indumentaria, lavado de manos, estado de salud', 'Diario'),
        ]
        for codigo, nombre, desc, freq in programas_invima:
            cur.execute(
                "INSERT INTO invima_programas (codigo, nombre, descripcion, frecuencia) VALUES (%s,%s,%s,%s)",
                (codigo, nombre, desc, freq)
            )

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Base de datos Daily Sistema Gestión inicializada")

def init_db():
    try:
        _create_tables()
    except Exception as e:
        print(f"⚠️  Sin base de datos: {e}. Corriendo en modo preview.")
