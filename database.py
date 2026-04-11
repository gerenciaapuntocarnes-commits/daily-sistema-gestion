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

    # Commit todas las tablas creadas hasta aquí antes de las migraciones
    # (evita que un rollback en el loop deshaga los CREATE TABLE)
    conn.commit()

    # Migraciones — columnas que pueden faltar en tablas existentes
    # Usa SAVEPOINT para que un fallo no deshaga las otras migraciones
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
            cur.execute("SAVEPOINT sp_migration")
            cur.execute(f"ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS {col} {default}")
            cur.execute("RELEASE SAVEPOINT sp_migration")
        except Exception:
            cur.execute("ROLLBACK TO SAVEPOINT sp_migration")

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

    # ── Contabilidad (datos de Siigo) ──────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS siigo_journals (
            id             TEXT PRIMARY KEY,
            name           TEXT,
            fecha          DATE NOT NULL,
            items          JSONB NOT NULL DEFAULT '[]',
            synced_at      TIMESTAMP DEFAULT NOW()
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS siigo_cuentas (
            codigo         TEXT PRIMARY KEY,
            nombre         TEXT,
            clase          TEXT,
            grupo_puc      TEXT,
            naturaleza     TEXT DEFAULT 'debit'
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS saldos_mensuales (
            id             SERIAL PRIMARY KEY,
            cuenta         TEXT NOT NULL,
            anio           INTEGER NOT NULL,
            mes            INTEGER NOT NULL,
            debito         NUMERIC(16,2) DEFAULT 0,
            credito        NUMERIC(16,2) DEFAULT 0,
            saldo          NUMERIC(16,2) DEFAULT 0,
            UNIQUE(cuenta, anio, mes)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS presupuestos (
            id             SERIAL PRIMARY KEY,
            cuenta         TEXT NOT NULL,
            cuenta_nombre  TEXT,
            anio           INTEGER NOT NULL,
            mes            INTEGER NOT NULL,
            monto          NUMERIC(16,2) NOT NULL DEFAULT 0,
            notas          TEXT,
            UNIQUE(cuenta, anio, mes)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sync_log (
            id             SERIAL PRIMARY KEY,
            tipo           TEXT NOT NULL,
            fecha          TIMESTAMP DEFAULT NOW(),
            registros      INTEGER DEFAULT 0,
            detalle        TEXT
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

    # ── Clientes (CRM / Ventas) ────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS clientes (
            id                 SERIAL PRIMARY KEY,
            nombre             TEXT NOT NULL,
            direccion          TEXT,
            apto               TEXT,
            info_adicional     TEXT,
            zona               TEXT,
            telefono           TEXT,
            cedula             TEXT,
            email              TEXT,
            shopify_customer_id TEXT,
            origen             TEXT DEFAULT 'manual',
            fecha_registro     DATE DEFAULT CURRENT_DATE,
            activo             BOOLEAN DEFAULT TRUE,
            creado_en          TIMESTAMP DEFAULT NOW()
        )
    """)
    # Migraciones: agregar columnas si la tabla ya existía sin ellas
    for col, defn in [
        ("direccion",           "TEXT"),
        ("apto",                "TEXT"),
        ("info_adicional",      "TEXT"),
        ("zona",                "TEXT"),
        ("telefono",            "TEXT"),
        ("email",               "TEXT"),
        ("cedula",              "TEXT"),
        ("shopify_customer_id", "TEXT"),
        ("origen",              "TEXT DEFAULT 'manual'"),
        ("fecha_registro",      "DATE DEFAULT CURRENT_DATE"),
        ("activo",              "BOOLEAN DEFAULT TRUE"),
    ]:
        try:
            cur.execute("SAVEPOINT sp_cl")
            cur.execute(f"ALTER TABLE clientes ADD COLUMN IF NOT EXISTS {col} {defn}")
            cur.execute("RELEASE SAVEPOINT sp_cl")
        except Exception:
            cur.execute("ROLLBACK TO SAVEPOINT sp_cl")
    try:
        cur.execute("SAVEPOINT sp_idx_ced")
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_clientes_cedula ON clientes(cedula) WHERE cedula IS NOT NULL AND cedula != ''")
        cur.execute("RELEASE SAVEPOINT sp_idx_ced")
    except Exception:
        cur.execute("ROLLBACK TO SAVEPOINT sp_idx_ced")
    try:
        cur.execute("SAVEPOINT sp_idx_sh")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_clientes_shopify ON clientes(shopify_customer_id) WHERE shopify_customer_id IS NOT NULL")
        cur.execute("RELEASE SAVEPOINT sp_idx_sh")
    except Exception:
        cur.execute("ROLLBACK TO SAVEPOINT sp_idx_sh")

    # Commit explícito de todas las migraciones de clientes
    conn.commit()

    # ── Ventas ───────────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ventas (
            id                SERIAL PRIMARY KEY,
            fecha             DATE NOT NULL DEFAULT CURRENT_DATE,
            cliente_id        INTEGER REFERENCES clientes(id) ON DELETE SET NULL,
            cliente_nombre    TEXT NOT NULL,
            factura           TEXT,
            valor             NUMERIC(14,2) NOT NULL DEFAULT 0,
            estado            TEXT NOT NULL DEFAULT 'pendiente',
            medio_pago        TEXT,
            canal             TEXT,
            shopify_order_id  TEXT,
            shopify_order_name TEXT,
            notas             TEXT,
            creado_en         TIMESTAMP DEFAULT NOW()
        )
    """)
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_ventas_shopify ON ventas(shopify_order_id) WHERE shopify_order_id IS NOT NULL")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ventas_cliente ON ventas(cliente_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ventas_fecha ON ventas(fecha)")

    # ── Shopify Sync Log ─────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS shopify_sync_log (
            id                    SERIAL PRIMARY KEY,
            fecha                 TIMESTAMP DEFAULT NOW(),
            tipo                  TEXT,
            registros_nuevos      INTEGER DEFAULT 0,
            registros_actualizados INTEGER DEFAULT 0,
            errores               INTEGER DEFAULT 0,
            detalle               TEXT
        )
    """)

    # ── CRM: clientes unificados (Siigo + Excel) ─────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS crm_clientes (
            id                  SERIAL PRIMARY KEY,
            siigo_id            TEXT UNIQUE,
            cedula              TEXT,
            nombre              TEXT NOT NULL,
            telefono            TEXT,
            email               TEXT,
            direccion           TEXT,
            ciudad              TEXT,
            origen_canal        TEXT,          -- 'CLIENTE DAILY','PAGINA','INSTAGRAM','REFERIDO','SIN INFO'
            notas               TEXT,
            activo              BOOLEAN DEFAULT TRUE,
            creado_en           TIMESTAMP DEFAULT NOW(),
            actualizado_en      TIMESTAMP DEFAULT NOW()
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_crm_cedula  ON crm_clientes(cedula)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_crm_siigo   ON crm_clientes(siigo_id)")
    # Migración: tipo de documento para RC en Siigo
    cur.execute("ALTER TABLE crm_clientes ADD COLUMN IF NOT EXISTS id_type_code TEXT DEFAULT '13'")
    cur.execute("ALTER TABLE crm_clientes ADD COLUMN IF NOT EXISTS person_type  TEXT DEFAULT 'Person'")
    # Migraciones CRM extendido
    for col, defn in [
        ("segmento",          "TEXT DEFAULT 'Sin clasificar'"),
        ("canal_adquisicion", "TEXT"),
        ("estado_cliente",    "TEXT DEFAULT 'activo'"),
        ("ultima_compra",     "DATE"),
        ("total_compras",     "NUMERIC(16,2) DEFAULT 0"),
        ("num_facturas",      "INTEGER DEFAULT 0"),
        ("responsable",       "TEXT"),
        ("cupo_credito",      "NUMERIC(14,2) DEFAULT 0"),
        ("dias_credito",      "INTEGER DEFAULT 0"),
        ("notas_crm",         "TEXT"),
    ]:
        cur.execute(f"ALTER TABLE crm_clientes ADD COLUMN IF NOT EXISTS {col} {defn}")

    # ── CRM: facturas desde Siigo ────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS crm_facturas (
            id                  SERIAL PRIMARY KEY,
            siigo_invoice_id    TEXT UNIQUE NOT NULL,
            numero              INTEGER,
            prefix              TEXT,
            cliente_id          INTEGER REFERENCES crm_clientes(id) ON DELETE SET NULL,
            cliente_nombre      TEXT,
            cliente_cedula      TEXT,
            fecha               DATE NOT NULL,
            total               NUMERIC(14,2) NOT NULL DEFAULT 0,
            balance             NUMERIC(14,2) NOT NULL DEFAULT 0,
            estado_pago         TEXT DEFAULT 'pendiente',  -- 'pendiente','pagado'
            medio_pago          TEXT,                      -- 'Efectivo','Bancolombia','Banco de Bogota'
            cuenta_debito       TEXT,                      -- código cuenta contable para RC
            origen_canal        TEXT,                      -- canal de venta
            movimiento_id       INTEGER,                   -- FK a movimientos_bancarios cuando se concilia
            rc_siigo_id         TEXT,                      -- ID del RC creado en Siigo
            rc_numero           TEXT,                      -- número RC (RC-1-XXXX)
            rc_modo_prueba      BOOLEAN DEFAULT TRUE,      -- TRUE = simulado, FALSE = real en Siigo
            sync_at             TIMESTAMP DEFAULT NOW()
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_crm_fact_cliente ON crm_facturas(cliente_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_crm_fact_fecha   ON crm_facturas(fecha)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_crm_fact_estado  ON crm_facturas(estado_pago)")

    # ── CRM: movimientos bancarios (desde Google Sheets) ─────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS movimientos_bancarios (
            id              SERIAL PRIMARY KEY,
            banco           TEXT NOT NULL,      -- 'BDB','BANCOLOMBIA'
            fecha           DATE NOT NULL,
            descripcion     TEXT,
            valor           NUMERIC(14,2) NOT NULL,  -- positivo=crédito, negativo=débito
            estado          TEXT,               -- 'CONCILIADO' u otro valor del Sheet
            rc_sheet        TEXT,               -- número RC que pusieron en el Sheet
            cliente_sheet   TEXT,               -- nombre cliente que pusieron en el Sheet
            conciliado      BOOLEAN DEFAULT FALSE,
            factura_id      INTEGER REFERENCES crm_facturas(id) ON DELETE SET NULL,
            sheet_row       INTEGER,            -- fila en el Sheet para referencia
            sheet_tab       TEXT,               -- pestaña del Sheet
            sync_at         TIMESTAMP DEFAULT NOW()
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_mov_banco ON movimientos_bancarios(banco)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_mov_fecha ON movimientos_bancarios(fecha)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_mov_valor ON movimientos_bancarios(valor)")

    # ── CRM: log de sync ─────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS crm_sync_log (
            id          SERIAL PRIMARY KEY,
            tipo        TEXT,   -- 'siigo_clientes','siigo_facturas','bancos','excel'
            registros   INTEGER DEFAULT 0,
            nuevos      INTEGER DEFAULT 0,
            actualizados INTEGER DEFAULT 0,
            fecha       TIMESTAMP DEFAULT NOW(),
            detalle     TEXT
        )
    """)

    # ── CRM: seguimientos (log de contacto) ─────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS crm_seguimientos (
            id              SERIAL PRIMARY KEY,
            cliente_id      INTEGER REFERENCES crm_clientes(id) ON DELETE CASCADE,
            tipo            TEXT NOT NULL,
            descripcion     TEXT NOT NULL,
            resultado       TEXT,
            fecha           TIMESTAMP DEFAULT NOW(),
            responsable     TEXT,
            proxima_accion  TEXT,
            proxima_fecha   DATE
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_crm_seg_cliente ON crm_seguimientos(cliente_id)")

    # ── CRM: prospectos ───────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS crm_prospectos (
            id                    SERIAL PRIMARY KEY,
            nombre                TEXT NOT NULL,
            empresa               TEXT,
            telefono              TEXT,
            email                 TEXT,
            direccion             TEXT,
            ciudad                TEXT,
            segmento              TEXT,
            canal                 TEXT,
            estado                TEXT DEFAULT 'contactado',
            responsable           TEXT,
            notas                 TEXT,
            fecha_contacto        DATE DEFAULT CURRENT_DATE,
            fecha_seguimiento     DATE,
            valor_potencial       NUMERIC(14,2) DEFAULT 0,
            convertido_cliente_id INTEGER REFERENCES crm_clientes(id) ON DELETE SET NULL,
            creado_en             TIMESTAMP DEFAULT NOW(),
            actualizado_en        TIMESTAMP DEFAULT NOW()
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_crm_pros_estado ON crm_prospectos(estado)")

    # ── CRM: campanas ─────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS crm_campanas (
            id                   SERIAL PRIMARY KEY,
            nombre               TEXT NOT NULL,
            tipo                 TEXT NOT NULL,
            descripcion          TEXT,
            segmento_objetivo    TEXT,
            valor                NUMERIC(10,2),
            fecha_inicio         DATE,
            fecha_fin            DATE,
            activa               BOOLEAN DEFAULT TRUE,
            presupuesto          NUMERIC(14,2) DEFAULT 0,
            clientes_objetivo    INTEGER DEFAULT 0,
            clientes_respondieron INTEGER DEFAULT 0,
            creado_en            TIMESTAMP DEFAULT NOW()
        )
    """)

    # ── Ventas Daily ──────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ventas_daily (
            id              SERIAL PRIMARY KEY,
            fecha_despacho  DATE,
            fecha_pago      DATE,
            cliente         TEXT NOT NULL,
            numero_factura  TEXT,
            valor           NUMERIC(14,2),
            medio_pago      TEXT,
            canal           TEXT,
            conciliacion    TEXT,
            notas           TEXT,
            creado_en       TIMESTAMP DEFAULT NOW()
        )
    """)
    cur.execute("ALTER TABLE ventas_daily ADD COLUMN IF NOT EXISTS conciliacion TEXT")
    cur.execute("ALTER TABLE ventas_daily ADD COLUMN IF NOT EXISTS factura_id INTEGER REFERENCES crm_facturas(id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_vd_factura ON ventas_daily(factura_id)")
    cur.execute("ALTER TABLE crm_facturas ADD COLUMN IF NOT EXISTS tiene_nc BOOLEAN DEFAULT FALSE")

    conn.commit()

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

    # ════════════════════════════════════════════════════════════════════════
    # FASE 0 — Núcleo transaccional: lotes, kardex, producción, trazabilidad
    # Principios irrevocables:
    #   · mp_lote nace al guardar remision_item, estado = 'cuarentena'
    #   · inv_movimientos solo se escribe al LIBERAR un lote (nunca en cuarentena)
    #   · stock disponible = SUM(inv_movimientos) − SUM(inv_reservas activas)
    #   · inv_movimientos es INMUTABLE: solo INSERT, jamás UPDATE ni DELETE
    # ════════════════════════════════════════════════════════════════════════

    # ── FASE 0 · Grupo A: Tablas maestras (sin dependencias entre sí) ────────

    # A1. unidades_medida — maestro de unidades con factor de conversión a base
    cur.execute("""
        CREATE TABLE IF NOT EXISTS unidades_medida (
            id          SERIAL PRIMARY KEY,
            codigo      TEXT UNIQUE NOT NULL,
            nombre      TEXT NOT NULL,
            tipo        TEXT NOT NULL,
                -- 'masa' | 'volumen' | 'unidad_discreta'
            factor_base NUMERIC(18,8) NOT NULL DEFAULT 1,
                -- multiplicador para convertir 1 unidad de este tipo a la unidad base:
                --   masa base = kg   → kg:factor=1, g:factor=0.001, lb:factor=0.453592
                --   volumen base = lt → lt:factor=1, ml:factor=0.001
                --   discreta base = und → und:factor=1
            activo      BOOLEAN NOT NULL DEFAULT TRUE,
            CONSTRAINT chk_um_tipo
                CHECK (tipo IN ('masa', 'volumen', 'unidad_discreta'))
        )
    """)
    # Seed idempotente de unidades de medida
    for _um_cod, _um_nom, _um_tipo, _um_fac in [
        ('kg',  'Kilogramo',  'masa',             1.0),
        ('g',   'Gramo',      'masa',              0.001),
        ('lb',  'Libra',      'masa',              0.45359237),
        ('lt',  'Litro',      'volumen',            1.0),
        ('ml',  'Mililitro',  'volumen',            0.001),
        ('und', 'Unidad',     'unidad_discreta',    1.0),
        ('prc', 'Porción',    'unidad_discreta',    1.0),
        ('cj',  'Caja',       'unidad_discreta',    1.0),
    ]:
        cur.execute(
            "INSERT INTO unidades_medida (codigo, nombre, tipo, factor_base) "
            "VALUES (%s, %s, %s, %s) ON CONFLICT (codigo) DO NOTHING",
            (_um_cod, _um_nom, _um_tipo, _um_fac)
        )

    # A2. proveedores — maestro con estado de aprobación; hoy es texto libre
    cur.execute("""
        CREATE TABLE IF NOT EXISTS proveedores (
            id                   SERIAL PRIMARY KEY,
            codigo               TEXT UNIQUE,
            nombre               TEXT NOT NULL,
            nit                  TEXT,
            dv                   TEXT,
            contacto_nombre      TEXT,
            contacto_tel         TEXT,
            contacto_email       TEXT,
            direccion            TEXT,
            ciudad               TEXT,
            estado               TEXT NOT NULL DEFAULT 'en_evaluacion',
                -- 'aprobado' | 'suspendido' | 'en_evaluacion' | 'rechazado'
            lead_time_dias       INTEGER NOT NULL DEFAULT 1,
            dias_entrega         TEXT,
            condiciones_pago     TEXT,
            notas                TEXT,
            created_at           TIMESTAMP NOT NULL DEFAULT NOW(),
            updated_at           TIMESTAMP NOT NULL DEFAULT NOW(),
            created_by           TEXT,
            CONSTRAINT chk_prov_estado
                CHECK (estado IN ('aprobado', 'suspendido', 'en_evaluacion', 'rechazado'))
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_prov_estado ON proveedores(estado)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_prov_nombre ON proveedores(nombre)")

    # A3. ubicaciones — catálogo de zonas físicas de almacenamiento
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ubicaciones (
            id           SERIAL PRIMARY KEY,
            codigo       TEXT UNIQUE NOT NULL,
            nombre       TEXT NOT NULL,
            tipo         TEXT NOT NULL DEFAULT 'ambiente',
                -- 'ambiente' | 'refrigerado' | 'congelado' | 'cuarentena' | 'despacho'
            temp_min     NUMERIC(5,1),
            temp_max     NUMERIC(5,1),
            capacidad_m3 NUMERIC(8,2),
            activo       BOOLEAN NOT NULL DEFAULT TRUE,
            created_at   TIMESTAMP NOT NULL DEFAULT NOW(),
            CONSTRAINT chk_ubic_tipo
                CHECK (tipo IN ('ambiente', 'refrigerado', 'congelado', 'cuarentena', 'despacho'))
        )
    """)
    # Seed idempotente de ubicaciones mínimas
    for _ub_cod, _ub_nom, _ub_tipo, _ub_tmin, _ub_tmax in [
        ('AMB-01',  'Almacén Ambiente',  'ambiente',    None,   None),
        ('FRIO-01', 'Cámara de Frío',    'refrigerado',  0.0,    8.0),
        ('CONG-01', 'Cuarto Congelado',  'congelado',  -20.0,  -15.0),
        ('CUAR-01', 'Zona Cuarentena',   'cuarentena',  None,   None),
    ]:
        cur.execute(
            "INSERT INTO ubicaciones (codigo, nombre, tipo, temp_min, temp_max) "
            "VALUES (%s, %s, %s, %s, %s) ON CONFLICT (codigo) DO NOTHING",
            (_ub_cod, _ub_nom, _ub_tipo, _ub_tmin, _ub_tmax)
        )

    conn.commit()
    print("✅ Fase 0 · Grupo A completado: unidades_medida, proveedores, ubicaciones")

    # ── FASE 0 · Grupo B: Modificaciones a tablas existentes ─────────────────

    # B1. materias_primas — cadena de frío, tipificación y unidades
    cur.execute("SAVEPOINT sp_f0_b_mp")
    cur.execute("""
        ALTER TABLE materias_primas
            ADD COLUMN IF NOT EXISTS tipo_almacenamiento    TEXT    DEFAULT 'ambiente',
            ADD COLUMN IF NOT EXISTS temp_min               NUMERIC(5,1),
            ADD COLUMN IF NOT EXISTS temp_max               NUMERIC(5,1),
            ADD COLUMN IF NOT EXISTS requiere_cadena_frio   BOOLEAN DEFAULT FALSE,
            ADD COLUMN IF NOT EXISTS vida_util_dias         INTEGER DEFAULT 30,
            ADD COLUMN IF NOT EXISTS tipo_mp                TEXT    DEFAULT 'ingrediente',
            ADD COLUMN IF NOT EXISTS unidad_base_id         INTEGER REFERENCES unidades_medida(id),
            ADD COLUMN IF NOT EXISTS proveedor_principal_id INTEGER REFERENCES proveedores(id)
    """)
    cur.execute("RELEASE SAVEPOINT sp_f0_b_mp")
    # CHECK constraints en tabla existente → DO $$ handler
    cur.execute("""
        DO $$ BEGIN
            ALTER TABLE materias_primas
                ADD CONSTRAINT chk_mp_tipo_almacenamiento
                CHECK (tipo_almacenamiento IN ('ambiente', 'refrigerado', 'congelado'));
        EXCEPTION WHEN duplicate_object THEN NULL;
        END $$
    """)
    cur.execute("""
        DO $$ BEGIN
            ALTER TABLE materias_primas
                ADD CONSTRAINT chk_mp_tipo_mp
                CHECK (tipo_mp IN ('ingrediente', 'empaque', 'insumo'));
        EXCEPTION WHEN duplicate_object THEN NULL;
        END $$
    """)

    # B2. recetas — merma esperada para comparar contra merma real de la OP
    cur.execute("SAVEPOINT sp_f0_b_recetas")
    cur.execute("""
        ALTER TABLE recetas
            ADD COLUMN IF NOT EXISTS merma_esperada_pct NUMERIC(6,2) DEFAULT 0
    """)
    cur.execute("RELEASE SAVEPOINT sp_f0_b_recetas")

    # B3. receta_ingredientes — factor de merma teórica y conversión de unidades
    #     factor_merma: fracción de pérdida esperada (0.15 = 15% de merma)
    #     factor_conversion_a_base: cuántas unidades base = 1 unidad_receta_id
    #       ej: receta en gramos, inventario en kg → factor = 0.001
    cur.execute("SAVEPOINT sp_f0_b_recing")
    cur.execute("""
        ALTER TABLE receta_ingredientes
            ADD COLUMN IF NOT EXISTS factor_merma           NUMERIC(6,4)  DEFAULT 0,
            ADD COLUMN IF NOT EXISTS unidad_receta_id       INTEGER REFERENCES unidades_medida(id),
            ADD COLUMN IF NOT EXISTS factor_conversion_a_base NUMERIC(18,8) DEFAULT 1
    """)
    cur.execute("RELEASE SAVEPOINT sp_f0_b_recing")

    # B4. ordenes_compra — FK al maestro de proveedores
    cur.execute("SAVEPOINT sp_f0_b_oc")
    cur.execute("""
        ALTER TABLE ordenes_compra
            ADD COLUMN IF NOT EXISTS proveedor_id INTEGER REFERENCES proveedores(id)
    """)
    cur.execute("RELEASE SAVEPOINT sp_f0_b_oc")

    # B5. remisiones — FK a proveedores, a OC y tipo de recepción
    cur.execute("SAVEPOINT sp_f0_b_rem")
    cur.execute("""
        ALTER TABLE remisiones
            ADD COLUMN IF NOT EXISTS proveedor_id   INTEGER REFERENCES proveedores(id),
            ADD COLUMN IF NOT EXISTS oc_id          INTEGER REFERENCES ordenes_compra(id),
            ADD COLUMN IF NOT EXISTS tipo_recepcion TEXT DEFAULT 'compra'
    """)
    cur.execute("RELEASE SAVEPOINT sp_f0_b_rem")
    cur.execute("""
        DO $$ BEGIN
            ALTER TABLE remisiones
                ADD CONSTRAINT chk_rem_tipo_recepcion
                CHECK (tipo_recepcion IN ('compra', 'devolucion_cliente', 'transferencia'));
        EXCEPTION WHEN duplicate_object THEN NULL;
        END $$
    """)

    # B6. remision_items — datos del lote del proveedor y temperatura de recepción
    #     mp_lote_id se agrega en Grupo C, después de crear mp_lotes
    cur.execute("SAVEPOINT sp_f0_b_remitems")
    cur.execute("""
        ALTER TABLE remision_items
            ADD COLUMN IF NOT EXISTS lote_proveedor              TEXT,
            ADD COLUMN IF NOT EXISTS fecha_vencimiento_proveedor DATE,
            ADD COLUMN IF NOT EXISTS temp_recepcion              NUMERIC(5,1)
    """)
    cur.execute("RELEASE SAVEPOINT sp_f0_b_remitems")

    conn.commit()
    print("✅ Fase 0 · Grupo B completado: 6 tablas existentes modificadas")

    # ── FASE 0 · Grupo C: mp_lotes y puente con remision_items ───────────────

    # C1. mp_lotes — entidad central de trazabilidad de materias primas
    #     Nace al guardar el remision_item, estado = 'cuarentena'.
    #     NO genera movimiento en kardex hasta que calidad lo libere.
    #     El stock real del lote = SUM(inv_movimientos.cantidad WHERE mp_lote_id = id).
    #     cantidad_recibida es el hecho histórico de recepción; no es el saldo.
    #     fecha_vencimiento = MIN(vencimiento_proveedor, fecha_recepcion + vida_util_dias)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS mp_lotes (
            id                    SERIAL PRIMARY KEY,
            lote                  TEXT UNIQUE NOT NULL,
                -- formato: LMP-YYYYMMDD-{id:06d}

            -- Qué MP es
            mp_id                 INTEGER NOT NULL REFERENCES materias_primas(id),
            unidad                TEXT NOT NULL,

            -- De dónde viene (origen documental)
            proveedor_id          INTEGER REFERENCES proveedores(id)    ON DELETE SET NULL,
            remision_id           INTEGER REFERENCES remisiones(id)     ON DELETE SET NULL,
            remision_item_id      INTEGER REFERENCES remision_items(id) ON DELETE SET NULL,

            -- Datos del proveedor (inmutables tras INSERT)
            lote_proveedor        TEXT,
            vencimiento_proveedor DATE,

            -- Fechas
            fecha_recepcion       DATE NOT NULL DEFAULT CURRENT_DATE,
            fecha_vencimiento     DATE NOT NULL,
                -- = MIN(vencimiento_proveedor, fecha_recepcion + mp.vida_util_dias)
                -- calculada por el servidor; nunca editable
            fecha_liberacion      DATE,

            -- Cantidad recibida (inmutable — no es el saldo)
            cantidad_recibida     NUMERIC(12,4) NOT NULL,
            precio_unitario       NUMERIC(12,4),
                -- precio de la OC / remisión al momento de entrada; base para costeo FIFO

            -- Estado de calidad
            estado                TEXT NOT NULL DEFAULT 'cuarentena',
                -- 'cuarentena'          → recibido, pendiente inspección
                -- 'liberado'            → aprobado por calidad, disponible
                -- 'rechazado'           → no apto, sin movimiento en kardex
                -- 'agotado'             → liberado y consumido totalmente
                -- 'vencido'             → superó fecha_vencimiento
                -- 'cuarentena_especial' → retención por anomalía (temp, docs, etc.)

            -- Cadena de frío
            temp_recepcion        NUMERIC(5,1),
            temp_dentro_rango     BOOLEAN,

            -- Ubicación física
            ubicacion_id          INTEGER REFERENCES ubicaciones(id) ON DELETE SET NULL,

            -- Auditoría de liberación / rechazo
            liberado_por          TEXT,
            rechazado_por         TEXT,
            motivo_rechazo        TEXT,
            notas                 TEXT,

            -- Auditoría general
            created_at            TIMESTAMP NOT NULL DEFAULT NOW(),
            updated_at            TIMESTAMP NOT NULL DEFAULT NOW(),
            created_by            TEXT,

            CONSTRAINT chk_mpl_estado CHECK (
                estado IN (
                    'cuarentena', 'liberado', 'rechazado',
                    'agotado', 'vencido', 'cuarentena_especial'
                )
            ),
            CONSTRAINT chk_mpl_cantidad    CHECK (cantidad_recibida > 0),
            CONSTRAINT chk_mpl_vencimiento CHECK (fecha_vencimiento >= fecha_recepcion)
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_mpl_mp_id      ON mp_lotes(mp_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_mpl_estado      ON mp_lotes(estado)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_mpl_vencimiento ON mp_lotes(fecha_vencimiento)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_mpl_proveedor   ON mp_lotes(proveedor_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_mpl_remision    ON mp_lotes(remision_id)")
    # Índice parcial FEFO: lotes liberados de una MP ordenados por próximo vencimiento
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_mpl_fefo
        ON mp_lotes(mp_id, fecha_vencimiento)
        WHERE estado = 'liberado'
    """)

    # C2. remision_items — FK al lote creado para este ítem (mp_lotes ya existe ✓)
    cur.execute("SAVEPOINT sp_f0_c_remitem_mplote")
    cur.execute("""
        ALTER TABLE remision_items
            ADD COLUMN IF NOT EXISTS mp_lote_id INTEGER
                REFERENCES mp_lotes(id) ON DELETE SET NULL
    """)
    cur.execute("RELEASE SAVEPOINT sp_f0_c_remitem_mplote")
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_remitem_mplote
        ON remision_items(mp_lote_id)
        WHERE mp_lote_id IS NOT NULL
    """)

    conn.commit()
    print("✅ Fase 0 · Grupo C completado: mp_lotes + puente remision_items.mp_lote_id")

    # ── FASE 0 · Grupo D: Producción (manejo de FK circular OP ↔ PT) ─────────

    # D1. ordenes_produccion — SIN pt_lote_id (se agrega en D4 después de pt_lotes)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ordenes_produccion (
            id                  SERIAL PRIMARY KEY,
            numero              TEXT UNIQUE NOT NULL,
                -- formato: OP-YYYYMMDD-{id:06d}

            -- Qué se produce
            receta_id           INTEGER NOT NULL REFERENCES recetas(id),
            receta_version      INTEGER NOT NULL DEFAULT 1,

            -- Cuánto
            porciones_planeadas NUMERIC(10,2) NOT NULL,
            porciones_reales    NUMERIC(10,2),
            peso_batch_kg       NUMERIC(10,3),

            -- Cuándo
            fecha_programada    DATE NOT NULL,
            fecha_inicio        TIMESTAMP,
            fecha_fin           TIMESTAMP,

            -- Quién
            operario            TEXT,
            supervisor          TEXT,
            turno               TEXT,

            -- Estado
            estado              TEXT NOT NULL DEFAULT 'programada',
                -- 'programada'  → creada, MP aún sin reservar
                -- 'confirmada'  → MP reservada en inv_reservas
                -- 'en_proceso'  → inicio de producción registrado
                -- 'terminada'   → consumos registrados, PT en inventario
                -- 'no_conforme' → terminó con desvío de calidad inaceptable
                -- 'suspendida'  → detenida temporalmente, reservas siguen activas
                -- 'anulada'     → cancelada, reservas liberadas

            -- pt_lote_id → se agrega con ALTER TABLE en D4

            -- Merma
            merma_real_pct      NUMERIC(6,2),
                -- calculado al cerrar: comparar con recetas.merma_esperada_pct

            -- Observaciones
            notas               TEXT,
            notas_calidad       TEXT,

            -- Auditoría
            created_at          TIMESTAMP NOT NULL DEFAULT NOW(),
            updated_at          TIMESTAMP NOT NULL DEFAULT NOW(),
            created_by          TEXT,
            approved_by         TEXT,

            CONSTRAINT chk_op_estado CHECK (
                estado IN (
                    'programada', 'confirmada', 'en_proceso',
                    'terminada', 'no_conforme', 'suspendida', 'anulada'
                )
            )
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_op_receta ON ordenes_produccion(receta_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_op_fecha  ON ordenes_produccion(fecha_programada)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_op_estado ON ordenes_produccion(estado)")

    # D2. produccion (legado) — bridge hacia ordenes_produccion
    #     NULL para registros históricos; los nuevos batches usan ordenes_produccion
    cur.execute("SAVEPOINT sp_f0_d_prod_bridge")
    cur.execute("""
        ALTER TABLE produccion
            ADD COLUMN IF NOT EXISTS op_id INTEGER
                REFERENCES ordenes_produccion(id) ON DELETE SET NULL
    """)
    cur.execute("RELEASE SAVEPOINT sp_f0_d_prod_bridge")

    # D3. pt_lotes — lote de Producto Terminado con vencimiento calculado y costos reales
    #     op_id FK ordenes_produccion es válida aquí (D1 ya existe ✓)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS pt_lotes (
            id                   SERIAL PRIMARY KEY,
            lote                 TEXT UNIQUE NOT NULL,
                -- formato: LOT-YYYYMMDD-{id:06d} (compatible con produccion.lote actual)

            -- Qué producto es
            receta_id            INTEGER NOT NULL REFERENCES recetas(id),
            tipo                 TEXT NOT NULL DEFAULT 'producto_terminado',
                -- 'producto_terminado' | 'subproducto' | 'reproceso'

            -- Origen
            op_id                INTEGER REFERENCES ordenes_produccion(id) ON DELETE SET NULL,
                -- OP que generó este lote
            op_origen_id         INTEGER REFERENCES ordenes_produccion(id) ON DELETE SET NULL,
                -- Para reprocesos: la OP original del lote rechazado

            -- Cantidades
            porciones_producidas NUMERIC(10,2) NOT NULL,
            unidad               TEXT NOT NULL DEFAULT 'porcion',
            peso_total_kg        NUMERIC(10,3),

            -- Vencimiento
            fecha_produccion     DATE NOT NULL DEFAULT CURRENT_DATE,
            fecha_vencimiento    DATE NOT NULL,
                -- = fecha_produccion + recetas.vida_util_dias; calculada por el servidor

            -- Estado
            estado               TEXT NOT NULL DEFAULT 'en_produccion',
                -- 'en_produccion' → OP abierta, lote no listo
                -- 'cuarentena'    → producción terminada, pendiente inspección final
                -- 'liberado'      → aprobado, disponible para despacho (FEFO)
                -- 'rechazado'     → no apto, no puede despacharse
                -- 'agotado'       → despachado en totalidad
                -- 'vencido'       → superó fecha_vencimiento
                -- 'retirado'      → recall activo
                -- 'reproceso'     → enviado a reprocesar (genera nueva OP + nuevo pt_lote)

            -- Ubicación
            ubicacion_id         INTEGER REFERENCES ubicaciones(id) ON DELETE SET NULL,

            -- Costos reales (se calculan al cerrar la OP)
            costo_mp_real        NUMERIC(14,2),
            costo_mo             NUMERIC(14,2),
            costo_servicios      NUMERIC(14,2),
            costo_total_real     NUMERIC(14,2),
            costo_por_porcion    NUMERIC(12,4),

            -- Calidad
            liberado_por         TEXT,
            rechazado_por        TEXT,
            motivo_rechazo       TEXT,
            notas                TEXT,

            -- Auditoría
            created_at           TIMESTAMP NOT NULL DEFAULT NOW(),
            updated_at           TIMESTAMP NOT NULL DEFAULT NOW(),
            created_by           TEXT,

            CONSTRAINT chk_ptl_estado CHECK (
                estado IN (
                    'en_produccion', 'cuarentena', 'liberado', 'rechazado',
                    'agotado', 'vencido', 'retirado', 'reproceso'
                )
            ),
            CONSTRAINT chk_ptl_tipo CHECK (
                tipo IN ('producto_terminado', 'subproducto', 'reproceso')
            ),
            CONSTRAINT chk_ptl_vencimiento CHECK (
                fecha_vencimiento >= fecha_produccion
            )
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ptl_receta      ON pt_lotes(receta_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ptl_estado      ON pt_lotes(estado)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ptl_vencimiento ON pt_lotes(fecha_vencimiento)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ptl_op          ON pt_lotes(op_id)")
    # Índice parcial FEFO: lotes liberados de un producto ordenados por vencimiento
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_ptl_fefo
        ON pt_lotes(receta_id, fecha_vencimiento)
        WHERE estado = 'liberado'
    """)

    # D4. ordenes_produccion — agregar pt_lote_id (pt_lotes ya existe ✓)
    cur.execute("SAVEPOINT sp_f0_d_op_ptlote")
    cur.execute("""
        ALTER TABLE ordenes_produccion
            ADD COLUMN IF NOT EXISTS pt_lote_id INTEGER
                REFERENCES pt_lotes(id) ON DELETE SET NULL
    """)
    cur.execute("RELEASE SAVEPOINT sp_f0_d_op_ptlote")

    conn.commit()
    print("✅ Fase 0 · Grupo D completado: ordenes_produccion, pt_lotes, bridge produccion.op_id")

    # ── FASE 0 · Grupo E: Kardex inmutable (inv_movimientos) ─────────────────

    # E1. inv_movimientos — único origen de verdad del stock
    #     BIGSERIAL para volumen alto de escritura. INMUTABLE por trigger en E3.
    #     Positivo = entrada. Negativo = salida. Cero = prohibido por constraint.
    #     El saldo de un lote = SUM(cantidad) WHERE mp_lote_id / pt_lote_id = X.
    cur.execute("""
        CREATE TABLE IF NOT EXISTS inv_movimientos (
            id              BIGSERIAL PRIMARY KEY,

            -- Cuándo
            fecha           DATE      NOT NULL DEFAULT CURRENT_DATE,
            fecha_hora      TIMESTAMP NOT NULL DEFAULT NOW(),

            -- Qué se mueve
            tipo_inventario TEXT NOT NULL,
                -- 'mp' | 'pt'

            -- Para tipo_inventario = 'mp'
            mp_lote_id      INTEGER REFERENCES mp_lotes(id),
            mp_id           INTEGER REFERENCES materias_primas(id),
                -- denormalizado de mp_lotes para queries de inventario sin JOIN

            -- Para tipo_inventario = 'pt'
            pt_lote_id      INTEGER REFERENCES pt_lotes(id),
            receta_id       INTEGER REFERENCES recetas(id),
                -- denormalizado de pt_lotes

            -- Tipo de movimiento
            tipo_movimiento TEXT NOT NULL,
                -- ENTRADAS (cantidad > 0):
                --   'recepcion'             → MP liberada por calidad desde remisión
                --   'ajuste_entrada'        → ajuste físico positivo autorizado
                --   'devolucion_cliente'    → PT devuelto por cliente
                --   'apertura_inventario'   → saldo inicial en migración
                --   'transferencia_entrada' → traslado entre ubicaciones (entrada)
                -- SALIDAS (cantidad < 0):
                --   'consumo_produccion'    → MP consumida en una OP
                --   'ajuste_salida'         → ajuste físico negativo autorizado
                --   'despacho'              → PT enviado a cliente
                --   'merma'                 → diferencia consumo real vs teórico
                --   'destruccion'           → baja autorizada de producto no apto
                --   'devolucion_proveedor'  → MP rechazada devuelta al proveedor
                --   'transferencia_salida'  → traslado entre ubicaciones (salida)
                --   'vencimiento_baja'      → baja automática por vencimiento

            -- Cantidad: positivo = entrada, negativo = salida. NUNCA cero.
            cantidad        NUMERIC(12,4) NOT NULL,
            unidad          TEXT NOT NULL,

            -- Costeo real al momento del movimiento
            costo_unitario  NUMERIC(12,4),
            costo_total     NUMERIC(14,2),
                -- ABS(cantidad) * costo_unitario; calculado por la aplicación

            -- Trazabilidad documental (hacia qué documento origina este movimiento)
            doc_tipo        TEXT,
                -- 'remision_entrada' | 'orden_produccion' | 'remision_salida'
                -- 'ajuste_inventario' | 'devolucion'
            doc_id          INTEGER,
            doc_numero      TEXT,

            -- Ubicación
            ubicacion_id    INTEGER REFERENCES ubicaciones(id) ON DELETE SET NULL,

            -- Auditoría — campos inmutables (nunca se actualizan)
            notas           TEXT,
            created_at      TIMESTAMP NOT NULL DEFAULT NOW(),
            created_by      TEXT      NOT NULL DEFAULT 'sistema',
            approved_by     TEXT,
                -- para ajuste_entrada / ajuste_salida: quien autorizó el ajuste

            -- Constraints de coherencia
            CONSTRAINT chk_invm_tipo_inv CHECK (
                tipo_inventario IN ('mp', 'pt')
            ),
            CONSTRAINT chk_invm_tipo_mov CHECK (
                tipo_movimiento IN (
                    'recepcion', 'ajuste_entrada', 'devolucion_cliente',
                    'apertura_inventario', 'transferencia_entrada',
                    'consumo_produccion', 'ajuste_salida', 'despacho',
                    'merma', 'destruccion', 'devolucion_proveedor',
                    'transferencia_salida', 'vencimiento_baja'
                )
            ),
            CONSTRAINT chk_invm_cantidad_no_cero CHECK (cantidad <> 0),
            CONSTRAINT chk_invm_mp_coherente CHECK (
                tipo_inventario <> 'mp' OR mp_lote_id IS NOT NULL
            ),
            CONSTRAINT chk_invm_pt_coherente CHECK (
                tipo_inventario <> 'pt' OR pt_lote_id IS NOT NULL
            )
        )
    """)
    # Índice parcial para calcular saldo de un lote de MP: SUM(cantidad) WHERE mp_lote_id=X
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_invm_mplote
        ON inv_movimientos(mp_lote_id, cantidad)
        WHERE mp_lote_id IS NOT NULL
    """)
    # Índice parcial para saldo de un lote de PT
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_invm_ptlote
        ON inv_movimientos(pt_lote_id, cantidad)
        WHERE pt_lote_id IS NOT NULL
    """)
    # Índice para trazabilidad hacia adelante: todos los movimientos de un mp_id
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_invm_mpid
        ON inv_movimientos(mp_id)
        WHERE mp_id IS NOT NULL
    """)
    # Índice para navegar desde un documento a sus movimientos
    cur.execute("CREATE INDEX IF NOT EXISTS idx_invm_doc      ON inv_movimientos(doc_tipo, doc_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_invm_fecha     ON inv_movimientos(fecha)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_invm_tipo_mov  ON inv_movimientos(tipo_movimiento)")

    # E2. Función del trigger de inmutabilidad del kardex
    #     current_setting('app.migration_mode', true): el segundo parámetro 'true'
    #     es missing_ok → devuelve NULL si el parámetro no existe (comportamiento normal).
    #     Para cargas de datos iniciales: SET app.migration_mode = 'true' en la sesión.
    #     TG_OP distingue UPDATE vs DELETE para el RETURN correcto en migration_mode.
    cur.execute("""
        CREATE OR REPLACE FUNCTION fn_kardex_immutable()
        RETURNS TRIGGER LANGUAGE plpgsql AS $$
        BEGIN
            IF current_setting('app.migration_mode', true) = 'true' THEN
                IF TG_OP = 'DELETE' THEN
                    RETURN OLD;
                END IF;
                RETURN NEW;
            END IF;
            RAISE EXCEPTION
                'inv_movimientos es inmutable (operación: %). '
                'Para corregir un error registre un movimiento compensatorio '
                'con doc_tipo=''correccion'' y referencia al id original en doc_id.',
                TG_OP;
            RETURN NULL;
        END;
        $$
    """)

    # E3. Trigger de inmutabilidad — DO $$ por idempotencia en re-deployments
    cur.execute("""
        DO $$ BEGIN
            CREATE TRIGGER tg_kardex_immutable
            BEFORE UPDATE OR DELETE ON inv_movimientos
            FOR EACH ROW EXECUTE FUNCTION fn_kardex_immutable();
        EXCEPTION WHEN duplicate_object THEN NULL;
        END $$
    """)

    conn.commit()
    print("✅ Fase 0 · Grupo E completado: inv_movimientos (BIGSERIAL) + trigger inmutabilidad")

    # ── FASE 0 · Grupo F: Consumos de producción ─────────────────────────────

    # F1. op_consumos — detalle de qué lote de MP se consumió en cada OP
    #     Puente de trazabilidad bidireccional: MP → OP → PT → cliente
    #     movimiento_id FK inv_movimientos es válida aquí (E1 ya existe ✓)
    #     diferencia = cantidad_real - cantidad_teorica (calculada por la aplicación)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS op_consumos (
            id               SERIAL PRIMARY KEY,

            op_id            INTEGER NOT NULL
                                 REFERENCES ordenes_produccion(id) ON DELETE CASCADE,
            mp_lote_id       INTEGER NOT NULL REFERENCES mp_lotes(id),
            mp_id            INTEGER NOT NULL REFERENCES materias_primas(id),

            -- Cantidades
            cantidad_teorica NUMERIC(12,4) NOT NULL,
                -- receta_ing.cantidad * factor_conversion_a_base / receta.porciones
                -- * op.porciones_planeadas * (1 + factor_merma)
                -- se calcula al confirmar la OP
            cantidad_real    NUMERIC(12,4),
                -- registrada por el operario al cerrar la OP; NULL mientras esté abierta
            diferencia       NUMERIC(12,4),
                -- cantidad_real - cantidad_teorica; calculada por la aplicación al cerrar
                -- positivo = usó más de lo previsto; negativo = usó menos
            unidad           TEXT NOT NULL,

            -- Costeo real
            costo_unitario   NUMERIC(12,4),
                -- precio_unitario del mp_lote en el momento del consumo
            costo_real       NUMERIC(14,2),
                -- cantidad_real * costo_unitario; calculado por la aplicación

            -- Tipo
            tipo             TEXT NOT NULL DEFAULT 'consumo',
                -- 'consumo' | 'devolucion_a_inventario'

            -- FK al movimiento del kardex generado al cerrar la OP
            movimiento_id    BIGINT REFERENCES inv_movimientos(id),
                -- NULL hasta que la OP se cierre y se registren los consumos reales

            -- Auditoría
            created_at       TIMESTAMP NOT NULL DEFAULT NOW(),
            created_by       TEXT,

            CONSTRAINT chk_opc_tipo          CHECK (tipo IN ('consumo', 'devolucion_a_inventario')),
            CONSTRAINT chk_opc_cantidad_teo  CHECK (cantidad_teorica > 0)
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_opc_op     ON op_consumos(op_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_opc_mplote ON op_consumos(mp_lote_id)")
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_opc_mpid
        ON op_consumos(mp_id)
    """)

    conn.commit()
    print("✅ Fase 0 · Grupo F completado: op_consumos")

    # ── FASE 0 · Grupo G: Reservas y ajustes de inventario ───────────────────

    # G1. inv_reservas — compromete stock sin mover el kardex
    #     stock_disponible(lote) = SUM(inv_movimientos.cantidad WHERE lote_id=X)
    #                            - SUM(inv_reservas.cantidad WHERE lote_id=X AND estado='activa')
    #     Ciclo: activa (al confirmar OP) → consumida (al cerrar OP) / liberada (al anular OP)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS inv_reservas (
            id              SERIAL PRIMARY KEY,
            tipo_inventario TEXT NOT NULL,

            mp_lote_id      INTEGER REFERENCES mp_lotes(id) ON DELETE CASCADE,
            pt_lote_id      INTEGER REFERENCES pt_lotes(id) ON DELETE CASCADE,

            -- Para qué documento se hace la reserva
            doc_tipo        TEXT NOT NULL,
                -- 'orden_produccion' | 'remision_salida'
            doc_id          INTEGER NOT NULL,
            doc_numero      TEXT,

            cantidad        NUMERIC(12,4) NOT NULL,
            unidad          TEXT NOT NULL,

            estado          TEXT NOT NULL DEFAULT 'activa',
                -- 'activa'    → reserva vigente; descuenta del stock disponible
                -- 'consumida' → la OP cerró y registró consumos reales en kardex
                -- 'liberada'  → la OP fue anulada; stock vuelve a estar disponible

            created_at      TIMESTAMP NOT NULL DEFAULT NOW(),
            updated_at      TIMESTAMP NOT NULL DEFAULT NOW(),
            created_by      TEXT,

            CONSTRAINT chk_res_tipo_inv CHECK (tipo_inventario IN ('mp', 'pt')),
            CONSTRAINT chk_res_estado   CHECK (estado IN ('activa', 'consumida', 'liberada')),
            CONSTRAINT chk_res_cantidad CHECK (cantidad > 0),
            -- XOR: una reserva es de MP o de PT, nunca ambos ni ninguno
            CONSTRAINT chk_res_lote_xor CHECK (
                (tipo_inventario = 'mp' AND mp_lote_id IS NOT NULL AND pt_lote_id IS NULL)
                OR
                (tipo_inventario = 'pt' AND pt_lote_id IS NOT NULL AND mp_lote_id IS NULL)
            )
        )
    """)
    # Índices parciales: solo reservas activas afectan el stock disponible
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_res_mp_lote
        ON inv_reservas(mp_lote_id, cantidad)
        WHERE mp_lote_id IS NOT NULL AND estado = 'activa'
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_res_pt_lote
        ON inv_reservas(pt_lote_id, cantidad)
        WHERE pt_lote_id IS NOT NULL AND estado = 'activa'
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_res_doc ON inv_reservas(doc_tipo, doc_id)")

    # G2. ajustes_inventario — cabecera del documento de ajuste físico
    #     Un ajuste requiere aprobación antes de generar movimientos en el kardex.
    #     La persona que lo aprueba no puede ser la misma que lo solicita (enforced en código).
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ajustes_inventario (
            id             SERIAL PRIMARY KEY,
            numero         TEXT UNIQUE NOT NULL,
                -- formato: AJU-YYYYMMDD-{id:06d}
            tipo           TEXT NOT NULL,
                -- 'conteo_fisico'          → resultado de inventario físico
                -- 'destruccion_autorizada' → baja de producto vencido, rechazado
                -- 'diferencia_produccion'  → ajuste post-cierre de OP
                -- 'correccion_error'       → corrección de registro incorrecto
            estado         TEXT NOT NULL DEFAULT 'pendiente',
                -- 'pendiente' → 'aprobado' → 'ejecutado'
                -- 'pendiente' → 'rechazado'
            fecha          DATE NOT NULL DEFAULT CURRENT_DATE,
            descripcion    TEXT NOT NULL,
            evidencia      TEXT,
                -- URL o referencia a foto / documento de soporte
            solicitado_por TEXT NOT NULL,
            aprobado_por   TEXT,
            aprobado_at    TIMESTAMP,
            rechazado_por  TEXT,
            motivo_rechazo TEXT,
            ejecutado_at   TIMESTAMP,
                -- cuándo se generaron los movimientos en inv_movimientos
            notas          TEXT,
            created_at     TIMESTAMP NOT NULL DEFAULT NOW(),

            CONSTRAINT chk_aju_tipo CHECK (
                tipo IN (
                    'conteo_fisico', 'destruccion_autorizada',
                    'diferencia_produccion', 'correccion_error'
                )
            ),
            CONSTRAINT chk_aju_estado CHECK (
                estado IN ('pendiente', 'aprobado', 'rechazado', 'ejecutado')
            )
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_aju_estado ON ajustes_inventario(estado)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_aju_fecha  ON ajustes_inventario(fecha)")

    # G3. ajuste_inventario_items — detalle por lote del ajuste
    #     movimiento_id se llena al ejecutar el ajuste (estado → 'ejecutado')
    #     inv_movimientos ya existe en este punto (Grupo E ✓)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ajuste_inventario_items (
            id               SERIAL PRIMARY KEY,
            ajuste_id        INTEGER NOT NULL
                                 REFERENCES ajustes_inventario(id) ON DELETE CASCADE,
            tipo_inventario  TEXT NOT NULL,
            mp_lote_id       INTEGER REFERENCES mp_lotes(id)       ON DELETE SET NULL,
            pt_lote_id       INTEGER REFERENCES pt_lotes(id)       ON DELETE SET NULL,
            cantidad_sistema NUMERIC(12,4) NOT NULL,
                -- lo que dice el kardex al momento del conteo
            cantidad_contada NUMERIC(12,4),
                -- lo que se encontró físicamente; NULL para destrucciones
            diferencia       NUMERIC(12,4),
                -- cantidad_contada - cantidad_sistema; calculado por la aplicación
            unidad           TEXT NOT NULL,
            motivo_item      TEXT,
            movimiento_id    BIGINT REFERENCES inv_movimientos(id),
                -- FK al movimiento generado en kardex al ejecutar el ajuste

            CONSTRAINT chk_ajui_tipo_inv    CHECK (tipo_inventario IN ('mp', 'pt')),
            CONSTRAINT chk_ajui_cant_contada CHECK (
                cantidad_contada IS NULL OR cantidad_contada >= 0
            )
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ajui_ajuste ON ajuste_inventario_items(ajuste_id)")
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_ajui_mplote
        ON ajuste_inventario_items(mp_lote_id)
        WHERE mp_lote_id IS NOT NULL
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_ajui_ptlote
        ON ajuste_inventario_items(pt_lote_id)
        WHERE pt_lote_id IS NOT NULL
    """)

    conn.commit()
    print("✅ Fase 0 · Grupo G completado: inv_reservas, ajustes_inventario, ajuste_inventario_items")

    # ── FASE 0 · Grupo H: Calidad básica de recepción ────────────────────────

    # H1. inspecciones_recepcion — registro formal de calidad por lote
    #     Es el único evento que cambia mp_lote.estado de 'cuarentena' a 'liberado'.
    #     Al aprobar → el servidor genera el movimiento 'recepcion' en inv_movimientos.
    #     Al rechazar → mp_lote queda en 'rechazado'; NUNCA se genera movimiento en kardex.
    cur.execute("""
        CREATE TABLE IF NOT EXISTS inspecciones_recepcion (
            id                SERIAL PRIMARY KEY,

            -- Vinculación al documento de recepción
            remision_id       INTEGER NOT NULL
                                  REFERENCES remisiones(id) ON DELETE CASCADE,
            remision_item_id  INTEGER REFERENCES remision_items(id) ON DELETE SET NULL,
            mp_lote_id        INTEGER REFERENCES mp_lotes(id)       ON DELETE SET NULL,

            fecha             DATE NOT NULL DEFAULT CURRENT_DATE,

            -- Cadena de frío
            temp_recepcion    NUMERIC(5,1),
            temp_dentro_rango BOOLEAN,
                -- TRUE si temp_recepcion está entre mp.temp_min y mp.temp_max

            -- Evaluación sensorial
            apariencia        TEXT NOT NULL DEFAULT 'conforme',
            olor              TEXT NOT NULL DEFAULT 'conforme',
            color             TEXT NOT NULL DEFAULT 'conforme',
            textura           TEXT NOT NULL DEFAULT 'conforme',
            empaque_estado    TEXT NOT NULL DEFAULT 'conforme',
                -- cada campo: 'conforme' | 'no_conforme'

            -- Documentación del proveedor
            docs_sanitarios   BOOLEAN NOT NULL DEFAULT FALSE,
                -- TRUE si el proveedor entregó guías sanitarias / certificados requeridos

            -- Resultado
            resultado         TEXT NOT NULL DEFAULT 'pendiente',
                -- 'pendiente'                → en evaluación
                -- 'aprobado'                 → todo conforme; libera el mp_lote
                -- 'aprobado_con_condiciones' → aprobado con observaciones; requiere seguimiento
                -- 'rechazado'                → no apto; mp_lote pasa a 'rechazado'

            observaciones     TEXT,

            -- Auditoría
            created_at        TIMESTAMP NOT NULL DEFAULT NOW(),
            created_by        TEXT NOT NULL DEFAULT 'sistema',
                -- quien hizo la inspección (operario de recepción)
            approved_by       TEXT,
                -- quien de calidad ratificó el resultado
            approved_at       TIMESTAMP,

            CONSTRAINT chk_insp_resultado CHECK (
                resultado IN (
                    'pendiente', 'aprobado', 'aprobado_con_condiciones', 'rechazado'
                )
            )
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_insp_remision  ON inspecciones_recepcion(remision_id)")
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_insp_mplote
        ON inspecciones_recepcion(mp_lote_id)
        WHERE mp_lote_id IS NOT NULL
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_insp_resultado ON inspecciones_recepcion(resultado)")

    conn.commit()
    print("✅ Fase 0 · Grupo H completado: inspecciones_recepcion")

    print("✅ Fase 0 completa: 9 tablas nuevas, 7 tablas modificadas, kardex inmutable activo")

    cur.close()
    conn.close()
    print("✅ Base de datos Daily Sistema Gestión inicializada")

def init_db():
    try:
        _create_tables()
    except Exception as e:
        print(f"⚠️  Sin base de datos: {e}. Corriendo en modo preview.")
