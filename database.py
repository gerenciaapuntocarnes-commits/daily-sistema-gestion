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
    cur.close()
    conn.close()
    print("✅ Base de datos Daily Sistema Gestión inicializada")

def init_db():
    try:
        _create_tables()
    except Exception as e:
        print(f"⚠️  Sin base de datos: {e}. Corriendo en modo preview.")
