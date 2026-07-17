"""
ETL v8: Extrae cierres de turno de SQL Server y los migra a PostgreSQL.
Instancia dedicada a una única estación (SQLSERVER_DB / SCHEMA / IDEstacion en .env).
Uso: python etl.py <fecha_desde> <fecha_hasta>
"""
import os
import sys
import re
import decimal
import datetime as _dt
import threading
from queue import Queue
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import pyodbc
import psycopg2
from psycopg2 import extras

# ---------------------------------------------------------------------------
# Cargar .env (encoding latin-1 para tildes y caracteres especiales)
# ---------------------------------------------------------------------------
_ENV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
if os.path.exists(_ENV_PATH):
    with open(_ENV_PATH, 'r', encoding='latin-1') as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith('#') and '=' in _line:
                _k, _v = _line.split('=', 1)
                os.environ[_k.strip()] = _v.strip()

import pg_tunnel

# ---------------------------------------------------------------------------
# Argumentos
# ---------------------------------------------------------------------------
MODO_INCREMENTAL = len(sys.argv) >= 2 and sys.argv[1] == '--incremental'

if not MODO_INCREMENTAL and len(sys.argv) < 3:
    print("Uso: python etl.py <fecha_desde> <fecha_hasta>")
    print("     python etl.py --incremental")
    print("  Ejemplo: python etl.py 2026-03-01 2026-03-31")
    sys.exit(1)

FECHA_DESDE = None if MODO_INCREMENTAL else sys.argv[1]
FECHA_HASTA = None if MODO_INCREMENTAL else sys.argv[2]

# Módulos en paralelo por ID de cierre
MAX_WORKERS = 5

# ---------------------------------------------------------------------------
# Configuración PostgreSQL (única BD, un solo schema — la estación de este .env)
# ---------------------------------------------------------------------------

SCHEMA = os.getenv('SCHEMA')

# sync_table() crea cada tabla en todos los schemas activos; en esta instancia
# de estación única, la lista contiene solo el schema propio.
ALL_SCHEMAS: list[str] = [SCHEMA] if SCHEMA else []

# Credenciales SQL Server
SQL_HOST = os.getenv('SQLSERVER_HOST_1')
SQL_PORT = os.getenv('SQLSERVER_PORT_1', '1433')
SQL_USER = os.getenv('SQLSERVER_USER_1')
SQL_PASS = os.getenv('SQLSERVER_PASSWORD_1')
SQL_DB   = os.getenv('SQLSERVER_DB')

ID_ESTACION = os.getenv('IDEstacion')
if not SQL_DB or not SCHEMA or not ID_ESTACION:
    print("Error: faltan SQLSERVER_DB, SCHEMA o IDEstacion en .env.")
    sys.exit(1)
ID_ESTACION = int(ID_ESTACION.strip())

# ---------------------------------------------------------------------------
# Rutas de logs
# ---------------------------------------------------------------------------
_BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
LOGS_IDS_DIR  = os.path.join(_BASE_DIR, 'logs_ids')
LOGS_EXEC_DIR = os.path.join(_BASE_DIR, 'logs_ejecuciones')
os.makedirs(LOGS_IDS_DIR,  exist_ok=True)
os.makedirs(LOGS_EXEC_DIR, exist_ok=True)

# ---------------------------------------------------------------------------
# Logger thread-safe (stdout + archivo simultáneo)
# ---------------------------------------------------------------------------
class Logger:
    def __init__(self, filepath):
        self._f    = open(filepath, 'w', encoding='utf-8')
        self._lock = threading.Lock()

    def log(self, msg=''):
        ts   = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        line = f"[{ts}] {msg}" if msg else ''
        with self._lock:
            print(line, flush=True)
            self._f.write(line + '\n')
            self._f.flush()

    def close(self):
        self._f.close()

# ---------------------------------------------------------------------------
# Pool de conexiones — se crea UNA VEZ por grupo y se reutiliza entre IDs
# ---------------------------------------------------------------------------
class ConnectionPool:
    def __init__(self, size, sql_db, schema):
        self._sql_q = Queue()
        self._pg_q  = Queue()
        for _ in range(size):
            self._sql_q.put(_connect_sql(sql_db))
            self._pg_q.put(_connect_pg(schema))

    def acquire(self):
        return self._sql_q.get(), self._pg_q.get()

    def release(self, sql_conn, pg_conn):
        self._sql_q.put(sql_conn)
        self._pg_q.put(pg_conn)

    def close_all(self):
        while not self._sql_q.empty():
            try:
                self._sql_q.get_nowait().close()
            except Exception:
                pass
        while not self._pg_q.empty():
            try:
                self._pg_q.get_nowait().close()
            except Exception:
                pass

# ---------------------------------------------------------------------------
# Conexiones
# ---------------------------------------------------------------------------
def _connect_sql(db):
    cs = (f'DRIVER={{SQL Server}};SERVER={SQL_HOST};DATABASE={db};'
          f'UID={SQL_USER};PWD={SQL_PASS};TrustServerCertificate=True')
    conn = pyodbc.connect(cs)
    conn.timeout = 600
    return conn

def _connect_pg(schema):
    return pg_tunnel.conectar_pg(schema)

# ---------------------------------------------------------------------------
# Mapeo de tipos pyodbc → PostgreSQL (basado en tipo real de SQL Server)
# ---------------------------------------------------------------------------
_TYPE_MAP = {
    bool:            'BOOLEAN',
    int:             'BIGINT',
    float:           'DOUBLE PRECISION',
    decimal.Decimal: 'NUMERIC(18,8)',
    _dt.datetime:    'TIMESTAMP',
    _dt.date:        'DATE',
    str:             'TEXT',
    bytes:           'BYTEA',
}

def get_pg_type(type_code):
    return _TYPE_MAP.get(type_code, 'TEXT')

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def to_snake(name):
    return re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()

# ---------------------------------------------------------------------------
# Cache de columnas PG — evita consultar information_schema en cada upsert
# ---------------------------------------------------------------------------
_pg_cols_cache:   dict = {}
_cajas_lkp_cache: dict = {}
_bancos_lkp_cache: dict = {}


def _get_pg_cols(pg_conn, schema, table):
    """Devuelve el set de columnas reales de {schema}.{table} en PG (cacheado)."""
    key = f'{schema}.{table}'
    if key not in _pg_cols_cache:
        cur = pg_conn.cursor()
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s",
            (schema, table)
        )
        _pg_cols_cache[key] = {r[0] for r in cur.fetchall()}
        cur.close()
    return _pg_cols_cache[key]


def _get_cajas_lkp(pg_conn, schema):
    """Devuelve {DESCRIPCION_UPPER: id_caja} para lookup por nombre de caja."""
    if schema not in _cajas_lkp_cache:
        cur = pg_conn.cursor()
        cur.execute(f'SELECT id_caja, descripcion FROM {schema}.cajas')
        _cajas_lkp_cache[schema] = {
            str(r[1] or '').strip().upper(): r[0] for r in cur.fetchall()
        }
        cur.close()
    return _cajas_lkp_cache[schema]


def _get_bancos_lkp(pg_conn, schema):
    """Devuelve {NOMBRE_UPPER: id_banco} para lookup por nombre de banco."""
    if schema not in _bancos_lkp_cache:
        cur = pg_conn.cursor()
        cur.execute(f'SELECT id_banco, nombre FROM {schema}.bancos')
        _bancos_lkp_cache[schema] = {
            str(r[1] or '').strip().upper(): r[0] for r in cur.fetchall()
        }
        cur.close()
    return _bancos_lkp_cache[schema]


# ---------------------------------------------------------------------------
# _handle_factura_pendiente — calcula importe pendiente desde @TablaResult
# ---------------------------------------------------------------------------
def _handle_factura_pendiente(cursor, id_cierre, schema, pg_conn):
    result_cols = None
    result_rows = None

    while True:
        if cursor.description:
            cols = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
            is_target = (
                (rows and cols[0] == 'Tabla' and str(rows[0][0]).startswith('@TablaResult'))
                or ('Despachado' in cols and 'FacturasContado' in cols and 'RemitosCuentaCorriente' in cols)
            )
            if is_target:
                result_cols = cols
                result_rows = rows
                break
        if not cursor.nextset():
            break

    if result_rows is None:
        return {}

    ci = {c: i for i, c in enumerate(result_cols)}

    def cv(row, name):
        idx = ci.get(name)
        v   = row[idx] if idx is not None else None
        return decimal.Decimal(str(v)) if v is not None else decimal.Decimal(0)

    total = decimal.Decimal(0)
    for row in result_rows:
        despachado  = cv(row, 'Despachado')
        total_fact  = (cv(row, 'FacturasContado') + cv(row, 'FacturasCuentaCorriente') +
                       cv(row, 'RemitosCuentaCorriente') + cv(row, 'RemitosContado') +
                       cv(row, 'EspecialesCuentaCorriente'))
        litros = despachado - total_fact
        if litros > 0:
            total += litros * cv(row, 'Precio')

    fp_ddl = '(id_cierre_turno BIGINT PRIMARY KEY, importe_pendiente NUMERIC(18,2))'
    cur = pg_conn.cursor()
    for s in ALL_SCHEMAS:
        cur.execute(f'CREATE TABLE IF NOT EXISTS {s}.facturas_pendientes {fp_ddl}')
    pg_conn.commit()
    cur.close()

    full_table = f'{schema}.facturas_pendientes'
    cur = pg_conn.cursor()
    try:
        cur.execute(f"""
            INSERT INTO {full_table} (id_cierre_turno, importe_pendiente)
            VALUES (%s, %s)
            ON CONFLICT (id_cierre_turno)
            DO UPDATE SET importe_pendiente = EXCLUDED.importe_pendiente
        """, (id_cierre, total))
        pg_conn.commit()
    except Exception:
        pg_conn.rollback()
        raise
    finally:
        cur.close()

    return {full_table: 1}


# ---------------------------------------------------------------------------
# _handle_cuenta_corriente_query — reemplazo del SP 018 con query directo
# Mismo resultado que Listado_TotalesCuentaCorrienteEnTurno pero ~4000x más rápido.
# ---------------------------------------------------------------------------
_Q_CC_TOTALES = """
;WITH Doc AS (
    SELECT m.EsRemito, m.EsCuentaCorriente, m.Precio, m.Cantidad,
           m.ImputacionCierreTurno, F.IdCliente
    FROM dbo.Funcion_MovimientosEnTurno(?) m
        LEFT JOIN MovimientosFac F WITH (NOLOCK) ON m.IdMovimientoFac = F.IdMovimientoFac
),
Des AS (
    SELECT Estado, RTRIM(COALESCE(RazonSocial,'')) AS RZ, Cantidad, PrecioPublico
    FROM Despachos WITH (NOLOCK) WHERE IdCierreTurno = ?
)
SELECT
    TotalCuentaCorriente          = (SELECT COALESCE(SUM(Precio*Cantidad*ImputacionCierreTurno),0)
                                       FROM Doc WHERE EsCuentaCorriente=1 AND EsRemito=0),
    TotalRemitosCuentaCorriente   = (SELECT COALESCE(SUM(Precio*Cantidad*ImputacionCierreTurno),0)
                                       FROM Doc WHERE EsRemito=1 AND EsCuentaCorriente=1
                                       AND COALESCE(IdCliente,-2)<>1),
    TotalYPFEnRutaCuentaCorriente = (SELECT COALESCE(SUM(Cantidad*PrecioPublico),0)
                                       FROM Des WHERE Estado='Y' OR RZ='EstadoOriginal: [Y]'),
    TotalYPFEnRutaContado         = (SELECT COALESCE(SUM(Cantidad*PrecioPublico),0)
                                       FROM Des WHERE Estado='R' OR RZ='EstadoOriginal: [R]')
"""

_Q_CC_DETALLES = """
;WITH Des AS (
    SELECT Estado, RTRIM(COALESCE(RazonSocial,'')) AS RZ, IdArticulo, Cantidad, PrecioPublico
    FROM Despachos WITH (NOLOCK) WHERE IdCierreTurno = ?
)
SELECT IdCierreTurno = ?,
       Operacion     = 'Operaciones de cuenta corriente',
       D.IdArticulo, A.Codigo, A.Descripcion AS DescripcionArticulo,
       Cantidad = SUM(D.Cantidad), Importe = SUM(D.Cantidad * D.PrecioPublico)
FROM Des D JOIN Articulos A ON A.IdArticulo = D.IdArticulo
WHERE D.Estado = 'Y' OR D.RZ = 'EstadoOriginal: [Y]'
GROUP BY D.IdArticulo, A.Codigo, A.Descripcion
UNION ALL
SELECT ?, 'Operaciones de contado',
       D.IdArticulo, A.Codigo, A.Descripcion,
       SUM(D.Cantidad), SUM(D.Cantidad * D.PrecioPublico)
FROM Des D JOIN Articulos A ON A.IdArticulo = D.IdArticulo
WHERE D.Estado = 'R' OR D.RZ = 'EstadoOriginal: [R]'
GROUP BY D.IdArticulo, A.Codigo, A.Descripcion
ORDER BY Operacion, Codigo
"""


def _handle_cuenta_corriente_query(id_cierre, schema, sql_conn, pg_conn):
    totales = {}

    # RS1 — Totales (sin id_cierre_turno; sync_table lo inyecta via inject_id)
    cur = sql_conn.cursor()
    try:
        cur.execute(_Q_CC_TOTALES, (id_cierre, id_cierre))
        cols  = [c[0] for c in cur.description]
        types = [c[1] for c in cur.description]
        rows  = cur.fetchall()
    finally:
        cur.close()

    count = sync_table(pg_conn, schema, 'cierre_turno_cuentacorriente_totales',
                       cols, types, rows,
                       pk_cols=['id_cierre_turno'], inject_id=id_cierre)
    if count > 0:
        totales['cierre_turno_cuentacorriente_totales'] = count

    # RS2 — Detalle por artículo (IdCierreTurno ya viene en el SELECT)
    cur = sql_conn.cursor()
    try:
        cur.execute(_Q_CC_DETALLES, (id_cierre, id_cierre, id_cierre))
        cols  = [c[0] for c in cur.description]
        types = [c[1] for c in cur.description]
        rows  = cur.fetchall()
    finally:
        cur.close()

    count = sync_table(pg_conn, schema, 'cierre_turno_cuentacorriente_detalles',
                       cols, types, rows,
                       pk_cols=['id_cierre_turno', 'operacion', 'id_articulo'],
                       inject_id=id_cierre)
    if count > 0:
        totales['cierre_turno_cuentacorriente_detalles'] = count

    return totales


# ---------------------------------------------------------------------------
# Sync de tablas maestras: SQL Server → PostgreSQL (Fase 0 del ETL)
# Orden de dependencia: estaciones → cajas/empleados; familias → grupos → articulos
# ---------------------------------------------------------------------------
_MASTER_TABLES = [
    # (sql_table,          pg_table,            pk_col,                preserve_case)
    ('Estaciones',         'estaciones',         'id_estacion',         False),
    ('FamiliasArticulos',  'familias_articulos', 'id_familia_articulo', False),
    ('GruposArticulos',    'grupos_articulos',   'id_grupo_articulo',   False),
    ('Articulos',          'articulos',          'id_articulo',         False),
    ('Cajas',              'cajas',              'id_caja',             False),
    ('Empleados',          'empleados',          'id_empleado',         False),
    ('Bancos',             'bancos',             'id_banco',            False),
    ('Clientes',           'Clientes',           'IdCliente',           True),
    ('TiposMovimiento',    'tipos_movimientos',  'id_tipo_movimiento',  False),
    ('TarjetasCredito',    'tarjetas_credito',   'id_tarjeta',         False),
]


def _sync_master_table(sql_conn, pg_conn, schema, sql_table, pg_table, pk_col, preserve_case=False):
    """Sincroniza una tabla maestra SQL Server → PG usando upsert completo.

    Cajas se filtra por IdEstacion, para restringir la extracción de
    cierres a la estación configurada en .env. Estaciones se sincroniza
    completa: es tabla de referencia y otras maestras (Empleados, etc.)
    tienen FKs a filas de Estaciones que no son la de esta estación."""
    cur_sql = sql_conn.cursor()
    if sql_table == 'Cajas':
        cur_sql.execute(f'SELECT * FROM {sql_table} WHERE IdEstacion = ?', (ID_ESTACION,))
    else:
        cur_sql.execute(f'SELECT * FROM {sql_table}')
    sql_cols_raw = [col[0] for col in cur_sql.description]
    sql_cols     = sql_cols_raw if preserve_case else [to_snake(c) for c in sql_cols_raw]
    rows_raw     = cur_sql.fetchall()
    cur_sql.close()

    if not rows_raw:
        return 0

    # Auto-filtro: solo insertar columnas que existan en la tabla PG
    pg_existing = _get_pg_cols(pg_conn, schema, pg_table)
    if pg_existing:
        keep     = [i for i, c in enumerate(sql_cols) if c in pg_existing]
        sql_cols = [sql_cols[i] for i in keep]
        rows     = [[list(r)[i] for i in keep] for r in rows_raw]
    else:
        rows = [list(r) for r in rows_raw]

    if not rows:
        return 0

    def q(name):
        return f'"{name}"' if preserve_case else name

    full_table = f'{schema}."{pg_table}"' if preserve_case else f'{schema}.{pg_table}'

    # DDL: crear tabla si no existe (necesario para tablas nuevas como tipos_movimiento)
    if not pg_existing:
        col_defs = []
        for col, r in zip(sql_cols, rows_raw[0]):
            pg_type = get_pg_type(type(r[sql_cols_raw.index(col)] if col in sql_cols_raw else str))
            inline_pk = ' PRIMARY KEY' if col == pk_col else ''
            col_defs.append(f'{q(col)} {pg_type}{inline_pk}')
        ddl = f'CREATE TABLE IF NOT EXISTS {full_table} ({", ".join(col_defs)})'
        cur_ddl = pg_conn.cursor()
        try:
            cur_ddl.execute(ddl)
            pg_conn.commit()
            # Actualizar cache
            _pg_cols_cache[f'{schema}.{pg_table}'] = set(sql_cols)
        except Exception:
            pg_conn.rollback()
        finally:
            cur_ddl.close()

    non_pk     = [c for c in sql_cols if c != pk_col]
    cols_str   = ', '.join(q(c) for c in sql_cols)
    if non_pk:
        set_clause = ', '.join(f'{q(c)} = EXCLUDED.{q(c)}' for c in non_pk)
        upsert_q   = (f'INSERT INTO {full_table} ({cols_str}) VALUES %s '
                      f'ON CONFLICT ({q(pk_col)}) DO UPDATE SET {set_clause}')
    else:
        upsert_q = (f'INSERT INTO {full_table} ({cols_str}) VALUES %s '
                    f'ON CONFLICT ({q(pk_col)}) DO NOTHING')

    cur_pg = pg_conn.cursor()
    try:
        extras.execute_values(cur_pg, upsert_q, [tuple(r) for r in rows], page_size=500)
        pg_conn.commit()
    except Exception:
        pg_conn.rollback()
        raise
    finally:
        cur_pg.close()

    return len(rows)


def sync_all_masters(sql_conn, pg_conn, schema, logger):
    """Sincroniza todas las tablas maestras en orden de dependencia."""
    logger.log(f'  [MASTERS] Sincronizando tablas maestras ({schema})...')
    for sql_table, pg_table, pk_col, preserve_case in _MASTER_TABLES:
        try:
            n = _sync_master_table(sql_conn, pg_conn, schema,
                                   sql_table, pg_table, pk_col, preserve_case)
            logger.log(f'  [MASTERS] {pg_table}: {n} filas')
        except Exception as e:
            logger.log(f'  [MASTERS] WARN {pg_table}: {e}')
    # Insertar registros dummy en grupos_articulos para percepciones (IDs sintéticos)
    _ensure_grupos_sinteticos(pg_conn, schema, logger)
    # Invalidar caches de lookup para usar datos frescos del sync
    _cajas_lkp_cache.pop(schema, None)
    _bancos_lkp_cache.pop(schema, None)


def _ensure_grupos_sinteticos(pg_conn, schema, logger):
    """Asegura la existencia de la tabla grupos_articulos y de los IDs
    sintéticos (-1, -2) que los SPs de SQL Server generan hardcodeados
    para percepciones. Seguro para base de datos limpia."""
    dummy_grupos = [
        (-1, 'PERCEPCIONES (CONTADO)'),
        (-2, 'PERCEPCIONES (CTA.CTE.)'),
    ]
    cur = pg_conn.cursor()
    try:
        # Crear la tabla si no existe (base limpia)
        cur.execute(f'''
            CREATE TABLE IF NOT EXISTS {schema}.grupos_articulos (
                id_grupo_articulo BIGINT PRIMARY KEY,
                descripcion TEXT
            )
        ''')
        pg_conn.commit()

        # Verificar qué registros faltan
        cur.execute(
            f'SELECT id_grupo_articulo FROM {schema}.grupos_articulos '
            f'WHERE id_grupo_articulo IN (-1, -2)'
        )
        existentes = {r[0] for r in cur.fetchall()}

        insertados = 0
        for gid, gdesc in dummy_grupos:
            if gid not in existentes:
                cur.execute(
                    f'INSERT INTO {schema}.grupos_articulos (id_grupo_articulo, descripcion) '
                    f'VALUES (%s, %s)',
                    (gid, gdesc)
                )
                insertados += 1

        if insertados > 0:
            pg_conn.commit()
            logger.log(f'  [MASTERS] grupos_articulos: {insertados} registros sintéticos insertados (-1, -2)')
        else:
            logger.log(f'  [MASTERS] grupos_articulos: registros sintéticos -1, -2 ya existentes')
    except Exception:
        pg_conn.rollback()
        raise
    finally:
        cur.close()


# ---------------------------------------------------------------------------
# sync_table — batch upsert con auto-filtro de columnas PG y derivación de FKs
# ---------------------------------------------------------------------------
def sync_table(pg_conn, schema, table_name, col_names, col_types, rows,
               pk_cols=None, inject_id=None,
               derive_caja_from=None, derive_banco_from=None):
    """
    Upsert normalizado de filas en {schema}.{table_name}.

    Auto-filtro: solo inserta columnas que existen en la tabla PG destino.
    Las columnas de texto eliminadas (descripcion_articulo, empleado, etc.)
    se descartan silenciosamente al no existir en PG.

    derive_caja_from : snake_case de la columna fuente para derivar id_caja.
        'descripcion'      → extrae id de '(123)- NOMBRE' con regex.
        'descripcion_caja' → lookup por nombre en tabla cajas.
    derive_banco_from : snake_case de la columna fuente para derivar id_banco.
        'nombre_banco'     → lookup por nombre en tabla bancos.
    """
    columns_final = list(col_names)
    types_final   = list(col_types)

    # Convertir a snake_case
    pg_cols = [to_snake(c) for c in columns_final]

    # Filas como listas mutables
    rows = [list(r) for r in rows]

    # Columnas reales de la tabla PG (cacheado)
    pg_existing = _get_pg_cols(pg_conn, schema, table_name)

    # ── DERIVACIONES (antes del filtro, para acceder a cols que se descartarán) ──

    if pg_existing and 'id_caja' in pg_existing and 'id_caja' not in pg_cols \
            and derive_caja_from and derive_caja_from in pg_cols:
        src_idx = pg_cols.index(derive_caja_from)
        if derive_caja_from == 'descripcion':
            # Patrón: "(123)- NOMBRE CAJA"  →  id_caja = 123
            for row in rows:
                val = str(row[src_idx] or '').strip()
                m   = re.match(r'^\((\d+)\s*\)', val)
                row.append(int(m.group(1)) if m else None)
        else:
            # Lookup por descripción literal en tabla cajas
            lkp = _get_cajas_lkp(pg_conn, schema)
            for row in rows:
                key = str(row[src_idx] or '').strip().upper()
                row.append(lkp.get(key))
        pg_cols.append('id_caja')
        columns_final.append('id_caja')
        types_final.append(int)

    if pg_existing and 'id_banco' in pg_existing and 'id_banco' not in pg_cols \
            and derive_banco_from and derive_banco_from in pg_cols:
        src_idx = pg_cols.index(derive_banco_from)
        lkp = _get_bancos_lkp(pg_conn, schema)
        for row in rows:
            key = str(row[src_idx] or '').strip().upper()
            row.append(lkp.get(key))
        pg_cols.append('id_banco')
        columns_final.append('id_banco')
        types_final.append(int)

    # ── AUTO-FILTRO: mantener solo columnas que existan en la tabla PG ──

    if pg_existing:
        keep_idx      = [i for i, sc in enumerate(pg_cols) if sc in pg_existing]
        columns_final = [columns_final[i] for i in keep_idx]
        types_final   = [types_final[i] for i in keep_idx]
        pg_cols       = [pg_cols[i] for i in keep_idx]
        rows          = [[r[i] for i in keep_idx] for r in rows]

    # ── INJECT id_cierre_turno (cuando el SP no lo devuelve) ──

    if inject_id is not None and 'id_cierre_turno' not in pg_cols:
        pg_cols.append('id_cierre_turno')
        columns_final.append('id_cierre_turno')
        types_final.append(int)
        rows = [r + [inject_id] for r in rows]

    # ── CAST id_banco TEXT → INT (SP 014 devuelve IdBanco como string) ──

    if 'id_banco' in pg_cols:
        idx = pg_cols.index('id_banco')
        for row in rows:
            v = row[idx]
            if v is not None and not isinstance(v, int):
                try:
                    row[idx] = int(str(v).strip()) if str(v).strip() else None
                except (ValueError, TypeError):
                    row[idx] = None

    # ── DDL: crear tabla si no existe (necesario para tablas _extra nuevas) ──

    col_defs = []
    for col, tc in zip(pg_cols, types_final):
        inline_pk = (' PRIMARY KEY'
                     if pk_cols and len(pk_cols) == 1 and col == pk_cols[0]
                     else '')
        col_defs.append(f'{col} {get_pg_type(tc)}{inline_pk}')

    pk_constraint = (f', PRIMARY KEY ({", ".join(pk_cols)})'
                     if pk_cols and len(pk_cols) > 1 else '')
    ddl_body = f'({", ".join(col_defs)}{pk_constraint})'

    cur = pg_conn.cursor()
    for s in ALL_SCHEMAS:
        cur.execute(f'CREATE TABLE IF NOT EXISTS {s}.{table_name} {ddl_body}')
    pg_conn.commit()
    cur.close()

    if not rows:
        return 0

    full_table = f'{schema}.{table_name}'

    # ── Deduplicar por PK ──

    if pk_cols:
        seen = {}
        for row in rows:
            try:
                key = tuple(row[pg_cols.index(pk)] for pk in pk_cols if pk in pg_cols)
            except (ValueError, IndexError):
                key = None
            seen[key if key is not None else id(row)] = row
        rows = list(seen.values())

    # ── Upsert ──

    cols_str = ', '.join(pg_cols)
    if pk_cols:
        non_pk = [c for c in pg_cols if c not in pk_cols]
        if non_pk:
            set_clause = ', '.join(f'{c} = EXCLUDED.{c}' for c in non_pk)
            upsert_q   = (f'INSERT INTO {full_table} ({cols_str}) VALUES %s '
                          f'ON CONFLICT ({", ".join(pk_cols)}) DO UPDATE SET {set_clause}')
        else:
            upsert_q = (f'INSERT INTO {full_table} ({cols_str}) VALUES %s '
                        f'ON CONFLICT ({", ".join(pk_cols)}) DO NOTHING')
    else:
        upsert_q = f'INSERT INTO {full_table} ({cols_str}) VALUES %s'

    cur = pg_conn.cursor()
    try:
        extras.execute_values(cur, upsert_q, [tuple(r) for r in rows], page_size=500)
        pg_conn.commit()
    except Exception:
        pg_conn.rollback()
        raise
    finally:
        cur.close()

    return len(rows)

# ---------------------------------------------------------------------------
# Definición de los 20 módulos ETL (mismos SPs y tablas que v4)
# ---------------------------------------------------------------------------
MODULES = [
    # 001
    {
        'name': '001_EncabezadoCierreTurno',
        'sp': "SET NOCOUNT ON; EXECUTE EncabezadoCierreTurno @IdCierreTurno = ?, @Debug = '0'",
        'sets': {
            1: {'table': 'cierre_turno_encabezado',
                'pk': ['id_cierre_turno'], 'inject': False,
                'derive_caja_from': 'descripcion'},
            2: {'table': 'cierre_turno_documentos',
                'pk': ['id_cierre_turno', 'id_tipo_movimiento', 'punto_venta'], 'inject': False},
            3: {'table': 'cierre_turno_empleados_fiscal',
                'pk': ['id_cierre_turno', 'empleado'], 'inject': False},
        },
        'extra': 'cierre_turno_encabezado_extra',
    },
    # 002
    {
        'name': '002_Listado_EstadoAforadores',
        'sp': "SET NOCOUNT ON; EXECUTE Listado_EstadoAforadores @IdCierreTurno = ?, @CaldenON = '0', @Debug = '0'",
        'sets': {
            1: {'table': 'cierre_turno_aforadores_detalle',
                'pk': ['id_cierre_detalle_surtidores'], 'inject': False},
            2: {'table': 'cierre_turno_aforadores_resumen',
                'pk': ['id_cierre_turno', 'id_articulo', 'periodo'], 'inject': False},
            3: {'table': 'cierre_turno_aforadores_diferencias',
                'pk': ['id_cierre_turno', 'id_manguera'], 'inject': False},
        },
        'extra': 'cierre_turno_aforadores_extra',
    },
    # 003
    {
        'name': '003_Listado_EstadoTanques',
        'sp': 'SET NOCOUNT ON; EXECUTE Listado_EstadoTanques @IdCierreTurno = ?',
        'sets': {
            1: {'table': 'cierre_turno_tanques_detalle',
                'pk': ['id_cierre_detalle_tanques'], 'inject': False,
                'derive_caja_from': 'descripcion_caja'},
            2: {'table': 'cierre_turno_tanques_resumen',
                'pk': ['id_cierre_turno', 'id_articulo'], 'inject': False},
        },
        'extra': 'cierre_turno_tanques_extra',
    },
    # 004
    {
        'name': '004_Listado_CierresDocumentos_F1',
        'sp': ("SET NOCOUNT ON; EXECUTE Listado_CierresDocumentos "
               "@IdCierreTurno = ?, @Formato = '1', @Agrupar = '0', "
               "@SinDetalle = '0', @EsCierreEmpleado = '0'"),
        'sets': {
            1: {'table': 'cierre_turno_listado_documentos_cabecera',
                'pk': ['id_movimientos_detalle_fac'], 'inject': False},
            2: {'table': 'cierre_turno_listado_documentos_detalle',
                'pk': ['id_movimientos_detalle_fac', 'id_movimiento_fac', 'orden'],
                'inject': False},
        },
        'extra': 'cierre_turno_listado_docs_f1_extra',
    },
    # 005
    {
        'name': '005_Listado_CierresDocumentos_F2',
        'sp': ("SET NOCOUNT ON; EXECUTE Listado_CierresDocumentos "
               "@IdCierreTurno = ?, @Formato = '2', @Agrupar = '0', "
               "@SinDetalle = '0', @EsCierreEmpleado = '0'"),
        'sets': {
            1: {'table': 'cierre_turno_listado_documentos_cabecera',
                'pk': ['id_movimientos_detalle_fac'], 'inject': False},
            2: {'table': 'cierre_turno_listado_documentos_detalle',
                'pk': ['id_movimientos_detalle_fac', 'id_movimiento_fac', 'orden'],
                'inject': False},
        },
        'extra': 'cierre_turno_listado_docs_f2_extra',
    },
    # 006
    {
        'name': '006_Listado_CierresDocumentos_F3',
        'sp': ("SET NOCOUNT ON; EXECUTE Listado_CierresDocumentos "
               "@IdCierreTurno = ?, @Formato = '3', @Agrupar = '0', "
               "@SinDetalle = '0', @EsCierreEmpleado = '0', @Debug = '0'"),
        'sets': {
            1: {'table': 'cierre_turno_listado_documentos',
                'pk': ['id_movimientos_detalle_fac'], 'inject': False},
            3: {'table': 'cierre_turno_listado_documentos_detalles',
                'pk': ['id_movimientos_detalle_fac', 'id_movimiento_fac', 'orden'],
                'inject': False},
        },
        'extra': 'cierre_turno_listado_documentos_extra',
    },
    # 007
    {
        'name': '007_Listado_Despachos_T4',
        'sp': ("SET NOCOUNT ON; EXECUTE Listado_Despachos "
               "@IdCierreTurno = ?, @TipoListado = '4', @DesdeLitros = 0, "
               "@HastaLitros = 0, @IdArticulo = 0, @IdEmpleado = 0, @MostrarEmpleado = 0"),
        'sets': {
            1: {'table': 'cierre_turno_despachos_detalle',
                'pk': ['id_despacho'], 'inject': True},
            2: {'table': 'cierre_turno_despachos_resumen',
                'pk': ['id_cierre_turno', 'codigo', 'orden'], 'inject': True},
        },
        'extra': 'cierre_turno_despachos_extra',
    },
    # 008
    {
        'name': '008_Listado_Despachos_T5',
        'sp': ("SET NOCOUNT ON; EXECUTE Listado_Despachos "
               "@FechaDesde = NULL, @FechaHasta = NULL, @DesdeLitros = '0', "
               "@HastaLitros = '0', @ListaSurtidores = NULL, @ListaMangueras = NULL, "
               "@IdArticulo = '0', @TipoListado = '5', @IdCierreTurno = ?, "
               "@ListaEstaciones = NULL, @IdEmpleado = '0', "
               "@ListaCierresEmpleado = NULL, @MostrarEmpleado = '0', @Debug = '0'"),
        'sets': {
            1: {'table': 'cierre_turno_listado_despachos',
                'pk': ['id_despacho'], 'inject': True},
            2: {'table': 'cierre_turno_listado_despachos_resumen',
                'pk': None, 'inject': True},
        },
        'extra': 'cierre_turno_listado_despachos_extra',
    },
    # 009
    {
        'name': '009_Listado_RecibosEnTurno',
        'sp': 'SET NOCOUNT ON; EXECUTE Listado_RecibosEnTurno @IdCierreTurno = ?',
        'sets': {
            1: {'table': 'cierre_turno_listado_recibos',
                'pk': ['comprobante', 'razon_social'], 'inject': False},
        },
        'extra': 'cierre_turno_listado_recibos_extra',
    },
    # 010
    {
        'name': '010_Listado_DocumentosExentos',
        'sp': ("SET NOCOUNT ON; EXECUTE Listado_DocumentosEnTurnoExentosPercepcion "
               "@IdCierreTurno = ?, @Debug = '0'"),
        'sets': {
            1: {'table': 'cierre_turno_listado_documentos_exentos',
                'pk': ['id_movimiento_fac'], 'inject': False},
        },
        'extra': 'cierre_turno_listado_exentos_extra',
    },
    # 011
    {
        'name': '011_Listado_CierresArticulos_F1',
        'sp': ("SET NOCOUNT ON; EXECUTE Listado_CierresArticulos "
               "@IdCierreTurno = ?, @Formato = '1', @Tipos = '0', "
               "@Orden = '1', @EsCierreEmpleado = '0', @Debug = '0'"),
        'sets': {
            1: {'table': 'cierre_turno_listado_articulos',
                'pk': ['id_cierre_turno', 'codigo', 'es_remito'], 'inject': False},
        },
        'extra': 'cierre_turno_listado_articulos_extra',
    },
    # 012
    {
        'name': '012_Listado_CierresArticulos_F2',
        'sp': ("SET NOCOUNT ON; EXECUTE Listado_CierresArticulos "
               "@IdCierreTurno = ?, @Formato = '2', @Tipos = '0', "
               "@Orden = '0', @EsCierreEmpleado = '0', @Debug = '0'"),
        'sets': {
            1: {'table': 'cierre_turno_listado_articulos_grupos',
                'pk': ['id_cierre_turno', 'id_grupo_articulo', 'es_remito'],
                'inject': False},
        },
        'extra': 'cierre_turno_listado_articulos_grupos_extra',
    },
    # 013
    {
        'name': '013_Listado_TransferenciasEnTurno',
        'sp': ("SET NOCOUNT ON; EXECUTE Listado_TransferenciasEnTurno "
               "@IdCierreTurno = ?, @EsCierreEmpleado = '0'"),
        'sets': {
            1: {'table': 'cierre_turno_transferencias_caja',
                'pk': ['id_otro_movimiento_caja_tesoreria'], 'inject': False},
            2: {'table': 'cierre_turno_transferencias_cheques',
                'pk': ['comprobante', 'numero'], 'inject': False,
                'derive_banco_from': 'nombre_banco'},
            3: {'table': 'cierre_turno_transferencias_resumen',
                'pk': ['id_cierre_turno', 'tipo_movimiento', 'orden'], 'inject': False},
        },
        'extra': 'cierre_turno_transferencias_extra',
    },
    # 014
    {
        'name': '014_Listado_ChequesEnTurno',
        'sp': ("SET NOCOUNT ON; EXECUTE Listado_ChequesTarjetasPagosValesEnTurno "
               "@IdCierreTurno = ?, @Buscar = '1', @EsCierreEmpleado = '0'"),
        'sets': {
            1: {'table': 'cierre_turno_listado_cheques',
                'pk': ['id_cheque_tercero'], 'inject': False},
        },
        'extra': 'cierre_turno_listado_cheques_extra',
    },
    # 015
    {
        'name': '015_Listado_TarjetasEnTurno',
        'sp': ("SET NOCOUNT ON; EXECUTE Listado_ChequesTarjetasPagosValesEnTurno "
               "@IdCierreTurno = ?, @Buscar = '2', @EsCierreEmpleado = '0'"),
        'sets': {
            1: {'table': 'cierre_turno_listado_tarjetas_cupones',
                'pk': ['id_cupon_tarjeta_credito'], 'inject': False},
            2: {'table': 'cierre_turno_listado_tarjetas_lotes',
                'pk': ['id_lote_tarjetas_credito'], 'inject': False},
            3: {'table': 'cierre_turno_listado_tarjetas_cupones',
                'pk': ['id_cupon_tarjeta_credito'], 'inject': False},
        },
        'extra': 'cierre_turno_listado_tarjetas_extra',
    },
    # 016 — inject id_cierre_turno
    {
        'name': '016_Listado_ChequesResumen',
        'sp': ("SET NOCOUNT ON; EXECUTE Listado_ChequesTarjetasPagosValesEnTurno "
               "@IdCierreTurno = ?, @Buscar = '5', @EsCierreEmpleado = '0'"),
        'sets': {
            1: {'table': 'cierre_turno_cheques_tarjetas_resumen',
                'pk': ['id_cierre_turno', 'descripcion'], 'inject': True},
        },
        'extra': 'cierre_turno_cheques_resumen_extra',
    },
    # 017 — sets 2 y 3 van a la misma tabla
    {
        'name': '017_Listado_ValoresEnTurno',
        'sp': ("SET NOCOUNT ON; EXECUTE Listado_ValoresEnTurno "
               "@IdCierreTurno = ?, @EsCierreEmpleado = '0'"),
        'sets': {
            1: {'table': 'cierre_turno_valores_resumen',
                'pk': ['id_cierre_turno'], 'inject': False},
            2: {'table': 'cierre_turno_valores_efectivo_detalle',
                'pk': ['id_cierre_detalle_efectivo'], 'inject': False},
            3: {'table': 'cierre_turno_valores_efectivo_detalle',
                'pk': ['id_cierre_detalle_efectivo'], 'inject': False},
        },
        'extra': 'cierre_turno_valores_extra',
    },
    # 018 — reemplazado por query directo (ver _handle_cuenta_corriente_query)
    {
        'name': '018_Listado_CuentaCorriente',
        'cuenta_corriente_query': True,
        'sets': {},
        'extra': 'cierre_turno_cuentacorriente_extra',
    },
    # 019 — inject id_cierre_turno
    {
        'name': '019_Listado_ResumenTurno',
        'sp': ("SET NOCOUNT ON; EXECUTE Listado_ResumenTurno "
               "@IdCierreTurno = ?, @EsCierreEmpleado = '0', "
               "@RecuperarSoloTablaDiferencia = '0', @Debug = '0'"),
        'sets': {
            1: {'table': 'cierre_turno_resumen_grupos',
                'pk': ['id_cierre_turno', 'id_grupo_articulo'], 'inject': True},
            2: {'table': 'cierre_turno_resumen_totales',
                'pk': ['id_cierre_turno'], 'inject': True},
            3: {'table': 'cierre_turno_resumen_diferencias',
                'pk': ['id_cierre_turno'], 'inject': True},
            4: {'table': 'cierre_turno_resumen_contado',
                'pk': ['id_cierre_turno'], 'inject': True},
        },
        'extra': 'cierre_turno_resumen_extra',
    },
    # 021
    {
        'name': '021_FacturaPendiente',
        'sp': "SET NOCOUNT ON; EXECUTE Listado_EstadoAforadores @IdCierreTurno = ?, @CaldenON = '1', @Debug = '1'",
        'sets': {},
        'extra': 'cierre_turno_aforadores_factura_extra',
        'factura_pendiente': True,
    },
    # 020 — antepone id_cierre, toma los primeros 10 campos del SP
    {
        'name': '020_DocumentosSinImputacion',
        'sp': ("SET NOCOUNT ON; EXECUTE DocumentosSinImputacionACierrePorEmpleado "
               "@IdCierreTurno = ?"),
        'sets': {
            1: {
                'table': 'cierre_turno_documentos_sin_imputacion',
                'pk': ['id_cierre_turno', 'id_movimiento_fac'],
                'inject': False,
                'col_override': ['id_cierre_turno', 'id_movimiento_fac', 'fecha',
                                 'comprobante', 'razon_social', 'numero_documento',
                                 'total', 'impuestos', 'neto', 'cta_cte', 'orden'],
                'prepend_id': True,
                'row_slice': 10,
            },
        },
        'extra': 'cierre_turno_sin_imputacion_extra',
    },
]

# ---------------------------------------------------------------------------
# Ejecutar un módulo para un IdCierreTurno dado
# ---------------------------------------------------------------------------
def run_module(mod, id_cierre, schema, sql_conn, pg_conn):
    if mod.get('cuenta_corriente_query'):
        return _handle_cuenta_corriente_query(id_cierre, schema, sql_conn, pg_conn)

    cursor = sql_conn.cursor()
    cursor.execute(mod['sp'], (id_cierre,))

    if mod.get('factura_pendiente'):
        result = _handle_factura_pendiente(cursor, id_cierre, schema, pg_conn)
        cursor.close()
        return result

    totales = {}
    set_num = 1

    while True:
        if cursor.description:
            columns = [col[0] for col in cursor.description]
            types   = [col[1] for col in cursor.description]
            rows    = cursor.fetchall()

            cfg = mod['sets'].get(set_num)
            if cfg:
                if cfg.get('col_override'):
                    slice_n = cfg.get('row_slice')
                    if cfg.get('prepend_id'):
                        rows  = [[id_cierre] + list(r[:slice_n]) for r in rows]
                        types = [int] + types[:slice_n]
                    elif slice_n:
                        rows  = [list(r[:slice_n]) for r in rows]
                        types = types[:slice_n]
                    columns = cfg['col_override']

                inject = id_cierre if cfg.get('inject') else None
                count  = sync_table(pg_conn, schema, cfg['table'], columns, types, rows,
                                    pk_cols=cfg['pk'], inject_id=inject,
                                    derive_caja_from=cfg.get('derive_caja_from'),
                                    derive_banco_from=cfg.get('derive_banco_from'))
                if count > 0:
                    totales[cfg['table']] = totales.get(cfg['table'], 0) + count
            elif rows:
                extra = f"{mod['extra']}_{set_num}"
                count = sync_table(pg_conn, schema, extra, columns, types, rows)
                if count > 0:
                    totales[extra] = count

        if not cursor.nextset():
            break
        set_num += 1

    cursor.close()
    return totales

# ---------------------------------------------------------------------------
# Fallback de id_caja: consulta CierresTurno en SQL Server si quedó NULL en PG
# ---------------------------------------------------------------------------
def _fix_id_caja(id_cierre, schema, sql_conn, pg_conn, logger):
    cur_pg = pg_conn.cursor()
    try:
        cur_pg.execute(
            f'SELECT id_caja FROM {schema}.cierre_turno_encabezado WHERE id_cierre_turno = %s',
            (id_cierre,)
        )
        row = cur_pg.fetchone()
        if not row or row[0] is not None:
            return
        cur_sql = sql_conn.cursor()
        cur_sql.execute('SELECT IdCaja FROM CierresTurno WHERE IdCierreTurno = ?', (id_cierre,))
        sql_row = cur_sql.fetchone()
        cur_sql.close()
        if not sql_row or sql_row[0] is None:
            logger.log(f'  [FIX id_caja] {id_cierre}: no encontrado en SQL Server')
            return
        cur_pg.execute(
            f'UPDATE {schema}.cierre_turno_encabezado SET id_caja = %s WHERE id_cierre_turno = %s',
            (sql_row[0], id_cierre)
        )
        pg_conn.commit()
        logger.log(f'  [FIX id_caja] {id_cierre}: corregido -> id_caja = {sql_row[0]}')
    except Exception as e:
        pg_conn.rollback()
        logger.log(f'  [FIX id_caja] {id_cierre}: ERROR — {e}')
    finally:
        cur_pg.close()

# ---------------------------------------------------------------------------
# Procesar un ID: lanza los 20 módulos en paralelo usando el pool compartido
# ---------------------------------------------------------------------------
def process_id(id_cierre, schema, pool, logger):
    def run_with_pool(mod):
        sql_conn, pg_conn = pool.acquire()
        try:
            return mod['name'], True, run_module(mod, id_cierre, schema, sql_conn, pg_conn)
        except Exception as e:
            try:
                pg_conn.rollback()
            except Exception:
                pass
            return mod['name'], False, str(e)
        finally:
            pool.release(sql_conn, pg_conn)

    mod_ok  = 0
    mod_err = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(run_with_pool, mod): mod for mod in MODULES}
        for future in as_completed(futures):
            name, ok, result = future.result()
            if ok:
                total_rows = sum(result.values())
                detalle    = ', '.join(f'{t}={c}' for t, c in result.items()) if result else 'sin datos'
                logger.log(f'  [{name}] OK  — {total_rows} filas ({detalle})')
                mod_ok += 1
            else:
                logger.log(f'  [{name}] ERROR: {result}')
                mod_err += 1

    # Fallback: si id_caja quedó NULL en el encabezado, lo toma directo de CierresTurno
    sql_conn, pg_conn = pool.acquire()
    try:
        _fix_id_caja(id_cierre, schema, sql_conn, pg_conn, logger)
        _set_fecha_importacion(id_cierre, schema, pg_conn, logger)
    finally:
        pool.release(sql_conn, pg_conn)

    return mod_ok, mod_err

# ---------------------------------------------------------------------------
# Obtener IDs en el rango de fechas desde SQL Server
# ---------------------------------------------------------------------------
def get_cierres(fecha_desde, fecha_hasta, sql_db, id_estacion):
    f_desde = fecha_desde + ' 00:00:00' if len(fecha_desde) <= 10 else fecha_desde
    f_hasta = fecha_hasta + ' 23:59:59' if len(fecha_hasta) <= 10 else fecha_hasta
    conn = _connect_sql(sql_db)
    cur  = conn.cursor()
    cur.execute(
        'SELECT ct.IdCierreTurno, ct.Fecha, ct.Numero '
        'FROM CierresTurno ct JOIN Cajas c ON c.IdCaja = ct.IdCaja '
        'WHERE c.IdEstacion = ? AND ct.Fecha >= ? AND ct.Fecha <= ? '
        'ORDER BY ct.Fecha ASC',
        (id_estacion, f_desde, f_hasta)
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

# ---------------------------------------------------------------------------
# fecha_importacion en cierre_turno_encabezado: no viene del SP, se agrega
# aparte. Registra cuando NOSOTROS importamos cada cierre (no el LastUpdated
# de origen), para poder detectar modificaciones comparando contra el
# LastUpdated actual de SQL Server sin depender de ningun watermark global.
# ---------------------------------------------------------------------------
def _ensure_fecha_importacion_column(pg_conn, schema):
    cur = pg_conn.cursor()
    cur.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema = %s AND table_name = 'cierre_turno_encabezado'",
        (schema,)
    )
    if cur.fetchone():
        cur.execute(
            f'ALTER TABLE {schema}.cierre_turno_encabezado '
            f'ADD COLUMN IF NOT EXISTS fecha_importacion TIMESTAMP'
        )
        pg_conn.commit()
    cur.close()

def _set_fecha_importacion(id_cierre, schema, pg_conn, logger):
    cur_pg = pg_conn.cursor()
    try:
        cur_pg.execute(
            f'UPDATE {schema}.cierre_turno_encabezado '
            f'SET fecha_importacion = NOW() WHERE id_cierre_turno = %s',
            (id_cierre,)
        )
        pg_conn.commit()
    except Exception as e:
        pg_conn.rollback()
        logger.log(f'  [FIX fecha_importacion] {id_cierre}: ERROR — {e}')
    finally:
        cur_pg.close()

# ---------------------------------------------------------------------------
# Modo incremental: nuevos y modificados se detectan con criterios distintos.
#
# Nuevos: IdCierreTurno mayor al maximo ya importado en PG. Los IDs son
# secuenciales (identity), asi que cualquier cierre creado despues de la
# ultima corrida tiene un ID mas alto que todo lo que ya tenemos — no hace
# falta ventana de fecha ni traer el historico completo.
#
# Modificados: de los que YA estan en PG, se acota a los ultimos
# VENTANA_DIAS_INCREMENTAL dias (por Fecha del cierre) y se compara el
# LastUpdated actual en SQL Server contra la fecha_importacion registrada en
# PG. Como este proceso corre varias veces al dia, es muy improbable que un
# cierre pase mas de esa ventana sin recibir su version final — un backfill
# historico real se hace aparte con "etl.py <desde> <hasta>".
# ---------------------------------------------------------------------------
VENTANA_DIAS_INCREMENTAL = 7

def get_cierres_incremental(sql_db, id_estacion, pg_conn, schema):
    pg_estado = {}
    pg_cols = _get_pg_cols(pg_conn, schema, 'cierre_turno_encabezado')
    if pg_cols:
        cur_pg = pg_conn.cursor()
        cur_pg.execute(
            f'SELECT id_cierre_turno, fecha_importacion '
            f'FROM {schema}.cierre_turno_encabezado'
        )
        pg_estado = {r[0]: r[1] for r in cur_pg.fetchall()}
        cur_pg.close()

    max_id = max(pg_estado) if pg_estado else None

    conn = _connect_sql(sql_db)
    cur  = conn.cursor()

    # ── Nuevos: por ID, sin importar la fecha ──
    if max_id is not None:
        cur.execute(
            'SELECT ct.IdCierreTurno, ct.Fecha, ct.Numero '
            'FROM CierresTurno ct JOIN Cajas c ON c.IdCaja = ct.IdCaja '
            'WHERE c.IdEstacion = ? AND ct.IdCierreTurno > ? '
            'ORDER BY ct.Fecha ASC',
            (id_estacion, max_id)
        )
    else:
        cur.execute(
            'SELECT ct.IdCierreTurno, ct.Fecha, ct.Numero '
            'FROM CierresTurno ct JOIN Cajas c ON c.IdCaja = ct.IdCaja '
            'WHERE c.IdEstacion = ? '
            'ORDER BY ct.Fecha ASC',
            (id_estacion,)
        )
    faltantes = cur.fetchall()

    # ── Modificados: ventana de dias + fecha_importacion ──
    modificados = []
    if pg_estado:
        corte = datetime.now() - _dt.timedelta(days=VENTANA_DIAS_INCREMENTAL)
        cur.execute(
            'SELECT ct.IdCierreTurno, ct.Fecha, ct.Numero, ct.LastUpdated '
            'FROM CierresTurno ct JOIN Cajas c ON c.IdCaja = ct.IdCaja '
            'WHERE c.IdEstacion = ? AND ct.Fecha >= ? '
            'ORDER BY ct.Fecha ASC',
            (id_estacion, corte)
        )
        for row in cur.fetchall():
            id_cierre, _fecha, _numero, last_updated = row
            if id_cierre in pg_estado and last_updated is not None and (
                    pg_estado[id_cierre] is None or last_updated > pg_estado[id_cierre]):
                modificados.append(row)

    cur.close()
    conn.close()

    return faltantes, modificados

# ---------------------------------------------------------------------------
# Main — itera los grupos solicitados en secuencia
# ---------------------------------------------------------------------------
def main():
    ts_inicio = datetime.now()
    stamp     = ts_inicio.strftime('%Y%m%d_%H%M%S')

    log_exec_path = os.path.join(LOGS_EXEC_DIR, f'ejecucion_{stamp}.txt')
    log_ids_path  = os.path.join(LOGS_IDS_DIR,  f'ids_{stamp}.txt')
    logger = Logger(log_exec_path)
    pg_tunnel.configurar_logging(logger.log)

    try:
        _init_conn = pg_tunnel.conectar_pg(SCHEMA)
        _ensure_fecha_importacion_column(_init_conn, SCHEMA)
        _init_conn.close()
    except Exception as e:
        logger.log(f'ERROR al conectar con PostgreSQL (tunel): {e}')
        pg_tunnel.cerrar_tunel()
        logger.close()
        sys.exit(1)

    logger.log('=' * 60)
    logger.log('INICIO ETL v8 - Cierres de Turno')
    if MODO_INCREMENTAL:
        logger.log('Modo       : incremental (faltantes + modificados)')
    else:
        logger.log(f'Rango      : {FECHA_DESDE} a {FECHA_HASTA}')
    logger.log(f'Estacion   : {ID_ESTACION} | BD SQL Server: {SQL_DB} | Schema PG: {SCHEMA}')
    logger.log(f'Workers    : {MAX_WORKERS} modulos en paralelo por ID')
    logger.log(f'Inicio     : {ts_inicio.strftime("%Y-%m-%d %H:%M:%S")}')
    logger.log('=' * 60)

    logger.log('Consultando IDs en SQL Server...')
    try:
        if MODO_INCREMENTAL:
            _pg_check = pg_tunnel.conectar_pg()
            faltantes, modificados = get_cierres_incremental(SQL_DB, ID_ESTACION, _pg_check, SCHEMA)
            _pg_check.close()
            logger.log(f'  Faltantes  : {len(faltantes)}')
            logger.log(f'  Modificados: {len(modificados)}')
            cierres = faltantes + modificados
        else:
            cierres = get_cierres(FECHA_DESDE, FECHA_HASTA, SQL_DB, ID_ESTACION)
    except Exception as e:
        logger.log(f'ERROR al consultar CierresTurno: {e}')
        pg_tunnel.cerrar_tunel()
        logger.close()
        sys.exit(1)

    logger.log(f'Se encontraron {len(cierres)} cierres.')

    with open(log_ids_path, 'w', encoding='utf-8') as fids:
        fids.write(f'Extraccion : {ts_inicio.strftime("%Y-%m-%d %H:%M:%S")}\n')
        if MODO_INCREMENTAL:
            fids.write('Modo       : incremental (faltantes + modificados)\n')
        else:
            fids.write(f'Rango      : {FECHA_DESDE} a {FECHA_HASTA}\n')
        fids.write(f'Estacion   : {ID_ESTACION} | {SQL_DB}\n')
        fids.write(f'Total      : {len(cierres)} cierres\n')
        fids.write('-' * 50 + '\n')
        fids.write('IdCierreTurno | Fecha                  | Numero\n')
        fids.write('-' * 50 + '\n')
        for row in cierres:
            fids.write(f'{row[0]} | {row[1]} | {row[2]}\n')
    logger.log(f'IDs guardados en: {log_ids_path}')

    if not cierres:
        logger.log('No hay cierres para procesar.')
        pg_tunnel.cerrar_tunel()
        logger.close()
        sys.exit(0)

    # ── FASE 0: Sincronizar tablas maestras antes de procesar cierres ──
    try:
        _sql_master = _connect_sql(SQL_DB)
        _pg_master  = pg_tunnel.conectar_pg()
        sync_all_masters(_sql_master, _pg_master, SCHEMA, logger)
        _sql_master.close()
        _pg_master.close()
    except Exception as _e:
        logger.log(f'[WARN] Sync maestros: {_e}. Continuando con datos existentes.')

    # Pool creado UNA VEZ para todos los IDs
    try:
        pool = ConnectionPool(MAX_WORKERS, SQL_DB, SCHEMA)
    except Exception as e:
        logger.log(f'ERROR al inicializar pool de conexiones: {e}')
        pg_tunnel.cerrar_tunel()
        logger.close()
        sys.exit(1)

    ok_count  = 0
    err_count = 0

    for cierre_row in cierres:
        id_cierre = cierre_row[0]
        fecha_c   = cierre_row[1]
        logger.log()
        logger.log(f'--- IdCierreTurno: {id_cierre} | Fecha: {fecha_c} ---')
        t0 = datetime.now()

        mod_ok, mod_err = process_id(id_cierre, SCHEMA, pool, logger)

        duracion = (datetime.now() - t0).total_seconds()
        logger.log(f'  Modulos: {mod_ok} OK, {mod_err} con error | Duracion: {duracion:.1f}s')

        if mod_err == 0:
            ok_count += 1
        else:
            err_count += 1

    pool.close_all()
    pg_tunnel.cerrar_tunel()

    duracion_total = (datetime.now() - ts_inicio).total_seconds()
    logger.log()
    logger.log('=' * 60)
    logger.log(f'RESUMEN ESTACION {ID_ESTACION} ({SQL_DB})')
    logger.log(f'  Cierres procesados : {len(cierres)}')
    logger.log(f'  Completados OK     : {ok_count}')
    logger.log(f'  Con errores        : {err_count}')
    logger.log(f'  Duracion total     : {duracion_total:.1f}s')
    logger.log('=' * 60)
    logger.close()

    sys.exit(0 if err_count == 0 else 1)


if __name__ == '__main__':
    main()
