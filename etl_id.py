"""
ETL v8 — Extrae un único cierre de turno por ID.
Instancia dedicada a una única estación (SQLSERVER_DB / SCHEMA en .env).
Uso: python etl_id.py <id_cierre_turno>
  Ejemplo: python etl_id.py 12345
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
# Cargar .env
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
if len(sys.argv) < 2:
    print("Uso: python etl_id.py <id_cierre_turno>")
    print("  Ejemplo: python etl_id.py 12345")
    sys.exit(1)

try:
    ID_CIERRE = int(sys.argv[1])
except ValueError:
    print(f"Error: '{sys.argv[1]}' no es un ID válido.")
    sys.exit(1)

MAX_WORKERS = 5

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------
SQL_HOST = os.getenv('SQLSERVER_HOST_1')
SQL_USER = os.getenv('SQLSERVER_USER_1')
SQL_PASS = os.getenv('SQLSERVER_PASSWORD_1')
SQL_DB   = os.getenv('SQLSERVER_DB')
SCHEMA   = os.getenv('SCHEMA')
ID_ESTACION = os.getenv('IDEstacion')

if not SQL_DB or not SCHEMA:
    print("Error: faltan SQLSERVER_DB o SCHEMA en .env.")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Rutas de logs
# ---------------------------------------------------------------------------
_BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
LOGS_IDS_DIR  = os.path.join(_BASE_DIR, 'logs_ids')
LOGS_EXEC_DIR = os.path.join(_BASE_DIR, 'logs_ejecuciones')
os.makedirs(LOGS_IDS_DIR,  exist_ok=True)
os.makedirs(LOGS_EXEC_DIR, exist_ok=True)

# ---------------------------------------------------------------------------
# Logger
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
# Pool de conexiones
# ---------------------------------------------------------------------------
class ConnectionPool:
    def __init__(self, size):
        self._sql_q = Queue()
        self._pg_q  = Queue()
        for _ in range(size):
            self._sql_q.put(_connect_sql())
            self._pg_q.put(_connect_pg())

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
def _connect_sql():
    cs = (f'DRIVER={{SQL Server}};SERVER={SQL_HOST};DATABASE={SQL_DB};'
          f'UID={SQL_USER};PWD={SQL_PASS};TrustServerCertificate=True')
    conn = pyodbc.connect(cs)
    conn.timeout = 600
    return conn

def _connect_pg():
    return pg_tunnel.conectar_pg(SCHEMA)

# ---------------------------------------------------------------------------
# Tipos
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

def to_snake(name):
    return re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()

# ── Caches de columnas PG y lookups de maestros ───────────────────────────
_pg_cols_cache:   dict = {}
_cajas_lkp_cache: dict = {}
_bancos_lkp_cache: dict = {}

def _get_pg_cols(pg_conn, table_name):
    key = f'{SCHEMA}.{table_name}'
    if key not in _pg_cols_cache:
        cur = pg_conn.cursor()
        cur.execute(
            'SELECT column_name FROM information_schema.columns '
            'WHERE table_schema = %s AND table_name = %s',
            (SCHEMA, table_name)
        )
        _pg_cols_cache[key] = {r[0] for r in cur.fetchall()}
        cur.close()
    return _pg_cols_cache[key]

def _get_cajas_lkp(pg_conn):
    if SCHEMA not in _cajas_lkp_cache:
        cur = pg_conn.cursor()
        cur.execute(f'SELECT id_caja, descripcion FROM {SCHEMA}.cajas')
        _cajas_lkp_cache[SCHEMA] = {
            str(r[1] or '').strip().upper(): r[0] for r in cur.fetchall()
        }
        cur.close()
    return _cajas_lkp_cache[SCHEMA]

def _get_bancos_lkp(pg_conn):
    if SCHEMA not in _bancos_lkp_cache:
        cur = pg_conn.cursor()
        cur.execute(f'SELECT id_banco, nombre FROM {SCHEMA}.bancos')
        _bancos_lkp_cache[SCHEMA] = {
            str(r[1] or '').strip().upper(): r[0] for r in cur.fetchall()
        }
        cur.close()
    return _bancos_lkp_cache[SCHEMA]

# ---------------------------------------------------------------------------
# _handle_factura_pendiente — calcula importe pendiente desde @TablaResult
# ---------------------------------------------------------------------------
def _handle_factura_pendiente(cursor, id_cierre, pg_conn):
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

    full_table = f'{SCHEMA}.facturas_pendientes'
    cur = pg_conn.cursor()
    try:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {full_table} (
                id_cierre_turno   BIGINT PRIMARY KEY,
                importe_pendiente NUMERIC(18,2)
            )
        """)
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
# Sync de tablas maestras (Fase 0)
# ---------------------------------------------------------------------------
_MASTER_TABLES = [
    ('Estaciones',        'estaciones',        'id_estacion',         False),
    ('FamiliasArticulos', 'familias_articulos', 'id_familia_articulo', False),
    ('GruposArticulos',   'grupos_articulos',   'id_grupo_articulo',   False),
    ('Articulos',         'articulos',          'id_articulo',         False),
    ('Cajas',             'cajas',              'id_caja',             False),
    ('Empleados',         'empleados',          'id_empleado',         False),
    ('Bancos',            'bancos',             'id_banco',            False),
    ('Clientes',          'Clientes',           'IdCliente',           True),
    ('TiposMovimiento',   'tipos_movimientos',  'id_tipo_movimiento',  False),
    ('TarjetasCredito',   'tarjetas_credito',   'id_tarjeta',         False),
]

def _sync_master_table(sql_conn, pg_conn, sql_table, pg_table, pk_col, preserve_case=False):
    cur = sql_conn.cursor()
    if sql_table == 'Cajas' and ID_ESTACION:
        cur.execute(f'SELECT * FROM {sql_table} WHERE IdEstacion = ?', (int(ID_ESTACION),))
    else:
        cur.execute(f'SELECT * FROM {sql_table}')
    col_names = [col[0] for col in cur.description]
    col_types = [col[1] for col in cur.description]
    rows      = cur.fetchall()
    cur.close()

    if not rows:
        return 0

    pg_cols = col_names if preserve_case else [to_snake(c) for c in col_names]

    # Auto-filtrar a columnas que existen en PG
    pg_existing = _get_pg_cols(pg_conn, pg_table)

    # DDL: crear tabla si no existe
    if not pg_existing:
        def q(name):
            return f'"{name}"' if preserve_case else name
        col_defs = []
        for col_name, r in zip(col_names, rows[0]):
            pg_type = get_pg_type(type(r))
            col_snake = col_name if preserve_case else to_snake(col_name)
            inline_pk = ' PRIMARY KEY' if col_snake == pk_col else ''
            col_defs.append(f'{q(col_snake)} {pg_type}{inline_pk}')
        full_table = f'{SCHEMA}."{pg_table}"' if preserve_case else f'{SCHEMA}.{pg_table}'
        cur_ddl = pg_conn.cursor()
        try:
            cur_ddl.execute(f'CREATE TABLE IF NOT EXISTS {full_table} ({", ".join(col_defs)})')
            pg_conn.commit()
            _pg_cols_cache[f'{SCHEMA}.{pg_table}'] = set(pg_cols)
            pg_existing = set(pg_cols)
        except Exception:
            pg_conn.rollback()
        finally:
            cur_ddl.close()

    keep_idx    = [i for i, sc in enumerate(pg_cols) if sc in pg_existing]
    if not keep_idx:
        return 0

    pg_cols_f  = [pg_cols[i]    for i in keep_idx]
    data_rows  = [[list(r)[i] for i in keep_idx] for r in rows]

    # Deduplicar por PK
    if pk_col in pg_cols_f:
        seen = {}
        for row in data_rows:
            key = row[pg_cols_f.index(pk_col)]
            seen[key] = row
        data_rows = list(seen.values())

    def q(name):
        return f'"{name}"' if preserve_case else name

    full_table = f'{SCHEMA}."{pg_table}"' if preserve_case else f'{SCHEMA}.{pg_table}'
    cols_str   = ', '.join(q(c) for c in pg_cols_f)
    non_pk     = [c for c in pg_cols_f if c != pk_col]
    if non_pk:
        set_clause = ', '.join(f'{q(c)} = EXCLUDED.{q(c)}' for c in non_pk)
        upsert_q = (f'INSERT INTO {full_table} ({cols_str}) VALUES %s '
                    f'ON CONFLICT ({q(pk_col)}) DO UPDATE SET {set_clause}')
    else:
        upsert_q = (f'INSERT INTO {full_table} ({cols_str}) VALUES %s '
                    f'ON CONFLICT ({q(pk_col)}) DO NOTHING')

    cur_pg = pg_conn.cursor()
    try:
        extras.execute_values(cur_pg, upsert_q, [tuple(r) for r in data_rows], page_size=500)
        pg_conn.commit()
    except Exception:
        pg_conn.rollback()
        raise
    finally:
        cur_pg.close()

    return len(data_rows)

def sync_all_masters(sql_conn, pg_conn, logger):
    for sql_table, pg_table, pk_col, preserve_case in _MASTER_TABLES:
        try:
            count = _sync_master_table(sql_conn, pg_conn, sql_table, pg_table, pk_col, preserve_case)
            logger.log(f'  [Maestros] {pg_table}: {count} filas')
        except Exception as e:
            logger.log(f'  [Maestros] {pg_table}: ERROR — {e}')
    _ensure_grupos_sinteticos(pg_conn, logger)
    # Invalidar caches de lookups para que usen datos frescos
    _cajas_lkp_cache.pop(SCHEMA, None)
    _bancos_lkp_cache.pop(SCHEMA, None)


def _ensure_grupos_sinteticos(pg_conn, logger):
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
            CREATE TABLE IF NOT EXISTS {SCHEMA}.grupos_articulos (
                id_grupo_articulo BIGINT PRIMARY KEY,
                descripcion TEXT
            )
        ''')
        pg_conn.commit()

        # Verificar qué registros faltan
        cur.execute(
            f'SELECT id_grupo_articulo FROM {SCHEMA}.grupos_articulos '
            f'WHERE id_grupo_articulo IN (-1, -2)'
        )
        existentes = {r[0] for r in cur.fetchall()}

        insertados = 0
        for gid, gdesc in dummy_grupos:
            if gid not in existentes:
                cur.execute(
                    f'INSERT INTO {SCHEMA}.grupos_articulos (id_grupo_articulo, descripcion) '
                    f'VALUES (%s, %s)',
                    (gid, gdesc)
                )
                insertados += 1

        if insertados > 0:
            pg_conn.commit()
            logger.log(f'  [Maestros] grupos_articulos: {insertados} registros sintéticos insertados (-1, -2)')
        else:
            logger.log(f'  [Maestros] grupos_articulos: registros sintéticos -1, -2 ya existentes')
    except Exception:
        pg_conn.rollback()
        raise
    finally:
        cur.close()

# ---------------------------------------------------------------------------
# sync_table
# ---------------------------------------------------------------------------
def sync_table(pg_conn, table_name, col_names, col_types, rows,
               pk_cols=None, inject_id=None,
               derive_caja_from=None, derive_banco_from=None):
    if not rows:
        return 0

    # ── 1. snake_case ────────────────────────────────────────────────────────
    pg_cols   = [to_snake(c) for c in col_names]
    data_rows = [list(r) for r in rows]
    types_mut = list(col_types)

    # ── 2. DERIVACIONES (antes del filtro) ───────────────────────────────────
    # 2a. id_caja desde descripcion con patron "(NNN)- ..."
    if derive_caja_from == 'descripcion' and 'descripcion' in pg_cols:
        src_idx = pg_cols.index('descripcion')
        pg_cols.append('id_caja')
        types_mut.append(int)
        _pat = re.compile(r'^\((\d+)\s*\)')
        for row in data_rows:
            val = str(row[src_idx] or '')
            m   = _pat.match(val)
            row.append(int(m.group(1)) if m else None)

    # 2b. id_caja desde descripcion_caja via lookup en tabla cajas
    if derive_caja_from == 'descripcion_caja' and 'descripcion_caja' in pg_cols:
        src_idx  = pg_cols.index('descripcion_caja')
        lkp      = _get_cajas_lkp(pg_conn)
        pg_cols.append('id_caja')
        types_mut.append(int)
        for row in data_rows:
            key = str(row[src_idx] or '').strip().upper()
            row.append(lkp.get(key))

    # 2c. id_banco desde nombre_banco via lookup en tabla bancos
    if derive_banco_from == 'nombre_banco' and 'nombre_banco' in pg_cols:
        src_idx = pg_cols.index('nombre_banco')
        lkp     = _get_bancos_lkp(pg_conn)
        pg_cols.append('id_banco')
        types_mut.append(int)
        for row in data_rows:
            key = str(row[src_idx] or '').strip().upper()
            row.append(lkp.get(key))

    # ── 3. AUTO-FILTRO: mantener solo columnas que existen en PG ─────────────
    pg_existing = _get_pg_cols(pg_conn, table_name)
    if pg_existing:
        keep_idx  = [i for i, sc in enumerate(pg_cols) if sc in pg_existing]
        pg_cols   = [pg_cols[i]    for i in keep_idx]
        types_mut = [types_mut[i]  for i in keep_idx]
        data_rows = [[row[i] for i in keep_idx] for row in data_rows]

    # ── 4. INJECT id_cierre_turno ────────────────────────────────────────────
    if inject_id is not None and 'id_cierre_turno' not in pg_cols:
        pg_cols.append('id_cierre_turno')
        types_mut.append(int)
        for row in data_rows:
            row.append(inject_id)

    # ── 5. CAST id_banco TEXT → INT ──────────────────────────────────────────
    if 'id_banco' in pg_cols:
        idx = pg_cols.index('id_banco')
        for row in data_rows:
            v = row[idx]
            if v is not None:
                try:
                    row[idx] = int(str(v).strip()) if str(v).strip() else None
                except (ValueError, TypeError):
                    row[idx] = None

    # ── 6. DDL (solo si la tabla no existe aun en PG) ────────────────────────
    full_table = f'{SCHEMA}.{table_name}'
    if not pg_existing:
        col_defs = []
        for col, tc in zip(pg_cols, types_mut):
            inline_pk = (' PRIMARY KEY'
                         if pk_cols and len(pk_cols) == 1 and col == pk_cols[0]
                         else '')
            col_defs.append(f'{col} {get_pg_type(tc)}{inline_pk}')
        pk_constraint = (f', PRIMARY KEY ({", ".join(pk_cols)})'
                         if pk_cols and len(pk_cols) > 1 else '')
        cur = pg_conn.cursor()
        try:
            cur.execute(f'CREATE TABLE IF NOT EXISTS {full_table} '
                        f'({", ".join(col_defs)}{pk_constraint})')
            pg_conn.commit()
        except Exception:
            pg_conn.rollback()
            raise
        finally:
            cur.close()
        # Actualizar cache para que la proxima llamada ya vea la tabla
        _pg_cols_cache[f'{SCHEMA}.{table_name}'] = set(pg_cols)

    # ── 7. Deduplicar por PK ─────────────────────────────────────────────────
    if pk_cols:
        seen = {}
        for row in data_rows:
            try:
                key = tuple(row[pg_cols.index(pk)] for pk in pk_cols if pk in pg_cols)
            except (ValueError, IndexError):
                key = None
            seen[key if key is not None else id(row)] = row
        data_rows = list(seen.values())

    if not data_rows:
        return 0

    # ── 8. Upsert ────────────────────────────────────────────────────────────
    cols_str = ', '.join(pg_cols)
    if pk_cols:
        non_pk = [c for c in pg_cols if c not in pk_cols]
        if non_pk:
            set_clause = ', '.join(f'{c} = EXCLUDED.{c}' for c in non_pk)
            upsert_q = (f'INSERT INTO {full_table} ({cols_str}) VALUES %s '
                        f'ON CONFLICT ({", ".join(pk_cols)}) DO UPDATE SET {set_clause}')
        else:
            upsert_q = (f'INSERT INTO {full_table} ({cols_str}) VALUES %s '
                        f'ON CONFLICT ({", ".join(pk_cols)}) DO NOTHING')
    else:
        upsert_q = f'INSERT INTO {full_table} ({cols_str}) VALUES %s'

    cur = pg_conn.cursor()
    try:
        extras.execute_values(cur, upsert_q, [tuple(r) for r in data_rows], page_size=500)
        pg_conn.commit()
    except Exception:
        pg_conn.rollback()
        raise
    finally:
        cur.close()

    return len(data_rows)

# ---------------------------------------------------------------------------
# Módulos ETL (mismos 20 que etl.py)
# ---------------------------------------------------------------------------
MODULES = [
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
    {
        'name': '009_Listado_RecibosEnTurno',
        'sp': 'SET NOCOUNT ON; EXECUTE Listado_RecibosEnTurno @IdCierreTurno = ?',
        'sets': {
            1: {'table': 'cierre_turno_listado_recibos',
                'pk': ['comprobante', 'razon_social'], 'inject': False},
        },
        'extra': 'cierre_turno_listado_recibos_extra',
    },
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
    {
        'name': '018_Listado_CuentaCorriente',
        'sp': ("SET NOCOUNT ON; EXECUTE Listado_TotalesCuentaCorrienteEnTurno "
               "@IdCierreTurno = ?, @Debug = '0'"),
        'sets': {
            1: {'table': 'cierre_turno_cuentacorriente_totales',
                'pk': ['id_cierre_turno'], 'inject': True},
            2: {'table': 'cierre_turno_cuentacorriente_detalles',
                'pk': ['id_cierre_turno', 'operacion', 'id_articulo'], 'inject': True},
        },
        'extra': 'cierre_turno_cuentacorriente_extra',
    },
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
    {
        'name': '021_FacturaPendiente',
        'sp': "SET NOCOUNT ON; EXECUTE Listado_EstadoAforadores @IdCierreTurno = ?, @CaldenON = '1', @Debug = '1'",
        'sets': {},
        'extra': 'cierre_turno_aforadores_factura_extra',
        'factura_pendiente': True,
    },
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
# run_module y process_id
# ---------------------------------------------------------------------------
def run_module(mod, id_cierre, sql_conn, pg_conn):
    cursor = sql_conn.cursor()
    cursor.execute(mod['sp'], (id_cierre,))

    if mod.get('factura_pendiente'):
        result = _handle_factura_pendiente(cursor, id_cierre, pg_conn)
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
                count  = sync_table(pg_conn, cfg['table'], columns, types, rows,
                                    pk_cols=cfg['pk'], inject_id=inject,
                                    derive_caja_from=cfg.get('derive_caja_from'),
                                    derive_banco_from=cfg.get('derive_banco_from'))
                if count > 0:
                    totales[cfg['table']] = totales.get(cfg['table'], 0) + count
            elif rows:
                extra = f"{mod['extra']}_{set_num}"
                count = sync_table(pg_conn, extra, columns, types, rows)
                if count > 0:
                    totales[extra] = count

        if not cursor.nextset():
            break
        set_num += 1

    cursor.close()
    return totales


# ---------------------------------------------------------------------------
# fecha_importacion en cierre_turno_encabezado: no viene del SP, se agrega
# aparte. Registra cuando NOSOTROS importamos el cierre, para que el modo
# incremental de etl.py no lo trate como "modificado" en la proxima corrida.
# ---------------------------------------------------------------------------
def _ensure_fecha_importacion_column(pg_conn):
    cur = pg_conn.cursor()
    cur.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema = %s AND table_name = 'cierre_turno_encabezado'",
        (SCHEMA,)
    )
    if cur.fetchone():
        cur.execute(
            f'ALTER TABLE {SCHEMA}.cierre_turno_encabezado '
            f'ADD COLUMN IF NOT EXISTS fecha_importacion TIMESTAMP'
        )
        pg_conn.commit()
    cur.close()

def _set_fecha_importacion(id_cierre, pg_conn, logger):
    cur_pg = pg_conn.cursor()
    try:
        cur_pg.execute(
            f'UPDATE {SCHEMA}.cierre_turno_encabezado '
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
# Fallback de id_caja: consulta CierresTurno en SQL Server si quedó NULL en PG
# ---------------------------------------------------------------------------
def _fix_id_caja(id_cierre, sql_conn, pg_conn, logger):
    cur_pg = pg_conn.cursor()
    try:
        cur_pg.execute(
            f'SELECT id_caja FROM {SCHEMA}.cierre_turno_encabezado WHERE id_cierre_turno = %s',
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
            f'UPDATE {SCHEMA}.cierre_turno_encabezado SET id_caja = %s WHERE id_cierre_turno = %s',
            (sql_row[0], id_cierre)
        )
        pg_conn.commit()
        logger.log(f'  [FIX id_caja] {id_cierre}: corregido -> id_caja = {sql_row[0]}')
    except Exception as e:
        pg_conn.rollback()
        logger.log(f'  [FIX id_caja] {id_cierre}: ERROR — {e}')
    finally:
        cur_pg.close()


def process_id(id_cierre, pool, logger):
    def run_with_pool(mod):
        sql_conn, pg_conn = pool.acquire()
        try:
            return mod['name'], True, run_module(mod, id_cierre, sql_conn, pg_conn)
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
        _fix_id_caja(id_cierre, sql_conn, pg_conn, logger)
        _set_fecha_importacion(id_cierre, pg_conn, logger)
    finally:
        pool.release(sql_conn, pg_conn)

    return mod_ok, mod_err

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    ts_inicio = datetime.now()
    stamp     = ts_inicio.strftime('%Y%m%d_%H%M%S')

    log_exec_path = os.path.join(LOGS_EXEC_DIR, f'ejecucion_{stamp}_id{ID_CIERRE}.txt')
    log_ids_path  = os.path.join(LOGS_IDS_DIR,  f'ids_{stamp}_id{ID_CIERRE}.txt')
    logger = Logger(log_exec_path)
    pg_tunnel.configurar_logging(logger.log)

    logger.log('=' * 60)
    logger.log('INICIO ETL v5 - Cierre por ID')
    logger.log(f'IdCierreTurno : {ID_CIERRE}')
    logger.log(f'Estacion      : {ID_ESTACION} | BD SQL Server: {SQL_DB} | Schema PG: {SCHEMA}')
    logger.log(f'Workers       : {MAX_WORKERS} modulos en paralelo')
    logger.log(f'Inicio        : {ts_inicio.strftime("%Y-%m-%d %H:%M:%S")}')
    logger.log('=' * 60)

    # Verificar que el ID existe y obtener sus datos
    logger.log('Verificando ID en SQL Server...')
    try:
        conn_check = _connect_sql()
        cur_check  = conn_check.cursor()
        cur_check.execute(
            'SELECT ct.IdCierreTurno, ct.Fecha, ct.Numero '
            'FROM CierresTurno ct JOIN Cajas c ON c.IdCaja = ct.IdCaja '
            'WHERE ct.IdCierreTurno = ? AND c.IdEstacion = ?',
            (ID_CIERRE, int(ID_ESTACION))
        )
        row = cur_check.fetchone()
        cur_check.close()
        conn_check.close()
    except Exception as e:
        logger.log(f'ERROR al consultar SQL Server: {e}')
        pg_tunnel.cerrar_tunel()
        logger.close()
        sys.exit(1)

    if not row:
        logger.log(f'ERROR: No se encontro el IdCierreTurno {ID_CIERRE} en {SQL_DB} para la estacion {ID_ESTACION}.')
        pg_tunnel.cerrar_tunel()
        logger.close()
        sys.exit(1)

    fecha_c  = row[1]
    numero_c = row[2]
    logger.log(f'Cierre encontrado — Fecha: {fecha_c} | Numero: {numero_c}')

    with open(log_ids_path, 'w', encoding='utf-8') as fids:
        fids.write(f'Extraccion    : {ts_inicio.strftime("%Y-%m-%d %H:%M:%S")}\n')
        fids.write(f'IdCierreTurno : {ID_CIERRE}\n')
        fids.write(f'Fecha         : {fecha_c}\n')
        fids.write(f'Numero        : {numero_c}\n')
        fids.write(f'Estacion      : {ID_ESTACION} | {SQL_DB}\n')

    # ── FASE 0: Sincronizar tablas maestras antes de procesar el cierre ──────
    logger.log()
    logger.log('FASE 0: Sincronizando tablas maestras...')
    try:
        _sql_master = _connect_sql()
        _pg_master  = pg_tunnel.conectar_pg()
        sync_all_masters(_sql_master, _pg_master, logger)
        _sql_master.close()
        _pg_master.close()
        logger.log('FASE 0: Maestros sincronizados OK.')
    except Exception as _e:
        logger.log(f'[WARN] Sync maestros: {_e}. Continuando con datos existentes.')

    try:
        pool = ConnectionPool(MAX_WORKERS)
    except Exception as e:
        logger.log(f'ERROR al inicializar pool de conexiones: {e}')
        pg_tunnel.cerrar_tunel()
        logger.close()
        sys.exit(1)

    _pg_ddl = _connect_pg()
    _ensure_fecha_importacion_column(_pg_ddl)
    _pg_ddl.close()

    logger.log()
    logger.log(f'--- Procesando IdCierreTurno: {ID_CIERRE} | Fecha: {fecha_c} ---')
    t0 = datetime.now()

    mod_ok, mod_err = process_id(ID_CIERRE, pool, logger)

    duracion = (datetime.now() - t0).total_seconds()
    pool.close_all()
    pg_tunnel.cerrar_tunel()

    logger.log()
    logger.log('=' * 60)
    logger.log('RESUMEN')
    logger.log(f'  IdCierreTurno : {ID_CIERRE}')
    logger.log(f'  Modulos OK    : {mod_ok}')
    logger.log(f'  Con errores   : {mod_err}')
    logger.log(f'  Duracion      : {duracion:.1f}s')
    logger.log('=' * 60)
    logger.close()

    sys.exit(0 if mod_err == 0 else 1)


if __name__ == '__main__':
    main()
