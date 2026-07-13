"""
Sincronizacion de tablas maestras (Fase 0) para los 3 grupos.
Equivale a ejecutar solo la Fase 0 del ETL sin procesar cierres de turno.
Si la tabla no existe en PG la crea automaticamente desde el esquema de SQL Server.
Uso: python actualizar_maestros.py
"""
import os
import re
import sys
import decimal
import datetime as _dt
import threading
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

# ---------------------------------------------------------------------------
# Configuracion
# ---------------------------------------------------------------------------
PG_HOST  = os.getenv('HOST')
PG_DB    = os.getenv('DATABASE')
PG_USER  = os.getenv('USERNAME')
PG_PASS  = os.getenv('PASSWORD')
PG_PORT  = os.getenv('PORT_1', '5432')

SQL_HOST = os.getenv('SQLSERVER_HOST_1')
SQL_USER = os.getenv('SQLSERVER_USER_1')
SQL_PASS = os.getenv('SQLSERVER_PASSWORD_1')

SQL_DB      = os.getenv('SQLSERVER_DB')
SCHEMA      = os.getenv('SCHEMA')
ID_ESTACION = os.getenv('IDEstacion')

# (sql_table, pg_table, pk_col, preserve_case)
# preserve_case=True: no convierte a snake_case; cita identificadores con comillas dobles en PG.
_MASTER_TABLES = [
    ('Estaciones',        'estaciones',         'id_estacion',         False),
    ('FamiliasArticulos', 'familias_articulos',  'id_familia_articulo', False),
    ('GruposArticulos',   'grupos_articulos',    'id_grupo_articulo',   False),
    ('Articulos',         'articulos',           'id_articulo',         False),
    ('Cajas',             'cajas',               'id_caja',             False),
    ('Empleados',         'empleados',           'id_empleado',         False),
    ('Bancos',            'bancos',              'id_banco',            False),
    ('Clientes',          'Clientes',            'IdCliente',           True),
]

# ---------------------------------------------------------------------------
# Logs
# ---------------------------------------------------------------------------
_BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
LOGS_EXEC_DIR = os.path.join(_BASE_DIR, 'logs_ejecuciones')
os.makedirs(LOGS_EXEC_DIR, exist_ok=True)


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
# Conexiones
# ---------------------------------------------------------------------------
def _connect_sql(db):
    cs = (f'DRIVER={{SQL Server}};SERVER={SQL_HOST};DATABASE={db};'
          f'UID={SQL_USER};PWD={SQL_PASS};TrustServerCertificate=True')
    conn = pyodbc.connect(cs)
    conn.timeout = 300
    return conn

def _connect_pg():
    return psycopg2.connect(
        host=PG_HOST, database=PG_DB, user=PG_USER,
        password=PG_PASS, port=PG_PORT
    )

# ---------------------------------------------------------------------------
# Mapeo de tipos pyodbc → PostgreSQL
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

def _pg_type(type_code):
    return _TYPE_MAP.get(type_code, 'TEXT')

# ---------------------------------------------------------------------------
# Conversion de nombres
# ---------------------------------------------------------------------------
def _to_snake(name):
    return re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()

# ---------------------------------------------------------------------------
# Cache de columnas PG
# ---------------------------------------------------------------------------
_pg_cols_cache: dict = {}

def _get_pg_cols(pg_conn, schema, table):
    key = f'{schema}.{table}'
    if key not in _pg_cols_cache:
        cur = pg_conn.cursor()
        cur.execute(
            'SELECT column_name FROM information_schema.columns '
            'WHERE table_schema = %s AND table_name = %s',
            (schema, table)
        )
        _pg_cols_cache[key] = {r[0] for r in cur.fetchall()}
        cur.close()
    return _pg_cols_cache[key]

# ---------------------------------------------------------------------------
# Sincronizar una tabla maestra
# ---------------------------------------------------------------------------
def _sync_master_table(sql_conn, pg_conn, schema, sql_table, pg_table, pk_col, preserve_case):
    cur_sql = sql_conn.cursor()
    if sql_table == 'Cajas' and ID_ESTACION:
        cur_sql.execute(f'SELECT * FROM {sql_table} WHERE IdEstacion = ?', (int(ID_ESTACION),))
    else:
        cur_sql.execute(f'SELECT * FROM {sql_table}')
    sql_cols_raw = [col[0] for col in cur_sql.description]
    col_types    = [col[1] for col in cur_sql.description]
    sql_cols     = sql_cols_raw if preserve_case else [_to_snake(c) for c in sql_cols_raw]
    rows_raw     = cur_sql.fetchall()
    cur_sql.close()

    if not rows_raw:
        return 0

    def q(name):
        return f'"{name}"' if preserve_case else name

    full_table = f'{schema}."{pg_table}"' if preserve_case else f'{schema}.{pg_table}'

    pg_existing = _get_pg_cols(pg_conn, schema, pg_table)

    # Si la tabla no existe en PG, crearla a partir del esquema de SQL Server
    if not pg_existing:
        col_defs = []
        for col, tc in zip(sql_cols, col_types):
            inline_pk = ' PRIMARY KEY' if col == pk_col else ''
            col_defs.append(f'{q(col)} {_pg_type(tc)}{inline_pk}')
        cur = pg_conn.cursor()
        try:
            cur.execute(f'CREATE TABLE IF NOT EXISTS {full_table} ({", ".join(col_defs)})')
            pg_conn.commit()
        except Exception:
            pg_conn.rollback()
            raise
        finally:
            cur.close()
        pg_existing = set(sql_cols)
        _pg_cols_cache[f'{schema}.{pg_table}'] = pg_existing

    # Auto-filtro: solo columnas que existen en la tabla PG destino
    keep     = [i for i, c in enumerate(sql_cols) if c in pg_existing]
    sql_cols = [sql_cols[i] for i in keep]
    rows     = [[list(r)[i] for i in keep] for r in rows_raw]

    if not rows:
        return 0

    # Deduplicar por PK
    if pk_col in sql_cols:
        seen = {}
        for row in rows:
            seen[row[sql_cols.index(pk_col)]] = row
        rows = list(seen.values())

    cols_str = ', '.join(q(c) for c in sql_cols)
    non_pk   = [c for c in sql_cols if c != pk_col]
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

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    ts_inicio = datetime.now()
    stamp     = ts_inicio.strftime('%Y%m%d_%H%M%S')
    log_path  = os.path.join(LOGS_EXEC_DIR, f'actualizar_maestros_{stamp}.txt')
    logger    = Logger(log_path)

    logger.log('=' * 60)
    logger.log('Actualizacion de Tablas Maestras (Fase 0)')
    logger.log(f'Inicio: {ts_inicio.strftime("%Y-%m-%d %H:%M:%S")}')
    logger.log('=' * 60)

    try:
        pg_conn = _connect_pg()
    except Exception as e:
        logger.log(f'ERROR al conectar a PostgreSQL: {e}')
        logger.close()
        sys.exit(1)

    if not SQL_DB or not SCHEMA:
        logger.log('Variables SQLSERVER_DB/SCHEMA no configuradas en .env.')
        logger.close()
        sys.exit(1)

    # Crear schema si no existe
    cur = pg_conn.cursor()
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
    pg_conn.commit()
    cur.close()

    errores = 0

    logger.log()
    logger.log(f'{SQL_DB} -> schema: {SCHEMA}')

    try:
        sql_conn = _connect_sql(SQL_DB)
    except Exception as e:
        logger.log(f'  ERROR al conectar a SQL Server: {e}')
        errores += 1
        sql_conn = None

    if sql_conn:
        for sql_table, pg_table, pk_col, preserve_case in _MASTER_TABLES:
            try:
                count = _sync_master_table(sql_conn, pg_conn, SCHEMA,
                                           sql_table, pg_table, pk_col, preserve_case)
                logger.log(f'  {pg_table}: {count} filas')
            except Exception as e:
                try:
                    pg_conn.rollback()
                except Exception:
                    pass
                logger.log(f'  {pg_table}: ERROR — {e}')
                errores += 1

        sql_conn.close()

    pg_conn.close()

    duracion = (datetime.now() - ts_inicio).total_seconds()
    logger.log()
    logger.log('=' * 60)
    logger.log(f'Fin. Duracion: {duracion:.1f}s | Errores: {errores}')
    logger.log('=' * 60)
    logger.close()

    sys.exit(0 if errores == 0 else 1)


if __name__ == '__main__':
    main()
