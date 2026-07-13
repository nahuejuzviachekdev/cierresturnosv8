"""
Proceso 024, 025 y 026 — Sincronizacion de tablas maestras:
Articulos, FamiliasArticulos y GruposArticulos.
Extrae las tablas directamente de cada base SQL Server (solo lectura)
y las replica en el schema PostgreSQL correspondiente.
Crea la tabla si no existe; upsert si hay datos nuevos.
Uso: python articulos_maestros.py
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
PG_HOST = os.getenv('HOST')
PG_DB   = os.getenv('DATABASE')
PG_USER = os.getenv('USERNAME')
PG_PASS = os.getenv('PASSWORD')
PG_PORT = os.getenv('PORT_1', '5432')

SQL_HOST = os.getenv('SQLSERVER_HOST_1')
SQL_USER = os.getenv('SQLSERVER_USER_1')
SQL_PASS = os.getenv('SQLSERVER_PASSWORD_1')

SQL_DB = os.getenv('SQLSERVER_DB')
SCHEMA = os.getenv('SCHEMA')

TABLAS = [
    {'proceso': '024_FamiliasArticulos', 'sql_table': 'FamiliasArticulos', 'pg_table': 'familias_articulos'},
    {'proceso': '025_GruposArticulos',   'sql_table': 'GruposArticulos',   'pg_table': 'grupos_articulos'},
    {'proceso': '026_Articulos',         'sql_table': 'Articulos',         'pg_table': 'articulos'},
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
# Mapeo de tipos pyodbc -> PostgreSQL
# ---------------------------------------------------------------------------
_TYPE_MAP = {
    bool:            'BOOLEAN',
    int:             'BIGINT',
    float:           'DOUBLE PRECISION',
    decimal.Decimal: 'NUMERIC',
    _dt.datetime:    'TIMESTAMP',
    _dt.date:        'DATE',
    str:             'TEXT',
    bytes:           'BYTEA',
}

def _pg_type(type_code):
    return _TYPE_MAP.get(type_code, 'TEXT')

def _snake(name):
    return re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()

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

def _init_schemas(pg_conn):
    cur = pg_conn.cursor()
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
    pg_conn.commit()
    cur.close()

# ---------------------------------------------------------------------------
# Sincronizar una tabla maestra
# ---------------------------------------------------------------------------
def sync_master_table(sql_conn, pg_conn, all_schemas, schema, sql_table, pg_table):
    cur = sql_conn.cursor()

    # Detectar PK desde SQL Server (solo lectura)
    cur.execute("""
        SELECT kcu.COLUMN_NAME
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
        JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
          ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
         AND tc.TABLE_NAME      = kcu.TABLE_NAME
        WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
          AND kcu.TABLE_NAME     = ?
        ORDER BY kcu.ORDINAL_POSITION
    """, (sql_table,))
    pk_raw  = [row[0] for row in cur.fetchall()]
    pk_cols = [_snake(c) for c in pk_raw] if pk_raw else None

    # Extraer datos (solo SELECT)
    cur.execute(f'SELECT * FROM {sql_table}')
    col_names = [col[0] for col in cur.description]
    col_types = [col[1] for col in cur.description]
    rows      = cur.fetchall()
    cur.close()

    # Fallback: primera columna si empieza con 'Id'
    if not pk_cols and col_names and col_names[0].lower().startswith('id'):
        pk_cols = [_snake(col_names[0])]

    pg_cols = [_snake(c) for c in col_names]

    # Construir DDL
    col_defs = []
    for col, tc in zip(pg_cols, col_types):
        inline_pk = (' PRIMARY KEY'
                     if pk_cols and len(pk_cols) == 1 and col == pk_cols[0]
                     else '')
        col_defs.append(f'{col} {_pg_type(tc)}{inline_pk}')
    pk_constraint = (f', PRIMARY KEY ({", ".join(pk_cols)})'
                     if pk_cols and len(pk_cols) > 1 else '')
    ddl_body = f'({", ".join(col_defs)}{pk_constraint})'

    # Crear tabla en todos los schemas si no existe
    cur_pg = pg_conn.cursor()
    for s in all_schemas:
        cur_pg.execute(f'CREATE TABLE IF NOT EXISTS {s}.{pg_table} {ddl_body}')
    pg_conn.commit()
    cur_pg.close()

    if not rows:
        return 0

    full_table = f'{schema}.{pg_table}'
    data_rows  = [list(r) for r in rows]

    # Deduplicar por PK
    if pk_cols:
        seen = {}
        for row in data_rows:
            try:
                key = tuple(row[pg_cols.index(pk)] for pk in pk_cols if pk in pg_cols)
            except (ValueError, IndexError):
                key = None
            seen[key if key is not None else id(row)] = row
        data_rows = list(seen.values())

    # Upsert
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

    cur_pg = pg_conn.cursor()
    try:
        extras.execute_values(cur_pg, upsert_q, [tuple(r) for r in data_rows], page_size=500)
        pg_conn.commit()
    except Exception:
        pg_conn.rollback()
        cur_pg = pg_conn.cursor()
        cur_pg.execute(f'DROP TABLE IF EXISTS {full_table} CASCADE')
        cur_pg.execute(f'CREATE TABLE {full_table} {ddl_body}')
        extras.execute_values(cur_pg, upsert_q, [tuple(r) for r in data_rows], page_size=500)
        pg_conn.commit()
    finally:
        cur_pg.close()

    return len(data_rows)

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    ts_inicio = datetime.now()
    stamp     = ts_inicio.strftime('%Y%m%d_%H%M%S')
    log_path  = os.path.join(LOGS_EXEC_DIR, f'articulos_maestros_{stamp}.txt')
    logger    = Logger(log_path)

    logger.log('=' * 60)
    logger.log('Articulos Maestros -- Procesos 024, 025 y 026')
    logger.log(f'Inicio: {ts_inicio.strftime("%Y-%m-%d %H:%M:%S")}')
    logger.log('=' * 60)

    if not SQL_DB or not SCHEMA:
        logger.log('Variables SQLSERVER_DB/SCHEMA no configuradas en .env.')
        logger.close()
        sys.exit(1)

    all_schemas = [SCHEMA]

    pg_conn = _connect_pg()
    _init_schemas(pg_conn)

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
        for tm in TABLAS:
            try:
                count = sync_master_table(sql_conn, pg_conn, all_schemas,
                                          SCHEMA, tm['sql_table'], tm['pg_table'])
                logger.log(f"  [{tm['proceso']}] OK -- {count} filas ({SCHEMA}.{tm['pg_table']})")
            except Exception as e:
                try:
                    pg_conn.rollback()
                except Exception:
                    pass
                logger.log(f"  [{tm['proceso']}] ERROR: {e}")
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
