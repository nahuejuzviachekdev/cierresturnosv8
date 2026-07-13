"""
Extrae id_movimiento_fac de cierre_turno_listado_documentos para la estacion
configurada en .env, consulta el IdCliente en SQL Server y guarda en
remitos_clientes del schema correspondiente.
"""
import os
import sys
import pyodbc
import psycopg2
from datetime import datetime

# ---------------------------------------------------------------------------
# Cargar .env desde la carpeta padre
# ---------------------------------------------------------------------------
_BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_ENV_PATH = os.path.join(_BASE_DIR, '.env')
if os.path.exists(_ENV_PATH):
    with open(_ENV_PATH, 'r', encoding='latin-1') as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith('#') and '=' in _line:
                _k, _v = _line.split('=', 1)
                os.environ[_k.strip()] = _v.strip()

# ---------------------------------------------------------------------------
# Config PostgreSQL
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

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def log(msg=''):
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{ts}] {msg}" if msg else '', flush=True)

def connect_pg():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT,
        dbname=PG_DB, user=PG_USER, password=PG_PASS
    )

def connect_sql(db):
    cs = (
        f'DRIVER={{SQL Server}};SERVER={SQL_HOST};DATABASE={db};'
        f'UID={SQL_USER};PWD={SQL_PASS};TrustServerCertificate=True'
    )
    conn = pyodbc.connect(cs)
    conn.timeout = 600
    return conn

# ---------------------------------------------------------------------------
# Procesar la estacion configurada en .env
# ---------------------------------------------------------------------------
def procesar_estacion(sql_db, schema, pg):
    db_ref = f'[{sql_db}]'

    log(f"--- SQL Server: {sql_db} | Schema PG: {schema} ---")

    cur_pg = pg.cursor()
    sql    = connect_sql(sql_db)
    cur_sql = sql.cursor()

    try:
        # Crear tabla si no existe en el schema correspondiente
        cur_pg.execute(f"""
            CREATE TABLE IF NOT EXISTS {schema}.remitos_clientes (
                id_movimiento_fac BIGINT NOT NULL,
                id_cliente        INT,
                PRIMARY KEY (id_movimiento_fac)
            )
        """)
        pg.commit()
        log(f"Tabla {schema}.remitos_clientes verificada / creada.")

        # Verificar que la tabla fuente existe antes de leer
        cur_pg.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = %s AND table_name = 'cierre_turno_listado_documentos'
            )
        """, (schema,))
        if not cur_pg.fetchone()[0]:
            log(f"La tabla {schema}.cierre_turno_listado_documentos no existe, se omite.")
            return

        # Leer IDs desde Postgres
        cur_pg.execute(f"""
            SELECT DISTINCT id_movimiento_fac
            FROM {schema}.cierre_turno_listado_documentos
            WHERE id_movimiento_fac IS NOT NULL
        """)
        ids = [row[0] for row in cur_pg.fetchall()]
        log(f"IDs encontrados: {len(ids)}")

        if not ids:
            log("Sin IDs para procesar.")
            return

        # Consultar IdCliente en SQL Server e insertar en Postgres
        insertados  = 0
        sin_cliente = 0

        for id_mov in ids:
            cur_sql.execute(
                f"SELECT IdCliente FROM {db_ref}.dbo.MovimientosFac WHERE IdMovimientoFac = ?",
                (id_mov,)
            )
            row = cur_sql.fetchone()
            id_cliente = row[0] if row else None

            if id_cliente is None:
                sin_cliente += 1

            cur_pg.execute(f"""
                INSERT INTO {schema}.remitos_clientes (id_movimiento_fac, id_cliente)
                VALUES (%s, %s)
                ON CONFLICT (id_movimiento_fac) DO UPDATE
                    SET id_cliente = EXCLUDED.id_cliente
            """, (id_mov, id_cliente))
            insertados += 1

        pg.commit()
        log(f"Insertados / actualizados: {insertados} | Sin cliente en SQL Server: {sin_cliente}")

    except Exception as e:
        pg.rollback()
        log(f"ERROR: {e}")
        raise
    finally:
        cur_pg.close()
        cur_sql.close()
        sql.close()

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    if not SQL_DB or not SCHEMA:
        log("Error: faltan SQLSERVER_DB o SCHEMA en .env.")
        sys.exit(1)

    log("=== Inicio: extraer remitos_clientes ===")

    pg = connect_pg()
    pg.autocommit = False

    errores = []
    try:
        procesar_estacion(SQL_DB, SCHEMA, pg)
    except Exception as e:
        errores.append(str(e))

    pg.close()

    if errores:
        log("=== Finalizado con errores ===")
        for err in errores:
            log(f"  {err}")
        sys.exit(1)
    else:
        log("=== Fin exitoso ===")

if __name__ == '__main__':
    main()
