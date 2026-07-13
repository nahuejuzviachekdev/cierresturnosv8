"""
Calcula la factura pendiente para todos los cierres de turno en un rango
de fechas (de la estacion configurada en .env) y guarda el resultado en
<schema>.facturas_pendientes.

Uso: python factura_pendiente_rango.py <fecha_desde> <fecha_hasta>
  Ejemplo: python factura_pendiente_rango.py 2026-04-01 2026-04-30
"""
import os
import sys
import decimal
from datetime import datetime

import pyodbc
import psycopg2

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
# Argumentos
# ---------------------------------------------------------------------------
if len(sys.argv) < 3:
    print("Uso: python factura_pendiente_rango.py <fecha_desde> <fecha_hasta>")
    print("  Ejemplo: python factura_pendiente_rango.py 2026-04-01 2026-04-30")
    sys.exit(1)

FECHA_DESDE = sys.argv[1]
FECHA_HASTA = sys.argv[2]

# ---------------------------------------------------------------------------
# Config
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

PG_HOST  = os.getenv('HOST')
PG_DB    = os.getenv('DATABASE')
PG_USER  = os.getenv('USERNAME')
PG_PASS  = os.getenv('PASSWORD')
PG_PORT  = os.getenv('PORT_1', '5432')

FULL_TABLE = f'{SCHEMA}.facturas_pendientes'

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def connect_sql():
    conn = pyodbc.connect(
        f'DRIVER={{SQL Server}};SERVER={SQL_HOST};DATABASE={SQL_DB};'
        f'UID={SQL_USER};PWD={SQL_PASS};TrustServerCertificate=True'
    )
    conn.timeout = 600
    return conn

def col_val(row, ci, name):
    idx = ci.get(name)
    if idx is None:
        return decimal.Decimal(0)
    v = row[idx]
    return decimal.Decimal(str(v)) if v is not None else decimal.Decimal(0)

def calcular_pendiente(sql_conn, id_cierre):
    """
    Ejecuta el SP y devuelve el importe pendiente total (solo positivos).
    Retorna None si no se encuentra la tabla @TablaResult.
    """
    cursor = sql_conn.cursor()
    cursor.execute(
        "SET NOCOUNT ON; EXECUTE Listado_EstadoAforadores "
        "@IdCierreTurno = ?, @CaldenON = '1', @Debug = '1'",
        (id_cierre,)
    )

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

    cursor.close()

    if result_rows is None:
        return None

    ci = {c: i for i, c in enumerate(result_cols)}
    total = decimal.Decimal(0)

    for row in result_rows:
        despachado   = col_val(row, ci, 'Despachado')
        fac_contado  = col_val(row, ci, 'FacturasContado')
        fac_cc       = col_val(row, ci, 'FacturasCuentaCorriente')
        remitos_cc   = col_val(row, ci, 'RemitosCuentaCorriente')
        remitos_cont = col_val(row, ci, 'RemitosContado')
        especiales   = col_val(row, ci, 'EspecialesCuentaCorriente')
        precio       = col_val(row, ci, 'Precio')

        litros_pend  = despachado - fac_contado - fac_cc - remitos_cc - remitos_cont - especiales
        if litros_pend > 0:
            total += litros_pend * precio

    return total

# ---------------------------------------------------------------------------
# Obtener IDs en el rango
# ---------------------------------------------------------------------------
print(f"\nConectando a SQL Server ({SQL_HOST} / {SQL_DB})...")
sql_conn = connect_sql()

f_desde = FECHA_DESDE + ' 00:00:00'
f_hasta = FECHA_HASTA + ' 23:59:59'

cur = sql_conn.cursor()
cur.execute(
    'SELECT ct.IdCierreTurno, ct.Fecha, ct.Numero '
    'FROM CierresTurno ct JOIN Cajas c ON c.IdCaja = ct.IdCaja '
    'WHERE c.IdEstacion = ? AND ct.Fecha >= ? AND ct.Fecha <= ? '
    'ORDER BY ct.Fecha ASC',
    (int(ID_ESTACION), f_desde, f_hasta)
)
cierres = cur.fetchall()
cur.close()

print(f"Rango      : {FECHA_DESDE} a {FECHA_HASTA} | Estacion: {ID_ESTACION} | BD: {SQL_DB}")
print(f"Cierres    : {len(cierres)} encontrados")

if not cierres:
    print("Nada que procesar.")
    sql_conn.close()
    sys.exit(0)

# ---------------------------------------------------------------------------
# Conectar PostgreSQL y asegurar tabla
# ---------------------------------------------------------------------------
print(f"Conectando a PostgreSQL ({FULL_TABLE})...")
pg_conn = psycopg2.connect(
    host=PG_HOST, database=PG_DB, user=PG_USER,
    password=PG_PASS, port=PG_PORT
)
pg_cur = pg_conn.cursor()
pg_cur.execute(f'CREATE SCHEMA IF NOT EXISTS {SCHEMA}')
pg_cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {FULL_TABLE} (
        id_cierre_turno   BIGINT PRIMARY KEY,
        importe_pendiente NUMERIC(18,2)
    )
""")
pg_conn.commit()

# ---------------------------------------------------------------------------
# Procesar cada cierre
# ---------------------------------------------------------------------------
print()
print(f"{'#':<5} {'IdCierre':>10}  {'Fecha':<20} {'N°':>6}  {'Importe Pendiente':>18}  Estado")
print("-" * 75)

ok = 0
err = 0
sin_tabla = 0
ts_inicio = datetime.now()

for i, (id_cierre, fecha, numero) in enumerate(cierres, 1):
    try:
        importe = calcular_pendiente(sql_conn, id_cierre)

        if importe is None:
            estado = 'SIN TABLA'
            sin_tabla += 1
            print(f"{i:<5} {id_cierre:>10}  {str(fecha):<20} {str(numero):>6}  {'---':>18}  {estado}")
            continue

        pg_cur.execute(f"""
            INSERT INTO {FULL_TABLE} (id_cierre_turno, importe_pendiente)
            VALUES (%s, %s)
            ON CONFLICT (id_cierre_turno)
            DO UPDATE SET importe_pendiente = EXCLUDED.importe_pendiente
        """, (id_cierre, importe))
        pg_conn.commit()

        estado = 'OK'
        ok += 1
        print(f"{i:<5} {id_cierre:>10}  {str(fecha):<20} {str(numero):>6}  {float(importe):>18.2f}  {estado}")

    except Exception as e:
        pg_conn.rollback()
        err += 1
        print(f"{i:<5} {id_cierre:>10}  {str(fecha):<20} {str(numero):>6}  {'---':>18}  ERROR: {e}")

# ---------------------------------------------------------------------------
# Resumen
# ---------------------------------------------------------------------------
pg_cur.close()
pg_conn.close()
sql_conn.close()

duracion = (datetime.now() - ts_inicio).total_seconds()
print()
print("=" * 75)
print(f"RESUMEN  |  OK: {ok}  |  Sin tabla: {sin_tabla}  |  Errores: {err}  |  Tiempo: {duracion:.1f}s")
print("=" * 75)
sys.exit(0 if err == 0 else 1)
