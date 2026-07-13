"""
Extrae la factura pendiente de un cierre de turno ejecutando
Listado_EstadoAforadores con @CaldenON='1' y @Debug='1' y leyendo
la 7ma tabla de resultados (@TablaResult interna del SP).

Guarda el resultado en PostgreSQL en la tabla
  <schema>.cierre_turno_aforadores_factura_pendiente

Uso:
  python consultar_factura_pendiente.py <id_cierre_turno>
  Ejemplo: python consultar_factura_pendiente.py 426785
"""
import os
import sys
import decimal
import re

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
# Argumentos
# ---------------------------------------------------------------------------
if len(sys.argv) < 2:
    print("Uso: python consultar_factura_pendiente.py <id_cierre_turno>")
    print("  Ejemplo: python consultar_factura_pendiente.py 426785")
    sys.exit(1)

try:
    ID_CIERRE = int(sys.argv[1])
except ValueError:
    print(f"Error: '{sys.argv[1]}' no es un ID válido.")
    sys.exit(1)

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

FULL_TABLE   = f'{SCHEMA}.facturas_pendientes'

# ---------------------------------------------------------------------------
# Conexión SQL Server
# ---------------------------------------------------------------------------
print(f"\nConectando a SQL Server ({SQL_HOST} / {SQL_DB})...")
sql_conn = pyodbc.connect(
    f'DRIVER={{SQL Server}};SERVER={SQL_HOST};DATABASE={SQL_DB};'
    f'UID={SQL_USER};PWD={SQL_PASS};TrustServerCertificate=True'
)
sql_conn.timeout = 600

# ---------------------------------------------------------------------------
# Verificar que el cierre pertenece a la estacion configurada
# ---------------------------------------------------------------------------
_cur_chk = sql_conn.cursor()
_cur_chk.execute(
    'SELECT ct.IdCierreTurno FROM CierresTurno ct JOIN Cajas c ON c.IdCaja = ct.IdCaja '
    'WHERE ct.IdCierreTurno = ? AND c.IdEstacion = ?',
    (ID_CIERRE, int(ID_ESTACION))
)
if not _cur_chk.fetchone():
    print(f"ERROR: El cierre {ID_CIERRE} no pertenece a la estacion {ID_ESTACION} en {SQL_DB}.")
    _cur_chk.close()
    sql_conn.close()
    sys.exit(1)
_cur_chk.close()

# ---------------------------------------------------------------------------
# Ejecutar SP y leer la 7ma tabla
# ---------------------------------------------------------------------------
print(f"Ejecutando Listado_EstadoAforadores para IdCierreTurno={ID_CIERRE}...")
cursor = sql_conn.cursor()
cursor.execute(
    "SET NOCOUNT ON; EXECUTE Listado_EstadoAforadores "
    "@IdCierreTurno = ?, @CaldenON = '1', @Debug = '1'",
    (ID_CIERRE,)
)

set_num     = 1
result_cols = None
result_rows = None

# Buscar el set cuya primera columna (Tabla) tenga valor '@TablaResult'
# o que contenga las columnas clave de facturación (Despachado + FacturasContado)
while True:
    if cursor.description:
        cols = [col[0] for col in cursor.description]
        rows = cursor.fetchall()
        is_target = False
        if rows and cols[0] == 'Tabla' and str(rows[0][0]).startswith('@TablaResult'):
            is_target = True
        elif 'Despachado' in cols and 'FacturasContado' in cols and 'RemitosCuentaCorriente' in cols:
            is_target = True
        if is_target:
            result_cols = cols
            result_rows = rows
            break
    if not cursor.nextset():
        break
    set_num += 1

cursor.close()
sql_conn.close()

if result_rows is None:
    print(f"\nERROR: No se encontro la tabla @TablaResult en los resultados del SP.")
    print("  Verifica que @CaldenON='1' este habilitado en el SP para este cierre.")
    sys.exit(1)

print(f"  @TablaResult encontrada en set {set_num}: {len(result_rows)} filas")

# ---------------------------------------------------------------------------
# Calcular pendientes
# ---------------------------------------------------------------------------
ci = {c: i for i, c in enumerate(result_cols)}

def col(row, name, default=decimal.Decimal(0)):
    idx = ci.get(name)
    if idx is None:
        return default
    v = row[idx]
    return decimal.Decimal(str(v)) if v is not None else default

rows_pendientes = []
for row in result_rows:
    despachado   = col(row, 'Despachado')
    fac_contado  = col(row, 'FacturasContado')
    fac_cc       = col(row, 'FacturasCuentaCorriente')
    remitos_cc   = col(row, 'RemitosCuentaCorriente')
    remitos_cont = col(row, 'RemitosContado')
    especiales   = col(row, 'EspecialesCuentaCorriente')
    precio       = col(row, 'Precio')

    # Total facturado/remitado por cualquier concepto
    total_fact   = fac_contado + fac_cc + remitos_cc + remitos_cont + especiales
    litros_pend  = despachado - total_fact
    importe_pend = litros_pend * precio

    if True:
        rows_pendientes.append({
            'id_cierre_turno':               ID_CIERRE,
            'id_articulo':                   row[ci['IdArticulo']] if 'IdArticulo' in ci else None,
            'descripcion_articulo':          row[ci['DescripcionArticulo']] if 'DescripcionArticulo' in ci else None,
            'despachado':                    despachado,
            'facturas_contado':              fac_contado,
            'facturas_cuenta_corriente':     fac_cc,
            'remitos_cuenta_corriente':      remitos_cc,
            'remitos_contado':               remitos_cont,
            'especiales_cuenta_corriente':   especiales,
            'precio':                        precio,
            'litros_pendientes':             litros_pend,
            'importe_pendiente':             importe_pend,
        })

# ---------------------------------------------------------------------------
# Mostrar resultado en pantalla
# ---------------------------------------------------------------------------
print()
print(f"Factura Pendiente — IdCierreTurno: {ID_CIERRE} | Estacion: {ID_ESTACION} | BD: {SQL_DB}")
print("=" * 100)

if not rows_pendientes:
    print("  Sin pendientes (todos los despachos estan facturados).")
else:
    hdr = (f"{'Art':<6}  {'Descripcion':<30} "
           f"{'Despachado':>11} {'Fac.Cont':>10} {'Fac.CC':>10} "
           f"{'Rem.CC':>9} {'Rem.Cont':>9} {'Espec':>8} "
           f"{'Pendiente':>11} {'Precio':>10} {'Importe':>14}")
    print(hdr)
    print("-" * 130)

    total_importe = decimal.Decimal(0)
    for r in rows_pendientes:
        marca = '(-)' if r['litros_pendientes'] < 0 else '   '
        print(
            f"{marca} "
            f"{str(r['id_articulo']):<6}  "
            f"{str(r['descripcion_articulo']):<30} "
            f"{float(r['despachado']):>11.2f} "
            f"{float(r['facturas_contado']):>10.2f} "
            f"{float(r['facturas_cuenta_corriente']):>10.2f} "
            f"{float(r['remitos_cuenta_corriente']):>9.2f} "
            f"{float(r['remitos_contado']):>9.2f} "
            f"{float(r['especiales_cuenta_corriente']):>8.2f} "
            f"{float(r['litros_pendientes']):>11.2f} "
            f"{float(r['precio']):>10.4f} "
            f"{float(r['importe_pendiente']):>14.2f}"
        )
        if r['litros_pendientes'] > 0:
            total_importe += r['importe_pendiente']

    print("=" * 130)
    print(f"{'TOTAL IMPORTE PENDIENTE':>116} {float(total_importe):>14.2f}")

# ---------------------------------------------------------------------------
# Guardar en PostgreSQL — tabla resumen con una fila por cierre
# ---------------------------------------------------------------------------
total_importe_pendiente = sum(
    r['importe_pendiente'] for r in rows_pendientes if r['litros_pendientes'] > 0
)

print(f"\nGuardando en PostgreSQL ({FULL_TABLE})...")
pg_conn = psycopg2.connect(
    host=PG_HOST, database=PG_DB, user=PG_USER,
    password=PG_PASS, port=PG_PORT
)
cur = pg_conn.cursor()

cur.execute(f'CREATE SCHEMA IF NOT EXISTS {SCHEMA}')
cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {FULL_TABLE} (
        id_cierre_turno  BIGINT PRIMARY KEY,
        importe_pendiente NUMERIC(18,2)
    )
""")
cur.execute(f"""
    INSERT INTO {FULL_TABLE} (id_cierre_turno, importe_pendiente)
    VALUES (%s, %s)
    ON CONFLICT (id_cierre_turno) DO UPDATE SET importe_pendiente = EXCLUDED.importe_pendiente
""", (ID_CIERRE, total_importe_pendiente))

pg_conn.commit()
cur.close()
pg_conn.close()

print(f"  id_cierre_turno={ID_CIERRE}, importe_pendiente={float(total_importe_pendiente):.2f}")
print("\nListo.")
