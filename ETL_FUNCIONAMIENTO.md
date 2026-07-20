# ETL v8 — Funcionamiento completo

Este documento describe en detalle cómo funciona el ETL de cierres de turno: arquitectura, configuración, modos de ejecución, el mecanismo de sincronización incremental y todos los componentes involucrados.

---

## 1. Qué hace

Extrae "cierres de turno" (el resumen operativo de un turno de estación de servicio: ventas, despachos, tarjetas, cheques, cuenta corriente, tanques, aforadores, etc.) desde una base **SQL Server** (el sistema POS de la estación) y los migra a una base **PostgreSQL** (destino de reporting/consolidación), tabla por tabla, cierre por cierre.

## 2. Arquitectura: instancia de estación única

Esta copia del proyecto está dedicada a **una sola estación**. Todo lo que identifica la estación (conexión SQL Server, base destino, schema PG, filtro de estación) sale de un único archivo `.env`. Para desplegar en otra estación (San Javier, Litoral, etc.) se copia el repo entero y se cambia solo el `.env` — no se toca código.

Esto es distinto de versiones anteriores del proyecto, que iteraban sobre 3 estaciones en una sola corrida con variables sufijadas `_1/_2/_3`.

## 3. Configuración (`.env`)

El repo trae [`example.env`](example.env) como plantilla versionada (sin credenciales reales) — para desplegar una instancia nueva, copiarlo a `.env` y completar los valores reales. `.env` está en `.gitignore`, nunca se commitea.

```
# SQL Server origen
SQLSERVER_HOST_1=192.168.2.230
SQLSERVER_PORT_1=1433
SQLSERVER_USER_1=sa
SQLSERVER_PASSWORD_1=********
SQLSERVER_DB=BunkerPetroValle.Net
IDEstacion=4

# PostgreSQL destino (tal como se ve DESDE el servidor destino -- normalmente
# 127.0.0.1, ya que la conexion real llega por el tunel SSH, ver seccion 14)
HOST=127.0.0.1
DATABASE=AdelValleCentro
USERNAME=postgres
PASSWORD=********
PORT_1=5433
SCHEMA=petrovalle

# Tunel SSH hacia el Postgres destino (ver seccion 14)
TUNNEL_VPS_HOST=sistema.petrovalle.com.ar
TUNNEL_VPS_PORT=22
TUNNEL_VPS_USER=tunel
TUNNEL_LOCAL_USER=Administrador
TUNNEL_REMOTE_PORT=2222
TUNNEL_KEY_FILE=.\llave_remota
TUNNEL_MAX_INTENTOS=3
TUNNEL_ESPERA_REINTENTO=15
TUNNEL_TIMEOUT=15

# Python (ver seccion 9)
PYTHON_PATH=C:\bots\programas\python_portable\python.exe
```

- `IDEstacion` filtra, dentro de la base SQL Server (que puede tener cajas de varias estaciones), cuáles cajas/cierres pertenecen a esta instancia. El filtro se aplica vía `JOIN Cajas ON Cajas.IdCaja = CierresTurno.IdCaja WHERE Cajas.IdEstacion = ?`.
- `SCHEMA` es el único schema destino en Postgres (ej. `petrovalle`).
- `HOST`/`PORT_1` ya no son la conexión directa a Postgres: son el host/puerto de Postgres **tal como se ven desde el servidor destino** (la conexión real la abre el túnel SSH, ver sección 14).
- El `.env` se lee manualmente con encoding `latin-1` (no se usa `python-dotenv`, salvo en `contar_bd.py`) para soportar tildes/ñ sin problemas.
- No hay credenciales hardcodeadas en ningún `.py` — todo sale de `os.getenv()`.

## 4. Los dos scripts principales

### `etl.py` — corridas por rango de fechas o incrementales

Dos modos de uso:

```
python etl.py <fecha_desde> <fecha_hasta>     # backfill manual por rango
python etl.py --incremental                    # modo diario (ver seccion 7)
```

### `etl_id.py` — un cierre puntual

```
python etl_id.py <IdCierreTurno>
```

Útil para reprocesar un cierre específico a mano (por ejemplo, si falló en una corrida anterior, o para probar el pipeline con un ID conocido).

Ambos scripts comparten el mismo diseño interno (pool de conexiones, los mismos 20+ módulos, el mismo mecanismo de upsert) pero están duplicados como archivos independientes — no hay un módulo compartido entre ambos.

## 5. Los 20+ módulos de un cierre

Cada cierre de turno se compone de **21 módulos**, cada uno mapeado a un stored procedure (SP) de SQL Server (excepto el módulo 018, que usa una query directa — ver sección 6.3). Cada módulo puede producir uno o más result sets, y cada result set se guarda en su propia tabla PG:

| Módulo | SP / origen | Tabla(s) PG principales |
|---|---|---|
| 001 | `EncabezadoCierreTurno` | `cierre_turno_encabezado`, `cierre_turno_documentos`, `cierre_turno_empleados_fiscal` |
| 002 | `Listado_EstadoAforadores` | `cierre_turno_aforadores_detalle/resumen/diferencias` |
| 003 | `Listado_EstadoTanques` | `cierre_turno_tanques_detalle/resumen` |
| 004 | `Listado_CierresDocumentos` (Formato 1) | `cierre_turno_listado_documentos_cabecera/detalle` |
| 005 | `Listado_CierresDocumentos` (Formato 2) | ídem formato 1 |
| 006 | `Listado_CierresDocumentos` (Formato 3) | `cierre_turno_listado_documentos(_detalles)` |
| 007 | `Listado_Despachos` (TipoListado 4) | `cierre_turno_despachos_detalle/resumen` |
| 008 | `Listado_Despachos` (TipoListado 5) | `cierre_turno_listado_despachos(_resumen)` |
| 009 | `Listado_RecibosEnTurno` | `cierre_turno_listado_recibos` |
| 010 | `Listado_DocumentosExentos` | `cierre_turno_listado_documentos_exentos` |
| 011 | `Listado_CierresArticulos` (Formato 1) | `cierre_turno_listado_articulos` |
| 012 | `Listado_CierresArticulos` (Formato 2) | `cierre_turno_listado_articulos_grupos` |
| 013 | `Listado_TransferenciasEnTurno` | `cierre_turno_transferencias_caja/cheques/resumen` |
| 014 | `Listado_ChequesEnTurno` | `cierre_turno_listado_cheques` |
| 015 | `Listado_TarjetasEnTurno` | `cierre_turno_listado_tarjetas_cupones/lotes` |
| 016 | `Listado_ChequesResumen` | `cierre_turno_cheques_tarjetas_resumen` |
| 017 | `Listado_ValoresEnTurno` | `cierre_turno_valores_resumen/efectivo_detalle` |
| 018 | Query directa (ver 6.3) | `cierre_turno_cuentacorriente_totales/detalles` |
| 019 | `Listado_ResumenTurno` | `cierre_turno_resumen_grupos/totales/diferencias/contado` |
| 020 | `DocumentosSinImputacionACierrePorEmpleado` | `cierre_turno_documentos_sin_imputacion` |
| 021 | `Listado_EstadoAforadores` (modo factura) | `facturas_pendientes` (calculado, ver 6.4) |

Los 21 módulos de un mismo `IdCierreTurno` se ejecutan **en paralelo** con `ThreadPoolExecutor` (`MAX_WORKERS = 5`), reutilizando un pool de conexiones SQL Server + PG creado una sola vez por corrida (`ConnectionPool`).

## 6. Mecanismo de guardado (`sync_table`)

### 6.1 Upsert genérico

`sync_table()` es el corazón del pipeline: toma las columnas/filas devueltas por un SP y las guarda en PG con estas reglas:

1. **Snake_case automático**: `IdCierreTurno` → `id_cierre_turno`.
2. **Auto-filtro de columnas**: solo inserta las columnas que ya existen en la tabla PG destino (las columnas de texto descriptivo que se agregaron/quitaron con el tiempo se descartan silenciosamente).
3. **Creación de tabla bajo demanda**: `CREATE TABLE IF NOT EXISTS` con tipos inferidos del tipo real devuelto por pyodbc (`bool→BOOLEAN`, `int→BIGINT`, `Decimal→NUMERIC(18,8)`, `datetime→TIMESTAMP`, etc.).
4. **Upsert real**: `INSERT ... ON CONFLICT (pk) DO UPDATE SET ...` — reprocesar un cierre ya existente sobrescribe sus filas, no las duplica.
5. **Derivaciones**: algunos módulos no traen `id_caja` directo, se deriva de un patrón de texto (`"(123)- NOMBRE CAJA"`) — con fallback a lookup por nombre contra la tabla `cajas` si el patrón no matchea (ver 6.2b); análogo para `id_banco`.
6. **Inyección de `id_cierre_turno`**: algunos SPs no devuelven ese campo en cada fila — se agrega manualmente (`inject_id`).

### 6.2 Fallback de `id_caja` en `cierre_turno_encabezado`

Si el módulo 001 deja `id_caja` en `NULL` (pasa cuando el patrón de descripción no matchea), `_fix_id_caja()` hace una consulta directa a `CierresTurno.IdCaja` en SQL Server y completa el dato en PG después de correr los módulos. **Este fallback es exclusivo de `cierre_turno_encabezado`** — no aplica a ninguna otra tabla.

### 6.2b Derivación de `id_caja` en `cierre_turno_tanques_detalle` (bug corregido)

El módulo 003 (`Listado_EstadoTanques`) trae un campo `DescripcionCaja` que originalmente se resolvía a `id_caja` con un **lookup por nombre exacto** contra `cajas.descripcion` (mayúsculas, `.strip()`). Ese lookup fallaba sistemáticamente: `DescripcionCaja` viene con el mismo formato `"(NNN)- NOMBRE"` que ya usa `cierre_turno_encabezado` (ej. `"(14)- A. DEL VALLE CTRO - PLAYA"`), mientras que `cajas.descripcion` guarda el nombre limpio, sin el prefijo (`"A. DEL VALLE CTRO - PLAYA"`) — nunca coincidían como string exacto, y `id_caja` quedaba `NULL` en todas las filas de esa tabla (confirmado en producción con el cierre `912995`, estación 4, `AdelValleCentro`).

**Fix:** ahora se intenta primero extraer el ID directo del prefijo `"(NNN)- "` con la misma regex que usa `cierre_turno_encabezado`; solo si `descripcion_caja` no trae ese prefijo, cae al lookup por nombre contra `cajas` como fallback. Aplicado en `sync_table()` de `etl.py` y `etl_id.py` (duplicado en ambos, igual que el resto del mecanismo).

- No hay fallback posterior tipo `_fix_id_caja()` para esta tabla — si `DescripcionCaja` viniera sin el prefijo `(NNN)-` y el nombre tampoco matcheara contra `cajas` (typo, estación con formato distinto), `id_caja` seguiría quedando `NULL` sin aviso más allá del log de la corrida.
- Para verificar el fix en un cierre ya importado con `id_caja` en `NULL`: reprocesarlo con `etl_id.py <id>` (upsert, sobrescribe las filas existentes).

### 6.3 Módulo 018 — reemplazo de SP por query directa

El módulo de cuenta corriente originalmente llamaba al SP `Listado_TotalesCuentaCorrienteEnTurno`, pero se reemplazó por dos queries directas (`_handle_cuenta_corriente_query`) que hacen exactamente lo mismo **~4000x más rápido**, usando la función `dbo.Funcion_MovimientosEnTurno(?)` y la tabla `Despachos` directamente.

### 6.4 Módulo 021 — Facturas pendientes

No es un módulo real de SP con result set tabular: reutiliza el SP 002 (`Listado_EstadoAforadores` en modo debug) para extraer un result set intermedio (`@TablaResult`) y calcula manualmente el importe pendiente de facturación (litros despachados menos litros ya facturados/remitados, multiplicado por precio). El resultado va a `facturas_pendientes`.

## 7. Modo incremental (`--incremental`)

Este es el modo que usa `ejecucion_diaria.bat`. Reemplazó a un esquema anterior de "traer todo lo de las últimas 48hs por fecha", que tenía un bug de fondo: nunca volvía a mirar cierres viejos editados fuera de esa ventana fija.

El modo incremental detecta **nuevos** y **modificados** con criterios distintos:

### 7.1 Nuevos — por `IdCierreTurno`

```sql
SELECT ... FROM CierresTurno ct JOIN Cajas c ON c.IdCaja = ct.IdCaja
WHERE c.IdEstacion = ? AND ct.IdCierreTurno > @max_id_ya_en_pg
```

`IdCierreTurno` es una columna identity/secuencial en SQL Server: cualquier cierre creado después del último ya importado tiene, por definición, un ID más alto. No se necesita ventana de fechas ni traer el histórico completo — el corte es exacto y barato.

Si PG está vacío (primera corrida, o sin datos aún para esta estación), no hay `max_id` y se traen todos los cierres de la estación — pensado como bootstrap, no como uso normal (para una carga histórica real conviene `etl.py <desde> <hasta>` en vez de dejar que el modo incremental traiga todo).

### 7.2 Modificados — por `LastUpdated` vs `fecha_importacion`, acotado a 7 días

`CierresTurno.LastUpdated` en SQL Server se actualiza cada vez que el cierre se edita en el POS (aunque haya sido creado antes). Para detectar ediciones a cierres **ya importados**:

```sql
SELECT ... FROM CierresTurno ct JOIN Cajas c ON c.IdCaja = ct.IdCaja
WHERE c.IdEstacion = ? AND ct.Fecha >= (HOY - 7 dias)
```

De ese conjunto acotado, se compara — en Python, no en la query — el `LastUpdated` de SQL Server contra la columna **`fecha_importacion`** que este mismo ETL graba en PG (ver sección 8). Si `LastUpdated > fecha_importacion`, el cierre se marcó como modificado y se vuelve a procesar (mismo upsert, sobrescribe todo).

**Por qué la ventana de 7 días:** el bot corre varias veces al día. Es muy improbable que un cierre pase más de una semana sin recibir su versión final. Acotar a 7 días evita que, si por algún motivo PG quedara desactualizado, la corrida diaria intente reprocesar años de historial en vez de solo lo reciente. Si alguna vez hace falta recuperar algo más viejo, se usa `etl.py <desde> <hasta>` a mano.

**Por qué no un solo criterio para todo:** usar únicamente `LastUpdated` para detectar "nuevos" tiene un bug de fondo (confirmado empíricamente en pruebas): un cierre puede crearse después de otro pero tener un `LastUpdated` más viejo si no fue editado de nuevo, mientras que uno anterior sí. Separar nuevos (por ID) de modificados (por fecha+ventana) evita ese caso.

## 8. `fecha_importacion` — cómo se rastrean las ediciones sin duplicar el `LastUpdated` de origen

Se agregó una columna `fecha_importacion` (`TIMESTAMP`) a `cierre_turno_encabezado`, agregada en caliente con `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` (no rompe instalaciones existentes).

- **No es una copia de `LastUpdated` de SQL Server.** Es un dato propio: "cuándo nosotros trajimos este cierre por última vez".
- Se actualiza a `NOW()` después de procesar cualquier cierre — nuevo o modificado —, sea vía `etl.py --incremental`, `etl.py <desde> <hasta>` o `etl_id.py <id>`. Los tres caminos son consistentes entre sí.
- Es la referencia que usa el modo incremental para decidir si un cierre ya conocido cambió en origen (sección 7.2).
- **Caveat conocido:** `fecha_importacion` se actualiza sin condicionar a que los 21 módulos hayan corrido sin error (mismo criterio que el fallback de `id_caja`). Si un módulo puntual falla, el cierre igual queda marcado como "importado ahora" y no se reintenta automáticamente al otro día salvo que se edite de nuevo en origen.

## 9. Sincronización de tablas maestras (Fase 0)

Antes de procesar cierres, cada corrida sincroniza tablas de referencia desde SQL Server (upsert completo, no incremental):

`Estaciones`, `FamiliasArticulos`, `GruposArticulos`, `Articulos`, `Cajas` (filtrada por `IdEstacion`), `Empleados`, `Bancos`, `Clientes`, `TiposMovimiento`, `TarjetasCredito`.

También existen scripts standalone para correr esta sincronización por separado: `actualizar_maestros.py` (orquestador), y uno individual por tabla (`cajas_maestros.py`, `articulos_maestros.py`, `bancos_maestros.py`, `empleados_maestros.py`, `estaciones_maestros.py`, `cajas_estaciones.py`), cada uno con su `.bat`.

## 10. Logging

Cada corrida genera dos archivos con timestamp:

- `logs_ejecuciones/ejecucion_<timestamp>.txt`: log completo, línea por línea, de todo lo que hizo el proceso (thread-safe, se escribe a stdout y a archivo simultáneamente).
- `logs_ids/ids_<timestamp>.txt`: lista de los `IdCierreTurno` procesados en esa corrida (para auditoría rápida sin tener que parsear el log completo).

`contar_bd.py` / `contar_bd.bat` es una utilidad aparte para contar filas por tabla en el schema destino (chequeo de sanidad, no forma parte del pipeline regular).

Los mensajes del túnel SSH (sección 14) se redirigen al mismo log de cada corrida vía `pg_tunnel.configurar_logging()`, con el prefijo `[tunel]` — no generan un archivo aparte.

## 11. Scripts `.bat` y selección de Python

Todos los `.bat` (`ejecucion_diaria.bat`, `migrar.bat`, `migrar_por_id.bat`, los `*_maestros.bat`, etc.) resuelven qué intérprete de Python usar con esta prioridad:

1. **Python del sistema** (`where python` — si está en el PATH, se usa `python` directo).
2. Si no hay Python de sistema instalado, cae al **Python portable** definido en `.env` (`PYTHON_PATH`).

```bat
set "PYTHON_EXE=python"
where python >nul 2>nul
if not %errorlevel%==0 (
    for /f "usebackq tokens=1,* delims==" %%a in ("%~dp0.env") do (
        if "%%a"=="PYTHON_PATH" set "PYTHON_EXE=%%b"
    )
)
```

Antes de este cambio, siempre se usaba el Python portable sin verificar si había uno de sistema disponible.

### `ejecucion_diaria.bat`

Es el `.bat` pensado para tarea programada (corre varias veces al día). Llama a `python etl.py --incremental` — sin argumentos de fecha, sin prompt interactivo.

### `migrar.bat` / `migrar_por_id.bat`

Uso manual/interactivo: piden fecha desde/hasta (o un ID) por teclado y llaman a `etl.py`/`etl_id.py` en modo no incremental. Pensados para backfill histórico o para reprocesar algo puntual.

### `probar_tunnel.bat`

Prueba de conectividad del túnel SSH (sección 14) aislada del resto del pipeline: abre el túnel, corre `SELECT version()` y cuenta las tablas del schema configurado. No toca ninguna tabla del ETL. Pensado como primer chequeo antes de correr `etl.py`/`etl_id.py` contra un servidor nuevo.

## 12. Qué queda fuera de este ETL (no migrado a la arquitectura de estación única)

Estos scripts siguen usando variables `.env` con sufijo `_1/_2/_3` de versiones anteriores y **romperían** si se ejecutan con el `.env` actual de estación única. Tampoco pasan por el túnel SSH (sección 14) — conectan directo a Postgres con `psycopg2.connect(host=..., port=...)`:

- `factura_pendiente.bat` / `factura_pendiente_rango.bat` y sus `.py` (`consultar_factura_pendiente.py`, `factura_pendiente_rango.py`)
- `extraer_ids/extraer_remitos_clientes.bat` / `.py`

Si se necesitan, hay que migrarlos al mismo patrón de variables singulares (y, si corresponde, al túnel) que el resto del proyecto.

## 13. Resumen del flujo end-to-end (modo incremental)

```
ejecucion_diaria.bat
  └─ python etl.py --incremental
       ├─ CREATE SCHEMA IF NOT EXISTS (PG)
       ├─ ALTER TABLE ... ADD COLUMN IF NOT EXISTS fecha_importacion
       ├─ get_cierres_incremental()
       │    ├─ Nuevos: IdCierreTurno > MAX(id ya en PG)
       │    └─ Modificados: LastUpdated (SQL Server, ultimos 7 dias) > fecha_importacion (PG)
       ├─ Fase 0: sync de tablas maestras (Estaciones, Cajas, Articulos, etc.)
       ├─ Por cada cierre (nuevo o modificado), en el pool de 5 workers:
       │    ├─ Corre los 21 modulos en paralelo (SPs o queries directas)
       │    ├─ sync_table(): upsert en las tablas PG correspondientes
       │    │    └─ deriva id_caja / id_banco donde corresponda (ver 6.2b)
       │    ├─ _fix_id_caja(): completa id_caja en cierre_turno_encabezado si quedo NULL
       │    └─ _set_fecha_importacion(): UPDATE fecha_importacion = NOW()
       └─ Log de resumen (OK / errores / duracion) en logs_ejecuciones/
```

## 14. Túnel SSH hacia PostgreSQL (`pg_tunnel.py`)

Cuando el ETL corre en un servidor propio (con su propia base SQL Server) y el Postgres de destino está en **otro** servidor sin acceso directo, la conexión PG no es un `psycopg2.connect(host=..., port=...)` directo: pasa por un túnel SSH de dos saltos.

### 14.1 Cadena de conexión

```
Servidor que corre el ETL
  -(SSH, TUNNEL_KEY_FILE)-> VPS (TUNNEL_VPS_HOST:TUNNEL_VPS_PORT, usuario TUNNEL_VPS_USER)
     -> canal direct-tcpip -> 127.0.0.1:TUNNEL_REMOTE_PORT (VPS)   <- tunel inverso ya existente
          -> SSHD del servidor con Postgres
               -(SSH, TUNNEL_KEY_FILE)-> TUNNEL_LOCAL_USER@servidor-destino
                    -> local-forward -> HOST:PORT_1 (Postgres del servidor destino)
psycopg2 se conecta a 127.0.0.1:<puerto-efimero> en el servidor que corre el ETL.
```

El VPS y el servidor destino no se tocan: Postgres nunca sale de su loopback en el servidor destino.

### 14.2 Módulo `pg_tunnel.py`

Es un módulo compartido (no un script standalone) importado por `etl.py`, `etl_id.py`, los `*_maestros.py`, `contar_bd.py` y `probar_tunnel.py`. Expone tres funciones:

- **`abrir_tunel()`**: abre el túnel (jump SSH al VPS + `SSHTunnelForwarder` al servidor destino) si todavía no está activo en el proceso, y devuelve el puerto local efímero. Reintenta hasta `TUNNEL_MAX_INTENTOS` veces, con `TUNNEL_ESPERA_REINTENTO` segundos entre intentos. Si ya hay un túnel activo (`forwarder.is_active`), lo reutiliza en vez de abrir uno nuevo — así el pool de 5 conexiones de `etl.py`/`etl_id.py` comparte un solo túnel, multiplexado por `sshtunnel`.
- **`conectar_pg(schema=None)`**: abre el túnel si hace falta y devuelve una conexión `psycopg2` sobre `127.0.0.1:<puerto_local>`. Si se pasa `schema`, ejecuta `CREATE SCHEMA IF NOT EXISTS` antes de devolver la conexión (reemplaza lo que antes hacía cada script a mano).
- **`cerrar_tunel()`**: detiene el forwarder y cierra la conexión SSH al VPS, en ese orden. Seguro de llamar aunque el túnel nunca se haya abierto. Cada script lo llama en todos sus puntos de salida (éxito y error), igual que ya hacían con `logger.close()`.

### 14.3 Configuración — todo por `.env`, nada hardcodeado

| Variable | Qué es |
|---|---|
| `TUNNEL_VPS_HOST` / `TUNNEL_VPS_PORT` / `TUNNEL_VPS_USER` | Datos del primer salto SSH, al VPS |
| `TUNNEL_LOCAL_USER` | Usuario SSH del segundo salto, en el servidor destino |
| `TUNNEL_REMOTE_PORT` | Puerto en el loopback del VPS que el túnel inverso ya existente mapea al SSHD del servidor destino |
| `TUNNEL_KEY_FILE` | Ruta a la llave privada SSH (misma llave para ambos saltos) |
| `TUNNEL_MAX_INTENTOS` / `TUNNEL_ESPERA_REINTENTO` / `TUNNEL_TIMEOUT` | Reintentos y timeouts |
| `HOST` / `PORT_1` | Host/puerto de Postgres tal como se ven **desde el servidor destino** (normalmente `127.0.0.1`) |
| `DATABASE` / `USERNAME` / `PASSWORD` | Credenciales de la base Postgres remota — se reutilizan las mismas variables que ya existían, sin duplicar |

La llave privada (`llave_remota`, referenciada por `TUNNEL_KEY_FILE`) vive junto al proyecto pero está excluida en `.gitignore` — nunca se commitea, igual que `.env`.

### 14.3b `sys.path` con Python portable (bug corregido)

El Python portable/embeddable (`PYTHON_PATH` en `.env`) **no agrega automáticamente la carpeta del script a `sys.path`** — a diferencia de un Python de sistema normal. Si el `.bat` cae al portable (porque no hay `python` en el `PATH` del servidor), `import pg_tunnel` fallaba con `ModuleNotFoundError` aunque `pg_tunnel.py` estuviera en la misma carpeta que el script (confirmado en producción, servidor `C:\bots\cierresturnosv8\`).

**Fix:** todos los scripts que importan `pg_tunnel` (`etl.py`, `etl_id.py`, los `*_maestros.py`, `contar_bd.py`, `probar_tunnel.py`) agregan su propia carpeta a `sys.path` justo antes del import:

```python
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pg_tunnel
```

Mismo patrón que ya usaba el proyecto de referencia del túnel (`EtlPostgres`) para el mismo problema.

### 14.4 Logging del túnel

`pg_tunnel.configurar_logging(log_fn)` redirige los logs internos de `paramiko`, `sshtunnel` y del propio `pg_tunnel` hacia la función de log del script que lo llama (con prefijo `[tunel]`), para que todo el proceso de conexión (apertura de saltos SSH, forwarder, reintentos) quede en el mismo archivo de log de la corrida (`logs_ejecuciones/...`), no en un log aparte.

### 14.5 Prueba de conectividad aislada

`probar_tunnel.bat` / `probar_tunnel.py` (sección 11) valida solo el túnel — abre la conexión, corre `SELECT version()` y cuenta tablas del schema — sin tocar ninguna tabla del ETL. Es el primer paso recomendado antes de correr `etl.py`/`etl_id.py` contra un servidor nuevo.

### 14.6 Dependencias

`paramiko`, `sshtunnel` y `python-dotenv` (este último solo lo usa `contar_bd.py`), sumadas a `pyodbc`/`psycopg2-binary` que ya existían — todas en `requirements.txt`.

### 14.7 Estado verificado

Conectividad probada en vivo contra el destino real: túnel VPS (`sistema.petrovalle.com.ar`) → servidor con Postgres → `AdelValleCentro`, Postgres 15.17, schema `petrovalle` con 51 tablas. `probar_tunnel.bat` y la Fase 0 de maestros (`cajas` sincronizada, 4 filas para la estación 4) confirmados funcionando end-to-end.
