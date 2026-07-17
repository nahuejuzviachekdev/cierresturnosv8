# -*- coding: utf-8 -*-
"""
Tunel SSH de dos saltos hacia el PostgreSQL remoto + conectar_pg() para abrir
conexiones psycopg2 sobre ese tunel. Modulo compartido por etl.py, etl_id.py
y los scripts *_maestros.py / contar_bd.py.

Cadena:
    Este servidor (origen)
      -(SSH, TUNNEL_KEY_FILE)-> VPS TUNNEL_VPS_HOST:TUNNEL_VPS_PORT (TUNNEL_VPS_USER)
         -> canal direct-tcpip -> 127.0.0.1:TUNNEL_REMOTE_PORT (VPS)  <- tunel inverso ya existente
              -> SSHD del servidor con Postgres
                   -(SSH, TUNNEL_KEY_FILE)-> TUNNEL_LOCAL_USER@servidor-destino
                        -> local-forward -> HOST:PORT_1 (Postgres del servidor destino)
    psycopg2 se conecta a 127.0.0.1:<puerto-efimero> en este servidor.

Toda la configuracion sale de .env (ninguna credencial hardcodeada):
  TUNNEL_VPS_HOST, TUNNEL_VPS_PORT, TUNNEL_VPS_USER, TUNNEL_LOCAL_USER,
  TUNNEL_REMOTE_PORT, TUNNEL_KEY_FILE, TUNNEL_MAX_INTENTOS,
  TUNNEL_ESPERA_REINTENTO, TUNNEL_TIMEOUT
Reutiliza HOST/PORT_1/DATABASE/USERNAME/PASSWORD ya existentes para los
datos del Postgres remoto (host/puerto tal como se ven desde el servidor
destino -- normalmente 127.0.0.1 -- y credenciales de esa base).

Uso tipico:
    import pg_tunnel
    pg_tunnel.configurar_logging(logger.log)   # opcional, para volcar al log propio
    conn = pg_tunnel.conectar_pg(schema)       # abre el tunel (si hace falta) + conecta
    ...
    pg_tunnel.cerrar_tunel()                   # al final de la corrida

Requiere: pip install paramiko sshtunnel psycopg2-binary
"""

import os
import time
import logging
import threading

import paramiko

# sshtunnel 0.4.0 referencia paramiko.DSSKey al armar su lista de tipos de
# llave, pero paramiko 5.x elimino DSSKey (DSA quedo obsoleto). Alias
# inofensivo para que no rompa el import; las llaves usadas son RSA/Ed25519.
if not hasattr(paramiko, "DSSKey"):
    paramiko.DSSKey = paramiko.RSAKey

import psycopg2
from sshtunnel import SSHTunnelForwarder

log = logging.getLogger('pg_tunnel')

_lock = threading.Lock()
_jump = None
_forwarder = None
_puerto_local = None


class _LogHandler(logging.Handler):
    """Reenvia los registros de logging (pg_tunnel/paramiko/sshtunnel) a la
    funcion de log del script llamador (p.ej. Logger.log de etl.py)."""

    def __init__(self, log_fn):
        super().__init__()
        self._log_fn = log_fn

    def emit(self, record):
        try:
            self._log_fn(f'[tunel] {self.format(record)}')
        except Exception:
            pass


def configurar_logging(log_fn):
    """Redirige los logs del tunel (paramiko, sshtunnel, este modulo) hacia
    log_fn(mensaje: str), para que queden en el mismo archivo de log del
    script que este corriendo (ejecucion_*.txt, etc.)."""
    handler = _LogHandler(log_fn)
    handler.setFormatter(logging.Formatter('%(message)s'))
    for nombre in ('pg_tunnel', 'paramiko', 'paramiko.transport', 'sshtunnel'):
        logger_obj = logging.getLogger(nombre)
        logger_obj.handlers = [h for h in logger_obj.handlers if not isinstance(h, _LogHandler)]
        logger_obj.addHandler(handler)
        logger_obj.setLevel(logging.INFO)
        logger_obj.propagate = False


def _cfg():
    return dict(
        vps_host=os.getenv('TUNNEL_VPS_HOST'),
        vps_port=int(os.getenv('TUNNEL_VPS_PORT', '22')),
        vps_user=os.getenv('TUNNEL_VPS_USER'),
        local_user=os.getenv('TUNNEL_LOCAL_USER'),
        remote_port=int(os.getenv('TUNNEL_REMOTE_PORT', '2222')),
        key_file=os.getenv('TUNNEL_KEY_FILE'),
        pg_host=os.getenv('HOST', '127.0.0.1'),
        pg_port=int(os.getenv('PORT_1', '5432')),
        pg_db=os.getenv('DATABASE'),
        pg_user=os.getenv('USERNAME'),
        pg_pass=os.getenv('PASSWORD'),
        max_intentos=int(os.getenv('TUNNEL_MAX_INTENTOS', '3')),
        espera=int(os.getenv('TUNNEL_ESPERA_REINTENTO', '15')),
        timeout=int(os.getenv('TUNNEL_TIMEOUT', '15')),
    )


def _validar_cfg(cfg):
    faltantes = [k for k in ('vps_host', 'vps_user', 'local_user', 'key_file',
                              'pg_db', 'pg_user', 'pg_pass')
                 if not cfg.get(k)]
    if faltantes:
        raise RuntimeError(f'Faltan variables de tunel en .env: {faltantes}')
    if not os.path.exists(cfg['key_file']):
        raise RuntimeError(f'No se encuentra la llave SSH del tunel: {cfg["key_file"]}')


def _abrir_jump(cfg):
    log.info('Conectando al VPS %s como %s ...', cfg['vps_host'], cfg['vps_user'])
    jump = paramiko.SSHClient()
    jump.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    jump.connect(cfg['vps_host'], port=cfg['vps_port'], username=cfg['vps_user'],
                 key_filename=cfg['key_file'], timeout=cfg['timeout'])
    canal = jump.get_transport().open_channel(
        'direct-tcpip', ('127.0.0.1', cfg['remote_port']), ('127.0.0.1', 0)
    )
    return jump, canal


def abrir_tunel():
    """Abre el tunel SSH de dos saltos (si no esta ya activo en este proceso)
    y devuelve el puerto local en el que quedo publicado Postgres."""
    global _jump, _forwarder, _puerto_local
    with _lock:
        if _forwarder is not None and _forwarder.is_active:
            return _puerto_local

        cfg = _cfg()
        _validar_cfg(cfg)

        ultimo_error = None
        for intento in range(1, cfg['max_intentos'] + 1):
            jump = forwarder = None
            try:
                jump, canal = _abrir_jump(cfg)
                log.info('Abriendo tunel local -> Postgres remoto ...')
                forwarder = SSHTunnelForwarder(
                    ('127.0.0.1', 22),
                    ssh_username=cfg['local_user'],
                    ssh_pkey=cfg['key_file'],
                    ssh_proxy=canal,
                    remote_bind_address=(cfg['pg_host'], cfg['pg_port']),
                    local_bind_address=('127.0.0.1', 0),
                    set_keepalive=30.0,
                )
                forwarder.start()
                _jump, _forwarder = jump, forwarder
                _puerto_local = forwarder.local_bind_port
                log.info('Tunel activo: 127.0.0.1:%d -> %s:%d (via servidor destino)',
                         _puerto_local, cfg['pg_host'], cfg['pg_port'])
                return _puerto_local
            except Exception as e:
                ultimo_error = e
                log.error('Intento %d/%d de apertura de tunel fallo: %s',
                          intento, cfg['max_intentos'], e)
                if forwarder is not None:
                    try:
                        forwarder.stop()
                    except Exception:
                        pass
                if jump is not None:
                    try:
                        jump.close()
                    except Exception:
                        pass
                if intento < cfg['max_intentos']:
                    log.info('Esperando %ds antes de reintentar ...', cfg['espera'])
                    time.sleep(cfg['espera'])

        raise ConnectionError(
            f'No se pudo abrir el tunel tras {cfg["max_intentos"]} intentos: {ultimo_error}'
        )


def conectar_pg(schema=None):
    """Devuelve una conexion psycopg2 al Postgres remoto, a traves del tunel
    SSH (lo abre si todavia no esta activo). Si se pasa 'schema', lo crea
    con CREATE SCHEMA IF NOT EXISTS antes de devolver la conexion."""
    puerto_local = abrir_tunel()
    cfg = _cfg()
    conn = psycopg2.connect(
        host='127.0.0.1', port=puerto_local, dbname=cfg['pg_db'],
        user=cfg['pg_user'], password=cfg['pg_pass'],
        connect_timeout=cfg['timeout'],
    )
    if schema:
        cur = conn.cursor()
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS {schema}')
        conn.commit()
        cur.close()
    return conn


def cerrar_tunel():
    """Cierra el forwarder y la conexion SSH al VPS. Seguro de llamar aunque
    el tunel nunca se haya abierto."""
    global _jump, _forwarder, _puerto_local
    with _lock:
        if _forwarder is not None:
            try:
                _forwarder.stop()
            except Exception:
                pass
        if _jump is not None:
            try:
                _jump.close()
            except Exception:
                pass
        if _forwarder is not None or _jump is not None:
            log.info('Tunel cerrado.')
        _jump = _forwarder = _puerto_local = None
