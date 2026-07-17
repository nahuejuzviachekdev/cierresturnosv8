# -*- coding: utf-8 -*-
"""
Prueba de conectividad del tunel SSH hacia el Postgres destino (pg_tunnel.py).
No toca ninguna tabla del ETL: solo abre el tunel, hace SELECT version() y
cuenta cuantas tablas hay en el schema configurado.
Uso: python probar_tunnel.py
"""
import os
import sys
import threading
from datetime import datetime

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

# El Python portable (embeddable) no agrega la carpeta del script a sys.path.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pg_tunnel

SCHEMA = os.getenv('SCHEMA')

# ---------------------------------------------------------------------------
# Logger (mismo formato que el resto del proyecto)
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


def main():
    stamp    = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_path = os.path.join(LOGS_EXEC_DIR, f'probar_tunnel_{stamp}.txt')
    logger   = Logger(log_path)
    pg_tunnel.configurar_logging(logger.log)

    logger.log('=' * 60)
    logger.log('PRUEBA DE CONEXION AL TUNEL SSH -> PostgreSQL destino')
    logger.log('=' * 60)

    try:
        conn = pg_tunnel.conectar_pg()

        with conn.cursor() as cur:
            cur.execute('SELECT version()')
            logger.log(f'Postgres responde: {cur.fetchone()[0]}')

        if SCHEMA:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM information_schema.tables "
                    "WHERE table_schema = %s AND table_type = 'BASE TABLE'",
                    (SCHEMA,)
                )
                logger.log(f'Tablas en schema "{SCHEMA}": {cur.fetchone()[0]}')
        else:
            logger.log('[WARN] SCHEMA no configurado en .env, se omite el conteo de tablas.')

        conn.close()

        logger.log('=' * 60)
        logger.log('PRUEBA OK: tunel y conexion a PostgreSQL funcionando.')
        logger.log('=' * 60)
        pg_tunnel.cerrar_tunel()
        logger.close()
        sys.exit(0)

    except Exception as e:
        logger.log(f'PRUEBA FALLO: {e}')
        pg_tunnel.cerrar_tunel()
        logger.close()
        sys.exit(1)


if __name__ == '__main__':
    main()
