import os
from datetime import datetime
from dotenv import load_dotenv

import pg_tunnel

load_dotenv(override=True)

SCHEMAS = [
    os.getenv("SCHEMA"),
]

CARPETA_LOG = os.path.join(os.path.dirname(__file__), "contar")


def contar_filas(cur, schema, tabla):
    cur.execute(f'SELECT COUNT(*) FROM "{schema}"."{tabla}"')
    return cur.fetchone()[0]


def obtener_tablas(cur, schema):
    cur.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s
          AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """,
        (schema,),
    )
    return [row[0] for row in cur.fetchall()]


def main():
    ahora = datetime.now()
    nombre_log = ahora.strftime("conteo_%Y%m%d_%H%M%S.log")

    os.makedirs(CARPETA_LOG, exist_ok=True)
    ruta_log = os.path.join(CARPETA_LOG, nombre_log)

    lineas = []
    lineas.append("=" * 60)
    lineas.append(f"  CONTEO DE REGISTROS - {ahora.strftime('%d/%m/%Y %H:%M:%S')}")
    lineas.append(f"  Base de datos: {os.getenv('DATABASE')} (via tunel SSH)")
    lineas.append("=" * 60)

    pg_tunnel.configurar_logging(lineas.append)

    try:
        conn = pg_tunnel.conectar_pg()
        cur = conn.cursor()

        for schema in SCHEMAS:
            if not schema:
                continue

            lineas.append(f"\nSCHEMA: {schema}")
            lineas.append("-" * 40)

            tablas = obtener_tablas(cur, schema)

            if not tablas:
                lineas.append("  (sin tablas)")
                continue

            total_schema = 0
            ancho_tabla = max(len(t) for t in tablas)

            for tabla in tablas:
                try:
                    filas = contar_filas(cur, schema, tabla)
                    total_schema += filas
                    lineas.append(f"  {tabla:<{ancho_tabla}}  {filas:>10,} filas")
                except Exception as e:
                    lineas.append(f"  {tabla:<{ancho_tabla}}  ERROR: {e}")

            lineas.append("-" * 40)
            lineas.append(f"  {'TOTAL':<{ancho_tabla}}  {total_schema:>10,} filas")

        cur.close()
        conn.close()

    except Exception as e:
        lineas.append(f"\nERROR DE CONEXION: {e}")

    pg_tunnel.cerrar_tunel()

    lineas.append("\n" + "=" * 60)

    contenido = "\n".join(lineas)
    with open(ruta_log, "w", encoding="utf-8") as f:
        f.write(contenido)

    print(contenido)
    print(f"\nLog guardado en: {ruta_log}")


if __name__ == "__main__":
    main()
