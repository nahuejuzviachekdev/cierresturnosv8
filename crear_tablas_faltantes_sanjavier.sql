-- =============================================================================
-- Script: crear_tablas_faltantes_sanjavier.sql
-- Completa las 7 tablas que faltaban en sanjavier y no fueron incluidas
-- en el script anterior.
-- SEGURO: usa CREATE TABLE IF NOT EXISTS.
-- =============================================================================

CREATE TABLE IF NOT EXISTS sanjavier.cierre_turno_aforadores_diferencias (
    id_cierre_turno           BIGINT          NOT NULL,
    id_manguera               INTEGER         NOT NULL,
    numero_manguera           INTEGER,
    id_articulo               INTEGER,
    descripcion_articulo      TEXT,
    manguera                  TEXT,
    por_aforadores            NUMERIC(18,8),
    por_despachos             NUMERIC(18,8),
    diferencia_litros         NUMERIC(18,8),
    diferencia_importe        NUMERIC(18,8),
    precio                    NUMERIC(18,8),
    PRIMARY KEY (id_cierre_turno, id_manguera)
);

CREATE TABLE IF NOT EXISTS sanjavier.cierre_turno_listado_cheques (
    id_cheque_tercero         BIGINT          NOT NULL,
    fecha                     TIMESTAMP WITHOUT TIME ZONE,
    numero                    BIGINT,
    id_banco                  TEXT,
    nombre_banco              TEXT,
    localidad                 TEXT,
    emisor                    TEXT,
    c_u_i_t_emisor            TEXT,
    importe                   NUMERIC(18,8),
    id_cierre_turno           BIGINT,
    documento                 TEXT,
    PRIMARY KEY (id_cheque_tercero)
);

CREATE TABLE IF NOT EXISTS sanjavier.cierre_turno_listado_recibos (
    comprobante               TEXT            NOT NULL,
    razon_social              TEXT            NOT NULL,
    efectivo                  NUMERIC(18,8),
    cheques                   NUMERIC(18,8),
    tarjetas                  NUMERIC(18,8),
    retencion_i_i_b_b         NUMERIC(18,8),
    retencion_ganancias       NUMERIC(18,8),
    retencion_i_v_a           NUMERIC(18,8),
    interdepositos            NUMERIC(18,8),
    retencion_cargas_sociales NUMERIC(18,8),
    retencion_sellados        NUMERIC(18,8),
    total                     NUMERIC(18,8),
    retenciones_otras         NUMERIC(18,8),
    PRIMARY KEY (comprobante, razon_social)
);

CREATE TABLE IF NOT EXISTS sanjavier.cierre_turno_transferencias_cheques (
    fecha                     TIMESTAMP WITHOUT TIME ZONE,
    numero                    BIGINT          NOT NULL,
    localidad                 TEXT,
    emisor                    TEXT,
    cuit_emisor               TEXT,
    importe                   NUMERIC(18,8),
    cliente                   TEXT,
    codigo                    TEXT,
    nombre_banco              TEXT,
    comprobante               TEXT            NOT NULL,
    PRIMARY KEY (comprobante, numero)
);

CREATE TABLE IF NOT EXISTS sanjavier.cierre_turno_listado_documentos_extra_2 (
    id_movimientos_detalle_fac BIGINT,
    id_movimiento_fac          BIGINT,
    fecha_comprobante          TIMESTAMP WITHOUT TIME ZONE,
    punto_venta                BIGINT,
    id_tipo_movimiento         TEXT,
    minimo                     TEXT,
    maximo                     TEXT,
    razon_social               TEXT,
    total                      NUMERIC(18,8),
    importe_turno              NUMERIC(18,8),
    id_cierre_turno            BIGINT,
    fecha_turno                TIMESTAMP WITHOUT TIME ZONE,
    id_caja                    BIGINT,
    descripcion_caja           TEXT,
    es_remito                  BOOLEAN,
    remito                     TEXT,
    percepciones               NUMERIC(18,8),
    es_cuenta_corriente        BOOLEAN,
    neto                       NUMERIC(18,8),
    numero_documento           TEXT,
    vale_pago                  TEXT,
    numero_renglones           BIGINT,
    comp                       TEXT
);

CREATE TABLE IF NOT EXISTS sanjavier.cierre_turno_listado_documentos_extra_4 (
    id_movimientos_detalle_fac  BIGINT,
    id_movimiento_fac           BIGINT,
    fecha                       TIMESTAMP WITHOUT TIME ZONE,
    id_tipo_movimiento          TEXT,
    descripcion_tipo            TEXT,
    imputacion_stock            BIGINT,
    imputacion_cuenta_corriente BIGINT,
    imputacion_cierre_turno     BIGINT,
    punto_venta                 BIGINT,
    numero                      BIGINT,
    razon_social                TEXT,
    total                       NUMERIC(18,8),
    id_articulo                 BIGINT,
    codigo                      TEXT,
    descripcion_articulo        TEXT,
    id_grupo_articulo           BIGINT,
    cantidad                    NUMERIC(18,8),
    precio                      NUMERIC(18,8),
    id_cierre_turno             BIGINT,
    fecha_turno                 TIMESTAMP WITHOUT TIME ZONE,
    id_caja                     BIGINT,
    descripcion_caja            TEXT,
    es_cuenta_corriente         BOOLEAN,
    es_remito                   BOOLEAN,
    id_categoria_i_v_a          BIGINT,
    importe_renglon             NUMERIC(18,8),
    impuesto_interno            NUMERIC(18,8),
    tasas                       NUMERIC(18,8),
    i_v_a                       NUMERIC(18,8),
    id_articulo_vinculado       BIGINT,
    descripcion_grupo           TEXT,
    es_consumidor_final         BOOLEAN,
    neto                        NUMERIC(18,8),
    id_deposito                 BIGINT,
    remito                      TEXT,
    percepciones                NUMERIC(18,8),
    numero_documento            TEXT,
    manguera                    TEXT,
    vale_pago                   TEXT,
    importe_vale_pago           NUMERIC(18,8),
    tipo                        BIGINT,
    orden                       BIGINT,
    comprobante                 TEXT
);

CREATE TABLE IF NOT EXISTS sanjavier.cierre_turno_listado_documentos_extra_5 (
    id_movimientos_detalle_fac BIGINT,
    id_movimiento_fac          BIGINT,
    fecha_comprobante          TIMESTAMP WITHOUT TIME ZONE,
    punto_venta                BIGINT,
    id_tipo_movimiento         TEXT,
    minimo                     TEXT,
    maximo                     TEXT,
    razon_social               TEXT,
    total                      NUMERIC(18,8),
    importe_turno              NUMERIC(18,8),
    id_cierre_turno            BIGINT,
    fecha_turno                TIMESTAMP WITHOUT TIME ZONE,
    id_caja                    BIGINT,
    descripcion_caja           TEXT,
    es_remito                  BOOLEAN,
    remito                     TEXT,
    percepciones               NUMERIC(18,8),
    es_cuenta_corriente        BOOLEAN,
    neto                       NUMERIC(18,8),
    numero_documento           TEXT,
    vale_pago                  TEXT,
    numero_renglones           BIGINT,
    comp                       TEXT
);

-- Verificacion: debe mostrar 40 para los 3 esquemas
SELECT table_schema, COUNT(*) AS total_tablas
FROM information_schema.tables
WHERE table_schema IN ('public', 'litoral', 'sanjavier')
  AND table_type = 'BASE TABLE'
GROUP BY table_schema
ORDER BY table_schema;
