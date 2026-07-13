-- =============================================================================
-- Script: crear_tablas_faltantes.sql
-- Base de datos: calden_cierresturnos
-- Objetivo: Crear en litoral y sanjavier las tablas que faltan para que los
--           tres esquemas tengan la misma estructura que public.
-- Generado: 2026-05-22
-- SEGURO: usa CREATE TABLE IF NOT EXISTS, no modifica datos existentes.
-- =============================================================================


-- =============================================================================
-- TABLAS FALTANTES EN: litoral  (14 tablas)
-- =============================================================================

-- Tabla copiada desde public.cierre_turno_aforadores_diferencias
CREATE TABLE IF NOT EXISTS litoral.cierre_turno_aforadores_diferencias (
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

-- Tabla copiada desde public.cierre_turno_listado_cheques
CREATE TABLE IF NOT EXISTS litoral.cierre_turno_listado_cheques (
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

-- Tabla copiada desde public.cierre_turno_listado_recibos
CREATE TABLE IF NOT EXISTS litoral.cierre_turno_listado_recibos (
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

-- Tabla copiada desde public.cierre_turno_transferencias_cheques
CREATE TABLE IF NOT EXISTS litoral.cierre_turno_transferencias_cheques (
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

-- Tabla copiada desde public.cierre_turno_listado_documentos_extra_2
CREATE TABLE IF NOT EXISTS litoral.cierre_turno_listado_documentos_extra_2 (
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

-- Tabla copiada desde public.cierre_turno_listado_documentos_extra_4
CREATE TABLE IF NOT EXISTS litoral.cierre_turno_listado_documentos_extra_4 (
    id_movimientos_detalle_fac BIGINT,
    id_movimiento_fac          BIGINT,
    fecha                      TIMESTAMP WITHOUT TIME ZONE,
    id_tipo_movimiento         TEXT,
    descripcion_tipo           TEXT,
    imputacion_stock           BIGINT,
    imputacion_cuenta_corriente BIGINT,
    imputacion_cierre_turno    BIGINT,
    punto_venta                BIGINT,
    numero                     BIGINT,
    razon_social               TEXT,
    total                      NUMERIC(18,8),
    id_articulo                BIGINT,
    codigo                     TEXT,
    descripcion_articulo       TEXT,
    id_grupo_articulo          BIGINT,
    cantidad                   NUMERIC(18,8),
    precio                     NUMERIC(18,8),
    id_cierre_turno            BIGINT,
    fecha_turno                TIMESTAMP WITHOUT TIME ZONE,
    id_caja                    BIGINT,
    descripcion_caja           TEXT,
    es_cuenta_corriente        BOOLEAN,
    es_remito                  BOOLEAN,
    id_categoria_i_v_a         BIGINT,
    importe_renglon            NUMERIC(18,8),
    impuesto_interno           NUMERIC(18,8),
    tasas                      NUMERIC(18,8),
    i_v_a                      NUMERIC(18,8),
    id_articulo_vinculado      BIGINT,
    descripcion_grupo          TEXT,
    es_consumidor_final        BOOLEAN,
    neto                       NUMERIC(18,8),
    id_deposito                BIGINT,
    remito                     TEXT,
    percepciones               NUMERIC(18,8),
    numero_documento           TEXT,
    manguera                   TEXT,
    vale_pago                  TEXT,
    importe_vale_pago          NUMERIC(18,8),
    tipo                       BIGINT,
    orden                      BIGINT,
    comprobante                TEXT
);

-- Tabla copiada desde public.cierre_turno_listado_documentos_extra_5
CREATE TABLE IF NOT EXISTS litoral.cierre_turno_listado_documentos_extra_5 (
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

-- Tabla copiada desde sanjavier.cierre_turno_cuentacorriente_detalles
CREATE TABLE IF NOT EXISTS litoral.cierre_turno_cuentacorriente_detalles (
    id_cierre_turno           BIGINT          NOT NULL,
    operacion                 TEXT            NOT NULL,
    id_articulo               BIGINT          NOT NULL,
    codigo                    TEXT,
    descripcion_articulo      TEXT,
    cantidad                  NUMERIC(18,8),
    importe                   NUMERIC(18,8),
    PRIMARY KEY (id_cierre_turno, operacion, id_articulo)
);

-- Tabla copiada desde sanjavier.cierre_turno_despachos_detalle
CREATE TABLE IF NOT EXISTS litoral.cierre_turno_despachos_detalle (
    fecha                     TIMESTAMP WITHOUT TIME ZONE,
    numero_surtidor           BIGINT,
    numero_manguera           BIGINT,
    cara                      TEXT,
    codigo                    TEXT,
    descripcion_articulo      TEXT,
    cantidad                  NUMERIC(18,8),
    importe                   NUMERIC(18,8),
    precio_publico            NUMERIC(18,8),
    id_estacion               BIGINT,
    nombre                    TEXT,
    fecha_order               TEXT,
    manguera                  TEXT,
    facturado                 BOOLEAN,
    comprobante               TEXT,
    es_remito                 BOOLEAN,
    despacho_manual           TEXT,
    id_despacho               BIGINT          NOT NULL,
    precio_a_fecha            NUMERIC(18,8),
    precio_despacho           NUMERIC(18,8),
    id_articulo               BIGINT,
    id_cierre_turno           BIGINT,
    fecha_cierre_turno        TIMESTAMP WITHOUT TIME ZONE,
    id_empleado               BIGINT,
    empleado                  TEXT,
    PRIMARY KEY (id_despacho)
);

-- Tabla copiada desde sanjavier.cierre_turno_despachos_resumen
CREATE TABLE IF NOT EXISTS litoral.cierre_turno_despachos_resumen (
    codigo                    TEXT            NOT NULL,
    descripcion_articulo      TEXT,
    cantidad                  NUMERIC(18,8),
    facturado                 NUMERIC(18,8),
    no_facturado              NUMERIC(18,8),
    remitido                  NUMERIC(18,8),
    importe                   NUMERIC(18,8),
    orden                     BIGINT          NOT NULL,
    cantidad_despachos        BIGINT,
    id_cierre_turno           BIGINT          NOT NULL,
    PRIMARY KEY (id_cierre_turno, codigo, orden)
);

-- Tabla copiada desde sanjavier.cierre_turno_listado_despachos
CREATE TABLE IF NOT EXISTS litoral.cierre_turno_listado_despachos (
    fecha                     TIMESTAMP WITHOUT TIME ZONE,
    numero_surtidor           BIGINT,
    numero_manguera           BIGINT,
    cara                      TEXT,
    codigo                    TEXT,
    descripcion_articulo      TEXT,
    cantidad                  NUMERIC(18,8),
    importe                   NUMERIC(18,8),
    precio_publico            NUMERIC(18,8),
    id_estacion               BIGINT,
    nombre                    TEXT,
    fecha_order               TEXT,
    manguera                  TEXT,
    facturado                 BOOLEAN,
    comprobante               TEXT,
    es_remito                 BOOLEAN,
    despacho_manual           TEXT,
    id_despacho               BIGINT          NOT NULL,
    precio_a_fecha            NUMERIC(18,8),
    precio_despacho           NUMERIC(18,8),
    id_articulo               BIGINT,
    id_cierre_turno           BIGINT,
    fecha_cierre_turno        TIMESTAMP WITHOUT TIME ZONE,
    id_empleado               BIGINT,
    empleado                  TEXT,
    PRIMARY KEY (id_despacho)
);

-- Tabla copiada desde sanjavier.cierre_turno_listado_despachos_resumen
-- (sin PK — igual que en sanjavier)
CREATE TABLE IF NOT EXISTS litoral.cierre_turno_listado_despachos_resumen (
    codigo                    TEXT,
    descripcion_articulo      TEXT,
    cantidad                  NUMERIC(18,8),
    facturado                 NUMERIC(18,8),
    no_facturado              NUMERIC(18,8),
    remitido                  NUMERIC(18,8),
    importe                   NUMERIC(18,8),
    orden                     BIGINT,
    cantidad_despachos        BIGINT,
    id_cierre_turno           BIGINT
);

-- Tabla copiada desde sanjavier.cierre_turno_tanques_detalle
CREATE TABLE IF NOT EXISTS litoral.cierre_turno_tanques_detalle (
    id_cierre_detalle_tanques BIGINT          NOT NULL,
    id_cierre_turno           BIGINT,
    fecha                     TIMESTAMP WITHOUT TIME ZONE,
    descripcion_caja          TEXT,
    descripcion               TEXT,
    id_tanque                 BIGINT,
    numero_tanque             BIGINT,
    capacidad                 NUMERIC(18,8),
    id_articulo               BIGINT,
    descripcion_articulo      TEXT,
    descarga                  NUMERIC(18,8),
    factura_remito            TEXT,
    medicion                  NUMERIC(18,8),
    vendido                   NUMERIC(18,8),
    stock_actual              NUMERIC(18,8),
    vacio                     NUMERIC(18,8),
    stock_anterior            NUMERIC(18,8),
    diferencia                NUMERIC(18,8),
    porcentaje_diferencia     NUMERIC(18,8),
    venta_promedio            NUMERIC(18,8),
    PRIMARY KEY (id_cierre_detalle_tanques)
);

-- Tabla copiada desde sanjavier.cierre_turno_tanques_resumen
CREATE TABLE IF NOT EXISTS litoral.cierre_turno_tanques_resumen (
    id_cierre_turno           BIGINT          NOT NULL,
    id_articulo               BIGINT          NOT NULL,
    descripcion_articulo      TEXT,
    descarga                  NUMERIC(18,8),
    medicion                  NUMERIC(18,8),
    despachos                 NUMERIC(18,8),
    stock_actual              NUMERIC(18,8),
    stock_anterior            NUMERIC(18,8),
    vacio_actual              NUMERIC(18,8),
    venta_promedio            NUMERIC(18,8),
    vacio24                   NUMERIC(18,8),
    vacio48                   NUMERIC(18,8),
    vacio72                   NUMERIC(18,8),
    PRIMARY KEY (id_cierre_turno, id_articulo)
);


-- =============================================================================
-- TABLAS FALTANTES EN: sanjavier  (1 tabla)
-- =============================================================================

-- Tabla copiada desde public.cierre_turno_valores_efectivo_detalle
CREATE TABLE IF NOT EXISTS sanjavier.cierre_turno_valores_efectivo_detalle (
    descripcion               TEXT,
    multiplo                  NUMERIC(18,8),
    cantidad                  NUMERIC(18,8),
    importe                   NUMERIC(18,8),
    id_cierre_detalle_efectivo NUMERIC(18,8)  NOT NULL,
    id_billete_moneda         BIGINT,
    PRIMARY KEY (id_cierre_detalle_efectivo)
);


-- =============================================================================
-- Verificación final: contar tablas por esquema (debe ser 40 en los 3)
-- =============================================================================
SELECT table_schema, COUNT(*) AS total_tablas
FROM information_schema.tables
WHERE table_schema IN ('public', 'litoral', 'sanjavier')
  AND table_type = 'BASE TABLE'
GROUP BY table_schema
ORDER BY table_schema;
