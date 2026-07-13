@echo off
chcp 65001 > nul
echo =============================================
echo  Consultar Factura Pendiente por Cierre
echo =============================================

if "%~1"=="" (
    set /p ID_CIERRE="ID de cierre de turno: "
) else (
    set ID_CIERRE=%~1
)

python consultar_factura_pendiente.py %ID_CIERRE%