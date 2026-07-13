@echo off
chcp 65001 > nul
echo =============================================
echo  Factura Pendiente por Rango de Fechas
echo =============================================

if "%~1"=="" (
    set /p FECHA_DESDE="Fecha desde (YYYY-MM-DD): "
) else (
    set FECHA_DESDE=%~1
)

if "%~2"=="" (
    set /p FECHA_HASTA="Fecha hasta  (YYYY-MM-DD): "
) else (
    set FECHA_HASTA=%~2
)

python factura_pendiente_rango.py %FECHA_DESDE% %FECHA_HASTA%
pause
