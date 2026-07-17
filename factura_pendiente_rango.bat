@echo off
chcp 65001 > nul

:: Preferir el Python del sistema; si no esta instalado, usar el portable (PYTHON_PATH en .env)
set "PYTHON_EXE=python"
where python >nul 2>nul
if not %errorlevel%==0 (
    for /f "usebackq tokens=1,* delims==" %%a in ("%~dp0.env") do (
        if "%%a"=="PYTHON_PATH" set "PYTHON_EXE=%%b"
    )
)

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

"%PYTHON_EXE%" factura_pendiente_rango.py %FECHA_DESDE% %FECHA_HASTA%
pause
