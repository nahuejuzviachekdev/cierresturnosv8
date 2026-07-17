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
echo  Consultar Factura Pendiente por Cierre
echo =============================================

if "%~1"=="" (
    set /p ID_CIERRE="ID de cierre de turno: "
) else (
    set ID_CIERRE=%~1
)

"%PYTHON_EXE%" consultar_factura_pendiente.py %ID_CIERRE%