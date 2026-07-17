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

echo ================================================
echo   Actualizacion de Tablas Maestras (Fase 0)
echo ================================================
echo.

"%PYTHON_EXE%" -u "%~dp0actualizar_maestros.py"

echo.
echo ================================================
echo   Proceso finalizado.
echo ================================================
