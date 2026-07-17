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
echo   Estaciones Maestros - Proceso 027
echo   Estaciones - Estacion configurada en .env
echo ================================================
echo.
"%PYTHON_EXE%" -u "%~dp0estaciones_maestros.py"
echo.
echo Proceso finalizado.
pause
