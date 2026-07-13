@echo off
chcp 65001 > nul
echo ================================================
echo   Estaciones Maestros - Proceso 027
echo   Estaciones - Estacion configurada en .env
echo ================================================
echo.
python -u "%~dp0estaciones_maestros.py"
echo.
echo Proceso finalizado.
pause
