@echo off
chcp 65001 > nul
echo ================================================
echo   Cajas Estaciones - Procesos 022 y 023
echo   Empleados y Cajas - Estacion configurada en .env
echo ================================================
echo.
python -u "%~dp0cajas_estaciones.py"
echo.
echo Proceso finalizado.
pause
