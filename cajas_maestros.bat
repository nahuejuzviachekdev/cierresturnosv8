@echo off
chcp 65001 > nul
echo ================================================
echo   Cajas Maestros - Proceso 030
echo   (incluye sincronizacion de Estaciones)
echo ================================================
echo.
python -u "%~dp0cajas_maestros.py"
echo.
echo Proceso finalizado.
pause
