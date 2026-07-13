@echo off
chcp 65001 > nul
echo ================================================
echo   Bancos Maestros - Proceso 028
echo   Bancos - Estacion configurada en .env
echo ================================================
echo.
python -u "%~dp0bancos_maestros.py"
echo.
echo Proceso finalizado.
pause
