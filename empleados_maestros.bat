@echo off
chcp 65001 > nul
echo ================================================
echo   Empleados Maestros - Proceso 029
echo ================================================
echo.
python -u "%~dp0empleados_maestros.py"
echo.
echo Proceso finalizado.
pause
