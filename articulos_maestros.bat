@echo off
chcp 65001 > nul
echo ================================================
echo   Articulos Maestros - Procesos 024, 025 y 026
echo   Familias, Grupos y Articulos - Estacion configurada en .env
echo ================================================
echo.
python -u "%~dp0articulos_maestros.py"
echo.
echo Proceso finalizado.
pause
