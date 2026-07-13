@echo off
chcp 65001 > nul

echo ================================================
echo   Actualizacion de Tablas Maestras (Fase 0)
echo ================================================
echo.

python -u "%~dp0actualizar_maestros.py"

echo.
echo ================================================
echo   Proceso finalizado.
echo ================================================
