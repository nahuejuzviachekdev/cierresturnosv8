@echo off
chcp 65001 > nul
echo ================================================
echo   ETL v8 - Cierres de Turno - Migracion por ID
echo ================================================
echo.
set /p ID="ID de cierre de turno: "
echo.
echo Iniciando migracion del cierre %ID%...
echo.
python -u "%~dp0etl_id.py" %ID%
echo.
echo Proceso finalizado.
pause
