@echo off
chcp 65001 > nul
echo ================================================
echo   ETL v8 - Cierres de Turno - Migracion Manual
echo ================================================
echo.
set /p DESDE="Fecha desde (YYYY-MM-DD): "
set /p HASTA="Fecha hasta  (YYYY-MM-DD): "
echo.
echo Iniciando migracion...
echo.
python -u "%~dp0etl.py" %DESDE% %HASTA%
echo.
echo Proceso finalizado.
pause
