@echo off
chcp 65001 > nul

:: Calcular fecha de anteayer (48hs atras)
for /f %%i in ('powershell -NoProfile -Command "(Get-Date).AddDays(-2).ToString('yyyy-MM-dd')"') do set ANTEAYER=%%i

echo ================================================
echo   ETL v8 - Ejecucion Diaria - %ANTEAYER%
echo ================================================
echo.

python -u "%~dp0etl.py" %ANTEAYER% %ANTEAYER%

echo.
echo ================================================
echo   Ejecucion diaria completada.
echo ================================================
