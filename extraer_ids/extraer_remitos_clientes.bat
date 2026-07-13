@echo off
chcp 65001 > nul
cd /d "%~dp0\.."
python extraer_ids\extraer_remitos_clientes.py

