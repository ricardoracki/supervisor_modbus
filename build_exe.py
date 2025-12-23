import PyInstaller.__main__
import os

# Define o nome do executável final
exe_name = "SupervisorIndustrial"

PyInstaller.__main__.run([
    'run.py',                         # Seu script orquestrador
    '--name=%s' % exe_name,
    '--onefile',                      # Gera apenas um arquivo .exe
    '--console',                      # Mantém o console aberto para ver os logs
    # Inclui o TOML (Semicolon ';' é para Windows)
    '--add-data=config/settings.toml;config',
    '--collect-all=fastapi',          # Garante que as rotas sejam encontradas
    '--collect-all=uvicorn',
])
