import multiprocessing
import sys
import asyncio
import uvicorn
from src.core.config import settings
from src.core.logger import get_logger

# Importe as funções/objetos diretamente dos seus arquivos
# Certifique-se de que seu main.py tem uma 'async def start()' ou similar
from main import main as start_collector
from api import app as fastapi_app

logger = get_logger(__name__)


def run_modbus_observer():
    """Executa o motor de coleta Modbus diretamente via função."""
    logger.info("Iniciando Coletor Modbus...")
    try:
        # Como o coletor é async, precisamos rodar o loop aqui
        asyncio.run(start_collector())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.error(f"Erro no Coletor Modbus: {e}")


def run_fastapi_api():
    """Executa o servidor de API usando Uvicorn programaticamente."""
    logger.info("Iniciando Servidor API (FastAPI)...")
    try:
        # Em vez de subprocess, chamamos o uvicorn.run diretamente
        uvicorn.run(
            fastapi_app,
            host=str(settings['api']['host']),
            port=settings['api']['port'],
            log_level="warning"
        )
    except Exception as e:
        logger.error(f"Erro ao iniciar o servidor API: {e}")


def main():
    logger.info("="*40)
    logger.info("SUPERVISÓRIO DE PROCESSOS - INICIANDO")
    logger.info("="*40)

    # Configuração dos processos usando funções Python diretas
    modbus_process = multiprocessing.Process(
        target=run_modbus_observer, name="ModbusWorker")
    api_process = multiprocessing.Process(
        target=run_fastapi_api, name="ApiWorker")

    try:
        modbus_process.start()
        api_process.start()

        logger.info(f" - Coletor Modbus iniciado [PID {modbus_process.pid}]")
        logger.info(
            f" - API Gateway iniciada [PID {api_process.pid}] na porta {settings['api']['port']}")

        # Mantém o script pai vivo
        modbus_process.join()
        api_process.join()

    except KeyboardInterrupt:
        logger.error("Encerrando sistema (Ctrl+C detectado)...")
        modbus_process.terminate()
        api_process.terminate()
        modbus_process.join()
        api_process.join()
        logger.info("Todos os serviços foram interrompidos.")


if __name__ == "__main__":
    # ESSA É A LINHA MAIS IMPORTANTE PARA WINDOWS EXE
    multiprocessing.freeze_support()

    main()
