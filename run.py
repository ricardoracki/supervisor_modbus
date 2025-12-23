import multiprocessing
import sys
import subprocess
from src.core.config import Settings
from src.core.logger import get_logger

logger = get_logger(__name__)
settings = Settings()


def load_config():
    s = Settings()
    return s


def run_modbus_observer():
    """Executa o script principal do coletor Modbus."""
    logger.info("Iniciando Coletor Modbus...")
    # Usamos subprocess para garantir que o contexto do Poetry/Python seja mantido
    subprocess.run([sys.executable, "main.py"])


def run_fastapi_api():
    """Executa o servidor de API usando Uvicorn."""
    api_settings = settings['api']
    logger.info("Iniciando Servidor API (FastAPI)...")
    # Executa o uvicorn apontando para o arquivo api.py e a instância 'app'

    try:
        subprocess.run([
            sys.executable, "-m", "uvicorn",
            "api:app",
            "--host", str(api_settings['host']),
            "--port", str(api_settings['port']),
            "--log-level", "warning"
        ])
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.error(f"Erro ao iniciar o servidor API: {e}")


if __name__ == "__main__":
    # Configuração dos processos
    modbus_process = multiprocessing.Process(target=run_modbus_observer)
    api_process = multiprocessing.Process(target=run_fastapi_api)

    try:
        # Inicia ambos os processos
        modbus_process.start()
        api_process.start()

        logger.info("="*40)
        logger.info("SUPERVISÓRIO DE PROCESSOS")
        logger.info(f" - Coletor Modbus: PID {modbus_process.pid}")
        logger.info(
            f" - API Gateway: PID {api_process.pid} em {settings['api']['url']}")
        logger.info("="*40 + "\n")

        # Mantém o script pai vivo enquanto os filhos rodam
        modbus_process.join()
        api_process.join()

    except KeyboardInterrupt:
        print("\n [LOG] Encerrando sistema (Ctrl+C detectado)...")
        modbus_process.terminate()
        api_process.terminate()

        # Garante que os processos fecharam
        modbus_process.join()
        api_process.join()
        print(" [LOG] Todos os serviços foram interrompidos com sucesso.")
