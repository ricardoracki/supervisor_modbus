import asyncio
from src.core.buffer import Buffer
from src.infrastructure.database.repositories import PesagemRepository, EventRepository
from src.infrastructure.database.connection import get_pool, close_pool
from src.services.workers import weight_worker
from src.infrastructure.CW import CheckWeigher
from src.core.logger import get_logger

logger = get_logger(__name__)


async def shutdown(loop):
    """Fecha as tarefas e conexões de forma limpa."""
    logger.info("Iniciando processo de shutdown...")

    # 1. Captura todas as tarefas, exceto a atual (que é o próprio shutdown)
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]

    if tasks:
        logger.info(f"Cancelando {len(tasks)} tarefas pendentes...")
        for task in tasks:
            task.cancel()

        # 2. Aguarda o cancelamento de todas as tarefas
        # return_exceptions=True evita que o shutdown quebre se uma task demorar a cancelar
        await asyncio.gather(*tasks, return_exceptions=True)

    # 3. Fecha conexões críticas
    logger.info("Fechando pool de conexões com o banco...")
    await close_pool()

    # NOTA: Removido o loop.stop() para não conflitar com o asyncio.run()
    logger.info("Shutdown finalizado com sucesso.")


async def main():
    logger.info("Iniciando aplicação de pesagem...")

    # 1. Inicializa o Buffer (Fila em memória)
    # Recomendado maxsize para evitar estouro de memória se o banco cair
    buffer = Buffer(maxsize=10_000)

    # 2. Inicializa o Pool de Conexões e o Banco de Dados
    await get_pool()
    await PesagemRepository.initialize()
    await EventRepository.initialize()

    # 3. Cria as Tarefas (Tasks)
    # Task do Worker: Consome do Buffer -> Banco
    worker_task = asyncio.create_task(
        weight_worker(buffer),
        name="Worker-Database"
    )

    # Task do Reader: Modbus -> Buffer
    # Supondo que sua função modbus_reader receba o buffer
    # criar instancias do cw
    cw1 = CheckWeigher(name='CW1', ip_address='192.168.1.70',
                       port=502, cw_id='1')

    cw1.on(CheckWeigher.eventTypes.WEIGHT_READ, buffer.put)

    cws = [cw1]
    tasks = [asyncio.create_task(
        cw.listener(), name=f"{cw.name} listener") for cw in cws]

    # 4. Monitoramento e Graceful Shutdown
    loop = asyncio.get_running_loop()

    try:
        # Mantém o main vivo enquanto as tasks rodam
        await asyncio.gather(worker_task, *tasks)
    except asyncio.CancelledError:
        logger.info("Aplicação encerrada.")

    except Exception as e:
        logger.exception(e)

    finally:
        await shutdown(loop)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.exception(e)
