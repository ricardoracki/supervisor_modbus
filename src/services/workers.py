import asyncio
from src.core.buffer import Buffer
from src.infrastructure.database.repositories import PesagemRepository
from src.core.logger import get_logger

logger = get_logger(__name__)


async def weight_worker(buffer: Buffer):
    logger.info("Worker de pesagem iniciado.")

    while True:
        try:
            # O próprio get_batch agora é responsável por esperar (await)
            # se a fila estiver vazia, sem precisar de sleep manual.
            batch = await buffer.get_batch(batch_size=500)

            if not batch:
                continue

            # Envia o lote para o banco
            await PesagemRepository.insert_many(batch)

            logger.debug(
                f"Batch de {len(batch)} itens processado com sucesso.")

        except asyncio.CancelledError:
            logger.info("Worker sendo encerrado...")
            break
        except Exception as e:
            logger.error(f"Erro crítico no worker: {e}")
            # Pequena pausa apenas em caso de erro para evitar loop infinito de exceções
            await asyncio.sleep(1)
