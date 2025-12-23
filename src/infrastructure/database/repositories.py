from datetime import date, datetime
from .connection import get_pool
from src.core.logger import get_logger

from src.core.types.ModbusReadPayload import ModbusReadPayload

logger = get_logger(__name__)


class PesagemRepository:
    _pool = None  # Armazenamos o pool aqui

    @classmethod
    async def initialize(cls):
        """Inicializa o pool de conexões e as tabelas."""
        if cls._pool is None:
            cls._pool = await get_pool()

        query = """
        CREATE TABLE IF NOT EXISTS pesagens (
            id SERIAL PRIMARY KEY,
            maquina_id TEXT NOT NULL,
            peso INTEGER NOT NULL,
            classificacao INTEGER NOT NULL DEFAULT 0,
            timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_pesagens_timestamp ON pesagens (timestamp DESC);
        """
        try:
            async with cls._pool.acquire() as conn:
                await conn.execute(query)
            logger.debug("Tabela de pesagens inicializada via Async.")
        except Exception as e:
            logger.error(f"Erro ao inicializar banco: {e}")
            raise

    @classmethod
    async def insert_many(cls, batch: list[ModbusReadPayload]):
        if not batch or cls._pool is None:
            return

        # Preparação ultra-rápida dos dados
        values = [(item.cw_id, item.weight, item.classification, item.timestamp)
                  for item in batch]

        # No asyncpg, usa-se $1, $2, $3 para placeholders
        query = "INSERT INTO pesagens (maquina_id, peso, classificacao, timestamp) VALUES ($1, $2, $3, $4)"

        try:
            async with cls._pool.acquire() as conn:
                # executemany do asyncpg é extremamente otimizado
                await conn.executemany(query, values)
                logger.info(
                    f"Batch de {len(batch)} itens inserido com sucesso em pesagens.")
        except Exception as e:
            logger.error(f"Erro ao inserir lote no banco: {e}")

    @classmethod
    async def find(cls, maquina_id: str | None = None, data_pesagem: date | None = None, classificacao: int | None = None):
        """
        Busca pesagens com filtros opcionais.
        Exemplo: find(maquina_id=1, data_pesagem=date.today())
        """
        if cls._pool is None:
            return []

        # 1. Base da Query
        query = "SELECT maquina_id, peso, classificacao, timestamp FROM pesagens WHERE 1=1"
        args = []
        counter = 1

        # 2. Construção Dinâmica dos Filtros
        if maquina_id is not None:
            query += f" AND maquina_id = ${counter}"
            args.append(maquina_id)
            counter += 1

        if classificacao is not None:
            query += f" AND classificacao = ${counter}"
            args.append(classificacao)
            counter += 1

        if data_pesagem is not None:
            # Filtra pelo dia (do início 00:00:00 até o fim 23:59:59)
            query += f" AND timestamp::date = ${counter}"
            args.append(data_pesagem)
            counter += 1

        # 3. Ordenação (Sempre importante para séries temporais)
        query += " ORDER BY timestamp DESC LIMIT 1000"

        try:
            async with cls._pool.acquire() as conn:
                # fetch() retorna Records do asyncpg que funcionam como dicionários
                rows = await conn.fetch(query, *args)

                # Opcional: converter para lista de dicionários puros
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Erro ao buscar pesagens: {e}")
            return []
