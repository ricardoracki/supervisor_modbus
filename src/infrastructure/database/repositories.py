from datetime import date, datetime
from src.infrastructure.database.connection import get_pool
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

        values = [(item.cw_id, item.weight, item.classification, item.timestamp)
                  for item in batch]

        query = "INSERT INTO pesagens (maquina_id, peso, classificacao, timestamp) VALUES ($1, $2, $3, $4)"

        try:
            async with cls._pool.acquire() as conn:
                await conn.executemany(query, values)
                logger.info(
                    f"Lote de {len(batch)} pesagens armazenado.")
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

        query += " ORDER BY timestamp DESC LIMIT 1000"

        try:
            async with cls._pool.acquire() as conn:
                rows = await conn.fetch(query, *args)
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Erro ao buscar pesagens: {e}")
            return []


class EventRepository:
    _pool = None

    @classmethod
    async def initialize(cls):
        if cls._pool is None:
            cls._pool = await get_pool()

        query = """
        CREATE TABLE IF NOT EXISTS events (
            id SERIAL PRIMARY KEY,
            maquina_id TEXT NOT NULL,
            evento INTEGER NOT NULL,
            reason INTEGER NOT NULL,
            timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events (timestamp DESC);
        """
        try:
            async with cls._pool.acquire() as conn:
                await conn.execute(query)
            logger.debug("Tabela de eventos inicializada via Async.")
        except Exception as e:
            logger.error(f"Erro ao inicializar banco: {e}")
            raise

    @classmethod
    async def insert_many(cls, batch: list[ModbusReadPayload]):
        if not batch or cls._pool is None:
            return

        values = [(item.cw_id, item.operation_type, item.reason, item.timestamp)
                  for item in batch]

        query = "INSERT INTO events (maquina_id, evento, reason, timestamp) VALUES ($1, $2, $3, $4)"

        try:
            async with cls._pool.acquire() as conn:
                await conn.executemany(query, values)
                logger.info(
                    f"Batch de {len(batch)} itens inserido com sucesso em events.")
        except Exception as e:
            logger.error(f"Erro ao inserir lote no banco: {e}")

    @classmethod
    async def find(cls, maquina_id: str | None = None, operation_type: int | None = None, reason: int | None = None, data_evento: date | None = None):
        """
        Busca eventos com filtros opcionais.
        Exemplo: find(maquina_id=1, operation_type=1, data_evento=date.today())
        """
        if cls._pool is None:
            return []

        # 1. Base da Query
        query = "SELECT maquina_id, evento, reason, timestamp FROM events WHERE 1=1"
        args = []
        counter = 1

        # 2. Construção Dinâmica dos Filtros
        if maquina_id is not None:
            query += f" AND maquina_id = ${counter}"
            args.append(maquina_id)
            counter += 1

        if operation_type is not None:
            query += f" AND evento = ${counter}"
            args.append(operation_type)
            counter += 1

        if reason is not None:
            query += f" AND reason = ${counter}"
            args.append(reason)
            counter += 1

        if data_evento is not None:
            # Filtra pelo dia (do início 00:00:00 até o fim 23:59:59)
            query += f" AND timestamp::date = ${counter}"
            args.append(data_evento)
            counter += 1

        query += " ORDER BY timestamp DESC LIMIT 1000"

        try:
            async with cls._pool.acquire() as conn:
                rows = await conn.fetch(query, *args)
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Erro ao buscar eventos: {e}")
            return []
