import asyncio
from src.infrastructure.database.repositories import PesagemRepository


async def main():
    # O initialize internamente faz: cls._pool = await get_pool()
    await PesagemRepository.initialize()

    # Agora o find() encontrará o pool preenchido
    r = await PesagemRepository.find()
    print(f"Resultados: {r}")

if __name__ == "__main__":
    asyncio.run(main())
