from fastapi import APIRouter, Query
from datetime import date
from src.infrastructure.database.repositories import PesagemRepository

router = APIRouter()


@router.get("/pesagens")
async def listar_pesagens(
    maquina_id: str = Query(None, description="ID da máquina"),
    data: date = Query(None, description="Data da pesagem (YYYY-MM-DD)"),
    classificacao: int = Query(None, description="Código da classificação")
):
    """Retorna as pesagens filtradas usando o repositório existente."""
    return await PesagemRepository.find(
        maquina_id=maquina_id,
        data_pesagem=data,
        classificacao=classificacao
    )


@router.get("/health")
async def health_check():
    return {"status": "online", "message": "Coletor e API operando"}
