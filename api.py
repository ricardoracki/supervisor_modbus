from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from src.api.routes import router
from src.infrastructure.database.repositories import PesagemRepository
from src.infrastructure.database.connection import close_pool
from src.core.logger import get_logger

logger = get_logger(__name__)

app = FastAPI(
    title="Supervisor Modbus API",
    description="API para consulta de dados de pesagem industrial",
    version="1.0.0"
)

# Configuração de CORS para permitir que o Streamlit ou outros Frontends acessem a API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Em produção, substitua pelo IP do seu Frontend
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup_event():
    """Executado quando a API inicia."""
    logger.info("Iniciando API e conectando ao Pool de Dados...")
    # Inicializa o repositório (conecta ao banco)
    await PesagemRepository.initialize()


@app.on_event("shutdown")
async def shutdown_event():
    """Executado quando a API desliga."""
    logger.info("Encerrando API e fechando conexões...")
    await close_pool()

# Inclui as rotas definidas no arquivo routes.py
app.include_router(router, prefix="/api/v1")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
