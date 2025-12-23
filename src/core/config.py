import os
import sys
import toml
from pathlib import Path
from src.core.logger import get_logger

logger = get_logger(__name__)


class Settings:
    def __init__(self):
        # Detecta se está rodando como script ou como executável congelado
        if getattr(sys, 'frozen', False):
            # Caminho onde o arquivo .exe está localizado
            base_dir = Path(sys.executable).parent
        else:
            # Caminho do script em desenvolvimento
            base_dir = Path(__file__).parent.parent.parent

        config_path = base_dir / "config" / "settings.toml"

        if not config_path.exists():
            logger.error(
                f"ERRO: Arquivo de configuração não encontrado em: {config_path}")
            sys.exit(1)

        self._data = toml.load(config_path)

    def __getitem__(self, name: str):
        return self._data[name]
