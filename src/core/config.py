import sys
import toml
from pathlib import Path
from src.core.logger import get_logger
from src.config.settings import CONFIG_PATH

logger = get_logger(__name__)


class Settings:
    def __init__(self):

        config_path = CONFIG_PATH / "settings.toml"

        if not config_path.exists():
            logger.error(
                f"ERRO: Arquivo de configuração não encontrado em: {config_path}")
            sys.exit(1)

        self._data = toml.load(config_path)

    def __getitem__(self, name: str):
        return self._data[name]


settings = Settings()
