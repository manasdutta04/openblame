from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings
from rich.console import Console


class Settings(BaseSettings):
    openmetadata_host: str = "http://localhost:8585"
    openmetadata_jwt_token: str
    ollama_host: str = "http://localhost:11434"
    ollama_model: str = "llama3"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache(maxsize=1)
def get_config() -> Settings:
    try:
        return Settings()
    except Exception as error:
        Console().print(
            "[red]Config error:[/red] "
            f"{error}\nCopy .env.example to .env and fill in your values."
        )
        raise SystemExit(1) from error
