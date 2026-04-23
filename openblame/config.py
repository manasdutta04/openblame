from __future__ import annotations

import subprocess
from functools import lru_cache

from pydantic_settings import BaseSettings
from rich.console import Console


def _detect_ollama_model() -> str:
    """Return first available Ollama model, fallback to qwen2.5:7b."""
    try:
        result = subprocess.run(
            ["ollama", "list"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        lines = result.stdout.strip().splitlines()
        # skip header line, get first model name
        for line in lines[1:]:
            name = line.split()[0] if line.split() else ""
            # skip embedding models
            if name and "embed" not in name.lower():
                return name
    except Exception:
        pass
    return "qwen2.5:7b"


class Settings(BaseSettings):
    openmetadata_host: str = "http://localhost:8585"
    openmetadata_jwt_token: str = ""
    ollama_host: str = "http://localhost:11434"
    ollama_model: str = ""

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}

    def get_model(self) -> str:
        """Return configured model or auto-detect from Ollama."""
        if self.ollama_model:
            return self.ollama_model
        return _detect_ollama_model()


@lru_cache
def get_config() -> Settings:
    try:
        return Settings()
    except Exception as e:
        Console().print(
            f"[red]Config error:[/red] {e}\n"
            "Copy .env.example to .env and fill in OPENMETADATA_JWT_TOKEN."
        )
        raise SystemExit(1)
