from __future__ import annotations

from functools import lru_cache

from pydantic import Field, ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict


class DataGhostConfig(BaseSettings):
    """Runtime settings loaded from environment or .env."""

    openmetadata_host: str = Field(alias="OPENMETADATA_HOST")
    openmetadata_jwt_token: str = Field(alias="OPENMETADATA_JWT_TOKEN")
    ollama_host: str = Field(alias="OLLAMA_HOST")
    ollama_model: str = Field(alias="OLLAMA_MODEL")
    http_timeout_seconds: float = 20.0

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
    )

    @property
    def om_auth_header(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.openmetadata_jwt_token}"}


def _format_missing_env_error(error: ValidationError) -> RuntimeError:
    missing_vars: list[str] = []
    for issue in error.errors():
        if issue.get("type") != "missing":
            continue
        loc = issue.get("loc", [])
        if not loc:
            continue
        missing_vars.append(str(loc[0]))
    names = ", ".join(sorted(set(missing_vars))) or "required variables"
    return RuntimeError(
        f"Missing required environment variables: {names}. "
        "Create a .env file from .env.example and retry."
    )


@lru_cache(maxsize=1)
def get_config() -> DataGhostConfig:
    """Load and cache DataGhost configuration."""

    try:
        return DataGhostConfig()
    except ValidationError as error:
        raise _format_missing_env_error(error) from error
