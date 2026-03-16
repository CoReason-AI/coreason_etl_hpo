from pydantic_settings import BaseSettings, SettingsConfigDict


class PipelineConfig(BaseSettings):
    """
    Configuration model for the pipeline.
    """

    # PostgreSQL configuration bounds
    pghost: str = "localhost"
    pgport: int = 5432
    pguser: str = "postgres"
    pgpassword: str = "postgres"
    pgdatabase: str = "coreason"

    # Environment level
    app_env: str = "development"

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    @property
    def postgres_uri(self) -> str:
        """Retrieves the full canonical connection manifest to PostgreSQL."""
        return f"postgresql://{self.pguser}:{self.pgpassword}@{self.pghost}:{self.pgport}/{self.pgdatabase}"
