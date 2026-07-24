import os
from unittest import mock

from coreason_etl_hpo.config import PipelineConfig


def test_pipeline_config_defaults() -> None:
    # Test default values without any environment variables
    with mock.patch.dict(os.environ, clear=True):
        config = PipelineConfig(_env_file=None) # ADDED: _env_file=None
        assert config.pghost == "localhost"
        assert config.pgport == 5432
        assert config.pguser == "postgres"
        assert config.pgpassword == "postgres"
        assert config.pgdatabase == "coreason"
        assert config.app_env == "development"
        assert config.postgres_uri == "postgresql://postgres:postgres@localhost:5432/coreason"

def test_pipeline_config_env_override() -> None:
    # Test overriding defaults with environment variables
    test_env = {
        "PGHOST": "db.example.com",
        "PGPORT": "5433",
        "PGUSER": "admin",
        "PGPASSWORD": "securepassword123",
        "PGDATABASE": "prod_db",
        "APP_ENV": "production",
    }

    with mock.patch.dict(os.environ, test_env, clear=True):
        config = PipelineConfig()
        assert config.pghost == "db.example.com"
        assert config.pgport == 5433
        assert config.pguser == "admin"
        assert config.pgpassword == "securepassword123"
        assert config.pgdatabase == "prod_db"
        assert config.app_env == "production"
        assert config.postgres_uri == "postgresql://admin:securepassword123@db.example.com:5433/prod_db"
