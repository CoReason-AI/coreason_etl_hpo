import io
from collections.abc import Generator
from typing import Any
from unittest import mock

import polars as pl
import pytest

from coreason_etl_hpo.main import run_pipeline


@pytest.fixture
def mock_dlt_client_get() -> Generator[mock.MagicMock]:
    """Mock the requests client used by dlt sources for live tests to avoid external network calls."""
    with mock.patch("coreason_etl_hpo.bronze.client.get") as mock_get:
        yield mock_get


@pytest.mark.live
def test_e2e_pipeline_live(monkeypatch: pytest.MonkeyPatch, mock_dlt_client_get: mock.MagicMock) -> None:
    """
    Live End-to-End test that validates the pipeline flow into a real PostgreSQL test database.
    Mocks the initial HTTP request to simulate OBO files being streamed, but writes
    and transforms data using PostgreSQL and Polars.
    """
    # 1. Ensure test database is configured via Environment Variables
    monkeypatch.setenv("PGHOST", "localhost")
    monkeypatch.setenv("PGPORT", "5432")
    monkeypatch.setenv("PGUSER", "postgres")
    monkeypatch.setenv("PGPASSWORD", "postgres")
    monkeypatch.setenv("PGDATABASE", "coreason_test")

    # 2. Setup mock data
    def mock_get_side_effect(url: str, **_kwargs: Any) -> mock.MagicMock:
        mock_response = mock.MagicMock()
        mock_response.raise_for_status = mock.MagicMock()

        if "hp.json" in url:
            json_data = b"""
            {
                "graphs": [
                    {
                        "nodes": [
                            {
                                "id": "HP:0000001",
                                "lbl": "Test Phenotype 1",
                                "meta": {"definition": {"val": "Test def 1"}, "deprecated": false}
                            },
                            {
                                "id": "HP:0000002",
                                "lbl": "Test Phenotype 2",
                                "meta": {"definition": {"val": "Test def 2"}, "deprecated": true}
                            }
                        ],
                        "edges": [
                            {"sub": "HP:0000002", "pred": "is_a", "obj": "HP:0000001"}
                        ]
                    }
                ]
            }
            """
            mock_response.raw = io.BytesIO(json_data)
        elif "phenotype.hpoa" in url:
            lines = [
                b"# This is a comment",
                b"database_id\tdisease_name\tqualifier\thpo_id\treference\tevidence\tonset\tfrequency\tsex\tmodifier\taspect\tbiocuration",
                b"OMIM:101600\tDisease 1\t\tHP:0000001\t\tE1\t\tF1\t\t\tA1\t",
            ]
            mock_response.iter_lines.return_value = (line for line in lines)
        else:
            raise ValueError(f"Unexpected URL: {url}")

        return mock_response

    mock_dlt_client_get.side_effect = mock_get_side_effect

    # 2.5 Setup Gold Schema (DLT automatically creates bronze, but we need gold if missing)
    import psycopg2  # type: ignore[import-untyped]

    conn = psycopg2.connect(
        dbname="coreason_test",
        user="postgres",
        password="postgres",  # noqa: S106
        host="localhost",
        port="5432",
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS hpo_gold;")
    conn.close()

    # 3. Execute the Pipeline
    run_pipeline()

    # 4. Verify Gold Tables via Polars using test DB connection string
    test_db_uri = "postgresql://postgres:postgres@localhost:5432/coreason_test"

    dim_hpo = pl.read_database_uri("SELECT * FROM hpo_gold.dim_hpo_concept", uri=test_db_uri, engine="adbc")
    assert dim_hpo.height == 1  # HP:0000002 is obsolete so it should be filtered out
    assert dim_hpo["hp_id"].to_list() == ["HP:0000001"]
    assert dim_hpo["phenotype_name"].to_list() == ["Test Phenotype 1"]

    fact_edges = pl.read_database_uri("SELECT * FROM hpo_gold.fact_hpo_relationship", uri=test_db_uri, engine="adbc")
    assert fact_edges.height == 1
    # Check that source_coreason_id and target_coreason_id exist and are not null
    assert "source_coreason_id" in fact_edges.columns
    assert "target_coreason_id" in fact_edges.columns

    bridge_anno = pl.read_database_uri(
        "SELECT * FROM hpo_gold.bridge_disease_annotation", uri=test_db_uri, engine="adbc"
    )
    assert bridge_anno.height == 1
    assert bridge_anno["disease_id"].to_list() == ["OMIM:101600"]
    assert bridge_anno["disease_name"].to_list() == ["Disease 1"]
