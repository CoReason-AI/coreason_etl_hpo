from collections.abc import Callable
from typing import Any
from unittest import mock

import pytest

from coreason_etl_hpo.bronze import hpo_graph_json


@pytest.fixture
def mock_response() -> Callable[..., Any]:
    class MockResponse:
        def __init__(self, json_data: dict[str, Any], status_code: int = 200) -> None:
            self.json_data = json_data
            self.status_code = status_code

        def json(self) -> dict[str, Any]:
            return self.json_data

        def raise_for_status(self) -> None:
            if self.status_code != 200:
                raise Exception("HTTP Error")

    return MockResponse


def test_hpo_graph_json_success(mock_response: Callable[..., Any]) -> None:
    valid_data = {
        "graphs": [
            {
                "nodes": [{"id": "HP:0000001", "lbl": "All"}],
                "edges": [{"sub": "HP:0000002", "pred": "is_a", "obj": "HP:0000001"}],
            }
        ]
    }

    with mock.patch("coreason_etl_hpo.bronze.client.get", return_value=mock_response(valid_data)):
        resource = hpo_graph_json()
        results = list(resource)

        assert len(results) == 2

        # Check nodes
        node = results[0]
        assert node["id"] == "HP:0000001"
        assert "ingestion_ts" in node
        assert node["source_file"] == "hp.json"

        # Let's just assert on dictionary contents.

        # Check edges
        edge = results[1]
        assert edge["sub"] == "HP:0000002"
        assert "ingestion_ts" in edge
        assert edge["source_file"] == "hp.json"


def test_hpo_graph_json_missing_graphs(mock_response: Callable[..., Any]) -> None:
    invalid_data: dict[str, list[Any]] = {"other": []}
    with (
        mock.patch("coreason_etl_hpo.bronze.client.get", return_value=mock_response(invalid_data)),
        pytest.raises(Exception, match=r"Invalid HPO JSON structure: Missing or empty 'graphs' array\."),
    ):
        list(hpo_graph_json())


def test_hpo_graph_json_empty_graphs(mock_response: Callable[..., Any]) -> None:
    invalid_data: dict[str, list[Any]] = {"graphs": []}
    with (
        mock.patch("coreason_etl_hpo.bronze.client.get", return_value=mock_response(invalid_data)),
        pytest.raises(Exception, match=r"Invalid HPO JSON structure: Missing or empty 'graphs' array\."),
    ):
        list(hpo_graph_json())


def test_hpo_graph_json_http_error(mock_response: Callable[..., Any]) -> None:
    with (
        mock.patch("coreason_etl_hpo.bronze.client.get", return_value=mock_response({}, status_code=500)),
        pytest.raises(Exception, match="HTTP Error"),
    ):
        list(hpo_graph_json())
