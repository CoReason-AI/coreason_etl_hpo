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


def test_hpo_annotations_success() -> None:
    # TSV data
    # Mock response to return lines
    tsv_content = (
        "#description: HPO annotations\n"
        "#version: 2021-06-08\n"
        "database_id\tdisease_name\tqualifier\thpo_id\treference\tevidence\tonset\t"
        "frequency\tsex\tmodifier\taspect\tbiocuration\n"
        "OMIM:619340\tDevelopmental and epileptic encephalopathy 96\t\tHP:0011097\t"
        "PMID:31675180\tPCS\t\t1/2\t\t\tP\tHPO:probinson[2021-06-21]\n"
        "OMIM:619340\tDevelopmental and epileptic encephalopathy 96\t\tHP:0002187\t"
        "PMID:31675180\tPCS\t\t1/1\t\t\tP\tHPO:probinson[2021-06-21]\n"
    )

    class MockStreamResponse:
        def __init__(self, content: bytes, status_code: int = 200) -> None:
            self.content = content
            self.status_code = status_code

        def raise_for_status(self) -> None:
            if self.status_code != 200:
                raise Exception("HTTP Error")

        def iter_lines(self) -> list[bytes]:
            return self.content.split(b"\n")

    with mock.patch("coreason_etl_hpo.bronze.client.get", return_value=MockStreamResponse(tsv_content.encode("utf-8"))):
        from coreason_etl_hpo.bronze import hpo_annotations

        resource = hpo_annotations()
        results = list(resource)

        assert len(results) == 2

        row1 = results[0]
        assert row1["database_id"] == "OMIM:619340"
        assert row1["hpo_id"] == "HP:0011097"
        assert "ingestion_ts" in row1
        assert row1["source_file"] == "phenotype.hpoa"

        row2 = results[1]
        assert row2["hpo_id"] == "HP:0002187"


def test_hpo_annotations_http_error() -> None:
    class MockStreamResponse:
        def __init__(self, status_code: int = 500) -> None:
            self.status_code = status_code

        def raise_for_status(self) -> None:
            if self.status_code != 200:
                raise Exception("HTTP Error")

    with mock.patch("coreason_etl_hpo.bronze.client.get", return_value=MockStreamResponse(status_code=500)):
        from coreason_etl_hpo.bronze import hpo_annotations

        with pytest.raises(Exception, match="HTTP Error"):
            list(hpo_annotations())
