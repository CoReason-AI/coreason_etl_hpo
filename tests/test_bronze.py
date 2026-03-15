import io
import json
from unittest import mock

import pytest

from coreason_etl_hpo.bronze import hpo_annotations, hpo_graph_json


def test_hpo_graph_json_success() -> None:
    """Test successful ingestion of HPO JSON nodes and edges."""
    mock_response = mock.MagicMock()
    # Mocking response.raw as a BytesIO stream
    json_data = b"""
    {
        "graphs": [
            {
                "nodes": [
                    {"id": "HP:0000001", "lbl": "Test Phenotype", "meta": {"definition": {"val": "Test def"}}}
                ],
                "edges": [{"sub": "HP:0000002", "pred": "is_a", "obj": "HP:0000001"}]
            }
        ]
    }
    """
    mock_response.raw = io.BytesIO(json_data)
    mock_response.raise_for_status = mock.MagicMock()

    with mock.patch("coreason_etl_hpo.bronze.client.get", return_value=mock_response):
        items = list(hpo_graph_json())

        # When iterating over dlt.resource directly, dlt flattens lists marked with table name
        # It yields the un-batched dictionaries. So len(items) is 2 (1 node, 1 edge)
        assert len(items) == 2

        assert items[0]["id"] == "HP:0000001"
        assert "ingestion_ts" in items[0]

        assert items[1]["sub"] == "HP:0000002"
        assert "ingestion_ts" in items[1]


def test_hpo_graph_json_invalid_node_schema() -> None:
    """Test validation error when node data does not match contract schema (missing id)."""
    mock_response = mock.MagicMock()
    json_data = b"""
    {
        "graphs": [
            {
                "nodes": [
                    {"lbl": "Test Phenotype"}
                ],
                "edges": []
            }
        ]
    }
    """
    mock_response.raw = io.BytesIO(json_data)
    mock_response.raise_for_status = mock.MagicMock()

    from dlt.extract.exceptions import ResourceExtractionError
    from pydantic import ValidationError

    with mock.patch("coreason_etl_hpo.bronze.client.get", return_value=mock_response):
        with pytest.raises((ResourceExtractionError, ValidationError)) as exc_info:
            list(hpo_graph_json())
        assert "validation error" in str(exc_info.value).lower()
        assert "HPONodeContract" in str(exc_info.value)


def test_hpo_graph_json_invalid_edge_schema() -> None:
    """Test validation error when edge data does not match contract schema (missing pred)."""
    mock_response = mock.MagicMock()
    json_data = b"""
    {
        "graphs": [
            {
                "nodes": [{"id": "HP:0000001"}],
                "edges": [
                    {"sub": "HP:0000002", "obj": "HP:0000001"}
                ]
            }
        ]
    }
    """
    mock_response.raw = io.BytesIO(json_data)
    mock_response.raise_for_status = mock.MagicMock()

    from dlt.extract.exceptions import ResourceExtractionError
    from pydantic import ValidationError

    with mock.patch("coreason_etl_hpo.bronze.client.get", return_value=mock_response):
        with pytest.raises((ResourceExtractionError, ValidationError)) as exc_info:
            list(hpo_graph_json())
        assert "validation error" in str(exc_info.value).lower()
        assert "HPOEdgeContract" in str(exc_info.value)


def test_hpo_graph_json_missing_graphs() -> None:
    """Test exception raised when graphs array is missing or empty."""
    mock_response = mock.MagicMock()
    json_data = b'{"missing_graphs": []}'
    mock_response.raw = io.BytesIO(json_data)
    mock_response.raise_for_status = mock.MagicMock()

    from dlt.extract.exceptions import ResourceExtractionError

    with mock.patch("coreason_etl_hpo.bronze.client.get", return_value=mock_response):
        with pytest.raises((ResourceExtractionError, ValueError)) as exc_info:
            list(hpo_graph_json())
        assert "Invalid HPO JSON structure" in str(exc_info.value)


def test_hpo_graph_json_batching() -> None:
    """Test successful ingestion of HPO JSON nodes and edges with batching."""
    mock_response = mock.MagicMock()
    # Mocking response.raw as a BytesIO stream
    nodes = [{"id": f"HP:{str(i).zfill(7)}", "lbl": f"Test {i}"} for i in range(1500)]
    edges = [{"sub": f"HP:{str(i).zfill(7)}", "pred": "is_a", "obj": "HP:0000000"} for i in range(1500)]

    json_data = json.dumps({"graphs": [{"nodes": nodes, "edges": edges}]}).encode("utf-8")

    mock_response.raw = io.BytesIO(json_data)
    mock_response.raise_for_status = mock.MagicMock()

    with mock.patch("coreason_etl_hpo.bronze.client.get", return_value=mock_response):
        items = list(hpo_graph_json())

        # Since list() flattens the items yielded by the dlt resource, we expect 3000 items total
        assert len(items) == 3000


def test_hpo_annotations_success() -> None:
    """Test successful ingestion of HPO annotations, ensuring comments are skipped."""
    mock_response = mock.MagicMock()
    # Mocking lines returned by iter_lines
    lines = [
        b"# This is a comment",
        b"database_id\tdisease_name\tqualifier\thpo_id\treference\tevidence\tonset\tfrequency\tsex\tmodifier\taspect\tbiocuration",
        b"OMIM:101600\tDisease 1\t\tHP:0000001\t\tE1\t\tF1\t\t\tA1\t",
    ]
    mock_response.iter_lines.return_value = (line for line in lines)
    mock_response.raise_for_status = mock.MagicMock()

    with mock.patch("coreason_etl_hpo.bronze.client.get", return_value=mock_response):
        items = list(hpo_annotations())

        assert len(items) == 1
        assert items[0]["database_id"] == "OMIM:101600"
        assert items[0]["hpo_id"] == "HP:0000001"
        assert "ingestion_ts" in items[0]
