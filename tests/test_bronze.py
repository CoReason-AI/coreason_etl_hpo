from unittest import mock

import pytest

from coreason_etl_hpo.bronze import hpo_annotations, hpo_graph_json


def test_hpo_graph_json_success() -> None:
    """Test successful ingestion of HPO JSON nodes and edges."""
    mock_response = mock.MagicMock()
    mock_response.json.return_value = {
        "graphs": [
            {
                "nodes": [
                    {"id": "HP:0000001", "lbl": "Test Phenotype", "meta": {"definition": {"val": "Test def"}}},
                ],
                "edges": [{"sub": "HP:0000002", "pred": "is_a", "obj": "HP:0000001"}],
            }
        ]
    }
    mock_response.raise_for_status = mock.MagicMock()

    with mock.patch("coreason_etl_hpo.bronze.client.get", return_value=mock_response):
        items = list(hpo_graph_json())

        assert len(items) == 2

        # dlt.mark.with_table_name modifies the object. dlt unwraps lists when iterating over resources.
        assert items[0]["id"] == "HP:0000001"
        assert "ingestion_ts" in items[0]

        assert items[1]["sub"] == "HP:0000002"
        assert "ingestion_ts" in items[1]


def test_hpo_graph_json_invalid_node_schema() -> None:
    """Test validation error when node data does not match contract schema (missing id)."""
    mock_response = mock.MagicMock()
    mock_response.json.return_value = {
        "graphs": [
            {
                "nodes": [
                    {"lbl": "Test Phenotype"}  # Missing required 'id'
                ],
                "edges": [],
            }
        ]
    }
    mock_response.raise_for_status = mock.MagicMock()

    from dlt.extract.exceptions import ResourceExtractionError

    with mock.patch("coreason_etl_hpo.bronze.client.get", return_value=mock_response):
        with pytest.raises(ResourceExtractionError) as exc_info:
            list(hpo_graph_json())
        assert "validation error" in str(exc_info.value).lower()
        assert "HPONodeContract" in str(exc_info.value)


def test_hpo_graph_json_invalid_edge_schema() -> None:
    """Test validation error when edge data does not match contract schema (missing pred)."""
    mock_response = mock.MagicMock()
    mock_response.json.return_value = {
        "graphs": [
            {
                "nodes": [{"id": "HP:0000001"}],
                "edges": [
                    {"sub": "HP:0000002", "obj": "HP:0000001"}  # Missing required 'pred'
                ],
            }
        ]
    }
    mock_response.raise_for_status = mock.MagicMock()

    from dlt.extract.exceptions import ResourceExtractionError

    with mock.patch("coreason_etl_hpo.bronze.client.get", return_value=mock_response):
        with pytest.raises(ResourceExtractionError) as exc_info:
            list(hpo_graph_json())
        assert "validation error" in str(exc_info.value).lower()
        assert "HPOEdgeContract" in str(exc_info.value)


def test_hpo_graph_json_missing_graphs() -> None:
    """Test exception raised when graphs array is missing or empty."""
    mock_response = mock.MagicMock()
    mock_response.json.return_value = {"graphs": []}
    mock_response.raise_for_status = mock.MagicMock()

    from dlt.extract.exceptions import ResourceExtractionError

    with mock.patch("coreason_etl_hpo.bronze.client.get", return_value=mock_response):
        with pytest.raises(ResourceExtractionError) as exc_info:
            list(hpo_graph_json())
        assert "Invalid HPO JSON structure" in str(exc_info.value)


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
