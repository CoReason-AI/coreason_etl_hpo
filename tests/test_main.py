from unittest import mock

import polars as pl

from coreason_etl_hpo.main import hello_world, run_pipeline


def test_hello_world() -> None:
    assert hello_world() == "Hello World!"


def test_run_pipeline() -> None:
    # We will mock the dlt pipeline, polars read/write methods

    with (
        mock.patch("dlt.pipeline") as mock_dlt_pipeline,
        mock.patch("polars.read_database") as mock_read_db,
        mock.patch("polars.DataFrame.write_database") as mock_write_db,
    ):
        # Mock DLT pipeline run
        mock_pipeline_instance = mock.MagicMock()
        mock_dlt_pipeline.return_value = mock_pipeline_instance
        mock_pipeline_instance.run.return_value = "Success"

        # Provide sample dataframes for the polars transformations
        bronze_nodes = pl.DataFrame(
            {
                "id": ["HP:0000001"],
                "lbl": ["Test"],
                "meta__definition__val": ["Def"],
                "meta__deprecated": [False],
                "_dlt_id": ["1"],
            }
        )
        bronze_synonyms = pl.DataFrame({"val": ["Syn1"], "_dlt_parent_id": ["1"]})
        bronze_edges = pl.DataFrame({"sub": ["HP:0000002"], "pred": ["is_a"], "obj": ["HP:0000001"]})
        bronze_annotations = pl.DataFrame(
            {
                "database_id": ["OMIM:1"],
                "disease_name": ["D1"],
                "hpo_id": ["HP:0000001"],
                "evidence": ["E1"],
                "frequency": ["F1"],
                "aspect": ["A1"],
                "reference": ["R1"],
                "onset": ["O1"],
                "modifier": ["M1"],
            }
        )

        mock_read_db.side_effect = [bronze_nodes, bronze_synonyms, bronze_edges, bronze_annotations]

        # Execute the pipeline
        run_pipeline()

        # Assertions
        mock_dlt_pipeline.assert_called_once()
        mock_pipeline_instance.run.assert_called_once()
        assert mock_read_db.call_count == 4
        assert mock_write_db.call_count == 7


def test_run_pipeline_no_synonyms() -> None:
    with (
        mock.patch("dlt.pipeline") as mock_dlt_pipeline,
        mock.patch("polars.read_database") as mock_read_db,
        mock.patch("polars.DataFrame.write_database") as mock_write_db,
    ):
        mock_pipeline_instance = mock.MagicMock()
        mock_dlt_pipeline.return_value = mock_pipeline_instance

        bronze_nodes = pl.DataFrame(
            {
                "id": ["HP:0000001"],
                "lbl": ["Test"],
                "meta__definition__val": ["Def"],
                "meta__deprecated": [False],
                "_dlt_id": ["1"],
            }
        )
        bronze_edges = pl.DataFrame({"sub": ["HP:0000002"], "pred": ["is_a"], "obj": ["HP:0000001"]})
        bronze_annotations = pl.DataFrame(
            {
                "database_id": ["OMIM:1"],
                "disease_name": ["D1"],
                "hpo_id": ["HP:0000001"],
                "evidence": ["E1"],
                "frequency": ["F1"],
                "aspect": ["A1"],
                "reference": ["R1"],
                "onset": ["O1"],
                "modifier": ["M1"],
            }
        )

        def mock_read_side_effect(query: str, connection: str) -> pl.DataFrame:
            _ = connection
            if "synonyms" in query:
                raise Exception("Table does not exist")
            if "bronze_nodes" in query:
                return bronze_nodes
            if "bronze_edges" in query:
                return bronze_edges
            if "bronze_annotations" in query:
                return bronze_annotations
            return pl.DataFrame()

        mock_read_db.side_effect = mock_read_side_effect

        run_pipeline()
        assert mock_read_db.call_count == 4
        assert mock_write_db.call_count == 7
