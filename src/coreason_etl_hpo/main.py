import dlt
import polars as pl

from coreason_etl_hpo.bronze import hpo_annotations, hpo_graph_json
from coreason_etl_hpo.config import PipelineConfig
from coreason_etl_hpo.gold import (
    project_bridge_disease_annotation,
    project_dim_hpo_concept,
    project_fact_hpo_relationship,
)
from coreason_etl_hpo.silver import (
    transform_silver_annotations,
    transform_silver_edges,
    transform_silver_nodes,
)
from coreason_etl_hpo.utils.logger import logger


def hello_world() -> str:
    logger.info("Hello World!")
    return "Hello World!"


def run_pipeline() -> None:
    """
    Executes the full Medallion Architecture pipeline for the HPO integration.
    """
    config = PipelineConfig()

    logger.info("Starting Bronze ingestion...")
    pipeline = dlt.pipeline(
        pipeline_name="hpo_pipeline",
        destination=dlt.destinations.postgres(config.postgres_uri),
        dataset_name="bronze",
    )

    # Run the ingestion sources
    load_info = pipeline.run([hpo_graph_json(), hpo_annotations()])
    logger.info(f"Bronze ingestion complete. Load info: {load_info}")

    logger.info("Starting Silver & Gold transformations via Polars...")

    # Read Bronze tables
    # Note: `engine` is not a valid parameter for `read_database` with `connection` as a URI string.
    # The appropriate engine is dynamically selected based on the URI string provided.
    bronze_nodes = pl.read_database(
        "SELECT * FROM bronze.coreason_etl_hpo_bronze_nodes", connection=config.postgres_uri
    )
    bronze_edges = pl.read_database(
        "SELECT * FROM bronze.coreason_etl_hpo_bronze_edges", connection=config.postgres_uri
    )
    bronze_annotations = pl.read_database(
        "SELECT * FROM bronze.coreason_etl_hpo_bronze_annotations", connection=config.postgres_uri
    )

    # Silver transformations
    silver_nodes = transform_silver_nodes(bronze_nodes)
    silver_edges = transform_silver_edges(bronze_edges)
    silver_annotations = transform_silver_annotations(bronze_annotations)

    # Persist Silver layer
    logger.info("Writing Silver tables to PostgreSQL...")
    silver_nodes.write_database(
        "silver.coreason_etl_hpo_silver_nodes", connection=config.postgres_uri, engine="adbc", if_table_exists="replace"
    )
    silver_edges.write_database(
        "silver.coreason_etl_hpo_silver_edges", connection=config.postgres_uri, engine="adbc", if_table_exists="replace"
    )
    silver_annotations.write_database(
        "silver.coreason_etl_hpo_silver_annotations",
        connection=config.postgres_uri,
        engine="adbc",
        if_table_exists="replace",
    )

    # Gold projections
    dim_concept = project_dim_hpo_concept(silver_nodes)
    fact_relationship = project_fact_hpo_relationship(silver_edges)
    bridge_annotation = project_bridge_disease_annotation(silver_annotations)

    # Write to Gold schema in DB
    logger.info("Writing Gold tables to PostgreSQL...")

    # The adbc/sqlalchemy engines would be used.
    # For a robust setup, it is recommended to write to a target schema using `adbc`
    # Here, writing via `write_database`

    dim_concept.write_database(
        "gold.coreason_etl_hpo_gold_dim_concept",
        connection=config.postgres_uri,
        engine="adbc",
        if_table_exists="replace",
    )
    fact_relationship.write_database(
        "gold.coreason_etl_hpo_gold_fact_relationship",
        connection=config.postgres_uri,
        engine="adbc",
        if_table_exists="replace",
    )
    bridge_annotation.write_database(
        "gold.coreason_etl_hpo_gold_bridge_annotation",
        connection=config.postgres_uri,
        engine="adbc",
        if_table_exists="replace",
    )

    logger.info("Pipeline execution completed successfully.")


if __name__ == "__main__":
    run_pipeline()  # pragma: no cover
