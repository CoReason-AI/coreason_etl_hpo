import datetime
from collections.abc import Generator
from typing import Any

import dlt
from dlt.sources.helpers.requests import client


@dlt.resource(name="hpo_graph_json", write_disposition="replace")  # type: ignore[misc]
def hpo_graph_json() -> Generator[Any]:
    """
    Ingests the primary HPO JSON source containing ontology graph nodes and edges.
    Yields data into `bronze_hpo_nodes` and `bronze_hpo_edges` tables.
    """
    url = "http://purl.obolibrary.org/obo/hp.json"
    response = client.get(url)
    response.raise_for_status()
    data = response.json()

    if "graphs" not in data or not isinstance(data["graphs"], list) or len(data["graphs"]) == 0:
        raise ValueError("Invalid HPO JSON structure: Missing or empty 'graphs' array.")

    graph = data["graphs"][0]
    ingestion_ts = datetime.datetime.now(datetime.UTC).isoformat()
    source_file = "hp.json"

    # Ingest nodes
    if "nodes" in graph:
        for node in graph["nodes"]:
            node["ingestion_ts"] = ingestion_ts
            node["source_file"] = source_file
            yield dlt.mark.with_table_name(node, "bronze_hpo_nodes")

    # Ingest edges
    if "edges" in graph:
        for edge in graph["edges"]:
            edge["ingestion_ts"] = ingestion_ts
            edge["source_file"] = source_file
            yield dlt.mark.with_table_name(edge, "bronze_hpo_edges")


@dlt.resource(name="hpo_annotations", write_disposition="replace")  # type: ignore[misc]
def hpo_annotations() -> Generator[Any]:
    """
    Ingests the secondary HPO annotations source (phenotype.hpoa).
    Yields data into `bronze_hpo_annotations` table.
    """
    import csv

    url = "http://purl.obolibrary.org/obo/hp/hpoa/phenotype.hpoa"
    response = client.get(url, stream=True)
    response.raise_for_status()

    def _filter_comments(it: Generator[bytes]) -> Generator[str]:
        for line in it:
            if line:
                decoded_line = line.decode("utf-8")
                if not decoded_line.startswith("#"):
                    yield decoded_line

    filtered_lines = _filter_comments(response.iter_lines())
    reader = csv.DictReader(filtered_lines, delimiter="\t")

    ingestion_ts = datetime.datetime.now(datetime.UTC).isoformat()
    source_file = "phenotype.hpoa"

    for row in reader:
        row["ingestion_ts"] = ingestion_ts
        row["source_file"] = source_file
        yield dlt.mark.with_table_name(row, "bronze_hpo_annotations")
