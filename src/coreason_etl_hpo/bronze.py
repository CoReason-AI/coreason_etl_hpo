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
