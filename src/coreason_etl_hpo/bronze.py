import datetime
from collections.abc import Generator
from typing import Any

import dlt
import ijson
from dlt.sources.helpers.requests import client
from pydantic import BaseModel, ConfigDict, Field


class HPONodeMetaDefinitionContract(BaseModel):
    """Data contract for the meta definition of an HPO node."""

    model_config = ConfigDict(extra="ignore")
    val: str = Field(description="Description of the phenotype.")


class HPONodeMetaContract(BaseModel):
    """Data contract for the meta data of an HPO node."""

    model_config = ConfigDict(extra="ignore")
    definition: HPONodeMetaDefinitionContract | None = Field(None, description="Definition of the node.")
    deprecated: bool | None = Field(None, description="True if the node is deprecated.")


class HPONodeContract(BaseModel):
    """Data contract for an HPO node."""

    model_config = ConfigDict(extra="ignore")
    id: str = Field(description="The HP Identifier (e.g., HP:0002240).")
    lbl: str | None = Field(None, description="Primary human-readable name.")
    meta: HPONodeMetaContract | None = Field(None, description="Metadata for the node.")


class HPOEdgeContract(BaseModel):
    """Data contract for an HPO edge."""

    model_config = ConfigDict(extra="ignore")
    sub: str = Field(description="Subject node ID.")
    pred: str = Field(description="Predicate/relationship (e.g., is_a).")
    obj: str = Field(description="Object node ID.")


@dlt.resource(name="hpo_graph_json", write_disposition="replace")  # type: ignore[misc]
def hpo_graph_json() -> Generator[Any]:
    """
    Ingests the primary HPO JSON source containing ontology graph nodes and edges.
    Yields data into `bronze_hpo_nodes` and `bronze_hpo_edges` tables.
    """
    url = "http://purl.obolibrary.org/obo/hp.json"
    response = client.get(url, stream=True)
    response.raise_for_status()

    ingestion_ts = datetime.datetime.now(datetime.UTC).isoformat()
    source_file = "hp.json"

    found_graphs = False

    nodes_batch = []
    edges_batch = []
    batch_size = 1000

    parser = ijson.parse(response.raw)

    for prefix, event, value in parser:
        if prefix == "graphs" and event == "start_array":
            found_graphs = True

        elif prefix == "graphs.item.nodes.item" and event == "start_map":
            builder = ijson.ObjectBuilder()
            builder.event(event, value)
            for p, e, v in parser:
                builder.event(e, v)
                if p == "graphs.item.nodes.item" and e == "end_map":
                    node = builder.value
                    HPONodeContract.model_validate(node)
                    node["ingestion_ts"] = ingestion_ts
                    node["source_file"] = source_file
                    nodes_batch.append(node)
                    if len(nodes_batch) >= batch_size:
                        yield dlt.mark.with_table_name(nodes_batch, "bronze_hpo_nodes")
                        nodes_batch = []
                    break

        elif prefix == "graphs.item.edges.item" and event == "start_map":
            builder = ijson.ObjectBuilder()
            builder.event(event, value)
            for p, e, v in parser:
                builder.event(e, v)
                if p == "graphs.item.edges.item" and e == "end_map":
                    edge = builder.value
                    HPOEdgeContract.model_validate(edge)
                    edge["ingestion_ts"] = ingestion_ts
                    edge["source_file"] = source_file
                    edges_batch.append(edge)
                    if len(edges_batch) >= batch_size:
                        yield dlt.mark.with_table_name(edges_batch, "bronze_hpo_edges")
                        edges_batch = []
                    break

    if nodes_batch:
        yield dlt.mark.with_table_name(nodes_batch, "bronze_hpo_nodes")

    if edges_batch:
        yield dlt.mark.with_table_name(edges_batch, "bronze_hpo_edges")

    if not found_graphs:
        raise ValueError("Invalid HPO JSON structure: Missing or empty 'graphs' array.")


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

    annotations_list = []
    for row in reader:
        row["ingestion_ts"] = ingestion_ts
        row["source_file"] = source_file
        annotations_list.append(row)

    if annotations_list:
        yield dlt.mark.with_table_name(annotations_list, "bronze_hpo_annotations")
