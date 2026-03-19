import datetime
from collections.abc import Generator
from typing import Any

import dlt
import ijson  # type: ignore[import-untyped]
from dlt.sources.helpers.requests import client
from pydantic import BaseModel, ConfigDict, Field, field_validator

from coreason_etl_hpo.config import PipelineConfig


class HPONodeMetaDefinition(BaseModel):
    """Data contract for the meta definition of an HPO node."""

    model_config = ConfigDict(extra="ignore")
    val: str = Field(description="Description of the phenotype.")


class HPONodeMeta(BaseModel):
    """Data contract for the meta data of an HPO node."""

    model_config = ConfigDict(extra="ignore")
    definition: HPONodeMetaDefinition | None = Field(None, description="Definition of the node.")
    deprecated: bool | None = Field(None, description="True if the node is deprecated.")


class HPONode(BaseModel):
    """Data contract for an HPO node."""

    model_config = ConfigDict(extra="ignore")
    id: str = Field(description="The HP Identifier (e.g., HP:0002240).")
    lbl: str | None = Field(None, description="Primary human-readable name.")
    meta: HPONodeMeta | None = Field(None, description="Metadata for the node.")


class HPOEdge(BaseModel):
    """Data contract for an HPO edge."""

    model_config = ConfigDict(extra="ignore")
    sub: str = Field(description="Subject node ID.")
    pred: str = Field(description="Predicate/relationship (e.g., is_a).")
    obj: str = Field(description="Object node ID.")


class HPOAnnotation(BaseModel):
    """Data contract for an HPO annotation."""

    model_config = ConfigDict(extra="ignore")
    database_id: str = Field(description="External database ID (e.g., OMIM:101600).")
    disease_name: str | None = Field(None, description="Name of the disease.")
    qualifier: str | None = Field(None, description="Qualifier for the annotation.")
    hpo_id: str = Field(description="The HP Identifier (e.g., HP:0002240).")
    reference: str | None = Field(None, description="Reference for the annotation.")
    evidence: str | None = Field(None, description="Evidence code.")
    onset: str | None = Field(None, description="Onset of the phenotype.")
    frequency: str | None = Field(None, description="Frequency of the phenotype.")
    sex: str | None = Field(None, description="Sex related to the phenotype.")
    modifier: str | None = Field(None, description="Modifier of the phenotype.")
    aspect: str | None = Field(None, description="Aspect of the phenotype.")
    biocuration: str | None = Field(None, description="Biocuration history.")

    @field_validator("hpo_id", "database_id")
    @classmethod
    def check_not_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("Field cannot be empty")
        return v


@dlt.resource(name="hpo_graph_json", write_disposition="replace")
def hpo_graph_json() -> Generator[Any]:
    """
    Ingests the primary HPO JSON source containing ontology graph nodes and edges.
    Yields data into `bronze_hpo_nodes` and `bronze_hpo_edges` tables.
    """
    config = PipelineConfig()
    response = client.get(config.hpo_graph_url, stream=True)
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
                    HPONode.model_validate(node)
                    node["ingestion_ts"] = ingestion_ts
                    node["source_file"] = source_file
                    nodes_batch.append(node)
                    if len(nodes_batch) >= batch_size:
                        yield dlt.mark.with_hints(
                            nodes_batch,
                            dlt.mark.make_hints(table_name="coreason_etl_hpo_bronze_nodes", columns=HPONode),
                        )
                        nodes_batch = []
                    break

        elif prefix == "graphs.item.edges.item" and event == "start_map":
            builder = ijson.ObjectBuilder()
            builder.event(event, value)
            for p, e, v in parser:
                builder.event(e, v)
                if p == "graphs.item.edges.item" and e == "end_map":
                    edge = builder.value
                    HPOEdge.model_validate(edge)
                    edge["ingestion_ts"] = ingestion_ts
                    edge["source_file"] = source_file
                    edges_batch.append(edge)
                    if len(edges_batch) >= batch_size:
                        yield dlt.mark.with_hints(
                            edges_batch,
                            dlt.mark.make_hints(table_name="coreason_etl_hpo_bronze_edges", columns=HPOEdge),
                        )
                        edges_batch = []
                    break

    if nodes_batch:
        yield dlt.mark.with_hints(
            nodes_batch, dlt.mark.make_hints(table_name="coreason_etl_hpo_bronze_nodes", columns=HPONode)
        )

    if edges_batch:
        yield dlt.mark.with_hints(
            edges_batch, dlt.mark.make_hints(table_name="coreason_etl_hpo_bronze_edges", columns=HPOEdge)
        )

    if not found_graphs:
        raise ValueError("Invalid HPO JSON structure: Missing or empty 'graphs' array.")


@dlt.resource(name="hpo_annotations", write_disposition="replace")
def hpo_annotations() -> Generator[Any]:
    """
    Ingests the secondary HPO annotations source (phenotype.hpoa).
    Yields data into `bronze_hpo_annotations` table.
    """
    import csv

    config = PipelineConfig()
    response = client.get(config.hpo_annotations_url, stream=True)
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

    annotations_batch = []
    batch_size = 1000

    for row in reader:
        HPOAnnotation.model_validate(row)
        row["ingestion_ts"] = ingestion_ts
        row["source_file"] = source_file
        annotations_batch.append(row)
        if len(annotations_batch) >= batch_size:
            yield dlt.mark.with_hints(
                annotations_batch,
                dlt.mark.make_hints(table_name="coreason_etl_hpo_bronze_annotations", columns=HPOAnnotation),
            )
            annotations_batch = []

    if annotations_batch:
        yield dlt.mark.with_hints(
            annotations_batch,
            dlt.mark.make_hints(table_name="coreason_etl_hpo_bronze_annotations", columns=HPOAnnotation),
        )
