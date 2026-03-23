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
    synonyms: list[Any] | None = Field(None, description="List of synonyms for the node.")


class HPONode(BaseModel):
    """Data contract for an HPO node."""

    model_config = ConfigDict(extra="ignore")
    id: str = Field(description="The HP Identifier (e.g., HP:0002240).")
    lbl: str | None = Field(None, description="Primary human-readable name.")
    type: str | None = Field(None, description="Type or property type of the node.")
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
    Yields data into `bronze_hpo_nodes` and `bronze_hpo_edges` tables natively via dlt.
    """
    config = PipelineConfig()
    response = client.get(config.hpo_graph_url, stream=True)
    response.raise_for_status()

    ingestion_ts = datetime.datetime.now(datetime.UTC).isoformat()
    source_file = "hp.json"

    # Use a separate HTTP request for checking structure/iterating nodes vs edges,
    # or process them iteratively in a generator wrapper.
    # Since `ijson.items` advances the stream, we iterate over `graphs.item` to capture both.

    found_graphs = False

    for graph in ijson.items(response.raw, "graphs.item"):
        found_graphs = True

        # Process and yield all nodes natively
        if "nodes" in graph:
            for node in graph["nodes"]:
                HPONode.model_validate(node)
                node["ingestion_ts"] = ingestion_ts
                node["source_file"] = source_file
                yield dlt.mark.with_hints(
                    node,
                    dlt.mark.make_hints(table_name="coreason_etl_hpo_bronze_nodes", columns=HPONode),
                )

        # Process and yield all edges natively
        if "edges" in graph:
            for edge in graph["edges"]:
                HPOEdge.model_validate(edge)
                edge["ingestion_ts"] = ingestion_ts
                edge["source_file"] = source_file
                yield dlt.mark.with_hints(
                    edge,
                    dlt.mark.make_hints(table_name="coreason_etl_hpo_bronze_edges", columns=HPOEdge),
                )

    if not found_graphs:
        raise ValueError("Invalid HPO JSON structure: Missing or empty 'graphs' array.")


@dlt.resource(
    name="hpo_annotations",
    table_name="coreason_etl_hpo_bronze_annotations",
    write_disposition="replace",
    columns=HPOAnnotation,
)
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

    for row in reader:
        HPOAnnotation.model_validate(row)
        out_row = dict(row)
        out_row["ingestion_ts"] = ingestion_ts
        out_row["source_file"] = source_file
        yield out_row
