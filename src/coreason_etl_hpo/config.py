# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_hpo

"""
AGENT INSTRUCTION: This module defines the strict configuration contracts
and boundaries for the pipeline. It must not contain application logic.
"""

from typing import Literal

from pydantic import AnyHttpUrl, Field, PostgresDsn
from pydantic_settings import BaseSettings, SettingsConfigDict


class PipelineConfigurationManifest(BaseSettings):
    """
    Defines the immutable configuration state and environmental bindings
    for the HPO extraction and transformation topology.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_env: Literal["development", "testing", "production"] = Field(
        default="development",
        description="The environmental boundary in which the process operates.",
        alias="APP_ENV",
    )

    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = Field(
        default="INFO",
        description="The severity threshold for telemetry emission.",
        alias="LOG_LEVEL",
    )

    hpo_graph_url: AnyHttpUrl = Field(
        default="http://purl.obolibrary.org/obo/hp.json",  # type: ignore
        description="The source vertex and edge terminology graph manifold.",
        alias="HPO_GRAPH_URL",
    )

    hpo_annotations_url: AnyHttpUrl = Field(
        default="http://purl.obolibrary.org/obo/hp/hpoa/phenotype.hpoa",  # type: ignore
        description="The secondary disease linkage annotation stream.",
        alias="HPO_ANNOTATIONS_URL",
    )

    database_url: PostgresDsn = Field(
        description="The targeted persistent store for state materialization.",
        alias="DATABASE_URL",
    )
