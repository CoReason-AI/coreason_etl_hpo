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
AGENT INSTRUCTION: This module tests the boundary constraints of the
PipelineConfigurationManifest to ensure configuration consistency.
"""

import os
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from coreason_etl_hpo.config import PipelineConfigurationManifest


def test_configuration_manifest_defaults() -> None:
    """
    Verify the instantiation of the PipelineConfigurationManifest using default
    network bindings and an explicit database target.
    """
    with patch.dict(os.environ, {"DATABASE_URL": "postgresql://user:pass@localhost:5432/db"}, clear=True):
        manifest = PipelineConfigurationManifest()

        assert manifest.app_env == "development"
        assert manifest.log_level == "INFO"
        assert str(manifest.hpo_graph_url) == "http://purl.obolibrary.org/obo/hp.json"
        assert str(manifest.hpo_annotations_url) == "http://purl.obolibrary.org/obo/hp/hpoa/phenotype.hpoa"
        assert str(manifest.database_url) == "postgresql://user:pass@localhost:5432/db"


def test_configuration_manifest_overrides() -> None:
    """
    Verify that explicit environmental bindings supersede default values
    for the configuration state.
    """
    env_overrides = {
        "APP_ENV": "production",
        "LOG_LEVEL": "DEBUG",
        "HPO_GRAPH_URL": "https://example.com/hp.json",
        "HPO_ANNOTATIONS_URL": "https://example.com/annotations.tsv",
        "DATABASE_URL": "postgresql://test:test@test-db:5432/testdb",
    }

    with patch.dict(os.environ, env_overrides, clear=True):
        manifest = PipelineConfigurationManifest()

        assert manifest.app_env == "production"
        assert manifest.log_level == "DEBUG"
        assert str(manifest.hpo_graph_url) == "https://example.com/hp.json"
        assert str(manifest.hpo_annotations_url) == "https://example.com/annotations.tsv"
        assert str(manifest.database_url) == "postgresql://test:test@test-db:5432/testdb"


def test_configuration_manifest_invalid_url() -> None:
    """
    Ensure the configuration boundary rejects structurally malformed network locators.
    """
    with patch.dict(os.environ, {"DATABASE_URL": "postgresql://db", "HPO_GRAPH_URL": "not_a_url"}, clear=True):
        with pytest.raises(ValidationError) as exc_info:
            PipelineConfigurationManifest()

        errors = exc_info.value.errors()
        assert any("url" in error["type"] for error in errors)


def test_configuration_manifest_invalid_db_url() -> None:
    """
    Ensure the configuration boundary rejects unsupported database dialects.
    """
    with patch.dict(os.environ, {"DATABASE_URL": "mysql://user:pass@localhost/db"}, clear=True):
        with pytest.raises(ValidationError) as exc_info:
            PipelineConfigurationManifest()

        errors = exc_info.value.errors()
        assert any("url_scheme" in error["type"] for error in errors)
