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
AGENT INSTRUCTION: This module tests the basic entrypoint execution.
"""

from coreason_etl_hpo.main import hello_world


def test_main_hello_world() -> None:
    """
    Verify the basic entrypoint execution.
    """
    result = hello_world()
    assert result == "Hello World!"
