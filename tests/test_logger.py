# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_hpo

import importlib
import sys
from pathlib import Path
from unittest.mock import patch


def test_logger_initialization() -> None:
    """
    Test the logger initialization specifically the path creation condition.
    """
    with patch.object(Path, "exists", return_value=False), patch.object(Path, "mkdir") as mock_mkdir:
        # We must reload the module to trigger the file-level statements
        importlib.reload(sys.modules["coreason_etl_hpo.utils.logger"])
        mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)
