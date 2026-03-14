import uuid

import polars as pl
from hypothesis import given
from hypothesis import strategies as st

from coreason_etl_hpo.identity import NAMESPACE_HPO, generate_coreason_id


def test_generate_coreason_id_valid() -> None:
    df = pl.DataFrame({"source_id": ["HP:0000001", "HP:0000002"]})
    res_df = df.with_columns(generate_coreason_id(pl.col("source_id")).alias("coreason_id"))

    assert res_df.height == 2

    # Expected UUID5s
    ns = uuid.UUID(NAMESPACE_HPO)
    expected_0 = str(uuid.uuid5(ns, "HP:0000001"))
    expected_1 = str(uuid.uuid5(ns, "HP:0000002"))

    assert res_df["coreason_id"][0] == expected_0
    assert res_df["coreason_id"][1] == expected_1


def test_generate_coreason_id_null() -> None:
    df = pl.DataFrame({"source_id": ["HP:0000001", None, "HP:0000003"]})
    res_df = df.with_columns(generate_coreason_id(pl.col("source_id")).alias("coreason_id"))

    assert res_df["coreason_id"][1] is None


@given(st.lists(st.text(), min_size=1, max_size=100))  # type: ignore[misc]
def test_hypothesis_generate_coreason_id(input_strings: list[str]) -> None:
    df = pl.DataFrame({"source_id": input_strings})
    res_df = df.with_columns(generate_coreason_id(pl.col("source_id")).alias("coreason_id"))

    ns = uuid.UUID(NAMESPACE_HPO)
    for i, val in enumerate(input_strings):
        assert res_df["coreason_id"][i] == str(uuid.uuid5(ns, val))
