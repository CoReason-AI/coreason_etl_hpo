# --- tests/test_gold.py ---

import polars as pl

from coreason_etl_hpo.gold import (
    project_bridge_disease_annotation,
    project_dim_hpo_concept,
    project_fact_hpo_relationship,
    project_obt_hpo_reporting,
)


def test_project_dim_hpo_concept() -> None:
    data = {
        "coreason_id": ["C1", "C2", "C3"],
        "hp_id": ["HP:0000001", "HP:0000002", "HP:0000003"],
        "phenotype_name": ["P1", "P2", "P3"],
        "definition": ["D1", "D2", "D3"],
<<<<<<< HEAD
        "synonym": ["S1", "S2", "S3"],
        "is_a_parent": [True, False, True],
        "type": ["CLASS", "CLASS", "PROPERTY"],
=======
        "synonyms": [None, '[{"val": "Syn1"}]', None],  # Added synonyms mock data
>>>>>>> 68b9210 (validation changes implemented)
        "is_obsolete": [False, True, None],
    }
    df = pl.DataFrame(data)
    result = project_dim_hpo_concept(df)

    assert result.height == 2
    assert result["hp_id"].to_list() == ["HP:0000001", "HP:0000003"]
    assert "is_obsolete" not in result.columns
<<<<<<< HEAD
    assert "synonym" in result.columns
    assert "is_a_parent" in result.columns
    assert "type" in result.columns
=======
    assert "synonyms" in result.columns  # Assert new column exists
>>>>>>> 68b9210 (validation changes implemented)


def test_project_fact_hpo_relationship() -> None:
    data = {
        "relationship_coreason_id": ["r1", "r2"],  # Added new PK
        "source_coreason_id": ["s1", "s2"],
        "target_coreason_id": ["t1", "t2"],
        "source_hp_id": ["HP:001", "HP:002"],
        "target_hp_id": ["HP:003", "HP:004"],
        "pred": ["is_a", "is_a"],
    }
    df = pl.DataFrame(data)
    result = project_fact_hpo_relationship(df)

    assert result.height == 2
    assert "relationship_coreason_id" in result.columns
    assert "source_hp_id" in result.columns  # We now WANT this to be in the columns
    assert "source_coreason_id" in result.columns


def test_project_bridge_disease_annotation() -> None:
    data = {
        "annotation_coreason_id": ["a1", "a2"],  # Added new PK
        "coreason_id": ["id1", "id2"],
        "disease_id": ["OMIM:1", "OMIM:2"],
        "disease_name": ["D1", "D2"],
        "evidence": ["E1", "E2"],
        "frequency": ["F1", "F2"],
        "aspect": ["A1", "A2"],
        "reference": ["R1", "R2"],
        "onset": ["O1", "O2"],
        "modifier": ["M1", "M2"],
        "hp_id": ["HP:1", "HP:2"],
    }
    df = pl.DataFrame(data)
    result = project_bridge_disease_annotation(df)

    assert result.height == 2
    assert "annotation_coreason_id" in result.columns
    assert "hp_id" in result.columns  # We now WANT this to be in the columns
    assert "disease_id" in result.columns
    assert "reference" in result.columns
    assert "onset" in result.columns
    assert "modifier" in result.columns


def test_project_obt_hpo_reporting() -> None:
    dim_data = {
        "coreason_id": ["C1", "C2"],
        "hp_id": ["HP:0000001", "HP:0000002"],
        "phenotype_name": ["Name1", "Name2"],
        "definition": ["Def1", "Def2"],
        "synonym": ["Syn1", "Syn2"],
        "is_a_parent": [True, False],
        "type": ["CLASS", "PROPERTY"],
    }
    dim_df = pl.DataFrame(dim_data)

    bridge_data = {
        "coreason_id": ["C1"],
        "disease_id": ["OMIM:1"],
        "disease_name": ["D1"],
        "evidence": ["E1"],
        "frequency": ["F1"],
        "aspect": ["A1"],
        "reference": ["Ref1"],
        "onset": ["Onset1"],
        "modifier": ["Mod1"],
    }
    bridge_df = pl.DataFrame(bridge_data)

    result = project_obt_hpo_reporting(dim_df, bridge_df)

    assert result.height == 2
    # Ensure all required OBT columns are present
    expected_cols = ["name", "definition", "hp_id", "synonym", "is_a_parent", "reference", "onset", "modifier"]
    for col in expected_cols:
        assert col in result.columns

    # Check joining logic
    # C1 should have bridge data
    row_c1 = result.filter(pl.col("hp_id") == "HP:0000001").row(0, named=True)
    assert row_c1["name"] == "Name1"
    assert row_c1["reference"] == "Ref1"

    # C2 should be null for bridge fields
    row_c2 = result.filter(pl.col("hp_id") == "HP:0000002").row(0, named=True)
    assert row_c2["name"] == "Name2"
    assert row_c2["reference"] is None
