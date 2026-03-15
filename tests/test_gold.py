import polars as pl

from coreason_etl_hpo.gold import (
    project_bridge_disease_annotation,
    project_dim_hpo_concept,
    project_fact_hpo_relationship,
)


def test_project_dim_hpo_concept() -> None:
    data = {
        "coreason_id": ["id1", "id2"],
        "hp_id": ["HP:001", "HP:002"],
        "phenotype_name": ["Name1", "Name2"],
        "definition": ["Def1", "Def2"],
        "is_obsolete": [False, True],
    }
    df = pl.DataFrame(data)
    result = project_dim_hpo_concept(df)

    assert result.height == 1
    assert result["coreason_id"][0] == "id1"
    assert "is_obsolete" not in result.columns


def test_project_fact_hpo_relationship() -> None:
    data = {
        "source_coreason_id": ["s1", "s2"],
        "target_coreason_id": ["t1", "t2"],
        "source_hp_id": ["HP:001", "HP:002"],
        "target_hp_id": ["HP:003", "HP:004"],
    }
    df = pl.DataFrame(data)
    result = project_fact_hpo_relationship(df)

    assert result.height == 2
    assert "source_hp_id" not in result.columns
    assert "source_coreason_id" in result.columns


def test_project_bridge_disease_annotation() -> None:
    data = {
        "coreason_id": ["id1", "id2"],
        "disease_id": ["OMIM:1", "OMIM:2"],
        "disease_name": ["D1", "D2"],
        "evidence": ["E1", "E2"],
        "frequency": ["F1", "F2"],
        "aspect": ["A1", "A2"],
        "hp_id": ["HP:1", "HP:2"],
    }
    df = pl.DataFrame(data)
    result = project_bridge_disease_annotation(df)

    assert result.height == 2
    assert "hp_id" not in result.columns
    assert "disease_id" in result.columns
