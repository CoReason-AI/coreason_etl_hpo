import polars as pl
import pytest

from coreason_etl_hpo.silver import transform_silver_annotations, transform_silver_edges, transform_silver_nodes


def test_transform_silver_nodes_complex_edge_cases() -> None:
    """Test silver node transformations with null labels, missing descriptions, spaces around ids, etc."""
    data = {
        "id": ["HP:0000001", "HP:0000002", "HP:0000003"],
        "lbl": [" All ", None, "\tAbnormality\n"],
        "meta__definition__val": ["Root of all terms.", None, ""],
        "meta__deprecated": [False, None, True],
    }
    df = pl.DataFrame(data)

    result = transform_silver_nodes(df)

    assert result.height == 3
    assert result["phenotype_name"].to_list() == ["All", None, "Abnormality"]
    assert result["hp_id"].to_list() == ["HP:0000001", "HP:0000002", "HP:0000003"]
    assert result["definition"].to_list() == ["Root of all terms.", None, ""]
    assert result["is_obsolete"].to_list() == [False, False, True]


def test_transform_silver_nodes_success() -> None:
    data = {
        "id": ["HP:0000001", "HP:0000002"],
        "lbl": [" All ", " Abnormality of body height "],
        "meta__definition__val": ["Root of all terms.", "Abnormality of height."],
        "meta__deprecated": [False, True],
    }
    df = pl.DataFrame(data)

    result = transform_silver_nodes(df)

    assert result.height == 2
    assert "coreason_id" in result.columns
    assert result["phenotype_name"].to_list() == ["All", "Abnormality of body height"]
    assert result["hp_id"].to_list() == ["HP:0000001", "HP:0000002"]
    assert result["definition"].to_list() == ["Root of all terms.", "Abnormality of height."]
    assert result["is_obsolete"].to_list() == [False, True]


def test_transform_silver_nodes_invalid_hp_id() -> None:
    data = {
        "id": ["HP:123", "HP:0000002"],
        "lbl": ["Invalid", "Valid"],
        "meta__definition__val": ["", ""],
        "meta__deprecated": [False, False],
    }
    df = pl.DataFrame(data)

    with pytest.raises(ValueError, match="Found invalid hp_id entries: \\['HP:123'\\]"):
        transform_silver_nodes(df)


def test_transform_silver_nodes_missing_meta_cols() -> None:
    data = {"id": ["HP:0000001"], "lbl": ["All"]}
    df = pl.DataFrame(data)

    result = transform_silver_nodes(df)

    assert result.height == 1
    assert result["definition"].to_list() == [None]
    assert result["is_obsolete"].to_list() == [False]


def test_transform_silver_nodes_lazyframe() -> None:
    data = {"id": ["HP:0000001"], "lbl": ["All"]}
    df = pl.DataFrame(data).lazy()

    result = transform_silver_nodes(df)

    assert isinstance(result, pl.DataFrame)
    assert result.height == 1


def test_transform_silver_edges_complex_filter() -> None:
    """Test filtering of non-is_a relationships."""
    data = {
        "sub": ["HP:0000002", "HP:0000003", "HP:0000004", "HP:0000005"],
        "pred": ["is_a", "has_part", "is_a", "part_of"],
        "obj": ["HP:0000001", "HP:0000001", "HP:0000001", "HP:0000001"],
    }
    df = pl.DataFrame(data)

    result = transform_silver_edges(df)

    assert result.height == 2
    assert result["source_hp_id"].to_list() == ["HP:0000002", "HP:0000004"]


def test_transform_silver_edges_success() -> None:
    data = {
        "sub": ["HP:0000002", "HP:0000003", "HP:0000004"],
        "pred": ["is_a", "is_a", "other_rel"],
        "obj": ["HP:0000001", "HP:0000001", "HP:0000001"],
    }
    df = pl.DataFrame(data)

    result = transform_silver_edges(df)

    assert result.height == 2
    assert "source_coreason_id" in result.columns
    assert "target_coreason_id" in result.columns
    assert result["source_hp_id"].to_list() == ["HP:0000002", "HP:0000003"]
    assert result["target_hp_id"].to_list() == ["HP:0000001", "HP:0000001"]


def test_transform_silver_edges_invalid_source_id() -> None:
    data = {"sub": ["HP:123", "HP:0000003"], "pred": ["is_a", "is_a"], "obj": ["HP:0000001", "HP:0000001"]}
    df = pl.DataFrame(data)

    with pytest.raises(ValueError, match="Found invalid source_hp_id entries: \\['HP:123'\\]"):
        transform_silver_edges(df)


def test_transform_silver_edges_invalid_target_id() -> None:
    data = {"sub": ["HP:0000002", "HP:0000003"], "pred": ["is_a", "is_a"], "obj": ["HP:123", "HP:0000001"]}
    df = pl.DataFrame(data)

    with pytest.raises(ValueError, match="Found invalid target_hp_id entries: \\['HP:123'\\]"):
        transform_silver_edges(df)


def test_transform_silver_annotations_success() -> None:
    data = {
        "database_id": ["OMIM:619340", "OMIM:619340"],
        "disease_name": ["Disease 1", "Disease 1"],
        "hpo_id": ["HP:0011097", "HP:0002187  "],
        "evidence": ["PCS", "PCS"],
        "frequency": ["1/2", "1/1"],
        "aspect": ["P", "P"],
    }
    df = pl.DataFrame(data)

    result = transform_silver_annotations(df)

    assert result.height == 2
    assert "coreason_id" in result.columns
    assert result["hp_id"].to_list() == ["HP:0011097", "HP:0002187"]
    assert result["disease_id"].to_list() == ["OMIM:619340", "OMIM:619340"]


def test_transform_silver_annotations_invalid_hp_id() -> None:
    data = {
        "database_id": ["OMIM:619340"],
        "disease_name": ["Disease 1"],
        "hpo_id": ["HP:123"],
        "evidence": ["PCS"],
        "frequency": ["1/2"],
        "aspect": ["P"],
    }
    df = pl.DataFrame(data)

    with pytest.raises(ValueError, match="Found invalid hp_id entries: \\['HP:123'\\]"):
        transform_silver_annotations(df)
