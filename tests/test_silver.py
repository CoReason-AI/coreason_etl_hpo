# --- tests/test_silver.py ---

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
        "_dlt_id": ["uuid1", "uuid2", "uuid3"],
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
        "type": ["CLASS", "PROPERTY"],
        "meta__definition__val": ["Root of all terms.", "Abnormality of height."],
        "meta__deprecated": [False, True],
<<<<<<< HEAD
        "_dlt_id": ["uuid1", "uuid2"],
=======
        "meta__synonyms": [None, '[{"val": "Short stature"}]'],  # Add mock synonyms
>>>>>>> 68b9210 (validation changes implemented)
    }
    df = pl.DataFrame(data)

    synonyms_data = {"val": ["Syn1", "Syn2", "Syn3"], "_dlt_parent_id": ["uuid1", "uuid1", "uuid2"]}
    synonyms_df = pl.DataFrame(synonyms_data)

    edges_data = {"sub": ["HP:0000003", "HP:0000004"], "pred": ["is_a", "other"], "obj": ["HP:0000001", "HP:0000002"]}
    edges_df = pl.DataFrame(edges_data)

    result = transform_silver_nodes(df, synonyms_df=synonyms_df, edges_df=edges_df)

    assert result.height == 2
    assert "coreason_id" in result.columns
    assert "synonyms" in result.columns  # Assert synonyms extracted
    assert result["phenotype_name"].to_list() == ["All", "Abnormality of body height"]
    assert result["hp_id"].to_list() == ["HP:0000001", "HP:0000002"]
    assert result["type"].to_list() == ["CLASS", "PROPERTY"]
    assert result["definition"].to_list() == ["Root of all terms.", "Abnormality of height."]
    assert result["is_obsolete"].to_list() == [False, True]
<<<<<<< HEAD
    assert result["synonym"].to_list() == ["Syn1|Syn2", "Syn3"]
    assert result["is_a_parent"].to_list() == [True, False]
=======
    assert result["synonyms"].to_list() == [None, '[{"val": "Short stature"}]']
>>>>>>> 68b9210 (validation changes implemented)


def test_transform_silver_nodes_invalid_hp_id() -> None:
    data = {
        "id": ["HP:123", "HP:0000002"],
        "lbl": ["Invalid", "Valid"],
        "meta__definition__val": ["", ""],
        "meta__deprecated": [False, False],
        "_dlt_id": ["uuid1", "uuid2"],
    }
    df = pl.DataFrame(data)

    with pytest.raises(ValueError, match="Found invalid hp_id entries: \\['HP:123'\\]"):
        transform_silver_nodes(df)


def test_transform_silver_nodes_missing_meta_cols() -> None:
    data = {"id": ["HP:0000001"], "lbl": ["All"], "_dlt_id": ["uuid1"]}
    df = pl.DataFrame(data)

    # Send empty dataframes with correct columns to hit the "else" blocks
    # when columns are completely missing
    synonyms_data = {"other_col": ["val"]}
    synonyms_df = pl.DataFrame(synonyms_data)

    edges_data = {"other_col": ["val"]}
    edges_df = pl.DataFrame(edges_data)

    result = transform_silver_nodes(df, synonyms_df=synonyms_df, edges_df=edges_df)

    assert result.height == 1
    assert result["definition"].to_list() == [None]
    assert result["is_obsolete"].to_list() == [False]
    assert result["type"].to_list() == [None]
    assert result["synonym"].to_list() == [None]
    assert result["is_a_parent"].to_list() == [False]


def test_transform_silver_nodes_lazyframe() -> None:
    data = {"id": ["HP:0000001"], "lbl": ["All"], "_dlt_id": ["uuid1"]}
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
    assert "relationship_coreason_id" in result.columns  # Assert new PK
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
        "reference": ["PMID:1", "PMID:2"],
        "onset": ["HP:01", "HP:02"],
        "modifier": ["M1", "M2"],
    }
    df = pl.DataFrame(data)

    result = transform_silver_annotations(df)

    assert result.height == 2
    assert "coreason_id" in result.columns
    assert "annotation_coreason_id" in result.columns  # Assert new PK
    assert result["hp_id"].to_list() == ["HP:0011097", "HP:0002187"]
    assert result["disease_id"].to_list() == ["OMIM:619340", "OMIM:619340"]
    assert result["reference"].to_list() == ["PMID:1", "PMID:2"]
    assert result["onset"].to_list() == ["HP:01", "HP:02"]
    assert result["modifier"].to_list() == ["M1", "M2"]


def test_transform_silver_annotations_invalid_hp_id() -> None:
    data = {
        "database_id": ["OMIM:619340"],
        "disease_name": ["Disease 1"],
        "hpo_id": ["HP:123"],
        "evidence": ["PCS"],
        "frequency": ["1/2"],
        "aspect": ["P"],
        "reference": [""],
        "onset": [""],
        "modifier": [""],
    }
    df = pl.DataFrame(data)

    with pytest.raises(ValueError, match="Found invalid hp_id entries: \\['HP:123'\\]"):
        transform_silver_annotations(df)
