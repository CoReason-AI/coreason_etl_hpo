import polars as pl

from coreason_etl_hpo.identity import generate_coreason_id

# Constants
HP_ID_REGEX = r"^HP:\d{7}$"


def transform_silver_nodes(bronze_df: pl.LazyFrame | pl.DataFrame) -> pl.DataFrame:
    """
    Transforms the raw bronze_hpo_nodes data into the clean silver layer format.

    Args:
        bronze_df: The input dataframe (e.g., loaded from the bronze_hpo_nodes table).

    Returns:
        A Polars DataFrame matching the silver layer schema for HPO concepts.
    """
    lazy_df = bronze_df.lazy() if isinstance(bronze_df, pl.DataFrame) else bronze_df

    # Extract required columns and map them to their silver names.
    # We use `.struct.field` if nested, but according to bronze ingestion,
    # native JSON parsing flattens dicts. We will assume columns exist as:
    # id, lbl, meta.definition.val, meta.deprecated
    # Note: DLT might unpack nested fields differently based on configuration.
    # Usually `meta__definition__val`. We need to handle variations.

    # Actually, DLT flattens nested JSON using double underscore by default.
    # So `meta.definition.val` -> `meta__definition__val`
    # `meta.deprecated` -> `meta__deprecated`

    available_cols = lazy_df.collect_schema().names()

    # Determine column names based on potential flattening
    def_col: str | None = (
        "meta__definition__val" if "meta__definition__val" in available_cols else "meta_definition_val"
    )
    if def_col not in available_cols:
        # Fallback if the path is actually different or not present in sample
        def_col = None

    dep_col: str | None = "meta__deprecated" if "meta__deprecated" in available_cols else "meta_deprecated"
    if dep_col not in available_cols:
        dep_col = None

    select_exprs = [
        # Extract HP_1234567 or HP:1234567 and replace the underscore with a colon
        pl.col("id").str.extract(r"(HP[_:]\d{7})").str.replace("_", ":").alias("hp_id"),
        pl.col("lbl").str.strip_chars().alias("phenotype_name"),
    ]

    if def_col:
        select_exprs.append(pl.col(def_col).alias("definition"))
    else:
        select_exprs.append(pl.lit(None, dtype=pl.String).alias("definition"))

    if dep_col:
        select_exprs.append(pl.col(dep_col).fill_null(False).cast(pl.Boolean).alias("is_obsolete"))
    else:
        select_exprs.append(pl.lit(False).alias("is_obsolete"))

    transformed_df = lazy_df.select(select_exprs).with_columns(
        generate_coreason_id(pl.col("hp_id")).alias("coreason_id")
    )

    result_df = transformed_df.collect()

    # Validate regex for hp_id
    invalid_ids = result_df.filter(~pl.col("hp_id").str.contains(HP_ID_REGEX))
    if not invalid_ids.is_empty():
        raise ValueError(f"Found invalid hp_id entries: {invalid_ids['hp_id'].to_list()}")

    return result_df


def transform_silver_edges(bronze_df: pl.LazyFrame | pl.DataFrame) -> pl.DataFrame:
    """
    Transforms the raw bronze_hpo_edges data into the clean silver layer format.

    Args:
        bronze_df: The input dataframe (e.g., loaded from the bronze_hpo_edges table).

    Returns:
        A Polars DataFrame matching the silver layer schema for HPO edges.
    """
    lazy_df = bronze_df.lazy() if isinstance(bronze_df, pl.DataFrame) else bronze_df

    # Extract required columns (sub, pred, obj)
    # Filter for 'is_a' or 'subClassOf' relationships and extract clean HP IDs
    transformed_df = (
        lazy_df.filter(pl.col("pred").str.contains("is_a|subClassOf"))
        .select(
            [
                pl.col("sub").str.extract(r"(HP[_:]\d{7})").str.replace("_", ":").alias("source_hp_id"),
                pl.col("obj").str.extract(r"(HP[_:]\d{7})").str.replace("_", ":").alias("target_hp_id"),
            ]
        )
        .drop_nulls(subset=["source_hp_id", "target_hp_id"])
        .with_columns(
            [
                generate_coreason_id(pl.col("source_hp_id")).alias("source_coreason_id"),
                generate_coreason_id(pl.col("target_hp_id")).alias("target_coreason_id"),
            ]
        )
    )

    result_df = transformed_df.collect()

    # Validate regex for hp_id
    invalid_sources = result_df.filter(~pl.col("source_hp_id").str.contains(HP_ID_REGEX))
    if not invalid_sources.is_empty():
        raise ValueError(f"Found invalid source_hp_id entries: {invalid_sources['source_hp_id'].to_list()}")

    invalid_targets = result_df.filter(~pl.col("target_hp_id").str.contains(HP_ID_REGEX))
    if not invalid_targets.is_empty():
        raise ValueError(f"Found invalid target_hp_id entries: {invalid_targets['target_hp_id'].to_list()}")

    return result_df


def transform_silver_annotations(bronze_df: pl.LazyFrame | pl.DataFrame) -> pl.DataFrame:
    """
    Transforms the raw bronze_hpo_annotations data into the clean silver layer format.

    Args:
        bronze_df: The input dataframe (e.g., loaded from the bronze_hpo_annotations table).

    Returns:
        A Polars DataFrame matching the silver layer schema for HPO annotations.
    """
    lazy_df = bronze_df.lazy() if isinstance(bronze_df, pl.DataFrame) else bronze_df

    # We need to map database_id (e.g., OMIM:619340) and hpo_id (e.g., HP:0011097)
    # The requirement mentions mapping external database identifiers to coreason_id
    # We will generate coreason_id based on hpo_id, and keep database_id

    transformed_df = (
        lazy_df.select(
            [
                pl.col("hpo_id").str.extract(r"(HP:\d+)").alias("hp_id"),
                pl.col("database_id").alias("disease_id"),
                pl.col("disease_name"),
                pl.col("evidence"),
                pl.col("frequency"),
                pl.col("aspect"),
            ]
        )
        .filter(pl.col("hp_id").is_not_null())
        .with_columns(generate_coreason_id(pl.col("hp_id")).alias("coreason_id"))
    )

    result_df = transformed_df.collect()

    # Validate regex for hp_id
    invalid_hps = result_df.filter(~pl.col("hp_id").str.contains(HP_ID_REGEX))
    if not invalid_hps.is_empty():
        raise ValueError(f"Found invalid hp_id entries: {invalid_hps['hp_id'].to_list()}")

    return result_df
