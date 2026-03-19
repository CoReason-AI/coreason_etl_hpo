import polars as pl


def project_dim_hpo_concept(silver_nodes_df: pl.LazyFrame | pl.DataFrame) -> pl.DataFrame:
    """
    Projects the silver HPO nodes into the gold dim_hpo_concept table.
    Filters out obsolete concepts.
    """
    lazy_df = silver_nodes_df.lazy() if isinstance(silver_nodes_df, pl.DataFrame) else silver_nodes_df

    return (
        lazy_df.filter(~pl.col("is_obsolete").fill_null(False))
        .select(["coreason_id", "hp_id", "phenotype_name", "definition"])
        .collect()
    )


def project_fact_hpo_relationship(silver_edges_df: pl.LazyFrame | pl.DataFrame) -> pl.DataFrame:
    """
    Projects the silver HPO edges into the gold fact_hpo_relationship table.
    """
    lazy_df = silver_edges_df.lazy() if isinstance(silver_edges_df, pl.DataFrame) else silver_edges_df

    return lazy_df.select(["source_coreason_id", "target_coreason_id"]).collect()


def project_bridge_disease_annotation(silver_annotations_df: pl.LazyFrame | pl.DataFrame) -> pl.DataFrame:
    """
    Projects the silver HPO annotations into the gold bridge_disease_annotation table.
    """
    lazy_df = silver_annotations_df.lazy() if isinstance(silver_annotations_df, pl.DataFrame) else silver_annotations_df

    return lazy_df.select(["coreason_id", "disease_id", "disease_name", "evidence", "frequency", "aspect"]).collect()
