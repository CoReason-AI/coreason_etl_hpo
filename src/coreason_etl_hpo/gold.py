import polars as pl


def project_dim_hpo_concept(silver_nodes_df: pl.LazyFrame | pl.DataFrame) -> pl.DataFrame:
    """
    Projects the silver HPO nodes into the gold dim_hpo_concept table.
    Filters out obsolete concepts.
    """
    lazy_df = silver_nodes_df.lazy() if isinstance(silver_nodes_df, pl.DataFrame) else silver_nodes_df

    return (
        lazy_df.filter(~pl.col("is_obsolete").fill_null(False))
        .select(["coreason_id", "hp_id", "phenotype_name", "definition", "synonym", "is_a_parent", "type"])
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

    return lazy_df.select(
        [
            "coreason_id",
            "disease_id",
            "disease_name",
            "evidence",
            "frequency",
            "aspect",
            "reference",
            "onset",
            "modifier",
        ]
    ).collect()


def project_obt_hpo_reporting(
    dim_hpo_concept_df: pl.LazyFrame | pl.DataFrame,
    bridge_disease_annotation_df: pl.LazyFrame | pl.DataFrame,
) -> pl.DataFrame:
    """
    Creates a denormalized One Big Table (OBT) from the Gold dimension and bridge tables
    for reporting and spot-checking purposes.
    """
    dim_lazy = dim_hpo_concept_df.lazy() if isinstance(dim_hpo_concept_df, pl.DataFrame) else dim_hpo_concept_df
    bridge_lazy = (
        bridge_disease_annotation_df.lazy()
        if isinstance(bridge_disease_annotation_df, pl.DataFrame)
        else bridge_disease_annotation_df
    )

    # Note: Using left join so we don't lose phenotypes that don't have annotations
    obt_lazy = dim_lazy.join(bridge_lazy, on="coreason_id", how="left")

    return obt_lazy.select(
        [
            pl.col("phenotype_name").alias("name"),
            pl.col("definition"),
            pl.col("hp_id"),
            pl.col("synonym"),
            pl.col("is_a_parent"),
            pl.col("reference"),
            pl.col("onset"),
            pl.col("modifier"),
        ]
    ).collect()
