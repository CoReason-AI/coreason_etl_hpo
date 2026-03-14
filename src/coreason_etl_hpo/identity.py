import polars as pl

# Namespace UUID for HPO
NAMESPACE_HPO = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"


def generate_coreason_id(source_id_col: pl.Expr) -> pl.Expr:
    """
    Generates a deterministic UUID5 hash for the provided source_id column.

    Returns a Polars expression representing the Coreason identity.
    """
    import uuid

    def _hash_batch(s: pl.Series) -> pl.Series:
        # Vectorized generation using the uuid library
        ns = uuid.UUID(NAMESPACE_HPO)
        return pl.Series(name=s.name, values=[str(uuid.uuid5(ns, val)) if val is not None else None for val in s])

    return source_id_col.map_batches(_hash_batch, return_dtype=pl.String)
