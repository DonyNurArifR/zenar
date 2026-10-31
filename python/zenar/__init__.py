"""Zenar: an embedded vector database with typed metadata filtering.

    from zenar import VectorDB, field as f

    db = VectorDB("./data")
    docs = db.create_collection("docs", dimension=3)
    docs.add(id=1, vector=[1.0, 0.0, 0.0], text="hello", metadata={"tenant": "A"})
    docs.search([1.0, 0.0, 0.0], top_k=1, filter=f.tenant == "A")
"""

from ._zenar import (
    CollectionAlreadyExistsError,
    CollectionNotFoundError,
    Collection,
    DatabaseLockedError,
    DimensionMismatchError,
    DuplicateIdError,
    InvalidArgumentError,
    InvalidCollectionNameError,
    InvalidFilterError,
    InvalidMetadataError,
    InvalidVectorError,
    MetadataStoreError,
    PersistenceError,
    RecoveryError,
    VectorDB,
    VectorNotFoundError,
    VectorStoreError,
    ZenarError,
    field,
)

__all__ = [
    "Collection",
    "CollectionAlreadyExistsError",
    "CollectionNotFoundError",
    "DatabaseLockedError",
    "DimensionMismatchError",
    "DuplicateIdError",
    "InvalidArgumentError",
    "InvalidCollectionNameError",
    "InvalidFilterError",
    "InvalidMetadataError",
    "InvalidVectorError",
    "MetadataStoreError",
    "PersistenceError",
    "RecoveryError",
    "VectorDB",
    "VectorNotFoundError",
    "VectorStoreError",
    "ZenarError",
    "field",
]

__version__ = "0.1.1"
