
use std::path::Path;
use turbovec::io::Durability;
use turbovec::IdMapIndex;

use crate::error::ZenarError;

pub struct VectorStore {
    index: IdMapIndex,
}

impl VectorStore {
    pub fn new(dim: usize, bit_width: usize) -> Result<Self, ZenarError> {
        let index = IdMapIndex::new(dim, bit_width).map_err(|e| ZenarError::InvalidConfig(e.to_string()))?;
        Ok(Self { index })
    }

    pub fn add(&mut self, vectors: &[f32], id: u64) -> Result<(), ZenarError> {
        self.index
        .add_with_ids(vectors, &[id])
        .map_err(|e| match e {
            turbovec::AddError::DimMismatch { existing, got } => ZenarError::InvalidDimension { expected: existing, actual: got },
            turbovec::AddError::IdAlreadyPresent(id) => ZenarError::DuplicateIdError(id),
            turbovec::AddError::InvalidInputValue{ .. } => ZenarError::InvalidVector(e.to_string()),
            _ => ZenarError::VectorStoreError(e.to_string()),
        })
    }

    pub fn remove(&mut self, id: u64) -> bool {
        self.index.remove(id)
    }

    pub fn contains(&self, id: u64) -> bool {
        self.index.contains(id)
    }

    pub fn search(
        &self,
        queries: &[f32],
        k: usize,
        allowlist: Option<&[u64]>,
    ) -> Result<(Vec<f32>, Vec<u64>), ZenarError> {
        self.index
        .search_with_allowlist(queries, k, allowlist)
        .map_err(|e| match e {
            turbovec::SearchError::UnknownId(id) => ZenarError::VectorStoreError(format!("unknown id: {id}")),
            _ => ZenarError::VectorStoreError(e.to_string()),
        })
    }

    pub fn sync(&mut self, path: impl AsRef<Path>) -> Result<(), ZenarError> {
        self.index.sync(path).map_err(|e| ZenarError::VectorStoreError(e.to_string()))
    }

    pub fn write(&self, path: impl AsRef<Path>, durability: Durability) -> Result<(), ZenarError> {
        self.index.write_with_durability(path, durability).map_err(|e| ZenarError::VectorStoreError(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_search_remove() {
        let mut store = VectorStore::new(8, 4).unwrap();
        let vectors = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0];
        let id = 42;

        // Add vector
        store.add(&vectors, id).unwrap();
        assert!(store.contains(id));

        // Search vector
        let (results, ids) = store.search(&vectors, 1, None).unwrap();
        assert_eq!(ids[0], id);
        assert_eq!(results.len(), 1);

        // Remove vector
        assert!(store.remove(id));
        assert!(!store.contains(id));
    }

    #[test]
    fn add_wrong_dimension_fails() {
        let mut store = VectorStore::new(8, 4).unwrap();
        let wrong_size = vec![1.0, 2.0, 3.0]; // only 3 elements, store expects 8
        let result = store.add(&wrong_size, 1);
        assert!(result.is_err());
    }

    #[test]
    fn add_nan_vector_fails() {
        let mut store = VectorStore::new(8, 4).unwrap();
        let mut bad_vector = vec![1.0; 8];
        bad_vector[0] = f32::NAN;
        let result = store.add(&bad_vector, 1);
        assert!(result.is_err());
    }

    #[test]
    fn add_near_zero_vector_succeeds() {
        let mut store = VectorStore::new(8, 4).unwrap();
        let zero_vector = vec![0.0; 8];
        let result = store.add(&zero_vector, 1);
        assert!(result.is_ok());

        let query = vec![1.0; 8];
        let (scores, ids) = store.search(&query, 1, None).unwrap();
        assert_eq!(ids[0], 1);
        assert_eq!(scores[0], 0.0);
    }


}