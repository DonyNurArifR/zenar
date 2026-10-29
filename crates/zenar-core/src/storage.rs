use std::borrow::Cow;
use std::path::Path;

use turbovec::io::Durability;
use turbovec::{AddError, IdMapIndex, MAX_DIM, SearchError};

use crate::error::ZenarError;

/// Vector magnitudes at or above this are rejected by TurboVec (spec section 69).
pub const MAX_ABS_VALUE: f32 = 1e16;

/// Wraps a TurboVec `IdMapIndex`. All input is validated here before it can
/// reach TurboVec, so no user input turns into a panic (spec sections 66, 69).
///
/// TurboVec requires its internal dimension to be a multiple of 8. Vectors
/// whose logical dimension is not are zero-padded up to the next multiple of
/// 8 on the way in and on the way out of search. Zero padding adds nothing to
/// dot products or norms, so scores are exact. Callers only ever see the
/// logical dimension.
pub struct VectorStore {
    index: IdMapIndex,
    /// The dimension callers use. `None` until the first add of a lazy index.
    logical_dim: Option<usize>,
}

impl VectorStore {
    /// An index whose dimension is locked by the first `add` (spec section 48).
    pub fn new_lazy(bit_width: usize) -> Result<Self, ZenarError> {
        let index = IdMapIndex::new_lazy(bit_width).map_err(construct_err)?;
        Ok(Self {
            index,
            logical_dim: None,
        })
    }

    pub fn new(dim: usize, bit_width: usize) -> Result<Self, ZenarError> {
        validate_dimension(dim)?;
        let index = IdMapIndex::new(padded(dim), bit_width).map_err(construct_err)?;
        Ok(Self {
            index,
            logical_dim: Some(dim),
        })
    }

    /// Loads a previously synced index. The file stores the padded dimension,
    /// so the logical one comes from the collection record.
    pub fn load(path: impl AsRef<Path>, logical_dim: Option<usize>) -> Result<Self, ZenarError> {
        let index =
            IdMapIndex::load(path).map_err(|e| ZenarError::PersistenceError(e.to_string()))?;
        // Build the search caches now, so the first query after open is not slow.
        index.prepare();
        let logical_dim = logical_dim.or_else(|| index.dim_opt());
        Ok(Self { index, logical_dim })
    }

    pub fn dim(&self) -> Option<usize> {
        self.logical_dim
    }

    pub fn bit_width(&self) -> usize {
        self.index.bit_width()
    }

    pub fn len(&self) -> usize {
        self.index.len()
    }

    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    /// Adds one vector. The first successful add on a lazy index locks its dimension.
    pub fn add(&mut self, vector: &[f32], id: u64) -> Result<(), ZenarError> {
        validate_vector(vector, self.logical_dim)?;
        let dim = vector.len();
        let row = pad(vector, dim);
        self.index
            .add_with_ids_2d(&row, padded(dim), &[id])
            .map_err(|e| match e {
                AddError::DimMismatch { existing, got } => ZenarError::DimensionMismatch {
                    expected: existing,
                    actual: got,
                },
                AddError::IdAlreadyPresent(id) => ZenarError::DuplicateId(id),
                AddError::DimNotMultipleOf8(d) => ZenarError::InvalidDimension(d),
                AddError::DimTooLarge { dim, .. } => ZenarError::InvalidDimension(dim),
                other => ZenarError::VectorStoreError(other.to_string()),
            })?;
        self.logical_dim.get_or_insert(dim);
        Ok(())
    }

    /// Removes a vector. Returns false if the id is not present.
    pub fn remove(&mut self, id: u64) -> bool {
        self.index.remove(id)
    }

    pub fn contains(&self, id: u64) -> bool {
        self.index.contains(id)
    }

    /// Every id currently in the index.
    pub fn ids(&self) -> impl Iterator<Item = u64> + '_ {
        self.index.iter_ids()
    }

    /// Top-`k` search, optionally restricted to `allowlist`. Every id in the
    /// allowlist must be present in the index (TurboVec errors otherwise), so
    /// callers filter stale ids first (spec section 89). An empty allowlist
    /// returns no results.
    pub fn search(
        &self,
        query: &[f32],
        k: usize,
        allowlist: Option<&[u64]>,
    ) -> Result<(Vec<f32>, Vec<u64>), ZenarError> {
        validate_vector(query, self.logical_dim)?;
        if k == 0 {
            return Ok((Vec::new(), Vec::new()));
        }
        if matches!(allowlist, Some(ids) if ids.is_empty()) {
            return Ok((Vec::new(), Vec::new()));
        }
        let row = pad(query, query.len());
        self.index
            .search_with_allowlist(&row, k, allowlist)
            .map_err(|e| match e {
                SearchError::UnknownId(id) => {
                    ZenarError::VectorStoreError(format!("unknown id in allowlist: {id}"))
                }
                other => ZenarError::VectorStoreError(other.to_string()),
            })
    }

    /// Fits the TQ+ calibration from a flat sample of vectors (spec section 88, Option 3).
    pub fn calibrate(&mut self, sample: &[f32]) -> Result<(), ZenarError> {
        let dim = self.logical_dim.ok_or_else(|| {
            ZenarError::InvalidArgument(
                "collection has no vectors yet; add vectors before calibrating".into(),
            )
        })?;
        if sample.is_empty() || !sample.len().is_multiple_of(dim) {
            return Err(ZenarError::DimensionMismatch {
                expected: dim,
                actual: sample.len() % dim,
            });
        }
        sample.iter().try_for_each(|v| check_value(*v))?;
        let mut rows = Vec::with_capacity(sample.len() / dim * padded(dim));
        for row in sample.chunks_exact(dim) {
            rows.extend_from_slice(&pad(row, dim));
        }
        self.index
            .calibrate(&rows)
            .map_err(|e| ZenarError::InvalidArgument(e.to_string()))
    }

    /// Incremental persist to `path`, durable on return (fsync + atomic rename).
    pub fn sync(&mut self, path: impl AsRef<Path>) -> Result<(), ZenarError> {
        self.index
            .sync(path)
            .map_err(|e| ZenarError::PersistenceError(e.to_string()))
    }

    pub fn write(&self, path: impl AsRef<Path>, durability: Durability) -> Result<(), ZenarError> {
        self.index
            .write_with_durability(path, durability)
            .map_err(|e| ZenarError::PersistenceError(e.to_string()))
    }
}

/// Logical dimensions must be positive and fit TurboVec once padded to a multiple of 8.
pub fn validate_dimension(dim: usize) -> Result<(), ZenarError> {
    if dim == 0 || padded(dim) > MAX_DIM {
        return Err(ZenarError::InvalidDimension(dim));
    }
    Ok(())
}

/// The dimension TurboVec stores: the next multiple of 8.
fn padded(dim: usize) -> usize {
    dim.div_ceil(8) * 8
}

/// Returns `v` zero-extended to `padded(dim)` values, borrowing when no padding is needed.
fn pad(v: &[f32], dim: usize) -> Cow<'_, [f32]> {
    let target = padded(dim);
    if v.len() == target {
        Cow::Borrowed(v)
    } else {
        let mut out = Vec::with_capacity(target);
        out.extend_from_slice(v);
        out.resize(target, 0.0);
        Cow::Owned(out)
    }
}

/// Checks a vector's length against `expected` (when known) and its values.
pub fn validate_vector(vector: &[f32], expected: Option<usize>) -> Result<(), ZenarError> {
    if vector.is_empty() {
        return Err(ZenarError::InvalidVector("vector is empty".into()));
    }
    match expected {
        Some(dim) if vector.len() != dim => {
            return Err(ZenarError::DimensionMismatch {
                expected: dim,
                actual: vector.len(),
            });
        }
        Some(_) => {}
        None => validate_dimension(vector.len())?,
    }
    vector.iter().try_for_each(|v| check_value(*v))
}

fn check_value(v: f32) -> Result<(), ZenarError> {
    if !v.is_finite() {
        return Err(ZenarError::InvalidVector(
            "values must be finite (no NaN or Inf)".into(),
        ));
    }
    if v.abs() >= MAX_ABS_VALUE {
        return Err(ZenarError::InvalidVector(format!(
            "value magnitude must be below {MAX_ABS_VALUE:e}"
        )));
    }
    Ok(())
}

fn construct_err(e: turbovec::ConstructError) -> ZenarError {
    match e {
        turbovec::ConstructError::DimNotPositiveMultipleOf8(d)
        | turbovec::ConstructError::DimTooLarge { dim: d, .. } => ZenarError::InvalidDimension(d),
        other => ZenarError::InvalidConfig(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_search_remove_roundtrip() {
        let mut store = VectorStore::new_lazy(4).unwrap();
        let v = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0];
        store.add(&v, 42).unwrap();
        assert!(store.contains(42));
        let (_, ids) = store.search(&v, 1, None).unwrap();
        assert_eq!(ids[0], 42);
        assert!(store.remove(42));
        assert!(!store.contains(42));
    }

    #[test]
    fn lazy_dimension_is_locked_by_first_add() {
        let mut store = VectorStore::new_lazy(4).unwrap();
        assert_eq!(store.dim(), None);
        store.add(&[0.5; 16], 1).unwrap();
        assert_eq!(store.dim(), Some(16));
        assert!(matches!(
            store.add(&[0.5; 8], 2),
            Err(ZenarError::DimensionMismatch {
                expected: 16,
                actual: 8
            })
        ));
    }

    #[test]
    fn non_multiple_of_8_dimension_is_padded_and_ranks_correctly() {
        let mut store = VectorStore::new(3, 4).unwrap();
        assert_eq!(store.dim(), Some(3));
        store.add(&[1.0, 0.0, 0.0], 1).unwrap();
        store.add(&[0.0, 1.0, 0.0], 2).unwrap();
        let (_, ids) = store.search(&[1.0, 0.0, 0.0], 1, None).unwrap();
        assert_eq!(ids[0], 1);
    }

    #[test]
    fn dimension_above_max_rejected() {
        assert!(matches!(
            VectorStore::new(20000, 4),
            Err(ZenarError::InvalidDimension(20000))
        ));
        assert!(matches!(
            VectorStore::new(0, 4),
            Err(ZenarError::InvalidDimension(0))
        ));
    }

    #[test]
    fn wrong_length_rejected_against_locked_dimension() {
        let mut store = VectorStore::new(8, 4).unwrap();
        assert!(matches!(
            store.add(&[1.0, 2.0, 3.0], 1),
            Err(ZenarError::DimensionMismatch { .. })
        ));
    }

    #[test]
    fn nan_and_inf_rejected() {
        let mut store = VectorStore::new(8, 4).unwrap();
        let mut bad = vec![1.0; 8];
        bad[0] = f32::NAN;
        assert!(matches!(
            store.add(&bad, 1),
            Err(ZenarError::InvalidVector(_))
        ));
        bad[0] = f32::INFINITY;
        assert!(matches!(
            store.add(&bad, 1),
            Err(ZenarError::InvalidVector(_))
        ));
    }

    #[test]
    fn huge_magnitude_rejected() {
        let mut store = VectorStore::new(8, 4).unwrap();
        let mut bad = vec![1.0; 8];
        bad[3] = 1e17;
        assert!(matches!(
            store.add(&bad, 1),
            Err(ZenarError::InvalidVector(_))
        ));
    }

    #[test]
    fn duplicate_id_rejected() {
        let mut store = VectorStore::new(8, 4).unwrap();
        store.add(&[1.0; 8], 5).unwrap();
        assert_eq!(store.add(&[1.0; 8], 5), Err(ZenarError::DuplicateId(5)));
    }

    #[test]
    fn near_zero_vector_accepted_and_scores_zero() {
        let mut store = VectorStore::new(8, 4).unwrap();
        store.add(&[0.0; 8], 1).unwrap();
        let (scores, ids) = store.search(&[1.0; 8], 1, None).unwrap();
        assert_eq!(ids[0], 1);
        assert_eq!(scores[0], 0.0);
    }

    #[test]
    fn stale_allowlist_id_is_an_error_at_this_layer() {
        let mut store = VectorStore::new(8, 4).unwrap();
        store.add(&[1.0; 8], 1).unwrap();
        assert!(store.search(&[1.0; 8], 1, Some(&[1, 999])).is_err());
    }

    #[test]
    fn sync_and_load_roundtrip_keeps_logical_dimension() {
        let dir = std::env::temp_dir().join(format!("zenar-storage-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("idx.tvim");
        let mut store = VectorStore::new(3, 4).unwrap();
        store.add(&[1.0, 2.0, 3.0], 9).unwrap();
        store.sync(&path).unwrap();
        let loaded = VectorStore::load(&path, Some(3)).unwrap();
        assert!(loaded.contains(9));
        assert_eq!(loaded.dim(), Some(3));
        std::fs::remove_dir_all(&dir).ok();
    }
}
