//! PyO3 bindings for zenar-core. Everything Python-specific lives here;
//! zenar-core never sees a Python object (spec sections 41, 53, 66).

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyTypeError};
use pyo3::prelude::*;
use pyo3::pyclass::CompareOp;
use pyo3::types::{PyBool, PyDict, PyFloat, PyInt, PyList, PyString};

use zenar_core as core;
use zenar_core::{Filter, MetaValue, ZenarError as CoreError};

// ----- exception hierarchy (spec section 74) ---------------------------------

create_exception!(
    _zenar,
    ZenarError,
    PyException,
    "Base class for all Zenar errors."
);
create_exception!(_zenar, CollectionNotFoundError, ZenarError);
create_exception!(_zenar, CollectionAlreadyExistsError, ZenarError);
create_exception!(_zenar, InvalidCollectionNameError, ZenarError);
create_exception!(_zenar, DimensionMismatchError, ZenarError);
create_exception!(_zenar, DuplicateIdError, ZenarError);
create_exception!(_zenar, VectorNotFoundError, ZenarError);
create_exception!(_zenar, InvalidVectorError, ZenarError);
create_exception!(_zenar, InvalidMetadataError, ZenarError);
create_exception!(_zenar, InvalidFilterError, ZenarError);
create_exception!(_zenar, InvalidArgumentError, ZenarError);
create_exception!(_zenar, DatabaseLockedError, ZenarError);
create_exception!(_zenar, VectorStoreError, ZenarError);
create_exception!(_zenar, MetadataStoreError, ZenarError);
create_exception!(_zenar, PersistenceError, ZenarError);
create_exception!(_zenar, RecoveryError, ZenarError);

/// Maps every zenar-core error variant to exactly one Python exception class.
fn to_py_err(e: CoreError) -> PyErr {
    let msg = e.to_string();
    match e {
        CoreError::CollectionNotFound(_) => CollectionNotFoundError::new_err(msg),
        CoreError::CollectionAlreadyExists(_) => CollectionAlreadyExistsError::new_err(msg),
        CoreError::InvalidCollectionName(_) => InvalidCollectionNameError::new_err(msg),
        CoreError::DimensionMismatch { .. } | CoreError::InvalidDimension(_) => {
            DimensionMismatchError::new_err(msg)
        }
        CoreError::DuplicateId(_) => DuplicateIdError::new_err(msg),
        CoreError::VectorNotFound(_) => VectorNotFoundError::new_err(msg),
        CoreError::InvalidVector(_) => InvalidVectorError::new_err(msg),
        CoreError::InvalidMetadata(_) => InvalidMetadataError::new_err(msg),
        CoreError::InvalidFilter(_) => InvalidFilterError::new_err(msg),
        CoreError::InvalidArgument(_) | CoreError::InvalidConfig(_) => {
            InvalidArgumentError::new_err(msg)
        }
        CoreError::DatabaseLocked(_) => DatabaseLockedError::new_err(msg),
        CoreError::VectorStoreError(_) | CoreError::Panic(_) => VectorStoreError::new_err(msg),
        CoreError::MetadataStoreError(_) => MetadataStoreError::new_err(msg),
        CoreError::PersistenceError(_) => PersistenceError::new_err(msg),
        CoreError::RecoveryError(_) => RecoveryError::new_err(msg),
    }
}

/// Runs a core call with the GIL released and no panic allowed to cross into Python (spec sections 28, 66).
fn guarded<T: Send>(
    py: Python<'_>,
    f: impl FnOnce() -> Result<T, CoreError> + Send,
) -> PyResult<T> {
    let outcome = py.detach(|| match catch_unwind(AssertUnwindSafe(f)) {
        Ok(result) => result,
        Err(payload) => Err(CoreError::Panic(panic_message(&payload))),
    });
    outcome.map_err(to_py_err)
}

fn panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic".to_string())
}

// ----- Python <-> core value conversion --------------------------------------

fn py_to_meta(value: &Bound<'_, PyAny>) -> PyResult<MetaValue> {
    if value.is_instance_of::<PyBool>() {
        return Ok(MetaValue::Boolean(value.extract()?));
    }
    if value.is_instance_of::<PyInt>() {
        let i: i64 = value.extract().map_err(|_| {
            to_py_err(CoreError::InvalidMetadata(
                "integer out of 64-bit range".into(),
            ))
        })?;
        return Ok(MetaValue::Integer(i));
    }
    if value.is_instance_of::<PyFloat>() {
        return Ok(MetaValue::Float(value.extract()?));
    }
    if value.is_instance_of::<PyString>() {
        return Ok(MetaValue::Text(value.extract()?));
    }
    if is_datetime(value)? {
        return Ok(MetaValue::Timestamp(py_datetime_to_utc(value)?));
    }
    Err(to_py_err(CoreError::InvalidMetadata(format!(
        "unsupported metadata type {}",
        value.get_type().name()?
    ))))
}

fn is_datetime(value: &Bound<'_, PyAny>) -> PyResult<bool> {
    let datetime = value.py().import("datetime")?.getattr("datetime")?;
    value.is_instance(&datetime)
}

/// Naive datetimes are rejected rather than assumed to be local time (spec section 76).
fn py_datetime_to_utc(value: &Bound<'_, PyAny>) -> PyResult<DateTime<Utc>> {
    if value.getattr("tzinfo")?.is_none() {
        return Err(to_py_err(CoreError::InvalidMetadata(
            "naive datetime is not allowed; pass a timezone-aware datetime".into(),
        )));
    }
    let micros: f64 = value.call_method0("timestamp")?.extract::<f64>()? * 1_000_000.0;
    DateTime::<Utc>::from_timestamp_micros(micros.round() as i64)
        .ok_or_else(|| to_py_err(CoreError::InvalidMetadata("datetime out of range".into())))
}

fn meta_to_py<'py>(py: Python<'py>, value: &MetaValue) -> PyResult<Bound<'py, PyAny>> {
    Ok(match value {
        MetaValue::Text(s) => PyString::new(py, s).into_any(),
        MetaValue::Integer(i) => i.into_pyobject(py)?.into_any(),
        MetaValue::Float(f) => f.into_pyobject(py)?.into_any(),
        MetaValue::Boolean(b) => PyBool::new(py, *b).to_owned().into_any(),
        MetaValue::Timestamp(t) => {
            let datetime = py.import("datetime")?;
            let tz = datetime.getattr("timezone")?.getattr("utc")?;
            datetime.getattr("datetime")?.call_method1(
                "fromtimestamp",
                (t.timestamp_micros() as f64 / 1_000_000.0, tz),
            )?
        }
    })
}

/// Accepts a list or tuple of floats, or an array-like such as numpy (spec section 83).
/// Array-likes take a byte path: the array is cast to little-endian float32 and copied
/// once. The limited-API build does not expose the buffer protocol, so this is the fast path.
fn extract_vector(value: &Bound<'_, PyAny>) -> PyResult<Vec<f32>> {
    if value.hasattr("tobytes")? && value.hasattr("astype")? {
        let bytes: Vec<u8> = value
            .call_method1("astype", ("<f4",))?
            .call_method0("tobytes")?
            .extract()?;
        let (words, _) = bytes.as_chunks::<4>();
        return Ok(words.iter().map(|w| f32::from_le_bytes(*w)).collect());
    }
    value.extract::<Vec<f32>>()
}

fn hit_to_py<'py>(py: Python<'py>, hit: &core::Hit) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    d.set_item("id", hit.id)?;
    d.set_item("score", hit.score)?;
    d.set_item("text", &hit.text)?;
    d.set_item("metadata", metadata_to_py(py, &hit.metadata)?)?;
    Ok(d)
}

fn doc_to_py<'py>(py: Python<'py>, doc: &core::DocumentRow) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    d.set_item("id", doc.id)?;
    d.set_item("text", &doc.text)?;
    d.set_item("metadata", metadata_to_py(py, &doc.metadata)?)?;
    Ok(d)
}

fn metadata_to_py<'py>(
    py: Python<'py>,
    fields: &[(String, MetaValue)],
) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    for (name, value) in fields {
        d.set_item(name, meta_to_py(py, value)?)?;
    }
    Ok(d)
}

fn metadata_from_py(metadata: Option<&Bound<'_, PyDict>>) -> PyResult<Vec<(String, MetaValue)>> {
    let Some(dict) = metadata else {
        return Ok(Vec::new());
    };
    dict.iter()
        .map(|(k, v)| {
            let key: String = k.extract().map_err(|_| {
                to_py_err(CoreError::InvalidMetadata(
                    "metadata keys must be strings".into(),
                ))
            })?;
            Ok((key, py_to_meta(&v)?))
        })
        .collect()
}

/// Converts a JSON value to Python objects: dicts for objects, lists for arrays.
fn json_to_py<'py>(py: Python<'py>, value: &serde_json::Value) -> PyResult<Bound<'py, PyAny>> {
    use serde_json::Value;
    Ok(match value {
        Value::Null => py.None().into_bound(py),
        Value::Bool(b) => PyBool::new(py, *b).to_owned().into_any(),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                i.into_pyobject(py)?.into_any()
            } else if let Some(u) = n.as_u64() {
                u.into_pyobject(py)?.into_any()
            } else {
                n.as_f64().unwrap_or(f64::NAN).into_pyobject(py)?.into_any()
            }
        }
        Value::String(s) => PyString::new(py, s).into_any(),
        Value::Array(items) => {
            let list = PyList::empty(py);
            for item in items {
                list.append(json_to_py(py, item)?)?;
            }
            list.into_any()
        }
        Value::Object(map) => {
            let dict = PyDict::new(py);
            for (k, v) in map {
                dict.set_item(k, json_to_py(py, v)?)?;
            }
            dict.into_any()
        }
    })
}

/// Converts the `scope` argument. `None` means no scope; callers pass it explicitly.
fn scope_json(
    py: Python<'_>,
    scope: Option<&Bound<'_, PyAny>>,
) -> PyResult<Option<serde_json::Value>> {
    match scope {
        Some(s) if !s.is_none() => Ok(Some(py_to_json(py, s)?)),
        _ => Ok(None),
    }
}

/// Serializes a Python `where` object to JSON with the standard `json` module, so the same
/// JSON-compatible values are accepted here as everywhere else.
fn py_to_json(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<serde_json::Value> {
    let text: String = py
        .import("json")?
        .call_method1("dumps", (value,))
        .map_err(|_| {
            InvalidFilterError::new_err(
                "where must be JSON-compatible (strings, numbers, booleans, lists, dicts)",
            )
        })?
        .extract()?;
    serde_json::from_str(&text).map_err(|e| InvalidFilterError::new_err(format!("where: {e}")))
}

/// Converts a Python integer argument to u64. Negative, oversized, and non-integer
/// values raise `InvalidArgumentError`, not PyO3's `OverflowError` or `TypeError`.
fn py_u64(value: &Bound<'_, PyAny>, what: &str) -> PyResult<u64> {
    let n: i128 = value
        .extract()
        .map_err(|_| InvalidArgumentError::new_err(format!("{what} must be an integer")))?;
    u64::try_from(n).map_err(|_| {
        InvalidArgumentError::new_err(format!("{what} must be between 0 and {}", u64::MAX))
    })
}

/// Same as `py_u64`, for `usize` arguments.
fn py_usize(value: &Bound<'_, PyAny>, what: &str) -> PyResult<usize> {
    let n = py_u64(value, what)?;
    usize::try_from(n).map_err(|_| InvalidArgumentError::new_err(format!("{what} is too large")))
}

// ----- filter expressions (spec sections 9, 10) ------------------------------

/// A compiled filter. Built from `field` comparisons, kwargs, or `&`, `|`, `~`.
#[pyclass(name = "Filter", module = "zenar._zenar", frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct PyFilter {
    inner: Filter,
}

#[pymethods]
impl PyFilter {
    fn __and__(&self, other: &Bound<'_, PyAny>) -> PyResult<PyFilter> {
        let other = other.extract::<PyRef<PyFilter>>()?;
        Ok(PyFilter {
            inner: Filter::And(vec![self.inner.clone(), other.inner.clone()]),
        })
    }

    fn __or__(&self, other: &Bound<'_, PyAny>) -> PyResult<PyFilter> {
        let other = other.extract::<PyRef<PyFilter>>()?;
        Ok(PyFilter {
            inner: Filter::Or(vec![self.inner.clone(), other.inner.clone()]),
        })
    }

    fn __invert__(&self) -> PyFilter {
        PyFilter {
            inner: Filter::Not(Box::new(self.inner.clone())),
        }
    }

    fn __repr__(&self) -> String {
        format!("Filter({:?})", self.inner)
    }

    fn __bool__(&self) -> PyResult<bool> {
        Err(PyTypeError::new_err(
            "a Filter has no truth value; combine filters with &, | and ~ (and parenthesize each comparison)",
        ))
    }
}

/// One field of `field`. Comparison operators build `Filter` values directly.
#[pyclass(name = "Field", module = "zenar._zenar", frozen)]
pub struct PyField {
    name: String,
}

#[pymethods]
impl PyField {
    fn __richcmp__(&self, other: &Bound<'_, PyAny>, op: CompareOp) -> PyResult<PyFilter> {
        let value = py_to_meta(other)?;
        let field = self.name.clone();
        let inner = match op {
            CompareOp::Eq => Filter::Eq { field, value },
            CompareOp::Ne => Filter::Ne { field, value },
            CompareOp::Gt => Filter::Gt { field, value },
            CompareOp::Ge => Filter::Gte { field, value },
            CompareOp::Lt => Filter::Lt { field, value },
            CompareOp::Le => Filter::Lte { field, value },
        };
        Ok(PyFilter { inner })
    }

    fn isin(&self, values: &Bound<'_, PyAny>) -> PyResult<PyFilter> {
        let values = values
            .try_iter()?
            .map(|v| py_to_meta(&v?))
            .collect::<PyResult<Vec<_>>>()?;
        Ok(PyFilter {
            inner: Filter::In {
                field: self.name.clone(),
                values,
            },
        })
    }

    fn __repr__(&self) -> String {
        format!("field.{}", self.name)
    }

    fn __hash__(&self) -> PyResult<isize> {
        Err(PyTypeError::new_err(
            "unhashable: field comparisons build filters",
        ))
    }
}

/// The `field` namespace: `field.price >= 100` returns a `Field` for `price`.
#[pyclass(name = "FieldNamespace", module = "zenar._zenar", frozen)]
pub struct PyFieldNamespace;

#[pymethods]
impl PyFieldNamespace {
    fn __getattr__(&self, name: &str) -> PyResult<PyField> {
        if name.starts_with('_') {
            return Err(pyo3::exceptions::PyAttributeError::new_err(
                name.to_string(),
            ));
        }
        Ok(PyField {
            name: name.to_string(),
        })
    }

    fn __call__(&self, name: &str) -> PyField {
        PyField {
            name: name.to_string(),
        }
    }
}

// ----- database and collection -----------------------------------------------

#[pyclass(name = "Collection", module = "zenar._zenar", frozen)]
pub struct PyCollection {
    inner: Arc<core::Collection>,
}

#[pymethods]
impl PyCollection {
    #[getter]
    fn name(&self) -> String {
        self.inner.name().to_string()
    }

    #[getter]
    fn dimension(&self) -> Option<usize> {
        self.inner.dimension()
    }

    #[getter]
    fn bit_width(&self) -> usize {
        self.inner.bit_width()
    }

    #[pyo3(signature = (id, vector, text, metadata=None))]
    fn add(
        &self,
        py: Python<'_>,
        id: &Bound<'_, PyAny>,
        vector: &Bound<'_, PyAny>,
        text: &str,
        metadata: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<()> {
        let id = py_u64(id, "id")?;
        let vector = extract_vector(vector)?;
        let metadata = metadata_from_py(metadata)?;
        let text = text.to_string();
        let inner = Arc::clone(&self.inner);
        guarded(py, move || inner.add(id, &vector, &text, metadata))
    }

    /// Adds records given as dicts with keys `id`, `vector`, `text`, and optional `metadata`.
    /// Atomic: either all records are added, or none is.
    fn add_many(&self, py: Python<'_>, records: &Bound<'_, PyAny>) -> PyResult<usize> {
        // Parse every record before the core call, so a malformed record fails before any write.
        let mut parsed = Vec::new();
        for record in records.try_iter()? {
            let record = record?;
            let dict = record.cast::<PyDict>()?;
            let id = py_u64(&required(dict, "id")?, "id")?;
            let vector = extract_vector(&required(dict, "vector")?)?;
            let text: String = required(dict, "text")?.extract()?;
            let metadata = match dict.get_item("metadata")? {
                Some(m) => metadata_from_py(Some(m.cast::<PyDict>()?))?,
                None => Vec::new(),
            };
            parsed.push(core::Record {
                id,
                vector,
                text,
                metadata,
            });
        }
        let inner = Arc::clone(&self.inner);
        guarded(py, move || inner.add_many(&parsed))
    }

    fn get<'py>(
        &self,
        py: Python<'py>,
        id: &Bound<'_, PyAny>,
    ) -> PyResult<Option<Bound<'py, PyDict>>> {
        let id = py_u64(id, "id")?;
        let inner = Arc::clone(&self.inner);
        match guarded(py, move || inner.get(id))? {
            Some(doc) => Ok(Some(doc_to_py(py, &doc)?)),
            None => Ok(None),
        }
    }

    fn get_many<'py>(
        &self,
        py: Python<'py>,
        ids: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyList>> {
        let ids = ids
            .try_iter()?
            .map(|id| py_u64(&id?, "id"))
            .collect::<PyResult<Vec<u64>>>()?;
        let inner = Arc::clone(&self.inner);
        let docs = guarded(py, move || inner.get_many(&ids))?;
        let out = PyList::empty(py);
        for doc in &docs {
            out.append(doc_to_py(py, doc)?)?;
        }
        Ok(out)
    }

    fn delete(&self, py: Python<'_>, id: &Bound<'_, PyAny>) -> PyResult<()> {
        let id = py_u64(id, "id")?;
        let inner = Arc::clone(&self.inner);
        guarded(py, move || inner.delete(id))
    }

    /// Deletes many ids atomically: either all exist and are deleted, or none is.
    fn delete_many(&self, py: Python<'_>, ids: &Bound<'_, PyAny>) -> PyResult<usize> {
        let ids = ids
            .try_iter()?
            .map(|id| py_u64(&id?, "id"))
            .collect::<PyResult<Vec<u64>>>()?;
        let inner = Arc::clone(&self.inner);
        guarded(py, move || inner.delete_many(&ids))
    }

    /// Field-level description for discovery: every metadata field with its type, the number of
    /// documents that carry it, and its distinct values with counts (or range for long lists).
    /// `scope` is required. Pass a dict to limit what is listed, for example the tenant. Pass
    /// `None` only from trusted code that may see the whole collection.
    #[pyo3(signature = (*, scope))]
    fn describe<'py>(
        &self,
        py: Python<'py>,
        scope: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let scope = scope_json(py, scope)?;
        let inner = Arc::clone(&self.inner);
        let info = guarded(py, move || inner.describe(scope.as_ref()))?;
        json_to_py(py, &info)
    }

    /// Finds values of one field that contain `text` (case-insensitive), most frequent first,
    /// within `scope`.
    #[pyo3(signature = (field, text, limit=20, *, scope))]
    fn find_values<'py>(
        &self,
        py: Python<'py>,
        field: &str,
        text: &str,
        limit: usize,
        scope: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let scope = scope_json(py, scope)?;
        let inner = Arc::clone(&self.inner);
        let field = field.to_string();
        let text = text.to_string();
        let found = guarded(py, move || {
            inner.find_values(&field, &text, limit, scope.as_ref())
        })?;
        json_to_py(py, &serde_json::Value::Array(found))
    }

    /// Filtered retrieval for an agent. `where` is the model's filter, a JSON-compatible dict such
    /// as `{"tenant": "A", "price": {"gte": 100}}`. `scope` is the caller's fixed filter. The two
    /// are combined on the server, so `where` cannot widen `scope`. `scope` is required.
    /// Returns `{"hits", "matched", "warnings"}`. `matched` counts every document that matched,
    /// before `top_k` cut the list.
    #[pyo3(signature = (vector, top_k=10, r#where=None, *, scope))]
    fn retrieve<'py>(
        &self,
        py: Python<'py>,
        vector: &Bound<'_, PyAny>,
        top_k: usize,
        r#where: Option<&Bound<'_, PyAny>>,
        scope: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let vector = extract_vector(vector)?;
        let where_json = match r#where {
            Some(w) if !w.is_none() => Some(py_to_json(py, w)?),
            _ => None,
        };
        let scope = scope_json(py, scope)?;
        let inner = Arc::clone(&self.inner);
        let retrieval = guarded(py, move || {
            inner.retrieve(&vector, top_k, where_json.as_ref(), scope.as_ref())
        })?;
        let hits = PyList::empty(py);
        for hit in &retrieval.hits {
            hits.append(hit_to_py(py, hit)?)?;
        }
        let out = PyDict::new(py);
        out.set_item("hits", hits)?;
        out.set_item("matched", retrieval.matched)?;
        out.set_item("warnings", retrieval.warnings)?;
        Ok(out.into_any())
    }

    /// Tool definition for this collection within `scope`, in the `name` / `description` /
    /// `parameters` shape, as a `zenar.agent.ToolSpec` (a dict with `create_prompt()`).
    /// With `prompt_only=True`, returns just the fields and their values as text.
    #[pyo3(signature = (name, prompt_only=false, *, scope))]
    fn tool_spec<'py>(
        &self,
        py: Python<'py>,
        name: &str,
        prompt_only: bool,
        scope: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let scope = scope_json(py, scope)?;
        if prompt_only {
            let inner = Arc::clone(&self.inner);
            let text = guarded(py, move || inner.fields_prompt(scope.as_ref()))?;
            return Ok(PyString::new(py, &text).into_any());
        }
        let inner = Arc::clone(&self.inner);
        let name = name.to_string();
        let spec = guarded(py, move || inner.tool_spec(&name, scope.as_ref()))?;
        // Wrap in zenar.agent.ToolSpec: a dict that also renders itself as a prompt.
        let dict = json_to_py(py, &spec)?;
        py.import("zenar.agent")?
            .getattr("ToolSpec")?
            .call1((dict,))
    }

    fn count(&self, py: Python<'_>) -> PyResult<usize> {
        let inner = Arc::clone(&self.inner);
        guarded(py, move || inner.count())
    }

    /// Fits the quantizer from a flat sample of vectors (n * dimension floats).
    fn calibrate(&self, py: Python<'_>, sample: &Bound<'_, PyAny>) -> PyResult<()> {
        let sample = extract_vector(sample)?;
        let inner = Arc::clone(&self.inner);
        guarded(py, move || inner.calibrate(&sample))
    }

    fn stats<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let inner = Arc::clone(&self.inner);
        let count = guarded(py, move || inner.count())?;
        let d = PyDict::new(py);
        d.set_item("name", self.inner.name())?;
        d.set_item("count", count)?;
        d.set_item("dimension", self.inner.dimension())?;
        d.set_item("bit_width", self.inner.bit_width())?;
        Ok(d)
    }

    /// Top-`top_k` search. `filter=` takes a `field` expression; keyword
    /// arguments are equality conditions ANDed together. The two are mutually exclusive.
    #[pyo3(signature = (vector, top_k=None, filter=None, **kwargs))]
    fn search<'py>(
        &self,
        py: Python<'py>,
        vector: &Bound<'_, PyAny>,
        top_k: Option<&Bound<'_, PyAny>>,
        filter: Option<&Bound<'_, PyAny>>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Bound<'py, PyList>> {
        let vector = extract_vector(vector)?;
        let top_k = match top_k {
            Some(k) => py_usize(k, "top_k")?,
            None => 10,
        };
        let compiled = build_filter(filter, kwargs)?;
        let inner = Arc::clone(&self.inner);
        let hits = guarded(py, move || inner.search(&vector, top_k, compiled.as_ref()))?;
        let out = PyList::empty(py);
        for hit in &hits {
            out.append(hit_to_py(py, hit)?)?;
        }
        Ok(out)
    }

    fn __repr__(&self) -> String {
        format!("Collection({:?})", self.inner.name())
    }
}

fn required<'py>(dict: &Bound<'py, PyDict>, key: &str) -> PyResult<Bound<'py, PyAny>> {
    dict.get_item(key)?
        .ok_or_else(|| InvalidArgumentError::new_err(format!("record is missing {key:?}")))
}

fn build_filter(
    filter: Option<&Bound<'_, PyAny>>,
    kwargs: Option<&Bound<'_, PyDict>>,
) -> PyResult<Option<Filter>> {
    let has_kwargs = kwargs.is_some_and(|k| !k.is_empty());
    match (filter, has_kwargs) {
        (Some(_), true) => Err(InvalidFilterError::new_err(
            "pass either filter= or keyword conditions, not both",
        )),
        (Some(f), false) => {
            if f.is_none() {
                return Ok(None);
            }
            let f = f.extract::<PyRef<PyFilter>>().map_err(|_| {
                InvalidFilterError::new_err("filter= must be built from field comparisons")
            })?;
            Ok(Some(f.inner.clone()))
        }
        (None, true) => {
            let mut parts = Vec::new();
            for (k, v) in kwargs.expect("checked above").iter() {
                let field: String = k.extract()?;
                parts.push(Filter::Eq {
                    field,
                    value: py_to_meta(&v)?,
                });
            }
            Ok(Some(Filter::And(parts)))
        }
        (None, false) => Ok(None),
    }
}

#[pyclass(name = "VectorDB", module = "zenar._zenar")]
pub struct PyVectorDB {
    inner: Option<Arc<core::VectorDB>>,
}

impl PyVectorDB {
    fn db(&self) -> PyResult<&Arc<core::VectorDB>> {
        self.inner
            .as_ref()
            .ok_or_else(|| PersistenceError::new_err("database is closed"))
    }
}

#[pymethods]
impl PyVectorDB {
    /// `durable=True` syncs the index and the document log on every add: power-safe per add, at a
    /// much lower ingest rate. The default syncs at sync boundaries. `flush()` makes everything before it durable.
    #[new]
    #[pyo3(signature = (path, durable=false))]
    fn new(py: Python<'_>, path: &str, durable: bool) -> PyResult<Self> {
        let path = path.to_string();
        let sync = if durable {
            core::MetadataSync::Full
        } else {
            core::MetadataSync::Normal
        };
        let db = guarded(py, move || core::VectorDB::open_with(path, sync))?;
        Ok(Self {
            inner: Some(Arc::new(db)),
        })
    }

    #[pyo3(signature = (name, dimension=None, bit_width=None))]
    fn create_collection(
        &self,
        py: Python<'_>,
        name: &str,
        dimension: Option<&Bound<'_, PyAny>>,
        bit_width: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyCollection> {
        let dimension = match dimension {
            Some(d) if !d.is_none() => Some(py_usize(d, "dimension")?),
            _ => None,
        };
        let bit_width = match bit_width {
            Some(b) => py_usize(b, "bit_width")?,
            None => 4,
        };
        let db = Arc::clone(self.db()?);
        let name = name.to_string();
        let inner = guarded(py, move || {
            db.create_collection(&name, dimension, bit_width)
        })?;
        Ok(PyCollection { inner })
    }

    fn get_collection(&self, py: Python<'_>, name: &str) -> PyResult<PyCollection> {
        let db = Arc::clone(self.db()?);
        let name = name.to_string();
        let inner = guarded(py, move || db.get_collection(&name))?;
        Ok(PyCollection { inner })
    }

    fn list_collections(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        let db = Arc::clone(self.db()?);
        guarded(py, move || db.list_collections())
    }

    fn drop_collection(&self, py: Python<'_>, name: &str) -> PyResult<()> {
        let db = Arc::clone(self.db()?);
        let name = name.to_string();
        guarded(py, move || db.drop_collection(&name))
    }

    /// Re-runs crash reconciliation for one collection and rewrites its index file.
    fn rebuild<'py>(&self, py: Python<'py>, name: &str) -> PyResult<Bound<'py, PyDict>> {
        let db = Arc::clone(self.db()?);
        let name = name.to_string();
        let report = guarded(py, move || {
            let collection = db.get_collection(&name)?;
            collection.reconcile()
        })?;
        report_to_py(py, report)
    }

    fn recovery_report<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        report_to_py(py, self.db()?.recovery_report())
    }

    fn stats<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let db = Arc::clone(self.db()?);
        let names = guarded(py, move || db.list_collections())?;
        let d = PyDict::new(py);
        d.set_item("path", self.db()?.path().display().to_string())?;
        let collections = PyDict::new(py);
        for name in names {
            let db = Arc::clone(self.db()?);
            let lookup = name.clone();
            let count = guarded(py, move || db.get_collection(&lookup)?.count())?;
            let entry = PyDict::new(py);
            entry.set_item("count", count)?;
            collections.set_item(name, entry)?;
        }
        d.set_item("collections", collections)?;
        Ok(d)
    }

    /// Makes every add so far durable: the index and the document log are synced.
    fn flush(&self, py: Python<'_>) -> PyResult<()> {
        let db = Arc::clone(self.db()?);
        guarded(py, move || db.flush())
    }

    /// Flushes, then releases the database and its lock.
    fn close(&mut self, py: Python<'_>) -> PyResult<()> {
        if let Some(db) = self.inner.take() {
            guarded(py, move || db.flush())?;
        }
        Ok(())
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    #[pyo3(signature = (_exc_type=None, _exc=None, _tb=None))]
    fn __exit__(
        &mut self,
        py: Python<'_>,
        _exc_type: Option<Py<PyAny>>,
        _exc: Option<Py<PyAny>>,
        _tb: Option<Py<PyAny>>,
    ) -> PyResult<bool> {
        self.close(py)?;
        Ok(false)
    }
}

fn report_to_py<'py>(py: Python<'py>, r: core::RecoveryReport) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    d.set_item("orphaned_rows_removed", r.orphaned_rows_removed)?;
    d.set_item("orphaned_vectors_removed", r.orphaned_vectors_removed)?;
    Ok(d)
}

/// Test hook: raises a panic inside a guarded core call, to verify that a
/// panic becomes a Python exception instead of aborting the process.
#[pyfunction]
fn _panic_for_test(py: Python<'_>) -> PyResult<()> {
    guarded(py, || -> Result<(), CoreError> {
        panic!("deliberate panic for boundary test")
    })
}

#[pymodule]
fn _zenar(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyVectorDB>()?;
    m.add_class::<PyCollection>()?;
    m.add_class::<PyFilter>()?;
    m.add_class::<PyField>()?;
    m.add_class::<PyFieldNamespace>()?;
    m.add("field", PyFieldNamespace)?;
    m.add("ZenarError", m.py().get_type::<ZenarError>())?;
    m.add(
        "CollectionNotFoundError",
        m.py().get_type::<CollectionNotFoundError>(),
    )?;
    m.add(
        "CollectionAlreadyExistsError",
        m.py().get_type::<CollectionAlreadyExistsError>(),
    )?;
    m.add(
        "InvalidCollectionNameError",
        m.py().get_type::<InvalidCollectionNameError>(),
    )?;
    m.add(
        "DimensionMismatchError",
        m.py().get_type::<DimensionMismatchError>(),
    )?;
    m.add("DuplicateIdError", m.py().get_type::<DuplicateIdError>())?;
    m.add(
        "VectorNotFoundError",
        m.py().get_type::<VectorNotFoundError>(),
    )?;
    m.add(
        "InvalidVectorError",
        m.py().get_type::<InvalidVectorError>(),
    )?;
    m.add(
        "InvalidMetadataError",
        m.py().get_type::<InvalidMetadataError>(),
    )?;
    m.add(
        "InvalidFilterError",
        m.py().get_type::<InvalidFilterError>(),
    )?;
    m.add(
        "InvalidArgumentError",
        m.py().get_type::<InvalidArgumentError>(),
    )?;
    m.add(
        "DatabaseLockedError",
        m.py().get_type::<DatabaseLockedError>(),
    )?;
    m.add("VectorStoreError", m.py().get_type::<VectorStoreError>())?;
    m.add(
        "MetadataStoreError",
        m.py().get_type::<MetadataStoreError>(),
    )?;
    m.add("PersistenceError", m.py().get_type::<PersistenceError>())?;
    m.add("RecoveryError", m.py().get_type::<RecoveryError>())?;
    m.add_function(wrap_pyfunction!(_panic_for_test, m)?)?;
    Ok(())
}
