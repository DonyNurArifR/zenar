//! End-to-end tests through the public zenar-core API only.

use std::fs;
use std::path::PathBuf;

use zenar_core::{Filter, MetaValue, VectorDB, ZenarError};

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("zenar-it-{tag}-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    dir
}

fn text(s: &str) -> MetaValue {
    MetaValue::Text(s.to_string())
}

/// Spec section 39: add one vector, search for it, get it back.
#[test]
fn vertical_slice_add_then_search() {
    let dir = temp_dir("slice");
    let db = VectorDB::open(&dir).unwrap();
    let c = db.create_collection("docs", Some(8), 4).unwrap();
    c.add(
        1,
        &[1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        "hello",
        vec![("tenant".into(), text("A"))],
    )
    .unwrap();
    let hits = c
        .search(&[1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], 1, None)
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].id, 1);
    assert_eq!(hits[0].text, "hello");
    assert_eq!(hits[0].metadata, vec![("tenant".to_string(), text("A"))]);
    fs::remove_dir_all(&dir).ok();
}

/// Spec section 33: 100 docs, 50 tenant A, 50 tenant B. Every filtered result is tenant A.
#[test]
fn tenant_isolation_never_leaks() {
    let dir = temp_dir("tenant");
    let db = VectorDB::open(&dir).unwrap();
    let c = db.create_collection("tenants", Some(16), 4).unwrap();
    for id in 0..100u64 {
        let tenant = if id % 2 == 0 { "A" } else { "B" };
        let vector: Vec<f32> = (0..16).map(|j| ((id * 7 + j) % 13) as f32 + 0.5).collect();
        c.add(
            id,
            &vector,
            &format!("doc {id}"),
            vec![("tenant".into(), text(tenant))],
        )
        .unwrap();
    }
    let query: Vec<f32> = (0..16).map(|j| j as f32 + 1.0).collect();
    let f = Filter::Eq {
        field: "tenant".into(),
        value: text("A"),
    };
    let hits = c.search(&query, 10, Some(&f)).unwrap();
    assert_eq!(hits.len(), 10);
    assert!(
        hits.iter().all(|h| h.id % 2 == 0),
        "tenant B leaked into results"
    );
    assert!(
        hits.iter()
            .all(|h| h.metadata == vec![("tenant".to_string(), text("A"))])
    );
    fs::remove_dir_all(&dir).ok();
}

#[test]
fn filter_operators_and_combinators() {
    let dir = temp_dir("ops");
    let db = VectorDB::open(&dir).unwrap();
    let c = db.create_collection("shop", Some(8), 4).unwrap();
    let categories = ["faq", "product", "tools", "electronics"];
    for id in 0..20u64 {
        let v: Vec<f32> = (0..8).map(|j| (id + j) as f32 + 1.0).collect();
        c.add(
            id,
            &v,
            "t",
            vec![
                ("category".into(), text(categories[(id % 4) as usize])),
                ("price".into(), MetaValue::Float(id as f64 * 10.0)),
                (
                    "language".into(),
                    text(if id % 5 == 0 { "fr" } else { "en" }),
                ),
                ("active".into(), MetaValue::Boolean(id % 3 != 0)),
            ],
        )
        .unwrap();
    }
    let q = vec![1.0; 8];

    let gte = Filter::Gte {
        field: "price".into(),
        value: MetaValue::Integer(100),
    };
    let hits = c.search(&q, 100, Some(&gte)).unwrap();
    assert!(hits.iter().all(|h| h.id >= 10));
    assert_eq!(hits.len(), 10);

    let isin = Filter::In {
        field: "category".into(),
        values: vec![text("faq"), text("product")],
    };
    let hits = c.search(&q, 100, Some(&isin)).unwrap();
    assert_eq!(hits.len(), 10);

    // (category == faq) | (category == product)
    let or = Filter::Or(vec![
        Filter::Eq {
            field: "category".into(),
            value: text("faq"),
        },
        Filter::Eq {
            field: "category".into(),
            value: text("product"),
        },
    ]);
    assert_eq!(c.search(&q, 100, Some(&or)).unwrap().len(), 10);

    // ~(language == fr): NULL-free column, so fr rows (ids 0,5,10,15) are excluded.
    let not_fr = Filter::Not(Box::new(Filter::Eq {
        field: "language".into(),
        value: text("fr"),
    }));
    let hits = c.search(&q, 100, Some(&not_fr)).unwrap();
    assert_eq!(hits.len(), 16);
    assert!(hits.iter().all(|h| h.id % 5 != 0));

    // Boolean equality.
    let active = Filter::Eq {
        field: "active".into(),
        value: MetaValue::Boolean(false),
    };
    assert_eq!(c.search(&q, 100, Some(&active)).unwrap().len(), 7);

    // Integer literal against a Float field compares numerically.
    let exact = Filter::Eq {
        field: "price".into(),
        value: MetaValue::Integer(50),
    };
    let hits = c.search(&q, 100, Some(&exact)).unwrap();
    assert_eq!(hits.iter().map(|h| h.id).collect::<Vec<_>>(), vec![5]);

    fs::remove_dir_all(&dir).ok();
}

#[test]
fn empty_candidate_set_returns_empty_without_error() {
    let dir = temp_dir("empty");
    let db = VectorDB::open(&dir).unwrap();
    let c = db.create_collection("e", Some(8), 4).unwrap();
    c.add(1, &[1.0; 8], "x", vec![("tenant".into(), text("A"))])
        .unwrap();
    let f = Filter::Eq {
        field: "tenant".into(),
        value: text("nobody"),
    };
    assert!(c.search(&[1.0; 8], 5, Some(&f)).unwrap().is_empty());
    fs::remove_dir_all(&dir).ok();
}

#[test]
fn unknown_field_and_wrong_type_are_errors() {
    let dir = temp_dir("badfilter");
    let db = VectorDB::open(&dir).unwrap();
    let c = db.create_collection("bf", Some(8), 4).unwrap();
    c.add(1, &[1.0; 8], "x", vec![("tenant".into(), text("A"))])
        .unwrap();
    let unknown = Filter::Eq {
        field: "nope".into(),
        value: text("A"),
    };
    assert!(matches!(
        c.search(&[1.0; 8], 5, Some(&unknown)),
        Err(ZenarError::InvalidFilter(_))
    ));
    let wrong = Filter::Eq {
        field: "tenant".into(),
        value: MetaValue::Integer(1),
    };
    assert!(matches!(
        c.search(&[1.0; 8], 5, Some(&wrong)),
        Err(ZenarError::InvalidFilter(_))
    ));
    fs::remove_dir_all(&dir).ok();
}

#[test]
fn null_metadata_never_matches_comparisons() {
    let dir = temp_dir("null");
    let db = VectorDB::open(&dir).unwrap();
    let c = db.create_collection("n", Some(8), 4).unwrap();
    c.add(1, &[1.0; 8], "has", vec![("language".into(), text("en"))])
        .unwrap();
    c.add(2, &[1.0; 8], "missing", vec![]).unwrap();
    // Row 2 has no language (NULL): excluded from both = and <>, per spec section 68.
    let eq = Filter::Eq {
        field: "language".into(),
        value: text("en"),
    };
    let ne = Filter::Ne {
        field: "language".into(),
        value: text("en"),
    };
    assert_eq!(
        c.search(&[1.0; 8], 5, Some(&eq))
            .unwrap()
            .iter()
            .map(|h| h.id)
            .collect::<Vec<_>>(),
        vec![1]
    );
    assert!(c.search(&[1.0; 8], 5, Some(&ne)).unwrap().is_empty());
    fs::remove_dir_all(&dir).ok();
}

#[test]
fn delete_removes_from_search_and_storage() {
    let dir = temp_dir("delete");
    let db = VectorDB::open(&dir).unwrap();
    let c = db.create_collection("d", Some(8), 4).unwrap();
    c.add(1, &[1.0; 8], "x", vec![]).unwrap();
    c.add(2, &[2.0; 8], "y", vec![]).unwrap();
    c.delete(1).unwrap();
    assert!(c.get(1).unwrap().is_none());
    assert_eq!(
        c.search(&[1.0; 8], 5, None)
            .unwrap()
            .iter()
            .map(|h| h.id)
            .collect::<Vec<_>>(),
        vec![2]
    );
    assert_eq!(c.delete(1), Err(ZenarError::VectorNotFound(1)));
    fs::remove_dir_all(&dir).ok();
}

#[test]
fn metadata_injection_is_stored_as_data() {
    let dir = temp_dir("inject");
    let db = VectorDB::open(&dir).unwrap();
    let c = db.create_collection("inj", Some(8), 4).unwrap();
    let payload = "'; DROP TABLE zdocs_inj; --";
    c.add(
        1,
        &[1.0; 8],
        payload,
        vec![("tenant".into(), text(payload))],
    )
    .unwrap();
    let f = Filter::Eq {
        field: "tenant".into(),
        value: text(payload),
    };
    let hits = c.search(&[1.0; 8], 5, Some(&f)).unwrap();
    assert_eq!(hits[0].text, payload);
    assert_eq!(c.count().unwrap(), 1);
    fs::remove_dir_all(&dir).ok();
}

#[test]
fn lazy_dimension_and_reopen() {
    let dir = temp_dir("lazy");
    {
        let db = VectorDB::open(&dir).unwrap();
        let c = db.create_collection("lz", None, 4).unwrap();
        assert_eq!(c.dimension(), None);
        assert!(c.search(&[1.0; 16], 5, None).unwrap().is_empty());
        c.add(1, &[0.5; 16], "first", vec![]).unwrap();
        assert_eq!(c.dimension(), Some(16));
        assert!(matches!(
            c.add(2, &[0.5; 8], "bad", vec![]),
            Err(ZenarError::DimensionMismatch { .. })
        ));
    }
    let db = VectorDB::open(&dir).unwrap();
    let c = db.get_collection("lz").unwrap();
    assert_eq!(c.dimension(), Some(16));
    assert_eq!(c.count().unwrap(), 1);
    fs::remove_dir_all(&dir).ok();
}

#[test]
fn collection_lifecycle() {
    let dir = temp_dir("life");
    let db = VectorDB::open(&dir).unwrap();
    db.create_collection("a", Some(8), 4).unwrap();
    assert!(matches!(
        db.create_collection("a", Some(8), 4),
        Err(ZenarError::CollectionAlreadyExists(_))
    ));
    assert!(matches!(
        db.create_collection("../x", Some(8), 4),
        Err(ZenarError::InvalidCollectionName(_))
    ));
    assert!(matches!(
        db.create_collection("b", Some(20000), 4),
        Err(ZenarError::InvalidDimension(20000))
    ));
    assert!(matches!(
        db.create_collection("b", Some(8), 5),
        Err(ZenarError::InvalidConfig(_))
    ));
    assert_eq!(db.list_collections().unwrap(), vec!["a".to_string()]);
    db.drop_collection("a").unwrap();
    assert!(matches!(
        db.get_collection("a"),
        Err(ZenarError::CollectionNotFound(_))
    ));
    fs::remove_dir_all(&dir).ok();
}

/// One writer and several readers at the same time. Readers must never see an
/// error, and every hit must carry the metadata its document was written with.
#[test]
fn concurrent_writes_and_filtered_reads_stay_consistent() {
    use std::sync::Arc;
    use std::thread;

    let dir = temp_dir("concurrent");
    let db = VectorDB::open(&dir).unwrap();
    let c = db.create_collection("conc", Some(8), 4).unwrap();
    // The first add locks the metadata schema. Filtering on `tenant` before that is an
    // InvalidFilter error by design, so the readers start after the schema exists.
    c.add(0, &[1.0; 8], "doc 0", vec![("tenant".into(), text("A"))])
        .unwrap();
    let writer = Arc::clone(&c);

    let w = thread::spawn(move || {
        for id in 1..1500u64 {
            let tenant = if id % 2 == 0 { "A" } else { "B" };
            let v: Vec<f32> = (0..8).map(|j| ((id + j) % 11) as f32 + 0.5).collect();
            writer
                .add(
                    id,
                    &v,
                    &format!("doc {id}"),
                    vec![("tenant".into(), text(tenant))],
                )
                .unwrap();
        }
    });

    let readers: Vec<_> = (0..4)
        .map(|r| {
            let reader = Arc::clone(&c);
            thread::spawn(move || {
                let f = Filter::Eq {
                    field: "tenant".into(),
                    value: text("A"),
                };
                let q: Vec<f32> = (0..8).map(|j| (j + r) as f32 + 1.0).collect();
                for _ in 0..300 {
                    let hits = reader
                        .search(&q, 5, Some(&f))
                        .expect("search must not fail during writes");
                    for h in hits {
                        assert_eq!(h.id % 2, 0, "tenant B id returned for tenant A");
                        assert_eq!(h.text, format!("doc {}", h.id));
                    }
                }
            })
        })
        .collect();

    w.join().unwrap();
    for r in readers {
        r.join().unwrap();
    }
    assert_eq!(c.count().unwrap(), 1500);
    fs::remove_dir_all(&dir).ok();
}

/// Before any add carries metadata, a filter on a field is an error, not an empty result.
#[test]
fn filter_before_schema_exists_is_an_error() {
    let dir = temp_dir("preschema");
    let db = VectorDB::open(&dir).unwrap();
    let c = db.create_collection("pre", Some(8), 4).unwrap();
    let f = Filter::Eq {
        field: "tenant".into(),
        value: text("A"),
    };
    assert!(matches!(
        c.search(&[1.0; 8], 5, Some(&f)),
        Err(ZenarError::InvalidFilter(_))
    ));
    fs::remove_dir_all(&dir).ok();
}

fn record(id: u64, tenant: &str) -> zenar_core::Record {
    zenar_core::Record {
        id,
        vector: (0..8).map(|j| ((id + j) % 9) as f32 + 0.5).collect(),
        text: format!("doc {id}"),
        metadata: vec![("tenant".into(), text(tenant))],
    }
}

#[test]
fn add_many_adds_everything_and_survives_reopen() {
    let dir = temp_dir("many-ok");
    {
        let db = VectorDB::open(&dir).unwrap();
        let c = db.create_collection("m", None, 4).unwrap();
        let batch: Vec<_> = (0..2500u64)
            .map(|id| record(id, if id % 2 == 0 { "A" } else { "B" }))
            .collect();
        assert_eq!(c.add_many(&batch).unwrap(), 2500);
        assert_eq!(c.count().unwrap(), 2500);
        assert_eq!(c.dimension(), Some(8));
        let f = Filter::Eq {
            field: "tenant".into(),
            value: text("A"),
        };
        let hits = c.search(&batch[0].vector, 10, Some(&f)).unwrap();
        assert!(hits.iter().all(|h| h.id % 2 == 0));
    }
    let db = VectorDB::open(&dir).unwrap();
    assert_eq!(db.get_collection("m").unwrap().count().unwrap(), 2500);
    fs::remove_dir_all(&dir).ok();
}

#[test]
fn add_many_with_bad_record_writes_nothing() {
    let dir = temp_dir("many-bad");
    let db = VectorDB::open(&dir).unwrap();
    let c = db.create_collection("b", None, 4).unwrap();
    let mut batch: Vec<_> = (0..50u64).map(|id| record(id, "A")).collect();
    batch[40].vector.pop(); // dimension mismatch near the end
    assert!(matches!(
        c.add_many(&batch),
        Err(ZenarError::DimensionMismatch { .. })
    ));
    assert_eq!(c.count().unwrap(), 0);
    assert!(c.search(&[1.0; 8], 5, None).unwrap().is_empty());
    assert_eq!(
        c.dimension(),
        None,
        "a failed first batch must not lock the dimension"
    );
    // The same ids are still usable after the failure.
    let good: Vec<_> = (0..50u64).map(|id| record(id, "A")).collect();
    assert_eq!(c.add_many(&good).unwrap(), 50);
    fs::remove_dir_all(&dir).ok();
}

#[test]
fn add_many_rejects_duplicates_within_batch_and_against_store() {
    let dir = temp_dir("many-dup");
    let db = VectorDB::open(&dir).unwrap();
    let c = db.create_collection("d", Some(8), 4).unwrap();
    c.add(5, &[1.0; 8], "existing", vec![("tenant".into(), text("A"))])
        .unwrap();

    let within = vec![record(1, "A"), record(2, "A"), record(1, "A")];
    assert_eq!(c.add_many(&within), Err(ZenarError::DuplicateId(1)));

    let against_store = vec![record(7, "A"), record(5, "A")];
    assert_eq!(c.add_many(&against_store), Err(ZenarError::DuplicateId(5)));

    assert_eq!(
        c.count().unwrap(),
        1,
        "neither failed batch may write anything"
    );
    assert!(c.get(7).unwrap().is_none());
    fs::remove_dir_all(&dir).ok();
}

#[test]
fn add_many_rejects_schema_conflict_and_leaves_schema_unchanged() {
    let dir = temp_dir("many-schema");
    let db = VectorDB::open(&dir).unwrap();
    let c = db.create_collection("s", Some(8), 4).unwrap();
    c.add(1, &[1.0; 8], "x", vec![("tenant".into(), text("A"))])
        .unwrap();
    // Second record uses the wrong type for an existing field.
    let batch = vec![
        zenar_core::Record {
            id: 2,
            vector: vec![1.0; 8],
            text: "y".into(),
            metadata: vec![("tenant".into(), MetaValue::Integer(3))],
        },
        zenar_core::Record {
            id: 3,
            vector: vec![1.0; 8],
            text: "z".into(),
            metadata: vec![("new_field".into(), text("q"))],
        },
    ];
    assert!(matches!(
        c.add_many(&batch),
        Err(ZenarError::InvalidMetadata(_))
    ));
    assert_eq!(c.count().unwrap(), 1);
    // The unknown field was not added to the schema by the failed batch.
    let f = Filter::Eq {
        field: "new_field".into(),
        value: text("q"),
    };
    assert!(matches!(
        c.search(&[1.0; 8], 5, Some(&f)),
        Err(ZenarError::InvalidFilter(_))
    ));
    fs::remove_dir_all(&dir).ok();
}
