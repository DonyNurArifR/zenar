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

/// Deleting most documents triggers log compaction. Search and a reopen must still agree
/// with the surviving documents.
#[test]
fn compaction_after_mass_delete_keeps_survivors_searchable_and_durable() {
    let dir = temp_dir("compact-e2e");
    {
        let db = VectorDB::open(&dir).unwrap();
        let c = db.create_collection("cp", Some(8), 4).unwrap();
        let batch: Vec<_> = (0..6000u64)
            .map(|id| record(id, if id % 3 == 0 { "A" } else { "B" }))
            .collect();
        c.add_many(&batch).unwrap();
        let doomed: Vec<u64> = (0..5000u64).collect();
        assert_eq!(c.delete_many(&doomed).unwrap(), 5000);
        assert_eq!(c.count().unwrap(), 1000);
        let f = Filter::Eq {
            field: "tenant".into(),
            value: text("A"),
        };
        let hits = c.search(&batch[5999].vector, 1000, Some(&f)).unwrap();
        assert!(hits.iter().all(|h| h.id >= 5000 && h.id % 3 == 0));
        assert_eq!(hits.len(), (5000..6000u64).filter(|id| id % 3 == 0).count());
        db.flush().unwrap();
    }
    let db = VectorDB::open(&dir).unwrap();
    let c = db.get_collection("cp").unwrap();
    assert_eq!(c.count().unwrap(), 1000);
    assert!(c.get(4999).unwrap().is_none());
    assert_eq!(c.get(5999).unwrap().unwrap().text, "doc 5999");
    assert!(
        db.recovery_report().is_clean(),
        "compaction must not leave orphans behind"
    );
    fs::remove_dir_all(&dir).ok();
}

#[test]
fn delete_many_is_atomic_and_durable() {
    let dir = temp_dir("delete-many");
    {
        let db = VectorDB::open(&dir).unwrap();
        let c = db.create_collection("dm", Some(8), 4).unwrap();
        let batch: Vec<_> = (0..20u64).map(|id| record(id, "A")).collect();
        c.add_many(&batch).unwrap();
        // One id is missing: nothing is deleted.
        assert_eq!(
            c.delete_many(&[1, 2, 99]),
            Err(ZenarError::VectorNotFound(99))
        );
        assert_eq!(c.count().unwrap(), 20);
        // A repeated id is rejected up front.
        assert!(matches!(
            c.delete_many(&[3, 3]),
            Err(ZenarError::InvalidArgument(_))
        ));
        assert_eq!(c.count().unwrap(), 20);
        assert_eq!(c.delete_many(&[1, 2, 3]).unwrap(), 3);
        assert_eq!(c.count().unwrap(), 17);
    }
    let db = VectorDB::open(&dir).unwrap();
    let c = db.get_collection("dm").unwrap();
    assert_eq!(c.count().unwrap(), 17);
    assert!(c.get(2).unwrap().is_none());
    assert!(c.get(4).unwrap().is_some());
    fs::remove_dir_all(&dir).ok();
}

/// Model-based test: random adds, batch adds, deletes, batch deletes, flushes and clean reopens,
/// checked after every step against a plain map. Any divergence fails with the step number.
#[test]
fn random_operations_agree_with_a_reference_model_across_reopens() {
    use std::collections::BTreeMap;

    struct Lcg(u64);
    impl Lcg {
        fn next(&mut self) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            self.0 >> 33
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    let dir = temp_dir("model");
    let mut rng = Lcg(0xC0FF_EE01);
    let mut model: BTreeMap<u64, (String, &'static str)> = BTreeMap::new();
    let mut next_id = 0u64;
    let mut db = VectorDB::open(&dir).unwrap();
    db.create_collection("m", Some(8), 4).unwrap();

    for step in 0..400 {
        let c = db.get_collection("m").unwrap();
        match rng.below(10) {
            0 | 1 => {
                let tenant = if rng.below(2) == 0 { "A" } else { "B" };
                let id = next_id;
                next_id += 1;
                c.add(
                    id,
                    &record(id, tenant).vector,
                    &format!("doc {id}"),
                    vec![("tenant".into(), text(tenant))],
                )
                .unwrap();
                model.insert(id, (format!("doc {id}"), tenant));
            }
            2 | 3 => {
                let n = 1 + rng.below(40);
                let mut batch = Vec::new();
                for _ in 0..n {
                    let tenant = if rng.below(2) == 0 { "A" } else { "B" };
                    let id = next_id;
                    next_id += 1;
                    batch.push(record(id, tenant));
                    model.insert(id, (format!("doc {id}"), tenant));
                }
                assert_eq!(c.add_many(&batch).unwrap(), n as usize, "step {step}");
            }
            4 | 5 => {
                if let Some(&id) = model
                    .keys()
                    .nth(rng.below(model.len().max(1) as u64) as usize)
                {
                    c.delete(id)
                        .unwrap_or_else(|e| panic!("step {step}: delete {id}: {e}"));
                    model.remove(&id);
                }
            }
            6 => {
                let victims: Vec<u64> = model
                    .keys()
                    .filter(|_| rng.below(5) == 0)
                    .copied()
                    .take(30)
                    .collect();
                if !victims.is_empty() {
                    assert_eq!(
                        c.delete_many(&victims).unwrap(),
                        victims.len(),
                        "step {step}"
                    );
                    for id in victims {
                        model.remove(&id);
                    }
                }
            }
            7 => db.flush().unwrap(),
            8 => {
                // Clean close and reopen.
                drop(c);
                drop(db);
                db = VectorDB::open(&dir).unwrap();
                assert!(
                    db.recovery_report().is_clean(),
                    "step {step}: clean reopen repaired something"
                );
            }
            _ => {}
        }

        let c = db.get_collection("m").unwrap();
        assert_eq!(
            c.count().unwrap(),
            model.len(),
            "count diverged at step {step}"
        );
        for _ in 0..3 {
            let probe = next_id.saturating_sub(rng.below(next_id.max(1)) + 1);
            let got = c.get(probe).unwrap();
            match model.get(&probe) {
                Some((text_value, _)) => assert_eq!(
                    got.map(|d| d.text),
                    Some(text_value.clone()),
                    "step {step} id {probe}"
                ),
                None => assert!(got.is_none(), "step {step}: deleted id {probe} came back"),
            }
        }
        if step % 25 == 0 {
            let f = Filter::Eq {
                field: "tenant".into(),
                value: text("A"),
            };
            let hits = c.search(&[1.0; 8], 10_000, Some(&f)).unwrap();
            let expected = model
                .values()
                .filter(|(_, t)| *t == "A")
                .count()
                .min(10_000);
            assert_eq!(
                hits.len(),
                expected,
                "tenant A hits diverged at step {step}"
            );
            assert!(
                hits.iter()
                    .all(|h| model.get(&h.id).map(|(_, t)| *t) == Some("A"))
            );
        }
    }
    drop(db);
    fs::remove_dir_all(&dir).ok();
}

fn doc_with(id: u64, tenant: &str, category: &str, price: f64) -> zenar_core::Record {
    zenar_core::Record {
        id,
        vector: (0..8).map(|j| ((id + j) % 9) as f32 + 0.5).collect(),
        text: format!("doc {id}"),
        metadata: vec![
            ("tenant".into(), text(tenant)),
            ("category".into(), text(category)),
            ("price".into(), MetaValue::Float(price)),
        ],
    }
}

#[test]
fn describe_reports_fields_values_and_counts_and_follows_writes() {
    let dir = temp_dir("describe");
    let db = VectorDB::open(&dir).unwrap();
    let c = db.create_collection("shop", None, 4).unwrap();
    c.add_many(&[
        doc_with(1, "A", "faq", 10.0),
        doc_with(2, "A", "tools", 20.0),
        doc_with(3, "B", "faq", 30.0),
    ])
    .unwrap();

    let info = c.describe(None).unwrap();
    assert_eq!(info["documents"], 3);
    let fields = info["fields"].as_array().unwrap();
    let tenant = fields.iter().find(|f| f["name"] == "tenant").unwrap();
    assert_eq!(tenant["type"], "text");
    assert_eq!(tenant["values"][0]["value"], "A");
    assert_eq!(tenant["values"][0]["count"], 2);
    let price = fields.iter().find(|f| f["name"] == "price").unwrap();
    assert_eq!(price["min"], 10.0);
    assert_eq!(price["max"], 30.0);

    // Cached statistics must not survive a write.
    c.add(4, &[1.0; 8], "new", vec![("tenant".into(), text("C"))])
        .unwrap();
    c.delete(1).unwrap();
    let info = c.describe(None).unwrap();
    let tenant = info["fields"]
        .as_array()
        .unwrap()
        .iter()
        .find(|f| f["name"] == "tenant")
        .unwrap()
        .clone();
    let values: Vec<(String, u64)> = tenant["values"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| {
            (
                v["value"].as_str().unwrap().to_string(),
                v["count"].as_u64().unwrap(),
            )
        })
        .collect();
    assert!(values.contains(&("A".to_string(), 1)), "{values:?}");
    assert!(values.contains(&("C".to_string(), 1)), "{values:?}");
    fs::remove_dir_all(&dir).ok();
}

#[test]
fn retrieve_reports_matches_and_warns_about_missing_values() {
    let dir = temp_dir("retrieve");
    let db = VectorDB::open(&dir).unwrap();
    let c = db.create_collection("r", None, 4).unwrap();
    c.add_many(&[
        doc_with(1, "A", "faq", 10.0),
        doc_with(2, "A", "tools", 20.0),
        doc_with(3, "B", "faq", 30.0),
    ])
    .unwrap();

    let r = c
        .retrieve(
            &[1.0; 8],
            1,
            Some(&serde_json::json!({"tenant": "A"})),
            None,
        )
        .unwrap();
    assert_eq!(r.hits.len(), 1);
    assert_eq!(r.matched, 2, "matched counts every document before top_k");
    assert!(r.warnings.is_empty());

    let r = c
        .retrieve(
            &[1.0; 8],
            5,
            Some(&serde_json::json!({"tenant": "Z"})),
            None,
        )
        .unwrap();
    assert!(r.hits.is_empty());
    assert_eq!(r.matched, 0);
    assert_eq!(r.warnings.len(), 1, "{:?}", r.warnings);
    assert!(r.warnings[0].contains("\"A\""), "{:?}", r.warnings);

    let r = c
        .retrieve(
            &[1.0; 8],
            5,
            Some(&serde_json::json!({"price": {"gte": 15, "lt": 35}})),
            None,
        )
        .unwrap();
    assert_eq!(r.matched, 2);

    let err = c
        .retrieve(
            &[1.0; 8],
            5,
            Some(&serde_json::json!({"catgory": "faq"})),
            None,
        )
        .unwrap_err();
    assert!(
        err.to_string().contains("did you mean \"category\""),
        "{err}"
    );

    let r = c
        .retrieve(&[1.0; 8], 5, Some(&serde_json::json!({})), None)
        .unwrap();
    assert_eq!(r.matched, 3, "an empty where matches everything");
    fs::remove_dir_all(&dir).ok();
}

#[test]
fn find_values_searches_values_and_tool_spec_lists_them() {
    let dir = temp_dir("agent-tools");
    let db = VectorDB::open(&dir).unwrap();
    let c = db.create_collection("t", None, 4).unwrap();
    c.add_many(&[
        doc_with(1, "north-east", "electronics", 1.0),
        doc_with(2, "south", "appliances", 2.0),
        doc_with(3, "north-west", "electronics", 3.0),
    ])
    .unwrap();

    let found = c.find_values("tenant", "NORTH", 10, None).unwrap();
    let names: Vec<&str> = found.iter().map(|v| v["value"].as_str().unwrap()).collect();
    assert_eq!(names.len(), 2, "{names:?}");
    assert!(names.iter().all(|n| n.starts_with("north")));
    assert!(c.find_values("nope", "x", 10, None).is_err());

    let spec = c.tool_spec("search_products", None).unwrap();
    assert_eq!(spec["name"], "search_products");
    assert!(
        spec["parameters"].get("description").is_none(),
        "description belongs at the top level"
    );
    let text_desc = spec["description"].as_str().unwrap();
    assert!(text_desc.contains("category (text)"), "{text_desc}");
    let enum_values =
        &spec["parameters"]["$defs"]["where"]["properties"]["category"]["anyOf"][0]["enum"];
    assert!(
        enum_values
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v == "electronics")
    );
    fs::remove_dir_all(&dir).ok();
}

/// A tenant's scope must hold against anything the model writes in `where`, and discovery must
/// show nothing outside the scope.
#[test]
fn scope_holds_against_model_filters_and_discovery() {
    let dir = temp_dir("scope");
    let db = VectorDB::open(&dir).unwrap();
    let c = db.create_collection("sc", None, 4).unwrap();
    let mut batch = Vec::new();
    for id in 0..30u64 {
        let tenant = if id % 3 == 0 { "A" } else { "B" };
        batch.push(doc_with(
            id,
            tenant,
            if id % 2 == 0 { "faq" } else { "tools" },
            id as f64,
        ));
    }
    c.add_many(&batch).unwrap();
    let scope_a = serde_json::json!({"tenant": "A"});
    let a_count = 10;
    let q = [1.0f32; 8];

    // The model asks for another tenant directly.
    let r = c
        .retrieve(
            &q,
            50,
            Some(&serde_json::json!({"tenant": "B"})),
            Some(&scope_a),
        )
        .unwrap();
    assert_eq!(r.matched, 0, "tenant B must not be reachable from scope A");

    // The model tries to widen the scope with `or`.
    let r = c
        .retrieve(
            &q,
            50,
            Some(&serde_json::json!({"or": [{"tenant": "A"}, {"tenant": "B"}]})),
            Some(&scope_a),
        )
        .unwrap();
    assert_eq!(r.matched, a_count);
    assert!(
        r.hits
            .iter()
            .all(|h| h.metadata.contains(&("tenant".to_string(), text("A"))))
    );

    // The model tries to exclude the scope with `not`.
    let r = c
        .retrieve(
            &q,
            50,
            Some(&serde_json::json!({"not": {"tenant": "A"}})),
            Some(&scope_a),
        )
        .unwrap();
    assert_eq!(r.matched, 0);

    // Without a scope, the same request sees everything. That is the trusted path only.
    let r = c
        .retrieve(
            &q,
            50,
            Some(&serde_json::json!({"not": {"tenant": "A"}})),
            None,
        )
        .unwrap();
    assert_eq!(r.matched, 20);

    // Discovery inside the scope mentions no other tenant.
    let info = c.describe(Some(&scope_a)).unwrap();
    assert_eq!(info["documents"], a_count);
    let rendered = info.to_string();
    assert!(
        !rendered.contains("\"B\""),
        "tenant B leaked into describe: {rendered}"
    );
    let found = c.find_values("tenant", "b", 10, Some(&scope_a)).unwrap();
    assert!(found.is_empty(), "tenant B leaked into find_values");
    let spec = c.tool_spec("search", Some(&scope_a)).unwrap();
    let spec_text = spec.to_string();
    assert!(
        !spec_text.contains("\"B\""),
        "tenant B leaked into the tool spec"
    );
    assert!(
        c.fields_prompt(Some(&scope_a))
            .unwrap()
            .find("\"B\"")
            .is_none()
    );

    // An unknown field in the scope is an error, not a silent empty scope.
    assert!(
        c.describe(Some(&serde_json::json!({"tennant": "A"})))
            .is_err()
    );

    // An empty scope allows everything.
    assert_eq!(
        c.describe(Some(&serde_json::json!({}))).unwrap()["documents"],
        30
    );
    fs::remove_dir_all(&dir).ok();
}

/// A warning about a missing value must not name values outside the scope.
#[test]
fn warnings_inside_a_scope_do_not_name_other_tenants() {
    let dir = temp_dir("scope-warning");
    let db = VectorDB::open(&dir).unwrap();
    let c = db.create_collection("sw", None, 4).unwrap();
    c.add_many(&[
        doc_with(1, "acme", "x", 1.0),
        doc_with(2, "globex", "x", 2.0),
    ])
    .unwrap();
    let scope = serde_json::json!({"tenant": "acme"});
    let r = c
        .retrieve(
            &[1.0; 8],
            5,
            Some(&serde_json::json!({"tenant": "zzz"})),
            Some(&scope),
        )
        .unwrap();
    assert_eq!(r.warnings.len(), 1);
    assert!(r.warnings[0].contains("\"acme\""), "{:?}", r.warnings);
    assert!(!r.warnings[0].contains("globex"), "{:?}", r.warnings);
    fs::remove_dir_all(&dir).ok();
}

/// A cached scope must not outlive a write.
#[test]
fn scoped_discovery_is_fresh_after_a_write() {
    let dir = temp_dir("scope-fresh");
    let db = VectorDB::open(&dir).unwrap();
    let c = db.create_collection("sf", None, 4).unwrap();
    c.add_many(&[doc_with(1, "acme", "x", 1.0)]).unwrap();
    let scope = serde_json::json!({"tenant": "acme"});
    assert_eq!(c.describe(Some(&scope)).unwrap()["documents"], 1);
    assert_eq!(
        c.describe(Some(&scope)).unwrap()["documents"],
        1,
        "second call is served from the cache"
    );
    c.add(
        2,
        &[1.0; 8],
        "two",
        vec![
            ("tenant".into(), text("acme")),
            ("category".into(), text("x")),
            ("price".into(), MetaValue::Float(2.0)),
        ],
    )
    .unwrap();
    assert_eq!(
        c.describe(Some(&scope)).unwrap()["documents"],
        2,
        "a write must invalidate the scope cache"
    );
    c.delete(1).unwrap();
    assert_eq!(c.describe(Some(&scope)).unwrap()["documents"], 1);
    fs::remove_dir_all(&dir).ok();
}

#[test]
fn retrieve_accepts_any_top_k_and_returns_at_most_the_matches() {
    let dir = temp_dir("topk");
    let db = VectorDB::open(&dir).unwrap();
    let c = db.create_collection("tk", None, 4).unwrap();
    c.add_many(&[doc_with(1, "acme", "x", 1.0), doc_with(2, "acme", "y", 2.0)])
        .unwrap();
    let big = c.retrieve(&[1.0; 8], 5000, None, None).unwrap();
    assert_eq!(big.hits.len(), 2);
    assert_eq!(big.matched, 2);
    fs::remove_dir_all(&dir).ok();
}

/// Agent-path calls while documents are being written: no errors, and no tenant leaks.
#[test]
fn agent_calls_stay_correct_during_concurrent_writes() {
    use std::sync::Arc;
    use std::thread;

    let dir = temp_dir("agent-concurrent");
    let db = VectorDB::open(&dir).unwrap();
    let c = db.create_collection("ac", None, 4).unwrap();
    c.add_many(&[
        doc_with(0, "acme", "x", 0.0),
        doc_with(1, "globex", "x", 1.0),
    ])
    .unwrap();
    let writer = Arc::clone(&c);
    let w = thread::spawn(move || {
        for id in 2..800u64 {
            let tenant = if id % 2 == 0 { "acme" } else { "globex" };
            writer
                .add(
                    id,
                    &[id as f32 + 1.0; 8],
                    "w",
                    vec![
                        ("tenant".into(), text(tenant)),
                        ("category".into(), text("x")),
                        ("price".into(), MetaValue::Float(id as f64)),
                    ],
                )
                .unwrap();
        }
    });
    let readers: Vec<_> = (0..3)
        .map(|_| {
            let reader = Arc::clone(&c);
            thread::spawn(move || {
                let scope = serde_json::json!({"tenant": "acme"});
                for _ in 0..200 {
                    let info = reader
                        .describe(Some(&scope))
                        .expect("describe must not fail during writes");
                    assert!(
                        !info.to_string().contains("globex"),
                        "globex leaked into a scoped describe"
                    );
                    let r = reader
                        .retrieve(
                            &[1.0; 8],
                            20,
                            Some(&serde_json::json!({"category": "x"})),
                            Some(&scope),
                        )
                        .expect("retrieve must not fail during writes");
                    for hit in r.hits {
                        assert!(
                            hit.metadata.contains(&("tenant".to_string(), text("acme"))),
                            "globex hit inside acme scope"
                        );
                    }
                }
            })
        })
        .collect();
    w.join().unwrap();
    for r in readers {
        r.join().unwrap();
    }
    fs::remove_dir_all(&dir).ok();
}

#[test]
fn tool_spec_refuses_names_the_function_calling_apis_reject() {
    let dir = temp_dir("toolname");
    let db = VectorDB::open(&dir).unwrap();
    let c = db.create_collection("tn", None, 4).unwrap();
    c.add_many(&[doc_with(1, "acme", "x", 1.0)]).unwrap();
    assert!(c.tool_spec("search_products-2", None).is_ok());
    for bad in [
        "",
        "search products",
        "search.products",
        "x".repeat(65).as_str(),
    ] {
        assert!(
            matches!(c.tool_spec(bad, None), Err(ZenarError::InvalidArgument(_))),
            "name {bad:?} should be refused"
        );
    }
    fs::remove_dir_all(&dir).ok();
}
