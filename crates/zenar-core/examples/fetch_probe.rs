//! Times Zenar's own MetadataStore::fetch_documents against an existing database,
//! before and after VectorDB::open (which runs reconciliation).
//! Usage: fetch_probe <database directory>
use std::time::Instant;
use zenar_core::document::MetadataStore;
use zenar_core::{FieldKind, VectorDB};

thread_local! { static ROWS: std::cell::Cell<u64> = const { std::cell::Cell::new(1_000_000) }; }

fn time_fetch(store: &MetadataStore, label: &str) {
    let fields = vec![
        ("tenant".to_string(), FieldKind::Text),
        ("category".to_string(), FieldKind::Text),
        ("price".to_string(), FieldKind::Float),
    ];
    let mut x: u64 = 88172645463325252;
    let ids: Vec<u64> = (0..10)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x % ROWS.with(|r| r.get())
        })
        .collect();
    let mut best = f64::MAX;
    for _ in 0..5 {
        let t = Instant::now();
        let docs = store.fetch_documents("zdocs_bench", &fields, &ids).unwrap();
        best = best.min(t.elapsed().as_secs_f64() * 1e3);
        assert_eq!(docs.len(), 10);
    }
    println!("{label}: fetch (10 ids) {best:.3} ms");
}

fn main() {
    let dir = std::env::args().nth(1).expect("database directory");
    if let Some(n) = std::env::args().nth(2) {
        ROWS.with(|r| r.set(n.parse().unwrap()));
    }
    let dsn = format!("file://{dir}/metadata.db?sync_mode=normal");
    let store = MetadataStore::open(&dsn).expect("open");
    time_fetch(&store, "before VectorDB::open");
    let db = VectorDB::open(&dir).expect("VectorDB::open");
    time_fetch(&store, "after  VectorDB::open");
    drop(db);
}
