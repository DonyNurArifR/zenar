//! Splits a compound filtered search into its steps, at 1M documents: the document-store
//! filter, the TurboVec search over the candidate ids, and the full path through `Collection`.
//! Usage: cargo run --release -p zenar-core --example compound_breakdown -- [rows] [dim]

use std::time::Instant;

use zenar_core::docstore::{DocRef, DocStore};
use zenar_core::storage::VectorStore;
use zenar_core::{FieldKind, Filter, MetaValue};

fn vector(id: u64, dim: usize) -> Vec<f32> {
    let mut state = id.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    (0..dim)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state % 2000) as f32 / 1000.0 - 1.0
        })
        .collect()
}

fn best_of<T>(runs: usize, mut f: impl FnMut() -> T) -> (f64, T) {
    let mut best = f64::MAX;
    let mut last = None;
    for _ in 0..runs {
        let t = Instant::now();
        let out = f();
        best = best.min(t.elapsed().as_secs_f64() * 1e3);
        last = Some(out);
    }
    (best, last.expect("at least one run"))
}

fn main() {
    let mut args = std::env::args().skip(1);
    let rows: u64 = args
        .next()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1_000_000);
    let dim: usize = args.next().and_then(|s| s.parse().ok()).unwrap_or(384);

    let dir = std::env::temp_dir().join(format!("zenar-breakdown-{rows}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let schema = vec![
        ("tenant".to_string(), FieldKind::Text),
        ("category".to_string(), FieldKind::Text),
        ("price".to_string(), FieldKind::Float),
    ];
    let categories = ["faq", "product", "tools", "electronics", "appliances"];
    let mut docs = DocStore::create(&dir.join("docs.zdocs"), schema).unwrap();
    let mut vectors = VectorStore::new(dim, 4).unwrap();

    let ids: Vec<u64> = (0..rows).collect();
    for chunk in ids.chunks(10_000) {
        let metas: Vec<Vec<(String, MetaValue)>> = chunk
            .iter()
            .map(|&i| {
                vec![
                    (
                        "tenant".to_string(),
                        MetaValue::Text(if i % 10 == 0 { "A" } else { "B" }.into()),
                    ),
                    (
                        "category".to_string(),
                        MetaValue::Text(categories[(i % 5) as usize].into()),
                    ),
                    ("price".to_string(), MetaValue::Float((i % 1000) as f64)),
                ]
            })
            .collect();
        let refs: Vec<DocRef<'_>> = chunk
            .iter()
            .zip(metas.iter())
            .map(|(&i, m)| DocRef {
                id: i,
                text: "doc",
                metadata: m.as_slice(),
            })
            .collect();
        let staged = docs.stage_puts(&refs).unwrap();
        docs.commit(staged);
        for &i in chunk {
            vectors.add(&vector(i, dim), i).unwrap();
        }
    }
    vectors.sync(dir.join("vectors.zvec")).unwrap();
    drop(docs);
    let t = Instant::now();
    let docs = DocStore::open(
        &dir.join("docs.zdocs"),
        vec![
            ("tenant".to_string(), FieldKind::Text),
            ("category".to_string(), FieldKind::Text),
            ("price".to_string(), FieldKind::Float),
        ],
    )
    .unwrap();
    println!(
        "document store reopen (log replay) {:>8.2} ms",
        t.elapsed().as_secs_f64() * 1e3
    );

    let compound = Filter::And(vec![
        Filter::Eq {
            field: "tenant".into(),
            value: MetaValue::Text("A".into()),
        },
        Filter::In {
            field: "category".into(),
            values: vec![
                MetaValue::Text("faq".into()),
                MetaValue::Text("product".into()),
            ],
        },
        Filter::Gte {
            field: "price".into(),
            value: MetaValue::Float(100.0),
        },
    ]);
    let query = vector(u64::MAX / 3, dim);

    let (t_filter, candidates) = best_of(5, || docs.matching_ids(&compound).unwrap());
    let (t_contains, live) = best_of(5, || {
        candidates
            .iter()
            .copied()
            .filter(|id| vectors.contains(*id))
            .collect::<Vec<u64>>()
    });
    println!("contains check over candidates    {t_contains:>8.2} ms");
    println!(
        "document store, compound filter   {t_filter:>8.2} ms  ({} candidates)",
        live.len()
    );

    let (t_search, _) = best_of(5, || vectors.search(&query, 5, Some(&live)).unwrap());
    println!("vector search over candidates     {t_search:>8.2} ms");

    let (t_unfiltered, _) = best_of(5, || vectors.search(&query, 5, None).unwrap());
    println!("vector search, no filter         {t_unfiltered:>8.2} ms");

    let (t_fetch, _) = best_of(5, || docs.fetch(&live[..live.len().min(5)]).unwrap());
    println!("fetch 5 rows from the log        {t_fetch:>8.2} ms");

    std::fs::remove_dir_all(&dir).ok();
}
