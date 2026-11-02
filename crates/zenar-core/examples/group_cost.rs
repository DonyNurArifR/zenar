//! Cost of syncing `group` batches of 50 rows with one index sync and one log sync, against one
//! pair of syncs per batch. This is the saving that group commit would make for concurrent writers.
//! Usage: cargo run --release -p zenar-core --example group_cost -- [group size]

use std::time::{Duration, Instant};

use zenar_core::docstore::{DocRef, DocStore};
use zenar_core::storage::VectorStore;
use zenar_core::{FieldKind, MetaValue};

fn vector(id: u64, dim: usize) -> Vec<f32> {
    let mut s = id.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    (0..dim)
        .map(|_| {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            (s % 2000) as f32 / 1000.0 - 1.0
        })
        .collect()
}

/// One batch of 50 rows: stage the log bytes and add the vectors. Returns the staged write.
fn stage(
    docs: &mut DocStore,
    store: &mut VectorStore,
    dim: usize,
    first: u64,
) -> zenar_core::docstore::Staged {
    let ids: Vec<u64> = (first..first + 50).collect();
    let metas: Vec<Vec<(String, MetaValue)>> = ids
        .iter()
        .map(|&i| {
            vec![
                ("tenant".to_string(), MetaValue::Text("A".into())),
                ("price".to_string(), MetaValue::Float((i % 1000) as f64)),
            ]
        })
        .collect();
    let refs: Vec<DocRef<'_>> = ids
        .iter()
        .zip(metas.iter())
        .map(|(&i, m)| DocRef {
            id: i,
            text: "doc",
            metadata: m,
        })
        .collect();
    let staged = docs.stage_puts(&refs).unwrap();
    for &i in &ids {
        store.add(&vector(i, dim), i).unwrap();
    }
    staged
}

fn main() {
    let group: usize = std::env::args()
        .nth(1)
        .map_or(4, |a| a.parse().expect("group size"));
    let dim = 384usize;
    let rounds = 100usize;
    let dir = std::env::temp_dir().join(format!("zenar-group-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let schema = vec![
        ("tenant".to_string(), FieldKind::Text),
        ("price".to_string(), FieldKind::Float),
    ];
    let index = dir.join("v.zvec");
    let (mut each, mut grouped) = (Duration::ZERO, Duration::ZERO);
    let mut next = 0u64;

    // Per-batch syncs: the cost of group size 1, times `group`.
    let mut docs = DocStore::create(&dir.join("a.zdocs"), schema.clone()).unwrap();
    let mut store = VectorStore::new(dim, 4).unwrap();
    for _ in 0..rounds {
        let t = Instant::now();
        for _ in 0..group {
            let staged = stage(&mut docs, &mut store, dim, next);
            next += 50;
            store.sync(&index).unwrap();
            docs.sync().unwrap();
            docs.commit(staged);
        }
        each += t.elapsed();
    }

    // One pair of syncs for the whole group.
    let mut docs = DocStore::create(&dir.join("b.zdocs"), schema).unwrap();
    let mut store = VectorStore::new(dim, 4).unwrap();
    let index = dir.join("w.zvec");
    next = 0;
    for _ in 0..rounds {
        let t = Instant::now();
        let mut staged = Vec::with_capacity(group);
        for _ in 0..group {
            staged.push(stage(&mut docs, &mut store, dim, next));
            next += 50;
        }
        store.sync(&index).unwrap();
        docs.sync().unwrap();
        for s in staged {
            docs.commit(s);
        }
        grouped += t.elapsed();
    }

    let per_batch = |d: Duration| d.as_secs_f64() * 1e3 / (rounds * group) as f64;
    println!("group of {group} batches (50 rows each), mean over {rounds} groups:");
    println!(
        "  one sync pair per batch   {:>7.3} ms per batch",
        per_batch(each)
    );
    println!(
        "  one sync pair per group   {:>7.3} ms per batch",
        per_batch(grouped)
    );
    println!(
        "  speed-up                  {:>7.2}x",
        each.as_secs_f64() / grouped.as_secs_f64()
    );
    std::fs::remove_dir_all(&dir).ok();
}
