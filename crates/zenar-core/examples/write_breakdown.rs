//! Splits one `add_many` batch into the steps the collection's write lock covers: the log write,
//! the vector add, the index sync, the log sync and the commit. Each step is timed alone, on a fresh
//! log and index, and averaged over the batches. Usage: cargo run --release -p zenar-core --example write_breakdown
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

fn main() {
    let dim = 384usize;
    let batches = 200usize;
    let batch = 50usize;
    let dir = std::env::temp_dir().join(format!("zenar-wb-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let schema = vec![
        ("tenant".to_string(), FieldKind::Text),
        ("category".to_string(), FieldKind::Text),
        ("price".to_string(), FieldKind::Float),
    ];
    let mut docs = DocStore::create(&dir.join("d.zdocs"), schema).unwrap();
    let mut store = VectorStore::new(dim, 4).unwrap();
    let index = dir.join("v.zvec");
    let (mut t_stage, mut t_vec, mut t_isync, mut t_dsync, mut t_commit) = (
        Duration::ZERO,
        Duration::ZERO,
        Duration::ZERO,
        Duration::ZERO,
        Duration::ZERO,
    );
    for b in 0..batches {
        let ids: Vec<u64> = (0..batch as u64).map(|i| (b * batch) as u64 + i).collect();
        let metas: Vec<Vec<(String, MetaValue)>> = ids
            .iter()
            .map(|&i| {
                vec![
                    ("tenant".to_string(), MetaValue::Text("A".into())),
                    ("category".to_string(), MetaValue::Text("faq".into())),
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
        let t = Instant::now();
        let staged = docs.stage_puts(&refs).unwrap();
        t_stage += t.elapsed();
        let t = Instant::now();
        for &i in &ids {
            store.add(&vector(i, dim), i).unwrap();
        }
        t_vec += t.elapsed();
        let t = Instant::now();
        store.sync(&index).unwrap();
        t_isync += t.elapsed();
        let t = Instant::now();
        docs.sync().unwrap();
        t_dsync += t.elapsed();
        let t = Instant::now();
        docs.commit(staged);
        t_commit += t.elapsed();
    }
    let per = |d: Duration| d.as_secs_f64() * 1e3 / batches as f64;
    println!("per 50-row batch (mean over {batches}):");
    println!(
        "  stage_puts (encode + log write)  {:>7.3} ms",
        per(t_stage)
    );
    println!("  vector add (50 in memory)        {:>7.3} ms", per(t_vec));
    println!(
        "  index sync                       {:>7.3} ms",
        per(t_isync)
    );
    println!(
        "  log sync                         {:>7.3} ms",
        per(t_dsync)
    );
    println!(
        "  commit (in memory)               {:>7.3} ms",
        per(t_commit)
    );
    println!(
        "  total                            {:>7.3} ms",
        per(t_stage + t_vec + t_isync + t_dsync + t_commit)
    );
    std::fs::remove_dir_all(&dir).ok();
}
