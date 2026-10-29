//! Benchmark: ingest, unfiltered search, and filtered search (spec sections 34, 90).
//!
//! Usage: cargo run --release -p zenar-core --example bench -- <rows> <dim> [data_dir]
//!
//! Ingest uses the default durability (MetadataSync::Normal, index synced every
//! SYNC_INTERVAL adds, then flush()). Search numbers are over 200 random queries.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use zenar_core::{Filter, MetaValue, VectorDB};

/// Small deterministic PRNG, so runs are comparable.
struct Lcg(u64);

impl Lcg {
    fn next_f32(&mut self) -> f32 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        ((self.0 >> 40) as f32 / (1u64 << 24) as f32) * 2.0 - 1.0
    }

    fn vector(&mut self, dim: usize) -> Vec<f32> {
        (0..dim).map(|_| self.next_f32()).collect()
    }
}

fn report(label: &str, mut samples: Vec<Duration>) {
    samples.sort();
    let at =
        |p: f64| samples[((samples.len() as f64 - 1.0) * p).round() as usize].as_secs_f64() * 1e3;
    println!(
        "{label:<28} p50 {:>9.3} ms   p95 {:>9.3} ms   p99 {:>9.3} ms",
        at(0.50),
        at(0.95),
        at(0.99)
    );
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let rows: u64 = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(10_000);
    let dim: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(384);
    let dir = args
        .get(3)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(format!("target/bench-data/{rows}-{dim}")));
    let _ = std::fs::remove_dir_all(&dir);

    let db = VectorDB::open(&dir).expect("open");
    // flush() at the end of ingest makes the whole run durable before measuring search.
    let c = db.create_collection("bench", Some(dim), 4).expect("create");

    let mut rng = Lcg(42);
    let categories = ["faq", "product", "tools", "electronics", "appliances"];
    let started = Instant::now();
    for id in 0..rows {
        let v = rng.vector(dim);
        let tenant = if id % 10 == 0 { "A" } else { "B" };
        c.add(
            id,
            &v,
            "doc",
            vec![
                ("tenant".into(), MetaValue::Text(tenant.into())),
                (
                    "category".into(),
                    MetaValue::Text(categories[(id % 5) as usize].into()),
                ),
                ("price".into(), MetaValue::Float((id % 1000) as f64)),
            ],
        )
        .expect("add");
    }
    db.flush().expect("flush");
    let ingest = started.elapsed();
    println!("rows {rows}, dim {dim}, bit_width 4");
    println!(
        "ingest (MetadataSync::Normal, index synced every 1000 adds): {:.2} s total, {:.3} ms/row",
        ingest.as_secs_f64(),
        ingest.as_secs_f64() * 1e3 / rows as f64
    );

    let queries: Vec<Vec<f32>> = (0..200).map(|_| rng.vector(dim)).collect();
    let time_queries = |filter: Option<&Filter>| -> Vec<Duration> {
        queries
            .iter()
            .map(|q| {
                let t = Instant::now();
                c.search(q, 10, filter).expect("search");
                t.elapsed()
            })
            .collect()
    };

    report("search, no filter", time_queries(None));

    // tenant = A: 10% selectivity.
    let tenant_a = Filter::Eq {
        field: "tenant".into(),
        value: MetaValue::Text("A".into()),
    };
    report("search, tenant = A (10%)", time_queries(Some(&tenant_a)));

    // tenant = A AND category IN (2 values) AND price >= 100: about 4% selectivity.
    let compound = Filter::And(vec![
        tenant_a,
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
    report("search, compound filter", time_queries(Some(&compound)));

    // price = 5: 0.1% selectivity, the case an index should help most.
    let selective = Filter::Eq {
        field: "price".into(),
        value: MetaValue::Float(5.0),
    };
    report("search, price = 5 (0.1%)", time_queries(Some(&selective)));
}
