//! Search latency on an existing database in a fresh process (the "reopen" case).
//! Run `bench` first to create the data. Usage: reopen_bench <database dir> <rows> <dim>

use std::time::{Duration, Instant};

use zenar_core::{Filter, MetaValue, VectorDB};

struct Lcg(u64);

impl Lcg {
    fn next_f32(&mut self) -> f32 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        ((self.0 >> 40) as f32 / (1u64 << 24) as f32) * 2.0 - 1.0
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
    let dir = &args[1];
    let dim: usize = args[3].parse().expect("dim");

    let open_start = Instant::now();
    let db = VectorDB::open(dir).expect("open");
    println!(
        "open + reconcile: {:.2} s",
        open_start.elapsed().as_secs_f64()
    );
    let c = db.get_collection("bench").expect("collection");

    let mut rng = Lcg(42);
    let queries: Vec<Vec<f32>> = (0..200)
        .map(|_| (0..dim).map(|_| rng.next_f32()).collect())
        .collect();
    let time = |filter: Option<&Filter>| -> Vec<Duration> {
        queries
            .iter()
            .map(|q| {
                let t = Instant::now();
                c.search(q, 10, filter).expect("search");
                t.elapsed()
            })
            .collect()
    };
    report("reopen, no filter", time(None));
    let tenant_a = Filter::Eq {
        field: "tenant".into(),
        value: MetaValue::Text("A".into()),
    };
    report("reopen, tenant = A (10%)", time(Some(&tenant_a)));
}
