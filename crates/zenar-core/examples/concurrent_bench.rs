//! Readers and a writer on one collection. Each case runs for a fixed time and reports search
//! throughput and latency for the readers, and batch throughput and latency for the writer.
//!
//! The writer's batch time is the time `add_many` takes, which is the time it holds the collection
//! lock for a write. It includes validation, the log write, the index write and both fsyncs.
//!
//! Writer threads share the writer cases: with `writers` threads, each case runs that many writers, all on one
//! collection. Usage: cargo run --release -p zenar-core --example concurrent_bench -- [rows] [dim] [readers] [seconds] [batch] [writers]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use zenar_core::{Collection, MetaValue, Record, VectorDB, WriteStats};

const CATEGORIES: [&str; 5] = ["faq", "product", "tools", "electronics", "appliances"];

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

fn record(id: u64, vector: Vec<f32>) -> Record {
    Record {
        id,
        vector,
        text: "doc".into(),
        metadata: vec![
            (
                "tenant".into(),
                MetaValue::Text(if id.is_multiple_of(10) { "A" } else { "B" }.into()),
            ),
            (
                "category".into(),
                MetaValue::Text(CATEGORIES[(id % 5) as usize].into()),
            ),
            ("price".into(), MetaValue::Float((id % 1000) as f64)),
        ],
    }
}

fn percentile(samples: &mut [Duration], p: f64) -> f64 {
    if samples.is_empty() {
        return f64::NAN;
    }
    samples.sort();
    samples[((samples.len() as f64 - 1.0) * p).round() as usize].as_secs_f64() * 1e3
}

struct Case {
    label: String,
    readers: usize,
    /// `None`: no writer. `Some(pause)`: a writer, sleeping `pause` between batches (zero: continuous).
    writer: Option<Duration>,
}

struct Outcome {
    wall: f64,
    search: Vec<Duration>,
    batch: Vec<Duration>,
}

/// Settings shared by every case of one run.
struct Run {
    dim: usize,
    seconds: f64,
    batch_size: usize,
    writers: usize,
}

fn run_case(
    col: &Arc<Collection>,
    queries: &Arc<Vec<Vec<f32>>>,
    next_id: &Arc<AtomicU64>,
    case: &Case,
    run: &Run,
) -> Outcome {
    let (dim, batch_size, writers) = (run.dim, run.batch_size, run.writers);
    let stop = Arc::new(AtomicBool::new(false));
    let start = Instant::now();
    let mut readers = Vec::with_capacity(case.readers);
    for r in 0..case.readers {
        let (col, queries, stop) = (Arc::clone(col), Arc::clone(queries), Arc::clone(&stop));
        readers.push(thread::spawn(move || {
            let mut samples = Vec::new();
            let mut i = r;
            while !stop.load(Ordering::Relaxed) {
                let q = &queries[i % queries.len()];
                let t = Instant::now();
                col.search(q, 10, None).expect("search");
                samples.push(t.elapsed());
                i += 1;
            }
            samples
        }));
    }
    let mut writer_threads = Vec::new();
    if let Some(pause) = case.writer {
        for w in 0..writers {
            let (col, stop, next_id) = (Arc::clone(col), Arc::clone(&stop), Arc::clone(next_id));
            writer_threads.push(thread::spawn(move || {
                let mut rng = Lcg(0xC0FFEE ^ (w as u64 + 1));
                let mut samples = Vec::new();
                while !stop.load(Ordering::Relaxed) {
                    let first = next_id.fetch_add(batch_size as u64, Ordering::Relaxed);
                    let records: Vec<Record> = (first..first + batch_size as u64)
                        .map(|id| record(id, rng.vector(dim)))
                        .collect();
                    let t = Instant::now();
                    col.add_many(&records).expect("add_many");
                    samples.push(t.elapsed());
                    if !pause.is_zero() {
                        thread::sleep(pause);
                    }
                }
                samples
            }));
        }
    }

    thread::sleep(Duration::from_secs_f64(run.seconds));
    stop.store(true, Ordering::Relaxed);
    let mut search = Vec::new();
    for handle in readers {
        search.extend(handle.join().expect("reader"));
    }
    let mut batch = Vec::new();
    for handle in writer_threads {
        batch.extend(handle.join().expect("writer"));
    }
    Outcome {
        wall: start.elapsed().as_secs_f64(),
        search,
        batch,
    }
}

/// The counters of one case: the difference between two snapshots.
fn delta(before: &WriteStats, after: &WriteStats) -> WriteStats {
    let mut group_writes = [0u64; zenar_core::GROUP_BUCKETS];
    for (i, out) in group_writes.iter_mut().enumerate() {
        *out = after.group_writes[i] - before.group_writes[i];
    }
    WriteStats {
        sync_groups: after.sync_groups - before.sync_groups,
        group_writes,
        synced_writes: after.synced_writes - before.synced_writes,
        write_holds: after.write_holds - before.write_holds,
        write_hold_total_ns: after.write_hold_total_ns - before.write_hold_total_ns,
        // Maxima are cumulative: a case's own maximum is only known when it set a new one.
        write_hold_max_ns: after.write_hold_max_ns,
        read_waits: after.read_waits - before.read_waits,
        read_wait_total_ns: after.read_wait_total_ns - before.read_wait_total_ns,
        read_wait_max_ns: after.read_wait_max_ns,
    }
}

/// One line of the write-path counters for a case: groups, writes per group, and the lock times.
fn report_stats(d: &WriteStats) {
    let mean_us = |total: u64, n: u64| {
        if n == 0 {
            0.0
        } else {
            total as f64 / n as f64 / 1e3
        }
    };
    let avg_group = if d.sync_groups == 0 {
        0.0
    } else {
        d.synced_writes as f64 / d.sync_groups as f64
    };
    let hist: Vec<String> = d
        .group_writes
        .iter()
        .enumerate()
        .filter(|(_, n)| **n > 0)
        .map(|(i, n)| {
            format!(
                "{}{}:{}",
                i + 1,
                if i + 1 == zenar_core::GROUP_BUCKETS {
                    "+"
                } else {
                    ""
                },
                n
            )
        })
        .collect();
    println!(
        "    groups {:>6}  writes/group {:>5.2}  sizes {{{}}}  state lock held by writes: mean {:>7.1} us, max {:>8.1} us  readers waited: mean {:>7.1} us, max {:>8.1} us",
        d.sync_groups,
        avg_group,
        hist.join(" "),
        mean_us(d.write_hold_total_ns, d.write_holds),
        d.write_hold_max_ns as f64 / 1e3,
        mean_us(d.read_wait_total_ns, d.read_waits),
        d.read_wait_max_ns as f64 / 1e3,
    );
}

fn report(case: &Case, mut out: Outcome, batch_size: usize) {
    let searches = out.search.len() as f64;
    let batches = out.batch.len() as f64;
    let search_p50 = percentile(&mut out.search, 0.50);
    let search_p95 = percentile(&mut out.search, 0.95);
    let search_p99 = percentile(&mut out.search, 0.99);
    let batch_p50 = percentile(&mut out.batch, 0.50);
    let batch_p95 = percentile(&mut out.batch, 0.95);
    let batch_p99 = percentile(&mut out.batch, 0.99);
    println!(
        "{:<34} readers {:>2}  search {:>8.0}/s  p50 {:>7.3}  p95 {:>7.3}  p99 {:>7.3} ms | writer {:>6.1} batch/s ({:>7.0} rows/s)  batch p50 {:>6.2}  p95 {:>6.2}  p99 {:>6.2} ms",
        case.label,
        case.readers,
        searches / out.wall,
        search_p50,
        search_p95,
        search_p99,
        batches / out.wall,
        batches * batch_size as f64 / out.wall,
        batch_p50,
        batch_p95,
        batch_p99,
    );
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let arg = |i: usize, default: &str| args.get(i).cloned().unwrap_or_else(|| default.to_string());
    let rows: u64 = arg(1, "100000").parse().expect("rows");
    let dim: usize = arg(2, "384").parse().expect("dim");
    let readers: usize = arg(3, "8").parse().expect("readers");
    let seconds: f64 = arg(4, "5").parse().expect("seconds");
    let batch_size: usize = arg(5, "50").parse().expect("batch");
    let writers: usize = arg(6, "1").parse().expect("writers");
    let run = Run {
        dim,
        seconds,
        batch_size,
        writers,
    };

    let dir = std::env::temp_dir().join(format!("zenar-concurrent-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let db = VectorDB::open(&dir).expect("open");
    let col = db.create_collection("c", Some(dim), 4).expect("create");

    let mut rng = Lcg(42);
    let mut id = 0u64;
    while id < rows {
        let chunk: Vec<Record> = (id..(id + 1000).min(rows))
            .map(|i| record(i, rng.vector(dim)))
            .collect();
        id += chunk.len() as u64;
        col.add_many(&chunk).expect("seed");
    }
    db.flush().expect("flush");
    println!(
        "collection: {rows} rows, dim {dim}, top-10 unfiltered search, batches of {batch_size}, {seconds} s per case"
    );

    let queries = Arc::new((0..256).map(|_| rng.vector(dim)).collect::<Vec<_>>());
    let next_id = Arc::new(AtomicU64::new(rows));

    let cases = [
        Case {
            label: "readers only".into(),
            readers: 1,
            writer: None,
        },
        Case {
            label: "readers only".into(),
            readers,
            writer: None,
        },
        Case {
            label: "readers + writer, 1 batch/s".into(),
            readers,
            writer: Some(Duration::from_secs(1)),
        },
        Case {
            label: "readers + writer, 10 batch/s".into(),
            readers,
            writer: Some(Duration::from_millis(100)),
        },
        Case {
            label: "readers + writer, continuous".into(),
            readers,
            writer: Some(Duration::ZERO),
        },
        Case {
            label: "writer only, continuous".into(),
            readers: 0,
            writer: Some(Duration::ZERO),
        },
    ];
    for case in &cases {
        let before = col.write_stats();
        let out = run_case(&col, &queries, &next_id, case, &run);
        report(case, out, batch_size);
        report_stats(&delta(&before, &col.write_stats()));
    }

    drop(col);
    drop(db);
    std::fs::remove_dir_all(&dir).ok();
}
