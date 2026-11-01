//! Concurrent search and write workload, run against the production code. Test-only and ignored by default.
//!
//! Run one configuration:
//!   BATCH=1000 READERS=8 WRITERS=1 TOP_K=10 LIVE=200000 SECS=5 \
//!   cargo test --release -p zenar-core bench_search_workload -- --ignored --nocapture
//!
//! Reports the search and writer latency percentiles, the peak heap above the heap at the end of the window (the
//! transient allocation), allocations per search, and how many searches retried with the full over-fetch.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use crate::Record;
use crate::database::VectorDB;
use crate::value::MetaValue;

struct Counting;

static HEAP: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
static ALLOCS: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let now = HEAP.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
        PEAK.fetch_max(now, Ordering::Relaxed);
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        HEAP.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let now = HEAP.fetch_add(new_size, Ordering::Relaxed) + new_size;
        PEAK.fetch_max(now, Ordering::Relaxed);
        HEAP.fetch_sub(layout.size(), Ordering::Relaxed);
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

fn env_num(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

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

fn record(id: u64, dim: usize) -> Record {
    Record {
        id,
        vector: vector(id, dim),
        text: "doc".into(),
        metadata: vec![
            (
                "tenant".into(),
                MetaValue::Text(if id.is_multiple_of(2) { "A" } else { "B" }.into()),
            ),
            ("price".into(), MetaValue::Float((id % 1000) as f64)),
        ],
    }
}

fn percentile(v: &mut [u64], p: f64) -> f64 {
    if v.is_empty() {
        return f64::NAN;
    }
    v.sort_unstable();
    v[((v.len() as f64 - 1.0) * p).round() as usize] as f64 / 1e6
}

/// The workload of `docs/BENCHMARKS.md`, "Search under concurrent writes". Configured by environment variables.
#[test]
#[ignore = "benchmark: run with --ignored --nocapture"]
fn bench_search_workload() {
    let live = env_num("LIVE", 200_000);
    let dim = env_num("DIM", 64) as usize;
    let readers = env_num("READERS", 8) as usize;
    let writers = env_num("WRITERS", 1) as usize;
    let batch = env_num("BATCH", 1000);
    let top_k = env_num("TOP_K", 10) as usize;
    let secs = env_num("SECS", 5) as f64;
    // HOT=h: readers search h fixed vectors, and every written vector is one of them, so the pending writes compete
    // with the committed copies for the top results. Without it, queries and writes are unrelated vectors.
    let hot = std::env::var("HOT")
        .ok()
        .and_then(|v| v.parse::<u64>().ok());

    let dir = std::env::temp_dir().join(format!("zenar-bench-search-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let db = VectorDB::open(&dir).unwrap();
    let col = db.create_collection("c", Some(dim), 4).unwrap();
    let mut next = 0u64;
    while next < live {
        let recs: Vec<Record> = (next..(next + 1000).min(live))
            .map(|i| record(i, dim))
            .collect();
        col.add_many(&recs).unwrap();
        next += 1000;
    }
    db.flush().unwrap();
    thread::sleep(Duration::from_millis(500));
    let base = HEAP.load(Ordering::Relaxed);
    let retries0 = col.search_retry_count();

    let stop = Arc::new(AtomicBool::new(false));
    PEAK.store(base, Ordering::Relaxed);
    let allocs0 = ALLOCS.load(Ordering::Relaxed);
    let bytes0 = BYTES.load(Ordering::Relaxed);
    let wall0 = Instant::now();
    let reader_handles: Vec<_> = (0..readers)
        .map(|r| {
            let (col, stop) = (Arc::clone(&col), Arc::clone(&stop));
            thread::spawn(move || {
                let mut lat = Vec::new();
                let mut i = r as u64;
                while !stop.load(Ordering::Relaxed) {
                    let q = vector(hot.map_or(i, |h| i % h) * 7919 + 1, dim);
                    let t = Instant::now();
                    col.search(&q, top_k, None).unwrap();
                    lat.push(t.elapsed().as_nanos() as u64);
                    i += 1;
                }
                lat
            })
        })
        .collect();
    let writer_handles: Vec<_> = (0..writers)
        .map(|w| {
            let (col, stop) = (Arc::clone(&col), Arc::clone(&stop));
            thread::spawn(move || {
                let mut lat = Vec::new();
                let mut id = 10_000_000u64 * (w as u64 + 1);
                while !stop.load(Ordering::Relaxed) {
                    let recs: Vec<Record> = (id..id + batch)
                        .map(|i| {
                            let mut r = record(i, dim);
                            if let Some(h) = hot {
                                r.vector = vector((i % h) * 7919 + 1, dim);
                            }
                            r
                        })
                        .collect();
                    let t = Instant::now();
                    col.add_many(&recs).unwrap();
                    lat.push(t.elapsed().as_nanos() as u64);
                    id += batch;
                }
                lat
            })
        })
        .collect();
    thread::sleep(Duration::from_secs_f64(secs));
    let peak = PEAK.load(Ordering::Relaxed);
    let heap_at_stop = HEAP.load(Ordering::Relaxed);
    stop.store(true, Ordering::Relaxed);
    let mut search: Vec<u64> = reader_handles
        .into_iter()
        .flat_map(|h| h.join().unwrap())
        .collect();
    let mut write: Vec<u64> = writer_handles
        .into_iter()
        .flat_map(|h| h.join().unwrap())
        .collect();
    let wall = wall0.elapsed().as_secs_f64();
    let allocs = ALLOCS.load(Ordering::Relaxed) - allocs0;
    let bytes = BYTES.load(Ordering::Relaxed) - bytes0;
    let retries = col.search_retry_count() - retries0;
    let n = search.len().max(1) as f64;
    let mb = |b: usize| b as f64 / (1024.0 * 1024.0);
    println!(
        "bench_search batch {batch} readers {readers} writers {writers} top_k {top_k} live {live} secs {secs}"
    );
    println!(
        "  search {:.0}/s  p50 {:.3}  p95 {:.3}  p99 {:.3} ms  (n {})",
        search.len() as f64 / wall,
        percentile(&mut search.clone(), 0.50),
        percentile(&mut search.clone(), 0.95),
        percentile(&mut search, 0.99),
        n as u64
    );
    println!(
        "  writer {:.1} batches/s ({:.0} rows/s)  p50 {:.2}  p95 {:.2}  p99 {:.2} ms",
        write.len() as f64 / wall,
        write.len() as f64 * batch as f64 / wall,
        percentile(&mut write.clone(), 0.50),
        percentile(&mut write.clone(), 0.95),
        percentile(&mut write, 0.99)
    );
    println!(
        "  transient {:.1} MB (peak {:+.1} MB over base, heap at stop {:+.1} MB)",
        mb(peak.saturating_sub(heap_at_stop)),
        mb(peak.saturating_sub(base)),
        mb(heap_at_stop) - mb(base)
    );
    println!(
        "  per search: {:.0} allocations, {:.0} bytes; retries {retries} ({:.3}% of searches)",
        allocs as f64 / n,
        bytes as f64 / n,
        100.0 * retries as f64 / n
    );
    drop(col);
    drop(db);
    std::fs::remove_dir_all(&dir).ok();
}
