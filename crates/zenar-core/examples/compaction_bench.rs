//! Compaction under load: a sliding window on one collection, with reader threads searching while a
//! writer adds and deletes. Deletes make dead rows, and the compactions they trigger run inside the
//! delete. Reports reader latency, writer latency and lock holds, compactions and heap in use.
//! Heap is counted with a global allocator, so peak and live memory are exact for this process.
//!
//! Usage: cargo run --release -p zenar-core --example compaction_bench -- [live] [rounds] [readers]

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::time::Instant;

use zenar_core::{MetaValue, Record, VectorDB};

struct Counting;

static HEAP: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let now = HEAP.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
        PEAK.fetch_max(now, Ordering::Relaxed);
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
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

const DIM: usize = 64;

fn vector(id: u64) -> Vec<f32> {
    let mut s = id.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    (0..DIM)
        .map(|_| {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            (s % 2000) as f32 / 1000.0 - 1.0
        })
        .collect()
}

fn record(id: u64) -> Record {
    Record {
        id,
        vector: vector(id),
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

fn pct(v: &mut [f64], p: f64) -> f64 {
    if v.is_empty() {
        return f64::NAN;
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[((v.len() as f64 - 1.0) * p).round() as usize]
}

fn mb(bytes: usize) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let live: u64 = args.get(1).map_or(200_000, |a| a.parse().expect("live"));
    let rounds: usize = args.get(2).map_or(40, |a| a.parse().expect("rounds"));
    let readers: usize = args.get(3).map_or(8, |a| a.parse().expect("readers"));
    let batch: u64 = 1000;
    let per_round: u64 = 20 * batch;

    let dir = std::env::temp_dir().join(format!("zenar-compaction-bench-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let db = VectorDB::open(&dir).expect("open");
    let col = db.create_collection("w", Some(DIM), 4).expect("create");
    let log = dir.join("docs").join("w.zdocs");
    let mut next = 0u64;
    while next < live {
        let records: Vec<Record> = (next..next + batch).map(record).collect();
        col.add_many(&records).expect("seed");
        next += batch;
    }
    db.flush().expect("flush");
    let base_heap = HEAP.load(Ordering::Relaxed);
    PEAK.store(base_heap, Ordering::Relaxed);

    let stop = Arc::new(AtomicBool::new(false));
    let epoch = Instant::now();
    let mut reader_handles = Vec::new();
    for r in 0..readers {
        let (col, stop) = (Arc::clone(&col), Arc::clone(&stop));
        reader_handles.push(thread::spawn(move || {
            let mut lat = Vec::new();
            let mut i = r as u64;
            while !stop.load(Ordering::Relaxed) {
                let q = vector(i * 7919);
                let t = Instant::now();
                col.search(&q, 10, None).expect("search");
                lat.push((
                    t.duration_since(epoch).as_secs_f64(),
                    t.elapsed().as_secs_f64() * 1e3,
                ));
                i += 1;
            }
            lat
        }));
    }

    let mut delete_holds = Vec::new();
    // (round, start, end, duration ms, peak heap in MB) for each delete batch that compacted.
    let mut compactions: Vec<(usize, f64, f64, f64, f64)> = Vec::new();
    let mut round_heap = Vec::new();
    let mut oldest = 0u64;
    let mut writer_batches = Vec::new();
    let run_start = Instant::now();
    for round in 0..rounds {
        for _ in 0..(per_round / batch) {
            let records: Vec<Record> = (next..next + batch).map(record).collect();
            let t = Instant::now();
            col.add_many(&records).expect("add_many");
            writer_batches.push(t.elapsed().as_secs_f64() * 1e3);
            next += batch;
            let ids: Vec<u64> = (oldest..oldest + batch).collect();
            oldest += batch;
            let before = std::fs::metadata(&log).expect("log").len();
            PEAK.store(HEAP.load(Ordering::Relaxed), Ordering::Relaxed);
            let t = Instant::now();
            col.delete_many(&ids).expect("delete_many");
            let took = t.elapsed().as_secs_f64() * 1e3;
            let peak_during = mb(PEAK.load(Ordering::Relaxed));
            let after = std::fs::metadata(&log).expect("log").len();
            delete_holds.push(took);
            if after < before {
                compactions.push((
                    round,
                    t.duration_since(epoch).as_secs_f64(),
                    t.duration_since(epoch).as_secs_f64() + took / 1e3,
                    took,
                    peak_during,
                ));
            }
        }
        round_heap.push(mb(HEAP.load(Ordering::Relaxed)));
    }
    let wall = run_start.elapsed().as_secs_f64();
    stop.store(true, Ordering::Relaxed);
    let samples: Vec<(f64, f64)> = reader_handles
        .into_iter()
        .flat_map(|h| h.join().expect("reader"))
        .collect();
    let mut search: Vec<f64> = samples.iter().map(|s| s.1).collect();
    let search_n = search.len();
    // Searches that started during a compacting delete batch.
    let mut during: Vec<f64> = samples
        .iter()
        .filter(|(start, _)| {
            compactions
                .iter()
                .any(|(_, cs, ce, _, _)| *start >= *cs && *start <= *ce)
        })
        .map(|s| s.1)
        .collect();

    let live_now = col.count().expect("count");
    println!(
        "collection: {live} live at start, {} live at end, dim {DIM}, {rounds} rounds of +{per_round} / -{per_round}, {readers} readers",
        live_now
    );
    println!("run: {wall:.1} s; compactions: {}", compactions.len());
    println!(
        "search: {:.0}/s  p50 {:.3}  p95 {:.3}  p99 {:.3} ms  (n = {search_n})",
        search_n as f64 / wall,
        pct(&mut search.clone(), 0.50),
        pct(&mut search.clone(), 0.95),
        pct(&mut search, 0.99)
    );
    println!(
        "writer batches: p50 {:.2}  p95 {:.2}  p99 {:.2} ms",
        pct(&mut writer_batches.clone(), 0.50),
        pct(&mut writer_batches.clone(), 0.95),
        pct(&mut writer_batches, 0.99)
    );
    let mut holds = delete_holds.clone();
    println!(
        "delete batches (lock held, includes compaction): p50 {:.1}  p95 {:.1}  max {:.1} ms",
        pct(&mut holds.clone(), 0.50),
        pct(&mut holds.clone(), 0.95),
        pct(&mut holds, 1.0)
    );
    println!(
        "searches that started during a compacting batch: n = {}, p50 {:.3}  p95 {:.3}  p99 {:.3} ms",
        during.len(),
        pct(&mut during.clone(), 0.50),
        pct(&mut during.clone(), 0.95),
        pct(&mut during, 0.99)
    );
    for (round, _, _, took, peak) in &compactions {
        println!(
            "  compaction in round {round}: delete batch {took:.1} ms, peak heap during it {peak:.1} MB"
        );
    }
    println!(
        "heap: base {:.1} MB, peak {:.1} MB, end {:.1} MB; per round: {}",
        mb(base_heap),
        mb(PEAK.load(Ordering::Relaxed)),
        mb(HEAP.load(Ordering::Relaxed)),
        round_heap
            .iter()
            .map(|h| format!("{h:.1}"))
            .collect::<Vec<_>>()
            .join(" ")
    );
    drop(col);
    drop(db);
    std::fs::remove_dir_all(&dir).ok();
}
