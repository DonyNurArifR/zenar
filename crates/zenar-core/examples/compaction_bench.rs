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

use zenar_core::{LockEvent, MetaValue, Record, VectorDB};

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
    // A second create, for comparison: the first persist of a new index may cost more than later ones.
    let second = db.create_collection("w2", Some(DIM), 4).expect("create");
    println!(
        "create (second collection): holds {} max {:.2} ms",
        second.write_stats().write_holds,
        second.write_stats().write_hold_max_ns as f64 / 1e6
    );
    drop(second);
    // The create's one-time index persist runs before the collection exists to trace it, so it is reported on its own.
    let created = col.write_stats();
    println!(
        "create: the one-time index persist ran before the trace could start: holds {} max {:.2} ms",
        created.write_holds,
        created.write_hold_max_ns as f64 / 1e6
    );
    let log = dir.join("docs").join("w.zdocs");
    // The trace covers the seeding, so every hold after the create is listed.
    col.set_lock_trace(true);
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

    // A sampler checks every millisecond whether a build's temporary log exists and how long the log is. A build
    // is the time the temporary log exists, and a swap is the step where the log gets shorter.
    let tmp_log = dir.join("docs").join("w.log.compact");
    let sampler_stop = Arc::new(AtomicBool::new(false));
    let sampler = {
        let (stop, log, tmp_log) = (Arc::clone(&sampler_stop), log.clone(), tmp_log.clone());
        thread::spawn(move || {
            let mut samples: Vec<(f64, bool, u64)> = Vec::new();
            // The peak heap of each build: the counter's peak is reset when a build starts, and read when it ends.
            let (mut was_building, mut build_peak_max) = (false, 0usize);
            while !stop.load(Ordering::Relaxed) {
                let t = epoch.elapsed().as_secs_f64();
                let building = tmp_log.exists();
                if building && !was_building {
                    PEAK.store(HEAP.load(Ordering::Relaxed), Ordering::Relaxed);
                }
                if !building && was_building {
                    build_peak_max = build_peak_max.max(PEAK.load(Ordering::Relaxed));
                }
                was_building = building;
                let len = std::fs::metadata(&log).map_or(0, |m| m.len());
                samples.push((t, building, len));
                thread::sleep(std::time::Duration::from_millis(1));
            }
            (samples, build_peak_max)
        })
    };

    let mut delete_holds = Vec::new();
    let mut round_heap = Vec::new();
    let mut oldest = 0u64;
    let mut writer_batches = Vec::new();
    let run_start = Instant::now();
    for _ in 0..rounds {
        for _ in 0..(per_round / batch) {
            let records: Vec<Record> = (next..next + batch).map(record).collect();
            let t = Instant::now();
            col.add_many(&records).expect("add_many");
            writer_batches.push(t.elapsed().as_secs_f64() * 1e3);
            next += batch;
            let ids: Vec<u64> = (oldest..oldest + batch).collect();
            oldest += batch;
            let t = Instant::now();
            col.delete_many(&ids).expect("delete_many");
            delete_holds.push(t.elapsed().as_secs_f64() * 1e3);
        }
        round_heap.push(mb(HEAP.load(Ordering::Relaxed)));
    }
    let wall = run_start.elapsed().as_secs_f64();
    stop.store(true, Ordering::Relaxed);
    sampler_stop.store(true, Ordering::Relaxed);
    let (log_samples, build_peak_max) = sampler.join().expect("sampler");
    // A build runs from the step where the temporary log appears to the step where it is gone; the swap is in
    // that step, and the log shrinks there.
    println!("peak heap during a build: {:.1} MB", mb(build_peak_max));
    let mut builds: Vec<(f64, f64)> = Vec::new();
    let mut swaps: Vec<(f64, u64)> = Vec::new();
    let mut build_start: Option<f64> = None;
    for pair in log_samples.windows(2) {
        let (prev, cur) = (pair[0], pair[1]);
        if cur.1 && !prev.1 {
            build_start = Some(cur.0);
        } else if prev.1 && !cur.1 {
            builds.extend(build_start.take().map(|start| (start, cur.0)));
        }
        if cur.2 < prev.2 {
            swaps.push((cur.0, prev.2 - cur.2));
        }
    }
    let samples: Vec<(f64, f64)> = reader_handles
        .into_iter()
        .flat_map(|h| h.join().expect("reader"))
        .collect();
    let mut search: Vec<f64> = samples.iter().map(|s| s.1).collect();
    let search_n = search.len();
    // Searches that started during a build.
    let mut during: Vec<f64> = samples
        .iter()
        .filter(|(start, _)| builds.iter().any(|(bs, be)| *start >= *bs && *start <= *be))
        .map(|s| s.1)
        .collect();

    let live_now = col.count().expect("count");
    println!(
        "collection: {live} live at start, {} live at end, dim {DIM}, {rounds} rounds of +{per_round} / -{per_round}, {readers} readers",
        live_now
    );
    println!(
        "run: {wall:.1} s; builds: {}; swaps: {}; sampler ticks with a build in progress: {}",
        builds.len(),
        swaps.len(),
        log_samples.iter().filter(|s| s.1).count()
    );
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
        "delete batches (call time): p50 {:.1}  p95 {:.1}  max {:.1} ms",
        pct(&mut holds.clone(), 0.50),
        pct(&mut holds.clone(), 0.95),
        pct(&mut holds, 1.0)
    );
    println!(
        "searches that started during a build: n = {}, p50 {:.3}  p95 {:.3}  p99 {:.3} ms",
        during.len(),
        pct(&mut during.clone(), 0.50),
        pct(&mut during.clone(), 0.95),
        pct(&mut during, 0.99)
    );
    for (at, dropped) in &swaps {
        println!(
            "  swap at {at:.2} s: log shrank by {:.1} MB",
            *dropped as f64 / 1e6
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
    print_lock_trace(&col.take_lock_trace());
    col.set_lock_trace(false);
    let stats = col.write_stats();
    println!(
        "write lock (whole run, includes the swap): holds {}  mean {:.2} ms  max {:.2} ms; readers waited max {:.3} ms",
        stats.write_holds,
        stats.write_hold_total_ns as f64 / stats.write_holds.max(1) as f64 / 1e6,
        stats.write_hold_max_ns as f64 / 1e6,
        stats.read_wait_max_ns as f64 / 1e6,
    );
    drop(col);
    drop(db);
    std::fs::remove_dir_all(&dir).ok();
}

/// Per-operation percentiles of the exclusive holds of the collection lock: the wait for it, the hold, and the
/// filesystem time inside the hold. Times are in milliseconds.
fn print_lock_trace(events: &[LockEvent]) {
    let mut ops: Vec<&'static str> = events.iter().map(|e| e.op).collect();
    ops.sort();
    ops.dedup();
    println!(
        "lock holds by operation (ms): n, wait p50/p95/p99/max, hold p50/p95/p99/max, fs mean/max"
    );
    for op in ops {
        let of: Vec<&LockEvent> = events.iter().filter(|e| e.op == op).collect();
        let ms = |f: fn(&LockEvent) -> u64| -> Vec<f64> {
            of.iter().map(|e| f(e) as f64 / 1e6).collect()
        };
        let (mut wait, mut hold, mut fs) = (ms(|e| e.wait_ns), ms(|e| e.hold_ns), ms(|e| e.fs_ns));
        let fs_mean = fs.iter().sum::<f64>() / fs.len() as f64;
        println!(
            "  {op:<14} n {:>5}  wait {:>7.2} {:>7.2} {:>7.2} {:>8.2}  hold {:>7.2} {:>7.2} {:>7.2} {:>8.2}  fs {:>7.2} {:>8.2}",
            of.len(),
            pct(&mut wait.clone(), 0.50),
            pct(&mut wait.clone(), 0.95),
            pct(&mut wait.clone(), 0.99),
            pct(&mut wait, 1.0),
            pct(&mut hold.clone(), 0.50),
            pct(&mut hold.clone(), 0.95),
            pct(&mut hold.clone(), 0.99),
            pct(&mut hold, 1.0),
            fs_mean,
            pct(&mut fs, 1.0),
        );
    }
}
