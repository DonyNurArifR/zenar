//! Memory of background compaction (docs/R3.md, section 14). Loads `collections` collections of `live` documents
//! each (dimension 64), then, for `rounds` rounds, deletes 60% of every collection at once, so each collection
//! compacts in the background, and waits for every swap. Reports the heap in use before the deletes, the peak
//! during the builds, the heap after the swaps, and the heap after each round.
//!
//! The probe waits for a swap by watching the log shrink. The old row table is dropped after the lock is released,
//! so the heap is read after a short settle.
//!
//! Usage: cargo run --release -p zenar-core --example build_memory -- [live] [collections] [rounds]

use std::alloc::{GlobalAlloc, Layout, System};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use zenar_core::{Collection, MetaValue, Record, VectorDB};

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

fn mb(bytes: usize) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

fn record(id: u64) -> Record {
    let mut s = id.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let vector = (0..DIM)
        .map(|_| {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            (s % 2000) as f32 / 1000.0 - 1.0
        })
        .collect();
    Record {
        id,
        vector,
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

/// Waits until the log is shorter than `before`, which is the swap's rename, or gives up.
fn wait_for_swap(log: &Path, before: u64) {
    let start = Instant::now();
    while std::fs::metadata(log).map(|m| m.len()).unwrap_or(0) >= before {
        assert!(
            start.elapsed() < Duration::from_secs(120),
            "swap did not happen"
        );
        thread::sleep(Duration::from_millis(5));
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let live: u64 = args.get(1).map_or(200_000, |a| a.parse().expect("live"));
    let collections: usize = args.get(2).map_or(1, |a| a.parse().expect("collections"));
    let rounds: usize = args.get(3).map_or(3, |a| a.parse().expect("rounds"));
    let readers: usize = args.get(4).map_or(0, |a| a.parse().expect("readers"));
    let writer: bool = args.get(5).is_some_and(|a| a == "writer");

    let dir = std::env::temp_dir().join(format!("zenar-build-memory-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let db = VectorDB::open(&dir).expect("open");
    let cols: Vec<(Arc<Collection>, PathBuf)> = (0..collections)
        .map(|c| {
            let name = format!("c{c}");
            let col = db.create_collection(&name, Some(DIM), 4).expect("create");
            let mut next = 0u64;
            while next < live {
                let records: Vec<Record> = (next..(next + 1000).min(live)).map(record).collect();
                col.add_many(&records).expect("seed");
                next += 1000;
            }
            let log = dir.join("docs").join(format!("{name}.zdocs"));
            (col, log)
        })
        .collect();
    db.flush().expect("flush");
    let base = HEAP.load(Ordering::Relaxed);
    println!(
        "{collections} collection(s) x {live} live, dimension {DIM}: heap in use before any compaction {:.1} MB",
        mb(base)
    );

    // Readers search the first collection for the whole run, so a build runs next to searches.
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let reader_threads: Vec<_> = (0..readers)
        .map(|r| {
            let (col, stop) = (Arc::clone(&cols[0].0), Arc::clone(&stop));
            thread::spawn(move || {
                let mut i = r as u64;
                while !stop.load(Ordering::Relaxed) {
                    let query = record(i * 7919).vector;
                    col.search(&query, 10, None).expect("search");
                    i += 1;
                }
            })
        })
        .collect();

    // A writer adds 1,000-row batches to the first collection for the whole run, so writes land during a build.
    let writer_thread = writer.then(|| {
        let (col, stop) = (Arc::clone(&cols[0].0), Arc::clone(&stop));
        thread::spawn(move || {
            let mut id = 10_000_000u64;
            while !stop.load(Ordering::Relaxed) {
                let records: Vec<Record> = (id..id + 1000).map(record).collect();
                col.add_many(&records).expect("writer add");
                id += 1000;
                thread::sleep(Duration::from_millis(20));
            }
        })
    });

    if rounds == 0 {
        // Writes alone, with no deletes: the peak the writer's own batches cause.
        PEAK.store(HEAP.load(Ordering::Relaxed), Ordering::Relaxed);
        thread::sleep(Duration::from_secs(5));
        println!(
            "writes only, 5 s: peak {:.1} MB (+{:.1} over base), heap {:.1} MB",
            mb(PEAK.load(Ordering::Relaxed)),
            mb(PEAK.load(Ordering::Relaxed).saturating_sub(base)),
            mb(HEAP.load(Ordering::Relaxed))
        );
    }

    // Each round: every collection loses its oldest 60% (all at once, so the builds overlap), then waits for its swap.
    let drop_n = live * 6 / 10;
    let mut oldest = vec![0u64; collections];
    let mut next = vec![live; collections];
    for round in 0..rounds {
        let before_logs: Vec<u64> = cols
            .iter()
            .map(|(_, log)| std::fs::metadata(log).expect("log").len())
            .collect();
        PEAK.store(HEAP.load(Ordering::Relaxed), Ordering::Relaxed);
        let t = Instant::now();
        for (c, (col, _)) in cols.iter().enumerate() {
            let ids: Vec<u64> = (oldest[c]..oldest[c] + drop_n).collect();
            oldest[c] += drop_n;
            col.delete_many(&ids).expect("delete");
        }
        let deleted_in = t.elapsed();
        for (c, (_, log)) in cols.iter().enumerate() {
            wait_for_swap(log, before_logs[c]);
        }
        let swapped_in = t.elapsed();
        let peak_during = PEAK.load(Ordering::Relaxed);
        thread::sleep(Duration::from_millis(300));
        let after = HEAP.load(Ordering::Relaxed);
        println!(
            "round {round}: deletes returned in {:.0} ms, all swaps done in {:.0} ms; peak during the round {:.1} MB (+{:.1} over base); heap after the swaps {:.1} MB",
            deleted_in.as_secs_f64() * 1e3,
            swapped_in.as_secs_f64() * 1e3,
            mb(peak_during),
            mb(peak_during.saturating_sub(base)),
            mb(after),
        );
        // Refill, so the next round compacts again: add 60% of the live size as new ids.
        for (c, (col, _)) in cols.iter().enumerate() {
            let records: Vec<Record> = (next[c]..next[c] + drop_n).map(record).collect();
            for chunk in records.chunks(1000) {
                col.add_many(chunk).expect("refill");
            }
            next[c] += drop_n;
        }
        db.flush().expect("flush");
        println!(
            "round {round}: refilled, heap {:.1} MB",
            mb(HEAP.load(Ordering::Relaxed))
        );
    }
    stop.store(true, Ordering::Relaxed);
    for handle in reader_threads {
        handle.join().expect("reader");
    }
    if let Some(handle) = writer_thread {
        handle.join().expect("writer");
    }
    drop(cols);
    drop(db);
    std::fs::remove_dir_all(&dir).ok();
}
