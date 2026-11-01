//! Resident memory of a document store under a sliding-window workload: each round adds new
//! documents and deletes the oldest live ones. Usage:
//! cargo run --release -p zenar-core --example memory_probe -- [rounds] [retain|compact]
//! `retain` never compacts, so dead rows stay in memory (the state a swap without reopen leaves).
//! `compact` runs the current compaction, which reopens the log and rebuilds the indexes.

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::VecDeque;
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};

use zenar_core::docstore::{DocRef, DocStore};
use zenar_core::{FieldKind, MetaValue};

/// Counts the bytes the program has allocated and not yet freed: the heap in use, which the process's
/// resident size cannot show, because the allocator keeps freed pages.
struct Counting;

static HEAP: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        HEAP.fetch_add(layout.size(), Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        HEAP.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        HEAP.fetch_add(new_size, Ordering::Relaxed);
        HEAP.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

fn rss_mb() -> f64 {
    let out = Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .expect("ps");
    let kb: f64 = String::from_utf8_lossy(&out.stdout)
        .trim()
        .parse()
        .unwrap_or(0.0);
    kb / 1024.0
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let rounds: usize = args.get(1).map_or(6, |a| a.parse().expect("rounds"));
    let compact = args.get(2).is_some_and(|m| m == "compact");
    let dir = std::env::temp_dir().join(format!("zenar-memory-probe-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("d.zdocs");
    let schema = vec![
        ("tenant".to_string(), FieldKind::Text),
        ("category".to_string(), FieldKind::Text),
        ("price".to_string(), FieldKind::Float),
    ];
    let mut docs = DocStore::create(&path, schema.clone()).unwrap();
    let per_round = 100_000u64;
    let expire = 90_000usize;
    let mut next = 0u64;
    let mut window: VecDeque<u64> = VecDeque::new();
    println!(
        "mode {}, {} rounds, +{} / -{} per round",
        if compact { "compact" } else { "retain" },
        rounds,
        per_round,
        expire
    );
    println!("round   live     dead   heap in use (MB)   resident (MB)   log bytes");
    for round in 1..=rounds {
        let ids: Vec<u64> = (next..next + per_round).collect();
        next += per_round;
        for chunk in ids.chunks(10_000) {
            let metas: Vec<Vec<(String, MetaValue)>> = chunk
                .iter()
                .map(|&i| {
                    vec![
                        (
                            "tenant".into(),
                            MetaValue::Text(if i % 2 == 0 { "A" } else { "B" }.into()),
                        ),
                        (
                            "category".into(),
                            MetaValue::Text(
                                ["faq", "product", "tools", "electronics", "appliances"]
                                    [(i % 5) as usize]
                                    .into(),
                            ),
                        ),
                        ("price".into(), MetaValue::Float((i % 1000) as f64)),
                    ]
                })
                .collect();
            let refs: Vec<DocRef<'_>> = chunk
                .iter()
                .zip(metas.iter())
                .map(|(&i, m)| DocRef {
                    id: i,
                    text: "doc",
                    metadata: m,
                })
                .collect();
            let staged = docs.stage_puts(&refs).unwrap();
            docs.commit(staged);
            window.extend(chunk.iter().copied());
        }
        let victims: Vec<u64> = (0..expire).filter_map(|_| window.pop_front()).collect();
        for chunk in victims.chunks(10_000) {
            let staged = docs.stage_deletes(chunk).unwrap();
            docs.sync().unwrap();
            docs.commit(staged);
        }
        if compact && docs.should_compact() {
            docs.compact().unwrap();
        }
        println!(
            "{round:>5} {:>7} {:>8} {:>18.1} {:>15.1} {:>11}",
            docs.live_count(),
            next as usize - docs.live_count(),
            HEAP.load(Ordering::Relaxed) as f64 / (1024.0 * 1024.0),
            rss_mb(),
            std::fs::metadata(&path).unwrap().len()
        );
    }
    drop(docs);
    std::fs::remove_dir_all(&dir).ok();
}
