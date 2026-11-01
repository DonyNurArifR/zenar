//! Heap held by the vector index before and after removals. Usage:
//! cargo run --release -p zenar-core --example vector_memory_probe -- [vectors] [dim]

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use zenar_core::storage::VectorStore;

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

fn mb() -> f64 {
    HEAP.load(Ordering::Relaxed) as f64 / (1024.0 * 1024.0)
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let n: u64 = args.get(1).map_or(200_000, |a| a.parse().expect("vectors"));
    let dim: usize = args.get(2).map_or(384, |a| a.parse().expect("dim"));
    let before = mb();
    let mut store = VectorStore::new(dim, 4).unwrap();
    for i in 0..n {
        store.add(&vector(i, dim), i).unwrap();
    }
    let full = mb();
    for i in 0..n * 9 / 10 {
        store.remove(i);
    }
    let after_removals = mb();
    // Refill with as many new vectors as were removed, to see whether the freed capacity is reused.
    for i in n..n + n * 9 / 10 {
        store.add(&vector(i, dim), i).unwrap();
    }
    let refilled = mb();
    println!(
        "{n} vectors, dim {dim}: heap in use {:.1} MB after the adds, {:.1} MB after removing 90%, {:.1} MB after adding 90% new ({} live)",
        full - before,
        after_removals - before,
        refilled - before,
        store.len()
    );
}
