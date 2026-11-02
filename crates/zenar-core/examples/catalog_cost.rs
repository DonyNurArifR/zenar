//! Cost of one catalog write: the atomic replacement of `catalog.json` (write a temporary file, sync
//! it, rename it over the catalog, sync the directory). Every delete makes two of these under the
//! collection lock, and compaction makes one more. Usage: cargo run --release -p zenar-core --example catalog_cost

use std::time::Instant;

use zenar_core::VectorDB;
use zenar_core::catalog::Catalog;

fn main() {
    let dir = std::env::temp_dir().join(format!("zenar-catalog-cost-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    {
        let db = VectorDB::open(&dir).expect("open");
        db.create_collection("c", Some(384), 4).expect("create");
    }
    let catalog = Catalog::open(&dir.join("catalog.json")).expect("catalog");
    let n = 200;
    let t = Instant::now();
    for i in 0..n {
        catalog
            .update("c", |entry| entry.durable_log_len = i as u64)
            .expect("update");
    }
    let mean = t.elapsed().as_secs_f64() * 1e3 / n as f64;
    println!("catalog update (atomic replace of catalog.json): {mean:.3} ms mean over {n} writes");
    std::fs::remove_dir_all(&dir).ok();
}
