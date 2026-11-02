//! Wall time of write operations through the public API, each timed on its own:
//! `add` (default durability), `add` with `durable=True`, `add_many` of 50, `delete` of one id,
//! and the `delete_many` batches that cross the compaction threshold on a collection of [rows] rows
//! (default 200,000). Usage: cargo run --release -p zenar-core --example write_cost -- [rows]

use std::time::Instant;

use zenar_core::{MetadataSync, Record, VectorDB};

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
        metadata: vec![],
    }
}

fn mean_ms(total: std::time::Duration, n: usize) -> f64 {
    total.as_secs_f64() * 1e3 / n as f64
}

fn main() {
    let dim = 384usize;
    let n = 200usize;
    let dir = std::env::temp_dir().join(format!("zenar-write-cost-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);

    let db = VectorDB::open(&dir).expect("open");
    let col = db.create_collection("c", Some(dim), 4).expect("create");
    for chunk in (0..10_000u64).collect::<Vec<_>>().chunks(1000) {
        let records: Vec<Record> = chunk.iter().map(|&i| record(i, dim)).collect();
        col.add_many(&records).expect("seed");
    }
    db.flush().expect("flush");

    let mut next = 10_000u64;
    let t = Instant::now();
    for _ in 0..n {
        col.add(next, &vector(next, dim), "doc", vec![])
            .expect("add");
        next += 1;
    }
    println!(
        "add (default durability)        {:>8.3} ms",
        mean_ms(t.elapsed(), n)
    );
    drop(col);
    drop(db);

    let db = VectorDB::open_with(&dir, MetadataSync::Full).expect("open durable");
    let col = db.get_collection("c").expect("get");
    let t = Instant::now();
    for _ in 0..n {
        col.add(next, &vector(next, dim), "doc", vec![])
            .expect("add durable");
        next += 1;
    }
    println!(
        "add (durable=True)              {:>8.3} ms",
        mean_ms(t.elapsed(), n)
    );

    let t = Instant::now();
    for _ in 0..n / 50 {
        let records: Vec<Record> = (next..next + 50).map(|i| record(i, dim)).collect();
        next += 50;
        col.add_many(&records).expect("add_many");
    }
    println!(
        "add_many, 50 rows (per batch)   {:>8.3} ms",
        mean_ms(t.elapsed(), n / 50)
    );

    let t = Instant::now();
    for id in 0..n as u64 {
        col.delete(id).expect("delete");
    }
    println!(
        "delete (one id)                 {:>8.3} ms",
        mean_ms(t.elapsed(), n)
    );
    drop(col);
    drop(db);
    std::fs::remove_dir_all(&dir).ok();

    // Compaction: once dead rows exceed live rows (and 4096), a delete rewrites the log under the lock.
    let dir = std::env::temp_dir().join(format!("zenar-compact-cost-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let db = VectorDB::open(&dir).expect("open");
    let col = db.create_collection("big", Some(dim), 4).expect("create");
    let rows: u64 = std::env::args()
        .nth(1)
        .map_or(200_000, |a| a.parse().expect("rows"));
    for start in (0..rows).step_by(1000) {
        let records: Vec<Record> = (start..start + 1000).map(|i| record(i, dim)).collect();
        col.add_many(&records).expect("seed big");
    }
    db.flush().expect("flush big");
    let log = dir.join("docs").join("big.zdocs");
    let mut ordinary = std::time::Duration::ZERO;
    let mut ordinary_n = 0usize;
    let mut compacting = None;
    let mut batch = 0usize;
    // Delete 1000 ids per call until the log shrinks (compaction) and then a few more batches.
    while compacting.is_none() || batch < compacting.unwrap() + 3 {
        let before = std::fs::metadata(&log).expect("log").len();
        let ids: Vec<u64> = (batch as u64 * 1000..(batch as u64 + 1) * 1000).collect();
        let t = Instant::now();
        col.delete_many(&ids).expect("delete_many");
        let took = t.elapsed();
        let after = std::fs::metadata(&log).expect("log").len();
        if after < before {
            compacting = Some(batch);
            println!(
                "delete_many, 1000 ids (compacts)  {:>8.3} ms  (log {} -> {} bytes)",
                took.as_secs_f64() * 1e3,
                before,
                after
            );
        } else if compacting.is_some() {
            println!(
                "delete_many, 1000 ids (after)     {:>8.3} ms",
                took.as_secs_f64() * 1e3
            );
        } else {
            ordinary += took;
            ordinary_n += 1;
        }
        batch += 1;
        if batch as u64 * 1000 >= rows {
            break;
        }
    }
    println!(
        "delete_many, 1000 ids (ordinary)  {:>8.3} ms  (mean of {ordinary_n})",
        mean_ms(ordinary, ordinary_n)
    );
    println!("live rows left: {}", col.count().expect("count"));
    drop(col);
    drop(db);
    std::fs::remove_dir_all(&dir).ok();
}
