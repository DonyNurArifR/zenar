//! Splits filtered-search latency at 1M rows into its two parts (spec section 90):
//!   (a) Stoolap candidate-id query, with and without an index on the filter column
//!   (b) TurboVec search_with_allowlist over the candidate ids vs. unfiltered search
//! Usage: cargo run --release -p zenar-core --example filter_probe -- <rows> <dim>

use std::time::Instant;

use stoolap::Database;
use turbovec::IdMapIndex;

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

fn time_ms<T>(mut f: impl FnMut() -> T, reps: usize) -> f64 {
    let mut best = f64::MAX;
    for _ in 0..reps {
        let t = Instant::now();
        std::hint::black_box(f());
        best = best.min(t.elapsed().as_secs_f64() * 1e3);
    }
    best
}

fn main() {
    if let Ok(dsn) = std::env::var("PROBE_DSN") {
        let db = Database::open(&dsn).unwrap();
        let sql = std::env::var("PROBE_SQL").unwrap_or_else(|_| {
            "SELECT id FROM zdocs_bench WHERE status = 'READY' AND (\"tenant\" = $1)".into()
        });
        for n in [1usize, 3, 10] {
            let ph: Vec<String> = (1..=n).map(|i| format!("${i}")).collect();
            let sql = format!(
                "SELECT id, text FROM zdocs_bench WHERE status = 'READY' AND id IN ({})",
                ph.join(", ")
            );
            let stmt = db.prepare(&sql).unwrap();
            let params: Vec<stoolap::Value> = (0..n)
                .map(|i| stoolap::Value::integer(i as i64 * 1000 + 5))
                .collect();
            println!(
                "pk lookup ({n} ids), prepared: {:.3} ms",
                time_ms(
                    || stmt.query(params.clone()).unwrap().collect_vec().unwrap(),
                    5
                )
            );
        }
        let cols = db.prepare("SELECT id, text, \"tenant\", \"category\", \"price\" FROM zdocs_bench WHERE status = 'READY' AND id IN ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)").unwrap();
        let p10b: Vec<stoolap::Value> = (0..10)
            .map(|i| stoolap::Value::integer(i * 1000 + 5))
            .collect();
        println!(
            "pk lookup (10 ids, +3 altered cols), prepared: {:.3} ms",
            time_ms(
                || cols.query(p10b.clone()).unwrap().collect_vec().unwrap(),
                5
            )
        );
        let no_status = db.prepare("SELECT id, text FROM zdocs_bench WHERE id IN ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)").unwrap();
        let p10: Vec<stoolap::Value> = (0..10)
            .map(|i| stoolap::Value::integer(i * 1000 + 5))
            .collect();
        println!(
            "pk lookup (10 ids, no status), prepared: {:.3} ms",
            time_ms(
                || no_status.query(p10.clone()).unwrap().collect_vec().unwrap(),
                5
            )
        );
        let n = db.query(&sql, ("A",)).unwrap().collect_vec().unwrap().len();
        let stmt = db.prepare(&sql).unwrap();
        println!(
            "existing db: {n} rows, prepared {:.2} ms",
            time_ms(|| stmt.query(("A",)).unwrap().collect_vec().unwrap(), 5)
        );
        return;
    }
    let args: Vec<String> = std::env::args().collect();
    let rows: u64 = args
        .get(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(1_000_000);
    let dim: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(384);
    let mut rng = Lcg(7);

    // ---- (b) TurboVec allowlist cost ----
    let mut index = IdMapIndex::new(dim, 4).unwrap();
    let batch = 10_000usize;
    let mut buf = vec![0f32; batch * dim];
    let mut id = 0u64;
    while id < rows {
        for x in buf.iter_mut() {
            *x = rng.next_f32();
        }
        let ids: Vec<u64> = (id..id + batch as u64).collect();
        index.add_with_ids_2d(&buf, dim, &ids).unwrap();
        id += batch as u64;
    }
    let query: Vec<f32> = (0..dim).map(|_| rng.next_f32()).collect();
    if std::env::var("PROBE_TV_PERROW").is_ok() {
        let mut rng2 = Lcg(9);
        let mut idx = IdMapIndex::new(dim, 4).unwrap();
        let path = std::env::temp_dir().join(format!("zenar-tv-perrow-{rows}.tvim"));
        for i in 0..rows {
            let v: Vec<f32> = (0..dim).map(|_| rng2.next_f32()).collect();
            idx.add_with_ids_2d(&v, dim, &[i]).unwrap();
            if (i + 1) % 1000 == 0 {
                idx.sync(&path).unwrap();
            }
        }
        idx.sync(&path).unwrap();
        let q: Vec<f32> = (0..dim).map(|_| rng2.next_f32()).collect();
        println!(
            "per-row built, in-memory:              {:8.2} ms",
            time_ms(|| idx.search(&q, 10), 9)
        );
        let loaded = IdMapIndex::load(&path).unwrap();
        println!(
            "per-row built, reloaded:               {:8.2} ms",
            time_ms(|| loaded.search(&q, 10), 9)
        );
        loaded.prepare();
        println!(
            "per-row built, reloaded + prepare:     {:8.2} ms",
            time_ms(|| loaded.search(&q, 10), 9)
        );
        return;
    }
    if std::env::var("PROBE_TV_LOAD").is_ok() {
        let path = std::env::temp_dir().join(format!("zenar-tv-probe-{rows}.tvim"));
        index.sync(&path).unwrap();
        let loaded = IdMapIndex::load(&path).unwrap();
        println!(
            "turbovec after load (no prepare):      {:8.2} ms",
            time_ms(|| loaded.search(&query, 10), 9)
        );
        loaded.prepare();
        println!(
            "turbovec after load + prepare:         {:8.2} ms",
            time_ms(|| loaded.search(&query, 10), 9)
        );
        println!(
            "turbovec in-memory index (reference):  {:8.2} ms",
            time_ms(|| index.search(&query, 10), 9)
        );
        return;
    }
    let tenth: Vec<u64> = (0..rows).filter(|i| i % 10 == 0).collect();
    let all: Vec<u64> = (0..rows).collect();
    println!("rows {rows}, dim {dim}");
    println!(
        "turbovec search, no allowlist:        {:8.2} ms",
        time_ms(|| index.search(&query, 10), 9)
    );
    println!(
        "turbovec allowlist, 10% ids:           {:8.2} ms",
        time_ms(
            || index
                .search_with_allowlist(&query, 10, Some(&tenth))
                .unwrap(),
            9
        )
    );
    println!(
        "turbovec allowlist, 100% ids:          {:8.2} ms",
        time_ms(
            || index.search_with_allowlist(&query, 10, Some(&all)).unwrap(),
            5
        )
    );

    // ---- (a) Stoolap candidate query ----
    let mem = std::env::var("PROBE_FILE").is_err();
    let dsn_dir = std::env::temp_dir().join(format!("zenar-probe-stoolap-{rows}"));
    let _ = std::fs::remove_dir_all(&dsn_dir);
    let db = if mem {
        Database::open_in_memory().unwrap()
    } else {
        Database::open(&format!(
            "file://{}/m.db?sync_mode=normal",
            dsn_dir.display()
        ))
        .unwrap()
    };
    if std::env::var("PROBE_ALTER").is_ok() {
        db.execute(
            "CREATE TABLE zdocs (id INTEGER PRIMARY KEY, text TEXT, status TEXT NOT NULL)",
            (),
        )
        .unwrap();
        db.execute("ALTER TABLE zdocs ADD COLUMN \"tenant\" TEXT", ())
            .unwrap();
        db.execute("ALTER TABLE zdocs ADD COLUMN \"category\" TEXT", ())
            .unwrap();
        db.execute("ALTER TABLE zdocs ADD COLUMN \"price\" FLOAT", ())
            .unwrap();
    } else {
        db.execute("CREATE TABLE zdocs (id INTEGER PRIMARY KEY, text TEXT, status TEXT NOT NULL, tenant TEXT, category TEXT, price FLOAT)", ()).unwrap();
    }
    let autocommit = std::env::var("PROBE_AUTOCOMMIT").is_ok();
    let mut tx = db.begin().unwrap();
    for i in 0..rows {
        let tenant = if i % 10 == 0 { "A" } else { "B" };
        let cat = ["faq", "product", "tools", "electronics", "appliances"][(i % 5) as usize];
        let status = if std::env::var("PROBE_UPDATE").is_ok() {
            "PENDING"
        } else {
            "READY"
        };
        if autocommit {
            db.execute(
                "INSERT INTO zdocs (id, text, status, tenant, category, price) VALUES ($1, $2, $3, $4, $5, $6)",
                (i as i64, "doc", status, tenant, cat, (i % 1000) as f64),
            )
            .unwrap();
        } else {
            tx.execute(
                "INSERT INTO zdocs (id, text, status, tenant, category, price) VALUES ($1, $2, $3, $4, $5, $6)",
                (i as i64, "doc", status, tenant, cat, (i % 1000) as f64),
            )
            .unwrap();
        }
    }
    if std::env::var("PROBE_UPDATE").is_ok() {
        tx.execute("UPDATE zdocs SET status = 'READY'", ()).unwrap();
    }
    tx.commit().unwrap();
    {
        let mut x: u64 = 88172645463325252;
        let ids: Vec<i64> = (0..10)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                (x % rows) as i64
            })
            .collect();
        let ph: Vec<String> = (1..=10).map(|i| format!("${i}")).collect();
        let stmt = db
            .prepare(&format!(
                "SELECT id, text FROM zdocs WHERE status = 'READY' AND id IN ({})",
                ph.join(", ")
            ))
            .unwrap();
        let params: Vec<stoolap::Value> = ids.iter().map(|i| stoolap::Value::integer(*i)).collect();
        println!(
            "rows returned: {}",
            stmt.query(params.clone())
                .unwrap()
                .collect_vec()
                .unwrap()
                .len()
        );
        println!(
            "[{}] random PK lookup (10 ids): {:8.2} ms",
            if mem { "memory" } else { "file" },
            time_ms(
                || stmt.query(params.clone()).unwrap().collect_vec().unwrap(),
                5
            )
        );
    }
    let sql = "SELECT id FROM zdocs WHERE status = 'READY' AND (\"tenant\" = $1)";
    let count = db.query(sql, ("A",)).unwrap().collect_vec().unwrap().len();
    println!(
        "[{}] stoolap candidate query (tenant=A, {count} ids), no index:  {:8.2} ms",
        if mem { "memory" } else { "file" },
        time_ms(|| db.query(sql, ("A",)).unwrap().collect_vec().unwrap(), 5)
    );
    let stmt = db.prepare(sql).unwrap();
    println!(
        "[{}] prepared candidate query, no index:          {:8.2} ms",
        if mem { "memory" } else { "file" },
        time_ms(|| stmt.query(("A",)).unwrap().collect_vec().unwrap(), 5)
    );
    db.execute("CREATE INDEX idx_tenant ON zdocs (tenant)", ())
        .unwrap();
    println!(
        "[{}] prepared candidate query, with index:        {:8.2} ms",
        if mem { "memory" } else { "file" },
        time_ms(|| stmt.query(("A",)).unwrap().collect_vec().unwrap(), 5)
    );
}
