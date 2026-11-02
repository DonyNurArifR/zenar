//! Where the time of a compaction goes: the rewrite of the live rows, the reopen that replays the
//! rewritten log, and the cost of reading each live row by id. A document store of [rows] documents
//! with half of them deleted (the state in which compaction runs). Usage:
//! cargo run --release -p zenar-core --example compaction_probe -- [rows]

use std::time::Instant;

use zenar_core::docstore::{DocRef, DocStore};
use zenar_core::{FieldKind, MetaValue};

fn main() {
    let rows: u64 = std::env::args()
        .nth(1)
        .map_or(1_000_000, |a| a.parse().expect("rows"));
    let dir = std::env::temp_dir().join(format!("zenar-compact-probe-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let schema = vec![
        ("tenant".to_string(), FieldKind::Text),
        ("category".to_string(), FieldKind::Text),
        ("price".to_string(), FieldKind::Float),
    ];
    let path = dir.join("d.zdocs");
    let mut docs = DocStore::create(&path, schema.clone()).unwrap();
    let categories = ["faq", "product", "tools", "electronics", "appliances"];
    let mut id = 0u64;
    while id < rows {
        let ids: Vec<u64> = (id..(id + 10_000).min(rows)).collect();
        let metas: Vec<Vec<(String, MetaValue)>> = ids
            .iter()
            .map(|&i| {
                vec![
                    (
                        "tenant".into(),
                        MetaValue::Text(if i % 2 == 0 { "A" } else { "B" }.into()),
                    ),
                    (
                        "category".into(),
                        MetaValue::Text(categories[(i % 5) as usize].into()),
                    ),
                    ("price".into(), MetaValue::Float((i % 1000) as f64)),
                ]
            })
            .collect();
        let refs: Vec<DocRef<'_>> = ids
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
        id = ids.last().unwrap() + 1;
    }
    docs.sync().unwrap();

    // Delete the even ids in batches of 10,000, until the store asks to compact.
    let evens: Vec<u64> = (0..rows).filter(|i| i % 2 == 0).collect();
    let mut deleted = 0usize;
    for chunk in evens.chunks(10_000) {
        if docs.should_compact() {
            break;
        }
        let staged = docs.stage_deletes(chunk).unwrap();
        docs.sync().unwrap();
        docs.commit(staged);
        deleted += chunk.len();
    }
    println!(
        "rows {rows}, deleted {deleted}, live {}, dead-triggered: {}",
        docs.live_count(),
        docs.should_compact()
    );

    let live_ids = docs.live_ids();
    let t = Instant::now();
    let fetched = docs.fetch(&live_ids).unwrap();
    let fetch_ms = t.elapsed().as_secs_f64() * 1e3;
    println!(
        "fetch of {} live rows (one pread each): {:>9.1} ms  ({:.2} us per row)",
        fetched.len(),
        fetch_ms,
        fetch_ms * 1e3 / fetched.len() as f64
    );

    // Sequential rewrite: read the log front to back, keep the PUT frames of live ids, write them out.
    // This is what a rewrite that does not read row by row would cost.
    use std::collections::HashSet;
    use std::io::{BufReader, BufWriter, Read, Write};
    let live: HashSet<u64> = live_ids.iter().copied().collect();
    let tmp = dir.join("seq.zdocs");
    let t = Instant::now();
    let mut input = BufReader::with_capacity(1 << 20, std::fs::File::open(&path).unwrap());
    let mut out = BufWriter::with_capacity(1 << 20, std::fs::File::create(&tmp).unwrap());
    let mut header = [0u8; 8];
    let mut kept = 0usize;
    while input.read_exact(&mut header).is_ok() {
        let len = u32::from_le_bytes([header[0], header[1], header[2], header[3]]) as usize;
        let mut payload = vec![0u8; len];
        input.read_exact(&mut payload).unwrap();
        if payload[0] == 1 {
            let id = u64::from_le_bytes(payload[1..9].try_into().unwrap());
            if live.contains(&id) {
                out.write_all(&header).unwrap();
                out.write_all(&payload).unwrap();
                kept += 1;
            }
        }
    }
    out.flush().unwrap();
    let seq_ms = t.elapsed().as_secs_f64() * 1e3;
    let t = Instant::now();
    out.get_ref().sync_all().unwrap();
    let sync_ms = t.elapsed().as_secs_f64() * 1e3;
    let t = Instant::now();
    std::fs::rename(&tmp, dir.join("seq-renamed.zdocs")).unwrap();
    let rename_ms = t.elapsed().as_secs_f64() * 1e3;
    println!(
        "sequential rewrite of {kept} live frames:   {seq_ms:>9.1} ms (read + filter + write, no sync)"
    );
    println!("  sync_all of the rewritten file:           {sync_ms:>9.1} ms");
    println!("  rename over the log:                      {rename_ms:>9.1} ms");

    let before = std::fs::metadata(&path).unwrap().len();
    let t = Instant::now();
    docs.compact().unwrap();
    let compact_ms = t.elapsed().as_secs_f64() * 1e3;
    let after = std::fs::metadata(&path).unwrap().len();
    println!(
        "compact() total (rewrite, sync, rename, reopen with replay): {compact_ms:>9.1} ms  log {before} -> {after} bytes"
    );

    drop(docs);
    let t = Instant::now();
    let reopened = DocStore::open(&path, schema).unwrap();
    let replay_ms = t.elapsed().as_secs_f64() * 1e3;
    println!(
        "reopen (replay of the rewritten log, {} live rows): {replay_ms:>9.1} ms",
        reopened.live_count()
    );
    std::fs::remove_dir_all(&dir).ok();
}
