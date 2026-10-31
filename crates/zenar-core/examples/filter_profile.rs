//! Times each leaf of the compound filter on the document store alone, at 1M documents.
//! Usage: cargo run --release -p zenar-core --example filter_profile -- [rows]

use std::time::Instant;

use zenar_core::docstore::{DocRef, DocStore};
use zenar_core::{FieldKind, Filter, MetaValue};

fn main() {
    let rows: u64 = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(1_000_000);
    let dir = std::env::temp_dir().join(format!("zenar-profile-{rows}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let schema = vec![
        ("tenant".to_string(), FieldKind::Text),
        ("category".to_string(), FieldKind::Text),
        ("price".to_string(), FieldKind::Float),
    ];
    let categories = ["faq", "product", "tools", "electronics", "appliances"];
    let mut store = DocStore::create(&dir.join("docs.zdocs"), schema).unwrap();
    for chunk in (0..rows).collect::<Vec<_>>().chunks(10_000) {
        let metas: Vec<Vec<(String, MetaValue)>> = chunk
            .iter()
            .map(|&i| {
                vec![
                    (
                        "tenant".to_string(),
                        MetaValue::Text(if i % 10 == 0 { "A" } else { "B" }.into()),
                    ),
                    (
                        "category".to_string(),
                        MetaValue::Text(categories[(i % 5) as usize].into()),
                    ),
                    ("price".to_string(), MetaValue::Float((i % 1000) as f64)),
                ]
            })
            .collect();
        let refs: Vec<DocRef<'_>> = chunk
            .iter()
            .zip(metas.iter())
            .map(|(&i, m)| DocRef {
                id: i,
                text: "doc",
                metadata: m.as_slice(),
            })
            .collect();
        let staged = store.stage_puts(&refs).unwrap();
        store.commit(staged);
    }

    let tenant_a = Filter::Eq {
        field: "tenant".into(),
        value: MetaValue::Text("A".into()),
    };
    let cats = Filter::In {
        field: "category".into(),
        values: vec![
            MetaValue::Text("faq".into()),
            MetaValue::Text("product".into()),
        ],
    };
    let price = Filter::Gte {
        field: "price".into(),
        value: MetaValue::Float(100.0),
    };
    let compound = Filter::And(vec![tenant_a.clone(), cats.clone(), price.clone()]);

    let time = |label: &str, f: &Filter| {
        let mut best = f64::MAX;
        let mut n = 0;
        for _ in 0..5 {
            let t = Instant::now();
            n = store.matching_ids(f).unwrap().len();
            best = best.min(t.elapsed().as_secs_f64() * 1e3);
        }
        println!("{label:<34} {best:>8.2} ms  ({n} matches)");
    };
    time("tenant = A", &tenant_a);
    time("category IN (faq, product)", &cats);
    time("price >= 100", &price);
    time("compound (all three)", &compound);
    let _ = std::fs::remove_dir_all(&dir);
}
