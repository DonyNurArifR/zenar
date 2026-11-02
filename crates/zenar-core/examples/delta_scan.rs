//! Cost of a brute-force scan over an in-memory delta of 1K to 16K vectors (dim 384): the work a
//! reader would add per query if recent writes were kept outside the main index. Naive scalar
//! loops, so this is an upper bound on a tuned scan. Usage: cargo run --release -p zenar-core --example delta_scan
use std::time::Instant;
fn main() {
    let dim = 384usize;
    for &n in &[1024usize, 4096, 16384] {
        let data: Vec<f32> = (0..n * dim)
            .map(|i| ((i * 2654435761) % 2000) as f32 / 1000.0 - 1.0)
            .collect();
        let q: Vec<f32> = (0..dim).map(|i| (i % 17) as f32 / 17.0 - 0.5).collect();
        let runs = 200;
        let mut best = f64::MAX;
        let mut sink = 0f32;
        for _ in 0..runs {
            let t = Instant::now();
            let mut top = (f32::MIN, 0usize);
            for (row, v) in data.chunks_exact(dim).enumerate() {
                let s: f32 = v.iter().zip(&q).map(|(a, b)| a * b).sum();
                if s > top.0 {
                    top = (s, row);
                }
            }
            sink += top.0;
            best = best.min(t.elapsed().as_secs_f64() * 1e3);
        }
        println!(
            "delta {n:>6} vectors: brute-force scan {best:.3} ms (best of {runs})  [{sink:.1}]"
        );
    }
}
