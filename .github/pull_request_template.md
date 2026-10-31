## What changed

<!-- A short description of the change and why. -->

## How it was checked

- [ ] `cargo test -p zenar-core` passes
- [ ] `pytest -q tests` passes after `maturin develop --release`
- [ ] `cargo fmt --all -- --check` and `cargo clippy --workspace -- -D warnings` pass
- [ ] Docs updated if behaviour or numbers changed (`docs/`, `BENCHMARKS.md`)

## Notes for the reviewer

<!-- Anything that needs a closer look. -->
