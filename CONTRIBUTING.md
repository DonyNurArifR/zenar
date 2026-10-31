# Contributing

## Setup

```sh
rustup toolchain install stable     # MSRV is 1.89
python3 -m venv .venv && . .venv/bin/activate
pip install maturin pytest numpy
maturin develop --release           # builds the extension into .venv
```

## Test

```sh
cargo test -p zenar-core            # Rust unit and integration tests
pytest -q                           # Python API and crash tests (needs maturin develop first)
cargo clippy -p zenar-core          # lint
```

The Python binding needs an interpreter (3.10 or newer) at build time. Point PyO3 at one:
`PYO3_PYTHON=.venv/bin/python cargo build --workspace`. Wheels are built by maturin, which enables the
`extension-module` feature. Without it a plain build links against libpython.

## Rules

- zenar-core stays pure Rust. No PyO3 types in `zenar-core`.
- No `unwrap()` or `expect()` on anything derived from user input. Return a `ZenarError`.
- Never build SQL by string interpolation. Values go through `$n` parameters. Identifiers are validated
  against the allowlist before use.
- Changes to the write path need a crash test (see STORAGE.md).
- Changes that affect performance need a `bench` or `filter_probe` run recorded in BENCHMARKS.md.
- TurboVec is pinned with `=`. Upgrading it means re-running the Rust and
  Python test suites and the benchmarks.
