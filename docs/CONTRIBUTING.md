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
cargo test -p zenar-core            # Rust unit and integration tests, no Python needed
PYO3_PYTHON=.venv/bin/python cargo test --workspace   # all Rust tests; the binding needs Python 3.10+
pytest -q                           # Python API and crash tests (needs maturin develop first)
cargo clippy -p zenar-core          # lint
```

The Python binding needs an interpreter (3.10 or newer) at build time. Point PyO3 at one:
`PYO3_PYTHON=.venv/bin/python cargo build --workspace`. Wheels are built by maturin, which enables the
`extension-module` feature. Without it a plain build links against libpython.

## Rules

- zenar-core stays pure Rust. No PyO3 types in `zenar-core`.
- No `unwrap()` or `expect()` on anything derived from user input. Return a `ZenarError`.
- No SQL. Filters are evaluated by the document store, and field names are checked against the schema
  before evaluation. Never add a code path that builds a query from user input.
- Changes to the write path need a crash test (see STORAGE.md).
- Changes that affect performance need a `bench` or `filter_probe` run recorded in BENCHMARKS.md.
- TurboVec is pinned with `=`. Upgrading it means re-running the Rust and
  Python test suites and the benchmarks.

## Releasing

1. Bump the version in `pyproject.toml`, `python/zenar/__init__.py`, and both `Cargo.toml` manifests, and refresh
   `Cargo.lock` with `cargo build`. Add the release to `CHANGELOG.md`.
2. Merge it to `main` with a commit message that does **not** contain `[skip ci]`. GitHub skips every workflow for a
   push whose head commit has that marker, and a tag push counts. The release workflow then never runs.
3. Tag the merge commit as `vX.Y.Z` and push the tag. The release workflow builds, smoke-tests, and creates the
   GitHub release. Publishing to PyPI stays off until the `PYPI_PUBLISH` repository variable is `true`.
