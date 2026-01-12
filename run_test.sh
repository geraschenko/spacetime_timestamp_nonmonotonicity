#!/bin/bash
set -euxo pipefail

# Build the WASM module
cargo build -p module --target wasm32-unknown-unknown --release

# Generate Rust bindings from the WASM
spacetime generate --lang rust --out-dir cli/src/generated --bin-path target/wasm32-unknown-unknown/release/module.wasm

# Publish to SpacetimeDB (creates or updates database)
spacetime publish timestamp-collision --bin-path target/wasm32-unknown-unknown/release/module.wasm --clear-database --yes

# Run the test
cargo run -p cli --release
