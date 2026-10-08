//! Not a test: writes a synthetic client database to `WORTEL_FIXTURE_DIR` so
//! the Python bindings can be smoke-tested against a real on-disk layout.
//! Run explicitly with:
//!   WORTEL_FIXTURE_DIR=/tmp/fixture cargo test --test make_fixture -- --ignored


#[test]
#[ignore = "fixture generator, run explicitly with WORTEL_FIXTURE_DIR set"]
fn write_fixture() {
    let dir = std::env::var("WORTEL_FIXTURE_DIR").expect("WORTEL_FIXTURE_DIR not set");
    let path = std::path::Path::new(&dir);
    std::fs::create_dir_all(path).unwrap();
    let (hash, _) = wortel::fixture::build_db(path);
    // Hand the block hash to the caller.
    std::fs::write(
        path.join("block_hash.hex"),
        hash.as_bytes()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>(),
    )
    .unwrap();
}
