check-fmt:
		cargo fmt --all -- --check

check-clippy:
		cargo clippy --all-targets --all-features --workspace -- -D warnings

cargo-sort:
		cargo install cargo-sort
		cargo sort -c -w
