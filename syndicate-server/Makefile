all: binary-debug

# cargo install cargo-watch
watch:
	cargo watch -c -x check -x 'test -- --nocapture'

run-watch:
	RUST_BACKTRACE=1 cargo watch -c -x 'build --all-targets' -x 'run'

inotifytest:
	inotifytest sh -c 'reset; cargo build && RUST_BACKTRACE=1 cargo test -- --nocapture'

binary: binary-release

binary-release:
	cargo build --release --all-targets

binary-debug:
	cargo build --all-targets
