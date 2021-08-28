all: binary-debug

# cargo install cargo-watch
watch:
	cargo watch -c -x check -x 'test -- --nocapture'

inotifytest:
	inotifytest sh -c 'reset; cargo build && RUST_BACKTRACE=1 cargo test -- --nocapture'

binary-debug:
	cargo build --all-targets
