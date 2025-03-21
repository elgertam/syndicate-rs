all: binary-debug

# cargo install cargo-watch
watch:
	cargo watch -c -x check -x 'test -- --nocapture'

inotifytest:
	inotifytest sh -c 'reset; cargo build && RUST_BACKTRACE=1 cargo test -- --nocapture'

binary-debug:
	cargo build --all-targets

bench:
	cargo bench --benches -- --save-baseline $$(git branch --show-current)

compare-to-main:
	cargo bench --benches -- --load-baseline $$(git branch --show-current) --baseline main
