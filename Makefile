# cargo install cargo-watch
watch:
	cargo watch -c -x check -x 'test -- --nocapture'

run-watch:
	RUST_BACKTRACE=1 cargo watch -c -x run

clippy-watch:
	cargo watch -c -x clippy

inotifytest:
	inotifytest sh -c 'reset; cargo build && RUST_BACKTRACE=1 cargo test -- --nocapture'

binary: binary-release

binary-release:
	cargo build --release --all-targets

binary-debug:
	cargo build --all-targets

arm-binary: arm-binary-release

arm-binary-setup:
	rustup target add armv7-unknown-linux-musleabihf

# sudo apt install binutils-arm-linux-gnueabihf
arm-binary-release: arm-binary-setup
	CARGO_TARGET_ARMV7_UNKNOWN_LINUX_MUSLEABIHF_LINKER=arm-linux-gnueabihf-ld cargo build --target=armv7-unknown-linux-musleabihf --release --all-targets
arm-binary-debug: arm-binary-setup
	CARGO_TARGET_ARMV7_UNKNOWN_LINUX_MUSLEABIHF_LINKER=arm-linux-gnueabihf-ld cargo build --target=armv7-unknown-linux-musleabihf --all-targets
