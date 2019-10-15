# cargo install cargo-watch
watch:
	cargo watch -c -x check -x 'test -- --nocapture'

inotifytest:
	inotifytest sh -c 'reset; cargo build && RUST_BACKTRACE=1 cargo test -- --nocapture'

arm-binary: arm-binary-release

arm-binary-setup:
	rustup target add armv7-unknown-linux-musleabihf

# sudo apt install binutils-arm-linux-gnueabihf
arm-binary-release: arm-binary-setup
	CARGO_TARGET_ARMV7_UNKNOWN_LINUX_MUSLEABIHF_LINKER=arm-linux-gnueabihf-ld cargo build --target=armv7-unknown-linux-musleabihf --release
arm-binary-debug: arm-binary-setup
	CARGO_TARGET_ARMV7_UNKNOWN_LINUX_MUSLEABIHF_LINKER=arm-linux-gnueabihf-ld cargo build --target=armv7-unknown-linux-musleabihf
