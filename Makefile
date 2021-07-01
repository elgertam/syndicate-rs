# cargo install cargo-watch
watch:
	cargo watch -c -x check -x 'test -- --nocapture'

run-watch:
	RUST_BACKTRACE=1 cargo watch -c -x 'build --all-targets' -x 'run'

clippy-watch:
	cargo watch -c -x clippy

inotifytest:
	inotifytest sh -c 'reset; cargo build && RUST_BACKTRACE=1 cargo test -- --nocapture'

binary: binary-release

binary-release:
	cargo build --release --all-targets

binary-debug:
	cargo build --all-targets

# OK, rather than doing it myself (per
# https://eighty-twenty.org/2019/10/15/cross-compiling-rust), it turns
# out past a certain level of complexity we need more than just a
# linker but also a C compiler, compatible headers, and so forth. This
# proved nontrivial. The Rust team maintains a docker-based
# cross-compilation environment that's very easy to use, so instead,
# I'll just use that!
#
# cargo install cross
#
# The `vendored-openssl` thing is necessary because otherwise I'd have
# to mess about with getting a musl build of openssl, plus its headers
# etc, ready on my system despite being otherwise able to rely on
# cross. I think. It's a bit confusing.

arm-binary: arm-binary-release

arm-binary-release:
	cross build --target=armv7-unknown-linux-musleabihf --release --all-targets --features vendored-openssl

arm-binary-debug:
	cross build --target=armv7-unknown-linux-musleabihf --all-targets --features vendored-openssl

pull-protocols:
	git subtree pull -P protocols \
		-m 'Merge latest changes from the syndicate-protocols repository' \
		git@git.syndicate-lang.org:syndicate-lang/syndicate-protocols \
		main
