__ignored__ := $(shell ./setup.sh)

# Use cargo release to manage publication and versions etc.
#
#   cargo install cargo-release

all:
	cargo build --all-targets

test:
	cargo test

test-all:
	cargo test --all-targets

ws-bump:
	cargo workspaces version \
		--no-global-tag \
		--individual-tag-prefix '%n-v' \
		--allow-branch 'main' \
		$(BUMP_ARGS)

ws-publish:
	cargo workspaces publish \
		--from-git

PROTOCOLS_BRANCH=main
pull-protocols:
	git subtree pull -P syndicate/protocols \
		-m 'Merge latest changes from the syndicate-protocols repository' \
		git@git.syndicate-lang.org:syndicate-lang/syndicate-protocols \
		$(PROTOCOLS_BRANCH)

static: static-x86_64

static-%:
	CARGO_TARGET_DIR=target/target.$* cross build --target $*-unknown-linux-musl --features vendored-openssl,jemalloc

###########################################################################

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

x86_64-binary: x86_64-binary-release

x86_64-binary-release:
	CARGO_TARGET_DIR=target/target.x86_64 cross build --target x86_64-unknown-linux-musl --release --all-targets --features vendored-openssl,jemalloc

x86_64-binary-debug:
	CARGO_TARGET_DIR=target/target.x86_64 cross build --target x86_64-unknown-linux-musl --all-targets --features vendored-openssl

armv7-binary: armv7-binary-release

armv7-binary-release:
	CARGO_TARGET_DIR=target/target.armv7 cross build --target=armv7-unknown-linux-musleabihf --release --all-targets --features vendored-openssl

armv7-binary-debug:
	CARGO_TARGET_DIR=target/target.armv7 cross build --target=armv7-unknown-linux-musleabihf --all-targets --features vendored-openssl

# As of 2023-05-12 (and probably earlier!) this is no longer required with current Rust nightlies
# # Hack to workaround https://github.com/rust-embedded/cross/issues/598
# HACK_WORKAROUND_ISSUE_598=CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_RUSTFLAGS="-C link-arg=/usr/local/aarch64-linux-musl/lib/libc.a"

aarch64-binary: aarch64-binary-release

aarch64-binary-release:
	CARGO_TARGET_DIR=target/target.aarch64 cross build --target=aarch64-unknown-linux-musl --release --all-targets --features vendored-openssl,jemalloc

aarch64-binary-debug:
	CARGO_TARGET_DIR=target/target.aarch64 cross build --target=aarch64-unknown-linux-musl --all-targets --features vendored-openssl

# NB. Not building the non-x86_64 debs in CI; as of 2025-02-06 they produce "error: linking with `cc` failed: exit status: 1"
# (this happens outside CI too)
#
ci-release: x86_64-binary-release aarch64-binary-release armv7-binary-release x86_64-deb
	rm -rf target/dist
	for arch in x86_64 aarch64 armv7; do \
		mkdir -p target/dist/$$arch; \
		cp -a target/target.$$arch/$$arch-unknown-linux-musl*/release/syndicate-macaroon target/dist/$$arch; \
		cp -a target/target.$$arch/$$arch-unknown-linux-musl*/release/syndicate-server target/dist/$$arch; \
	done
	cp -a target/target.x86_64/x86_64-unknown-linux-musl*/debian/*.deb target/dist/x86_64

#####################################################################################
# Debian packages via cargo-deb:
#
#   cargo install cargo-deb

x86_64-deb:
	CARGO_TARGET_DIR=target/target.x86_64 cargo deb --target=x86_64-unknown-linux-musl -p syndicate-server
	CARGO_TARGET_DIR=target/target.x86_64 cargo deb --target=x86_64-unknown-linux-musl -p syndicate-tools

armv7-deb:
	CARGO_TARGET_DIR=target/target.armv7 cargo deb --target=armv7-unknown-linux-musleabihf -p syndicate-server
	CARGO_TARGET_DIR=target/target.armv7 cargo deb --target=armv7-unknown-linux-musleabihf -p syndicate-tools

aarch64-deb:
	CARGO_TARGET_DIR=target/target.aarch64 cargo deb --target=aarch64-unknown-linux-musl -p syndicate-server
	CARGO_TARGET_DIR=target/target.aarch64 cargo deb --target=aarch64-unknown-linux-musl -p syndicate-tools
