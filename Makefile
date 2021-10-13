# Use cargo release to manage publication and versions etc.
#
#   cargo install cargo-release

all:
	cargo build --all-targets

test:
	cargo test

test-all:
	cargo test --all-targets

# Try
#
#   make release-minor
#
# to check things, and
#
#   make release-minor RELEASE_DRY_RUN=
#
# to do things for real.

RELEASE_DRY_RUN=--dry-run
release-%:
	PUBLISH_GRACE_SLEEP=15 cargo release \
		$(RELEASE_DRY_RUN) \
		-vv --no-dev-version --exclude-unchanged \
		$*

pull-protocols:
	git subtree pull -P syndicate/protocols \
		-m 'Merge latest changes from the syndicate-protocols repository' \
		git@git.syndicate-lang.org:syndicate-lang/syndicate-protocols \
		main

static: static-x86_64

static-%:
	cross build --target $*-unknown-linux-musl --features vendored-openssl

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

arm-binary: arm-binary-release

arm-binary-release:
	cross build --target=armv7-unknown-linux-musleabihf --release --all-targets --features vendored-openssl

arm-binary-debug:
	cross build --target=armv7-unknown-linux-musleabihf --all-targets --features vendored-openssl

# Hack to workaround https://github.com/rust-embedded/cross/issues/598
HACK_WORKAROUND_ISSUE_598=CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_RUSTFLAGS="-C link-arg=/usr/local/aarch64-linux-musl/lib/libc.a"

aarch64-binary: aarch64-binary-release

aarch64-binary-release:
	$(HACK_WORKAROUND_ISSUE_598) \
		cross build --target=aarch64-unknown-linux-musl --release --all-targets --features vendored-openssl

aarch64-binary-debug:
	$(HACK_WORKAROUND_ISSUE_598) \
		cross build --target=aarch64-unknown-linux-musl --all-targets --features vendored-openssl
