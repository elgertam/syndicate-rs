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
