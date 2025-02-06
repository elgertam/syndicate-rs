#!/bin/sh
#
# You need to have already logged in:
#
#     docker login git.syndicate-lang.org
#
# Use a token with read/write access to package scope.

set -e
docker build -t git.syndicate-lang.org/syndicate-lang/rust-builder .
docker push git.syndicate-lang.org/syndicate-lang/rust-builder
