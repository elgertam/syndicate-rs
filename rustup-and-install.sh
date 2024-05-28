#!/bin/sh
set -e
rustup update
cargo +nightly install --path `pwd`/syndicate-server
