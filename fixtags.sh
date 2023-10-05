#!/bin/sh

buildtag() {
    name=$(grep '^name' "$1" | head -1 | sed -e 's:^.*"\([^"]*\)":\1:')
    version=$(grep '^version' "$1" | head -1 | sed -e 's:^.*"\([^"]*\)":\1:')
    echo "$name-v$version"
}

git tag "$(buildtag syndicate/Cargo.toml)"
git tag "$(buildtag syndicate-macros/Cargo.toml)"
git tag "$(buildtag syndicate-server/Cargo.toml)"
git tag "$(buildtag syndicate-tools/Cargo.toml)"
