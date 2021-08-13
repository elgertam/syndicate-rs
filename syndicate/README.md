This crate implements the
[Syndicated Actor model](https://syndicate-lang.org/about/) for Rust,
including

 - intra-process communication (the [actor] module),
 - point-to-point links between actor spaces (the [relay] module),
 - and Dataspace objects (the [dataspace] module) for replicating
   state and messages among interested parties.
