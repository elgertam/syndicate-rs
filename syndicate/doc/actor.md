The [actor][crate::actor] module is the core of the Syndicated Actor model implementation.

Central features:

 - struct [`Activation`], the API for programming a Syndicated Actor
   object
 - trait [`Entity`], the core protocol that must be implemented by
   every object
 - struct [`Facet`], a node in the tree of nested conversations that
   an Actor is participating in
 - type [`AnyValue`], the type of messages and assertions that can be
   exchanged among distributed objects, including via
   [dataspace][crate::dataspace]
 - struct [`Ref<M>`], a reference to a local or remote object
 - struct [`Cap`], a specialization of `Ref<M>` for
   messages/assertions of type `AnyValue`
 - struct [`Guard`], an adapter for converting an underlying
   [`Ref<M>`] to a [`Cap`]
