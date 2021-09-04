# What is an Actor?

A [Syndicated Actor][Actor] ([Garnock-Jones 2017](#GarnockJones2017))
is a collection of stateful [Entities][Entity], organised in a tree of
[Facets][Facet], with each facet representing a
[(sub)conversation](https://syndicate-lang.org/about/#conversational-concurrency-1)
that the Actor is engaged in. Each entity belongs to exactly one
facet; each facet has exactly one parent and zero or more children;
each actor has exactly one associated root facet. When a facet is its
actor's root facet, its parent is the actor itself; otherwise, its
parent is always another facet.

In the taxonomy of De Koster *et al.* ([2016](#DeKoster2016)), the
Syndicated Actor model is a *Communicating Event-Loop* actor model,
similar to that offered by the E programming language
([Wikipedia](https://en.wikipedia.org/wiki/E_(programming_language));
[erights.org](http://erights.org/)).

 - [Actor], [ActorRef], [Facet], [FacetRef], [ActorState], [Mailbox],
   [Activation]

**References.**

 - De Koster, Joeri, Tom Van Cutsem, and Wolfgang De Meuter. <a
   name="DeKoster2016"
   href="http://soft.vub.ac.be/Publications/2016/vub-soft-tr-16-11.pdf">“43
   Years of Actors: A Taxonomy of Actor Models and Their Key
   Properties.”</a> In Proc. AGERE, 31–40. Amsterdam, The
   Netherlands, 2016. [DOI](https://doi.org/10.1145/3001886.3001890).
   [PDF](http://soft.vub.ac.be/Publications/2016/vub-soft-tr-16-11.pdf).

 - Garnock-Jones, Tony. <a name="GarnockJones2017"
   href="https://syndicate-lang.org/tonyg-dissertation/html/">“Conversational
   Concurrency.”</a> PhD, Northeastern University, 2017.
   [Permalink](http://hdl.handle.net/2047/D20261862).
   [PDF@Northeastern](https://repository.library.northeastern.edu/files/neu:cj82qs441/fulltext.pdf).
   [PDF@syndicate-lang.org](https://syndicate-lang.org/papers/conversational-concurrency-201712310922.pdf).
   [HTML](https://syndicate-lang.org/tonyg-dissertation/html/).
