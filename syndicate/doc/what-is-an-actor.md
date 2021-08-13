# What is an Actor?

A [Syndicated Actor][Actor] is a collection of stateful
[Entities][Entity]. In the taxonomy of De Koster *et al.*
([2016](#DeKoster2016)), the Syndicated Actor model is a
*Communicating Event-Loop* actor model, similar to that offered by the
E programming language
([Wikipedia](https://en.wikipedia.org/wiki/E_(programming_language));
[erights.org](http://erights.org/)).

**Note.** In the full Syndicated Actor model, entities are arranged in a tree of
*facets*; the current Rust implementation does not yet include support
for facets.

 - Actor, ActorRef, ActorState, Mailbox

**References.**

 - De Koster, Joeri, Tom Van Cutsem, and Wolfgang De Meuter. <a
   name="DeKoster2016">“43 Years of Actors: A Taxonomy of Actor Models
   and Their Key Properties.”</a> In Proc. AGERE, 31–40. Amsterdam,
   The Netherlands, 2016.
   [DOI](https://doi.org/10.1145/3001886.3001890).
   [PDF](http://soft.vub.ac.be/Publications/2016/vub-soft-tr-16-11.pdf).
