# Flow control

 - struct [`Account`]
 - struct [`LoanedItem`]

In order to handle high-speed scenarios where actors can become
overloaded by incoming events, this crate takes a
([possibly novel](https://syndicate-lang.org/journal/2021/09/02/internal-flow-control))
approach to *internal flow control* that is a variant of "credit-based
flow control" (as widely used in telephony systems).

The idea is to associate each individually-identifiable activity in an
actor system with an [*account*][Account] that records how much
outstanding work has to be done, system-wide, to fully complete
processing of that activity.

Each Actor scheduling new activities in response to some external
source (e.g while reading from a network socket in a
[linked task][Activation::linked_task]) calls
[`Account::ensure_clear_funds`] on its associated [`Account`]. This
will suspend the actor until enough of the account's "debt" has been
"cleared". (In the case of reading from a socket, this causes the TCP
socket's read buffers to fill up and the TCP window to close, which
throttles upstream senders.)

Every time any actor sends an event to any other actor, a
[`LoanedItem`] is constructed which "borrows" enough credit from some
nominated [`Account`] to cover the event. Crucially, when an actor is
*responding* to an event by *sending* more events, the account chosen
is *the one that the triggering event was charged to*. This lets the
server automatically account for fan-out of events.[^corollary]
Finally, once a `LoanedItem` is completely processed (i.e. when it is
[dropped][LoanedItem::drop]), its cost is "repaid" to its associated
account.

## Does it work?

Anecdotally, this approach appears to work well. Experimenting using
`syndicate-server` with producers sending as quickly as they can,
producers are throttled by the server, and the server seems stable
even though its consumers are not able to keep up with the unthrottled
send rate of each producer.

## Example

Imagine an actor *A* receiving publications from a TCP/IP socket. If
it ever "owes" more than, say, 5 units of cost on its account, it
stops reading from its socket until its debt decreases. Each message
it forwards on to another actor costs it 1 unit. Say a given incoming
message *M* is routed to a dataspace actor *D* (thereby charging *A*'s
account 1 unit), where it results in nine outbound events *M′* to peer
actors *O1*···*O9*.

Then, when *D* receives *M*, 1 unit is repaid to *A*'s account. When
*D* sends *M′* on to each of *O1*···*O9*, 1 unit is charged to *A*'s
account, resulting in a total of 9 units charged. At this point in
time, *A*'s account has had net +1−1+9 = 9 units withdrawn from it as
a result of *M*'s processing.

Imagine now that all of *O1*···*O9* are busy with other work. Then,
next time around *A*'s main loop, *A* notices that its outstanding
debt is higher than its configured threshold, and stops reading from
its socket. As each of *O1*···*O9* eventually gets around to
processing its copy of *M′*, it repays the associated 1 unit to *A*'s
account.[^may-result-in-further-costs] Eventually, *A*'s account drops
below the threshold, *A* is woken up, and it resumes reading from its
socket.

[^corollary]: A corollary to this is that, for each event internal to
    the system, you can potentially identify the "ultimate cause" of
    the event: namely, the actor owning the associated account.

[^may-result-in-further-costs]: Of course, if *O1*, say, sends *more*
    events internally as a result of receiving *M′*, more units will
    be charged to *A*'s account!

