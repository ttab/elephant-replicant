# Catching up replicates the present, not the history

While a target is behind the source's log, its worker reads the compacted
eventlog, which yields one event per document in the window, and replicates
each document's current version with the status heads that point at it, not
the sequence of versions and statuses the document went through. Only once
the follower reaches the end of the log does it switch to replaying events
one by one.

The alternative, replaying the plain log from the stored position, would
produce a faithful history at the cost of one `Get` and one `Update` per
event over however long the target was stopped. In October 2026 the
production target was tailing at around 18,000 events a day, at position
19.9 million. A stage environment is kept
populated so that people can work against realistic current content, and the
history of how it got there is not what it is for.

## Consequences

* After a long stop, intermediate versions, statuses on older versions and
  ACL changes that were later undone never reach the target.
* A document the target already has gets a new version carrying the current
  content; its ACL is not touched in catch-up, only on a new document.
* `eventlog_follower_position{state="compact"}` is how to tell a target is in
  this mode; it should be transient.
