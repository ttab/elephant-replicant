# Edits in the target win, and a conflict is permanent for the document

Every update of a document the service has written before carries the target
version it last wrote as `IfMatch`. If the target's current version is
another one, somebody or something wrote to the document in the target, the
update is refused with `failed_precondition`, and the worker logs the
conflict and moves on. The remembered version is not updated, so every later
source event for that document conflicts too, until the document is deleted
in the source or an operator deletes it in the target and clears its
`document` row.

The alternative, overwriting, would make stage unusable for the thing it is
for: a tester who changes a document to try something would have it replaced
by production on the next event. The other alternative, adopting the target's
version as the new base and continuing to apply source updates on top of it,
would interleave production and stage edits in one document with no record
of which is which.

## Consequences

* A conflict is logged at info level with the document UUID and nothing else
  happens; there is no metric and no retry.
* `SendDocument` with `force` is the intended operator tool for re-sending a
  conflicted document and is not implemented. Until it is, the recovery is
  manual: delete in the target, delete the `document` row, wait for the next
  event.
