# inotify and huge-file ingestion spike

Status: verified

Date: 2026-07-17

## Question

Can Linux filesystem monitoring detect the required file lifecycle while
bounded suffix reads remain independent of transcript size and queue overflow is
explicitly observable?

## Environment

- `fs.inotify.max_user_watches = 161336`
- `fs.inotify.max_user_instances = 128`
- `fs.inotify.max_queued_events = 16384`

## Rust tests and result

`tools/spikes/tests/inotify.rs` creates the watch before performing partial
write, append, close, rotation, replacement, and truncation. The normal run
observed targeted create/modify/close/move events. The explicitly enabled
overflow test queued 25,000 file creations before draining and observed exactly
one `IN_Q_OVERFLOW`. Its ledger record is
`8ed4dfe6afb1b9bd00946c46eb63ae4c`.

`tools/spikes/tests/suffix.rs` creates an 8 GiB sparse transcript, saves its end
offset, appends one 39-byte record, and reads from that cursor with a 64 KiB
budget. Only the appended record is returned. Separate tests prove that a byte
budget stops the read and a cursor past the new end reports truncation rather
than restarting from byte zero. The ledger record is
`d4bf885fa5ebc0985f1e24c7b26c52c0`.

An external syscall trace corroborated the sparse-file behavior: the reader
seeked directly to byte 8,589,934,592 and issued one file read returning only the
39 bytes used by that earlier fixture; the sparse file occupied eight disk
blocks. This trace is corroboration only—the committed executable probes are
Rust.

## Decision

- Use one deduplicated watch service and treat inotify as an invalidation signal,
  not as a complete durable event log.
- Persist device/inode identity plus byte offset and parser version.
- On `IN_Q_OVERFLOW`, replacement, rotation, or truncation, mark only the
  affected scope uncertain and schedule bounded reconciliation.
- Never silently restart a huge transcript at byte zero. A cursor invalidation
  requires an adapter-specific safe boundary and a configured byte/time budget.
- Expose watch-count and queue-overflow health with host-tuning guidance.
