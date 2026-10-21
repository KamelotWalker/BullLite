# @bulllite/memory (Phase 0 spike)

In-memory `IQueueBackend` for `bullmq@6.4.0`, used via `withBackend(createMemoryBackend)`.
`connection: { namespace }` selects the isolation domain. Implemented: Queue.add -> Worker (single attempt) -> completed/failed,
`getJob`/`getState`, lock renewal, stalled recovery. Everything else rejects with `UnsupportedError` naming the method.
`scratch/` holds the call-recording tool used to measure which backend members the path needs (`node scratch/measure.ts`).
