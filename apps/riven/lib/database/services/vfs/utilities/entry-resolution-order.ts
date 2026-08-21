/**
 * Deterministic tiebreak for resolving a VFS path to a single entry.
 *
 * A single VFS path can match several rows: re-scraping an item inserts a fresh
 * `MediaItem` (and its own `FileSystemEntry`) rather than upserting, so one
 * episode/movie can accumulate many entries pointing at the same path but at
 * *different* releases.
 *
 * Without an explicit order, `findOne`/`findOneOrFail` emit `LIMIT 1` with no
 * `ORDER BY` and Postgres is free to return any matching row. That made `getattr`
 * and `open` resolve to different releases for the same path, so the size reported
 * by `stat` could describe one file while `read` served the bytes of another —
 * which is enough to crash a media server mid-playback.
 *
 * Every VFS resolution site must use this same order so `stat`, `open` and `read`
 * always agree on which entry a path refers to. Newest wins, with `id` as a final
 * tiebreak for rows sharing a `createdAt`.
 */
export const ENTRY_RESOLUTION_ORDER = {
  createdAt: "desc",
  id: "desc",
} as const;
