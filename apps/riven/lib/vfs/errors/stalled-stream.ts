import { FuseError } from "./fuse-error.ts";

/**
 * Thrown when the upstream response body stops delivering bytes.
 *
 * Distinct from a generic {@link FuseError} because a stalled body is
 * *recoverable*: the bytes are still there, the connection carrying them is
 * not. Callers holding the file descriptor's cached stream can drop it and
 * reconnect from the chunk that stalled, rather than surfacing the failure to
 * the player.
 */
export class StalledStreamError extends FuseError {
  public override name = "StalledStreamError";
}
