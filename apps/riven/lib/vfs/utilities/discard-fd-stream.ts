import {
  fdToCurrentStreamPositionMap,
  fdToResponsePromiseMap,
} from "./file-handle-map.ts";

import type { Dispatcher } from "undici";

/**
 * Drops the stream cached against a file descriptor so the next read opens a
 * fresh one.
 *
 * The body is drained without blocking - a slow or large stream must not delay
 * the reconnect - and a failure to drain is ignored, since the stream is being
 * abandoned either way.
 */
export function discardFdStream(
  fd: number,
  stream:
    | Dispatcher.ResponseData
    | Promise<Dispatcher.ResponseData>
    | undefined,
) {
  void Promise.resolve(stream)
    .then(async (resolved) => resolved?.body.dump())
    .catch(() => undefined);

  fdToResponsePromiseMap.delete(fd);
  fdToCurrentStreamPositionMap.delete(fd);
}
