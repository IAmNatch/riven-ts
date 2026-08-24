import { logger } from "../../utilities/logger/logger.ts";
import { discardFdStream } from "./discard-fd-stream.ts";
import { getVfsOperationContext } from "./vfs-operation-context.ts";

/**
 * Handles a stream seek when reusing an existing file descriptor.
 *
 * Closes the existing stream and opens a new one from the new position.
 *
 * @param from The previous stream position
 * @param to The new stream position
 */
export function seek(from: number, to: number) {
  const {
    fd,
    context: { responsePromise, seekController },
  } = getVfsOperationContext("read");

  logger.debug(
    `Seeking to new start position for fd ${fd.toString()} (${from.toString()} -> ${to.toString()})`,
  );

  discardFdStream(fd, responsePromise);

  seekController.abort();
}
