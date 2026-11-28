import { Buffer } from "node:buffer";
import { randomBytes } from "node:crypto";
import { PassThrough } from "node:stream";
import { setTimeout as sleep } from "node:timers/promises";
import { expect, vi } from "vitest";

import { it } from "../../../__tests__/test-context.ts";
import { config } from "../../config.ts";
import { chunkCache } from "../chunk-cache.ts";
import { calculateChunkRange } from "../chunks/calculate-chunk-range.ts";
import { calculateFileChunks } from "../chunks/calculate-file-chunks.ts";
import {
  fdToCurrentStreamPositionMap,
  fdToResponsePromiseMap,
  fileNameToFileChunkCalculationsMap,
} from "../file-handle-map.ts";
import { withVfsOperationContext } from "../vfs-operation-context.ts";

import type { MediaFileHandleMetadata } from "../file-handle-map.ts";
import type { createStreamRequest } from "../requests/create-stream-request.ts";
import type { VfsOperationContext } from "../vfs-operation-context.ts";
import type { Dispatcher } from "undici";

const streams: PassThrough[] = [];

vi.mock(import("../requests/create-stream-request.ts"), async () => {
  const { getVfsOperationContext } =
    await import("../vfs-operation-context.ts");
  const fileHandleMap = await import("../file-handle-map.ts");

  return {
    createStreamRequest: vi.fn<typeof createStreamRequest>(
      (_url, [requestStart]) => {
        const { fd } = getVfsOperationContext("read");
        const body = new PassThrough({ highWaterMark: config.chunkSize });
        const promise = Promise.resolve({
          body,
        } as unknown as Dispatcher.ResponseData);

        streams.push(body);

        fileHandleMap.fdToCurrentStreamPositionMap.set(fd, requestStart);
        fileHandleMap.fdToResponsePromiseMap.set(fd, promise);

        return promise;
      },
    ),
  };
});

const fd = 0;
const fileName = "episode.mkv";
const fileSize = 1024 ** 3;

const fileHandleMetadata: MediaFileHandleMetadata = {
  type: "media",
  fileSize,
  filePath: `/files/${fileName}`,
  fileBaseName: fileName,
  originalFileName: fileName,
  url: `http://example.com/files/${fileName}`,
};

const chunkAt = (position: number) => {
  const { chunks } = calculateChunkRange({
    chunkSize: config.chunkSize,
    fileSize,
    requestRange: [position, position + config.blockSize - 1],
    fileName,
  });

  return chunks;
};

function bodyRead(position: number) {
  return withVfsOperationContext(
    {
      operationName: "read",
      path: fileHandleMetadata.filePath,
      fd,
      position,
      length: config.blockSize,
      buffer: Buffer.alloc(config.blockSize),
      context: {
        fileHandleMetadata,
        previousReadPosition: position,
        get currentStreamPosition() {
          return fdToCurrentStreamPositionMap.get(fd);
        },
        responsePromise: fdToResponsePromiseMap.get(fd),
        seekController: new AbortController(),
      },
    } satisfies VfsOperationContext,
    async () => {
      const { performBodyRead } = await import("./perform-body-read.ts");

      return performBodyRead(chunkAt(position));
    },
  );
}

it.beforeEach(() => {
  fdToResponsePromiseMap.clear();
  fdToCurrentStreamPositionMap.clear();
  chunkCache.clear();
  streams.length = 0;

  fileNameToFileChunkCalculationsMap.set(
    fileName,
    calculateFileChunks(fileName, fileSize),
  );
});

it("keeps one stream per fd when concurrent reads stall on the same chunk", async () => {
  const originalTimeout = config.chunkTimeoutSeconds;

  Object.assign(config, { chunkTimeoutSeconds: 0.2 });

  try {
    const [firstChunk] = chunkAt(config.chunkSize * 40);
    const [secondChunk] = chunkAt(config.chunkSize * 41);

    const firstChunkData = randomBytes(config.chunkSize);
    const secondChunkData = randomBytes(config.chunkSize);

    // A stream that never delivers a byte: the upstream stalled.
    const stalledStream = new PassThrough();

    fdToCurrentStreamPositionMap.set(fd, firstChunk.range[0]);
    fdToResponsePromiseMap.set(
      fd,
      Promise.resolve({
        body: stalledStream,
      } as unknown as Dispatcher.ResponseData),
    );

    // Two kernel reads landing in the same chunk, as FUSE issues them.
    const readA = bodyRead(firstChunk.range[0]);
    const readB = bodyRead(firstChunk.range[0] + config.blockSize);

    await vi.waitFor(() => {
      expect(streams.length).toBeGreaterThan(0);
    });

    // Give a second reconnect the chance to happen before data arrives.
    await sleep(50);

    // Every replacement stream serves the file from the stalled chunk onwards.
    // The first one answers first, as one connection always does.
    const respond = (stream: PassThrough) =>
      stream.write(Buffer.concat([firstChunkData, secondChunkData]));

    const [firstStream, ...laterStreams] = streams;

    expect.assert(firstStream);

    respond(firstStream);

    const [dataA, dataB] = await Promise.all([readA, readB]);

    for (const stream of laterStreams) {
      respond(stream);
    }

    expect(dataA.equals(firstChunkData)).toBe(true);
    expect(dataB.equals(firstChunkData)).toBe(true);

    // The next sequential read must get the next chunk's bytes, not a
    // repeat of the chunk that stalled.
    const dataC = await bodyRead(secondChunk.range[0]);

    expect(dataC.equals(secondChunkData)).toBe(true);
    expect(streams).toHaveLength(1);
  } finally {
    Object.assign(config, { chunkTimeoutSeconds: originalTimeout });
  }
});
