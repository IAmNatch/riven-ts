import { MediaEntry } from "@repo/util-plugin-sdk/dto/entities";

import nodePath from "node:path";

import { ENTRY_RESOLUTION_ORDER } from "./entry-resolution-order.ts";

import type { PathInfo } from "../schemas/path-info.schema.ts";
import type { EntityManager, FindOneOptions } from "@mikro-orm/core";

export async function getVfsMediaEntry<
  Hint extends string = never,
  Fields extends string = never,
  Excludes extends string = never,
>(
  em: EntityManager,
  pathInfo: PathInfo,
  options?: FindOneOptions<MediaEntry, Hint, Fields, Excludes>,
) {
  const entry = await resolveByMediaItem(em, pathInfo, options);

  if (!entry) {
    return null;
  }

  // The query above matches on the *media item*, not on the path, because one
  // item can own many entries and only their extension distinguishes the paths
  // they generate (the name comes from `getPrettyName()`, the extension from
  // that release's `originalFilename`). Only the entry `ENTRY_RESOLUTION_ORDER`
  // picks is a real file - the other extensions are phantoms that would resolve
  // here to this same entry and serve its bytes under the wrong container.
  //
  // Directory listings already advertise only the winner, but a caller that
  // remembers an old path can still ask for it directly, so answering that
  // lookup keeps the phantom alive: a media server re-checks the paths it knows
  // with `stat` rather than re-reading the directory, and a successful `getattr`
  // tells it the file is still there.
  const entryPath = readEntryPath(entry);

  if (
    pathInfo.isFile &&
    (!entryPath || nodePath.basename(entryPath) !== pathInfo.base)
  ) {
    return null;
  }

  return entry;
}

/**
 * `options` can narrow the selected fields, so `path` is not statically known to
 * be present on the resolved entry. Read it defensively rather than widening the
 * caller-facing generics.
 */
function readEntryPath(entry: object): string | undefined {
  const value = (entry as { path?: unknown }).path;

  return typeof value === "string" ? value : undefined;
}

function resolveByMediaItem<
  Hint extends string = never,
  Fields extends string = never,
  Excludes extends string = never,
>(
  em: EntityManager,
  pathInfo: PathInfo,
  options?: FindOneOptions<MediaEntry, Hint, Fields, Excludes>,
) {
  if (pathInfo.tmdbId) {
    return em.findOne(
      MediaEntry,
      {
        mediaItem: {
          type: "movie",
          tmdbId: pathInfo.tmdbId,
        },
      },
      { ...options, orderBy: ENTRY_RESOLUTION_ORDER },
    );
  }

  if (pathInfo.tvdbId && pathInfo.season && pathInfo.episode) {
    return em.findOne(
      MediaEntry,
      {
        mediaItem: {
          type: "episode",
          tvdbId: pathInfo.tvdbId,
          number: pathInfo.episode,
          season: {
            number: pathInfo.season,
          },
        },
      },
      { ...options, orderBy: ENTRY_RESOLUTION_ORDER },
    );
  }

  return Promise.resolve(null);
}
