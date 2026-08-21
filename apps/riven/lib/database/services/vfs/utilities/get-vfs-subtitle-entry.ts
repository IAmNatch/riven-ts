import { SubtitleEntry } from "@repo/util-plugin-sdk/dto/entities";

import { ENTRY_RESOLUTION_ORDER } from "./entry-resolution-order.ts";

import type { PathInfo } from "../schemas/path-info.schema.ts";
import type { EntityManager } from "@mikro-orm/core";

export async function getVfsSubtitleEntry(
  em: EntityManager,
  pathInfo: PathInfo,
) {
  if (pathInfo.tmdbId) {
    return em.findOne(
      SubtitleEntry,
      {
        mediaItem: { tmdbId: pathInfo.tmdbId },
        path: { $like: `%${pathInfo.base}` },
      },
      { orderBy: ENTRY_RESOLUTION_ORDER },
    );
  }

  if (pathInfo.tvdbId && pathInfo.season && pathInfo.episode) {
    return em.findOne(
      SubtitleEntry,
      {
        mediaItem: {
          type: "episode",
          number: pathInfo.episode,
          season: { number: pathInfo.season },
          tvdbId: pathInfo.tvdbId,
        },
        path: { $like: `%${pathInfo.base}` },
      },
      { orderBy: ENTRY_RESOLUTION_ORDER },
    );
  }

  return null;
}
