import { FileSystemEntry } from "@repo/util-plugin-sdk/dto/entities";

import path from "node:path";

import { ENTRY_RESOLUTION_ORDER } from "./entry-resolution-order.ts";

import type { PathInfo } from "../schemas/path-info.schema.ts";
import type { EntityManager } from "@mikro-orm/core";

const extractPart = (
  entry: FileSystemEntry,
  tvdbId?: string,
  season?: number,
) => {
  if (!entry.path) {
    return;
  }

  const { dir, base } = path.parse(entry.path);
  const [showName, seasonName] = dir.split(path.sep);

  return tvdbId ? (season ? base : seasonName) : showName;
};

export const getShowsDirectoryEntries = async (
  em: EntityManager,
  { tvdbId, season }: PathInfo,
): Promise<string[]> => {
  const entries = await em.find(
    FileSystemEntry,
    {
      type: {
        $in: ["media", "subtitle"],
      },
      mediaItem: {
        type: "episode",
        ...(tvdbId && { tvdbId }),
        ...(season && {
          season: {
            number: season,
          },
        }),
      },
    },
    // Newest first, so the first media entry seen for an episode is the one the
    // VFS resolves. See the collapsing logic below.
    { orderBy: ENTRY_RESOLUTION_ORDER },
  );

  const pathNames = new Set<string>();

  // An episode can own several media entries - re-scraping inserts a fresh
  // MediaItem with its own entry rather than upserting. Their VFS paths differ
  // only by extension, because the name is derived from the media item
  // (`getPrettyName()`) while the extension comes from the release's
  // `originalFilename`. Listing every entry therefore advertised one episode as
  // several files (`- s01e01.mkv` *and* `- s01e01.mp4`), even though every one of
  // those paths resolves to the same single entry that
  // `ENTRY_RESOLUTION_ORDER` picks - so the extra names were phantoms serving
  // another release's container.
  //
  // Collapse them here: entries arrive newest first, so the first media entry for
  // a given extension-less path wins, which is exactly the entry `getattr` and
  // `open` resolve. Subtitles are keyed by language rather than release and are
  // left alone.
  const claimedEpisodePaths = new Set<string>();

  for (const entry of entries) {
    const isFileListing = Boolean(tvdbId && season);

    if (isFileListing && entry.type === "media" && entry.path) {
      const { dir, name } = path.parse(entry.path);
      const episodePath = path.join(dir, name);

      if (claimedEpisodePaths.has(episodePath)) {
        continue;
      }

      claimedEpisodePaths.add(episodePath);
    }

    const part = extractPart(entry, tvdbId, season);

    if (part) {
      pathNames.add(part);
    }
  }

  return [...pathNames];
};
