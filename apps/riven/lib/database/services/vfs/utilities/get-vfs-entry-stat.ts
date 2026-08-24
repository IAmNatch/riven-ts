import {
  Episode,
  MediaEntry,
  Movie,
  Season,
  Show,
  SubtitleEntry,
} from "@repo/util-plugin-sdk/dto/entities";

import Fuse from "@zkochan/fuse-native";
import { DateTime } from "luxon";

import { FuseError } from "../../../../vfs/errors/fuse-error.ts";
import { PathInfo } from "../schemas/path-info.schema.ts";
import { PersistentDirectory } from "../schemas/persistent-directory.schema.ts";
import { getVfsMediaEntry } from "./get-vfs-media-entry.ts";
import { getEntry } from "./get-vfs-path-entry.ts";
import { stat } from "./stat.ts";

import type { EntityManager } from "@mikro-orm/core";

/**
 * Resolves the size `getattr` reports for a file path.
 *
 * For media files this deliberately goes through {@link getVfsMediaEntry} - the
 * exact resolver `open` uses - rather than reading the size off the collection of
 * the entity `getEntry` happened to return. A path can match several media items,
 * each owning its own entry for a different release, so resolving the size and the
 * bytes independently let `stat` describe one file while `read` served another.
 * Sharing one resolver keeps the reported size and the served bytes in agreement.
 */
async function getEntryFileSize(
  em: EntityManager,
  pathInfo: PathInfo,
  entry: Movie | Episode | SubtitleEntry,
) {
  if (entry instanceof Movie || entry instanceof Episode) {
    const mediaEntry = await getVfsMediaEntry(em, pathInfo);

    // `getEntry` resolves the media *item* from the ids in the path, so it
    // happily returns one for any filename sitting in the right directory. Only
    // the resolver can say whether this exact path is a file, so a miss here is
    // ENOENT rather than a zero-byte file - reporting success for a path that
    // cannot be opened leaves callers believing a deleted file still exists.
    if (!mediaEntry) {
      throw new FuseError(Fuse.ENOENT, "No media entry found");
    }

    return mediaEntry.fileSize;
  }

  return entry.fileSize;
}

export async function getVfsEntryStat(em: EntityManager, path: string) {
  switch (path) {
    case "/": {
      const oldestMediaEntryQuery = em.findOne(
        MediaEntry,
        { type: "media" },
        {
          orderBy: {
            createdAt: "asc nulls last",
          },
          fields: ["createdAt"],
        },
      );

      const mostRecentlyUpdatedMediaEntry = em.findOne(
        MediaEntry,
        { type: "media" },
        {
          orderBy: {
            updatedAt: "desc nulls last",
          },
          fields: ["updatedAt"],
        },
      );

      const [oldestEntry, mostRecentlyUpdatedEntry] = await Promise.all([
        oldestMediaEntryQuery,
        mostRecentlyUpdatedMediaEntry,
      ]);

      const fallbackDate = DateTime.utc().toJSDate();

      const entryStat = stat(
        {
          mtime:
            mostRecentlyUpdatedEntry?.updatedAt ??
            oldestEntry?.createdAt ??
            fallbackDate,
          atime:
            mostRecentlyUpdatedEntry?.updatedAt ??
            oldestEntry?.createdAt ??
            fallbackDate,
          ctime: oldestEntry?.createdAt ?? fallbackDate,
          mode: "dir",
        },
        PersistentDirectory.options.length,
      );

      return entryStat;
    }
    case "/shows": {
      const totalShowsQuery = em.count(Show, {
        seasons: {
          episodes: {
            filesystemEntries: {
              $some: {
                type: "media",
                mediaItem: {
                  type: "episode",
                },
              },
            },
          },
        },
      });

      const oldestShowQuery = em.findOne(
        MediaEntry,
        {
          type: "media",
          mediaItem: {
            type: "episode",
          },
        },
        {
          orderBy: {
            createdAt: "asc nulls last",
          },
          fields: ["createdAt"],
        },
      );

      const lastUpdatedShowQuery = em.findOne(
        MediaEntry,
        {
          type: "media",
          mediaItem: {
            type: "episode",
          },
        },
        {
          orderBy: {
            updatedAt: "desc nulls last",
          },
          fields: ["updatedAt"],
        },
      );

      const [totalShows, oldestShow, lastUpdatedShow] = await Promise.all([
        totalShowsQuery,
        oldestShowQuery,
        lastUpdatedShowQuery,
      ]);

      const fallbackDate = DateTime.utc().toJSDate();

      const entryStat = stat(
        {
          mtime:
            lastUpdatedShow?.updatedAt ?? oldestShow?.createdAt ?? fallbackDate,
          atime:
            lastUpdatedShow?.updatedAt ?? oldestShow?.createdAt ?? fallbackDate,
          ctime: oldestShow?.createdAt ?? fallbackDate,
          mode: "dir",
        },
        totalShows,
      );

      return entryStat;
    }
    case "/movies": {
      const totalMoviesQuery = em.count(Movie, {
        filesystemEntries: {
          $some: {
            type: "media",
            mediaItem: {
              type: "movie",
            },
          },
        },
      });

      const lastUpdatedMovieQuery = em.findOne(
        MediaEntry,
        {
          type: "media",
          mediaItem: {
            type: "movie",
          },
        },
        {
          orderBy: {
            updatedAt: "desc nulls last",
          },
          fields: ["updatedAt"],
        },
      );

      const oldestMovieQuery = em.findOne(
        MediaEntry,
        {
          type: "media",
          mediaItem: {
            type: "movie",
          },
        },
        {
          orderBy: {
            createdAt: "asc nulls last",
          },
          fields: ["createdAt"],
        },
      );

      const [totalMovies, lastUpdatedMovie, oldestMovie] = await Promise.all([
        totalMoviesQuery,
        lastUpdatedMovieQuery,
        oldestMovieQuery,
      ]);

      const fallbackDate = DateTime.utc().toJSDate();

      const entryStat = stat(
        {
          mtime:
            lastUpdatedMovie?.updatedAt ??
            oldestMovie?.createdAt ??
            fallbackDate,
          atime:
            lastUpdatedMovie?.updatedAt ??
            oldestMovie?.createdAt ??
            fallbackDate,
          ctime: oldestMovie?.createdAt ?? fallbackDate,
          mode: "dir",
        },
        totalMovies,
      );

      return entryStat;
    }
  }

  const pathInfo = PathInfo.safeParse(path);

  if (!pathInfo.success) {
    throw new FuseError(Fuse.ENOENT, "Unable to parse path info");
  }

  const entry = await getEntry(em, pathInfo.data);

  if (!entry) {
    throw new FuseError(Fuse.ENOENT, "No VFS entry found");
  }

  const subDirectoryCount =
    pathInfo.data.pathType === "show-seasons"
      ? await em.count(Season, {
          show: {
            tvdbId: String(pathInfo.data.tvdbId),
          },
          episodes: {
            filesystemEntries: {
              $some: {
                type: "media",
              },
            },
          },
        })
      : 0;

  const isFileEntry =
    entry instanceof Movie ||
    entry instanceof Episode ||
    entry instanceof SubtitleEntry;

  const attrs = stat(
    {
      ctime: entry.createdAt,
      atime: entry.updatedAt ?? entry.createdAt,
      mtime: entry.updatedAt ?? entry.createdAt,
      ...(isFileEntry && pathInfo.data.isFile
        ? {
            size: await getEntryFileSize(em, pathInfo.data, entry),
            mode: "file",
          }
        : { mode: "dir" }),
    },
    subDirectoryCount,
  );

  return attrs;
}
