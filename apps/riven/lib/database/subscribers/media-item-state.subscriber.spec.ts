import { BlacklistedStream, Season } from "@repo/util-plugin-sdk/dto/entities";

import { DateTime } from "luxon";
import { describe, expect, vi } from "vitest";

import { it } from "../../__tests__/test-context.ts";
import { settings } from "../../utilities/settings.ts";

import type { EpisodeFactory } from "../factories/episode.factory.ts";
import type { MediaEntryFactory } from "../factories/media-entry.factory.ts";
import type { SeasonFactory } from "../factories/season.factory.ts";
import type { ShowFactory } from "../factories/show.factory.ts";
import type { EntityManager } from "@mikro-orm/core";

const indexedAt = DateTime.utc().toJSDate();
const pastReleaseDate = DateTime.utc().minus({ years: 1 }).toJSDate();
const futureReleaseDate = DateTime.utc().plus({ years: 1 }).toJSDate();

describe("beforeUpsert", () => {
  it("moves an unreleased item to indexed once its release date has passed", async ({
    em,
    factories: { movieFactory },
  }) => {
    const movie = await movieFactory.createOne({
      releaseDate: futureReleaseDate,
      state: "unreleased",
      indexedAt,
    });

    movie.releaseDate = pastReleaseDate;

    await em.upsert(movie);

    expect(movie.state).toBe("indexed");
  });

  it("moves a released item back to unreleased if its release date moves into the future", async ({
    em,
    factories: { movieFactory },
  }) => {
    const movie = await movieFactory.createOne({
      releaseDate: pastReleaseDate,
      indexedAt,
    });

    movie.releaseDate = futureReleaseDate;

    await em.upsert(movie);

    expect(movie.state).toBe("unreleased");
  });

  it("leaves an unreleased item untouched if it is still unreleased", async ({
    em,
    factories: { movieFactory },
  }) => {
    const movie = await movieFactory.createOne({
      releaseDate: futureReleaseDate,
      state: "unreleased",
      indexedAt,
    });

    await em.upsert(movie);

    expect(movie.state).toBe("unreleased");
  });

  it("leaves a released item's state untouched", async ({
    em,
    factories: { movieFactory, streamFactory },
  }) => {
    const movie = await movieFactory.createOne({
      releaseDate: pastReleaseDate,
      indexedAt,
    });

    const stream = await streamFactory.createOne();

    movie.streams.add(stream);

    await em.flush();

    expect(movie.state).toBe("scraped");

    await em.upsert(movie);

    expect(movie.state).toBe("scraped");
  });
});

describe("onFlush", () => {
  describe("leaf media items", () => {
    it("sets an unreleased movie to unreleased", async ({
      factories: { movieFactory },
    }) => {
      const movie = await movieFactory.createOne({
        releaseDate: futureReleaseDate,
        indexedAt,
      });

      expect(movie.state).toBe("unreleased");
    });

    it("does not override a paused (fixed) movie's state", async ({
      em,
      factories: { movieFactory },
    }) => {
      const movie = await movieFactory.createOne({
        releaseDate: pastReleaseDate,
        state: "paused",
        indexedAt,
      });

      movie.title = "A brand new title";

      await em.flush();

      expect(movie.state).toBe("paused");
    });

    it("sets a released movie with too many failed scrape attempts to failed", async ({
      factories: { movieFactory },
    }) => {
      const movie = await movieFactory.createOne({
        releaseDate: pastReleaseDate,
        failedScrapeAttempts: 10,
        indexedAt,
      });

      expect(movie.state).toBe("failed");
    });

    it("sets a released movie with a media filesystem entry to completed", async ({
      em,
      factories: { movieFactory, mediaEntryFactory },
    }) => {
      const movie = await movieFactory.createOne({
        releaseDate: pastReleaseDate,
        indexedAt,
      });

      const mediaEntry = mediaEntryFactory.makeOne({ mediaItem: movie });

      movie.filesystemEntries.add(mediaEntry);

      await em.flush();

      expect(movie.state).toBe("completed");
    });

    it("sets a released movie with an available stream to scraped", async ({
      em,
      factories: { movieFactory, streamFactory },
    }) => {
      const movie = await movieFactory.createOne({
        releaseDate: pastReleaseDate,
        indexedAt,
      });

      const stream = await streamFactory.createOne();

      movie.streams.add(stream);

      await em.flush();

      expect(movie.state).toBe("scraped");
    });

    it("sets a released movie with no streams or filesystem entries to indexed", async ({
      factories: { movieFactory },
    }) => {
      const movie = await movieFactory.createOne({
        releaseDate: pastReleaseDate,
        indexedAt,
      });

      expect(movie.state).toBe("indexed");
    });

    it("does not treat a fully blacklisted stream as available", async ({
      em,
      factories: { movieFactory, streamFactory },
    }) => {
      const movie = await movieFactory.createOne({
        releaseDate: pastReleaseDate,
        indexedAt,
      });

      const stream = await streamFactory.createOne();

      movie.streams.add(stream);

      await em.flush();

      expect(movie.state).toBe("scraped");

      em.create(BlacklistedStream, {
        stream,
        mediaItem: movie,
        plugin: "test-plugin",
        provider: null,
      });

      movie.streams.remove(stream);

      await em.flush();

      expect(movie.state).toBe("indexed");
    });
  });

  describe("season and show aggregation from children", () => {
    it("does not override a paused (fixed) season's state, regardless of its episodes", async ({
      em,
      factories: { showFactory, seasonFactory, episodeFactory },
    }) => {
      const show = await showFactory.createOne({
        status: "ended",
        releaseDate: pastReleaseDate,
        indexedAt,
      });

      const season = await seasonFactory.createOne({
        show,
        itemRequest: show.itemRequest,
        state: "paused",
        releaseDate: pastReleaseDate,
        indexedAt,
      });

      await episodeFactory.createOne({
        season,
        show,
        itemRequest: show.itemRequest,
        number: 1,
        isSpecial: false,
        isRequested: true,
        releaseDate: pastReleaseDate,
        indexedAt,
      });

      season.title = "A brand new title";

      await em.flush();

      expect(season.state).toBe("paused");
    });

    it("falls back to its own state when a season has no episodes", async ({
      factories: { showFactory, seasonFactory },
    }) => {
      const show = await showFactory.createOne({
        status: "ended",
        releaseDate: pastReleaseDate,
        indexedAt,
      });

      const season = await seasonFactory.createOne({
        show,
        itemRequest: show.itemRequest,
        releaseDate: pastReleaseDate,
        indexedAt,
      });

      expect(season.state).toBe("indexed");
    });

    it("sets a season's state to completed when all requested episodes are completed", async ({
      em,
      factories: {
        showFactory,
        seasonFactory,
        episodeFactory,
        mediaEntryFactory,
      },
    }) => {
      const show = await showFactory.createOne({
        status: "ended",
        releaseDate: pastReleaseDate,
        indexedAt,
      });

      const season = await seasonFactory.createOne({
        show,
        itemRequest: show.itemRequest,
        releaseDate: pastReleaseDate,
        indexedAt,
      });

      const episode = await episodeFactory.createOne({
        season,
        show,
        itemRequest: show.itemRequest,
        number: 1,
        isSpecial: false,
        isRequested: true,
        releaseDate: pastReleaseDate,
        indexedAt,
      });

      const mediaEntry = mediaEntryFactory.makeOne({ mediaItem: episode });

      episode.filesystemEntries.add(mediaEntry);

      await em.flush();

      expect(episode.state).toBe("completed");
      expect(season.state).toBe("completed");
    });

    it("sets a season's state to partially_completed when only some requested episodes are completed", async ({
      em,
      factories: {
        showFactory,
        seasonFactory,
        episodeFactory,
        mediaEntryFactory,
      },
    }) => {
      const show = await showFactory.createOne({
        status: "ended",
        releaseDate: pastReleaseDate,
        indexedAt,
      });

      const season = await seasonFactory.createOne({
        show,
        itemRequest: show.itemRequest,
        releaseDate: pastReleaseDate,
        indexedAt,
      });

      const completedEpisode = await episodeFactory.createOne({
        season,
        show,
        itemRequest: show.itemRequest,
        number: 1,
        isSpecial: false,
        isRequested: true,
        releaseDate: pastReleaseDate,
        indexedAt,
      });

      await episodeFactory.createOne({
        season,
        show,
        itemRequest: show.itemRequest,
        number: 2,
        isSpecial: false,
        isRequested: true,
        releaseDate: pastReleaseDate,
        indexedAt,
      });

      const mediaEntry = mediaEntryFactory.makeOne({
        mediaItem: completedEpisode,
      });

      completedEpisode.filesystemEntries.add(mediaEntry);

      await em.flush();

      expect(season.state).toBe("partially_completed");
    });

    it("excludes unrequested episodes from a season's aggregated state", async ({
      em,
      factories: {
        showFactory,
        seasonFactory,
        episodeFactory,
        mediaEntryFactory,
      },
    }) => {
      const show = await showFactory.createOne({
        status: "ended",
        releaseDate: pastReleaseDate,
        indexedAt,
      });

      const season = await seasonFactory.createOne({
        show,
        itemRequest: show.itemRequest,
        releaseDate: pastReleaseDate,
        indexedAt,
      });

      const completedEpisode = await episodeFactory.createOne({
        season,
        show,
        itemRequest: show.itemRequest,
        number: 1,
        isSpecial: false,
        isRequested: true,
        releaseDate: pastReleaseDate,
        indexedAt,
      });

      await episodeFactory.createOne({
        season,
        show,
        itemRequest: show.itemRequest,
        number: 2,
        isSpecial: false,
        isRequested: false,
        releaseDate: pastReleaseDate,
        indexedAt,
      });

      const mediaEntry = mediaEntryFactory.makeOne({
        mediaItem: completedEpisode,
      });

      completedEpisode.filesystemEntries.add(mediaEntry);

      await em.flush();

      expect(season.state).toBe("completed");
    });

    it("cascades a completed episode's state up through the season to the show in a single flush", async ({
      em,
      factories: {
        showFactory,
        seasonFactory,
        episodeFactory,
        mediaEntryFactory,
      },
    }) => {
      const show = await showFactory.createOne({
        status: "ended",
        releaseDate: pastReleaseDate,
        indexedAt,
      });

      const season = await seasonFactory.createOne({
        show,
        itemRequest: show.itemRequest,
        releaseDate: pastReleaseDate,
        indexedAt,
      });

      const episode = await episodeFactory.createOne({
        season,
        show,
        itemRequest: show.itemRequest,
        number: 1,
        isSpecial: false,
        isRequested: true,
        releaseDate: pastReleaseDate,
        indexedAt,
      });

      const mediaEntry = mediaEntryFactory.makeOne({ mediaItem: episode });

      episode.filesystemEntries.add(mediaEntry);

      await em.flush();

      expect(episode.state).toBe("completed");
      expect(season.state).toBe("completed");
      expect(show.state).toBe("completed");
    });

    it("cascades a reset episode's state up through the season to the show in a single flush", async ({
      em,
      completedShowContext: { episodes },
    }) => {
      const [episode] = episodes;

      expect.assert(episode);

      episode.reset();

      await em.persist(episode).flush();

      expect(episode.state).toBe("indexed");
      expect(episode.season.getProperty("state")).toBe("partially_completed");
      expect(episode.show.getProperty("state")).toBe("partially_completed");
    });
  });

  describe("newly requested seasons", () => {
    it("recomputes the show's state directly when a season becomes requested, without changing the season's own state", async ({
      em,
      factories: { showFactory, seasonFactory },
    }) => {
      const show = await showFactory.createOne({
        status: "ended",
        releaseDate: pastReleaseDate,
        indexedAt,
      });

      const season = await seasonFactory.createOne({
        show,
        itemRequest: show.itemRequest,
        isRequested: false,
        state: "paused",
        releaseDate: pastReleaseDate,
        indexedAt,
      });

      season.isRequested = true;

      await em.flush();

      // A single requested season, in a fixed "paused" state, propagates
      // directly to the show.
      expect(show.state).toBe("paused");

      // The season's own state is left untouched by this path.
      expect(season.state).toBe("paused");
    });
  });

  describe("newly released episodes", () => {
    const airDate = DateTime.utc().plus({ days: 1 }).startOf("minute");

    /**
     * Creates an airing show whose season has exhausted its scrape attempts:
     * one released episode and one that airs at `airDate`.
     */
    async function createExhaustedAiringSeason(
      em: EntityManager,
      {
        showFactory,
        seasonFactory,
        episodeFactory,
        mediaEntryFactory,
      }: {
        showFactory: ShowFactory;
        seasonFactory: SeasonFactory;
        episodeFactory: EpisodeFactory;
        mediaEntryFactory: MediaEntryFactory;
      },
      { isFirstEpisodeCompleted }: { isFirstEpisodeCompleted: boolean },
    ) {
      const show = await showFactory.createOne({
        status: "continuing",
        releaseDate: pastReleaseDate,
        indexedAt,
      });

      const season = await seasonFactory.createOne({
        show,
        itemRequest: show.itemRequest,
        number: 1,
        releaseDate: pastReleaseDate,
        indexedAt,
      });

      const releasedEpisode = await episodeFactory.createOne({
        season,
        show,
        itemRequest: show.itemRequest,
        number: 1,
        releaseDate: pastReleaseDate,
        indexedAt,
      });

      if (isFirstEpisodeCompleted) {
        releasedEpisode.filesystemEntries.add(
          mediaEntryFactory.makeOne({ mediaItem: releasedEpisode }),
        );

        await em.flush();
      }

      const unreleasedEpisode = await episodeFactory.createOne({
        season,
        show,
        itemRequest: show.itemRequest,
        number: 2,
        releaseDate: airDate.toJSDate(),
        indexedAt,
      });

      season.failedScrapeAttempts = settings.maximumFailedAttempts;

      await em.flush();

      expect(unreleasedEpisode.state).toBe("unreleased");

      return { show, season, releasedEpisode, unreleasedEpisode };
    }

    it("resets the season's failed scrape attempts when one of its episodes is released", async ({
      em,
      factories,
    }) => {
      const { season, unreleasedEpisode } = await createExhaustedAiringSeason(
        em,
        factories,
        { isFirstEpisodeCompleted: true },
      );

      expect(season.state).toBe("partially_completed");

      vi.useFakeTimers({ now: airDate.plus({ hours: 1 }).toJSDate() });

      unreleasedEpisode.year = null; // Trigger a change on the entity to update the state

      await em.flush();

      expect(unreleasedEpisode.state).toBe("indexed");
      expect(season.state).toBe("partially_completed");

      const persistedSeason = await em.fork().findOneOrFail(Season, season.id);

      expect(persistedSeason.failedScrapeAttempts).toBe(0);
      expect(persistedSeason.state).toBe("partially_completed");
    });

    it("un-fails an exhausted season and its show when one of its episodes is released", async ({
      em,
      factories,
    }) => {
      const { show, season, unreleasedEpisode } =
        await createExhaustedAiringSeason(em, factories, {
          isFirstEpisodeCompleted: false,
        });

      expect(season.state).toBe("failed");
      expect(show.state).toBe("failed");

      vi.useFakeTimers({ now: airDate.plus({ hours: 1 }).toJSDate() });

      unreleasedEpisode.year = null; // Trigger a change on the entity to update the state

      await em.flush();

      const persistedSeason = await em.fork().findOneOrFail(Season, season.id, {
        populate: ["show"],
      });

      expect(persistedSeason.failedScrapeAttempts).toBe(0);
      expect(persistedSeason.state).toBe("indexed");
      expect(persistedSeason.show.getProperty("state")).toBe("indexed");
    });

    it("leaves an exhausted season alone when no new episode is released", async ({
      em,
      factories,
    }) => {
      const { show, season, releasedEpisode, unreleasedEpisode } =
        await createExhaustedAiringSeason(em, factories, {
          isFirstEpisodeCompleted: false,
        });

      // An already released episode changing state is not a new episode.
      releasedEpisode.reset();
      releasedEpisode.title = "A brand new title";

      // Nor is an unreleased episode that still hasn't aired.
      unreleasedEpisode.year = null;

      await em.flush();

      expect(unreleasedEpisode.state).toBe("unreleased");

      const persistedSeason = await em.fork().findOneOrFail(Season, season.id, {
        populate: ["show"],
      });

      expect(persistedSeason.failedScrapeAttempts).toBe(
        settings.maximumFailedAttempts,
      );
      expect(persistedSeason.state).toBe("failed");
      expect(show.state).toBe("failed");
    });

    it("resets the season when a re-index finds that a new episode has aired", async ({
      em,
      services: { indexerService },
      factories: { showItemRequestFactory },
    }) => {
      vi.useFakeTimers({ now: DateTime.utc().toJSDate() });

      const itemRequest = await showItemRequestFactory.createOne({
        imdbId: "tt7654321",
        state: "requested",
      });

      const indexData = {
        id: itemRequest.id,
        title: "Test Show",
        imdbId: "tt7654321",
        contentRating: "tv-14",
        genres: [],
        type: "show",
        network: "Test Network",
        status: "continuing",
        seasons: {
          1: {
            number: 1,
            title: "Season 1",
            episodes: [1, 2, 3].map((number) => ({
              absoluteNumber: number,
              contentRating: "tv-14",
              number,
              airedAt: airDate.plus({ weeks: number - 2 }).toISO(),
              title: `Episode ${number.toString()}`,
              runtime: 60,
            })),
          },
        },
      } as const satisfies Parameters<typeof indexerService.indexItem>[0];

      const { item: show } = await indexerService.indexItem(indexData);

      const [season] = await show.seasons.loadItems();

      expect.assert(season);

      await em
        .fork()
        .nativeUpdate(
          Season,
          { id: season.id },
          { failedScrapeAttempts: settings.maximumFailedAttempts },
        );

      // Re-indexing before the next episode airs keeps the exhausted count.
      await indexerService.indexItem(indexData);

      await expect(
        em.fork().findOneOrFail(Season, season.id),
      ).resolves.toMatchObject({
        failedScrapeAttempts: settings.maximumFailedAttempts,
      });

      vi.setSystemTime(airDate.plus({ hours: 1 }).toJSDate());

      await indexerService.indexItem(indexData);

      await expect(
        em.fork().findOneOrFail(Season, season.id),
      ).resolves.toMatchObject({ failedScrapeAttempts: 0, state: "indexed" });
    });
  });
});
