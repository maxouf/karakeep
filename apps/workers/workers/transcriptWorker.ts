import { workerStatsCounter } from "metrics";
import { getProxyAgent, validateUrl } from "network";
import { withWorkerTracing } from "workerTracing";

import { db } from "@karakeep/db";
import { AssetTypes } from "@karakeep/db/schema";
import {
  QuotaService,
  StorageQuotaError,
  TranscriptWorkerQueue,
  ZTranscriptRequest,
  zTranscriptRequestSchema,
} from "@karakeep/shared-server";
import { newAssetId, saveAsset } from "@karakeep/shared/assetdb";
import serverConfig from "@karakeep/shared/config";
import logger from "@karakeep/shared/logger";
import { DequeuedJob, getQueueClient } from "@karakeep/shared/queueing";

import { getBookmarkDetails, updateAsset } from "../workerUtils";

export class TranscriptWorker {
  static async build() {
    logger.info("Starting transcript worker ...");

    return (await getQueueClient())!.createRunner<ZTranscriptRequest>(
      TranscriptWorkerQueue,
      {
        run: withWorkerTracing("transcriptWorker.run", runWorker),
        onComplete: async (job) => {
          workerStatsCounter.labels("transcript", "completed").inc();
          logger.info(
            `[TranscriptWorker][${job.id}] Transcript extraction completed`,
          );
          return Promise.resolve();
        },
        onError: async (job) => {
          workerStatsCounter.labels("transcript", "failed").inc();
          if (job.numRetriesLeft == 0) {
            workerStatsCounter.labels("transcript", "failed_permanent").inc();
          }
          logger.error(
            `[TranscriptWorker][${job.id}] Transcript extraction failed: ${job.error}`,
          );
          return Promise.resolve();
        },
      },
      {
        pollIntervalMs: 1000,
        timeoutSecs: 120,
        concurrency: 1,
        validator: zTranscriptRequestSchema,
      },
    );
  }
}

function extractVideoId(url: string): string | null {
  const patterns = [
    /(?:youtube\.com\/watch\?v=|youtu\.be\/|youtube\.com\/embed\/)([^&\n?#]+)/,
    /youtube\.com\/v\/([^&\n?#]+)/,
    /youtube\.com\/shorts\/([^&\n?#]+)/,
  ];
  for (const pattern of patterns) {
    const match = url.match(pattern);
    if (match) return match[1];
  }
  return null;
}

interface YTCaptionTrack {
  baseUrl: string;
  languageCode: string;
  kind?: string;
  name?: { simpleText?: string };
}

/**
 * Fetches YouTube captions by scraping the video page for captionTracks,
 * then downloading the VTT subtitle file directly.
 * No yt-dlp needed — pure HTTP fetch.
 */
async function fetchYouTubeTranscript(
  videoId: string,
  proxyUrl: string | undefined,
): Promise<string | null> {
  const lang =
    serverConfig.inference.inferredTagLang === "french" ? "fr" : "en";

  // Fetch the YouTube video page to extract caption tracks
  const pageUrl = `https://www.youtube.com/watch?v=${videoId}`;
  const fetchOptions: RequestInit = {};
  if (proxyUrl) {
    // Node fetch doesn't natively support proxies, but we try without
    logger.info(
      `[TranscriptWorker] Proxy configured but using direct fetch for YouTube page`,
    );
  }

  const pageResponse = await fetch(pageUrl, {
    ...fetchOptions,
    headers: {
      "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
      "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
    },
  });

  if (!pageResponse.ok) {
    logger.warn(
      `[TranscriptWorker] Failed to fetch YouTube page: ${pageResponse.status}`,
    );
    return null;
  }

  const pageHtml = await pageResponse.text();

  // Extract captionTracks from the page's player response JSON
  const captionTracksMatch = pageHtml.match(
    /"captionTracks"\s*:\s*(\[.*?\])\s*,\s*"/,
  );
  if (!captionTracksMatch) {
    logger.info(
      `[TranscriptWorker] No caption tracks found in YouTube page for ${videoId}`,
    );
    return null;
  }

  let captionTracks: YTCaptionTrack[];
  try {
    captionTracks = JSON.parse(captionTracksMatch[1]) as YTCaptionTrack[];
  } catch {
    logger.warn(
      `[TranscriptWorker] Failed to parse caption tracks JSON for ${videoId}`,
    );
    return null;
  }

  if (captionTracks.length === 0) {
    return null;
  }

  // Pick the best track: prefer manual subs in target lang, then auto subs in target lang, then any
  let track: YTCaptionTrack | undefined;

  // 1. Manual subs in preferred language
  track = captionTracks.find(
    (t) => t.languageCode === lang && t.kind !== "asr",
  );

  // 2. Auto-generated subs in preferred language
  if (!track) {
    track = captionTracks.find(
      (t) => t.languageCode === lang && t.kind === "asr",
    );
  }

  // 3. Manual subs in English as fallback
  if (!track) {
    track = captionTracks.find(
      (t) => t.languageCode === "en" && t.kind !== "asr",
    );
  }

  // 4. Auto subs in English
  if (!track) {
    track = captionTracks.find(
      (t) => t.languageCode === "en" && t.kind === "asr",
    );
  }

  // 5. Any available track
  if (!track) {
    track = captionTracks[0];
  }

  if (!track) {
    return null;
  }

  // Fetch the subtitle content as VTT
  const subtitleUrl = `${track.baseUrl}&fmt=vtt`;
  const subResponse = await fetch(subtitleUrl, {
    headers: {
      "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    },
  });

  if (!subResponse.ok) {
    logger.warn(
      `[TranscriptWorker] Failed to fetch subtitles: ${subResponse.status}`,
    );
    return null;
  }

  return subResponse.text();
}

/**
 * Parses VTT content and returns a JSON array of cues with start/end times in seconds.
 */
function vttToJson(
  vtt: string,
): { start: number; end: number; text: string }[] {
  const cues: { start: number; end: number; text: string }[] = [];
  const lines = vtt.split("\n");
  let i = 0;

  while (i < lines.length) {
    const line = lines[i].trim();
    // Look for timestamp lines: "00:00:01.000 --> 00:00:04.000"
    const match = line.match(
      /^(\d{2}:\d{2}:\d{2}\.\d{3})\s+-->\s+(\d{2}:\d{2}:\d{2}\.\d{3})/,
    );
    if (match) {
      const start = parseTimestamp(match[1]);
      const end = parseTimestamp(match[2]);
      i++;
      const textLines: string[] = [];
      while (i < lines.length && lines[i].trim() !== "") {
        textLines.push(lines[i].trim());
        i++;
      }
      const text = textLines
        .join(" ")
        .replace(/<[^>]+>/g, "") // Strip VTT formatting tags
        .replace(/&amp;/g, "&")
        .replace(/&lt;/g, "<")
        .replace(/&gt;/g, ">")
        .trim();
      if (text) {
        // Deduplicate: YouTube auto-subs often repeat lines
        const lastCue = cues[cues.length - 1];
        if (!lastCue || lastCue.text !== text) {
          cues.push({ start, end, text });
        } else {
          // Extend the end time of the previous cue
          lastCue.end = end;
        }
      }
    }
    i++;
  }
  return cues;
}

function parseTimestamp(ts: string): number {
  const parts = ts.split(":");
  const hours = parseInt(parts[0], 10);
  const minutes = parseInt(parts[1], 10);
  const secParts = parts[2].split(".");
  const seconds = parseInt(secParts[0], 10);
  const ms = parseInt(secParts[1], 10);
  return hours * 3600 + minutes * 60 + seconds + ms / 1000;
}

async function runWorker(job: DequeuedJob<ZTranscriptRequest>) {
  const jobId = job.id;
  const { bookmarkId } = job.data;

  const {
    url,
    userId,
    transcriptAssetId: oldTranscriptAssetId,
  } = await getBookmarkDetails(bookmarkId);

  const proxy = getProxyAgent(url);
  const validation = await validateUrl(url, !!proxy);
  if (!validation.ok) {
    logger.warn(
      `[TranscriptWorker][${jobId}] Skipping transcript for disallowed URL "${url}": ${validation.reason}`,
    );
    return;
  }
  const normalizedUrl = validation.url.toString();

  const videoId = extractVideoId(normalizedUrl);
  if (!videoId) {
    logger.info(
      `[TranscriptWorker][${jobId}] Could not extract video ID from "${normalizedUrl}"`,
    );
    return;
  }

  try {
    logger.info(
      `[TranscriptWorker][${jobId}] Extracting transcript for video ${videoId}`,
    );

    const vttContent = await fetchYouTubeTranscript(
      videoId,
      proxy?.proxy.toString(),
    );

    if (!vttContent) {
      logger.info(
        `[TranscriptWorker][${jobId}] No subtitles found for video ${videoId}`,
      );
      return;
    }

    // Convert VTT to structured JSON with timestamps
    const cues = vttToJson(vttContent);
    if (cues.length === 0) {
      logger.info(
        `[TranscriptWorker][${jobId}] Subtitles were empty after parsing for video ${videoId}`,
      );
      return;
    }

    const transcriptJson = JSON.stringify(cues);
    const transcriptBuffer = Buffer.from(transcriptJson, "utf-8");
    const transcriptAssetId = newAssetId();

    const quotaApproved = await QuotaService.checkStorageQuota(
      db,
      userId,
      transcriptBuffer.length,
    );

    await saveAsset({
      userId,
      assetId: transcriptAssetId,
      asset: transcriptBuffer,
      metadata: { contentType: "application/json" },
      quotaApproved,
    });

    await db.transaction(async (txn) => {
      await updateAsset(
        oldTranscriptAssetId,
        {
          id: transcriptAssetId,
          bookmarkId,
          userId,
          assetType: AssetTypes.LINK_TRANSCRIPT,
          contentType: "application/json",
          size: transcriptBuffer.length,
        },
        txn,
      );
    });

    logger.info(
      `[TranscriptWorker][${jobId}] Saved transcript (${cues.length} cues) for video ${videoId}`,
    );
  } catch (error) {
    if (error instanceof StorageQuotaError) {
      logger.warn(
        `[TranscriptWorker][${jobId}] Skipping transcript due to quota exceeded: ${(error as Error).message}`,
      );
      return;
    }
    throw error;
  }
}
