import fs from "fs";
import * as os from "os";
import path from "path";
import { execa } from "execa";
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

const TMP_FOLDER = path.join(os.tmpdir(), "transcript_downloads");

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

/**
 * Extracts YouTube subtitles using yt-dlp and returns them as a VTT string.
 * Tries: 1) manual subs in target lang, 2) auto subs in target lang, 3) any available subs.
 */
async function extractSubtitles(
  url: string,
  tmpDir: string,
  proxy: string | undefined,
  abortSignal: AbortSignal,
): Promise<string | null> {
  const baseName = "transcript";
  const lang =
    serverConfig.inference.inferredTagLang === "french" ? "fr" : "en";

  // Try to get subtitles (manual + auto) in the preferred language
  const args = [
    url,
    "--write-subs",
    "--write-auto-subs",
    "--sub-lang",
    `${lang}.*,${lang}`,
    "--sub-format",
    "vtt",
    "--skip-download",
    "--no-playlist",
    "-o",
    path.join(tmpDir, baseName),
  ];
  if (proxy) {
    args.push("--proxy", proxy);
  }

  try {
    await execa("yt-dlp", args, { cancelSignal: abortSignal });
  } catch (e) {
    const err = e as Error;
    if (
      err.message.includes("ERROR: Unsupported URL:") ||
      err.message.includes("No media found")
    ) {
      return null;
    }
    logger.warn(
      `[TranscriptWorker] yt-dlp subtitle extraction warning: ${err.message}`,
    );
  }

  // Find the downloaded VTT file
  const vttFile = await findVttFile(tmpDir);
  if (!vttFile) {
    return null;
  }

  return fs.promises.readFile(vttFile, "utf-8");
}

async function findVttFile(dir: string): Promise<string | null> {
  try {
    const files = await fs.promises.readdir(dir);
    const vtt = files.find((f) => f.endsWith(".vtt"));
    return vtt ? path.join(dir, vtt) : null;
  } catch {
    return null;
  }
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

  const tmpDir = path.join(TMP_FOLDER, jobId);
  await fs.promises.mkdir(tmpDir, { recursive: true });

  try {
    logger.info(
      `[TranscriptWorker][${jobId}] Extracting transcript from "${normalizedUrl}"`,
    );

    const vttContent = await extractSubtitles(
      normalizedUrl,
      tmpDir,
      proxy?.proxy.toString(),
      job.abortSignal,
    );

    if (!vttContent) {
      logger.info(
        `[TranscriptWorker][${jobId}] No subtitles found for "${normalizedUrl}"`,
      );
      return;
    }

    // Convert VTT to structured JSON with timestamps
    const cues = vttToJson(vttContent);
    if (cues.length === 0) {
      logger.info(
        `[TranscriptWorker][${jobId}] Subtitles were empty after parsing for "${normalizedUrl}"`,
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
      `[TranscriptWorker][${jobId}] Saved transcript (${cues.length} cues) for "${normalizedUrl}"`,
    );
  } catch (error) {
    if (error instanceof StorageQuotaError) {
      logger.warn(
        `[TranscriptWorker][${jobId}] Skipping transcript due to quota exceeded: ${(error as Error).message}`,
      );
      return;
    }
    throw error;
  } finally {
    // Clean up temp directory
    await fs.promises.rm(tmpDir, { recursive: true, force: true }).catch(() => {
      /* ignore cleanup errors */
    });
  }
}
