"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { Play } from "lucide-react";

import { BookmarkTypes, ZBookmark } from "@karakeep/shared/types/bookmarks";

import { ContentRenderer } from "./types";

// -- YouTube video ID extraction --

function extractYouTubeVideoId(url: string): string | null {
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

function canRenderYouTube(bookmark: ZBookmark): boolean {
  if (bookmark.content.type !== BookmarkTypes.LINK) return false;
  return extractYouTubeVideoId(bookmark.content.url) !== null;
}

// -- Transcript types --

interface TranscriptCue {
  start: number;
  end: number;
  text: string;
}

// -- YouTube IFrame API types --

interface YTPlayer {
  getCurrentTime: () => number;
  seekTo: (seconds: number, allowSeekAhead: boolean) => void;
  destroy: () => void;
}

interface YTPlayerEvent {
  target: YTPlayer;
}

declare global {
  interface Window {
    YT?: {
      Player: new (
        elementId: string,
        config: {
          videoId: string;
          events?: {
            onReady?: (event: YTPlayerEvent) => void;
          };
          playerVars?: Record<string, string | number>;
        },
      ) => YTPlayer;
    };
    onYouTubeIframeAPIReady?: () => void;
  }
}

// -- Load YouTube IFrame API --

let ytApiLoaded = false;
let ytApiPromise: Promise<void> | null = null;

function loadYouTubeApi(): Promise<void> {
  if (ytApiLoaded && window.YT) return Promise.resolve();
  if (ytApiPromise) return ytApiPromise;

  ytApiPromise = new Promise<void>((resolve) => {
    if (window.YT) {
      ytApiLoaded = true;
      resolve();
      return;
    }
    const tag = document.createElement("script");
    tag.src = "https://www.youtube.com/iframe_api";
    const prev = window.onYouTubeIframeAPIReady;
    window.onYouTubeIframeAPIReady = () => {
      ytApiLoaded = true;
      prev?.();
      resolve();
    };
    document.head.appendChild(tag);
  });
  return ytApiPromise;
}

// -- Synchronized transcript component --

function SyncedTranscript({
  cues,
  currentTime,
  onClickCue,
}: {
  cues: TranscriptCue[];
  currentTime: number;
  onClickCue: (time: number) => void;
}) {
  const activeRef = useRef<HTMLSpanElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);

  // Auto-scroll to active cue
  useEffect(() => {
    if (activeRef.current && containerRef.current) {
      const container = containerRef.current;
      const el = activeRef.current;
      const containerRect = container.getBoundingClientRect();
      const elRect = el.getBoundingClientRect();
      const offset = elRect.top - containerRect.top - containerRect.height / 3;
      if (Math.abs(offset) > 50) {
        container.scrollBy({ top: offset, behavior: "smooth" });
      }
    }
  }, [currentTime]);

  return (
    <div
      ref={containerRef}
      className="overflow-y-auto border-t p-4"
      style={{ maxHeight: "300px" }}
    >
      <p className="text-sm leading-relaxed">
        {cues.map((cue, i) => {
          const isActive = currentTime >= cue.start && currentTime < cue.end;
          return (
            <span
              key={i}
              ref={isActive ? activeRef : undefined}
              role="button"
              tabIndex={0}
              onClick={() => onClickCue(cue.start)}
              onKeyDown={(e) => {
                if (e.key === "Enter" || e.key === " ") onClickCue(cue.start);
              }}
              className={`cursor-pointer rounded px-0.5 transition-colors duration-200 hover:bg-yellow-100 dark:hover:bg-yellow-900 ${
                isActive
                  ? "bg-yellow-200 font-medium dark:bg-yellow-800"
                  : "text-muted-foreground"
              }`}
            >
              {cue.text}{" "}
            </span>
          );
        })}
      </p>
    </div>
  );
}

// -- Main YouTube renderer with transcript --

function YouTubeRendererComponent({ bookmark }: { bookmark: ZBookmark }) {
  const content = bookmark.content;
  const linkContent = content.type === BookmarkTypes.LINK ? content : null;
  const url = linkContent?.url ?? "";
  const videoId = extractYouTubeVideoId(url);
  const transcriptAssetId = linkContent?.transcriptAssetId ?? null;
  const playerContainerId = `yt-player-${bookmark.id}`;

  const [cues, setCues] = useState<TranscriptCue[] | null>(null);
  const [currentTime, setCurrentTime] = useState(0);
  const playerRef = useRef<YTPlayer | null>(null);
  const timerRef = useRef<ReturnType<typeof setInterval> | null>(null);

  // Fetch transcript
  useEffect(() => {
    if (!transcriptAssetId) return;
    fetch(`/api/assets/${transcriptAssetId}`)
      .then((r) => r.json())
      .then((data: TranscriptCue[]) => {
        if (Array.isArray(data) && data.length > 0) {
          setCues(data);
        }
      })
      .catch(() => {
        /* transcript fetch failed, continue without it */
      });
  }, [transcriptAssetId]);

  // Initialize YouTube player
  useEffect(() => {
    if (!cues || !videoId) return;

    let destroyed = false;

    loadYouTubeApi().then(() => {
      if (destroyed || !window.YT) return;

      const _player = new window.YT.Player(playerContainerId, {
        videoId,
        playerVars: {
          rel: 0,
          modestbranding: 1,
        },
        events: {
          onReady: (event: YTPlayerEvent) => {
            playerRef.current = event.target;
            timerRef.current = setInterval(() => {
              if (playerRef.current) {
                try {
                  setCurrentTime(playerRef.current.getCurrentTime());
                } catch {
                  /* Player might be destroyed */
                }
              }
            }, 250);
          },
        },
      });
      // _player reference is held by the YT API internally
      void _player;
    });

    return () => {
      destroyed = true;
      if (timerRef.current) clearInterval(timerRef.current);
      if (playerRef.current) {
        try {
          playerRef.current.destroy();
        } catch {
          /* ignore */
        }
      }
      playerRef.current = null;
    };
  }, [cues, videoId, playerContainerId]);

  const handleSeek = useCallback((time: number) => {
    if (playerRef.current) {
      playerRef.current.seekTo(time, true);
    }
  }, []);

  if (!videoId) return null;

  // If no transcript, use simple iframe (no API needed)
  if (!cues) {
    return (
      <div className="relative h-full w-full overflow-hidden">
        <div className="absolute inset-0 h-full w-full">
          <iframe
            src={`https://www.youtube.com/embed/${videoId}`}
            title="YouTube video player"
            allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share"
            allowFullScreen
            className="h-full w-full border-0"
          />
        </div>
      </div>
    );
  }

  // With transcript: use YouTube API player + synced transcript
  return (
    <div className="flex h-full w-full flex-col overflow-hidden">
      <div className="relative w-full" style={{ paddingBottom: "56.25%" }}>
        <div
          id={playerContainerId}
          className="absolute inset-0 h-full w-full"
        />
      </div>
      <SyncedTranscript
        cues={cues}
        currentTime={currentTime}
        onClickCue={handleSeek}
      />
    </div>
  );
}

export const youTubeRenderer: ContentRenderer = {
  id: "youtube",
  name: "YouTube",
  icon: Play,
  canRender: canRenderYouTube,
  component: YouTubeRendererComponent,
  priority: 10,
};
