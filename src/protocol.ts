/**
 * Wire protocol. JSON text frames over one WebSocket (path /ws).
 * Mirrors the frontend's src/lib/room/protocol.ts — keep them in sync.
 * All timestamps are epoch ms on the server's authoritative clock.
 */
import { z } from "zod";

export interface Identity {
  name: string;
  country?: string | null;
}

export interface ChatMessage {
  id: string;
  author: string;
  text: string;
  ts: number;
  country?: string | null;
}

export type RejectReason =
  | "rate_limited"
  | "challenge_required"
  | "too_long"
  | "invalid"
  | "stale_room"
  | "blocked";

export type ServerEvent =
  | {
      type: "welcome";
      identity: Identity;
      day: number;
      dayEndsAt: number;
      serverTime: number;
      online: number;
      history: ChatMessage[];
      resume: string;
      maxLength: number;
    }
  | { type: "message"; message: ChatMessage }
  /** Several messages coalesced into one frame under load (same semantics as N `message` events). */
  | { type: "messages"; messages: ChatMessage[] }
  | { type: "ack"; clientId: string; message: ChatMessage }
  | { type: "reject"; clientId: string; reason: RejectReason; retryAfterMs?: number }
  | { type: "presence"; online: number }
  | { type: "reset"; day: number; dayEndsAt: number; identity: Identity; resume: string }
  | { type: "challenge"; provider: "turnstile" | "mock"; siteKey?: string }
  | { type: "challenge_result"; ok: boolean }
  | { type: "rate_limited"; retryAfterMs: number }
  | { type: "reported"; messageId: string }
  | { type: "error"; code: string; message?: string }
  | { type: "pong"; t: number };

/** Client ids are opaque but bounded. */
const clientId = z.string().min(1).max(64).regex(/^[A-Za-z0-9_-]+$/);

export const clientEventSchema = z.discriminatedUnion("type", [
  z
    .object({
      type: z.literal("join"),
      resume: z.string().max(128).nullish(),
      verification: z.string().max(4096).nullish(),
    })
    .strict(),
  z
    .object({
      type: z.literal("send"),
      clientId,
      // Raw bound only; real limits are applied after normalization.
      text: z.string().max(8000),
      /** Day the client believes it is in. Lets the server reject cross-midnight in-flight sends. */
      day: z.number().int().positive().optional(),
    })
    .strict(),
  z.object({ type: z.literal("challenge_response"), token: z.string().min(1).max(4096) }).strict(),
  z.object({ type: z.literal("ping"), t: z.number().finite() }).strict(),
  z
    .object({
      type: z.literal("report"),
      messageId: z.string().min(1).max(64),
      reason: z.enum(["spam", "harassment", "illegal", "other"]).default("other"),
    })
    .strict(),
]);

export type ClientEvent = z.infer<typeof clientEventSchema>;

/** Hard cap on a single inbound frame. A max-length message in 4-byte UTF-8 + JSON fits comfortably. */
export const MAX_FRAME_BYTES = 8 * 1024;

/** Close codes. 4400–4499 = the client must not auto-reconnect. */
export const Close = {
  NORMAL: 1000,
  GOING_AWAY: 1001,
  POLICY: 1008,
  TOO_BIG: 1009,
  RESTART: 1012,
  TRY_LATER: 1013,
  BLOCKED: 4403,
} as const;
