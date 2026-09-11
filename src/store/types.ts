import type { ChatMessage } from "../protocol.js";

/**
 * Internal, per-day anonymous session. Never sent to clients.
 * `sid` is a random internal id; `name` is the public daily identity.
 * There is deliberately no link between sessions of different days.
 */
export interface SessionRecord {
  sid: string;
  roomId: string;
  name: string;
  country: string | null;
  /** HMAC of the client network (daily-rotating salt). Never a raw IP. */
  ipHash: string;
  createdAt: number;
  suspicion: number;
  suspicionAt: number;
  challengePending: boolean;
  challengeFails: number;
  trustedUntil: number;
  /** Temporary blocks applied today (drives escalation). */
  blocks: number;
}

/** A message as stored for the day. `sid` is internal (for reports/enforcement) and never broadcast. */
export interface StoredMessage extends ChatMessage {
  sid: string;
}

export interface AbuseEvent {
  id: string;
  fields: Record<string, string>;
}

export interface StateStore {
  /** Authoritative time (ms). */
  time(): Promise<number>;
  ping(): Promise<boolean>;

  // — identities & sessions (all expire at the end of the day) —
  claimName(roomId: string, name: string, sid: string, expireAt: number): Promise<boolean>;
  nameOwner(roomId: string, name: string): Promise<string | null>;
  saveSession(rec: SessionRecord, expireAt: number): Promise<void>;
  getSession(sid: string): Promise<SessionRecord | null>;
  setResume(resumeHash: string, sid: string, expireAt: number): Promise<void>;
  resolveResume(resumeHash: string): Promise<string | null>;

  // — ephemeral messages —
  appendMessage(roomId: string, msg: StoredMessage, keep: number, expireAt: number): Promise<void>;
  recentMessages(roomId: string, limit: number): Promise<StoredMessage[]>;
  deleteRoom(roomId: string): Promise<void>;

  // — counters & limits —
  /** Fixed-window counter: increments and returns the count for the current window. */
  hit(key: string, windowMs: number, by?: number): Promise<number>;
  /** Adds member to a short-lived set and returns its cardinality. */
  addToSet(key: string, member: string, ttlMs: number): Promise<number>;

  // — enforcement —
  setBlock(key: string, ttlMs: number, reason: string): Promise<void>;
  /** Remaining block time in ms (0 = not blocked). */
  blockTtl(key: string): Promise<number>;
  deleteBlock(key: string): Promise<void>;

  // — presence (approximate, per instance) —
  reportPresence(instanceId: string, count: number, now: number): Promise<void>;
  removePresence(instanceId: string): Promise<void>;
  totalPresence(now: number, staleMs: number): Promise<number>;

  // — abuse log (bounded retention) —
  addAbuseEvent(fields: Record<string, string | number>, retentionMs: number): Promise<void>;
  listAbuseEvents(limit: number): Promise<AbuseEvent[]>;

  /** Returns true for exactly one caller per key within ttl (cluster-wide). */
  acquireOnce(key: string, ttlMs: number): Promise<boolean>;

  close(): Promise<void>;
}

export type BusEvent =
  /** `exclude` is the internal key of the sending connection (it receives an ack instead). */
  | { kind: "message"; origin: string; roomId: string; message: ChatMessage; exclude?: string }
  | { kind: "kick"; origin: string; sid: string; reason: string };

/** Cross-instance event fan-out. */
export interface Backplane {
  publish(evt: BusEvent): Promise<void>;
  subscribe(handler: (evt: BusEvent) => void): Promise<void>;
  close(): Promise<void>;
}
