/**
 * In-process implementations for development, tests and single-instance
 * deployments. Several app instances can share one MemoryStore/MemoryBus in
 * the same process to simulate a cluster.
 */
import type { AbuseEvent, Backplane, BusEvent, SessionRecord, StateStore, StoredMessage } from "./types.js";

interface Entry<T> {
  v: T;
  exp: number;
}

export class MemoryStore implements StateStore {
  private kv = new Map<string, Entry<unknown>>();
  private lists = new Map<string, Entry<StoredMessage[]>>();
  private presence = new Map<string, { n: number; ts: number }>();
  private abuse: Array<{ id: string; ts: number; fields: Record<string, string> }> = [];
  private seq = 0;

  constructor(private readonly nowFn: () => number = Date.now) {}

  private get<T>(key: string): T | null {
    const e = this.kv.get(key);
    if (!e) return null;
    if (e.exp <= this.nowFn()) {
      this.kv.delete(key);
      return null;
    }
    return e.v as T;
  }
  private put(key: string, v: unknown, exp: number) {
    this.kv.set(key, { v, exp });
  }

  async time() {
    return this.nowFn();
  }
  async ping() {
    return true;
  }

  async claimName(roomId: string, name: string, sid: string, expireAt: number) {
    const key = `name:${roomId}:${name}`;
    if (this.get(key) !== null) return false;
    this.put(key, sid, expireAt);
    return true;
  }
  async nameOwner(roomId: string, name: string) {
    return this.get<string>(`name:${roomId}:${name}`);
  }
  async saveSession(rec: SessionRecord, expireAt: number) {
    this.put(`sess:${rec.sid}`, { ...rec }, expireAt);
  }
  async getSession(sid: string) {
    const r = this.get<SessionRecord>(`sess:${sid}`);
    return r ? { ...r } : null;
  }
  async setResume(hash: string, sid: string, expireAt: number) {
    this.put(`resume:${hash}`, sid, expireAt);
  }
  async resolveResume(hash: string) {
    return this.get<string>(`resume:${hash}`);
  }

  async appendMessage(roomId: string, msg: StoredMessage, keep: number, expireAt: number) {
    const key = `msgs:${roomId}`;
    let e = this.lists.get(key);
    if (!e || e.exp <= this.nowFn()) e = { v: [], exp: expireAt };
    e.v.push(msg);
    if (e.v.length > keep) e.v.splice(0, e.v.length - keep);
    e.exp = expireAt;
    this.lists.set(key, e);
  }
  async recentMessages(roomId: string, limit: number) {
    const e = this.lists.get(`msgs:${roomId}`);
    if (!e || e.exp <= this.nowFn() || limit <= 0) return [];
    return e.v.slice(-limit);
  }
  async deleteRoom(roomId: string) {
    this.lists.delete(`msgs:${roomId}`);
    for (const k of this.kv.keys()) if (k.startsWith(`name:${roomId}:`)) this.kv.delete(k);
  }

  async hit(key: string, windowMs: number, by = 1) {
    const now = this.nowFn();
    const k = `rl:${key}:${Math.floor(now / windowMs)}`;
    const n = (this.get<number>(k) ?? 0) + by;
    this.put(k, n, now + windowMs);
    return n;
  }
  async addToSet(key: string, member: string, ttlMs: number) {
    const k = `set:${key}`;
    const set = this.get<Set<string>>(k) ?? new Set<string>();
    set.add(member);
    this.put(k, set, this.nowFn() + ttlMs);
    return set.size;
  }

  async setBlock(key: string, ttlMs: number, reason: string) {
    this.put(`block:${key}`, reason, this.nowFn() + ttlMs);
  }
  async blockTtl(key: string) {
    const e = this.kv.get(`block:${key}`);
    if (!e) return 0;
    return Math.max(0, e.exp - this.nowFn());
  }
  async deleteBlock(key: string) {
    this.kv.delete(`block:${key}`);
  }

  async reportPresence(instanceId: string, count: number, now: number) {
    this.presence.set(instanceId, { n: count, ts: now });
  }
  async removePresence(instanceId: string) {
    this.presence.delete(instanceId);
  }
  async totalPresence(now: number, staleMs: number) {
    let total = 0;
    for (const [id, p] of this.presence) {
      if (now - p.ts > staleMs) this.presence.delete(id);
      else total += p.n;
    }
    return total;
  }

  async addAbuseEvent(fields: Record<string, string | number>, retentionMs: number) {
    const now = this.nowFn();
    this.abuse = this.abuse.filter((e) => now - e.ts < retentionMs).slice(-9999);
    this.abuse.push({
      id: `${now}-${this.seq++}`,
      ts: now,
      fields: Object.fromEntries(Object.entries(fields).map(([k, v]) => [k, String(v)])),
    });
  }
  async listAbuseEvents(limit: number): Promise<AbuseEvent[]> {
    return this.abuse.slice(-limit).reverse().map(({ id, fields }) => ({ id, fields }));
  }

  async acquireOnce(key: string, ttlMs: number) {
    const k = `lock:${key}`;
    if (this.get(k) !== null) return false;
    this.put(k, 1, this.nowFn() + ttlMs);
    return true;
  }

  async close() {}
}

/** In-process pub/sub hub. Each app instance gets its own `connect()` view. */
export class MemoryBus {
  private handlers = new Set<(e: BusEvent) => void>();

  /** Returns a view that shares this bus but can be closed independently (one per app instance). */
  connect(): Backplane {
    const mine = new Set<(e: BusEvent) => void>();
    return {
      publish: async (evt) => {
        // Serialize like a real transport would, and deliver asynchronously.
        const wire = JSON.stringify(evt);
        queueMicrotask(() => this.handlers.forEach((h) => h(JSON.parse(wire))));
      },
      subscribe: async (handler) => {
        mine.add(handler);
        this.handlers.add(handler);
      },
      close: async () => {
        mine.forEach((h) => this.handlers.delete(h));
        mine.clear();
      },
    };
  }
}
