/**
 * Redis-backed shared state + pub/sub backplane.
 *
 * Key schema (all keys under REDIS_PREFIX, default "room:"):
 *   msgs:{roomId}            LIST  JSON StoredMessage, capped (LTRIM), PEXPIREAT end-of-day + grace
 *   name:{roomId}:{name}     STRING sid                SET NX PXAT end-of-day   (public-name uniqueness)
 *   sess:{sid}               STRING JSON SessionRecord PXAT end-of-day
 *   resume:{sha256(token)}   STRING sid                PXAT end-of-day
 *   rl:{key}:{window}        STRING counter            PEXPIRE window
 *   set:{key}                SET    members            PEXPIRE ttl   (cross-session duplicate detection)
 *   block:{key}              STRING reason             PX ttl
 *   lock:{key}               STRING "1"                SET NX PX     (run-once, e.g. room cleanup)
 *   presence                 HASH   instanceId → "count:ts"
 *   abuse                    STREAM abuse events/reports, MAXLEN ~10k + MINID retention
 *   events                   PUB/SUB channel for cross-instance fan-out
 */
import { Redis } from "ioredis";
import type { AbuseEvent, Backplane, BusEvent, SessionRecord, StateStore, StoredMessage } from "./types.js";

const ROOM_GRACE_MS = 10 * 60_000;

export function createRedis(url: string, name: string): Redis {
  return new Redis(url, {
    connectionName: name,
    // Fail fast rather than queueing realtime work behind a dead connection.
    maxRetriesPerRequest: 2,
    commandTimeout: 3000,
    enableAutoPipelining: true,
    retryStrategy: (times) => Math.min(times * 200, 3000),
  });
}

export class RedisStore implements StateStore {
  constructor(private readonly r: Redis, private readonly p: string) {}

  private k(key: string) {
    return this.p + key;
  }

  async time() {
    const [s, us] = await this.r.time();
    return Number(s) * 1000 + Math.floor(Number(us) / 1000);
  }

  async ping() {
    try {
      return (await this.r.ping()) === "PONG";
    } catch {
      return false;
    }
  }

  async claimName(roomId: string, name: string, sid: string, expireAt: number) {
    const res = await this.r.set(this.k(`name:${roomId}:${name}`), sid, "PXAT", expireAt, "NX");
    return res === "OK";
  }
  async nameOwner(roomId: string, name: string) {
    return this.r.get(this.k(`name:${roomId}:${name}`));
  }
  async saveSession(rec: SessionRecord, expireAt: number) {
    await this.r.set(this.k(`sess:${rec.sid}`), JSON.stringify(rec), "PXAT", expireAt);
  }
  async getSession(sid: string) {
    const raw = await this.r.get(this.k(`sess:${sid}`));
    return raw ? (JSON.parse(raw) as SessionRecord) : null;
  }
  async setResume(hash: string, sid: string, expireAt: number) {
    await this.r.set(this.k(`resume:${hash}`), sid, "PXAT", expireAt);
  }
  async resolveResume(hash: string) {
    return this.r.get(this.k(`resume:${hash}`));
  }

  async appendMessage(roomId: string, msg: StoredMessage, keep: number, expireAt: number) {
    const key = this.k(`msgs:${roomId}`);
    await this.r
      .multi()
      .rpush(key, JSON.stringify(msg))
      .ltrim(key, -keep, -1)
      .pexpireat(key, expireAt + ROOM_GRACE_MS)
      .exec();
  }
  async recentMessages(roomId: string, limit: number) {
    if (limit <= 0) return [];
    const raw = await this.r.lrange(this.k(`msgs:${roomId}`), -limit, -1);
    return raw.map((s) => JSON.parse(s) as StoredMessage);
  }
  async deleteRoom(roomId: string) {
    // Names/sessions/resume tokens expire exactly at end of day via PXAT.
    await this.r.del(this.k(`msgs:${roomId}`));
  }

  async hit(key: string, windowMs: number, by = 1) {
    const k = this.k(`rl:${key}:${Math.floor(Date.now() / windowMs)}`);
    const res = await this.r.multi().incrby(k, by).pexpire(k, windowMs).exec();
    return Number(res?.[0]?.[1] ?? 0);
  }
  async addToSet(key: string, member: string, ttlMs: number) {
    const k = this.k(`set:${key}`);
    const res = await this.r.multi().sadd(k, member).pexpire(k, ttlMs).scard(k).exec();
    return Number(res?.[2]?.[1] ?? 0);
  }

  async setBlock(key: string, ttlMs: number, reason: string) {
    await this.r.set(this.k(`block:${key}`), reason.slice(0, 200), "PX", Math.max(1, Math.round(ttlMs)));
  }
  async blockTtl(key: string) {
    const t = await this.r.pttl(this.k(`block:${key}`));
    return t > 0 ? t : 0;
  }
  async deleteBlock(key: string) {
    await this.r.del(this.k(`block:${key}`));
  }

  async reportPresence(instanceId: string, count: number, now: number) {
    await this.r.hset(this.k("presence"), instanceId, `${count}:${now}`);
  }
  async removePresence(instanceId: string) {
    await this.r.hdel(this.k("presence"), instanceId);
  }
  async totalPresence(now: number, staleMs: number) {
    const all = await this.r.hgetall(this.k("presence"));
    let total = 0;
    const stale: string[] = [];
    for (const [id, v] of Object.entries(all)) {
      const [n, ts] = v.split(":").map(Number);
      if (!Number.isFinite(ts) || now - ts > staleMs) stale.push(id);
      else total += n || 0;
    }
    if (stale.length) await this.r.hdel(this.k("presence"), ...stale);
    return total;
  }

  async addAbuseEvent(fields: Record<string, string | number>, retentionMs: number) {
    const key = this.k("abuse");
    const flat = Object.entries(fields).flatMap(([k, v]) => [k, String(v)]);
    await this.r
      .multi()
      .xadd(key, "MAXLEN", "~", "10000", "*", ...flat)
      .xtrim(key, "MINID", "~", String(Date.now() - retentionMs))
      .exec();
  }
  async listAbuseEvents(limit: number): Promise<AbuseEvent[]> {
    const rows = await this.r.xrevrange(this.k("abuse"), "+", "-", "COUNT", limit);
    return rows.map(([id, flat]) => {
      const fields: Record<string, string> = {};
      for (let i = 0; i < flat.length; i += 2) fields[flat[i]] = flat[i + 1];
      return { id, fields };
    });
  }

  async acquireOnce(key: string, ttlMs: number) {
    return (await this.r.set(this.k(`lock:${key}`), "1", "PX", ttlMs, "NX")) === "OK";
  }

  async close() {
    this.r.disconnect();
  }
}

export class RedisBus implements Backplane {
  private readonly channel: string;

  constructor(private readonly pub: Redis, private readonly sub: Redis, prefix: string) {
    this.channel = `${prefix}events`;
  }

  async publish(evt: BusEvent) {
    await this.pub.publish(this.channel, JSON.stringify(evt));
  }

  async subscribe(handler: (evt: BusEvent) => void) {
    this.sub.on("message", (channel: string, raw: string) => {
      if (channel !== this.channel) return;
      try {
        handler(JSON.parse(raw) as BusEvent);
      } catch {
        /* ignore malformed bus frames */
      }
    });
    await this.sub.subscribe(this.channel);
  }

  async close() {
    this.sub.disconnect();
    this.pub.disconnect();
  }
}
