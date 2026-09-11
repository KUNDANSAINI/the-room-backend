import { createHash } from "node:crypto";
import { performance } from "node:perf_hooks";
import type { RawData, WebSocket } from "ws";
import { MESSAGE_TIERS, TokenBucket, violationCooldownMs, type Tier } from "../abuse/rate-limit.js";
import { SIGNALS, SuspicionScore, THRESHOLDS, TimingAnalyzer, type Signal } from "../abuse/suspicion.js";
import { logTag, newMessageId } from "../identity/tokens.js";
import { fingerprint, normalizeText } from "../messaging/validate.js";
import type { NetworkIdentity } from "../net/ip.js";
import { Close, clientEventSchema, type ChatMessage, type ClientEvent, type ServerEvent } from "../protocol.js";
import { roomAt, type DailyRoom } from "../room/day.js";
import type { SessionRecord, StoredMessage } from "../store/types.js";
import type { RoomServer } from "./room-server.js";

export interface AdmitInfo {
  net: NetworkIdentity;
  hasOrigin: boolean;
  automatedUA: boolean;
  country: string | null;
}

type SendEvent = Extract<ClientEvent, { type: "send" }>;
type Reject = Extract<ServerEvent, { type: "reject" }>;

const JOIN_TIMEOUT_MS = 10_000;
const IDLE_TIMEOUT_MS = 120_000;
const MAX_QUEUED_FRAMES = 16;
const MAX_INVALID_FRAMES = 5;
const SLOW_CONSUMER_BYTES = 1 << 20;
const STALE_ROOM_GRACE_MS = 3_000;
const TRUST_WINDOW_MS = 30 * 60_000;
const NETWORK_MSG_WINDOW_MS = 10_000;
const NETWORK_MSG_LIMIT = 80;
const CROSS_DUP_MIN_CHARS = 12;
const CROSS_DUP_SESSIONS = 4;
const REPORT_RETENTION_MS = 30 * 86_400_000;
const EVENT_RETENTION_MS = 7 * 86_400_000;

let seq = 0;

export class Connection {
  readonly key: string;
  rec: SessionRecord | null = null;
  joined = false;

  private resumeToken: string | null = null;
  private alive = true;
  private lastFrameAt: number;
  private joinedAt = 0;
  private rolledAt = 0;
  private closed = false;

  private queue: Promise<void> = Promise.resolve();
  private queued = 0;
  private invalidFrames = 0;

  private readonly score = new SuspicionScore();
  private readonly timing = new TimingAnalyzer();
  private bucket: TokenBucket;
  private tier: Tier = MESSAGE_TIERS.normal;
  private strikes = 0;
  private lastStrikeAt = 0;
  private cooldownUntil = 0;
  private lastIgnoredSignal = 0;
  private blockedUntil = 0;
  private recentOwn: Array<{ fp: string; t: number }> = [];
  private replies = new Map<string, ServerEvent>();
  private pingWindow = { start: 0, n: 0 };
  private lastChallengeAttempt = 0;
  private lastChallengeSent = 0;
  private persistTimer: NodeJS.Timeout | undefined;
  private joinTimer: NodeJS.Timeout;

  constructor(private readonly srv: RoomServer, private readonly ws: WebSocket, readonly info: AdmitInfo) {
    this.key = `${srv.instanceId}:${(++seq).toString(36)}`;
    const now = srv.clock.now();
    this.lastFrameAt = now;
    this.bucket = new TokenBucket(this.tier.capacity, this.tier.refillPerSec, now);

    if (info.automatedUA) this.signal("automationUserAgent");
    if (!info.hasOrigin) this.signal("missingOrigin");

    ws.on("message", (data, isBinary) => this.onFrame(data, isBinary));
    ws.on("pong", () => {
      this.alive = true;
      this.lastFrameAt = this.srv.clock.now();
    });
    ws.on("close", () => this.onClose());
    ws.on("error", () => {
      /* 'close' follows; errors like oversized frames are handled by ws */
    });

    this.joinTimer = setTimeout(() => {
      if (!this.joined) this.close(Close.POLICY, "join timeout");
    }, JOIN_TIMEOUT_MS);
    this.joinTimer.unref();
  }

  get sid() {
    return this.rec?.sid ?? null;
  }

  get suspicion() {
    return this.score.value(this.srv.clock.now());
  }

  // ————— transport —————

  send(evt: ServerEvent) {
    this.sendRaw(JSON.stringify(evt));
  }

  sendRaw(frame: string): boolean {
    if (this.ws.readyState !== this.ws.OPEN) return false;
    if (this.ws.bufferedAmount > SLOW_CONSUMER_BYTES) {
      // A client that can't keep up would otherwise grow server memory unboundedly.
      this.srv.metrics.errors.inc({ kind: "slow_consumer" });
      this.ws.terminate();
      return false;
    }
    this.ws.send(frame);
    return true;
  }

  close(code: number, reason = "") {
    if (this.ws.readyState === this.ws.OPEN || this.ws.readyState === this.ws.CONNECTING) {
      this.ws.close(code, reason.slice(0, 120));
    }
  }

  terminate() {
    this.ws.terminate();
  }

  /** Called by the server's heartbeat loop. */
  heartbeat(now: number) {
    if (!this.alive || now - this.lastFrameAt > IDLE_TIMEOUT_MS) {
      this.ws.terminate();
      return;
    }
    this.alive = false;
    try {
      this.ws.ping();
    } catch {
      this.ws.terminate();
    }
  }

  private onClose() {
    if (this.closed) return;
    this.closed = true;
    clearTimeout(this.joinTimer);
    clearTimeout(this.persistTimer);
    if (this.rec) void this.persist();
    this.srv.onClosed(this);
  }

  // ————— inbound frames —————

  private onFrame(data: RawData, isBinary: boolean) {
    this.lastFrameAt = this.srv.clock.now();
    this.alive = true;
    if (isBinary) {
      this.invalid("binary");
      return;
    }
    if (this.queued >= MAX_QUEUED_FRAMES) {
      // Frames arriving faster than we can process them: flood.
      this.srv.metrics.invalidFrames.inc({ reason: "queue_overflow" });
      const now = this.srv.clock.now();
      if (now - this.lastIgnoredSignal >= 250) {
        this.lastIgnoredSignal = now;
        this.signal("ignoredCooldown");
      }
      return;
    }
    const raw = Array.isArray(data) ? Buffer.concat(data).toString("utf8") : data.toString("utf8");
    this.queued++;
    this.queue = this.queue
      .then(() => this.handle(raw))
      .catch((err) => {
        this.srv.metrics.errors.inc({ kind: "handler" });
        this.srv.log.error({ err, conn: this.key }, "frame handler failed");
      })
      .finally(() => {
        this.queued--;
      });
  }

  private async handle(raw: string) {
    if (this.closed) return;
    let json: unknown;
    try {
      json = JSON.parse(raw);
    } catch {
      return this.invalid("json");
    }
    const parsed = clientEventSchema.safeParse(json);
    if (!parsed.success) return this.invalid("schema");
    const e = parsed.data;
    switch (e.type) {
      case "join":
        return this.join(e.resume ?? null, e.verification ?? null);
      case "send":
        return this.handleSend(e);
      case "challenge_response":
        return this.challengeResponse(e.token);
      case "ping":
        return this.ping(e.t);
      case "report":
        return this.report(e.messageId, e.reason);
    }
  }

  private invalid(reason: "binary" | "json" | "schema") {
    this.invalidFrames++;
    this.srv.metrics.invalidFrames.inc({ reason });
    this.signal(reason === "binary" ? "binaryFrame" : "malformedFrame");
    this.send({ type: "error", code: "bad_request", message: reason === "binary" ? "Text frames only." : "Malformed event." });
    if (this.invalidFrames >= MAX_INVALID_FRAMES) this.close(Close.POLICY, "malformed");
  }

  // ————— scoring —————

  private trusted(now: number) {
    return (this.rec?.trustedUntil ?? 0) > now;
  }

  private signal(name: Signal) {
    const now = this.srv.clock.now();
    this.score.add(SIGNALS[name], now, this.trusted(now));
    this.srv.metrics.signals.inc({ signal: name });
    this.applyTier(now);
    this.persistSoon();
  }

  private applyTier(now: number) {
    const v = this.score.value(now);
    const next =
      v >= THRESHOLDS.severe ? MESSAGE_TIERS.restricted : v >= THRESHOLDS.throttle ? MESSAGE_TIERS.throttled : MESSAGE_TIERS.normal;
    if (next !== this.tier) {
      this.tier = next;
      this.bucket.reconfigure(next.capacity, next.refillPerSec, now);
    }
  }

  // ————— join —————

  private async join(resume: string | null, verification: string | null) {
    if (this.joined) return;
    const { store, sessions, config, verifier, metrics } = this.srv;
    const now = this.srv.clock.now();

    let rec: SessionRecord | null = null;
    let token = resume;
    for (let attempt = 0; attempt < 2; attempt++) {
      const room = this.srv.room;
      rec = token ? await sessions.resume(room, token).catch(() => null) : null;
      if (rec) {
        const churn = await store.hit(`rejoin:${rec.sid}`, 5 * 60_000).catch(() => 0);
        if (churn > 12) this.signal("reconnectChurn");
      } else {
        const created = await store.hit(`newsess:${this.info.net.dailyHash}`, 3_600_000).catch(() => 0);
        if (created > config.NEW_SESSIONS_PER_IP_HOUR * 3) {
          metrics.connectionsRejected.inc({ reason: "session_flood" });
          this.send({ type: "error", code: "try_later", message: "Too many new sessions from your network." });
          this.close(Close.TRY_LATER, "session flood");
          return;
        }
        if (created > config.NEW_SESSIONS_PER_IP_HOUR) this.signal("manyNewSessionsFromNetwork");
        const s = await sessions.create(room, {
          ipHash: this.info.net.dailyHash,
          country: this.info.country,
          now,
          carry: { suspicion: this.score.value(now), challengePending: false },
        });
        rec = s.rec;
        token = s.token;
      }
      // Midnight may have passed while we awaited the store; if so, redo for the new room.
      if (rec.roomId === this.srv.room.roomId) break;
      token = null;
    }
    if (!rec || !token || this.closed) return;

    const blocked = await store.blockTtl(`sess:${rec.sid}`).catch(() => 0);
    if (blocked > 0) return this.blocked(blocked);

    // Carry over suspicion from the stored session (reconnecting doesn't wash it away).
    const stored = new SuspicionScore(rec.suspicion, rec.suspicionAt).value(now);
    if (stored > this.score.value(now)) this.score.add(stored - this.score.value(now), now);

    this.rec = rec;
    this.resumeToken = token;
    this.joined = true;
    this.joinedAt = now;
    clearTimeout(this.joinTimer);
    this.applyTier(now);
    this.srv.onJoined(this);

    const room = this.srv.room;
    const history = await store.recentMessages(room.roomId, config.HISTORY_LIMIT).catch(() => [] as StoredMessage[]);
    this.send({
      type: "welcome",
      identity: { name: rec.name, country: rec.country },
      day: room.day,
      dayEndsAt: room.endsAt,
      serverTime: this.srv.clock.now(),
      online: Math.max(this.srv.online, 1),
      history: history.map(toPublic),
      resume: token,
      maxLength: config.MAX_MESSAGE_LENGTH,
    });
    if (rec.challengePending) this.sendChallenge(true);

    // Invisible background verification: adjusts trust, never blocks entry.
    if (verifier.provider === "turnstile") {
      if (!verification) this.signal("verificationMissing");
      else
        void verifier.verify(verification).then((ok) => {
          if (ok) this.score.add(-15, this.srv.clock.now());
          else this.signal("verificationFailed");
        });
    }
    this.persistSoon();
  }

  /** Midnight: replace identity with a fresh, unlinked one for the new room. */
  async rollover(next: DailyRoom) {
    if (!this.joined || !this.rec || this.closed) return;
    const now = this.srv.clock.now();
    const { rec, token } = await this.srv.sessions.create(next, {
      ipHash: this.info.net.dailyHash,
      country: this.info.country,
      now,
      carry: { suspicion: this.score.value(now), challengePending: this.rec.challengePending },
    });
    if (this.closed) return;
    this.rec = rec;
    this.resumeToken = token;
    this.rolledAt = now;
    this.replies.clear();
    this.recentOwn = [];
    this.send({ type: "reset", day: next.day, dayEndsAt: next.endsAt, identity: { name: rec.name, country: rec.country }, resume: token });
    if (rec.challengePending) this.sendChallenge(true);
  }

  // ————— sending —————

  private async handleSend(e: SendEvent) {
    const t0 = performance.now();
    if (!this.joined || !this.rec) {
      this.signal("sendBeforeJoin");
      this.send({ type: "reject", clientId: e.clientId, reason: "invalid" });
      return;
    }
    // Idempotent per clientId: replays get the original answer, never a duplicate message.
    const prior = this.replies.get(e.clientId);
    if (prior) return this.send(prior);

    const reply = await this.decide(e);
    this.replies.set(e.clientId, reply);
    if (this.replies.size > 64) this.replies.delete(this.replies.keys().next().value!);
    this.send(reply);
    if (reply.type === "ack") this.srv.metrics.ackLatency.observe((performance.now() - t0) / 1000);
  }

  private reject(clientId: string, reason: Reject["reason"], retryAfterMs?: number): Reject {
    this.srv.metrics.messages.inc({ result: reason });
    return retryAfterMs ? { type: "reject", clientId, reason, retryAfterMs } : { type: "reject", clientId, reason };
  }

  private async decide(e: SendEvent): Promise<ServerEvent> {
    const { store, bus, config, metrics } = this.srv;
    const now = this.srv.clock.now();
    const rec = this.rec!;

    // 1. Day boundary. A message always belongs to the room of its server timestamp.
    if (roomAt(now, config.ROOM_TIMEZONE, config.ROOM_EPOCH_DATE).roomId !== this.srv.room.roomId) {
      this.srv.checkRoom();
      return this.reject(e.clientId, "stale_room");
    }
    if (e.day !== undefined && e.day !== this.srv.room.day) return this.reject(e.clientId, "stale_room");
    if (e.day === undefined && now - this.rolledAt < STALE_ROOM_GRACE_MS) return this.reject(e.clientId, "stale_room");
    if (rec.roomId !== this.srv.room.roomId) return this.reject(e.clientId, "stale_room"); // rollover in progress

    // 2. Enforcement state.
    if (this.blockedUntil > now) return this.reject(e.clientId, "blocked", this.blockedUntil - now);
    if (rec.challengePending) {
      this.sendChallenge();
      return this.reject(e.clientId, "challenge_required");
    }

    // 3. Content.
    const v = normalizeText(e.text, { maxLength: config.MAX_MESSAGE_LENGTH, maxLines: config.MAX_MESSAGE_LINES });
    if (!v.ok) return this.reject(e.clientId, v.reason);
    const text = v.text;

    // 4. Adaptive per-session rate limit with progressive cooldown.
    if (now < this.cooldownUntil) {
      metrics.rateLimited.inc({ scope: "cooldown" });
      if (now - this.lastIgnoredSignal >= 250) {
        this.lastIgnoredSignal = now;
        this.signal("ignoredCooldown");
        const escalated = await this.escalate(now);
        if (escalated) return this.reject(e.clientId, escalated);
      }
      return this.reject(e.clientId, "rate_limited", this.cooldownUntil - now);
    }
    const wait = this.bucket.take(now);
    if (wait > 0) {
      if (now - this.lastStrikeAt > 60_000) this.strikes = 0;
      this.strikes++;
      this.lastStrikeAt = now;
      this.cooldownUntil = now + Math.max(wait, violationCooldownMs(this.strikes));
      metrics.rateLimited.inc({ scope: "session" });
      this.signal("rateLimitHit");
      const escalated = await this.escalate(now);
      if (escalated) return this.reject(e.clientId, escalated);
      return this.reject(e.clientId, "rate_limited", this.cooldownUntil - now);
    }

    // 5. Network-level safeguard (many sessions from one network). Generous for shared NATs.
    const netCount = await store.hit(`ipmsg:${this.info.net.dailyHash}`, NETWORK_MSG_WINDOW_MS).catch(() => 0);
    if (netCount > NETWORK_MSG_LIMIT) {
      metrics.rateLimited.inc({ scope: "network" });
      this.signal("networkFlood");
      return this.reject(e.clientId, "rate_limited", 5_000);
    }

    // 6. Behavioral signals (each weak on its own).
    if (this.timing.record(now)) this.signal("machineRegularTiming");
    if (now - this.joinedAt < 400) this.signal("instantAfterJoin");
    const fp = fingerprint(text);
    this.recentOwn = this.recentOwn.filter((m) => now - m.t < 60_000);
    if (this.recentOwn.some((m) => m.fp === fp)) this.signal("ownDuplicate");
    this.recentOwn.push({ fp, t: now });
    if (this.recentOwn.length > 8) this.recentOwn.shift();
    if ([...fp].length >= CROSS_DUP_MIN_CHARS) {
      const h = createHash("sha256").update(fp).digest("base64url").slice(0, 16);
      const senders = await store.addToSet(`dup:${rec.roomId}:${h}`, rec.sid, 30_000).catch(() => 0);
      if (senders >= CROSS_DUP_SESSIONS) this.signal("crossSessionDuplicate");
    }

    // 7. Escalation: soft throttle (already applied via tier) → challenge → block.
    const escalated = await this.escalate(now);
    if (escalated) return this.reject(e.clientId, escalated);

    // 8. Accept: persist to today's room, fan out cluster-wide, ack the sender.
    const message: ChatMessage = { id: newMessageId(now), author: rec.name, text, ts: now, country: rec.country };
    try {
      const room = this.srv.room;
      await store.appendMessage(room.roomId, { ...message, sid: rec.sid }, config.HISTORY_LIMIT, room.endsAt);
      await bus.publish({ kind: "message", origin: this.srv.instanceId, roomId: room.roomId, message, exclude: this.key });
    } catch (err) {
      metrics.errors.inc({ kind: "store" });
      this.srv.log.error({ err }, "failed to persist/publish message");
      return this.reject(e.clientId, "invalid");
    }
    metrics.messages.inc({ result: "accepted" });
    return { type: "ack", clientId: e.clientId, message };
  }

  /** Returns a reject reason if the session must stop sending now. */
  private async escalate(now: number): Promise<Reject["reason"] | null> {
    const v = this.score.value(now);
    if (v < THRESHOLDS.challenge) return null;
    if (this.trusted(now) && v < THRESHOLDS.severe) return null;
    if (this.srv.verifier.provider) {
      await this.startChallenge(v);
      return "challenge_required";
    }
    // No challenge provider: rely on the restricted tier, and block only for severe abuse.
    if (v >= THRESHOLDS.severe) {
      await this.tempBlock("severe_automation");
      return "blocked";
    }
    return null;
  }

  // ————— challenge —————

  private async startChallenge(score: number) {
    const rec = this.rec!;
    if (rec.challengePending) return;
    rec.challengePending = true;
    this.srv.metrics.challenges.inc({ result: "issued" });
    this.srv.log.info({ session: logTag(rec.sid), score: Math.round(score) }, "challenge issued");
    void this.srv.store
      .addAbuseEvent({ type: "challenge_issued", session: logTag(rec.sid), score: Math.round(score) }, EVENT_RETENTION_MS)
      .catch(() => {});
    await this.persist();
    this.sendChallenge(true);
  }

  private sendChallenge(force = false) {
    const { provider, siteKey } = this.srv.verifier;
    const now = this.srv.clock.now();
    if (!provider || (!force && now - this.lastChallengeSent < 5_000)) return;
    this.lastChallengeSent = now;
    this.send(siteKey ? { type: "challenge", provider, siteKey } : { type: "challenge", provider });
  }

  private async challengeResponse(token: string) {
    const rec = this.rec;
    if (!rec?.challengePending) return;
    const now = this.srv.clock.now();
    if (now - this.lastChallengeAttempt < 1_000) return;
    this.lastChallengeAttempt = now;

    const ok = await this.srv.verifier.verify(token);
    if (this.closed) return;
    if (ok) {
      rec.challengePending = false;
      rec.challengeFails = 0;
      rec.trustedUntil = now + TRUST_WINDOW_MS;
      this.score.reset(now);
      this.strikes = 0;
      this.cooldownUntil = 0;
      this.applyTier(now);
      this.bucket = new TokenBucket(this.tier.capacity, this.tier.refillPerSec, now);
      this.srv.metrics.challenges.inc({ result: "passed" });
      await this.persist();
      this.send({ type: "challenge_result", ok: true });
      return;
    }
    rec.challengeFails++;
    this.srv.metrics.challenges.inc({ result: "failed" });
    this.send({ type: "challenge_result", ok: false });
    if (rec.challengeFails >= 3) await this.tempBlock("challenge_failed");
    else await this.persist();
  }

  // ————— enforcement —————

  private async tempBlock(reason: string) {
    const rec = this.rec!;
    const now = this.srv.clock.now();
    rec.blocks++;
    const room = this.srv.room;
    const ttl = rec.blocks === 1 ? 10 * 60_000 : rec.blocks === 2 ? 60 * 60_000 : Math.max(60_000, room.endsAt - now);
    const { store, metrics } = this.srv;
    await store.setBlock(`sess:${rec.sid}`, ttl, reason).catch(() => {});
    metrics.blocks.inc({ scope: "session" });
    // Network-level block only for repeated or severe cases, and always temporary (shared NATs exist).
    if (rec.blocks >= 2 || this.score.value(now) >= THRESHOLDS.severe) {
      const netTtl = Math.min(24 * 3_600_000, 15 * 60_000 * rec.blocks);
      await store.setBlock(`net:${this.info.net.blockHash}`, netTtl, reason).catch(() => {});
      metrics.blocks.inc({ scope: "network" });
    }
    void store
      .addAbuseEvent({ type: "block", reason, session: logTag(rec.sid), ttlMs: ttl, blocks: rec.blocks }, EVENT_RETENTION_MS)
      .catch(() => {});
    this.srv.log.warn({ session: logTag(rec.sid), reason, ttlMs: ttl }, "session blocked");
    await this.persist();
    this.blocked(ttl);
  }

  /** Tell the client and disconnect with a no-reconnect code. */
  blocked(ttlMs: number, message?: string) {
    this.blockedUntil = this.srv.clock.now() + ttlMs;
    const minutes = Math.max(1, Math.ceil(ttlMs / 60_000));
    this.send({
      type: "error",
      code: "blocked",
      message: message ?? `You’ve been paused for about ${minutes} minute${minutes === 1 ? "" : "s"}.`,
    });
    this.close(Close.BLOCKED, "blocked");
  }

  // ————— misc events —————

  private ping(t: number) {
    const now = this.srv.clock.now();
    if (now - this.pingWindow.start > 1_000) this.pingWindow = { start: now, n: 0 };
    if (++this.pingWindow.n === 6) this.signal("pingFlood");
    if (this.pingWindow.n > 6) return;
    this.send({ type: "pong", t });
  }

  private async report(messageId: string, reason: string) {
    const rec = this.rec;
    if (!rec) return;
    const { store, config, metrics } = this.srv;
    const n = await store.hit(`report:${rec.sid}`, 3_600_000).catch(() => 0);
    if (n > 20) return this.send({ type: "error", code: "rate_limited", message: "Too many reports." });
    const recent = await store.recentMessages(this.srv.room.roomId, config.HISTORY_LIMIT).catch(() => []);
    const target = recent.find((m) => m.id === messageId);
    if (!target) return this.send({ type: "error", code: "not_found", message: "That message is no longer available." });
    // Reports keep a snapshot (needed for review/legal requests) with bounded retention.
    await store.addAbuseEvent(
      {
        type: "report",
        reason,
        roomId: this.srv.room.roomId,
        messageId,
        author: target.author,
        text: target.text,
        reportedSid: target.sid,
        reporter: logTag(rec.sid),
      },
      REPORT_RETENTION_MS,
    );
    metrics.reports.inc();
    this.send({ type: "reported", messageId });
  }

  // ————— persistence —————

  private persistSoon() {
    if (!this.rec || this.persistTimer) return;
    this.persistTimer = setTimeout(() => {
      this.persistTimer = undefined;
      void this.persist();
    }, 5_000);
    this.persistTimer.unref();
  }

  private async persist() {
    const rec = this.rec;
    if (!rec || rec.roomId !== this.srv.room.roomId) return;
    const now = this.srv.clock.now();
    Object.assign(rec, this.score.snapshot(now));
    await this.srv.sessions.save(rec, this.srv.room).catch(() => {});
  }
}

export const toPublic = (m: StoredMessage): ChatMessage => ({
  id: m.id,
  author: m.author,
  text: m.text,
  ts: m.ts,
  country: m.country ?? null,
});
