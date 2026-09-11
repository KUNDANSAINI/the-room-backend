import { createServer, STATUS_CODES, type IncomingMessage, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import type { Duplex } from "node:stream";
import { WebSocketServer, type WebSocket } from "ws";
import type { ChallengeVerifier } from "../abuse/challenge.js";
import { looksAutomatedUserAgent } from "../abuse/suspicion.js";
import type { Config } from "../config.js";
import { handleHttp } from "../http/routes.js";
import { SessionService } from "../identity/sessions.js";
import { clientIp, networkIdentity } from "../net/ip.js";
import type { Logger } from "../observability/logger.js";
import type { Metrics } from "../observability/metrics.js";
import { Close, MAX_FRAME_BYTES, type ChatMessage, type ServerEvent } from "../protocol.js";
import type { Clock } from "../room/clock.js";
import { roomAt, type DailyRoom } from "../room/day.js";
import type { Backplane, BusEvent, StateStore } from "../store/types.js";
import { Connection, type AdmitInfo } from "./connection.js";

export interface RoomServerDeps {
  config: Config;
  store: StateStore;
  bus: Backplane;
  clock: Clock;
  verifier: ChallengeVerifier;
  log: Logger;
  metrics: Metrics;
  instanceId: string;
}

type Admission = { ok: true; info: AdmitInfo } | { ok: false; status: number; reason: string };

const PRESENCE_STALE_MS = 20_000;
const RESET_BATCH = 250;

export class RoomServer {
  readonly config: Config;
  readonly store: StateStore;
  readonly bus: Backplane;
  readonly clock: Clock;
  readonly verifier: ChallengeVerifier;
  readonly log: Logger;
  readonly metrics: Metrics;
  readonly instanceId: string;
  readonly sessions: SessionService;
  readonly http: Server;

  room: DailyRoom;
  online = 0;
  shuttingDown = false;

  private readonly wss: WebSocketServer;
  private readonly conns = new Set<Connection>();
  private readonly perNetwork = new Map<string, number>();
  private joinedCount = 0;
  private timers: NodeJS.Timeout[] = [];
  private rolling: Promise<void> | null = null;
  private outbox: Array<Extract<BusEvent, { kind: "message" }>> = [];
  private flushScheduled = false;

  constructor(deps: RoomServerDeps) {
    this.config = deps.config;
    this.store = deps.store;
    this.bus = deps.bus;
    this.clock = deps.clock;
    this.verifier = deps.verifier;
    this.log = deps.log;
    this.metrics = deps.metrics;
    this.instanceId = deps.instanceId;
    this.sessions = new SessionService(deps.store);
    this.room = this.computeRoom();

    this.wss = new WebSocketServer({
      noServer: true,
      maxPayload: MAX_FRAME_BYTES,
      perMessageDeflate: false, // no compression: avoids zip bombs and CPU amplification
      clientTracking: false,
    });

    this.http = createServer((req, res) => void handleHttp(this, req, res));
    this.http.headersTimeout = 10_000;
    this.http.requestTimeout = 15_000;
    this.http.keepAliveTimeout = 5_000;
    this.http.on("upgrade", (req, socket, head) => this.onUpgrade(req, socket, head));
  }

  // ————— lifecycle —————

  async start(): Promise<number> {
    await this.bus.subscribe((evt) => this.onBus(evt));
    await new Promise<void>((resolve) =>
      this.http.listen({ port: this.config.PORT, host: this.config.HOST, backlog: 4096 }, resolve),
    );
    await this.refreshPresence().catch(() => {});

    const every = (ms: number, fn: () => void) => {
      const t = setInterval(fn, ms);
      t.unref();
      this.timers.push(t);
    };
    every(this.config.HEARTBEAT_MS, () => this.heartbeat());
    every(this.config.PRESENCE_MS, () => void this.refreshPresence().catch((err) => this.log.warn({ err }, "presence refresh failed")));
    // Room boundary check every second; cheap and robust against timer drift / suspended hosts.
    every(1_000, () => this.checkRoom());

    const port = (this.http.address() as AddressInfo).port;
    this.log.info({ port, room: this.room.roomId, day: this.room.day, endsAt: new Date(this.room.endsAt).toISOString() }, "room server started");
    return port;
  }

  async stop(drainMs = this.config.SHUTDOWN_DRAIN_MS) {
    if (this.shuttingDown) return;
    this.shuttingDown = true;
    this.timers.forEach(clearInterval);
    this.timers = [];
    const httpClosed = new Promise<void>((r) => this.http.close(() => r()));
    await this.store.removePresence(this.instanceId).catch(() => {});

    // Spread disconnects so clients don't all reconnect to the survivors at the same instant.
    const list = [...this.conns];
    list.forEach((c, i) => {
      const t = setTimeout(() => c.close(Close.RESTART, "server restart"), list.length > 1 ? (drainMs * i) / list.length : 0);
      t.unref();
    });
    await new Promise((r) => setTimeout(r, drainMs + 250));
    for (const c of this.conns) c.terminate();
    this.http.closeAllConnections();
    await httpClosed;
    await this.bus.close();
    this.log.info("room server stopped");
  }

  // ————— admission —————

  private onUpgrade(req: IncomingMessage, socket: Duplex, head: Buffer) {
    socket.on("error", () => socket.destroy());
    const path = (req.url ?? "/").split("?")[0];
    if (path !== "/ws") return rejectUpgrade(socket, 404);

    this.admit(req)
      .then((verdict) => {
        if (!verdict.ok) {
          this.metrics.connectionsRejected.inc({ reason: verdict.reason });
          return rejectUpgrade(socket, verdict.status);
        }
        if (socket.destroyed) return;
        this.wss.handleUpgrade(req, socket, head, (ws) => this.accept(ws, verdict.info));
      })
      .catch((err) => {
        this.metrics.errors.inc({ kind: "admit" });
        this.log.error({ err }, "admission failed");
        rejectUpgrade(socket, 500);
      });
  }

  private async admit(req: IncomingMessage): Promise<Admission> {
    const c = this.config;
    if (this.shuttingDown) return { ok: false, status: 503, reason: "shutting_down" };

    const origin = req.headers.origin;
    if (origin && c.ALLOWED_ORIGINS.length && !c.ALLOWED_ORIGINS.includes(origin)) {
      return { ok: false, status: 403, reason: "origin" };
    }
    if (this.conns.size >= c.MAX_CONNECTIONS) return { ok: false, status: 503, reason: "capacity" };

    const net = networkIdentity(c.SERVER_SECRET, this.room.roomId, clientIp(req, c.TRUST_PROXY));
    if ((this.perNetwork.get(net.dailyHash) ?? 0) >= c.MAX_CONNECTIONS_PER_IP) {
      return { ok: false, status: 429, reason: "per_network" };
    }

    // Shared-store checks fail open (availability first); local limits above still apply.
    try {
      const [netBlock, backoff] = await Promise.all([
        this.store.blockTtl(`net:${net.blockHash}`),
        this.store.blockTtl(`cbo:${net.dailyHash}`),
      ]);
      if (netBlock > 0) return { ok: false, status: 403, reason: "blocked" };
      if (backoff > 0) return { ok: false, status: 429, reason: "backoff" };

      const attempts = await this.store.hit(`conn:${net.dailyHash}`, 60_000);
      if (attempts > c.CONNECT_ATTEMPTS_PER_MIN) {
        // Exponential backoff for reconnect storms: 2s, 4s, 8s … up to 10 minutes.
        const n = await this.store.hit(`connx:${net.dailyHash}`, 3_600_000);
        await this.store.setBlock(`cbo:${net.dailyHash}`, Math.min(600_000, 1000 * 2 ** Math.min(n, 10)), "connect_flood");
        return { ok: false, status: 429, reason: "connect_flood" };
      }
    } catch (err) {
      this.metrics.errors.inc({ kind: "store" });
      this.log.warn({ err }, "admission store check failed; failing open");
    }

    let country: string | null = null;
    if (c.EXPOSE_COUNTRY && c.TRUST_PROXY) {
      const h = req.headers["cf-ipcountry"];
      if (typeof h === "string" && /^[A-Z]{2}$/.test(h) && h !== "XX" && h !== "T1") country = h;
    }

    return {
      ok: true,
      info: {
        net,
        hasOrigin: typeof origin === "string" && origin.length > 0,
        automatedUA: looksAutomatedUserAgent(req.headers["user-agent"]),
        country,
      },
    };
  }

  private accept(ws: WebSocket, info: AdmitInfo) {
    const conn = new Connection(this, ws, info);
    this.conns.add(conn);
    this.perNetwork.set(info.net.dailyHash, (this.perNetwork.get(info.net.dailyHash) ?? 0) + 1);
    this.metrics.connections.set(this.conns.size);
  }

  onJoined(_conn: Connection) {
    this.joinedCount++;
    this.metrics.joined.set(this.joinedCount);
  }

  onClosed(conn: Connection) {
    if (!this.conns.delete(conn)) return;
    if (conn.joined) this.joinedCount--;
    const k = conn.info.net.dailyHash;
    const n = (this.perNetwork.get(k) ?? 1) - 1;
    if (n <= 0) this.perNetwork.delete(k);
    else this.perNetwork.set(k, n);
    this.metrics.connections.set(this.conns.size);
    this.metrics.joined.set(this.joinedCount);
  }

  // ————— fan-out —————

  private onBus(evt: BusEvent) {
    if (evt.kind === "message") {
      // Delayed cross-midnight events never leak into the new room.
      if (evt.roomId !== this.room.roomId) {
        this.metrics.staleDropped.inc();
        return;
      }
      this.outbox.push(evt);
      if (!this.flushScheduled) {
        this.flushScheduled = true;
        // Everything that arrives in the same event-loop turn goes out as one frame per socket.
        // Idle rooms: one message per frame, no added latency. Busy rooms: batching grows with load.
        setImmediate(() => this.flushOutbox());
      }
    } else if (evt.kind === "kick") {
      for (const c of this.conns) if (c.sid === evt.sid) c.blocked(60_000, "You’ve been removed from the room for now.");
    }
  }

  private flushOutbox() {
    this.flushScheduled = false;
    const batch = this.outbox.filter((e) => e.roomId === this.room.roomId);
    this.outbox = [];
    if (!batch.length) return;

    const encode = (messages: ChatMessage[]) =>
      messages.length === 1
        ? JSON.stringify({ type: "message", message: messages[0] } satisfies ServerEvent)
        : JSON.stringify({ type: "messages", messages } satisfies ServerEvent);

    const common = encode(batch.map((e) => e.message));
    // Senders already got an ack for their own message: give them a variant without it.
    const senders = new Set(batch.map((e) => e.exclude).filter((k): k is string => !!k));
    const variants = new Map<string, string | null>();
    let n = 0;
    for (const c of this.conns) {
      if (!c.joined || c.rec?.roomId !== this.room.roomId) continue;
      let frame: string | null = common;
      if (senders.has(c.key)) {
        if (!variants.has(c.key)) {
          const rest = batch.filter((e) => e.exclude !== c.key).map((e) => e.message);
          variants.set(c.key, rest.length ? encode(rest) : null);
        }
        frame = variants.get(c.key)!;
      }
      if (frame && c.sendRaw(frame)) n++;
    }
    this.metrics.fanout.inc(n);
  }

  broadcastLocal(evt: ServerEvent) {
    const frame = JSON.stringify(evt);
    for (const c of this.conns) if (c.joined) c.sendRaw(frame);
  }

  // ————— heartbeat & presence —————

  private heartbeat() {
    const now = this.clock.now();
    let suspicious = 0;
    for (const c of this.conns) {
      c.heartbeat(now);
      if (c.suspicion >= 30) suspicious++;
    }
    this.metrics.suspicious.set(suspicious);
  }

  async refreshPresence() {
    const now = this.clock.now();
    await this.store.reportPresence(this.instanceId, this.joinedCount, now);
    const total = await this.store.totalPresence(now, PRESENCE_STALE_MS);
    this.metrics.onlineTotal.set(total);
    if (total !== this.online) {
      this.online = total;
      this.broadcastLocal({ type: "presence", online: total });
    }
  }

  // ————— daily reset —————

  private computeRoom() {
    return roomAt(this.clock.now(), this.config.ROOM_TIMEZONE, this.config.ROOM_EPOCH_DATE);
  }

  /** Before start only: align the room to the (now synced) clock without reset side effects. */
  recomputeRoom() {
    if (this.conns.size === 0) this.room = this.computeRoom();
  }

  /** Detect the midnight boundary (on the authoritative clock) and roll over once. */
  checkRoom(): Promise<void> | null {
    if (this.rolling) return this.rolling;
    const next = this.computeRoom();
    if (next.roomId === this.room.roomId) return null;
    this.rolling = this.rollover(next).finally(() => {
      this.rolling = null;
    });
    return this.rolling;
  }

  private async rollover(next: DailyRoom) {
    const prev = this.room;
    // Switch first: from this instant, every accepted message is stamped with the new room,
    // and bus events for the old room are dropped.
    this.room = next;
    this.metrics.resets.inc();
    this.log.info({ from: prev.roomId, to: next.roomId, day: next.day, connections: this.conns.size }, "room reset");

    // Exactly one instance deletes yesterday's history; TTLs are the safety net.
    try {
      if (await this.store.acquireOnce(`reset:${prev.roomId}`, 3_600_000)) {
        await this.store.deleteRoom(prev.roomId);
        await this.store.addAbuseEvent({ type: "room_reset", from: prev.roomId, to: next.roomId }, 7 * 86_400_000);
      }
    } catch (err) {
      this.log.warn({ err }, "room cleanup failed (TTL will expire it)");
    }

    // Give every live connection a new, unlinked identity — in batches to avoid a spike.
    const live = [...this.conns].filter((c) => c.joined);
    for (let i = 0; i < live.length; i += RESET_BATCH) {
      await Promise.all(
        live.slice(i, i + RESET_BATCH).map((c) =>
          c.rollover(next).catch((err) => {
            this.metrics.errors.inc({ kind: "rollover" });
            this.log.error({ err }, "rollover failed; dropping connection so it rejoins");
            c.close(Close.TRY_LATER, "reset");
          }),
        ),
      );
      await new Promise((r) => setImmediate(r));
    }
  }

  get connectionCount() {
    return this.conns.size;
  }
}

function rejectUpgrade(socket: Duplex, status: number) {
  if (socket.destroyed) return;
  socket.write(`HTTP/1.1 ${status} ${STATUS_CODES[status] ?? ""}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n`);
  socket.destroy();
}
