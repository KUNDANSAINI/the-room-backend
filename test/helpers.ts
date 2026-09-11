import WebSocket from "ws";
import { createApp, type App } from "../src/app.js";
import { MockVerifier, type ChallengeVerifier } from "../src/abuse/challenge.js";
import { testConfig, type Config } from "../src/config.js";
import type { Clock } from "../src/room/clock.js";
import { MemoryBus, MemoryStore } from "../src/store/memory.js";
import type { Backplane, StateStore } from "../src/store/types.js";

export const ORIGIN = "http://localhost:3000";
export const UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/140 Safari/537.36";

export interface Started {
  app: App;
  port: number;
  url: string;
  http: string;
  store: StateStore;
}

export async function startApp(
  opts: {
    env?: Partial<Record<keyof Config, string>>;
    store?: StateStore;
    bus?: Backplane;
    clock?: Clock;
    verifier?: ChallengeVerifier;
    instanceId?: string;
  } = {},
): Promise<Started> {
  const config = testConfig({ SHUTDOWN_DRAIN_MS: "0", HOST: "127.0.0.1", ...opts.env });
  const store = opts.store ?? new MemoryStore(opts.clock ? () => opts.clock!.now() : Date.now);
  const bus = opts.bus ?? new MemoryBus().connect();
  const app = createApp(config, {
    store,
    bus,
    clock: opts.clock ?? { now: () => Date.now() },
    verifier: opts.verifier ?? new MockVerifier(),
    instanceId: opts.instanceId,
    collectDefaultMetrics: false,
  });
  const port = await app.start();
  return { app, port, url: `ws://127.0.0.1:${port}/ws`, http: `http://127.0.0.1:${port}`, store };
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type Frame = any;

export class TestClient {
  readonly frames: Frame[] = [];
  private waiters: Array<() => void> = [];
  readonly closed: Promise<{ code: number; reason: string }>;

  private constructor(readonly ws: WebSocket) {
    ws.on("message", (data, isBinary) => {
      if (!isBinary) this.frames.push(JSON.parse(data.toString()));
      this.waiters.splice(0).forEach((w) => w());
    });
    this.closed = new Promise((resolve) => ws.on("close", (code, reason) => resolve({ code, reason: reason.toString() })));
  }

  static connect(
    url: string,
    opts: { origin?: string | null; ua?: string; headers?: Record<string, string>; autoPong?: boolean } = {},
  ): Promise<TestClient> {
    const headers: Record<string, string> = { "user-agent": opts.ua ?? UA, ...opts.headers };
    if (opts.origin !== null) headers.origin = opts.origin ?? ORIGIN;
    const ws = new WebSocket(url, { headers, autoPong: opts.autoPong ?? true });
    const client = new TestClient(ws);
    return new Promise((resolve, reject) => {
      ws.once("open", () => resolve(client));
      ws.once("unexpected-response", (_req, res) => reject(Object.assign(new Error(`HTTP ${res.statusCode}`), { status: res.statusCode })));
      ws.once("error", reject);
    });
  }

  send(obj: unknown) {
    this.ws.send(JSON.stringify(obj));
  }

  sendRaw(data: string | Buffer, binary = false) {
    this.ws.send(data, { binary });
  }

  /** Resolves with (and consumes) the first frame of `type`, waiting if needed. */
  async next(type: string, timeoutMs = 3000, where: (f: Frame) => boolean = () => true): Promise<Frame> {
    const deadline = Date.now() + timeoutMs;
    for (;;) {
      const i = this.frames.findIndex((f) => f.type === type && where(f));
      if (i >= 0) return this.frames.splice(i, 1)[0];
      const left = deadline - Date.now();
      if (left <= 0) throw new Error(`timeout waiting for "${type}"; have: ${this.frames.map((f) => f.type).join(",")}`);
      await new Promise<void>((resolve) => {
        const t = setTimeout(resolve, left);
        this.waiters.push(() => {
          clearTimeout(t);
          resolve();
        });
      });
    }
  }

  /** True if a frame of `type` arrives within `ms`. */
  async receives(type: string, ms = 300, where?: (f: Frame) => boolean) {
    try {
      await this.next(type, ms, where);
      return true;
    } catch {
      return false;
    }
  }

  async join(resume?: string | null) {
    this.send({ type: "join", resume: resume ?? null });
    return this.next("welcome");
  }

  close() {
    this.ws.close();
  }
}

let n = 0;
export const cid = () => `c${Date.now().toString(36)}${(n++).toString(36)}`;
export const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
