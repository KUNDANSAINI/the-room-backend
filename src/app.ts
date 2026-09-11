import { randomBytes } from "node:crypto";
import { MockVerifier, NoChallenge, TurnstileVerifier, type ChallengeVerifier } from "./abuse/challenge.js";
import type { Config } from "./config.js";
import { RoomServer } from "./gateway/room-server.js";
import { createLogger } from "./observability/logger.js";
import { createMetrics } from "./observability/metrics.js";
import { SyncedClock, type Clock } from "./room/clock.js";
import { MemoryBus, MemoryStore } from "./store/memory.js";
import { RedisBus, RedisStore, createRedis } from "./store/redis.js";
import type { Backplane, StateStore } from "./store/types.js";

export interface AppOverrides {
  store?: StateStore;
  bus?: Backplane;
  clock?: Clock;
  verifier?: ChallengeVerifier;
  instanceId?: string;
  collectDefaultMetrics?: boolean;
}

export interface App {
  server: RoomServer;
  start(): Promise<number>;
  stop(drainMs?: number): Promise<void>;
}

export function createVerifier(config: Config): ChallengeVerifier {
  switch (config.CHALLENGE_PROVIDER) {
    case "turnstile":
      return new TurnstileVerifier(config.TURNSTILE_SECRET, config.TURNSTILE_SITE_KEY);
    case "mock":
      return new MockVerifier();
    default:
      return new NoChallenge();
  }
}

/** Wires config → store/backplane/clock → RoomServer. Tests inject their own dependencies. */
export function createApp(config: Config, o: AppOverrides = {}): App {
  const instanceId = o.instanceId ?? config.INSTANCE_ID ?? `i-${randomBytes(4).toString("hex")}`;
  const log = createLogger(config.LOG_LEVEL, instanceId);
  const metrics = createMetrics(instanceId, o.collectDefaultMetrics ?? true);

  let store = o.store;
  let bus = o.bus;
  if (!store || !bus) {
    if (config.REDIS_URL) {
      const main = createRedis(config.REDIS_URL, `room-${instanceId}`);
      main.on("error", (err) => log.error({ err: err.message }, "redis error"));
      const sub = main.duplicate();
      sub.on("error", (err) => log.error({ err: err.message }, "redis sub error"));
      store ??= new RedisStore(main, config.REDIS_PREFIX);
      bus ??= new RedisBus(main.duplicate(), sub, config.REDIS_PREFIX);
    } else {
      log.warn("REDIS_URL not set: using in-memory state (single instance only)");
      store ??= new MemoryStore();
      bus ??= new MemoryBus().connect();
    }
  }

  const s = store;
  const clock =
    o.clock ??
    new SyncedClock(
      () => s.time(),
      (offset) => {
        if (Math.abs(offset) > 2_000) log.warn({ offsetMs: Math.round(offset) }, "host clock drift vs store clock");
      },
    );

  const server = new RoomServer({
    config,
    store,
    bus,
    clock,
    verifier: o.verifier ?? createVerifier(config),
    log,
    metrics,
    instanceId,
  });

  return {
    server,
    async start() {
      if (clock instanceof SyncedClock) {
        await clock.start();
        server.recomputeRoom(); // align to the store clock before accepting anyone
      }
      return server.start();
    },
    async stop(drainMs) {
      await server.stop(drainMs);
      if (clock instanceof SyncedClock) clock.stop();
      if (!o.store) await s.close();
    },
  };
}
