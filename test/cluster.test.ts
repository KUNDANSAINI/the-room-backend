import { randomBytes } from "node:crypto";
import { Redis } from "ioredis";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { MemoryBus, MemoryStore } from "../src/store/memory.js";
import { RedisBus, RedisStore } from "../src/store/redis.js";
import type { Backplane, StateStore } from "../src/store/types.js";
import { cid, sleep, startApp, TestClient, type Started } from "./helpers.js";

const REDIS_URL = process.env.TEST_REDIS_URL ?? "redis://127.0.0.1:6379/15";

async function redisAvailable() {
  const r = new Redis(REDIS_URL, { lazyConnect: true, maxRetriesPerRequest: 0, retryStrategy: () => null });
  try {
    await r.connect();
    return (await r.ping()) === "PONG";
  } catch {
    return false;
  } finally {
    r.disconnect();
  }
}

interface Backend {
  name: string;
  make(): { store: StateStore; bus: Backplane };
  cleanup(): Promise<void>;
}

const memoryBackend = (): Backend => {
  const store = new MemoryStore();
  const hub = new MemoryBus();
  return { name: "memory", make: () => ({ store, bus: hub.connect() }), cleanup: async () => {} };
};

const redisBackend = (): Backend => {
  const prefix = `test:${randomBytes(4).toString("hex")}:`;
  const conns: Redis[] = [];
  const conn = () => {
    const r = new Redis(REDIS_URL, { maxRetriesPerRequest: 1 });
    conns.push(r);
    return r;
  };
  return {
    name: "redis",
    make: () => ({ store: new RedisStore(conn(), prefix), bus: new RedisBus(conn(), conn(), prefix) }),
    cleanup: async () => {
      const r = conn();
      const keys = await r.keys(`${prefix}*`);
      if (keys.length) await r.del(...keys);
      conns.forEach((c) => c.disconnect());
    },
  };
};

const hasRedis = await redisAvailable();

for (const [label, factory, enabled] of [
  ["memory", memoryBackend, true],
  ["redis", redisBackend, hasRedis],
] as const) {
  describe.runIf(enabled)(`multi-instance (${label})`, () => {
    let backend: Backend;
    let a: Started;
    let b: Started;
    const clients: TestClient[] = [];

    beforeAll(() => {
      backend = factory();
    });
    afterAll(async () => {
      await backend.cleanup();
    });

    const cluster = async () => {
      const ia = backend.make();
      const ib = backend.make();
      a = await startApp({ ...ia, instanceId: "i-a", env: { PRESENCE_MS: "100" } });
      b = await startApp({ ...ib, instanceId: "i-b", env: { PRESENCE_MS: "100" } });
    };
    const connect = async (s: Started) => {
      const c = await TestClient.connect(s.url);
      clients.push(c);
      return c;
    };

    afterEach(async () => {
      clients.splice(0).forEach((c) => c.ws.terminate());
      await Promise.all([a?.app.stop(0), b?.app.stop(0)]);
    });

    it("propagates messages across instances exactly once", async () => {
      await cluster();
      const onA = await connect(a);
      const onB = await connect(b);
      const alsoB = await connect(b);
      await onA.join();
      await onB.join();
      await alsoB.join();
      onA.send({ type: "send", clientId: cid(), text: "hello from instance A" });
      const ack = await onA.next("ack");
      expect((await onB.next("message")).message.id).toBe(ack.message.id);
      expect((await alsoB.next("message")).message.id).toBe(ack.message.id);
      expect(await onB.receives("message", 250)).toBe(false);
      expect(await onA.receives("message", 100)).toBe(false);
    });

    it("shares history, identity resume and presence across instances", async () => {
      await cluster();
      const onA = await connect(a);
      const w = await onA.join();
      onA.send({ type: "send", clientId: cid(), text: "persisted once, visible everywhere" });
      await onA.next("ack");
      onA.close();

      // Reconnect lands on the other instance: same identity, same history.
      const onB = await connect(b);
      const wb = await onB.join(w.resume);
      expect(wb.identity.name).toBe(w.identity.name);
      expect(wb.history.at(-1).text).toBe("persisted once, visible everywhere");

      const other = await connect(a);
      await other.join();
      const p = await onB.next("presence", 5000, (f) => f.online === 2);
      expect(p.online).toBe(2);
    });

    it("keeps public names unique cluster-wide and expires everything at midnight", async () => {
      await cluster();
      const names = new Set<string>();
      for (let i = 0; i < 10; i++) {
        const c = await connect(i % 2 ? a : b);
        names.add((await c.join()).identity.name);
      }
      expect(names.size).toBe(10);
      const room = a.app.server.room;
      const [name] = names;
      const owner = await a.store.nameOwner(room.roomId, name);
      expect(owner).toBeTruthy();
      const sess = await a.store.getSession(owner!);
      expect(sess?.roomId).toBe(room.roomId);
      expect(sess).not.toHaveProperty("ip"); // only hashed network ids
    });

    it("applies blocks cluster-wide", async () => {
      await cluster();
      const onA = await connect(a);
      const w = await onA.join();
      const sid = await a.store.nameOwner(a.app.server.room.roomId, w.identity.name);
      await a.store.setBlock(`sess:${sid}`, 60_000, "test");
      await a.app.server.bus.publish({ kind: "kick", origin: "test", sid: sid!, reason: "test" });
      expect((await onA.closed).code).toBe(4403);
      const onB = await connect(b);
      onB.send({ type: "join", resume: w.resume });
      expect((await onB.next("error")).code).toBe("blocked");
    });
  });
}

describe.runIf(hasRedis)("redis TTL semantics", () => {
  it("stores day-scoped keys with expiry at the end of the day", async () => {
    const prefix = `test:${randomBytes(4).toString("hex")}:`;
    const r = new Redis(REDIS_URL);
    const store = new RedisStore(r, prefix);
    const endsAt = Date.now() + 5_000;
    await store.claimName("2026-09-11", "BlueTiger_2841", "sid1", endsAt);
    await store.appendMessage("2026-09-11", { id: "m1", author: "BlueTiger_2841", text: "x", ts: 1, sid: "sid1" }, 300, endsAt);
    const nameTtl = await r.pttl(`${prefix}name:2026-09-11:BlueTiger_2841`);
    const msgTtl = await r.pttl(`${prefix}msgs:2026-09-11`);
    expect(nameTtl).toBeGreaterThan(0);
    expect(nameTtl).toBeLessThanOrEqual(5_000);
    expect(msgTtl).toBeGreaterThan(5_000); // grace window for late cleanup
    expect(msgTtl).toBeLessThanOrEqual(5_000 + 10 * 60_000);
    expect(await store.claimName("2026-09-11", "BlueTiger_2841", "sid2", endsAt)).toBe(false);
    await store.deleteRoom("2026-09-11");
    expect(await store.recentMessages("2026-09-11", 10)).toEqual([]);
    await r.del(...(await r.keys(`${prefix}*`)));
    r.disconnect();
    await sleep(0);
  });
});
