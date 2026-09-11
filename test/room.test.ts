import WebSocket from "ws";
import { afterEach, describe, expect, it } from "vitest";
import { NoChallenge } from "../src/abuse/challenge.js";
import { ManualClock } from "../src/room/clock.js";
import { MemoryBus, MemoryStore } from "../src/store/memory.js";
import { cid, ORIGIN, sleep, startApp, TestClient, type Started } from "./helpers.js";

const started: Started[] = [];
const clients: TestClient[] = [];

async function boot(opts: Parameters<typeof startApp>[0] = {}) {
  const s = await startApp(opts);
  started.push(s);
  return s;
}
async function client(url: string, opts?: Parameters<typeof TestClient.connect>[1]) {
  const c = await TestClient.connect(url, opts);
  clients.push(c);
  return c;
}

afterEach(async () => {
  clients.splice(0).forEach((c) => c.ws.terminate());
  await Promise.all(started.splice(0).map((s) => s.app.stop(0)));
});

describe("anonymous sessions", () => {
  it("joins without any account and receives a server-generated daily identity", async () => {
    const s = await boot();
    const c = await client(s.url);
    const w = await c.join();
    expect(w.identity.name).toMatch(/^[A-Z][a-z]+[A-Z][a-z]+_\d{4}$/);
    expect(w.day).toBeGreaterThan(0);
    expect(w.dayEndsAt).toBeGreaterThan(Date.now());
    expect(typeof w.resume).toBe("string");
    expect(w.history).toEqual([]);
    expect(w.maxLength).toBe(500);
  });

  it("never exposes the internal session id", async () => {
    const s = await boot();
    const a = await client(s.url);
    const b = await client(s.url);
    const wa = await a.join();
    await b.join();
    a.send({ type: "send", clientId: cid(), text: "hello" });
    await a.next("ack");
    await b.next("message");
    const sid = await s.store.nameOwner(s.app.server.room.roomId, wa.identity.name);
    expect(sid).toBeTruthy();
    const everything = JSON.stringify([...a.frames, ...b.frames, wa]);
    expect(everything).not.toContain(sid!);
  });

  it("resumes the same identity within the day and issues a new one for unknown tokens", async () => {
    const s = await boot();
    const a = await client(s.url);
    const w1 = await a.join();
    a.close();
    const b = await client(s.url);
    const w2 = await b.join(w1.resume);
    expect(w2.identity.name).toBe(w1.identity.name);
    const c = await client(s.url);
    const w3 = await c.join("forged-or-expired-token");
    expect(w3.identity.name).not.toBe(w1.identity.name);
  });

  it("rejects sends before join", async () => {
    const s = await boot();
    const c = await client(s.url);
    c.send({ type: "send", clientId: "x1", text: "hi" });
    const r = await c.next("reject");
    expect(r.reason).toBe("invalid");
  });
});

describe("realtime messaging", () => {
  it("broadcasts accepted messages to everyone else and acks the sender", async () => {
    const s = await boot();
    const [a, b, c] = await Promise.all([client(s.url), client(s.url), client(s.url)]);
    const wa = await a.join();
    await b.join();
    await c.join();
    a.send({ type: "send", clientId: "m1", text: "  anyone here from India?  " });
    const ack = await a.next("ack");
    expect(ack.message.text).toBe("anyone here from India?");
    expect(ack.message.author).toBe(wa.identity.name);
    const mb = await b.next("message");
    const mc = await c.next("message");
    expect(mb.message).toEqual(ack.message);
    expect(mc.message.id).toBe(ack.message.id);
    expect(await a.receives("message", 200)).toBe(false); // sender gets the ack only
  });

  it("includes today's history for late joiners", async () => {
    const s = await boot();
    const a = await client(s.url);
    await a.join();
    for (const t of ["one", "two", "three"]) {
      a.send({ type: "send", clientId: cid(), text: t });
      await a.next("ack");
    }
    const b = await client(s.url);
    const w = await b.join();
    expect(w.history.map((m: { text: string }) => m.text)).toEqual(["one", "two", "three"]);
  });

  it("is idempotent per clientId (replayed sends don't duplicate)", async () => {
    const s = await boot();
    const a = await client(s.url);
    const b = await client(s.url);
    await a.join();
    await b.join();
    a.send({ type: "send", clientId: "same", text: "once" });
    a.send({ type: "send", clientId: "same", text: "once" });
    const ack1 = await a.next("ack");
    const ack2 = await a.next("ack");
    expect(ack2.message.id).toBe(ack1.message.id);
    await b.next("message");
    expect(await b.receives("message", 250)).toBe(false);
  });

  it("rejects forged fields: clients cannot choose their name, id or timestamp", async () => {
    const s = await boot();
    const a = await client(s.url);
    await a.join();
    a.send({ type: "send", clientId: "f1", text: "hi", author: "Admin_0001", ts: 1 });
    const err = await a.next("error");
    expect(err.code).toBe("bad_request");
    expect(await a.receives("ack", 200)).toBe(false);
  });

  it("validates content server-side", async () => {
    const s = await boot();
    const a = await client(s.url);
    await a.join();
    a.send({ type: "send", clientId: "v1", text: "x".repeat(501) });
    expect((await a.next("reject")).reason).toBe("too_long");
    a.send({ type: "send", clientId: "v2", text: "   \n  " });
    expect((await a.next("reject")).reason).toBe("invalid");
    a.send({ type: "send", clientId: "v3", text: "<img src=x onerror=alert(1)>" });
    expect((await a.next("ack")).message.text).toBe("<img src=x onerror=alert(1)>"); // literal text, JSON-encoded
  });
});

describe("malformed & hostile frames", () => {
  it("answers malformed JSON and unknown events with an error, and disconnects repeat offenders", async () => {
    const s = await boot();
    const c = await client(s.url);
    c.sendRaw("{not json");
    expect((await c.next("error")).code).toBe("bad_request");
    c.send({ type: "sudo", cmd: "rm -rf" });
    expect((await c.next("error")).code).toBe("bad_request");
    for (let i = 0; i < 4; i++) c.sendRaw("[]");
    const closed = await c.closed;
    expect(closed.code).toBe(1008);
  });

  it("rejects binary frames", async () => {
    const s = await boot();
    const c = await client(s.url);
    c.sendRaw(Buffer.from([0xde, 0xad, 0xbe, 0xef]), true);
    expect((await c.next("error")).code).toBe("bad_request");
  });

  it("closes the socket on oversized frames", async () => {
    const s = await boot();
    const c = await client(s.url);
    c.sendRaw(JSON.stringify({ type: "send", clientId: "big", text: "a".repeat(20_000) }));
    const closed = await c.closed;
    expect(closed.code).toBe(1009);
  });
});

describe("rate limiting & anti-automation", () => {
  it("lets a human burst of quick one-liners through untouched", async () => {
    const s = await boot();
    const a = await client(s.url);
    await a.join();
    for (const t of ["wait", "what", "no way", "lol", "😂", "ok ok", "brb"]) {
      a.send({ type: "send", clientId: cid(), text: t });
      await sleep(120);
    }
    for (let i = 0; i < 7; i++) await a.next("ack");
    expect(a.frames.some((f) => f.type === "reject")).toBe(false);
  });

  it("rate-limits floods with a retry hint", async () => {
    const s = await boot({ verifier: new NoChallenge() });
    const a = await client(s.url);
    await a.join();
    for (let i = 0; i < 30; i++) a.send({ type: "send", clientId: `f${i}`, text: `flood ${i}` });
    const r = await a.next("reject", 3000, (f) => f.reason === "rate_limited");
    expect(r.retryAfterMs).toBeGreaterThan(0);
    await sleep(300);
    const acks = a.frames.filter((f) => f.type === "ack").length + 0;
    expect(acks).toBeLessThanOrEqual(10);
  });

  it("escalates a flooding bot to a challenge, restores access when it passes", async () => {
    const s = await boot();
    const bot = await client(s.url);
    await bot.join();
    const flood = setInterval(() => bot.send({ type: "send", clientId: cid(), text: "buy cheap followers now" }), 20);
    try {
      const ch = await bot.next("challenge", 15_000);
      expect(ch.provider).toBe("mock");
    } finally {
      clearInterval(flood);
    }
    await sleep(100);
    bot.send({ type: "send", clientId: cid(), text: "still here?" });
    expect((await bot.next("reject", 3000, (f) => f.reason === "challenge_required")).reason).toBe("challenge_required");

    bot.send({ type: "challenge_response", token: "mock-human" });
    expect((await bot.next("challenge_result")).ok).toBe(true);
    bot.frames.length = 0;
    bot.send({ type: "send", clientId: cid(), text: "sorry, got excited" });
    expect((await bot.next("ack")).message.text).toBe("sorry, got excited");
  });

  it("temporarily blocks sessions that fail the challenge repeatedly", async () => {
    const s = await boot();
    const bot = await client(s.url);
    const w = await bot.join();
    const flood = setInterval(() => bot.send({ type: "send", clientId: cid(), text: "spam spam spam spam" }), 20);
    try {
      await bot.next("challenge", 15_000);
    } finally {
      clearInterval(flood);
    }
    for (let i = 0; i < 3; i++) {
      bot.send({ type: "challenge_response", token: "i-am-a-robot" });
      await bot.next("challenge_result", 3000, (f) => f.ok === false);
      await sleep(1_050);
    }
    const closed = await bot.closed;
    expect(closed.code).toBe(4403);
    // The block survives reconnecting with the same identity.
    const again = await client(s.url);
    again.send({ type: "join", resume: w.resume });
    expect((await again.next("error")).code).toBe("blocked");
  });

  it("carries suspicion across reconnects (can't wash it away)", async () => {
    const s = await boot();
    const bot = await client(s.url);
    const w = await bot.join();
    const flood = setInterval(() => bot.send({ type: "send", clientId: cid(), text: "zzzzzzzzzzzzzzz" }), 20);
    try {
      await bot.next("challenge", 15_000);
    } finally {
      clearInterval(flood);
    }
    bot.close();
    await sleep(100);
    const back = await client(s.url);
    await back.join(w.resume);
    expect((await back.next("challenge")).provider).toBe("mock");
  });
});

describe("connection management", () => {
  it("rejects foreign origins", async () => {
    const s = await boot({ env: { ALLOWED_ORIGINS: "https://theroom.app" } });
    await expect(TestClient.connect(s.url, { origin: "https://evil.example" })).rejects.toMatchObject({ status: 403 });
    const ok = await client(s.url, { origin: "https://theroom.app" });
    expect((await ok.join()).identity.name).toBeTruthy();
  });

  it("limits connection floods per network with exponential backoff", async () => {
    const s = await boot({ env: { CONNECT_ATTEMPTS_PER_MIN: "5" } });
    for (let i = 0; i < 5; i++) (await client(s.url)).close();
    await expect(TestClient.connect(s.url)).rejects.toMatchObject({ status: 429 });
    await expect(TestClient.connect(s.url)).rejects.toMatchObject({ status: 429 }); // now in backoff
  });

  it("caps concurrent connections per network", async () => {
    const s = await boot({ env: { MAX_CONNECTIONS_PER_IP: "3" } });
    await client(s.url);
    await client(s.url);
    await client(s.url);
    await expect(TestClient.connect(s.url)).rejects.toMatchObject({ status: 429 });
  });

  it("terminates stale connections that stop answering heartbeats", async () => {
    const s = await boot({ env: { HEARTBEAT_MS: "150" } });
    const zombie = await client(s.url, { autoPong: false });
    await zombie.join();
    const healthy = await client(s.url);
    await healthy.join();
    const closed = await zombie.closed;
    expect(closed.code).toBe(1006);
    expect(healthy.ws.readyState).toBe(WebSocket.OPEN);
    for (let i = 0; i < 20 && s.app.server.connectionCount !== 1; i++) await sleep(25);
    expect(s.app.server.connectionCount).toBe(1);
  });

  it("answers application pings", async () => {
    const s = await boot();
    const c = await client(s.url);
    await c.join();
    c.send({ type: "ping", t: 123 });
    expect((await c.next("pong")).t).toBe(123);
  });

  it("publishes approximate presence", async () => {
    const s = await boot({ env: { PRESENCE_MS: "100" } });
    const a = await client(s.url);
    await a.join();
    const b = await client(s.url);
    await b.join();
    const p = await a.next("presence", 3000, (f) => f.online === 2);
    expect(p.online).toBe(2);
  });

  it("shuts down gracefully with a reconnectable close code", async () => {
    const s = await boot();
    const c = await client(s.url);
    await c.join();
    const stopping = s.app.stop(0);
    started.splice(started.indexOf(s), 1);
    expect((await c.closed).code).toBe(1012);
    await stopping;
  });
});

describe("midnight reset", () => {
  it("ends the day atomically: new room, new identities, history gone, stale sends rejected", async () => {
    const midnight = Date.UTC(2026, 8, 12);
    const clock = new ManualClock(midnight - 5_000);
    const store = new MemoryStore(() => clock.now());
    const s = await boot({ clock, store });
    const a = await client(s.url);
    const b = await client(s.url);
    const wa = await a.join();
    const wb = await b.join();
    expect(wa.day).toBe(185);
    a.send({ type: "send", clientId: cid(), text: "last words of day 185", day: 185 });
    await a.next("ack");
    await b.next("message");

    clock.advance(6_000);
    await s.app.server.checkRoom();

    const ra = await a.next("reset");
    const rb = await b.next("reset");
    expect(ra.day).toBe(186);
    expect(ra.dayEndsAt).toBe(midnight + 86_400_000);
    expect(ra.identity.name).not.toBe(wa.identity.name);
    expect(rb.identity.name).not.toBe(wb.identity.name);
    expect(ra.resume).not.toBe(wa.resume);

    // In-flight send composed for the old day is refused, not moved into the new room.
    a.send({ type: "send", clientId: cid(), text: "sent at 23:59:59", day: 185 });
    expect((await a.next("reject")).reason).toBe("stale_room");

    // Yesterday is gone for everyone.
    const late = await client(s.url);
    const wl = await late.join(wa.resume); // yesterday's token no longer resolves
    expect(wl.day).toBe(186);
    expect(wl.history).toEqual([]);
    expect(wl.identity.name).not.toBe(wa.identity.name);
    expect(await store.recentMessages("2026-09-11", 100)).toEqual([]);

    // The new day works normally.
    a.send({ type: "send", clientId: cid(), text: "good morning", day: 186 });
    expect((await a.next("ack")).message.text).toBe("good morning");
  });

  it("coalesces bursts into one frame per socket, preserving order", async () => {
    const bus = new MemoryBus();
    const s = await boot({ bus: bus.connect() });
    const c = await client(s.url);
    await c.join();
    const other = bus.connect();
    const roomId = s.app.server.room.roomId;
    for (let i = 0; i < 3; i++) {
      void other.publish({ kind: "message", origin: "i-x", roomId, message: { id: `b${i}`, author: "EchoOwl_1234", text: `m${i}`, ts: i } });
    }
    const f = await c.next("messages");
    expect(f.messages.map((m: { id: string }) => m.id)).toEqual(["b0", "b1", "b2"]);
  });

  it("drops cross-midnight events delivered late by other instances", async () => {
    const clock = new ManualClock(Date.UTC(2026, 8, 11, 12));
    const bus = new MemoryBus();
    const s = await boot({ clock, bus: bus.connect() });
    const c = await client(s.url);
    await c.join();
    const other = bus.connect();
    await other.publish({
      kind: "message",
      origin: "i-other",
      roomId: "2026-09-10",
      message: { id: "late", author: "OldFox_1111", text: "from yesterday", ts: 0 },
    });
    expect(await c.receives("message", 250)).toBe(false);
  });
});

describe("abuse handling hooks", () => {
  const admin = { authorization: "Bearer test-admin-token-0123456789", "content-type": "application/json" };

  it("keeps admin endpoints closed unless configured, and requires the token", async () => {
    const off = await boot();
    expect((await fetch(`${off.http}/admin/reports`)).status).toBe(404);
    const on = await boot({ env: { ADMIN_TOKEN: "test-admin-token-0123456789" } });
    expect((await fetch(`${on.http}/admin/reports`)).status).toBe(401);
    expect((await fetch(`${on.http}/admin/reports`, { headers: { authorization: "Bearer nope" } })).status).toBe(401);
    expect((await fetch(`${on.http}/admin/reports`, { headers: admin })).status).toBe(200);
  });

  it("lets users report a message id and lets operators block that session", async () => {
    const s = await boot({ env: { ADMIN_TOKEN: "test-admin-token-0123456789" } });
    const offender = await client(s.url);
    const victim = await client(s.url);
    await offender.join();
    await victim.join();
    offender.send({ type: "send", clientId: cid(), text: "something seriously abusive" });
    const { message } = await victim.next("message");

    victim.send({ type: "report", messageId: message.id, reason: "harassment" });
    expect((await victim.next("reported")).messageId).toBe(message.id);

    const reports = (await (await fetch(`${s.http}/admin/reports`, { headers: admin })).json()) as {
      events: Array<{ fields: Record<string, string> }>;
    };
    const report = reports.events.find((e) => e.fields.type === "report");
    expect(report?.fields.messageId).toBe(message.id);

    const res = await fetch(`${s.http}/admin/block`, {
      method: "POST",
      headers: admin,
      body: JSON.stringify({ messageId: message.id, minutes: 30, reason: "harassment" }),
    });
    expect(res.status).toBe(200);
    expect((await offender.closed).code).toBe(4403);
  });

  it("rejects oversized or malformed admin bodies", async () => {
    const s = await boot({ env: { ADMIN_TOKEN: "test-admin-token-0123456789" } });
    const big = await fetch(`${s.http}/admin/block`, { method: "POST", headers: admin, body: "x".repeat(10_000) });
    expect(big.status).toBe(413);
    const bad = await fetch(`${s.http}/admin/block`, { method: "POST", headers: admin, body: JSON.stringify({ minutes: 5 }) });
    expect(bad.status).toBe(400);
  });
});

describe("public HTTP", () => {
  it("serves health and public stats with CORS for allowed origins", async () => {
    const s = await boot({ env: { ALLOWED_ORIGINS: ORIGIN } });
    expect((await fetch(`${s.http}/healthz`)).status).toBe(200);
    expect((await fetch(`${s.http}/readyz`)).status).toBe(200);
    const res = await fetch(`${s.http}/stats`, { headers: { origin: ORIGIN } });
    expect(res.headers.get("access-control-allow-origin")).toBe(ORIGIN);
    const body = await res.json();
    expect(body).toMatchObject({ day: expect.any(Number), online: expect.any(Number), dayEndsAt: expect.any(Number) });
    const foreign = await fetch(`${s.http}/stats`, { headers: { origin: "https://evil.example" } });
    expect(foreign.headers.get("access-control-allow-origin")).toBeNull();
  });

  it("exposes Prometheus metrics", async () => {
    const s = await boot();
    const text = await (await fetch(`${s.http}/metrics`)).text();
    expect(text).toContain("room_ws_connections");
    expect(text).toContain("room_messages_total");
  });
});
