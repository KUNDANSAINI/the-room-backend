/**
 * Load / abuse simulator.
 *
 *   npx tsx loadtest/run.ts --url ws://localhost:8080/ws --humans 2000 --bots 20 --flood 300 --duration 60
 *
 * Simulates, concurrently:
 *   humans  – ramped connections, natural chat (irregular gaps, occasional quick bursts, repeats)
 *   bots    – spam at a fixed machine cadence, ignoring rate limits
 *   flood   – connection flood from one network (tries to open N sockets as fast as possible)
 *
 * Each simulated human/bot uses its own X-Forwarded-For address, so run the
 * target with TRUST_PROXY=true (load-test environments only!) to model distinct
 * networks from a single load generator. The flood uses one fixed address.
 *
 * Success criteria printed at the end:
 *   - humans: ~0 rejected sends, low ack latency
 *   - bots: rate-limited quickly, challenged within seconds, blocked if they keep failing
 *   - flood: most attempts refused with 429 once the per-network limit is hit
 */
import WebSocket from "ws";

const args = Object.fromEntries(
  process.argv.slice(2).reduce<[string, string][]>((acc, a, i, all) => {
    if (a.startsWith("--")) acc.push([a.slice(2), all[i + 1] ?? "1"]);
    return acc;
  }, []),
);
const URL_ = args.url ?? "ws://127.0.0.1:8080/ws";
const HUMANS = Number(args.humans ?? 500);
const BOTS = Number(args.bots ?? 10);
const FLOOD = Number(args.flood ?? 200);
const DURATION = Number(args.duration ?? 30) * 1000;
const RAMP = Number(args.ramp ?? Math.min(20, DURATION / 3000)) * 1000;
const ORIGIN = args.origin ?? "http://localhost:3000";
const UA = "Mozilla/5.0 (loadtest) AppleWebKit/537.36 Chrome/140 Safari/537.36";

const LINES = ["hi", "hello room", "anyone from Brazil?", "lol", "same", "what time is it there", "😂", "good morning", "brb", "true", "no way", "tell me something", "i can't sleep", "ok", "haha yes"];

const stats = {
  humans: { connected: 0, failedConnect: 0, sent: 0, acked: 0, rejected: {} as Record<string, number>, challenged: 0, received: 0, latencies: [] as number[] },
  bots: { connected: 0, sent: 0, acked: 0, rateLimited: 0, challenged: 0, blocked: 0, firstChallengeMs: [] as number[] },
  flood: { attempted: 0, opened: 0, refused: {} as Record<string, number> },
};

const inc = (o: Record<string, number>, k: string) => (o[k] = (o[k] ?? 0) + 1);
const rand = (a: number, b: number) => a + Math.random() * (b - a);
const fakeIp = (i: number) => `10.${(i >> 16) & 255}.${(i >> 8) & 255}.${i & 255}`;
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
let ipSeq = 1;

function open(ip: string): Promise<WebSocket> {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(URL_, { headers: { origin: ORIGIN, "user-agent": UA, "x-forwarded-for": ip } });
    ws.once("open", () => resolve(ws));
    ws.once("unexpected-response", (_q, res) => reject(new Error(String(res.statusCode))));
    ws.once("error", (e) => reject(e));
  });
}

async function human(i: number, until: number) {
  await sleep(rand(0, RAMP));
  let ws: WebSocket;
  try {
    ws = await open(fakeIp(ipSeq++));
  } catch {
    stats.humans.failedConnect++;
    return;
  }
  stats.humans.connected++;
  const pending = new Map<string, number>();
  let day = 0;
  ws.on("message", (raw) => {
    const e = JSON.parse(raw.toString());
    if (e.type === "welcome") day = e.day;
    else if (e.type === "message") stats.humans.received++;
    else if (e.type === "messages") stats.humans.received += e.messages.length;
    else if (e.type === "ack") {
      stats.humans.acked++;
      const t = pending.get(e.clientId);
      if (t) stats.humans.latencies.push(performance.now() - t);
      pending.delete(e.clientId);
    } else if (e.type === "reject") inc(stats.humans.rejected, e.reason);
    else if (e.type === "challenge") stats.humans.challenged++;
  });
  ws.send(JSON.stringify({ type: "join", resume: null }));
  let n = 0;
  while (Date.now() < until && ws.readyState === ws.OPEN) {
    // Most people mostly read: long irregular gaps, occasional rapid bursts.
    await sleep(Math.random() < 0.15 ? rand(400, 1500) : rand(8_000, 60_000));
    if (Date.now() >= until || ws.readyState !== ws.OPEN) break;
    const id = `h${i}_${n++}`;
    pending.set(id, performance.now());
    ws.send(JSON.stringify({ type: "send", clientId: id, text: LINES[Math.floor(Math.random() * LINES.length)], day }));
    stats.humans.sent++;
  }
  ws.close();
}

async function bot(i: number, until: number) {
  let ws: WebSocket;
  try {
    ws = await open(fakeIp(200_000 + i));
  } catch {
    return;
  }
  stats.bots.connected++;
  const start = Date.now();
  let challenged = false;
  ws.on("message", (raw) => {
    const e = JSON.parse(raw.toString());
    if (e.type === "ack") stats.bots.acked++;
    if (e.type === "reject" && e.reason === "rate_limited") stats.bots.rateLimited++;
    if (e.type === "challenge" && !challenged) {
      challenged = true;
      stats.bots.challenged++;
      stats.bots.firstChallengeMs.push(Date.now() - start);
      // Bots fail challenges.
      for (let k = 0; k < 3; k++) setTimeout(() => ws.readyState === ws.OPEN && ws.send(JSON.stringify({ type: "challenge_response", token: "bot" })), 1100 * (k + 1));
    }
  });
  ws.on("close", (code) => code === 4403 && stats.bots.blocked++);
  ws.send(JSON.stringify({ type: "join", resume: null }));
  await sleep(300);
  let n = 0;
  while (Date.now() < until && ws.readyState === ws.OPEN) {
    ws.send(JSON.stringify({ type: "send", clientId: `b${i}_${n++}`, text: "BUY CHEAP FOLLOWERS at spam.example" }));
    stats.bots.sent++;
    await sleep(50); // 20 msg/s, perfectly regular
  }
  ws.terminate();
}

async function flood() {
  const ip = "10.250.250.250";
  const attempts = Array.from({ length: FLOOD }, async () => {
    stats.flood.attempted++;
    try {
      const ws = await open(ip);
      stats.flood.opened++;
      setTimeout(() => ws.terminate(), 2000);
    } catch (e) {
      inc(stats.flood.refused, (e as Error).message);
    }
  });
  await Promise.all(attempts);
}

const pct = (xs: number[], p: number) => {
  if (!xs.length) return 0;
  const s = [...xs].sort((a, b) => a - b);
  return Math.round(s[Math.min(s.length - 1, Math.floor((p / 100) * s.length))] * 10) / 10;
};

const until = Date.now() + DURATION;
console.log(`→ ${URL_}: ${HUMANS} humans, ${BOTS} bots, ${FLOOD} flood attempts, ${DURATION / 1000}s`);
const progress = setInterval(() => {
  console.log(`  humans ${stats.humans.connected} conn, ${stats.humans.acked}/${stats.humans.sent} acked, ${stats.humans.received} delivered | bots ${stats.bots.acked} acked, ${stats.bots.challenged} challenged, ${stats.bots.blocked} blocked`);
}, 5000);

await Promise.all([
  ...Array.from({ length: HUMANS }, (_, i) => human(i, until)),
  ...Array.from({ length: BOTS }, (_, i) => bot(i, until)),
  sleep(RAMP / 2).then(flood),
]);
clearInterval(progress);
await sleep(500);

const h = stats.humans;
console.log("\n=== RESULTS ===");
console.log(
  JSON.stringify(
    {
      humans: {
        connected: h.connected,
        failedConnect: h.failedConnect,
        sent: h.sent,
        acked: h.acked,
        rejected: h.rejected,
        challenged: h.challenged,
        messagesDelivered: h.received,
        ackLatencyMs: { p50: pct(h.latencies, 50), p95: pct(h.latencies, 95), p99: pct(h.latencies, 99) },
      },
      bots: {
        ...stats.bots,
        firstChallengeMs: { p50: pct(stats.bots.firstChallengeMs, 50), max: pct(stats.bots.firstChallengeMs, 100) },
        acceptedShare: stats.bots.sent ? `${((stats.bots.acked / stats.bots.sent) * 100).toFixed(2)}%` : "n/a",
      },
      flood: stats.flood,
    },
    null,
    2,
  ),
);
const humanRejects = Object.values(h.rejected).reduce((a, b) => a + b, 0);
console.log(humanRejects === 0 && h.challenged === 0 ? "✓ humans untouched" : `✗ humans affected: ${humanRejects} rejects, ${h.challenged} challenges`);
process.exit(0);
