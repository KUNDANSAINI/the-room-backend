# The Room — backend

One global, anonymous, text-only, ephemeral chat room. Open → get today's ID → type → chat.
Security stays invisible unless behaviour is abnormal.

```bash
npm install
npm run dev        # in-memory state, http://localhost:8080 (ws: /ws)
npm test           # 68 tests; the multi-instance suite also runs against Redis if reachable (TEST_REDIS_URL, default redis://127.0.0.1:6379/15)
npm run build && npm start
```

Point the web client at it with `NEXT_PUBLIC_ROOM_WS_URL=ws://localhost:8080/ws` and `NEXT_PUBLIC_ROOM_API_URL=http://localhost:8080`.

---

## 1. Architecture

```
            browsers ──WebSocket /ws──┐          ┌── GET /stats, /healthz, /readyz, /metrics
                                      ▼          ▼
   ┌──────────────── L7 load balancer (WS-aware, any node) ───────────────┐
   ▼                               ▼                                      ▼
 gateway A                      gateway B          …                   gateway N      stateless Node processes
 (ws + http)                    (ws + http)                            (ws + http)
   │   ▲                          │   ▲                                  │   ▲
   │   └──── Redis PUB/SUB "events" (message fan-out, kicks) ────────────┘   │
   └──────── Redis: history, sessions, names, limits, blocks, presence, TIME ┘
```

- **Gateways are stateless.** Any client can reconnect to any instance: identity, suspicion, blocks and history live in Redis. An instance only holds its open sockets.
- **Fan-out.** An accepted message is written once to today's history list, then published once. Every instance, including the origin, delivers it to its local sockets. Messages that arrive in the same event-loop turn are coalesced into one frame per socket (`messages`). Idle rooms get no added latency; under load, frame count drops sharply (measured: 3.0M deliveries in 1.2M frames).
- **Clock.** Every instance aligns to Redis `TIME` (re-synced every 30s), so the midnight boundary is identical cluster-wide even if host clocks drift.
- **No PostgreSQL.** Nothing about users is permanent. Short-lived abuse events and reports live in a capped, time-trimmed Redis stream.
- **Single-process mode.** Without `REDIS_URL`, in-memory adapters implement the same interfaces (dev/tests). Production refuses to start without Redis.

```
src/
  index.ts                 process entry, signals, graceful shutdown
  app.ts                   wiring (config → store/bus/clock/verifier → RoomServer)
  config.ts                validated env config (+ production safety checks)
  protocol.ts              wire contract + strict zod schemas for client events
  gateway/room-server.ts   upgrade admission, fan-out, heartbeat, presence, midnight rollover, shutdown
  gateway/connection.ts    per-socket state machine: join, send pipeline, scoring, challenge, blocks
  http/routes.ts           /stats /healthz /readyz /metrics /admin/*
  identity/                names, tokens (ids, resume tokens), SessionService
  messaging/validate.ts    text normalization & limits
  abuse/                   token buckets, suspicion scoring, challenge verifiers
  net/ip.ts                client IP extraction, IPv6 /64 grouping, HMAC network pseudonyms
  room/day.ts, clock.ts    timezone calendar math, synced clock
  store/                   StateStore/Backplane interfaces, Redis + memory implementations
  observability/           pino logger (redacting), prom-client metrics
```

## 2. Data model

Internal and public identity are strictly separate:

| Concept | Where | Contents | Lifetime |
|---|---|---|---|
| **DailyRoom** | computed, not stored | `roomId` = calendar date in `ROOM_TIMEZONE` (e.g. `2026-09-11`), `day` number, `startsAt`, `endsAt` | deterministic |
| **AnonymousSession** | `sess:{sid}` | `sid` (128-bit random, **never sent**), `name` (public), `roomId`, daily network hash, suspicion snapshot, challenge state, block count | expires **at `endsAt`** |
| **Public name** | `name:{roomId}:{name}` → sid | uniqueness claim (`SET NX`) | expires at `endsAt` |
| **Resume token** | `resume:{sha256(token)}` → sid | only the hash is stored | expires at `endsAt` |
| **Message** | `msgs:{roomId}` list | `id, author (name snapshot), text, ts, country?` + internal `sid` (for reports; never broadcast) | capped to `HISTORY_LIMIT`, deleted at reset, TTL `endsAt + 10 min` as a safety net |

Redis keys (prefix `REDIS_PREFIX`, default `room:`):

```
msgs:{roomId}          LIST    JSON messages, RPUSH + LTRIM + PEXPIREAT
name:{roomId}:{name}   STRING  sid                     SET NX PXAT endsAt
sess:{sid}             STRING  JSON session            PXAT endsAt
resume:{hash}          STRING  sid                     PXAT endsAt
rl:{key}:{window}      STRING  fixed-window counters   (conn:, newsess:, ipmsg:, rejoin:, report:, connx:)
set:dup:{room}:{hash}  SET     sids sending same text  30s
block:{key}            STRING  reason                  PX ttl  (sess:{sid} | net:{blockHash} | cbo:{dailyHash})
lock:reset:{roomId}    STRING  run-once cleanup lock
presence               HASH    instanceId → "count:ts" (stale > 20s ignored & pruned)
abuse                  STREAM  reports/blocks/challenges (MAXLEN ~10k, MINID retention)
events                 PUBSUB  {kind:"message"|"kick", …}
```

Run Redis with `maxmemory-policy volatile-ttl`. Every key has a TTL except `presence`, which is pruned, and `abuse`, which is trimmed.

## 3. Event contracts

JSON text frames on `wss://…/ws`. Client frames are validated with strict schemas: unknown fields, unknown types, wrong types and oversized values are rejected. The web client's `src/lib/room/protocol.ts` mirrors this.

Mapping to the spec's example names: `join_room`→`join`, `send_message`→`send`, `heartbeat`→`ping`, `room_state`→`welcome`, `presence_update`→`presence`, `verification_required`→`challenge`, `room_reset`→`reset`. `connection_state` is expressed through close codes.

**Client → server**

| type | payload | notes |
|---|---|---|
| `join` | `{resume?: string, verification?: string}` | Must be sent within 10s. `resume` keeps today's identity; `verification` is an optional invisible Turnstile token. |
| `send` | `{clientId: [A-Za-z0-9_-]{1,64}, text: string, day?: number}` | `clientId` makes sends idempotent. `day` rejects cross-midnight in-flight sends. |
| `challenge_response` | `{token}` | Only meaningful while a challenge is pending. |
| `ping` | `{t: number}` | App-level keepalive; answered with `pong`. |
| `report` | `{messageId, reason: spam\|harassment\|illegal\|other}` | Rate-limited; stores a snapshot for review. |

**Server → client**

| type | payload |
|---|---|
| `welcome` | `{identity:{name,country?}, day, dayEndsAt, serverTime, online, history: Message[], resume, maxLength}` |
| `message` / `messages` | `{message}` / `{messages: Message[]}` (coalesced; same meaning) |
| `ack` | `{clientId, message}`: sender's confirmation (the sender doesn't also get `message`) |
| `reject` | `{clientId, reason: rate_limited\|challenge_required\|too_long\|invalid\|stale_room\|blocked, retryAfterMs?}` |
| `presence` | `{online}` (approximate, ≤ `PRESENCE_MS` stale) |
| `reset` | `{day, dayEndsAt, identity, resume}` |
| `challenge` | `{provider: turnstile\|mock, siteKey?}` |
| `challenge_result` | `{ok}` |
| `reported` | `{messageId}` |
| `error` | `{code: bad_request\|blocked\|try_later\|rate_limited\|not_found, message?}` |
| `pong` | `{t}` |

`Message = {id, author, text, ts, country?}`. Text is plain text, never HTML.

**Close codes:**
- `1008`: policy (join timeout, repeated malformed frames).
- `1009`: frame over 8 KB.
- `1012`: server restart; reconnect.
- `1013`: try later.
- `4403`: blocked. The web client treats 4400–4499 as "do not reconnect".

**Upgrade refusals (HTTP):**
- `403`: foreign origin, or network blocked.
- `429`: per-network connections, connect flood, or backoff.
- `503`: capacity, or shutting down.

**HTTP:**
- `GET /stats` → `{online, day, dayEndsAt, serverTime}`, with CORS for `ALLOWED_ORIGINS`.
- `/healthz` and `/readyz` (readiness checks Redis).
- `/metrics` (Prometheus; optional bearer token).
- `/admin/*`: see §9.

## 4. Rate limiting (human-first)

| Layer | Limit | Notes |
|---|---|---|
| Frame size | 8 KB (`maxPayload`), no permessage-deflate | Prevents oversized frames and compression bombs |
| Frame queue | 16 unprocessed frames per socket | Excess is dropped before parsing |
| Session message bucket, **normal** | burst 10, refill 1/s | Checked by simulation: 2,000 natural messages with frequent bursts → 0 limited |
| Session bucket, **throttled** (score ≥ 30) | burst 5, refill 0.33/s | Invisible soft throttle |
| Session bucket, **restricted** (score ≥ 90) | burst 2, refill 0.1/s | |
| Progressive cooldown | strikes 1–2: none; then 2s, 4s, 8s … max 60s | Strikes reset after 60s clean |
| Network message window | 80 msgs / 10s per network | Generous for campus/carrier NAT |
| Connections per network per instance | 16 concurrent | |
| Connection attempts | 40/min per network, then exponential backoff 2s → 10 min | Stops reconnect storms |
| New identities | 60/h per network signals; > 180/h refused | Identity-farming guard |
| Reports | 20/h per session | |

Humans are grouped by network pseudonym (IPv4 or IPv6 /64), never by raw IP.

## 5. Anti-automation scoring

Each connection has a suspicion score. Points **decay with a 4-minute half-life**, and a session that recently passed a challenge accumulates at half rate. **Every signal is worth less than the challenge threshold on its own** (enforced by a test). Repeating a message is a +3 signal: "lol" ten times in a minute stays below the throttle.

| Signal | Points |
|---|---|
| automation user agent (headless, python, curl, …) | 22 |
| missing Origin header | 12 |
| Turnstile configured, token missing / failed | 8 / 25 |
| many new identities from one network | 20 |
| reconnect churn (> 12 rejoins / 5 min) | 10 |
| malformed frame / binary frame | 10 / 15 |
| send before join | 6 |
| ping flood (> 5/s) | 6 |
| hit the rate limit | 7 |
| kept sending during an announced cooldown (≤ 4 per s) | 4 |
| network-wide flood | 8 |
| machine-regular cadence (10 gaps, coefficient of variation < 8%) | 18 |
| sent < 400 ms after join | 6 |
| own duplicate within 60s | 3 |
| same ≥ 12-char text from ≥ 4 sessions in 30s | 12 |

**Flow:**

```
normal ──(score ≥ 30)──▶ soft throttle (tighter bucket, invisible)
       ──(score ≥ 60)──▶ challenge required (sends rejected; text returns to the composer)
                           ├─ pass ──▶ score reset, 30 min trust, full speed
                           └─ 3 fails ─▶ temporary block: 10 min → 1 h → rest of day
                                          + network block 15 min × n (only on repeat / severe)
no provider configured: restricted tier; block only at score ≥ 90
```

Suspicion is saved with the session, so reconnecting doesn't wash it away. At midnight it's carried in memory to the new identity, without storing any link between days. Blocks are temporary; nothing is permanent.

**Measured** (local, 40s, one machine): 2,000 humans sent 1,681 messages with **0 rejects and 0 challenges**. 20 spam bots were challenged in **0.5–0.6s**, failed, and were blocked; 4.6% of their sends got through before the challenge. A 300-socket connection flood from one network got 40 through; the rest were refused.

## 6. Midnight reset — exact semantics

- **The boundary is T** = 00:00 in `ROOM_TIMEZONE` on the Redis clock, computed with DST-safe calendar math. It never uses client time.
- **Stamping:** a message belongs to the room of its server timestamp at acceptance. `send` re-checks the boundary and triggers the rollover itself if the 1-second ticker hasn't fired yet, then rejects with `stale_room`. So no message is stamped into the wrong day.
- **At T, each instance independently:**
  1. switches `room` (from this instant, bus events for the old room are dropped);
  2. the first instance to take `lock:reset:{old}` deletes the old history, with TTLs as the backstop;
  3. it gives every live connection a brand-new session, name and resume token, sends `reset` (batched, 250 at a time), and returns pending challenge state with it.
- **In-flight sends:**
  - `send.day ≠ current day` → `stale_room`. The web client puts the text back in the composer.
  - Sends without `day` within 3s after a connection's rollover are also `stale_room`.
- **Late bus events** (delayed pub/sub, slow instance) carrying the old `roomId` are dropped and counted in `room_stale_events_dropped_total`.
- **Disconnected over midnight:** yesterday's resume token has expired, so the client gets a new identity in `welcome`, whose new `day` triggers the client-side ritual.
- **Restarts:** the room is recomputed from the clock. Sessions and history are in Redis, so a restart mid-day changes nothing.
- **Clock drift:** instances sync to Redis `TIME`, and drift over 2s is logged. Residual skew of a few ms is covered by the drop rule above.

## 7. Privacy & retention

- **Never stored:** raw IPs, user agents, emails, accounts, cross-day links.
- **Network pseudonyms:** HMAC(`SERVER_SECRET`, day, network) for counters, rotating daily. A separate stable HMAC is used only as the key of a temporary network block.
- **Everything day-scoped expires at midnight:** sessions, names, resume tokens, history, counters.
- **Abuse stream:** 7-day retention for challenge/block events. Reports keep a message snapshot for 30 days, the minimum needed for review and legal requests.
- **Logs:** no message text, tokens or IPs (pino redaction). Sessions appear only as a short one-way `logTag`.
- **Turnstile:** verification doesn't forward client IPs.

## 8. Security checklist

| Threat | Mitigation |
|---|---|
| XSS / injection | Plain text only; clients render text (React). Server strips control chars, bidi overrides (Trojan Source), zero-width/invisible chars, lone surrogates and zalgo, and normalizes NFC. It deliberately doesn't HTML-escape, which would corrupt `<3`. |
| Forged events / spoofed IDs | Strict zod schemas (unknown fields → error). `author`, `id`, `ts` and `country` are always server-assigned. Names are generated server-side and unique via `SET NX`. |
| Replay | `clientId` idempotency per connection. Resume tokens are 192-bit, hashed at rest, and die at midnight. |
| Malformed / binary / oversized frames | Parse guard, schema, 8 KB max payload, repeat offenders closed, UTF-8 validated by `ws`. |
| Payload & connection flooding | Bounded per-socket queue, token buckets, per-network windows, backoff, connection caps, slow-consumer eviction (1 MB buffered). |
| CSWSH | `Origin` allow-list on upgrade. |
| Redis abuse | No client-controlled key names (only hashes/generated ids), bounded lists/streams, TTLs, fail-fast command timeouts, admission checks fail open (local limits still apply). |
| Admin endpoints | Disabled unless `ADMIN_TOKEN` is set; constant-time bearer check; 4 KB JSON bodies; audit events. Serve on a private network. |
| Proxy spoofing | `X-Forwarded-For` honoured only with `TRUST_PROXY`, right-most hop. |
| Unsafe config | Production requires a secret, Redis and origins, and forbids the mock challenge. |

## 9. Operations

**Admin API** (`Authorization: Bearer $ADMIN_TOKEN`):
- `GET /admin/reports?limit=50`
- `GET /admin/stats`
- `POST /admin/block {publicName | messageId, minutes, reason}`: blocks the session and kicks it on every instance.
- `POST /admin/unblock {publicName}`

**Metrics** (Prometheus):
- `room_ws_connections`, `room_ws_joined`, `room_online_total`, `room_suspicious_sessions`
- `room_messages_total{result}`, `room_fanout_frames_total`, `room_send_ack_seconds`
- `room_rate_limited_total{scope}`, `room_suspicion_signals_total{signal}`, `room_challenges_total{result}`, `room_blocks_total{scope}`
- `room_ws_connections_rejected_total{reason}`, `room_invalid_frames_total{reason}`
- `room_resets_total`, `room_stale_events_dropped_total`, `room_errors_total{kind}`, `room_reports_total`
- Node process defaults

**Shutdown:** `SIGTERM` → not-ready → stop accepting → disconnect clients with `1012`, spread over `SHUTDOWN_DRAIN_MS` so survivors aren't stampeded → close Redis. Set the orchestrator stop timeout above that window.

**Scaling:**
- 10 → 1,000 users: a single instance, with Redis optional.
- 10,000+: several instances behind any WebSocket-capable LB (no sticky sessions needed) plus one Redis. Size per instance with `MAX_CONNECTIONS`.
- At very large scale, fan-out volume, not connection count, is the bound: a single room grows as messages × listeners. Coalescing keeps frames per socket bounded by the event-loop tick rate. Beyond one Redis, shard pub/sub or move fan-out to Redis Streams / NATS; the `Backplane` interface isolates that change.

## 10. Testing

`npm test` has 68 tests:

- **Unit:**
  - timezone/DST day math and the exact midnight switch;
  - name/token generation;
  - normalization (bidi, zero-width, ZWJ emoji, zalgo, lone surrogates, code-point limits);
  - a human-conversation simulation that must never be rate-limited;
  - signal weights and decay;
  - timing-regularity detection;
  - IPv6 grouping and hash rotation;
  - refusal of unsafe production config.
- **Integration** (real servers + `ws` clients):
  - anonymous join, and no internal id in any frame;
  - same-day resume;
  - broadcast and ack semantics;
  - history for late joiners;
  - clientId idempotency;
  - forged-field rejection and server-side validation;
  - malformed, binary and oversized frames;
  - human bursts pass while floods are rate-limited;
  - bot → challenge → pass restores access;
  - 3 fails → 4403 block that survives reconnect;
  - suspicion persists across reconnect;
  - origin allow-list, connection-flood backoff and per-network caps;
  - stale heartbeat termination, presence, and graceful `1012`;
  - midnight reset (new day, new identities, old token dead, history deleted, `stale_room`);
  - cross-midnight bus events dropped and coalesced delivery order;
  - reports and admin block/kick, and admin auth/body limits;
  - `/stats` CORS and `/metrics`.
- **Multi-instance** (in-memory hub **and real Redis**):
  - exactly-once cross-instance delivery;
  - shared history, resume and presence;
  - cluster-wide unique names;
  - cluster-wide blocks;
  - Redis TTL/`PXAT` semantics.

## 11. Load testing

```bash
# target (load-test env only: TRUST_PROXY lets the generator simulate distinct networks)
REDIS_URL=redis://127.0.0.1:6379/14 TRUST_PROXY=true CHALLENGE_PROVIDER=mock PORT=8090 LOG_LEVEL=warn npx tsx src/index.ts
# generator
npx tsx loadtest/run.ts --url ws://127.0.0.1:8090/ws --humans 2000 --bots 20 --flood 300 --duration 60
```

Scenarios run concurrently:
- ramped humans chatting naturally;
- regular-cadence spam bots that ignore limits and fail challenges;
- a single-network connection flood.

The generator prints human rejects/challenges (target: 0), ack latency percentiles, bot time-to-challenge, bot accepted share, and flood refusals.

For thousands of clients, run several generator processes or machines: one Node generator parsing millions of frames becomes the bottleneck before the server does. Compare against the server-side `room_send_ack_seconds`.
