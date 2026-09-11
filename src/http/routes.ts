import type { IncomingMessage, ServerResponse } from "node:http";
import { z } from "zod";
import { PUBLIC_NAME_RE } from "../identity/names.js";
import { logTag, safeEqual } from "../identity/tokens.js";
import type { RoomServer } from "../gateway/room-server.js";

const MAX_BODY = 4096;

function send(res: ServerResponse, status: number, body: unknown, headers: Record<string, string> = {}) {
  const payload = body === undefined ? "" : JSON.stringify(body);
  res.writeHead(status, {
    "content-type": "application/json; charset=utf-8",
    "cache-control": "no-store",
    "x-content-type-options": "nosniff",
    "referrer-policy": "no-referrer",
    "content-security-policy": "default-src 'none'; frame-ancestors 'none'",
    ...headers,
  });
  res.end(payload);
}

function bearer(req: IncomingMessage) {
  const h = req.headers.authorization ?? "";
  return h.startsWith("Bearer ") ? h.slice(7) : "";
}

async function readJson(req: IncomingMessage): Promise<unknown> {
  if (!(req.headers["content-type"] ?? "").includes("application/json")) throw new HttpError(415, "json_required");
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of req) {
    size += (chunk as Buffer).length;
    if (size > MAX_BODY) throw new HttpError(413, "too_large");
    chunks.push(chunk as Buffer);
  }
  try {
    return JSON.parse(Buffer.concat(chunks).toString("utf8"));
  } catch {
    throw new HttpError(400, "bad_json");
  }
}

class HttpError extends Error {
  constructor(readonly status: number, readonly code: string) {
    super(code);
  }
}

const blockSchema = z
  .object({
    publicName: z.string().regex(PUBLIC_NAME_RE).optional(),
    messageId: z.string().max(64).optional(),
    minutes: z.number().int().min(1).max(7 * 24 * 60).default(60),
    reason: z.string().max(200).default("admin"),
  })
  .strict()
  .refine((b) => b.publicName || b.messageId, "publicName or messageId required");

const unblockSchema = z.object({ publicName: z.string().regex(PUBLIC_NAME_RE) }).strict();

export async function handleHttp(srv: RoomServer, req: IncomingMessage, res: ServerResponse) {
  const url = new URL(req.url ?? "/", "http://localhost");
  const path = url.pathname;
  const method = req.method ?? "GET";
  try {
    // — public —
    if (path === "/healthz" && method === "GET") return send(res, 200, { ok: true });

    if (path === "/readyz" && method === "GET") {
      const ok = !srv.shuttingDown && (await srv.store.ping());
      return send(res, ok ? 200 : 503, { ok });
    }

    if (path === "/stats") {
      const origin = req.headers.origin;
      const allowed = srv.config.ALLOWED_ORIGINS;
      const cors: Record<string, string> = {};
      if (origin && (allowed.length === 0 ? srv.config.NODE_ENV !== "production" : allowed.includes(origin))) {
        cors["access-control-allow-origin"] = origin;
        cors["vary"] = "Origin";
      }
      if (method === "OPTIONS") return send(res, 204, undefined, { ...cors, "access-control-allow-methods": "GET" });
      if (method !== "GET") return send(res, 405, { error: "method_not_allowed" });
      const room = srv.room;
      return send(
        res,
        200,
        { online: srv.online, day: room.day, dayEndsAt: room.endsAt, serverTime: srv.clock.now() },
        { ...cors, "cache-control": "public, max-age=5" },
      );
    }

    if (path === "/metrics" && method === "GET") {
      const token = srv.config.METRICS_TOKEN;
      if (token && !safeEqual(bearer(req), token)) return send(res, 401, { error: "unauthorized" });
      const body = await srv.metrics.registry.metrics();
      res.writeHead(200, { "content-type": srv.metrics.registry.contentType, "cache-control": "no-store" });
      return res.end(body);
    }

    // — admin (disabled unless ADMIN_TOKEN is set; expose only on a private network) —
    if (path.startsWith("/admin/")) {
      const token = srv.config.ADMIN_TOKEN;
      if (!token) return send(res, 404, { error: "not_found" });
      if (!safeEqual(bearer(req), token)) {
        srv.log.warn({ path }, "unauthorized admin request");
        return send(res, 401, { error: "unauthorized" });
      }
      return await admin(srv, req, res, path, method, url);
    }

    return send(res, 404, { error: "not_found" });
  } catch (err) {
    if (err instanceof HttpError) return send(res, err.status, { error: err.code });
    srv.metrics.errors.inc({ kind: "http" });
    srv.log.error({ err, path }, "http handler failed");
    return send(res, 500, { error: "internal" });
  }
}

async function admin(srv: RoomServer, req: IncomingMessage, res: ServerResponse, path: string, method: string, url: URL) {
  if (path === "/admin/reports" && method === "GET") {
    const limit = Math.min(500, Math.max(1, Number(url.searchParams.get("limit")) || 50));
    return send(res, 200, { events: await srv.store.listAbuseEvents(limit) });
  }

  if (path === "/admin/stats" && method === "GET") {
    return send(res, 200, {
      instance: srv.instanceId,
      room: srv.room,
      online: srv.online,
      connectionsHere: srv.connectionCount,
    });
  }

  if (path === "/admin/block" && method === "POST") {
    const parsed = blockSchema.safeParse(await readJson(req));
    if (!parsed.success) return send(res, 400, { error: "invalid", details: parsed.error.issues.map((i) => i.message) });
    const b = parsed.data;
    let sid: string | null = null;
    if (b.publicName) sid = await srv.store.nameOwner(srv.room.roomId, b.publicName);
    if (!sid && b.messageId) {
      const recent = await srv.store.recentMessages(srv.room.roomId, srv.config.HISTORY_LIMIT);
      sid = recent.find((m) => m.id === b.messageId)?.sid ?? null;
    }
    if (!sid) return send(res, 404, { error: "session_not_found" });
    const ttl = b.minutes * 60_000;
    await srv.store.setBlock(`sess:${sid}`, ttl, `admin:${b.reason}`);
    await srv.bus.publish({ kind: "kick", origin: srv.instanceId, sid, reason: b.reason });
    await srv.store.addAbuseEvent({ type: "admin_block", session: logTag(sid), minutes: b.minutes, reason: b.reason }, 30 * 86_400_000);
    srv.metrics.blocks.inc({ scope: "admin" });
    srv.log.warn({ session: logTag(sid), minutes: b.minutes }, "admin block");
    return send(res, 200, { ok: true, session: logTag(sid), minutes: b.minutes });
  }

  if (path === "/admin/unblock" && method === "POST") {
    const parsed = unblockSchema.safeParse(await readJson(req));
    if (!parsed.success) return send(res, 400, { error: "invalid" });
    const sid = await srv.store.nameOwner(srv.room.roomId, parsed.data.publicName);
    if (!sid) return send(res, 404, { error: "session_not_found" });
    await srv.store.deleteBlock(`sess:${sid}`);
    return send(res, 200, { ok: true });
  }

  return send(res, 404, { error: "not_found" });
}
