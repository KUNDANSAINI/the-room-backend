import { z } from "zod";

const bool = z
  .union([z.boolean(), z.string()])
  .transform((v) => (typeof v === "boolean" ? v : ["1", "true", "yes"].includes(v.toLowerCase())));
const list = z
  .string()
  .default("")
  .transform((s) =>
    s
      .split(",")
      // Browsers send Origin without a trailing slash, so "https://x.app/" would never match.
      .map((x) => x.trim().replace(/\/+$/, ""))
      .filter(Boolean),
  );

const schema = z.object({
  NODE_ENV: z.enum(["development", "test", "production"]).default("development"),
  HOST: z.string().default("0.0.0.0"),
  PORT: z.coerce.number().int().min(0).max(65535).default(8080),
  /** Stable per process; defaults to a random id. */
  INSTANCE_ID: z.string().optional(),

  /** Empty → in-memory store/backplane (single process, dev/test only). */
  REDIS_URL: z.string().default(""),
  REDIS_PREFIX: z.string().default("room:"),

  /** IANA timezone defining the official midnight. */
  ROOM_TIMEZONE: z.string().default("UTC"),
  /** Calendar date (in ROOM_TIMEZONE) that is "Day 1". */
  ROOM_EPOCH_DATE: z.string().regex(/^\d{4}-\d{2}-\d{2}$/).default("2026-03-11"),
  HISTORY_LIMIT: z.coerce.number().int().min(0).max(5000).default(300),
  MAX_MESSAGE_LENGTH: z.coerce.number().int().min(10).max(4000).default(500),
  MAX_MESSAGE_LINES: z.coerce.number().int().min(1).max(100).default(12),

  /** Secret used for daily-rotating IP hashing. Required in production. */
  SERVER_SECRET: z.string().default(""),
  /**
   * Comma-separated allowed browser origins for the WebSocket and /stats, e.g.
   * "https://the-room.vercel.app,http://localhost:3000". Scheme + host (+ port), no path.
   * Trailing slashes/case are normalized; "*" is allowed inside the host for preview URLs.
   */
  ALLOWED_ORIGINS: list,
  /** Trust X-Forwarded-For / CF-Connecting-IP (only behind a proxy you control). */
  TRUST_PROXY: bool.default(false),
  /** Show a country code next to names, from a trusted edge header (CF-IPCountry). Country-level only. */
  EXPOSE_COUNTRY: bool.default(false),
  HEARTBEAT_MS: z.coerce.number().int().min(100).default(25_000),
  PRESENCE_MS: z.coerce.number().int().min(50).default(5_000),
  /** Spread client disconnects over this window on shutdown to avoid a reconnect stampede. */
  SHUTDOWN_DRAIN_MS: z.coerce.number().int().min(0).default(8_000),

  MAX_CONNECTIONS: z.coerce.number().int().min(1).default(20_000),
  MAX_CONNECTIONS_PER_IP: z.coerce.number().int().min(1).default(16),
  CONNECT_ATTEMPTS_PER_MIN: z.coerce.number().int().min(1).default(40),
  NEW_SESSIONS_PER_IP_HOUR: z.coerce.number().int().min(1).default(60),

  CHALLENGE_PROVIDER: z.enum(["turnstile", "mock", "none"]).default("none"),
  TURNSTILE_SECRET: z.string().default(""),
  TURNSTILE_SITE_KEY: z.string().default(""),

  ADMIN_TOKEN: z.string().default(""),
  METRICS_TOKEN: z.string().default(""),

  LOG_LEVEL: z.enum(["fatal", "error", "warn", "info", "debug", "trace", "silent"]).default("info"),
});

export type Config = z.infer<typeof schema>;

export function loadConfig(env: Record<string, string | undefined> = process.env): Config {
  const parsed = schema.safeParse(env);
  if (!parsed.success) {
    throw new Error(`Invalid configuration:\n${z.prettifyError(parsed.error)}`);
  }
  const c = parsed.data;
  try {
    new Intl.DateTimeFormat("en-US", { timeZone: c.ROOM_TIMEZONE });
  } catch {
    throw new Error(`Invalid ROOM_TIMEZONE: ${c.ROOM_TIMEZONE}`);
  }
  c.ALLOWED_ORIGINS = c.ALLOWED_ORIGINS.map((o) => o.trim().replace(/^["']|["']$/g, "").toLowerCase().replace(/\/+$/, ""));
  const badOrigin = c.ALLOWED_ORIGINS.find((o) => !/^https?:\/\/[a-z0-9.*-]+(:\d+)?$/.test(o));
  if (badOrigin) {
    throw new Error(
      `ALLOWED_ORIGINS entry "${badOrigin}" is not an origin. Use scheme://host[:port] with no path, e.g. https://the-room.vercel.app`,
    );
  }
  if (c.NODE_ENV === "production") {
    if (c.SERVER_SECRET.length < 32) throw new Error("SERVER_SECRET (>=32 chars) is required in production");
    if (!c.REDIS_URL) throw new Error("REDIS_URL is required in production");
    if (c.CHALLENGE_PROVIDER === "mock") throw new Error("CHALLENGE_PROVIDER=mock is not allowed in production");
    if (c.CHALLENGE_PROVIDER === "turnstile" && !c.TURNSTILE_SECRET) throw new Error("TURNSTILE_SECRET is required");
    if (!c.ALLOWED_ORIGINS.length) throw new Error("ALLOWED_ORIGINS is required in production");
  }
  if (!c.SERVER_SECRET) c.SERVER_SECRET = "dev-insecure-secret-change-me-dev-insecure";
  return c;
}

/** Convenience for tests. */
export function testConfig(overrides: Partial<Record<keyof Config, string>> = {}): Config {
  return loadConfig({ NODE_ENV: "test", PORT: "0", LOG_LEVEL: "silent", ...overrides });
}
