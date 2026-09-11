import type { IncomingMessage } from "node:http";
import { isIP } from "node:net";
import { hmac } from "../identity/tokens.js";

/**
 * Client address. Proxy headers are honored only when TRUST_PROXY is set;
 * for X-Forwarded-For we take the right-most entry (added by our own proxy),
 * which cannot be forged by the client.
 */
export function clientIp(req: IncomingMessage, trustProxy: boolean): string {
  if (trustProxy) {
    const cf = req.headers["cf-connecting-ip"];
    if (typeof cf === "string" && isIP(cf.trim())) return cf.trim();
    const xff = req.headers["x-forwarded-for"];
    const raw = Array.isArray(xff) ? xff.join(",") : xff;
    if (raw) {
      const parts = raw.split(",").map((p) => p.trim()).filter(Boolean);
      const last = parts[parts.length - 1];
      if (last && isIP(last)) return last;
    }
  }
  return req.socket.remoteAddress ?? "0.0.0.0";
}

/** Network key: IPv4 address, or the /64 prefix for IPv6 (one household/device). */
export function networkKey(ip: string): string {
  const v4mapped = ip.match(/^::ffff:(\d+\.\d+\.\d+\.\d+)$/i);
  if (v4mapped) return v4mapped[1];
  if (isIP(ip) === 6) {
    const full = expandV6(ip);
    return full.split(":").slice(0, 4).join(":") + "::/64";
  }
  return ip;
}

function expandV6(ip: string): string {
  const [head, tail = ""] = ip.split("::");
  const h = head ? head.split(":") : [];
  const t = tail ? tail.split(":") : [];
  const fill = Array(Math.max(0, 8 - h.length - t.length)).fill("0");
  return [...h, ...fill, ...t].map((x) => x.padStart(4, "0").toLowerCase()).join(":");
}

export interface NetworkIdentity {
  /** Daily-rotating pseudonym, used for counters/limits. Unlinkable across days. */
  dailyHash: string;
  /** Stable pseudonym, only ever persisted as the key of a temporary network block. */
  blockHash: string;
}

export function networkIdentity(secret: string, roomId: string, ip: string): NetworkIdentity {
  const net = networkKey(ip);
  return { dailyHash: hmac(secret, `d|${roomId}|${net}`), blockHash: hmac(secret, `b|${net}`) };
}
