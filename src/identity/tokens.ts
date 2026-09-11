import { createHash, createHmac, randomBytes, timingSafeEqual } from "node:crypto";

/** Internal session id. 128 bits, never sent to clients. */
export const newSessionId = () => randomBytes(16).toString("base64url");

/** Opaque bearer token the client uses to keep its identity across reconnects (same day only). */
export const newResumeToken = () => randomBytes(24).toString("base64url");

/** Only hashes of resume tokens are stored, so a store dump cannot hijack identities. */
export const hashToken = (token: string) => createHash("sha256").update(token).digest("base64url");

/** Public message id: time-sortable, unguessable, not derived from any session data. */
export function newMessageId(now: number) {
  return `${now.toString(36)}${randomBytes(6).toString("base64url")}`;
}

/** Short, non-reversible tag for logs (never log raw internal ids). */
export const logTag = (value: string) => createHash("sha256").update(value).digest("hex").slice(0, 10);

export const hmac = (secret: string, value: string) =>
  createHmac("sha256", secret).update(value).digest("base64url").slice(0, 22);

export function safeEqual(a: string, b: string) {
  const ab = Buffer.from(a);
  const bb = Buffer.from(b);
  return ab.length === bb.length && timingSafeEqual(ab, bb);
}
