/**
 * Browser-origin allow-list shared by the WebSocket upgrade and HTTP CORS.
 *
 * Entries are normalized (trimmed, lower-cased, trailing slash removed), so
 * "https://App.example.com/" matches "https://app.example.com".
 * A "*" may stand for part of the host, e.g. "https://the-room-*.vercel.app"
 * for preview deployments. Keep wildcards narrow: "https://*.vercel.app"
 * would admit anyone's Vercel app.
 */

export function normalizeOrigin(value: string): string {
  return value.trim().toLowerCase().replace(/\/+$/, "");
}

export type OriginMatcher = (origin: string) => boolean;

export function originMatcher(entries: string[]): OriginMatcher {
  const exact = new Set<string>();
  const patterns: RegExp[] = [];
  for (const raw of entries) {
    const e = normalizeOrigin(raw);
    if (!e) continue;
    if (e.includes("*")) {
      const [scheme, host] = e.split("://");
      if (!host) continue;
      // "*" matches within the host only: never across "/" or ":" and never an empty label.
      const hostRe = host
        .split("*")
        .map((part) => part.replace(/[.+?^${}()|[\]\\]/g, "\\$&"))
        .join("[a-z0-9-]+");
      patterns.push(new RegExp(`^${scheme}://${hostRe}$`));
    } else {
      exact.add(e);
    }
  }
  return (origin: string) => {
    const o = normalizeOrigin(origin);
    return exact.has(o) || patterns.some((p) => p.test(o));
  };
}
