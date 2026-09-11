import { describe, expect, it } from "vitest";
import { MESSAGE_TIERS, TokenBucket, violationCooldownMs } from "../src/abuse/rate-limit.js";
import { SIGNALS, SuspicionScore, THRESHOLDS, TimingAnalyzer, looksAutomatedUserAgent } from "../src/abuse/suspicion.js";
import { generatePublicName, PUBLIC_NAME_RE } from "../src/identity/names.js";
import { hashToken, newMessageId, newResumeToken, newSessionId } from "../src/identity/tokens.js";
import { normalizeText } from "../src/messaging/validate.js";
import { networkIdentity, networkKey } from "../src/net/ip.js";
import { originMatcher } from "../src/net/origin.js";
import { roomAt } from "../src/room/day.js";
import { loadConfig } from "../src/config.js";

const ch = (...cps: number[]) => String.fromCodePoint(...cps);
const limits = { maxLength: 500, maxLines: 12 };

describe("daily room calendar", () => {
  it("derives a deterministic room from the configured timezone", () => {
    const t = Date.UTC(2026, 8, 11, 12, 0, 0);
    const r = roomAt(t, "UTC", "2026-03-11");
    expect(r.roomId).toBe("2026-09-11");
    expect(r.startsAt).toBe(Date.UTC(2026, 8, 11));
    expect(r.endsAt).toBe(Date.UTC(2026, 8, 12));
    expect(r.day).toBe(185);
  });

  it("uses server timezone midnight, not UTC (Asia/Kolkata, +05:30)", () => {
    // 20:00 UTC on Sep 11 is already 01:30 on Sep 12 in India.
    const r = roomAt(Date.UTC(2026, 8, 11, 20, 0), "Asia/Kolkata", "2026-03-11");
    expect(r.roomId).toBe("2026-09-12");
    expect(r.startsAt).toBe(Date.UTC(2026, 8, 11, 18, 30));
    expect(r.endsAt).toBe(Date.UTC(2026, 8, 12, 18, 30));
  });

  it("handles DST days (America/New_York spring-forward is 23h long)", () => {
    const r = roomAt(Date.UTC(2026, 2, 8, 15), "America/New_York", "2026-03-01");
    expect(r.roomId).toBe("2026-03-08");
    expect(r.endsAt - r.startsAt).toBe(23 * 3_600_000);
    expect(r.day).toBe(8);
  });

  it("switches exactly at midnight", () => {
    const midnight = Date.UTC(2026, 8, 12);
    expect(roomAt(midnight - 1, "UTC", "2026-03-11").roomId).toBe("2026-09-11");
    expect(roomAt(midnight, "UTC", "2026-03-11").roomId).toBe("2026-09-12");
  });
});

describe("identity", () => {
  it("generates readable daily names", () => {
    const names = new Set(Array.from({ length: 2000 }, generatePublicName));
    for (const n of names) expect(n).toMatch(PUBLIC_NAME_RE);
    expect(names.size).toBeGreaterThan(1990); // collisions are rare and retried against the store anyway
  });

  it("uses unguessable internal ids and tokens, storing only hashes", () => {
    expect(newSessionId()).toMatch(/^[\w-]{22}$/);
    const t = newResumeToken();
    expect(t.length).toBeGreaterThanOrEqual(32);
    expect(hashToken(t)).not.toContain(t);
    expect(newResumeToken()).not.toBe(t);
    expect(newMessageId(Date.now())).not.toBe(newMessageId(Date.now()));
  });
});

describe("message normalization", () => {
  it("accepts ordinary text, slang and profanity unchanged", () => {
    for (const s of ["anyone here from India?", "yes 😂", "wtf lol", "a < b && c > d", "<3"]) {
      expect(normalizeText(s, limits)).toEqual({ ok: true, text: s });
    }
  });

  it("keeps HTML as literal text (clients render text, never HTML)", () => {
    const r = normalizeText('<script>alert("x")</script>', limits);
    expect(r).toEqual({ ok: true, text: '<script>alert("x")</script>' });
  });

  it("strips control, bidi-override and zero-width characters", () => {
    const raw = `hi${ch(0x202e)}evil${ch(0x200b)}${ch(0x0000)}${ch(0x0007)}${ch(0xfeff)}!`;
    expect(normalizeText(raw, limits)).toEqual({ ok: true, text: "hievil!" });
  });

  it("keeps emoji ZWJ sequences intact", () => {
    const family = ch(0x1f468, 0x200d, 0x1f469, 0x200d, 0x1f467);
    expect(normalizeText(`hello ${family}`, limits)).toEqual({ ok: true, text: `hello ${family}` });
  });

  it("rejects empty / invisible-only messages", () => {
    for (const s of ["", "   ", "\n\n\t", ch(0x200b, 0x200b), ch(0x0301, 0x0301)]) {
      expect(normalizeText(s, limits).ok).toBe(false);
    }
  });

  it("enforces max length in code points", () => {
    expect(normalizeText("a".repeat(500), limits).ok).toBe(true);
    expect(normalizeText("a".repeat(501), limits)).toEqual({ ok: false, reason: "too_long" });
    expect(normalizeText("😀".repeat(500), limits).ok).toBe(true); // 1000 UTF-16 units, 500 code points
  });

  it("caps combining-mark floods (zalgo) and blank-line runs", () => {
    const zalgo = "Z" + ch(0x0301).repeat(40);
    const r = normalizeText(zalgo, limits);
    expect(r.ok && [...r.text].length).toBe(4);
    expect(normalizeText("a\n\n\n\n\nb", limits)).toEqual({ ok: true, text: "a\n\nb" });
  });

  it("folds excess lines instead of rejecting", () => {
    const r = normalizeText(Array.from({ length: 30 }, (_, i) => `l${i}`).join("\n"), limits);
    expect(r.ok && r.text.split("\n").length).toBe(12);
  });

  it("drops lone surrogates (malformed UTF-16)", () => {
    expect(normalizeText(`ok${String.fromCharCode(0xd800)}`, limits)).toEqual({ ok: true, text: "ok" });
  });
});

describe("rate limiting", () => {
  it("never limits natural human conversation", () => {
    // 2,000 messages: mostly 2–20s apart, with frequent rapid bursts of 3–6 one-liners.
    const t0 = 1_000_000;
    const b = new TokenBucket(MESSAGE_TIERS.normal.capacity, MESSAGE_TIERS.normal.refillPerSec, t0);
    let now = t0;
    let limited = 0;
    let seed = 42;
    const rnd = () => ((seed = (seed * 1664525 + 1013904223) % 2 ** 32) / 2 ** 32);
    for (let i = 0; i < 2000; i++) {
      if (rnd() < 0.2) {
        const burst = 3 + Math.floor(rnd() * 4);
        for (let j = 0; j < burst; j++) {
          now += 500 + rnd() * 1000;
          if (b.take(now) > 0) limited++;
        }
      }
      now += 2000 + rnd() * 18000;
      if (b.take(now) > 0) limited++;
    }
    expect(limited).toBe(0);
  });

  it("stops floods and reports when to retry", () => {
    const b = new TokenBucket(10, 1, 0);
    let accepted = 0;
    let retry = 0;
    for (let i = 0; i < 200; i++) {
      const w = b.take(i); // 200 msgs in 200ms
      if (w === 0) accepted++;
      else retry = w;
    }
    expect(accepted).toBe(10);
    expect(retry).toBeGreaterThan(0);
  });

  it("applies progressive cooldowns only after repeated violations", () => {
    expect([1, 2, 3, 4, 5, 20].map(violationCooldownMs)).toEqual([0, 0, 2000, 4000, 8000, 60000]);
  });
});

describe("suspicion scoring", () => {
  it("no single signal can trigger a challenge on its own", () => {
    for (const [name, points] of Object.entries(SIGNALS)) {
      expect(points, name).toBeLessThan(THRESHOLDS.challenge);
    }
  });

  it("repeated identical messages alone never approach a challenge", () => {
    const s = new SuspicionScore();
    for (let i = 0; i < 10; i++) s.add(SIGNALS.ownDuplicate, i * 6_000); // "lol" x10 in a minute
    expect(s.value(60_000)).toBeLessThan(THRESHOLDS.throttle);
  });

  it("decays with a half-life", () => {
    const s = new SuspicionScore();
    s.add(40, 0);
    expect(s.value(4 * 60_000)).toBeCloseTo(20, 0);
    expect(s.value(60 * 60_000)).toBe(0);
  });

  it("verified sessions accumulate at half rate", () => {
    const s = new SuspicionScore();
    s.add(20, 0, true);
    expect(s.value(0)).toBe(10);
  });

  it("flags machine-regular cadence, not human jitter", () => {
    const bot = new TimingAnalyzer();
    let flagged = false;
    for (let i = 0; i < 12; i++) flagged ||= bot.record(10_000 + i * 1500);
    expect(flagged).toBe(true);

    const human = new TimingAnalyzer();
    let seed = 7;
    const rnd = () => ((seed = (seed * 1664525 + 1013904223) % 2 ** 32) / 2 ** 32);
    let t = 0;
    let humanFlag = false;
    for (let i = 0; i < 500; i++) {
      t += 800 + rnd() * 9000;
      humanFlag ||= human.record(t);
    }
    expect(humanFlag).toBe(false);
  });

  it("recognizes automation user agents without flagging browsers", () => {
    expect(looksAutomatedUserAgent("python-requests/2.31")).toBe(true);
    expect(looksAutomatedUserAgent("Mozilla/5.0 HeadlessChrome/140")).toBe(true);
    expect(looksAutomatedUserAgent(undefined)).toBe(true);
    expect(looksAutomatedUserAgent("Mozilla/5.0 (iPhone; CPU iPhone OS 18_0) Safari/604.1")).toBe(false);
  });
});

describe("network privacy", () => {
  it("groups IPv6 by /64 and unwraps v4-mapped addresses", () => {
    expect(networkKey("2001:db8:1:2:3:4:5:6")).toBe("2001:0db8:0001:0002::/64");
    expect(networkKey("2001:db8:1:2::9")).toBe("2001:0db8:0001:0002::/64");
    expect(networkKey("::ffff:10.0.0.1")).toBe("10.0.0.1");
  });

  it("never stores raw IPs; daily hashes rotate, block hashes are stable", () => {
    const a = networkIdentity("s".repeat(32), "2026-09-11", "203.0.113.9");
    const b = networkIdentity("s".repeat(32), "2026-09-12", "203.0.113.9");
    expect(a.dailyHash).not.toContain("203");
    expect(a.dailyHash).not.toBe(b.dailyHash);
    expect(a.blockHash).toBe(b.blockHash);
  });
});

describe("origin allow-list", () => {
  it("tolerates trailing slashes, case and whitespace", () => {
    const allowed = originMatcher([" https://The-Room-Frontend-mu.vercel.app/ ", "http://localhost:3000"]);
    expect(allowed("https://the-room-frontend-mu.vercel.app")).toBe(true);
    expect(allowed("http://localhost:3000")).toBe(true);
    expect(allowed("http://the-room-frontend-mu.vercel.app")).toBe(false); // scheme matters
    expect(allowed("https://evil.example")).toBe(false);
  });

  it("supports narrow host wildcards for preview deployments", () => {
    const allowed = originMatcher(["https://the-room-frontend-*.vercel.app"]);
    expect(allowed("https://the-room-frontend-git-main-kundan.vercel.app")).toBe(true);
    expect(allowed("https://someone-else.vercel.app")).toBe(false);
    expect(allowed("https://the-room-frontend-.vercel.app")).toBe(false);
    expect(allowed("https://the-room-frontend-x.vercel.app.evil.com")).toBe(false);
    expect(allowed("https://the-room-frontend-a/b.vercel.app")).toBe(false);
  });

  it("rejects entries that are URLs rather than origins", () => {
    expect(() => loadConfig({ ALLOWED_ORIGINS: "https://the-room.vercel.app/room" })).toThrow(/not an origin/);
    expect(loadConfig({ ALLOWED_ORIGINS: '"https://A.vercel.app/",http://localhost:3000' }).ALLOWED_ORIGINS).toEqual([
      "https://a.vercel.app",
      "http://localhost:3000",
    ]);
  });
});

describe("configuration", () => {
  it("refuses unsafe production configs", () => {
    expect(() => loadConfig({ NODE_ENV: "production" })).toThrow(/SERVER_SECRET/);
    expect(() =>
      loadConfig({
        NODE_ENV: "production",
        SERVER_SECRET: "x".repeat(40),
        REDIS_URL: "redis://r",
        ALLOWED_ORIGINS: "https://theroom.app",
        CHALLENGE_PROVIDER: "mock",
      }),
    ).toThrow(/mock/);
    expect(() => loadConfig({ ROOM_TIMEZONE: "Mars/Olympus" })).toThrow(/ROOM_TIMEZONE/);
  });
});
