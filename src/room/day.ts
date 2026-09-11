/**
 * Calendar math for the daily room in a configurable IANA timezone.
 * The room for a given instant is the calendar date of that instant in ROOM_TIMEZONE.
 * No dependency on process TZ or any client clock.
 */

const DAY_MS = 86_400_000;
const fmtCache = new Map<string, Intl.DateTimeFormat>();

function formatter(tz: string) {
  let f = fmtCache.get(tz);
  if (!f) {
    f = new Intl.DateTimeFormat("en-US", {
      timeZone: tz,
      hourCycle: "h23",
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
    });
    fmtCache.set(tz, f);
  }
  return f;
}

interface Parts {
  y: number;
  m: number;
  d: number;
  hh: number;
  mm: number;
  ss: number;
}

function partsAt(ts: number, tz: string): Parts {
  const p: Record<string, number> = {};
  for (const part of formatter(tz).formatToParts(ts)) {
    if (part.type !== "literal") p[part.type] = Number(part.value);
  }
  return { y: p.year, m: p.month, d: p.day, hh: p.hour === 24 ? 0 : p.hour, mm: p.minute, ss: p.second };
}

/** Offset (ms) of `tz` from UTC at instant `ts`. */
function offsetAt(ts: number, tz: string): number {
  const p = partsAt(ts, tz);
  const asUtc = Date.UTC(p.y, p.m - 1, p.d, p.hh, p.mm, p.ss);
  return asUtc - Math.floor(ts / 1000) * 1000;
}

/** UTC instant of local midnight starting calendar date (y, m, d) in tz. DST-safe. */
export function midnightUtc(y: number, m: number, d: number, tz: string): number {
  const naive = Date.UTC(y, m - 1, d);
  let guess = naive - offsetAt(naive, tz);
  // Re-evaluate once in case the offset differs at the guessed instant (DST edges).
  guess = naive - offsetAt(guess, tz);
  return guess;
}

export function roomDateAt(ts: number, tz: string): string {
  const p = partsAt(ts, tz);
  return `${p.y}-${String(p.m).padStart(2, "0")}-${String(p.d).padStart(2, "0")}`;
}

export interface DailyRoom {
  /** Deterministic id: the calendar date, e.g. "2026-09-11". */
  roomId: string;
  /** 1-based day number since ROOM_EPOCH_DATE. */
  day: number;
  startsAt: number;
  endsAt: number;
}

export function roomAt(ts: number, tz: string, epochDate: string): DailyRoom {
  const p = partsAt(ts, tz);
  const startsAt = midnightUtc(p.y, p.m, p.d, tz);
  const next = new Date(Date.UTC(p.y, p.m - 1, p.d + 1));
  const endsAt = midnightUtc(next.getUTCFullYear(), next.getUTCMonth() + 1, next.getUTCDate(), tz);
  const [ey, em, ed] = epochDate.split("-").map(Number);
  const day = Math.round((Date.UTC(p.y, p.m - 1, p.d) - Date.UTC(ey, em - 1, ed)) / DAY_MS) + 1;
  return { roomId: roomDateAt(ts, tz), day, startsAt, endsAt };
}
