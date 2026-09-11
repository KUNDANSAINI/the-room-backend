/**
 * Layered automation scoring. No single weak signal can reach the challenge
 * threshold on its own; points decay (half-life) so a human who briefly
 * looked odd drifts back to zero without ever noticing.
 */

export const SIGNALS = {
  // Connection-time
  automationUserAgent: 22,
  missingOrigin: 12,
  verificationMissing: 8,
  verificationFailed: 25,
  manyNewSessionsFromNetwork: 20,
  reconnectChurn: 10,
  // Protocol hygiene
  malformedFrame: 10,
  binaryFrame: 15,
  sendBeforeJoin: 6,
  pingFlood: 6,
  // Behavior
  rateLimitHit: 7,
  /** Kept sending during an announced cooldown (the web client prevents this). Throttled. */
  ignoredCooldown: 4,
  networkFlood: 8,
  machineRegularTiming: 18,
  instantAfterJoin: 6,
  ownDuplicate: 3,
  crossSessionDuplicate: 12,
} as const;

export type Signal = keyof typeof SIGNALS;

export const THRESHOLDS = {
  /** Invisible: tighter token bucket. */
  throttle: 30,
  /** Ask for a challenge (if a provider is configured). */
  challenge: 60,
  /** Severe: restrict hard / block temporarily when challenges are unavailable or ignored. */
  severe: 90,
  max: 150,
} as const;

const HALF_LIFE_MS = 4 * 60_000;
/** Recently verified humans accumulate at half rate. */
const TRUSTED_FACTOR = 0.5;

export class SuspicionScore {
  constructor(private score = 0, private at = 0) {}

  value(now: number): number {
    if (this.score <= 0) return 0;
    const decayed = this.score * 0.5 ** ((now - this.at) / HALF_LIFE_MS);
    return decayed < 0.5 ? 0 : decayed;
  }

  add(points: number, now: number, trusted = false): number {
    const v = this.value(now) + points * (trusted ? TRUSTED_FACTOR : 1);
    this.score = Math.max(0, Math.min(THRESHOLDS.max, v));
    this.at = now;
    return this.score;
  }

  reset(now: number) {
    this.score = 0;
    this.at = now;
  }

  snapshot(now: number) {
    return { suspicion: Math.round(this.value(now) * 10) / 10, suspicionAt: now };
  }
}

/**
 * Humans are irregular. A long run of near-identical gaps between messages
 * (coefficient of variation < 8%) at conversational speed is a strong
 * automation signal. Requires many samples so it can't fire on a few quick replies.
 */
export class TimingAnalyzer {
  private gaps: number[] = [];
  private last = 0;

  constructor(private readonly window = 10) {}

  /** Records a message; returns true if the recent cadence looks machine-generated. */
  record(now: number): boolean {
    if (this.last) {
      this.gaps.push(now - this.last);
      if (this.gaps.length > this.window) this.gaps.shift();
    }
    this.last = now;
    if (this.gaps.length < this.window) return false;
    const mean = this.gaps.reduce((a, b) => a + b, 0) / this.gaps.length;
    if (mean > 20_000) return false;
    const variance = this.gaps.reduce((a, g) => a + (g - mean) ** 2, 0) / this.gaps.length;
    const cv = Math.sqrt(variance) / Math.max(1, mean);
    if (cv < 0.08) {
      this.gaps = []; // don't re-fire on every subsequent message
      return true;
    }
    return false;
  }
}

const AUTOMATION_UA = /(headless|phantomjs|puppeteer|playwright|selenium|webdriver|python|curl|wget|go-http-client|okhttp|java\/|node-fetch|axios|libwww|scrapy|httpclient)/i;

export const looksAutomatedUserAgent = (ua: string | undefined) => !ua || AUTOMATION_UA.test(ua);
