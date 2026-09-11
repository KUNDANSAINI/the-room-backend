/**
 * Token bucket: generous bursts for fast typists, bounded sustained rate.
 */
export class TokenBucket {
  private tokens: number;
  private last: number;

  constructor(public capacity: number, public refillPerSec: number, now: number) {
    this.tokens = capacity;
    this.last = now;
  }

  /** Change limits in place (adaptive throttling) without refilling. */
  reconfigure(capacity: number, refillPerSec: number, now: number) {
    this.refill(now);
    this.capacity = capacity;
    this.refillPerSec = refillPerSec;
    this.tokens = Math.min(this.tokens, capacity);
  }

  private refill(now: number) {
    const elapsed = Math.max(0, now - this.last) / 1000;
    this.tokens = Math.min(this.capacity, this.tokens + elapsed * this.refillPerSec);
    this.last = now;
  }

  /** Returns 0 if allowed, otherwise ms until `cost` tokens are available. */
  take(now: number, cost = 1): number {
    this.refill(now);
    if (this.tokens >= cost) {
      this.tokens -= cost;
      return 0;
    }
    return Math.ceil(((cost - this.tokens) / this.refillPerSec) * 1000);
  }
}

export interface Tier {
  capacity: number;
  refillPerSec: number;
}

/**
 * Message rate tiers by suspicion. "normal" is intentionally invisible to
 * humans: 10-message bursts and ~1 msg/s sustained is far beyond natural chat.
 */
export const MESSAGE_TIERS = {
  normal: { capacity: 10, refillPerSec: 1 },
  throttled: { capacity: 5, refillPerSec: 0.33 },
  restricted: { capacity: 2, refillPerSec: 0.1 },
} satisfies Record<string, Tier>;

/** Progressive cooldown after repeated violations: 0, 0, 2s, 4s, 8s … capped at 60s. */
export function violationCooldownMs(strikes: number): number {
  if (strikes < 3) return 0;
  return Math.min(60_000, 1000 * 2 ** (strikes - 2));
}
