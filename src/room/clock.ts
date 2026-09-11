/**
 * Authoritative clock. All instances align to the shared store's clock
 * (Redis TIME) so the midnight boundary is the same everywhere even if host
 * clocks drift. Falls back to the local clock if the store is unreachable.
 */
export interface Clock {
  now(): number;
}

export class SyncedClock implements Clock {
  private offset = 0;
  private timer: NodeJS.Timeout | undefined;

  constructor(private readonly source: () => Promise<number>, private readonly onDrift?: (ms: number) => void) {}

  now() {
    return Date.now() + this.offset;
  }

  async sync() {
    try {
      const t0 = Date.now();
      const remote = await this.source();
      const t1 = Date.now();
      // Assume symmetric latency.
      this.offset = Math.round(remote - (t0 + t1) / 2);
      this.onDrift?.(this.offset);
    } catch {
      /* keep previous offset */
    }
  }

  async start(intervalMs = 30_000) {
    await this.sync();
    this.timer = setInterval(() => void this.sync(), intervalMs);
    this.timer.unref();
  }

  stop() {
    clearInterval(this.timer);
  }
}

/** Manually controlled clock for tests. */
export class ManualClock implements Clock {
  constructor(public t = Date.now()) {}
  now() {
    return this.t;
  }
  advance(ms: number) {
    this.t += ms;
  }
}
