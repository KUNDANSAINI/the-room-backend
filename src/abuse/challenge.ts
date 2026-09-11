/**
 * Challenge providers. The backend decides when a challenge is needed; the
 * client only renders it. Never the default entry step.
 */
export interface ChallengeVerifier {
  /** Provider name sent to the client, or null if challenges are unavailable. */
  readonly provider: "turnstile" | "mock" | null;
  readonly siteKey?: string;
  verify(token: string): Promise<boolean>;
}

export class TurnstileVerifier implements ChallengeVerifier {
  readonly provider = "turnstile" as const;

  constructor(
    private readonly secret: string,
    readonly siteKey: string,
    private readonly endpoint = "https://challenges.cloudflare.com/turnstile/v0/siteverify",
    private readonly fetchFn: typeof fetch = fetch,
  ) {}

  async verify(token: string): Promise<boolean> {
    if (!token || token.length > 4096) return false;
    try {
      const body = new URLSearchParams({ secret: this.secret, response: token });
      // The client IP is intentionally not forwarded (data minimization).
      const res = await this.fetchFn(this.endpoint, { method: "POST", body, signal: AbortSignal.timeout(5000) });
      if (!res.ok) return false;
      const data = (await res.json()) as { success?: boolean };
      return data.success === true;
    } catch {
      return false;
    }
  }
}

/** Development only (rejected by config in production). Matches the frontend's dev mock. */
export class MockVerifier implements ChallengeVerifier {
  readonly provider = "mock" as const;
  async verify(token: string) {
    return token === "mock-human";
  }
}

export class NoChallenge implements ChallengeVerifier {
  readonly provider = null;
  async verify() {
    return false;
  }
}
