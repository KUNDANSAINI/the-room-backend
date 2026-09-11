import { pino, type Logger } from "pino";

export type { Logger };

/**
 * Structured JSON logs. Message text, raw IPs, resume tokens and internal
 * session ids are never logged (use logTag() for correlation).
 */
export function createLogger(level: string, instanceId: string): Logger {
  return pino({
    level,
    base: { instance: instanceId },
    timestamp: pino.stdTimeFunctions.isoTime,
    redact: { paths: ["text", "*.text", "token", "*.token", "resume", "*.resume", "ip", "*.ip"], censor: "[redacted]" },
  });
}
