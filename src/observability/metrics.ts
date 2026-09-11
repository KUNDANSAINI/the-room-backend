import { Counter, Gauge, Histogram, Registry, collectDefaultMetrics } from "prom-client";

export function createMetrics(instanceId: string, collectDefaults = true) {
  const registry = new Registry();
  registry.setDefaultLabels({ instance: instanceId });
  if (collectDefaults) collectDefaultMetrics({ register: registry });

  const m = {
    registry,
    connections: new Gauge({ name: "room_ws_connections", help: "Open WebSocket connections on this instance", registers: [registry] }),
    joined: new Gauge({ name: "room_ws_joined", help: "Joined sessions on this instance", registers: [registry] }),
    onlineTotal: new Gauge({ name: "room_online_total", help: "Approximate cluster-wide online count", registers: [registry] }),
    suspicious: new Gauge({ name: "room_suspicious_sessions", help: "Connections above the throttle threshold", registers: [registry] }),
    connectionsRejected: new Counter({
      name: "room_ws_connections_rejected_total",
      help: "Refused upgrades",
      labelNames: ["reason"],
      registers: [registry],
    }),
    messages: new Counter({
      name: "room_messages_total",
      help: "Send attempts by result",
      labelNames: ["result"],
      registers: [registry],
    }),
    fanout: new Counter({ name: "room_fanout_frames_total", help: "Message frames delivered to sockets", registers: [registry] }),
    rateLimited: new Counter({
      name: "room_rate_limited_total",
      help: "Rate-limit decisions",
      labelNames: ["scope"],
      registers: [registry],
    }),
    signals: new Counter({
      name: "room_suspicion_signals_total",
      help: "Automation signals observed",
      labelNames: ["signal"],
      registers: [registry],
    }),
    challenges: new Counter({
      name: "room_challenges_total",
      help: "Challenge lifecycle",
      labelNames: ["result"],
      registers: [registry],
    }),
    blocks: new Counter({ name: "room_blocks_total", help: "Temporary blocks applied", labelNames: ["scope"], registers: [registry] }),
    invalidFrames: new Counter({
      name: "room_invalid_frames_total",
      help: "Malformed/unsupported inbound frames",
      labelNames: ["reason"],
      registers: [registry],
    }),
    resets: new Counter({ name: "room_resets_total", help: "Daily room resets performed", registers: [registry] }),
    staleDropped: new Counter({ name: "room_stale_events_dropped_total", help: "Cross-midnight events dropped", registers: [registry] }),
    errors: new Counter({ name: "room_errors_total", help: "Internal errors", labelNames: ["kind"], registers: [registry] }),
    reports: new Counter({ name: "room_reports_total", help: "Abuse reports", registers: [registry] }),
    ackLatency: new Histogram({
      name: "room_send_ack_seconds",
      help: "Time from receiving a send to acking it",
      buckets: [0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1],
      registers: [registry],
    }),
  };
  return m;
}

export type Metrics = ReturnType<typeof createMetrics>;
