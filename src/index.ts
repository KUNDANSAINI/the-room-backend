import { createApp } from "./app.js";
import { loadConfig } from "./config.js";

const config = loadConfig();
const app = createApp(config);
const log = app.server.log;

let stopping = false;
async function shutdown(signal: string) {
  if (stopping) return;
  stopping = true;
  log.info({ signal }, "shutting down");
  const hardExit = setTimeout(() => process.exit(1), config.SHUTDOWN_DRAIN_MS + 10_000);
  hardExit.unref();
  try {
    await app.stop();
    process.exit(0);
  } catch (err) {
    log.error({ err }, "shutdown failed");
    process.exit(1);
  }
}

process.on("SIGTERM", () => void shutdown("SIGTERM"));
process.on("SIGINT", () => void shutdown("SIGINT"));
process.on("unhandledRejection", (err) => log.error({ err }, "unhandled rejection"));
process.on("uncaughtException", (err) => {
  log.fatal({ err }, "uncaught exception");
  void shutdown("uncaughtException");
});

app.start().catch((err) => {
  log.fatal({ err }, "failed to start");
  process.exit(1);
});
