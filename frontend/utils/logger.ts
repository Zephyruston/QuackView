const isDevelopment = (() => {
  if (
    typeof globalThis !== "undefined" &&
    "location" in globalThis &&
    globalThis.location
  ) {
    try {
      return (
        globalThis.location.hostname === "localhost" ||
        globalThis.location.hostname === "127.0.0.1"
      );
    } catch {
      return true;
    }
  }
  return true;
})();

type LogLevel = "debug" | "info" | "warn" | "error";

function formatLog(
  level: LogLevel,
  module: string,
  message: string,
  ..._args: unknown[]
): string {
  const timestamp = new Date().toISOString();
  const levelUpper = level.toUpperCase().padEnd(5);
  return `[${timestamp}] [${levelUpper}] [${module}] ${message}`;
}

function logMessage(
  level: LogLevel,
  module: string,
  message: string,
  ...args: unknown[]
) {
  const formattedMessage = formatLog(level, module, message, ...args);

  switch (level) {
    case "debug":
      if (isDevelopment) {
        console.log(formattedMessage, ...args);
      }
      break;
    case "info":
      console.info(formattedMessage, ...args);
      break;
    case "warn":
      console.warn(formattedMessage, ...args);
      break;
    case "error":
      console.error(formattedMessage, ...args);
      break;
  }
}

function createLogger(moduleName: string) {
  return {
    debug: (message: string, ...args: unknown[]) =>
      logMessage("debug", moduleName, message, ...args),
    info: (message: string, ...args: unknown[]) =>
      logMessage("info", moduleName, message, ...args),
    warn: (message: string, ...args: unknown[]) =>
      logMessage("warn", moduleName, message, ...args),
    error: (message: string, ...args: unknown[]) =>
      logMessage("error", moduleName, message, ...args),
  };
}

export const analysis = createLogger("analysis");
export const api = createLogger("api");
export const ui = createLogger("ui");
export const logger = createLogger("default");

export const debug = (message: string, ...args: unknown[]) =>
  logMessage("debug", "default", message, ...args);
export const info = (message: string, ...args: unknown[]) =>
  logMessage("info", "default", message, ...args);
export const warn = (message: string, ...args: unknown[]) =>
  logMessage("warn", "default", message, ...args);
export const error = (message: string, ...args: unknown[]) =>
  logMessage("error", "default", message, ...args);
