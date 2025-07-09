import morgan from "morgan";

import logger from "../config/logger.js";
import { getRealClientIP } from "../lib/util.js";

const morganFormat =
  ':remote-addr - :remote-user [:date[clf]] ":method :url HTTP/:http-version" :status :res[content-length] ":referrer" ":user-agent" - :response-time ms';

const requestLogger = morgan(morganFormat, {
  stream: {
    write: (message) => {
      const trimmedMessage = message.trim();

      const statusMatch = trimmedMessage.match(/" (\d{3}) /);
      const statusCode = statusMatch ? parseInt(statusMatch[1]) : 200;

      if (statusCode >= 500) {
        logger.error(trimmedMessage);
      } else if (statusCode >= 400) {
        logger.warn(trimmedMessage);
      } else {
        logger.info(trimmedMessage);
      }
    },
  },
});

const errorLogger = (error, req, res, next) => {
  if (error.status !== 404) {
    logger.error(`${req.method} ${req.originalUrl} - ${error.message}`, {
      error: error.stack,
      ip: getRealClientIP(req),
      userAgent: req.get("User-Agent"),
    });
  }
  next(error);
};

export { requestLogger, errorLogger };
