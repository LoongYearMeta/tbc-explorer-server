import dotenv from 'dotenv';
import winston from 'winston';
import path from 'path';
import { fileURLToPath } from 'url';
import fs from 'fs';

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const logsDir = path.join(__dirname, '../logs');
if (!fs.existsSync(logsDir)) {
	fs.mkdirSync(logsDir, { recursive: true });
}

const logFormat = winston.format.combine(
	winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
	winston.format.errors({ stack: true }),
	winston.format.printf(({ timestamp, level, message, stack }) => {
		return stack
			? `${timestamp} [${level.toUpperCase()}]: ${message}\n${stack}`
			: `${timestamp} [${level.toUpperCase()}]: ${message}`;
	}),
);

const logger = winston.createLogger({
	level: process.env.LOG_LEVEL || 'info',
	format: logFormat,
	transports: [
		new winston.transports.File({
			filename: path.join(__dirname, '../logs/app.log'),
			level: 'info',
			format: winston.format.combine(
				winston.format((info) => {
					return ['info', 'warn'].includes(info.level) ? info : false;
				})(),
				logFormat,
			),
		}),
		new winston.transports.File({
			filename: path.join(__dirname, '../logs/error.log'),
			level: 'error',
		}),
	],
	exceptionHandlers: [
		new winston.transports.File({
			filename: path.join(__dirname, '../logs/exceptions.log'),
		}),
	],
	rejectionHandlers: [
		new winston.transports.File({
			filename: path.join(__dirname, '../logs/rejections.log'),
		}),
	],
});

if (process.env.NODE_ENV && process.env.NODE_ENV === 'development') {
	logger.add(
		new winston.transports.Console({
			format: winston.format.combine(winston.format.colorize(), winston.format.simple()),
		}),
	);
}

export default logger;
