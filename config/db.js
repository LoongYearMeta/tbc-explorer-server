import mongoose from 'mongoose';
import dotenv from 'dotenv';

import logger from '../config/logger.js';

dotenv.config();

const connectDB = async () => {
	try {
		const db = await mongoose.connect(process.env.MONGO, {
			maxPoolSize: parseInt(process.env.MONGO_MAX_POOL_SIZE) || 1500, 
			minPoolSize: parseInt(process.env.MONGO_MIN_POOL_SIZE) || 50,   
			maxIdleTimeMS: parseInt(process.env.MONGO_MAX_IDLE_TIME) || 30000, 
			serverSelectionTimeoutMS: parseInt(process.env.MONGO_SERVER_SELECTION_TIMEOUT) || 5000,
			heartbeatFrequencyMS: parseInt(process.env.MONGO_HEARTBEAT_FREQUENCY) || 10000,
			retryWrites: true,
			retryReads: true,
		});
		
		mongoose.connection.on('connected', () => {
			logger.info('MongoDB connected');
		});
		
		mongoose.connection.on('error', (err) => {
			logger.error('MongoDB connection error', { error: err.message });
		});
		
		mongoose.connection.on('disconnected', () => {
			logger.warn('MongoDB disconnected');
		});
		
		setInterval(() => {
			const connState = mongoose.connection.readyState;
			const states = {
				0: 'disconnected',
				1: 'connected', 
				2: 'connecting',
				3: 'disconnecting'
			};
			
			if (connState !== 1) {
				logger.warn('MongoDB connection status', {
					state: states[connState],
					host: mongoose.connection.host,
					name: mongoose.connection.name,
					readyState: connState
				});
			}
		}, 300000); 
		
		logger.info('MongoDB connected successfully', {
			host: db.connection.host,
			name: db.connection.name,
			maxPoolSize: db.connection.options?.maxPoolSize,
			minPoolSize: db.connection.options?.minPoolSize
		});
	} catch (error) {
		logger.error('MongoDB connection failed', {
			error: error.message,
			stack: error.stack,
		});
		process.exit(1);
	}
};

const disconnectDB = async () => {
	try {
		await mongoose.connection.close(true);
		logger.info('MongoDB disconnected successfully');
	} catch (error) {
		logger.error('MongoDB disconnect failed', {
			error: error.message,
			stack: error.stack,
		});
	}
};

export { connectDB, disconnectDB };
