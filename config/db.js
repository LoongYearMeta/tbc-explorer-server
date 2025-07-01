import mongoose from 'mongoose';
import dotenv from 'dotenv';

import logger from '../config/logger.js';

dotenv.config();

const connectDB = async () => {
	try {
		const db = await mongoose.connect(process.env.MONGO);
		logger.info('MongoDB connected successfully', {
			host: db.connection.host,
			name: db.connection.name,
		});
	} catch (error) {
		logger.error('MongoDB connection failed', {
			error: error.message,
			stack: error.stack,
		});
		process.exit(1);
	}
};

export default connectDB;
