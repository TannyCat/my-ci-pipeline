const express = require('express');
const { Pool } = require('pg');
const mqtt = require('mqtt');
const cors = require('cors');
require('dotenv').config();

const app = express();
app.use(cors());
app.use(express.json());

// Database Configuration
const pool = new Pool({
  user: process.env.DB_USER,
  host: process.env.DB_HOST,
  database: process.env.DB_NAME,
  password: process.env.DB_PASSWORD,
  port: 5432,
  connectionTimeoutMillis: 5000,
  idleTimeoutMillis: 30000,
  max: 20
});

// MQTT Client
const mqttClient = mqtt.connect(process.env.MQTT_BROKER_URL);

// Health Check Endpoint
app.get('/api/health', (req, res) => {
  res.status(200).json({
    status: 'healthy',
    db_connected: !!pool,
    mqtt_connected: mqttClient.connected
  });
});

// Initialize Database
async function initDB() {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS meter_data (
        id SERIAL PRIMARY KEY,
        meter_id VARCHAR(50) NOT NULL,
        kwh DECIMAL(10,2) NOT NULL,
        voltage DECIMAL(10,2),
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )`);
    console.log('Database initialized');
  } catch (err) {
    console.error('Database initialization error:', err);
    throw err;
  }
}

// Test Database Connection
async function testConnection() {
  let retries = parseInt(process.env.DB_CONNECT_RETRIES || 5);
  while (retries > 0) {
    try {
      await pool.query('SELECT 1');
      console.log('Database connected successfully');
      return;
    } catch (err) {
      console.error(`Database connection failed (${retries} retries left):`, err.message);
      retries--;
      await new Promise(res => setTimeout(res, 5000));
    }
  }
  throw new Error('Unable to connect to database after retries');
}

// MQTT Handler
mqttClient.on('connect', () => {
  console.log('Connected to MQTT Broker');
  mqttClient.subscribe('meter/data');
});

mqttClient.on('message', async (topic, message) => {
  if (topic === 'meter/data') {
    try {
      const data = JSON.parse(message.toString());
      const { meter_id, kwh, voltage } = data;

      const result = await pool.query(
        'INSERT INTO meter_data (meter_id, kwh, voltage) VALUES ($1, $2, $3) RETURNING *',
        [meter_id, kwh, voltage]
      );
      console.log('Data saved from MQTT:', result.rows[0]);
    } catch (err) {
      console.error('MQTT Processing Error:', err);
    }
  }
});

// API Routes
app.post('/api/meter-data', async (req, res) => {
  try {
    const { meter_id, kwh, voltage } = req.body;
    const result = await pool.query(
      'INSERT INTO meter_data (meter_id, kwh, voltage) VALUES ($1, $2, $3) RETURNING *',
      [meter_id, kwh, voltage]
    );
    res.status(201).json(result.rows[0]);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Database operation failed' });
  }
});

app.get('/api/meter-data', async (req, res) => {
  try {
    const result = await pool.query(
      'SELECT * FROM meter_data ORDER BY timestamp DESC LIMIT 100'
    );
    res.json(result.rows);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Database query failed' });
  }
});

app.get('/api/meter-data/summary', async (req, res) => {
  try {
    const summary = await pool.query(`
      SELECT
        meter_id,
        COUNT(*) as readings,
        AVG(kwh) as avg_kwh,
        AVG(voltage) as avg_voltage,
        MAX(timestamp) as last_reading
      FROM meter_data
      GROUP BY meter_id
    `);
    res.json(summary.rows);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Database query failed' });
  }
});

// Start Server
const PORT = process.env.PORT || 3000;

async function startServer() {
  try {
    await testConnection();
    await initDB();

    app.listen(PORT, () => {
      console.log(`Server running on port ${PORT}`);
      console.log(`MQTT Broker: ${process.env.MQTT_BROKER_URL}`);
      console.log(`Database: ${process.env.DB_HOST}:5432/${process.env.DB_NAME}`);
    });
  } catch (err) {
    console.error('Failed to start server:', err);
    process.exit(1);
  }
}

startServer();