// api/health.js
import { Pool } from 'pg';
const pool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } });
export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  let db = 'unknown';
  try {
    await pool.query('SELECT NOW()');
    db = 'connected';
  } catch (e) {
    db = `error: ${String(e.message || e)}`;
  }
  return res.status(200).json({
    status: 'healthy',
    service: 'Assaí Dashboard API',
    timestamp: new Date().toISOString(),
    database: db,
    endpoints: {
      main: '/api/assai/dashboard',
      health: '/api/health',
      usage: '?endpoint=summary&start_date=2025-12-01&end_date=2025-12-02'
    }
  });
}