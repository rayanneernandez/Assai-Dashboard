// api/_express.js (arquivo principal na raiz da pasta /api)
import express from 'express';
import cors from 'cors';
import { Pool } from 'pg';

const app = express();
app.use(cors());
app.use(express.json());

// Config do PostgreSQL (use variáveis de ambiente)
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

// Suas rotas existentes
app.get('/api/visitors/list', async (req, res) => {
  const { start, end, page = 1, pageSize = 20 } = req.query;
  
  try {
    const result = await pool.query(
      `SELECT * FROM visitors 
       WHERE entry_time BETWEEN $1 AND $2 
       ORDER BY entry_time DESC 
       LIMIT $3 OFFSET $4`,
      [start, end, pageSize, (page - 1) * pageSize]
    );
    
    res.json(result.rows);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Export como função serverless do Vercel
export default app;