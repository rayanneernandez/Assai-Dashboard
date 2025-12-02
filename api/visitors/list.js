import { Pool } from 'pg';

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

export default async function handler(req, res) {
  if (req.method !== 'GET') {
    return res.status(405).json({ error: 'Método não permitido' });
  }

  try {
    const { start, end, deviceId, page = "1", pageSize = "40" } = req.query;
    
    if (!start || !end) {
      return res.status(400).json({ error: 'start e end YYYY-MM-DD são obrigatórios' });
    }
    
    const p = Math.max(1, parseInt(page) || 1);
    const ps = Math.min(1000, Math.max(1, parseInt(pageSize) || 40));
    const offset = (p - 1) * ps;
    
    let whereConditions = ["day_date >= $1", "day_date <= $2"];
    const params = [start, end];
    
    if (deviceId && deviceId !== "all") {
      whereConditions.push("store_id = $3");
      params.push(String(deviceId));
    }
    
    const whereClause = whereConditions.join(" AND ");
    
    // Count
    const countQuery = `SELECT COUNT(*)::int AS total FROM visitors WHERE ${whereClause}`;
    const countResult = await pool.query(countQuery, params);
    const total = countResult.rows[0]?.total ?? 0;
    
    // Data
    const dataParams = [...params];
    const dataQuery = `
      SELECT 
        visitor_id, 
        day_date, 
        timestamp, 
        store_id, 
        store_name, 
        gender, 
        age, 
        day_of_week, 
        smile 
      FROM visitors 
      WHERE ${whereClause}
      ORDER BY timestamp DESC
      LIMIT $${dataParams.length + 1} OFFSET $${dataParams.length + 2}
    `;
    
    dataParams.push(ps, offset);
    const dataResult = await pool.query(dataQuery, dataParams);
    
    return res.status(200).json({
      items: dataResult.rows,
      total,
      page: p,
      pageSize: ps,
      totalPages: Math.ceil(total / ps)
    });
    
  } catch (error) {
    console.error('Erro em /api/visitors/list:', error);
    return res.status(500).json({ error: error.message });
  }
}