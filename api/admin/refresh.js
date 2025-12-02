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
    const { start, end, deviceId } = req.query;
    const storeId = deviceId ? String(deviceId) : "all";
    const s = start || new Date().toISOString().slice(0,10);
    const e = end || s;
    
    const days = [];
    let d = new Date(`${s}T00:00:00Z`);
    const endD = new Date(`${e}T00:00:00Z`);
    
    while (d <= endD) {
      days.push(d.toISOString().slice(0, 10));
      d = new Date(d.getTime() + 86400000);
    }
    
    // Simulação - em produção você integraria com a API DisplayForce
    return res.status(200).json({
      ok: true,
      days: days.length,
      storeId,
      message: `Refresh agendado para ${days.length} dias (simulação)`
    });
    
  } catch (error) {
    console.error('Erro em /api/admin/refresh:', error);
    return res.status(500).json({ error: error.message });
  }
}