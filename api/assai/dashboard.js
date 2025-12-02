// api/assai/dashboard.js - API ÚNICA PARA O DASHBOARD ASSAÍ
import { Pool } from 'pg';

// Configurar conexão com PostgreSQL (Render)
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

// Token da DisplayForce
const DISPLAYFORCE_TOKEN = process.env.DISPLAYFORCE_API_TOKEN || '4AUH-BX6H-G2RJ-G7PB';
const DISPLAYFORCE_BASE = process.env.DISPLAYFORCE_API_URL || 'https://api.displayforce.ai/public/v1';

export default async function handler(req, res) {
  // Configurar CORS
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  
  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }
  
  const { endpoint, start_date, end_date, store_id, source } = req.query;
  
  try {
    console.log(`📊 API Request: ${endpoint || 'none'}`);
    
    switch (endpoint) {
      case 'visitors':
        return await getVisitors(req, res, start_date, end_date, store_id);
      
      case 'summary':
        return await getSummary(req, res, start_date, end_date);
      
      case 'stores':
        return await getStores(req, res);
      
      case 'devices':
        return await getDevices(req, res);
      
      case 'test':
        return res.status(200).json({
          success: true,
          message: 'API Assaí está funcionando!',
          endpoints: ['visitors', 'summary', 'stores', 'devices', 'test'],
          timestamp: new Date().toISOString()
        });
      
      default:
        return res.status(200).json({
          success: true,
          message: 'API Assaí Dashboard',
          usage: 'Use ?endpoint=summary&start_date=2025-12-01&end_date=2025-12-02',
          available_endpoints: [
            'visitors - Lista de visitantes',
            'summary - Resumo do dashboard',
            'stores - Lista de lojas',
            'devices - Dispositivos da DisplayForce',
            'test - Teste da API'
          ]
        });
    }
    
  } catch (error) {
    console.error('🔥 API Error:', error);
    return res.status(500).json({
      success: false,
      error: 'Internal server error',
      message: error.message
    });
  }
}

// ===========================================
// 1. VISITANTES
// ===========================================
async function getVisitors(req, res, start_date, end_date, store_id) {
  try {
    if (req.query.source === 'displayforce') {
      return await getVisitorsFromDisplayForce(res, start_date, end_date, store_id);
    }
    let query = `
      SELECT 
        visitor_id as id,
        day as day,
        store_id,
        store_name,
        timestamp,
        gender,
        age,
        day_of_week,
        smile
      FROM visitors
      WHERE 1=1
    `;
    
    const params = [];
    let paramCount = 1;
    
    if (start_date) {
      query += ` AND day >= ${paramCount}`;
      params.push(start_date);
      paramCount++;
    }
    
    if (end_date) {
      query += ` AND day <= ${paramCount}`;
      params.push(end_date);
      paramCount++;
    }
    
    if (store_id && store_id !== 'all') {
      query += ` AND store_id = ${paramCount}`;
      params.push(store_id);
      paramCount++;
    }
    
    query += ` ORDER BY timestamp DESC LIMIT 100`;
    
    console.log('📋 Query:', query.substring(0, 150));
    
    const result = await pool.query(query, params);
    
    if (result.rows.length > 0) {
      const visitors = result.rows.map(row => ({
        id: row.id,
        date: row.day,
        store_id: row.store_id,
        store_name: row.store_name || `Loja ${row.store_id}`,
        timestamp: row.timestamp,
        gender: row.gender === 'M' ? 'Masculino' : 'Feminino',
        age: row.age,
        day_of_week: row.day_of_week,
        smile: row.smile
      }));
      return res.status(200).json({
        success: true,
        data: visitors,
        count: visitors.length,
        source: 'database',
        query: { start_date, end_date, store_id }
      });
    }
    
    return await getVisitorsFromDisplayForce(res, start_date, end_date, store_id);
    
  } catch (error) {
    console.error('❌ Visitors error:', error);
    
    // Fallback com dados simulados
    const fallbackData = Array.from({ length: 20 }, (_, i) => ({
      id: `visitor-${i}`,
      date: '2025-12-02',
      store_id: 15287,
      store_name: 'Loja Principal',
      timestamp: `2025-12-02T${10 + Math.floor(i/5)}:${(i%5)*10}:00`,
      gender: i % 2 === 0 ? 'Masculino' : 'Feminino',
      age: 25 + (i % 30),
      day_of_week: 'Ter',
      smile: i % 3 === 0
    }));
    
    return res.status(200).json({
      success: true,
      data: fallbackData,
      count: fallbackData.length,
      isFallback: true,
      error: error.message
    });
  }
}

// ===========================================
// 2. RESUMO DO DASHBOARD
// ===========================================
async function getSummary(req, res, start_date, end_date) {
  try {
    const { store_id, source } = req.query;
    if (source === 'displayforce') {
      const s = start_date || new Date().toISOString().slice(0,10);
      const e = end_date || s;
      const days = [];
      let d = new Date(`${s}T00:00:00Z`);
      const endD = new Date(`${e}T00:00:00Z`);
      while (d <= endD) { days.push(d.toISOString().slice(0,10)); d = new Date(d.getTime() + 86400000); }
      const map = ['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'];
      let total = 0, male = 0, female = 0, avgSum = 0, avgCount = 0;
      const visitsByDay = { Sunday:0, Monday:0, Tuesday:0, Wednesday:0, Thursday:0, Friday:0, Saturday:0 };
      for (const day of days) {
        let offset = 0;
        const limitReq = 500;
        while (true) {
          const bodyPayload = { start: `${day}T00:00:00Z`, end: `${day}T23:59:59Z`, limit: limitReq, offset, tracks: true };
          if (store_id && store_id !== 'all') bodyPayload.device_id = store_id;
          const response = await fetch(`${DISPLAYFORCE_BASE}/stats/visitor/list`, { method: 'POST', headers: { 'X-API-Token': DISPLAYFORCE_TOKEN, 'Content-Type': 'application/json' }, body: JSON.stringify(bodyPayload) });
          if (!response.ok) { const t = await response.text().catch(()=>"" ); throw new Error(`DF stats ${response.status} ${t}`); }
          const page = await response.json();
          const arr = Array.isArray(page.payload || page.data) ? (page.payload || page.data) : [];
          for (const v of arr) {
            total++;
            if (v.sex === 1) male++; else female++;
            const age = Number(v.age || 0); if (age>0) { avgSum += age; avgCount++; }
            const ts = String(v.start || v.tracks?.[0]?.start || new Date().toISOString());
            const wd = map[new Date(ts).getUTCDay()];
            visitsByDay[wd] = (visitsByDay[wd] || 0) + 1;
          }
          const pg = page.pagination; const pageLimit = Number(pg?.limit ?? limitReq);
          if (pg?.total && total >= Number(pg.total)) break;
          if (arr.length < pageLimit) break;
          offset += pageLimit;
        }
      }
      return res.status(200).json({
        success: true,
        totalVisitors: total,
        totalMale: male,
        totalFemale: female,
        averageAge: avgCount ? Math.round(avgSum / avgCount) : 0,
        visitsByDay
      });
    }
    // Buscar dados do dashboard diário
    let query = `
      SELECT
        COALESCE(SUM(total_visitors),0) as total,
        COALESCE(SUM(male),0) as male,
        COALESCE(SUM(female),0) as female,
        COALESCE(SUM(avg_age_sum),0) as avg_age_sum,
        COALESCE(SUM(avg_age_count),0) as avg_age_count,
        COALESCE(SUM(monday),0) as monday,
        COALESCE(SUM(tuesday),0) as tuesday,
        COALESCE(SUM(wednesday),0) as wednesday,
        COALESCE(SUM(thursday),0) as thursday,
        COALESCE(SUM(friday),0) as friday,
        COALESCE(SUM(saturday),0) as saturday,
        COALESCE(SUM(sunday),0) as sunday
      FROM dashboard_daily
      WHERE 1=1
    `;

    const params = [];
    let paramCount = 1;

    if (start_date) {
      query += ` AND day >= ${paramCount}`;
      params.push(start_date);
      paramCount++;
    }

    if (end_date) {
      query += ` AND day <= ${paramCount}`;
      params.push(end_date);
      paramCount++;
    }

    if (store_id && store_id !== 'all') {
      query += ` AND store_id = ${paramCount}`;
      params.push(store_id);
      paramCount++;
    }

    console.log('📊 Summary query:', query);

    const result = await pool.query(query, params);
    const row = result.rows[0] || {};
    const avgAgeValue = Number(row.avg_age_count || 0) > 0 ? Number(row.avg_age_sum || 0) / Number(row.avg_age_count || 0) : 0;

    const total = Number(row.total || 0);
    const male = Number(row.male || 0);
    const female = Number(row.female || 0);
    const avgAge = Math.round(avgAgeValue);

    const visitsByDay = {
      Monday: Number(row.monday || 0),
      Tuesday: Number(row.tuesday || 0),
      Wednesday: Number(row.wednesday || 0),
      Thursday: Number(row.thursday || 0),
      Friday: Number(row.friday || 0),
      Saturday: Number(row.saturday || 0),
      Sunday: Number(row.sunday || 0)
    };

    return res.status(200).json({
      success: true,
      totalVisitors: total,
      totalMale: male,
      totalFemale: female,
      averageAge: avgAge,
      visitsByDay,
      genderDistribution: {
        male: total > 0 ? Math.round((male / total) * 1000) / 10 : 0,
        female: total > 0 ? Math.round((female / total) * 1000) / 10 : 0
      },
      peakHours: ["14:00-15:00", "15:00-16:00", "16:00-17:00"],
      query: { start_date, end_date, store_id: store_id || 'all' }
    });
    
  } catch (error) {
    console.error('❌ Summary error:', error);
    
    // Fallback
    return res.status(200).json({
      success: true,
      totalVisitors: 7466,
      totalMale: 5054,
      totalFemale: 2412,
      averageAge: 31,
      visitsByDay: {
        "Monday": 1200, "Tuesday": 1350, "Wednesday": 1100,
        "Thursday": 1450, "Friday": 1600, "Saturday": 2000, "Sunday": 800
      },
      genderDistribution: { male: 67.7, female: 32.3 },
      peakHours: ["14:00-15:00", "15:00-16:00", "16:00-17:00"],
      isFallback: true
    });
  }
}

// ===========================================
// 3. LOJAS
// ===========================================
async function getStores(req, res) {
  try {
    const query = `
      SELECT DISTINCT 
        store_id as id,
        store_name as name,
        COUNT(*) as visitor_count
      FROM visitors
      WHERE store_id IS NOT NULL
      GROUP BY store_id, store_name
      ORDER BY visitor_count DESC
      LIMIT 10
    `;
    
    const result = await pool.query(query);
    
    const stores = result.rows.map(row => ({
      id: row.id,
      name: row.name || `Loja ${row.id}`,
      visitor_count: parseInt(row.visitor_count),
      status: 'active'
    }));
    
    return res.status(200).json({
      success: true,
      stores: stores,
      count: stores.length
    });
    
  } catch (error) {
    console.error('❌ Stores error:', error);
    
    return res.status(200).json({
      success: true,
      stores: [
        { id: 15287, name: 'Loja Principal', visitor_count: 5000, status: 'active' },
        { id: 15288, name: 'Loja Norte', visitor_count: 1500, status: 'active' },
        { id: 15289, name: 'Loja Sul', visitor_count: 966, status: 'active' }
      ],
      isFallback: true
    });
  }
}

// ===========================================
// 4. DISPOSITIVOS (DisplayForce)
// ===========================================
async function getDevices(req, res) {
  try {
    console.log('🌐 Calling DisplayForce API...');
    
    let response = await fetch(`${DISPLAYFORCE_BASE}/device/list`, {
      method: 'POST',
      headers: {
        'X-API-Token': DISPLAYFORCE_TOKEN,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
      },
      body: JSON.stringify({})
    });
    
    if (!response.ok && response.status === 405) {
      response = await fetch(`${DISPLAYFORCE_BASE}/device/list`, {
        method: 'GET',
        headers: {
          'X-API-Token': DISPLAYFORCE_TOKEN,
          'Accept': 'application/json'
        }
      });
    }
    
    if (!response.ok) {
      throw new Error(`DisplayForce: ${response.status}`);
    }
    
    const data = await response.json();
    
    return res.status(200).json({
      success: true,
      devices: data.devices || data.data || [],
      source: 'displayforce'
    });
    
  } catch (error) {
    console.error('❌ Devices error:', error);
    
    return res.status(200).json({
      success: true,
      devices: [
        {
          id: 1,
          name: "Sensor Entrada",
          status: "active",
          location: "Loja Principal",
          last_seen: new Date().toISOString()
        },
        {
          id: 2,
          name: "Sensor Eletros",
          status: "active",
          location: "Loja Norte", 
          last_seen: new Date().toISOString()
        }
      ],
      isFallback: true,
      error: error.message
    });
  }
}