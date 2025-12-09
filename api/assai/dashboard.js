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
        return await getSummary(req, res, start_date, end_date, store_id);
      
      case 'stores':
        return await getStores(req, res);
      
      case 'devices':
        return await getDevices(req, res);
      
      case 'refresh':
        return await refreshRange(req, res, start_date, end_date, store_id);
      
      case 'refresh_all':
        return await refreshAll(req, res, start_date, end_date);
      
      case 'auto_refresh':
        return await autoRefresh(req, res);
      
      case 'optimize':
        return await ensureIndexes(req, res);
      
      case 'test':
        return res.status(200).json({
          success: true,
          message: 'API Assaí está funcionando!',
          endpoints: ['visitors', 'summary', 'stores', 'devices', 'refresh', 'refresh_all', 'auto_refresh', 'optimize', 'test'],
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
            'refresh - Preenche banco a partir da DisplayForce (uma loja ou todas)',
            'refresh_all - Atualiza todas as lojas para o período selecionado',
            'auto_refresh - Atualiza automaticamente o dia anterior para todas as lojas',
            'optimize - Cria índices de performance',
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
    // Se pedirem explicitamente displayforce, mantém
    if (req.query.source === "displayforce") {
      return await getVisitorsFromDisplayForce(
        res,
        start_date,
        end_date,
        store_id
      );
    }

    let query = `
      SELECT 
        visitor_id AS id,
        day,
        store_id,
        store_name,
        timestamp,
        gender,
        age,
        day_of_week,
        smile,
        hour
      FROM visitors
      WHERE 1=1
    `;

    const params = [];
    let paramCount = 1;

    if (start_date) {
      query += ` AND day >= $${paramCount}`;
      params.push(start_date);
      paramCount++;
    }

    if (end_date) {
      query += ` AND day <= $${paramCount}`;
      params.push(end_date);
      paramCount++;
    }

    if (store_id && store_id !== "all") {
      query += ` AND store_id = $${paramCount}`;
      params.push(store_id);
      paramCount++;
    }

    query += ` ORDER BY timestamp DESC LIMIT 1000`;

    console.log("📋 Visitors query:", query, params);

    const result = await pool.query(query, params);

    return res.status(200).json({
      success: true,
      data: result.rows,
      count: result.rows.length
    });
  } catch (error) {
    console.error("❌ Visitors error:", error);

    return res.status(200).json({
      success: true,
      data: [],
      isFallback: true,
      error: error.message
    });
  }
}

async function getVisitorsFromDisplayForce(res, start_date, end_date, store_id) {
  const tz = parseInt(process.env.TIMEZONE_OFFSET_HOURS || "-3", 10);
  const sign = tz >= 0 ? "+" : "-";
  const hh = String(Math.abs(tz)).padStart(2, "0");
  const tzStr = `${sign}${hh}:00`;
  const startISO = `${(start_date || new Date().toISOString().split('T')[0])}T00:00:00${tzStr}`;
  const endISO = `${(end_date || new Date().toISOString().split('T')[0])}T23:59:59${tzStr}`;
  const LIMIT_REQ = 500;
  let offset = 0;
  const all = [];
  
  while (true) {
    const bodyPayload = {
      start: startISO,
      end: endISO,
      limit: LIMIT_REQ,
      offset,
      tracks: true,
      face_quality: true,
      glasses: true,
      facial_hair: true,
      hair_color: true,
      hair_type: true,
      headwear: true,
    };
    
    if (store_id && store_id !== 'all') bodyPayload.devices = [store_id];
    
    const response = await fetch(`${DISPLAYFORCE_BASE}/stats/visitor/list`, {
      method: 'POST',
      headers: { 'X-API-Token': DISPLAYFORCE_TOKEN, 'Content-Type': 'application/json' },
      body: JSON.stringify(bodyPayload)
    });
    
    if (!response.ok) {
      const body = await response.text().catch(() => '');
      throw new Error(`DisplayForce stats: ${response.status} ${response.statusText} ${body}`);
    }
    
    const page = await response.json();
    const payload = page.payload || page.data || [];
    const arr = Array.isArray(payload) ? payload : [];
    all.push(...arr);
    
    const pg = page.pagination;
    const pageLimit = Number(pg?.limit ?? LIMIT_REQ);
    if (pg?.total && all.length >= Number(pg.total)) break;
    if (arr.length < pageLimit) break;
    offset += pageLimit;
  }
  
  const DAYS = ['Dom','Seg','Ter','Qua','Qui','Sex','Sáb'];
  const visitors = [];
  
  for (const v of all) {
    try {
      const ts = String(v.start ?? v.tracks?.[0]?.start ?? new Date().toISOString());
      const d = new Date(ts);
      const isoUTC = d.toISOString();
      const local = new Date(d.getTime() + tz * 3600000);
      const di = local.getDay();
      const day_of_week = DAYS[di];
      
      const attrsA = Array.isArray(v.additional_attributes) ? v.additional_attributes : [];
      const attrsB = Array.isArray(v.additional_atributes) ? v.additional_atributes : [];
      const attrs = [...attrsA, ...attrsB];
      const last = attrs.length ? attrs[attrs.length - 1] : {};
      
      const smile = String(last?.smile ?? v.smile ?? '').toLowerCase() === 'yes';
      const ageVal = v.age ?? last?.age ?? 0;
      const deviceId = String(v.tracks?.[0]?.device_id ?? (Array.isArray(v.devices) ? v.devices[0] : ''));
      const hour = local.getHours();
      const dateStr = local.toISOString().slice(0,10);
      
      const visitor = {
        id: String(v.visitor_id ?? v.session_id ?? v.id ?? ''),
        day: dateStr,
        store_id: deviceId,
        store_name: `Loja ${deviceId}`,
        timestamp: isoUTC,
        gender: (v.sex === 1 ? 'M' : 'F'),
        age: Number(ageVal || 0),
        day_of_week,
        smile,
        hour: hour
      };
      
      visitors.push(visitor);
      
      // Insere no banco imediatamente
      await pool.query(
        `INSERT INTO public.visitors (visitor_id, day, store_id, store_name, timestamp, gender, age, day_of_week, smile, hour)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) 
         ON CONFLICT (visitor_id, timestamp) DO UPDATE SET
           day = EXCLUDED.day,
           store_id = EXCLUDED.store_id,
           store_name = EXCLUDED.store_name,
           gender = EXCLUDED.gender,
           age = EXCLUDED.age,
           day_of_week = EXCLUDED.day_of_week,
           smile = EXCLUDED.smile,
           hour = EXCLUDED.hour`,
        [
          visitor.id, 
          visitor.day, 
          visitor.store_id, 
          visitor.store_name, 
          visitor.timestamp,
          visitor.gender,
          visitor.age,
          visitor.day_of_week,
          visitor.smile,
          visitor.hour
        ]
      );
    } catch (e) {
      console.error('❌ Erro ao processar visitante:', e.message);
    }
  }
  
  return res.status(200).json({ 
    success: true, 
    data: visitors, 
    count: visitors.length, 
    source: 'displayforce', 
    query: { start_date, end_date, store_id } 
  });
}

// ===========================================
// 2. RESUMO DO DASHBOARD (CORRIGIDO)
// ===========================================
async function getSummary(req, res, start_date, end_date, store_id) {
  try {
    // Primeiro, tenta buscar dos dados agregados (dashboard_daily)
    let query = `
      SELECT 
        COALESCE(SUM(total_visitors), 0) AS total_visitors,
        COALESCE(SUM(male), 0) AS total_male,
        COALESCE(SUM(female), 0) AS total_female,
        COALESCE(SUM(avg_age_sum), 0) AS avg_age_sum,
        COALESCE(SUM(avg_age_count), 0) AS avg_age_count,
        COALESCE(SUM(age_18_25), 0) AS age_18_25,
        COALESCE(SUM(age_26_35), 0) AS age_26_35,
        COALESCE(SUM(age_36_45), 0) AS age_36_45,
        COALESCE(SUM(age_46_60), 0) AS age_46_60,
        COALESCE(SUM(age_60_plus), 0) AS age_60_plus,
        COALESCE(SUM(sunday), 0) AS sunday,
        COALESCE(SUM(monday), 0) AS monday,
        COALESCE(SUM(tuesday), 0) AS tuesday,
        COALESCE(SUM(wednesday), 0) AS wednesday,
        COALESCE(SUM(thursday), 0) AS thursday,
        COALESCE(SUM(friday), 0) AS friday,
        COALESCE(SUM(saturday), 0) AS saturday
      FROM dashboard_daily
      WHERE 1=1
    `;

    const params = [];
    let paramCount = 1;

    if (start_date) {
      query += ` AND day >= $${paramCount}`;
      params.push(start_date);
      paramCount++;
    }

    if (end_date) {
      query += ` AND day <= $${paramCount}`;
      params.push(end_date);
      paramCount++;
    }

    if (store_id && store_id !== "all") {
      query += ` AND store_id = $${paramCount}`;
      params.push(store_id);
      paramCount++;
    } else {
      query += ` AND store_id = 'all'`;
    }

    console.log("📊 Summary query:", query, params);
    
    const result = await pool.query(query, params);
    const row = result.rows[0] || {};
    
    // Se não houver dados nos agregados, busca dos dados brutos
    if (Number(row.total_visitors || 0) === 0) {
      console.log("📊 Buscando dados diretamente da tabela visitors...");
      return await getSummaryFromVisitors(res, start_date, end_date, store_id);
    }
    
    // Calcula idade média
    const avgAgeCount = Number(row.avg_age_count || 0);
    const averageAge = avgAgeCount > 0 ? Math.round(Number(row.avg_age_sum || 0) / avgAgeCount) : 0;
    
    // Busca dados por hora
    const hourlyData = await getHourlyData(start_date, end_date, store_id);
    
    // Busca distribuição por idade e gênero
    const ageGenderData = await getAgeGenderData(start_date, end_date, store_id);
    
    const response = {
      success: true,
      totalVisitors: Number(row.total_visitors || 0),
      totalMale: Number(row.total_male || 0),
      totalFemale: Number(row.total_female || 0),
      averageAge: averageAge,
      visitsByDay: {
        Sunday: Number(row.sunday || 0),
        Monday: Number(row.monday || 0),
        Tuesday: Number(row.tuesday || 0),
        Wednesday: Number(row.wednesday || 0),
        Thursday: Number(row.thursday || 0),
        Friday: Number(row.friday || 0),
        Saturday: Number(row.saturday || 0),
      },
      byAgeGroup: {
        "18-25": Number(row.age_18_25 || 0),
        "26-35": Number(row.age_26_35 || 0),
        "36-45": Number(row.age_36_45 || 0),
        "46-60": Number(row.age_46_60 || 0),
        "60+": Number(row.age_60_plus || 0),
      },
      byAgeGender: ageGenderData,
      byHour: hourlyData.byHour,
      byGenderHour: hourlyData.byGenderHour,
      isFallback: false,
      source: 'database'
    };

    return res.status(200).json(response);
  } catch (error) {
    console.error("❌ Summary error:", error);
    return await getSummaryFromVisitors(res, start_date, end_date, store_id);
  }
}

async function getSummaryFromVisitors(res, start_date, end_date, store_id) {
  try {
    let query = `
      SELECT 
        COUNT(*) AS total_visitors,
        SUM(CASE WHEN gender = 'M' THEN 1 ELSE 0 END) AS total_male,
        SUM(CASE WHEN gender = 'F' THEN 1 ELSE 0 END) AS total_female,
        SUM(age) AS avg_age_sum,
        SUM(CASE WHEN age > 0 THEN 1 ELSE 0 END) AS avg_age_count,
        SUM(CASE WHEN age BETWEEN 18 AND 25 THEN 1 ELSE 0 END) AS age_18_25,
        SUM(CASE WHEN age BETWEEN 26 AND 35 THEN 1 ELSE 0 END) AS age_26_35,
        SUM(CASE WHEN age BETWEEN 36 AND 45 THEN 1 ELSE 0 END) AS age_36_45,
        SUM(CASE WHEN age BETWEEN 46 AND 60 THEN 1 ELSE 0 END) AS age_46_60,
        SUM(CASE WHEN age > 60 THEN 1 ELSE 0 END) AS age_60_plus,
        SUM(CASE WHEN day_of_week = 'Dom' THEN 1 ELSE 0 END) AS sunday,
        SUM(CASE WHEN day_of_week = 'Seg' THEN 1 ELSE 0 END) AS monday,
        SUM(CASE WHEN day_of_week = 'Ter' THEN 1 ELSE 0 END) AS tuesday,
        SUM(CASE WHEN day_of_week = 'Qua' THEN 1 ELSE 0 END) AS wednesday,
        SUM(CASE WHEN day_of_week = 'Qui' THEN 1 ELSE 0 END) AS thursday,
        SUM(CASE WHEN day_of_week = 'Sex' THEN 1 ELSE 0 END) AS friday,
        SUM(CASE WHEN day_of_week = 'Sáb' THEN 1 ELSE 0 END) AS saturday
      FROM visitors
      WHERE 1=1
    `;

    const params = [];
    let paramCount = 1;

    if (start_date) {
      query += ` AND day >= $${paramCount}`;
      params.push(start_date);
      paramCount++;
    }

    if (end_date) {
      query += ` AND day <= $${paramCount}`;
      params.push(end_date);
      paramCount++;
    }

    if (store_id && store_id !== "all") {
      query += ` AND store_id = $${paramCount}`;
      params.push(store_id);
      paramCount++;
    }

    console.log("📊 Summary from visitors query:", query, params);
    
    const result = await pool.query(query, params);
    const row = result.rows[0] || {};
    
    // Calcula idade média
    const avgAgeCount = Number(row.avg_age_count || 0);
    const averageAge = avgAgeCount > 0 ? Math.round(Number(row.avg_age_sum || 0) / avgAgeCount) : 0;
    
    // Busca dados por hora
    const hourlyData = await getHourlyData(start_date, end_date, store_id);
    
    // Busca distribuição por idade e gênero
    const ageGenderData = await getAgeGenderData(start_date, end_date, store_id);
    
    const response = {
      success: true,
      totalVisitors: Number(row.total_visitors || 0),
      totalMale: Number(row.total_male || 0),
      totalFemale: Number(row.total_female || 0),
      averageAge: averageAge,
      visitsByDay: {
        Sunday: Number(row.sunday || 0),
        Monday: Number(row.monday || 0),
        Tuesday: Number(row.tuesday || 0),
        Wednesday: Number(row.wednesday || 0),
        Thursday: Number(row.thursday || 0),
        Friday: Number(row.friday || 0),
        Saturday: Number(row.saturday || 0),
      },
      byAgeGroup: {
        "18-25": Number(row.age_18_25 || 0),
        "26-35": Number(row.age_26_35 || 0),
        "36-45": Number(row.age_36_45 || 0),
        "46-60": Number(row.age_46_60 || 0),
        "60+": Number(row.age_60_plus || 0),
      },
      byAgeGender: ageGenderData,
      byHour: hourlyData.byHour,
      byGenderHour: hourlyData.byGenderHour,
      isFallback: true,
      source: 'visitors_table'
    };

    return res.status(200).json(response);
  } catch (error) {
    console.error("❌ Summary from visitors error:", error);
    
    // Fallback completo
    const byHour = {};
    const byGenderHour = { male: {}, female: {} };
    
    for (let h = 0; h < 24; h++) {
      byHour[h] = 0;
      byGenderHour.male[h] = 0;
      byGenderHour.female[h] = 0;
    }
    
    return res.status(200).json({
      success: true,
      totalVisitors: 0,
      totalMale: 0,
      totalFemale: 0,
      averageAge: 0,
      visitsByDay: {
        Sunday: 0,
        Monday: 0,
        Tuesday: 0,
        Wednesday: 0,
        Thursday: 0,
        Friday: 0,
        Saturday: 0,
      },
      byAgeGroup: {
        "18-25": 0,
        "26-35": 0,
        "36-45": 0,
        "46-60": 0,
        "60+": 0,
      },
      byAgeGender: {
        "<20": { male: 0, female: 0 },
        "20-29": { male: 0, female: 0 },
        "30-45": { male: 0, female: 0 },
        ">45": { male: 0, female: 0 }
      },
      byHour: byHour,
      byGenderHour: byGenderHour,
      isFallback: true,
      source: 'fallback'
    });
  }
}

async function getHourlyData(start_date, end_date, store_id) {
  try {
    let query = `
      SELECT 
        hour,
        COUNT(*) AS total,
        SUM(CASE WHEN gender = 'M' THEN 1 ELSE 0 END) AS male,
        SUM(CASE WHEN gender = 'F' THEN 1 ELSE 0 END) AS female
      FROM visitors
      WHERE 1=1
    `;

    const params = [];
    let paramCount = 1;

    if (start_date) {
      query += ` AND day >= $${paramCount}`;
      params.push(start_date);
      paramCount++;
    }

    if (end_date) {
      query += ` AND day <= $${paramCount}`;
      params.push(end_date);
      paramCount++;
    }

    if (store_id && store_id !== "all") {
      query += ` AND store_id = $${paramCount}`;
      params.push(store_id);
      paramCount++;
    }

    query += ` GROUP BY hour ORDER BY hour ASC`;

    const result = await pool.query(query, params);
    
    const byHour = {};
    const byGenderHour = { male: {}, female: {} };
    
    // Inicializa todas as horas
    for (let h = 0; h < 24; h++) {
      byHour[h] = 0;
      byGenderHour.male[h] = 0;
      byGenderHour.female[h] = 0;
    }
    
    // Preenche com os dados do banco
    for (const row of result.rows) {
      const hour = Number(row.hour);
      byHour[hour] = Number(row.total || 0);
      byGenderHour.male[hour] = Number(row.male || 0);
      byGenderHour.female[hour] = Number(row.female || 0);
    }
    
    return { byHour, byGenderHour };
  } catch (error) {
    console.error("❌ Hourly data error:", error);
    
    const byHour = {};
    const byGenderHour = { male: {}, female: {} };
    
    for (let h = 0; h < 24; h++) {
      byHour[h] = 0;
      byGenderHour.male[h] = 0;
      byGenderHour.female[h] = 0;
    }
    
    return { byHour, byGenderHour };
  }
}

async function getAgeGenderData(start_date, end_date, store_id) {
  try {
    let query = `
      SELECT 
        gender,
        age
      FROM visitors
      WHERE age > 0
    `;

    const params = [];
    let paramCount = 1;

    if (start_date) {
      query += ` AND day >= $${paramCount}`;
      params.push(start_date);
      paramCount++;
    }

    if (end_date) {
      query += ` AND day <= $${paramCount}`;
      params.push(end_date);
      paramCount++;
    }

    if (store_id && store_id !== "all") {
      query += ` AND store_id = $${paramCount}`;
      params.push(store_id);
      paramCount++;
    }

    const result = await pool.query(query, params);
    
    const byAgeGender = {
      "<20": { male: 0, female: 0 },
      "20-29": { male: 0, female: 0 },
      "30-45": { male: 0, female: 0 },
      ">45": { male: 0, female: 0 }
    };
    
    for (const row of result.rows) {
      const gender = row.gender === 'M' ? 'male' : 'female';
      const age = Number(row.age || 0);
      
      if (age < 20) {
        byAgeGender["<20"][gender]++;
      } else if (age <= 29) {
        byAgeGender["20-29"][gender]++;
      } else if (age <= 45) {
        byAgeGender["30-45"][gender]++;
      } else {
        byAgeGender[">45"][gender]++;
      }
    }
    
    return byAgeGender;
  } catch (error) {
    console.error("❌ Age gender data error:", error);
    return {
      "<20": { male: 0, female: 0 },
      "20-29": { male: 0, female: 0 },
      "30-45": { male: 0, female: 0 },
      ">45": { male: 0, female: 0 }
    };
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
      WHERE store_id IS NOT NULL AND store_id != 'all'
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

// ===========================================
// 5. REFRESH - ATUALIZAÇÃO DE DADOS
// ===========================================
async function refreshRange(req, res, start_date, end_date, store_id) {
  try {
    const s = start_date || new Date().toISOString().slice(0, 10);
    const e = end_date || s;
    const days = [];
    let d = new Date(`${s}T00:00:00Z`);
    const endD = new Date(`${e}T00:00:00Z`);
    
    while (d <= endD) {
      days.push(d.toISOString().slice(0, 10));
      d = new Date(d.getTime() + 86400000);
    }
    
    console.log(`🔄 Refresh: ${days.length} dias, loja: ${store_id || 'all'}`);
    
    for (const day of days) {
      await refreshDayData(day, store_id);
    }
    
    return res.status(200).json({
      success: true,
      message: `Dados atualizados para ${days.length} dia(s)`,
      days: days.length,
      store_id: store_id || 'all',
      updated_at: new Date().toISOString()
    });
    
  } catch (error) {
    console.error('❌ Refresh error:', error);
    return res.status(500).json({
      success: false,
      error: error.message
    });
  }
}

async function refreshDayData(day, store_id) {
  const tz = parseInt(process.env.TIMEZONE_OFFSET_HOURS || "-3", 10);
  const sign = tz >= 0 ? "+" : "-";
  const hh = String(Math.abs(tz)).padStart(2, "0");
  const tzStr = `${sign}${hh}:00`;
  const startISO = `${day}T00:00:00${tzStr}`;
  const endISO = `${day}T23:59:59${tzStr}`;
  
  const LIMIT_REQ = 500;
  let offset = 0;
  const allVisitors = [];
  
  console.log(`📥 Buscando dados da DisplayForce para ${day}...`);
  
  // Busca todos os visitantes do dia
  while (true) {
    const bodyPayload = {
      start: startISO,
      end: endISO,
      limit: LIMIT_REQ,
      offset,
      tracks: true
    };
    
    if (store_id && store_id !== 'all') {
      bodyPayload.devices = [store_id];
    }
    
    const response = await fetch(`${DISPLAYFORCE_BASE}/stats/visitor/list`, {
      method: 'POST',
      headers: { 
        'X-API-Token': DISPLAYFORCE_TOKEN, 
        'Content-Type': 'application/json' 
      },
      body: JSON.stringify(bodyPayload)
    });
    
    if (!response.ok) {
      const text = await response.text();
      throw new Error(`DisplayForce API error: ${response.status} - ${text}`);
    }
    
    const data = await response.json();
    const visitors = data.payload || data.data || [];
    
    allVisitors.push(...visitors);
    
    const pagination = data.pagination;
    if (!pagination || visitors.length < LIMIT_REQ) {
      break;
    }
    
    offset += LIMIT_REQ;
  }
  
  console.log(`📊 ${allVisitors.length} visitantes encontrados para ${day}`);
  
  if (allVisitors.length === 0) {
    console.log(`⚠️ Nenhum visitante encontrado para ${day}`);
    return;
  }
  
  const DAYS = ['Dom', 'Seg', 'Ter', 'Qua', 'Qui', 'Sex', 'Sáb'];
  
  // Processa e insere visitantes
  for (const visitor of allVisitors) {
    try {
      const ts = visitor.start || visitor.tracks?.[0]?.start || new Date().toISOString();
      const date = new Date(ts);
      const isoUTC = date.toISOString();
      const localDate = new Date(date.getTime() + tz * 3600000);
      
      const dayOfWeek = DAYS[localDate.getDay()];
      const hour = localDate.getHours();
      const dateStr = localDate.toISOString().slice(0, 10);
      
      const deviceId = visitor.tracks?.[0]?.device_id || 
                     (Array.isArray(visitor.devices) ? visitor.devices[0] : '') || 
                     store_id || 
                     'unknown';
      
      const gender = visitor.sex === 1 ? 'M' : 'F';
      const age = Number(visitor.age || 0);
      
      const attrsA = Array.isArray(visitor.additional_attributes) ? visitor.additional_attributes : [];
      const attrsB = Array.isArray(visitor.additional_atributes) ? visitor.additional_atributes : [];
      const attrs = [...attrsA, ...attrsB];
      const lastAttr = attrs.length ? attrs[attrs.length - 1] : {};
      
      const smile = String(lastAttr?.smile || visitor.smile || '').toLowerCase() === 'yes';
      
      await pool.query(
        `INSERT INTO visitors (
          visitor_id, day, store_id, store_name, 
          timestamp, gender, age, day_of_week, smile, hour
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        ON CONFLICT (visitor_id, timestamp) DO UPDATE SET
          day = EXCLUDED.day,
          store_id = EXCLUDED.store_id,
          store_name = EXCLUDED.store_name,
          gender = EXCLUDED.gender,
          age = EXCLUDED.age,
          day_of_week = EXCLUDED.day_of_week,
          smile = EXCLUDED.smile,
          hour = EXCLUDED.hour`,
        [
          visitor.visitor_id || visitor.session_id || visitor.id || `temp_${Date.now()}_${Math.random()}`,
          dateStr,
          deviceId,
          `Loja ${deviceId}`,
          isoUTC,
          gender,
          age,
          dayOfWeek,
          smile,
          hour
        ]
      );
    } catch (error) {
      console.error('❌ Erro ao inserir visitante:', error.message);
    }
  }
  
  // Atualiza dashboard_daily para esta loja
  await updateDailyDashboard(day, store_id);
  
  // Atualiza dashboard_daily para 'all' (agregado de todas as lojas)
  if (store_id && store_id !== 'all') {
    await updateDailyDashboard(day, 'all');
  }
  
  console.log(`✅ Dados atualizados para ${day}`);
}

async function updateDailyDashboard(day, store_id) {
  try {
    // Calcula estatísticas para o dia
    let query = `
      SELECT 
        COUNT(*) AS total_visitors,
        SUM(CASE WHEN gender = 'M' THEN 1 ELSE 0 END) AS male,
        SUM(CASE WHEN gender = 'F' THEN 1 ELSE 0 END) AS female,
        SUM(age) AS avg_age_sum,
        SUM(CASE WHEN age > 0 THEN 1 ELSE 0 END) AS avg_age_count,
        SUM(CASE WHEN age BETWEEN 18 AND 25 THEN 1 ELSE 0 END) AS age_18_25,
        SUM(CASE WHEN age BETWEEN 26 AND 35 THEN 1 ELSE 0 END) AS age_26_35,
        SUM(CASE WHEN age BETWEEN 36 AND 45 THEN 1 ELSE 0 END) AS age_36_45,
        SUM(CASE WHEN age BETWEEN 46 AND 60 THEN 1 ELSE 0 END) AS age_46_60,
        SUM(CASE WHEN age > 60 THEN 1 ELSE 0 END) AS age_60_plus,
        SUM(CASE WHEN day_of_week = 'Dom' THEN 1 ELSE 0 END) AS sunday,
        SUM(CASE WHEN day_of_week = 'Seg' THEN 1 ELSE 0 END) AS monday,
        SUM(CASE WHEN day_of_week = 'Ter' THEN 1 ELSE 0 END) AS tuesday,
        SUM(CASE WHEN day_of_week = 'Qua' THEN 1 ELSE 0 END) AS wednesday,
        SUM(CASE WHEN day_of_week = 'Qui' THEN 1 ELSE 0 END) AS thursday,
        SUM(CASE WHEN day_of_week = 'Sex' THEN 1 ELSE 0 END) AS friday,
        SUM(CASE WHEN day_of_week = 'Sáb' THEN 1 ELSE 0 END) AS saturday
      FROM visitors
      WHERE day = $1
    `;
    
    const params = [day];
    
    if (store_id !== 'all') {
      query += ` AND store_id = $2`;
      params.push(store_id);
    }
    
    const result = await pool.query(query, params);
    const stats = result.rows[0] || {};
    
    // Insere ou atualiza no dashboard_daily
    const upsertQuery = `
      INSERT INTO dashboard_daily (
        day, store_id, total_visitors, male, female,
        avg_age_sum, avg_age_count, age_18_25, age_26_35,
        age_36_45, age_46_60, age_60_plus,
        monday, tuesday, wednesday, thursday, friday, saturday, sunday,
        updated_at
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, NOW())
      ON CONFLICT (day, store_id) DO UPDATE SET
        total_visitors = EXCLUDED.total_visitors,
        male = EXCLUDED.male,
        female = EXCLUDED.female,
        avg_age_sum = EXCLUDED.avg_age_sum,
        avg_age_count = EXCLUDED.avg_age_count,
        age_18_25 = EXCLUDED.age_18_25,
        age_26_35 = EXCLUDED.age_26_35,
        age_36_45 = EXCLUDED.age_36_45,
        age_46_60 = EXCLUDED.age_46_60,
        age_60_plus = EXCLUDED.age_60_plus,
        monday = EXCLUDED.monday,
        tuesday = EXCLUDED.tuesday,
        wednesday = EXCLUDED.wednesday,
        thursday = EXCLUDED.thursday,
        friday = EXCLUDED.friday,
        saturday = EXCLUDED.saturday,
        sunday = EXCLUDED.sunday,
        updated_at = EXCLUDED.updated_at
    `;
    
    await pool.query(upsertQuery, [
      day,
      store_id,
      Number(stats.total_visitors || 0),
      Number(stats.male || 0),
      Number(stats.female || 0),
      Number(stats.avg_age_sum || 0),
      Number(stats.avg_age_count || 0),
      Number(stats.age_18_25 || 0),
      Number(stats.age_26_35 || 0),
      Number(stats.age_36_45 || 0),
      Number(stats.age_46_60 || 0),
      Number(stats.age_60_plus || 0),
      Number(stats.monday || 0),
      Number(stats.tuesday || 0),
      Number(stats.wednesday || 0),
      Number(stats.thursday || 0),
      Number(stats.friday || 0),
      Number(stats.saturday || 0),
      Number(stats.sunday || 0)
    ]);
    
    // Atualiza dados por hora
    await updateHourlyDashboard(day, store_id);
    
  } catch (error) {
    console.error('❌ Erro ao atualizar dashboard_daily:', error);
  }
}

async function updateHourlyDashboard(day, store_id) {
  try {
    // Primeiro, deleta dados existentes para este dia/loja
    const deleteQuery = `DELETE FROM dashboard_hourly WHERE day = $1 AND store_id = $2`;
    await pool.query(deleteQuery, [day, store_id]);
    
    // Calcula estatísticas por hora
    let query = `
      SELECT 
        hour,
        COUNT(*) AS total,
        SUM(CASE WHEN gender = 'M' THEN 1 ELSE 0 END) AS male,
        SUM(CASE WHEN gender = 'F' THEN 1 ELSE 0 END) AS female
      FROM visitors
      WHERE day = $1
    `;
    
    const params = [day];
    
    if (store_id !== 'all') {
      query += ` AND store_id = $2`;
      params.push(store_id);
    }
    
    query += ` GROUP BY hour ORDER BY hour`;
    
    const result = await pool.query(query, params);
    
    // Insere dados por hora
    for (const row of result.rows) {
      const insertQuery = `
        INSERT INTO dashboard_hourly (day, store_id, hour, total, male, female)
        VALUES ($1, $2, $3, $4, $5, $6)
      `;
      
      await pool.query(insertQuery, [
        day,
        store_id,
        Number(row.hour),
        Number(row.total || 0),
        Number(row.male || 0),
        Number(row.female || 0)
      ]);
    }
    
  } catch (error) {
    console.error('❌ Erro ao atualizar dashboard_hourly:', error);
  }
}

// ===========================================
// 6. REFRESH ALL - TODAS AS LOJAS
// ===========================================
async function refreshAll(req, res, start_date, end_date) {
  try {
    const s = start_date || new Date().toISOString().slice(0, 10);
    const e = end_date || s;
    
    // Primeiro, busca todas as lojas
    const stores = await getStoreIds();
    
    console.log(`🔄 Refresh All: ${stores.length} lojas, período: ${s} a ${e}`);
    
    // Atualiza cada loja individualmente
    for (const storeId of stores) {
      try {
        await refreshRange({}, res, s, e, storeId);
      } catch (error) {
        console.error(`❌ Erro ao atualizar loja ${storeId}:`, error.message);
      }
    }
    
    // Atualiza agregado 'all'
    await refreshRange({}, res, s, e, 'all');
    
    return res.status(200).json({
      success: true,
      message: `Todas as ${stores.length} lojas atualizadas`,
      stores: stores.length,
      period: `${s} a ${e}`,
      updated_at: new Date().toISOString()
    });
    
  } catch (error) {
    console.error('❌ Refresh All error:', error);
    return res.status(500).json({
      success: false,
      error: error.message
    });
  }
}

async function getStoreIds() {
  try {
    // Busca lojas da DisplayForce
    const response = await fetch(`${DISPLAYFORCE_BASE}/device/list`, {
      method: 'POST',
      headers: {
        'X-API-Token': DISPLAYFORCE_TOKEN,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({})
    });
    
    if (!response.ok) {
      throw new Error(`DisplayForce API error: ${response.status}`);
    }
    
    const data = await response.json();
    const devices = data.devices || data.data || [];
    
    // Extrai IDs dos dispositivos
    return devices
      .map(device => String(device.id || device.device_id || ''))
      .filter(id => id && id !== 'all');
      
  } catch (error) {
    console.error('❌ Erro ao buscar lojas:', error);
    
    // Fallback: busca lojas do banco
    const result = await pool.query(`
      SELECT DISTINCT store_id 
      FROM visitors 
      WHERE store_id IS NOT NULL AND store_id != 'all'
      LIMIT 10
    `);
    
    return result.rows.map(row => row.store_id);
  }
}

// ===========================================
// 7. AUTO REFRESH
// ===========================================
async function autoRefresh(req, res) {
  try {
    // Data de ontem
    const yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);
    const dateStr = yesterday.toISOString().slice(0, 10);
    
    console.log(`🤖 Auto-refresh para ${dateStr}`);
    
    // Chama refresh para ontem para todas as lojas
    await refreshRange({}, res, dateStr, dateStr, 'all');
    
    return res.status(200).json({
      success: true,
      message: `Auto-refresh executado para ${dateStr}`,
      date: dateStr,
      triggered_at: new Date().toISOString()
    });
    
  } catch (error) {
    console.error('❌ Auto-refresh error:', error);
    return res.status(500).json({
      success: false,
      error: error.message
    });
  }
}

// ===========================================
// 8. OTIMIZAÇÃO DE ÍNDICES
// ===========================================
async function ensureIndexes(req, res) {
  try {
    console.log('🔧 Criando índices de performance...');
    
    // Índices para a tabela visitors
    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_visitors_day ON visitors(day);
      CREATE INDEX IF NOT EXISTS idx_visitors_store_id ON visitors(store_id);
      CREATE INDEX IF NOT EXISTS idx_visitors_day_store ON visitors(day, store_id);
      CREATE INDEX IF NOT EXISTS idx_visitors_gender ON visitors(gender);
      CREATE INDEX IF NOT EXISTS idx_visitors_age ON visitors(age);
      CREATE INDEX IF NOT EXISTS idx_visitors_hour ON visitors(hour);
      CREATE INDEX IF NOT EXISTS idx_visitors_timestamp ON visitors(timestamp);
    `);
    
    // Índices para a tabela dashboard_daily
    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_dashboard_daily_day ON dashboard_daily(day);
      CREATE INDEX IF NOT EXISTS idx_dashboard_daily_store ON dashboard_daily(store_id);
      CREATE INDEX IF NOT EXISTS idx_dashboard_daily_day_store ON dashboard_daily(day, store_id);
    `);
    
    // Índices para a tabela dashboard_hourly
    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_dashboard_hourly_day ON dashboard_hourly(day);
      CREATE INDEX IF NOT EXISTS idx_dashboard_hourly_store ON dashboard_hourly(store_id);
      CREATE INDEX IF NOT EXISTS idx_dashboard_hourly_hour ON dashboard_hourly(hour);
      CREATE INDEX IF NOT EXISTS idx_dashboard_hourly_day_store_hour ON dashboard_hourly(day, store_id, hour);
    `);
    
    console.log('✅ Índices criados com sucesso');
    
    return res.status(200).json({
      success: true,
      message: 'Índices otimizados com sucesso'
    });
    
  } catch (error) {
    console.error('❌ Erro ao criar índices:', error);
    return res.status(500).json({
      success: false,
      error: error.message
    });
  }
}

// ===========================================
// 9. FUNÇÃO AUXILIAR PARA RESUMO DA DISPLAYFORCE
// ===========================================
async function getSummaryFromDisplayForce(res, start_date, end_date, store_id) {
  try {
    // Esta função é mantida para compatibilidade
    // Mas agora usamos principalmente os dados do banco
    
    const tz = parseInt(process.env.TIMEZONE_OFFSET_HOURS || "-3", 10);
    const sign = tz >= 0 ? "+" : "-";
    const hh = String(Math.abs(tz)).padStart(2, "0");
    const tzStr = `${sign}${hh}:00`;
    const startISO = `${start_date}T00:00:00${tzStr}`;
    const endISO = `${end_date}T23:59:59${tzStr}`;
    
    const LIMIT_REQ = 500;
    let offset = 0;
    const allVisitors = [];
    
    while (true) {
      const bodyPayload = {
        start: startISO,
        end: endISO,
        limit: LIMIT_REQ,
        offset,
        tracks: true
      };
      
      if (store_id && store_id !== 'all') {
        bodyPayload.devices = [store_id];
      }
      
      const response = await fetch(`${DISPLAYFORCE_BASE}/stats/visitor/list`, {
        method: 'POST',
        headers: { 'X-API-Token': DISPLAYFORCE_TOKEN, 'Content-Type': 'application/json' },
        body: JSON.stringify(bodyPayload)
      });
      
      if (!response.ok) break;
      
      const data = await response.json();
      const visitors = data.payload || data.data || [];
      allVisitors.push(...visitors);
      
      const pagination = data.pagination;
      if (!pagination || visitors.length < LIMIT_REQ) break;
      
      offset += LIMIT_REQ;
    }
    
    // Processa os dados
    const stats = {
      totalVisitors: allVisitors.length,
      totalMale: 0,
      totalFemale: 0,
      ageSum: 0,
      ageCount: 0,
      byAgeGroup: { "18-25": 0, "26-35": 0, "36-45": 0, "46-60": 0, "60+": 0 },
      visitsByDay: { Sunday: 0, Monday: 0, Tuesday: 0, Wednesday: 0, Thursday: 0, Friday: 0, Saturday: 0 },
      byHour: {},
      byGenderHour: { male: {}, female: {} }
    };
    
    const DAYS_EN = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
    
    for (const visitor of allVisitors) {
      const ts = visitor.start || visitor.tracks?.[0]?.start || new Date().toISOString();
      const date = new Date(ts);
      const localDate = new Date(date.getTime() + tz * 3600000);
      const hour = localDate.getHours();
      const dayOfWeek = DAYS_EN[localDate.getDay()];
      
      // Gênero
      if (visitor.sex === 1) {
        stats.totalMale++;
        stats.byGenderHour.male[hour] = (stats.byGenderHour.male[hour] || 0) + 1;
      } else {
        stats.totalFemale++;
        stats.byGenderHour.female[hour] = (stats.byGenderHour.female[hour] || 0) + 1;
      }
      
      // Idade
      const age = Number(visitor.age || 0);
      if (age > 0) {
        stats.ageSum += age;
        stats.ageCount++;
        
        if (age >= 18 && age <= 25) stats.byAgeGroup["18-25"]++;
        else if (age >= 26 && age <= 35) stats.byAgeGroup["26-35"]++;
        else if (age >= 36 && age <= 45) stats.byAgeGroup["36-45"]++;
        else if (age >= 46 && age <= 60) stats.byAgeGroup["46-60"]++;
        else if (age > 60) stats.byAgeGroup["60+"]++;
      }
      
      // Dia da semana
      stats.visitsByDay[dayOfWeek]++;
      
      // Hora
      stats.byHour[hour] = (stats.byHour[hour] || 0) + 1;
    }
    
    // Preenche horas vazias
    for (let h = 0; h < 24; h++) {
      stats.byHour[h] = stats.byHour[h] || 0;
      stats.byGenderHour.male[h] = stats.byGenderHour.male[h] || 0;
      stats.byGenderHour.female[h] = stats.byGenderHour.female[h] || 0;
    }
    
    const response = {
      success: true,
      totalVisitors: stats.totalVisitors,
      totalMale: stats.totalMale,
      totalFemale: stats.totalFemale,
      averageAge: stats.ageCount > 0 ? Math.round(stats.ageSum / stats.ageCount) : 0,
      visitsByDay: stats.visitsByDay,
      byAgeGroup: stats.byAgeGroup,
      byAgeGender: {
        "<20": { male: 0, female: 0 },
        "20-29": { male: 0, female: 0 },
        "30-45": { male: 0, female: 0 },
        ">45": { male: 0, female: 0 }
      },
      byHour: stats.byHour,
      byGenderHour: stats.byGenderHour,
      source: 'displayforce_direct'
    };
    
    return res.status(200).json(response);
    
  } catch (error) {
    console.error('❌ DisplayForce summary error:', error);
    return res.status(500).json({
      success: false,
      error: error.message
    });
  }
}