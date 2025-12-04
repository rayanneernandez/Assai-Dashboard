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
      
      case 'optimize':
        return await ensureIndexes(req, res);
      
      case 'test':
        return res.status(200).json({
          success: true,
          message: 'API Assaí está funcionando!',
          endpoints: ['visitors', 'summary', 'stores', 'devices', 'refresh', 'optimize', 'test'],
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
            'refresh - Preenche banco a partir da DisplayForce',
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
        day        AS day,
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

    const rows = result.rows || [];

    return res.status(200).json({
      success: true,
      data: rows,
    });
  } catch (error) {
    console.error("❌ Visitors error:", error);

    return res.status(200).json({
      success: true,
      data: [],
      isFallback: true,
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
    if (store_id && store_id !== 'all') bodyPayload.device_id = store_id;
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
  const visitors = all.map((v) => {
    const ts = String(v.start ?? v.tracks?.[0]?.start ?? new Date().toISOString());
    const base = new Date(ts);
    const local = new Date(base.getTime() + tz * 3600000);
    const di = local.getDay();
    const day_of_week = DAYS[di];
    const attrs = Array.isArray(v.additional_atributes) ? v.additional_atributes : [];
    const last = attrs.length ? attrs[attrs.length - 1] : {};
    const smile = String(last?.smile ?? v.smile ?? '').toLowerCase() === 'yes';
    const deviceId = String(v.tracks?.[0]?.device_id ?? (Array.isArray(v.devices) ? v.devices[0] : ''));
    return {
      id: String(v.visitor_id ?? v.session_id ?? v.id ?? ''),
      date: local.toISOString().slice(0,10),
      store_id: deviceId,
      store_name: `Loja ${deviceId}`,
      timestamp: ts,
      gender: (v.sex === 1 ? 'Masculino' : 'Feminino'),
      age: Number(v.age ?? 0),
      day_of_week,
      smile,
    };
  });
  try {
    for (const r of visitors) {
      await pool.query(
        `INSERT INTO public.visitors (visitor_id, day, timestamp, store_id, store_name, gender, age, day_of_week, smile)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT DO NOTHING`,
        [r.id, r.date, r.timestamp, r.store_id, r.store_name || null, r.gender === 'Masculino' ? 'M' : 'F', r.age, r.day_of_week, r.smile]
      );
    }
  } catch (e) {
    console.error('❌ Erro ao inserir visitantes (DF->DB):', e.message);
  }
  return res.status(200).json({ success: true, data: visitors, count: visitors.length, source: 'displayforce', query: { start_date, end_date, store_id } });
}

// ===========================================
// 2. RESUMO DO DASHBOARD
// ===========================================
async function getSummary(req, res, start_date, end_date, store_id) {
  const source = req.query.source;

  try {
    // Se for explícito usar DisplayForce, mantém como já estava
    if (source === "displayforce") {
      return await getSummaryFromDisplayForce(res, start_date, end_date, store_id);
    }

    // ---------- DAILY / RESUMO GERAL ----------
    let query = `
      SELECT
        COALESCE(SUM(total_visitors), 0)  AS total_visitors,
        COALESCE(SUM(male), 0)            AS total_male,
        COALESCE(SUM(female), 0)          AS total_female,
        COALESCE(SUM(avg_age_sum), 0)     AS avg_age_sum,
        COALESCE(SUM(avg_age_count), 0)   AS avg_age_count,
        COALESCE(SUM(age_18_25), 0)       AS age_18_25,
        COALESCE(SUM(age_26_35), 0)       AS age_26_35,
        COALESCE(SUM(age_36_45), 0)       AS age_36_45,
        COALESCE(SUM(age_46_60), 0)       AS age_46_60,
        COALESCE(SUM(age_60_plus), 0)     AS age_60_plus,
        COALESCE(SUM(sunday), 0)          AS sunday,
        COALESCE(SUM(monday), 0)          AS monday,
        COALESCE(SUM(tuesday), 0)         AS tuesday,
        COALESCE(SUM(wednesday), 0)       AS wednesday,
        COALESCE(SUM(thursday), 0)        AS thursday,
        COALESCE(SUM(friday), 0)          AS friday,
        COALESCE(SUM(saturday), 0)        AS saturday
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
  // Filtra uma loja específica
  query += ` AND store_id = $${paramCount}`;
  params.push(store_id);
  paramCount++;
} else {
  // Quando não enviar store_id OU enviar "all",
  // usa apenas a linha agregada (store_id = 'all')
  query += ` AND store_id = 'all'`;
}


    console.log("📊 Summary query:", query, params);

    const result = await pool.query(query, params);
    let row = result.rows[0] || {};

    if ((Number(row.total_visitors || 0) === 0) && store_id && store_id !== "all") {
      const vParams = [];
      let vc = 1;
      let vq = `
        SELECT
          COUNT(*) AS total_visitors,
          SUM(CASE WHEN gender='M' THEN 1 ELSE 0 END) AS total_male,
          SUM(CASE WHEN gender='F' THEN 1 ELSE 0 END) AS total_female,
          SUM(CASE WHEN age > 0 THEN age ELSE 0 END) AS avg_age_sum,
          SUM(CASE WHEN age > 0 THEN 1 ELSE 0 END) AS avg_age_count,
          SUM(CASE WHEN age BETWEEN 18 AND 25 THEN 1 ELSE 0 END) AS age_18_25,
          SUM(CASE WHEN age BETWEEN 26 AND 35 THEN 1 ELSE 0 END) AS age_26_35,
          SUM(CASE WHEN age BETWEEN 36 AND 45 THEN 1 ELSE 0 END) AS age_36_45,
          SUM(CASE WHEN age BETWEEN 46 AND 60 THEN 1 ELSE 0 END) AS age_46_60,
          SUM(CASE WHEN age > 60 THEN 1 ELSE 0 END) AS age_60_plus,
          SUM(CASE WHEN day_of_week='Dom' THEN 1 ELSE 0 END) AS sunday,
          SUM(CASE WHEN day_of_week='Seg' THEN 1 ELSE 0 END) AS monday,
          SUM(CASE WHEN day_of_week='Ter' THEN 1 ELSE 0 END) AS tuesday,
          SUM(CASE WHEN day_of_week='Qua' THEN 1 ELSE 0 END) AS wednesday,
          SUM(CASE WHEN day_of_week='Qui' THEN 1 ELSE 0 END) AS thursday,
          SUM(CASE WHEN day_of_week='Sex' THEN 1 ELSE 0 END) AS friday,
          SUM(CASE WHEN day_of_week='Sáb' THEN 1 ELSE 0 END) AS saturday
        FROM visitors
        WHERE 1=1
      `;
      if (start_date) { vq += ` AND day >= $${vc}`; vParams.push(start_date); vc++; }
      if (end_date) { vq += ` AND day <= $${vc}`; vParams.push(end_date); vc++; }
      vq += ` AND store_id = $${vc}`; vParams.push(store_id);

      const vRes = await pool.query(vq, vParams);
      row = vRes.rows[0] || row;
    }

    const avgCount = Number(row.avg_age_count || 0);
    const averageAge =
      avgCount > 0 ? Math.round(Number(row.avg_age_sum || 0) / avgCount) : 0;

    // ---------- HOURLY ----------
    let hQuery = `
      SELECT
        hour,
        COALESCE(SUM(total), 0)  AS total,
        COALESCE(SUM(male), 0)   AS male,
        COALESCE(SUM(female), 0) AS female
      FROM dashboard_hourly
      WHERE 1=1
    `;

    const hParams = [];
    let hc = 1;

    if (start_date) {
      hQuery += ` AND day >= $${hc}`;
      hParams.push(start_date);
      hc++;
    }

    if (end_date) {
      hQuery += ` AND day <= $${hc}`;
      hParams.push(end_date);
      hc++;
    }

    if (store_id && store_id !== "all") {
      hQuery += ` AND store_id = $${hc}`;
      hParams.push(store_id);
      hc++;
    } else {
      hQuery += ` AND store_id = 'all'`;
    }


    hQuery += ` GROUP BY hour ORDER BY hour ASC`;

    console.log("⏰ Hourly query:", hQuery, hParams);

    const hRes = await pool.query(hQuery, hParams);
    let hRows = hRes.rows;
    if (hRows.length === 0 && store_id && store_id !== "all") {
      let hvq = `
        SELECT EXTRACT(HOUR FROM timestamp) AS hour,
               COUNT(*) AS total,
               SUM(CASE WHEN gender='M' THEN 1 ELSE 0 END) AS male,
               SUM(CASE WHEN gender='F' THEN 1 ELSE 0 END) AS female
        FROM visitors
        WHERE 1=1
      `;
      const hvParams = [];
      let hvc = 1;
      if (start_date) { hvq += ` AND day >= $${hvc}`; hvParams.push(start_date); hvc++; }
      if (end_date) { hvq += ` AND day <= $${hvc}`; hvParams.push(end_date); hvc++; }
      hvq += ` AND store_id = $${hvc}`; hvParams.push(store_id);
      hvq += ` GROUP BY hour ORDER BY hour ASC`;
      const hvRes = await pool.query(hvq, hvParams);
      hRows = hvRes.rows || [];
    }

    const byHour = {};
    const byGenderHour = { male: {}, female: {} };

    for (const r of hRows) {
      const hour = String(r.hour);
      byHour[hour] = Number(r.total || 0);
      byGenderHour.male[hour] = Number(r.male || 0);
      byGenderHour.female[hour] = Number(r.female || 0);
    }

    // ---------- MONTA RESPOSTA ----------
    const response = {
      success: true,
      totalVisitors: Number(row.total_visitors || 0),
      totalMale: Number(row.total_male || 0),
      totalFemale: Number(row.total_female || 0),
      averageAge,
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
      byHour,
      byGenderHour,
      isFallback: false,
    };

    return res.status(200).json(response);
  } catch (error) {
    console.error("❌ Summary error:", error);

    // fallback antigo, se quiser manter
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
      byHour: {},
      byGenderHour: { male: {}, female: {} },
      isFallback: true,
    });
  }
}


// Funções de refresh (serverless)
async function refreshRange(req, res, start_date, end_date, store_id) {
  try {
    const s = start_date || new Date().toISOString().slice(0,10);
    const e = end_date || s;
    const days = [];
    let d = new Date(`${s}T00:00:00Z`);
    const endD = new Date(`${e}T00:00:00Z`);
    while (d <= endD) { days.push(d.toISOString().slice(0,10)); d = new Date(d.getTime() + 86400000); }
    const tz = parseInt(process.env.TIMEZONE_OFFSET_HOURS || "-3", 10);
    const sign = tz >= 0 ? "+" : "-"; const hh = String(Math.abs(tz)).padStart(2, "0"); const tzStr = `${sign}${hh}:00`;
    for (const day of days) {
      let offset = 0; const limit = 500; const payload = [];
      while (true) {
        const body = { start: `${day}T00:00:00${tzStr}`, end: `${day}T23:59:59${tzStr}`, limit, offset, tracks: true };
        if (store_id && store_id !== 'all') body.device_id = store_id;
        const resp = await fetch(`${DISPLAYFORCE_BASE}/stats/visitor/list`, { method: 'POST', headers: { 'X-API-Token': DISPLAYFORCE_TOKEN, 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
        if (!resp.ok) { const t = await resp.text().catch(()=>""); throw new Error(`DF ${resp.status} ${t}`); }
        const json = await resp.json(); const arr = Array.isArray(json.payload || json.data) ? (json.payload || json.data) : [];
        payload.push(...arr);
        const pg = json.pagination; const pageLimit = Number(pg?.limit ?? limit);
        if (pg?.total && payload.length >= Number(pg.total)) break;
        if (arr.length < pageLimit) break;
        offset += pageLimit;
      }
      let total=0,male=0,female=0,avgSum=0,avgCount=0; const byAge={ '18-25':0,'26-35':0,'36-45':0,'46-60':0,'60+':0 }; const byWeek={ Monday:0,Tuesday:0,Wednesday:0,Thursday:0,Friday:0,Saturday:0,Sunday:0 }; const byHour={}; const byGenderHour={ male:{}, female:{} };
      for (const v of payload) {
        const ts = String(v.start || v.tracks?.[0]?.start || new Date().toISOString());
        const base = new Date(ts);
        const local = new Date(base.getTime() + tz*3600000);
        const dstrLocal = `${local.getFullYear()}-${String(local.getMonth()+1).padStart(2,'0')}-${String(local.getDate()).padStart(2,'0')}`;
        if (dstrLocal !== day) continue;
        total++;
        const g = v.sex===1?'M':'F'; if (g==='M') male++; else female++;
        const age = Number(v.age||0); if (age>0) { avgSum+=age; avgCount++; }
        if (age>=18&&age<=25) byAge['18-25']++; else if (age>=26&&age<=35) byAge['26-35']++; else if (age>=36&&age<=45) byAge['36-45']++; else if (age>=46&&age<=60) byAge['46-60']++; else if (age>60) byAge['60+']++;
        const map = ['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'];
        const wd = map[local.getDay()]; byWeek[wd] = (byWeek[wd]||0)+1;
        const h = local.getHours(); byHour[h] = (byHour[h]||0)+1;
        if (g==='M') byGenderHour.male[h]=(byGenderHour.male[h]||0)+1; else byGenderHour.female[h]=(byGenderHour.female[h]||0)+1;
      }
      const avgAgeSum = avgSum; const avgAgeCount = avgCount;
      const exists = await pool.query("SELECT 1 FROM public.dashboard_daily WHERE day=$1 AND (store_id IS NOT DISTINCT FROM $2)", [day, store_id||'all']);
      if (exists.rows.length) {
        await pool.query(`UPDATE public.dashboard_daily SET total_visitors=$3, male=$4, female=$5, avg_age_sum=$6, avg_age_count=$7, age_18_25=$8, age_26_35=$9, age_36_45=$10, age_46_60=$11, age_60_plus=$12, monday=$13, tuesday=$14, wednesday=$15, thursday=$16, friday=$17, saturday=$18, sunday=$19, updated_at=NOW() WHERE day=$1 AND (store_id IS NOT DISTINCT FROM $2)`, [day, store_id||'all', total, male, female, avgAgeSum, avgAgeCount, byAge['18-25'], byAge['26-35'], byAge['36-45'], byAge['46-60'], byAge['60+'], byWeek.Monday, byWeek.Tuesday, byWeek.Wednesday, byWeek.Thursday, byWeek.Friday, byWeek.Saturday, byWeek.Sunday]);
      } else {
        await pool.query(`INSERT INTO public.dashboard_daily (day, store_id, total_visitors, male, female, avg_age_sum, avg_age_count, age_18_25, age_26_35, age_36_45, age_46_60, age_60_plus, monday, tuesday, wednesday, thursday, friday, saturday, sunday) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)`, [day, store_id||'all', total, male, female, avgAgeSum, avgAgeCount, byAge['18-25'], byAge['26-35'], byAge['36-45'], byAge['46-60'], byAge['60+'], byWeek.Monday, byWeek.Tuesday, byWeek.Wednesday, byWeek.Thursday, byWeek.Friday, byWeek.Saturday, byWeek.Sunday]);
      }
      for (let h=0; h<24; h++) {
        const tot = Number(byHour[h]||0); const m = Number(byGenderHour.male[h]||0); const f = Number(byGenderHour.female[h]||0);
        await pool.query(`INSERT INTO public.dashboard_hourly (day, store_id, hour, total, male, female) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (day, store_id, hour) DO UPDATE SET total=$4, male=$5, female=$6`, [day, store_id||'all', h, tot, m, f]);
      }
      const mapPt = ['Dom','Seg','Ter','Qua','Qui','Sex','Sáb'];
      for (const v of payload) {
        const ts = String(v.start || v.tracks?.[0]?.start || new Date().toISOString()); const base = new Date(ts); const local = new Date(base.getTime() + tz*3600000); const dstr = `${local.getFullYear()}-${String(local.getMonth()+1).padStart(2,'0')}-${String(local.getDate()).padStart(2,'0')}`; const dayOfWeek = mapPt[local.getDay()]; const deviceId = String(v.tracks?.[0]?.device_id ?? (Array.isArray(v.devices)? v.devices[0] : ''));
        await pool.query(`INSERT INTO public.visitors (visitor_id, day, timestamp, store_id, store_name, gender, age, day_of_week, smile) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT DO NOTHING`, [String(v.visitor_id ?? v.session_id ?? v.id ?? ''), dstr, ts, deviceId, String(v.store_name ?? ''), (v.sex===1?'M':'F'), Number(v.age||0), dayOfWeek, String(v.smile||'').toLowerCase()==='yes']);
      }
    }
    return res.status(200).json({ ok: true, days: days.length, store_id: store_id||'all' });
  } catch (e) {
    console.error('❌ Refresh error:', e.message);
    return res.status(500).json({ error: e.message });
  }
}

async function ensureIndexes(req, res) {
  try {
    await pool.query("CREATE INDEX IF NOT EXISTS visitors_day_idx ON public.visitors(day)");
    await pool.query("CREATE INDEX IF NOT EXISTS visitors_store_day_idx ON public.visitors(store_id, day)");
    await pool.query("CREATE INDEX IF NOT EXISTS visitors_ts_idx ON public.visitors(timestamp)");
    await pool.query("CREATE INDEX IF NOT EXISTS dashboard_daily_day_store_idx ON public.dashboard_daily(day, store_id)");
    await pool.query("CREATE INDEX IF NOT EXISTS dashboard_hourly_day_store_hour_idx ON public.dashboard_hourly(day, store_id, hour)");
    return res.status(200).json({ ok: true });
  } catch (e) {
    return res.status(500).json({ error: e.message });
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
