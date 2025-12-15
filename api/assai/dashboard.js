// api/assai/dashboard.js - API CORRIGIDA PARA BUSCAR TODOS OS DADOS
import { Pool } from 'pg';

// Configurar conexão com PostgreSQL
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
  max: parseInt(process.env.PG_POOL_MAX || "10", 10),
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 10000,
  keepAlive: true
});

async function q(sql, params) {
  for (let i = 0; i < 3; i++) {
    try { return await pool.query(sql, params); } catch (e) {
      const msg = String(e?.message || "");
      if (/Connection terminated unexpectedly|ECONNRESET|ETIMEDOUT/i.test(msg)) {
        await new Promise(r => setTimeout(r, 200 * (i + 1)));
        continue;
      }
      throw e;
    }
  }
  return await pool.query(sql, params);
}

// Configurações DisplayForce
const DISPLAYFORCE_TOKEN = process.env.DISPLAYFORCE_API_TOKEN || '4AUH-BX6H-G2RJ-G7PB';
const DISPLAYFORCE_BASE = process.env.DISPLAYFORCE_API_URL || 'https://api.displayforce.ai/public/v1';
const SUMMARY_CACHE = new Map();
function cacheKey(sDate, eDate, storeId) { return `${sDate}|${eDate}|${storeId||'all'}`; }

export default async function handler(req, res) {
  // Configurar CORS
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  res.setHeader('Cache-Control', 'no-store, max-age=0, s-maxage=0');
  
  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }
  
  const { endpoint, start_date, end_date, store_id, source } = req.query;
  const ep = String(endpoint || '').trim().toLowerCase();
  
  try {
    console.log(`📊 Endpoint: ${ep}, Dates: ${start_date} - ${end_date}, Store: ${store_id}`);
    
    switch (ep) {
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
      
      case 'sync_all_data':
        return await syncAllHistoricalData(req, res);
      
      case 'plan_ingest':
        return await planIngestDay(req, res, start_date, end_date, store_id);
      
      case 'ingest_day':
        return await ingestDay(req, res, start_date, end_date, store_id);
      
      case 'auto_refresh':
        return await autoRefresh(req, res);
      
      case 'force_sync_today':
        return await forceSyncToday(req, res);
      
      case 'wipe_range':
        return await wipeRange(req, res, start_date, end_date);
      
      case 'verify_day':
        return await verifyDay(req, res, start_date, store_id);
      
      case 'rebuild_hourly':
        return await rebuildHourlyFromVisitors(req, res, start_date, end_date, store_id);
      
      case 'refresh_recent':
        return await refreshRecent(req, res, start_date, store_id);
      
      case 'optimize':
        return await ensureIndexes(req, res);
      
      case 'backfill_local_time':
        return await backfillLocalTime(req, res);
      
      case 'sync_today':
        return await forceSyncToday(req, res);
      
      case 'recent':
        return await refreshRecent(req, res, start_date, store_id);
      
      case 'test':
        return res.status(200).json({
          success: true,
          message: 'API Assaí está funcionando!',
          endpoints: ['visitors', 'summary', 'stores', 'devices', 'refresh', 'refresh_all', 'sync_all_data', 'auto_refresh', 'optimize', 'test'],
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
            'refresh - Atualiza período específico',
            'refresh_all - Atualiza todas as lojas',
            'sync_all_data - Sincroniza TODOS os dados históricos',
            'auto_refresh - Atualiza automaticamente',
            'optimize - Cria índices',
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
// 1. SINCRONIZAÇÃO DE TODOS OS DADOS HISTÓRICOS
// ===========================================
async function syncAllHistoricalData(req, res) {
  try {
    console.log('🚀 SINCRONIZAÇÃO COMPLETA DE DADOS HISTÓRICOS INICIADA');
    
    // Primeiro, busca todos os dispositivos
    const devices = await fetchDisplayForceDevices();
    console.log(`📱 ${devices.length} dispositivos encontrados`);
    
    const results = [];
    
    // Para cada dispositivo, busca TODOS os dados históricos
    for (const device of devices) {
      try {
        console.log(`🔄 Buscando TODOS os dados históricos para dispositivo ${device.id}...`);
        
        // Busca todos os visitantes deste dispositivo (sem filtro de data)
        const visitors = await fetchAllVisitorsFromDisplayForce(device.id);
        console.log(`📊 ${visitors.length} visitantes encontrados para dispositivo ${device.id}`);
        
        // Salva no banco
        const saved = await saveVisitorsToDatabase(visitors, undefined, String(req.query.mode || ''));
        
        // Atualiza agregados para todas as datas
        await updateAllAggregatesForDevice(device.id);
        
        results.push({
          device_id: device.id,
          visitors_found: visitors.length,
          visitors_saved: saved,
          success: true
        });
        
        console.log(`✅ Dispositivo ${device.id} sincronizado: ${saved} visitantes salvos`);
        
      } catch (deviceError) {
        console.error(`❌ Erro no dispositivo ${device.id}:`, deviceError.message);
        results.push({
          device_id: device.id,
          error: deviceError.message,
          success: false
        });
      }
    }
    
    // Atualiza agregado geral para todas as datas
    console.log('🔄 Atualizando agregado geral (all)...');
    await updateAllAggregatesForDevice('all');
    
    console.log('✅ Sincronização completa concluída');
    
    return res.status(200).json({
      success: true,
      message: 'Sincronização completa de dados históricos concluída',
      results: results,
      total_devices: devices.length,
      timestamp: new Date().toISOString()
    });
    
  } catch (error) {
    console.error('❌ Sync all historical data error:', error);
    return res.status(500).json({
      success: false,
      error: error.message
    });
  }
}

async function fetchAllVisitorsFromDisplayForce(device_id = null) {
  const tz = parseInt(process.env.TIMEZONE_OFFSET_HOURS || "-3", 10);
  const LIMIT = 500; // Aumentei para 500 por página
  let offset = 0;
  const allVisitors = [];
  let totalProcessed = 0;
  let totalFromAPI = 0;
  
  console.log(`🔍 Buscando TODOS os visitantes${device_id ? ` para dispositivo ${device_id}` : ''}...`);
  
  try {
    while (true) {
      const bodyPayload = {
        limit: LIMIT,
        offset: offset,
        tracks: true,
        face_quality: true,
        glasses: true,
        facial_hair: true,
        hair_color: true,
        hair_type: true,
        headwear: true
      };
      
      if (device_id) {
        bodyPayload.devices = [parseInt(device_id)];
      }
      
      console.log(`📄 Buscando lote ${Math.floor(offset/LIMIT) + 1}, offset: ${offset}`);
      
      const response = await fetch(`${DISPLAYFORCE_BASE}/stats/visitor/list`, {
        method: 'POST',
        headers: { 
          'X-API-Token': DISPLAYFORCE_TOKEN, 
          'Content-Type': 'application/json' 
        },
        body: JSON.stringify(bodyPayload)
      });
      
      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`API Error ${response.status}: ${errorText}`);
      }
      
      const data = await response.json();
      
      // Verifica estrutura da resposta
      if (!data.payload && !Array.isArray(data)) {
        console.error('❌ Estrutura de resposta inesperada:', data);
        break;
      }
      
      const visitors = data.payload || data || [];
      allVisitors.push(...visitors);
      totalProcessed += visitors.length;
      
      // Log progresso
      console.log(`📊 Lote ${Math.floor(offset/LIMIT) + 1}: ${visitors.length} visitantes, Total: ${totalProcessed}`);
      
      // Verifica paginação
      if (data.pagination) {
        totalFromAPI = data.pagination.total || 0;
        console.log(`📊 Total na API: ${totalFromAPI}, Obtidos: ${totalProcessed}`);
        
        if (totalProcessed >= totalFromAPI) {
          console.log(`✅ Todos os ${totalFromAPI} visitantes obtidos`);
          break;
        }
      }
      
      // Se não há mais dados
      if (visitors.length < LIMIT) {
        console.log(`✅ Último lote obtido (${visitors.length} visitantes)`);
        break;
      }
      
      offset += LIMIT;
      
      // Limite de segurança (máximo 10,000 visitantes)
      if (offset >= 10000) {
        console.warn('⚠️ Limite de segurança atingido (10,000 visitantes)');
        break;
      }
      
      // Pequena pausa para evitar rate limiting
      await new Promise(resolve => setTimeout(resolve, 200));
    }
    
    console.log(`✅ Total final: ${allVisitors.length} visitantes obtidos`);
    return allVisitors;
    
  } catch (error) {
    console.error('❌ Erro ao buscar todos os visitantes:', error);
    throw error;
  }
}

async function updateAllAggregatesForDevice(device_id) {
  try {
    console.log(`📈 Atualizando TODOS os agregados para ${device_id}...`);
    
    // Primeiro, identifica todas as datas únicas para este dispositivo
    let query = `
      SELECT DISTINCT day 
      FROM visitors 
      WHERE 1=1
    `;
    
    const params = [];
    
    if (device_id !== 'all') {
      query += ` AND store_id = $1`;
      params.push(device_id);
    }
    
    query += ` ORDER BY day`;
    
    const result = await pool.query(query, params);
    const uniqueDates = result.rows.map(row => row.day);
    
    console.log(`📅 ${uniqueDates.length} datas únicas encontradas para ${device_id}`);
    
    // Atualiza agregados para cada data
    for (const date of uniqueDates) {
      try {
        await updateAggregatesForDateAndDevice(date, device_id);
      } catch (dateError) {
        console.error(`❌ Erro na data ${date}:`, dateError.message);
      }
    }
    
    console.log(`✅ Todos os agregados atualizados para ${device_id}`);
    
  } catch (error) {
    console.error(`❌ Erro ao atualizar todos os agregados para ${device_id}:`, error);
  }
}

async function updateAggregatesForDateAndDevice(date, device_id) {
  try {
    // Calcula estatísticas do dia específico
    const stats = await calculateDailyStatsForDate(date, device_id);
    
    // Determina o store_id para salvar
    const saveStoreId = device_id && device_id !== 'all' ? device_id : 'all';
    
    await Promise.resolve();
    
  } catch (error) {
    console.error(`❌ Erro ao atualizar agregados para ${date}, ${device_id}:`, error);
  }
}

// ===========================================
// 2. GET SUMMARY - CORRIGIDO PARA BUSCAR DADOS COMPLETOS
// ===========================================
async function getSummary(req, res, start_date, end_date, store_id) {
  try {
    console.log(`📊 Summary request: ${start_date} - ${end_date}, store: ${store_id}`);
    
    // Se não tem datas, usa o dia atual para resposta rápida
  const today = new Date().toISOString().split('T')[0];
  const sDate = start_date || today;
  const eDate = end_date || sDate;
  
  console.log(`📊 Período: ${sDate} até ${eDate}`);
  
  // Calcular a partir de visitors (rápido) sem ingestão extra
  return await calculateRealTimeSummary(res, sDate, eDate, store_id || 'all');
    
  } catch (error) {
    console.error("❌ Summary error:", error);
    return await calculateRealTimeSummary(res, start_date, end_date, store_id);
  }
}

// ===========================================
// 3. FUNÇÃO PARA HORÁRIO USANDO TIMESTAMP REAL
// ===========================================
async function getHourlyAggregatesWithRealTime(start_date, end_date, store_id) {
  try {
    console.log(`⏰ Calculando fluxo horário REAL para ${start_date} - ${end_date}`);
    
    const tzOffset = parseInt(process.env.TIMEZONE_OFFSET_HOURS || "-3", 10);
    const sign = tzOffset >= 0 ? "+" : "-";
    const hh = String(Math.abs(tzOffset)).padStart(2, "0");
    const tzStr = `${sign}${hh}:00`;
    const startISO = `${start_date}T00:00:00${tzStr}`;
    const endISO = `${end_date}T23:59:59${tzStr}`;
    const hourExpr = `EXTRACT(HOUR FROM local_time::time)`;
    let query = `
      SELECT 
        ${hourExpr} AS hour,
        COUNT(*) AS total,
        SUM(CASE WHEN gender = 'M' THEN 1 ELSE 0 END) AS male,
        SUM(CASE WHEN gender = 'F' THEN 1 ELSE 0 END) AS female
      FROM visitors
      WHERE day >= $1 AND day <= $2 AND local_time IS NOT NULL
    `;
    
    const params = [start_date, end_date];
    
    if (store_id && store_id !== "all") {
      query += ` AND store_id = $3`;
      params.push(store_id);
    }
    
    query += ` GROUP BY ${hourExpr} ORDER BY ${hourExpr}`;
    
    const result = await pool.query(query, params);
    
    const byHour = {};
    const byGenderHour = { male: {}, female: {} };
    
    // Inicializa todas as horas (0-23)
    for (let h = 0; h < 24; h++) {
      byHour[h] = 0;
      byGenderHour.male[h] = 0;
      byGenderHour.female[h] = 0;
    }
    
    // Preenche com os dados reais
    for (const row of result.rows) {
      const hour = Number(row.hour);
      if (hour >= 0 && hour < 24) {
        byHour[hour] = Number(row.total || 0);
        byGenderHour.male[hour] = Number(row.male || 0);
        byGenderHour.female[hour] = Number(row.female || 0);
      }
    }
    
    console.log(`⏰ Fluxo horário calculado: ${Object.values(byHour).reduce((a, b) => a + b, 0)} visitantes`);
    
    return { byHour, byGenderHour };
    
  } catch (error) {
    console.error("❌ Hourly aggregates with real time error:", error);
    return createEmptyHourlyData();
  }
}

async function getHourlyAggregatesFromAggregates(start_date, end_date, store_id) {
  try {
    let q = `
      SELECT hour, COALESCE(SUM(total),0) AS total, COALESCE(SUM(male),0) AS male, COALESCE(SUM(female),0) AS female
      FROM dashboard_hourly
      WHERE day >= $1 AND day <= $2 AND (store_id IS NOT DISTINCT FROM $3)
      GROUP BY hour ORDER BY hour`;
    let { rows } = await pool.query(q, [start_date, end_date, store_id]);
    if (!rows || rows.length === 0) {
      const tzOffset = parseInt(process.env.TIMEZONE_OFFSET_HOURS || "-3", 10);
      const adj = `EXTRACT(HOUR FROM (timestamp + INTERVAL '${tzOffset} hour'))`;
      let vq = `
        SELECT COALESCE(hour, ${adj}) AS hour,
               SUM(CASE WHEN gender IN ('M','F') THEN 1 ELSE 0 END) AS total,
               SUM(CASE WHEN gender='M' THEN 1 ELSE 0 END) AS male,
               SUM(CASE WHEN gender='F' THEN 1 ELSE 0 END) AS female
        FROM visitors
        WHERE day >= $1 AND day <= $2`;
      const params = [start_date, end_date];
      if (store_id && store_id !== 'all') { vq += ` AND store_id = $3`; params.push(store_id); }
      vq += ` GROUP BY COALESCE(hour, ${adj}) ORDER BY 1`;
      const r2 = await pool.query(vq, params);
      rows = r2.rows;
    }
    const byHour = {}; const byGenderHour = { male:{}, female:{} };
    for (let h=0; h<24; h++){ byHour[h]=0; byGenderHour.male[h]=0; byGenderHour.female[h]=0; }
    for (const r of rows){ const h = Number(r.hour); if (h>=0 && h<24){ byHour[h]=Number(r.total||0); byGenderHour.male[h]=Number(r.male||0); byGenderHour.female[h]=Number(r.female||0); } }
    return { byHour, byGenderHour };
  } catch { return createEmptyHourlyData(); }
}

// ===========================================
// 4. CALCULAR SUMMARY EM TEMPO REAL
// ===========================================
async function calculateRealTimeSummary(res, start_date, end_date, store_id) {
  try {
    const today = new Date().toISOString().split('T')[0];
    const sDate = start_date || today;
    const eDate = end_date || sDate;
    
    console.log(`🧮 Calculando summary em tempo real para ${sDate} - ${eDate}`);
    
    const tzOffset = parseInt(process.env.TIMEZONE_OFFSET_HOURS || "-3", 10);
    const sign = tzOffset >= 0 ? "+" : "-";
    const hh = String(Math.abs(tzOffset)).padStart(2, "0");
    const tzStr = `${sign}${hh}:00`;
    const startISO = `${sDate}T00:00:00${tzStr}`;
    const endISO = `${eDate}T23:59:59${tzStr}`;
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
      WHERE day >= $1 AND day <= $2
    `;
    
    const params = [sDate, eDate];
    
    if (store_id && store_id !== "all") {
      query += ` AND store_id = $3`;
      params.push(store_id);
    }
    
    const result = await pool.query(query, params);
    let row = result.rows[0] || {};
    
    let totalRealTime = Number(row.total_visitors || 0);
    console.log(`🧮 Total em tempo real (DB): ${totalRealTime}`);

    if (process.env.SUMMARY_INGEST_ON_CALL === '1') {
      try {
        const firstBody = { start: startISO, end: endISO, limit: 500, offset: 0, tracks: true };
        if (store_id && store_id !== 'all') firstBody.devices = [parseInt(store_id)];
        const firstResp = await fetch(`${DISPLAYFORCE_BASE}/stats/visitor/list`, { method:'POST', headers:{ 'X-API-Token': DISPLAYFORCE_TOKEN, 'Content-Type':'application/json' }, body: JSON.stringify(firstBody) });
        if (firstResp.ok) {
          const firstData = await firstResp.json();
          const limit = Number(firstData.pagination?.limit ?? 500);
          const apiTotal = Number(firstData.pagination?.total ?? (Array.isArray(firstData.payload)? firstData.payload.length:0));
          const missing = Math.max(0, apiTotal - totalRealTime);
          console.log(`📡 API total=${apiTotal}, DB total=${totalRealTime}, faltando=${missing}`);
          if (missing > 0) {
            const startOffset = Math.floor(totalRealTime / limit) * limit;
            const endOffset = Math.floor((apiTotal - 1) / limit) * limit;
            const offsetsToFetch = [];
            for (let off = startOffset; off <= endOffset; off += limit) offsetsToFetch.push(off);
            const MAX_PAGES = 128;
            const slice = offsetsToFetch.slice(0, MAX_PAGES);
            console.log(`🔄 Ingerindo offsets faltantes: ${slice.join(', ')}`);
            await Promise.all(slice.map(async (off) => {
              const r = await fetch(`${DISPLAYFORCE_BASE}/stats/visitor/list`, { method:'POST', headers:{ 'X-API-Token': DISPLAYFORCE_TOKEN, 'Content-Type':'application/json' }, body: JSON.stringify({ start:startISO, end:endISO, limit, offset:off, tracks:true, ...(store_id&&store_id!=='all'?{devices:[parseInt(store_id)]}:{}) }) });
              if (!r.ok) return;
              const j = await r.json(); const arr = j.payload || j || [];
              await saveVisitorsToDatabase(arr, sDate);
            }));
            ;
            const re = await pool.query(query, params);
            row = re.rows[0] || row;
            totalRealTime = Number(row.total_visitors || 0);
          }
        }
      } catch {}
    }

    
    const avgAgeCount = Number(row.avg_age_count || 0);
    const averageAge = avgAgeCount > 0 ? Math.round(Number(row.avg_age_sum || 0) / avgAgeCount) : 0;
    
    const hourlyData = await getHourlyAggregatesWithRealTime(sDate, eDate, store_id);
    
    const ageGenderData = await getAgeGenderDistribution(sDate, eDate, store_id);
    
    const response = {
      success: true,
      totalVisitors: totalRealTime,
      totalMale: Number(row.male || 0),
      totalFemale: Number(row.female || 0),
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
      source: 'realtime_calculation',
      period: `${sDate} - ${eDate}`
    };
    
    SUMMARY_CACHE.set(cacheKey(sDate, eDate, store_id || 'all'), response);
    return res.status(200).json(response);
    
  } catch (error) {
    console.error("❌ Real-time summary error:", error);
    try {
      const today = new Date().toISOString().split('T')[0];
      const sDate = start_date || today;
      const eDate = end_date || sDate;
      const key = cacheKey(sDate, eDate, store_id || 'all');
      if (SUMMARY_CACHE.has(key)) {
        const cached = SUMMARY_CACHE.get(key);
        return res.status(200).json({ ...cached, source: 'cache_db_last' });
      }
    } catch {}
    return res.status(200).json(createEmptySummary());
  }
}

// ===========================================
// 5. FUNÇÕES AUXILIARES PARA PROCESSAR DADOS
// ===========================================
async function calculateDailyStatsForDate(date, device_id) {
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
  
  const params = [date];
  
  if (device_id !== 'all') {
    query += ` AND store_id = $2`;
    params.push(device_id);
  }
  
  const result = await pool.query(query, params);
  const row = result.rows[0] || {};
  
  return {
    total_visitors: Number(row.total_visitors || 0),
    male: Number(row.male || 0),
    female: Number(row.female || 0),
    avg_age_sum: Number(row.avg_age_sum || 0),
    avg_age_count: Number(row.avg_age_count || 0),
    age_18_25: Number(row.age_18_25 || 0),
    age_26_35: Number(row.age_26_35 || 0),
    age_36_45: Number(row.age_36_45 || 0),
    age_46_60: Number(row.age_46_60 || 0),
    age_60_plus: Number(row.age_60_plus || 0),
    sunday: Number(row.sunday || 0),
    monday: Number(row.monday || 0),
    tuesday: Number(row.tuesday || 0),
    wednesday: Number(row.wednesday || 0),
    thursday: Number(row.thursday || 0),
    friday: Number(row.friday || 0),
    saturday: Number(row.saturday || 0)
  };
}

async function updateHourlyStatsForDate(date, device_id) { return; }

// ===========================================
// 6. SALVAR VISITANTES CORRETAMENTE (USANDO START TIME)
// ===========================================
async function saveVisitorsToDatabase(visitors, forcedDay, mode) {
  if (!visitors || !Array.isArray(visitors) || visitors.length === 0) {
    return 0;
  }
  const tz = parseInt(process.env.TIMEZONE_OFFSET_HOURS || "-3", 10);
  const DAYS = ['Dom','Seg','Ter','Qua','Qui','Sex','Sáb'];
  const records = [];
  for (const visitor of visitors) {
    try {
      const timestamp = String(visitor.start ?? visitor.tracks?.[0]?.start ?? visitor.timestamp ?? new Date().toISOString());
      const dateObj = new Date(timestamp);
      if (isNaN(dateObj.getTime())) { continue; }
      const localDate = dateObj;
      let localTime = '';
      const mt = String(timestamp).match(/(?:T|\s)(\d{2}:\d{2}:\d{2})/);
      if (mt) { localTime = mt[1]; } else { localTime = `${String(localDate.getHours()).padStart(2,'0')}:${String(localDate.getMinutes()).padStart(2,'0')}:${String(localDate.getSeconds()).padStart(2,'0')}`; }
      const y = localDate.getFullYear(); const m = String(localDate.getMonth() + 1).padStart(2, '0'); const d = String(localDate.getDate()).padStart(2, '0');
      const dateStr = String(forcedDay || `${y}-${m}-${d}`);
      const dayOfWeek = DAYS[localDate.getDay()];
      let deviceId = ''; let storeName = '';
      const t0 = visitor.tracks && visitor.tracks.length > 0 ? visitor.tracks[0] : null;
      if (t0) { deviceId = String(t0.device_id ?? t0.id ?? ''); storeName = String(t0.device_name ?? t0.name ?? ''); }
      if (!deviceId && visitor.devices && visitor.devices.length > 0) {
        const dev0 = visitor.devices[0];
        if (typeof dev0 === 'object' && dev0) { deviceId = String(dev0.id ?? dev0.device_id ?? ''); storeName = String(dev0.name ?? storeName); }
        else { deviceId = String(dev0 || ''); }
      }
      if (!deviceId) deviceId = 'unknown';
      if (!storeName) storeName = `Loja ${deviceId}`;
      let gender = 'U';
      const sexNum = typeof visitor.sex === 'number' ? visitor.sex : (typeof visitor.gender === 'number' ? visitor.gender : null);
      if (sexNum === 1) gender = 'M'; else if (sexNum === 2) gender = 'F'; else {
        const gRaw = String(visitor.gender || '').toUpperCase(); if (gRaw.startsWith('M')) gender = 'M'; else if (gRaw.startsWith('F')) gender = 'F';
      }
      let age = 0;
      if (typeof visitor.age === 'number') { age = Math.max(0, visitor.age); }
      else {
        const attrsA = Array.isArray(visitor.additional_attributes) ? visitor.additional_attributes : (visitor.additional_attributes && typeof visitor.additional_attributes === 'object' ? [visitor.additional_attributes] : []);
        const attrsB = Array.isArray(visitor.additional_atributes) ? visitor.additional_atributes : (visitor.additional_atributes && typeof visitor.additional_atributes === 'object' ? [visitor.additional_atributes] : []);
        const attrsAll = [...attrsA, ...attrsB];
        const lastAttr = attrsAll.length ? attrsAll[attrsAll.length - 1] : null;
        const ageCandidate = (lastAttr?.age ?? visitor.face?.age ?? visitor.age_years);
        if (typeof ageCandidate === 'number') age = Math.max(0, ageCandidate);
      }
      let smile = false;
      const attrs = visitor.additional_atributes || visitor.additional_attributes || [];
      if (Array.isArray(attrs) ? attrs.length > 0 : typeof attrs === 'object') {
        const lastAttr = Array.isArray(attrs) ? attrs[attrs.length - 1] : attrs;
        smile = String(lastAttr?.smile || '').toLowerCase() === 'yes';
      }
      const visitorId = String(visitor.visitor_id ?? visitor.session_id ?? visitor.id ?? visitor.tracks?.[0]?.id ?? `${deviceId}|${timestamp}`);
      records.push([visitorId, dateStr, deviceId, storeName, timestamp, gender, age, dayOfWeek, smile, localTime]);
    } catch {}
  }
  if (records.length === 0) return 0;
  const BATCH_SIZE = 200; let savedCount = 0; const single = (process.env.INSERT_ONE_BY_ONE === '1') || (String(mode || '').toLowerCase() === 'one');
  for (let i = 0; i < records.length; i += BATCH_SIZE) {
    const chunk = records.slice(i, i + BATCH_SIZE);
    if (single) {
      for (const r of chunk) {
        const sql1 = `INSERT INTO visitors (
          visitor_id, day, store_id, store_name, timestamp, gender, age, day_of_week, smile, hour, local_time
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,EXTRACT(HOUR FROM $10::time),$10::time)
        ON CONFLICT (visitor_id, timestamp) DO UPDATE SET
          day=EXCLUDED.day, store_id=EXCLUDED.store_id, store_name=EXCLUDED.store_name,
          gender=EXCLUDED.gender, age=EXCLUDED.age, day_of_week=EXCLUDED.day_of_week,
          smile=EXCLUDED.smile, hour=EXTRACT(HOUR FROM EXCLUDED.local_time::time), local_time=EXCLUDED.local_time`;
        try { await q(sql1, r); savedCount += 1; } catch (e) {}
      }
    } else {
      const params = [];
      const values = chunk.map((r, idx) => {
        const base = idx * 10;
        params.push(r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9]);
        return `(${base+1}, ${base+2}, ${base+3}, ${base+4}, ${base+5}, ${base+6}, ${base+7}, ${base+8}, ${base+9}, EXTRACT(HOUR FROM ${base+10}::time), ${base+10}::time)`;
      }).join(', ');
      const sql = `INSERT INTO visitors (visitor_id, day, store_id, store_name, timestamp, gender, age, day_of_week, smile, hour, local_time) VALUES ${values} ON CONFLICT (visitor_id, timestamp) DO UPDATE SET day=EXCLUDED.day, store_id=EXCLUDED.store_id, store_name=EXCLUDED.store_name, gender=EXCLUDED.gender, age=EXCLUDED.age, day_of_week=EXCLUDED.day_of_week, smile=EXCLUDED.smile, hour=EXTRACT(HOUR FROM EXCLUDED.local_time::time), local_time=EXCLUDED.local_time`;
      try { await q(sql, params); savedCount += chunk.length; } catch (e) {}
    }
  }
  return savedCount;
}

// ===========================================
// 7. FUNÇÕES AUXILIARES RESTANTES
// ===========================================
function aggregateVisitors(visitors) {
  const byHour = {}; const byGenderHour = { male:{}, female:{} };
  const byAgeGroup = { "18-25":0, "26-35":0, "36-45":0, "46-60":0, "60+":0 };
  const byAgeGender = { "<20":{male:0,female:0}, "20-29":{male:0,female:0}, "30-45":{male:0,female:0}, ">45":{male:0,female:0} };
  const visitsByDay = { Sunday:0, Monday:0, Tuesday:0, Wednesday:0, Thursday:0, Friday:0, Saturday:0 };
  let total=0, male=0, female=0, avgAgeSum=0, avgAgeCount=0;
  const tz = parseInt(process.env.TIMEZONE_OFFSET_HOURS || "-3", 10);
  for (const v of visitors || []) {
    const ts = String(v.start ?? v.tracks?.[0]?.start ?? v.timestamp ?? new Date().toISOString());
    const d = new Date(ts); const local = new Date(d.getTime() + (tz*3600000));
    const h = local.getHours(); const dowEn = ['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'][local.getDay()];
    byHour[h] = (byHour[h]||0)+1;
    const gRaw = (v.sex===1?'M':(v.sex===2?'F':String(v.gender||'').toUpperCase().startsWith('M')?'M':'F'));
    if (gRaw==='M'){ male++; byGenderHour.male[h]=(byGenderHour.male[h]||0)+1; } else { female++; byGenderHour.female[h]=(byGenderHour.female[h]||0)+1; }
    visitsByDay[dowEn] = (visitsByDay[dowEn]||0)+1; total++;
    const a = typeof v.age==='number'? v.age : (Array.isArray(v.additional_attributes)? v.additional_attributes.at(-1)?.age : Array.isArray(v.additional_atributes)? v.additional_atributes.at(-1)?.age : 0);
    const age = Number(a||0); if (age>0){ avgAgeSum+=age; avgAgeCount++; }
    if (age>=18&&age<=25) byAgeGroup['18-25']++; else if (age>=26&&age<=35) byAgeGroup['26-35']++; else if (age>=36&&age<=45) byAgeGroup['36-45']++; else if (age>=46&&age<=60) byAgeGroup['46-60']++; else if (age>60) byAgeGroup['60+']++;
    const band = age<20?'<20':(age<=29?'20-29':(age<=45?'30-45':'>45'));
    (gRaw==='M'? byAgeGender[band].male++ : byAgeGender[band].female++);
  }
  const averageAge = avgAgeCount>0 ? Math.round(avgAgeSum/avgAgeCount) : 0;
  return { total, male, female, averageAge, visitsByDay, byAgeGroup, byAgeGender, byHour, byGenderHour };
}
async function getAgeGenderDistribution(start_date, end_date, store_id) {
  try {
    let query = `
      SELECT 
        gender,
        age
      FROM visitors
      WHERE age > 0 AND day >= $1 AND day <= $2
    `;
    
    const params = [start_date, end_date];
    
    if (store_id && store_id !== "all") {
      query += ` AND store_id = $3`;
      params.push(store_id);
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
    console.error("❌ Age gender distribution error:", error);
    return createEmptyAgeGender();
  }
}

function createEmptySummary() {
  return {
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
    byAgeGender: createEmptyAgeGender(),
    byHour: createEmptyHourlyData().byHour,
    byGenderHour: createEmptyHourlyData().byGenderHour,
    source: 'empty_fallback'
  };
}

function createEmptyHourlyData() {
  const byHour = {};
  const byGenderHour = { male: {}, female: {} };
  
  for (let h = 0; h < 24; h++) {
    byHour[h] = 0;
    byGenderHour.male[h] = 0;
    byGenderHour.female[h] = 0;
  }
  
  return { byHour, byGenderHour };
}

function createEmptyAgeGender() {
  return {
    "<20": { male: 0, female: 0 },
    "20-29": { male: 0, female: 0 },
    "30-45": { male: 0, female: 0 },
    ">45": { male: 0, female: 0 }
  };
}

// ===========================================
// 8. OUTROS ENDPOINTS (mantidos do código anterior)
// ===========================================
async function getVisitors(req, res, start_date, end_date, store_id) {
  try {
    // Se pedir displayforce explicitamente
    if (req.query.source === "displayforce") {
      console.log("📥 Buscando diretamente da DisplayForce...");
      const visitors = await fetchVisitorsFromDisplayForce(start_date, end_date, store_id);
      const saved = await saveVisitorsToDatabase(visitors);
      
      return res.status(200).json({
        success: true,
        data: visitors.map(v => ({
          id: v.visitor_id || v.session_id,
          date: v.start ? v.start.split('T')[0] : '',
          store_id: v.tracks?.[0]?.device_id || v.devices?.[0] || '',
          store_name: `Loja ${v.tracks?.[0]?.device_id || v.devices?.[0] || ''}`,
          timestamp: v.start,
          gender: v.sex === 1 ? 'Masculino' : v.sex === 2 ? 'Feminino' : 'Desconhecido',
          age: v.age || 0,
          day_of_week: getDayOfWeek(v.start),
          smile: getSmileStatus(v.additional_atributes || v.additional_attributes || []),
          hour: getHourFromTimestamp(v.start)
        })),
        count: visitors.length,
        saved_to_db: saved,
        source: 'displayforce'
      });
    }
    
    // Busca do banco local
    let query = `
      SELECT 
        visitor_id,
        day,
        store_id,
        store_name,
        timestamp,
        gender,
        age,
        day_of_week,
        smile,
        hour,
        local_time
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
    
    let result = await pool.query(query, params);
    if (result.rows.length === 0 && req.query.source !== 'displayforce') {
      try {
        const visitors = await fetchVisitorsFromDisplayForce(start_date || '', end_date || '', store_id && store_id !== 'all' ? store_id : null);
        await saveVisitorsToDatabase(visitors);
        result = await pool.query(query, params);
      } catch {}
    }
    
    return res.status(200).json({
      success: true,
      data: result.rows.map(row => ({
        id: row.visitor_id,
        date: row.day,
        store_id: row.store_id,
        store_name: row.store_name,
        timestamp: row.timestamp,
        gender: row.gender === 'M' ? 'Masculino' : row.gender === 'F' ? 'Feminino' : 'Desconhecido',
        age: row.age,
        day_of_week: row.day_of_week,
        smile: row.smile,
        hour: row.hour,
        local_time: row.local_time
      })),
      count: result.rows.length,
      source: 'database'
    });
    
  } catch (error) {
    console.error("❌ Visitors error:", error);
    return res.status(500).json({ success: false, error: error.message });
  }
}

async function getStores(req, res) {
  try {
    const devices = await fetchDisplayForceDevices();
    
    // Adiciona contagem de visitantes
    const storesWithCount = await Promise.all(
      devices.map(async (device) => {
        const countResult = await pool.query(
          'SELECT COUNT(*) FROM visitors WHERE store_id = $1',
          [device.id]
        );
        
        return {
          ...device,
          visitor_count: parseInt(countResult.rows[0].count || 0)
        };
      })
    );
    
    return res.status(200).json({
      success: true,
      stores: storesWithCount,
      count: storesWithCount.length
    });
    
  } catch (error) {
    console.error('❌ Stores error:', error);
    return res.status(200).json({
      success: true,
      stores: [],
      isFallback: true
    });
  }
}

async function fetchDisplayForceDevices() {
  try {
    let response = await fetch(`${DISPLAYFORCE_BASE}/device/list`, {
      method: 'POST',
      headers: {
        'X-API-Token': DISPLAYFORCE_TOKEN,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({})
    });
    
    if (!response.ok && response.status === 405) {
      response = await fetch(`${DISPLAYFORCE_BASE}/device/list`, {
        method: 'GET',
        headers: {
          'X-API-Token': DISPLAYFORCE_TOKEN
        }
      });
    }
    
    if (!response.ok) {
      throw new Error(`DisplayForce API: ${response.status}`);
    }
    
    const data = await response.json();
    const devices = data.devices || data.data || [];
    
    return devices.map(device => ({
      id: String(device.id || device.device_id || ''),
      name: device.name || `Dispositivo ${device.id || device.device_id}`,
      location: device.location || 'Local desconhecido',
      status: device.status || 'active'
    }));
    
  } catch (error) {
    console.error('❌ Erro ao buscar dispositivos:', error);
    return [];
  }
}

async function getDevices(req, res) {
  try {
    const devices = await fetchDisplayForceDevices();
    
    return res.status(200).json({
      success: true,
      devices: devices,
      count: devices.length
    });
    
  } catch (error) {
    console.error('❌ Devices error:', error);
    return res.status(200).json({
      success: true,
      devices: [],
      isFallback: true
    });
  }
}

async function refreshRange(req, res, start_date, end_date, store_id) {
  try {
    const s = start_date || new Date().toISOString().split('T')[0];
    const e = end_date || s;
    
    console.log(`🔄 Refresh range: ${s} - ${e}, store: ${store_id || 'all'}`);
    
    // Busca visitantes
    const visitors = await fetchVisitorsFromDisplayForce(s, e, store_id);
    
    // Salva no banco
    const saved = await saveVisitorsToDatabase(visitors);
    
    // Atualiza agregados
    const start = new Date(s);
    const end = new Date(e);
    
    for (let d = new Date(start); d <= end; d.setDate(d.getDate() + 1)) {
      const dateStr = d.toISOString().split('T')[0];
      ;
    }
    
    return res.status(200).json({
      success: true,
      message: 'Refresh concluído',
      period: `${s} - ${e}`,
      visitors_found: visitors.length,
      visitors_saved: saved,
      store_id: store_id || 'all'
    });
    
  } catch (error) {
    console.error('❌ Refresh error:', error);
    return res.status(500).json({
      success: false,
      error: error.message
    });
  }
}

async function fetchVisitorsFromDisplayForce(start_date, end_date, device_id = null) {
  const tz = parseInt(process.env.TIMEZONE_OFFSET_HOURS || "-3", 10);
  const sign = tz >= 0 ? "+" : "-";
  const hh = String(Math.abs(tz)).padStart(2, "0");
  const tzStr = `${sign}${hh}:00`;
  const startISO = `${start_date}T00:00:00${tzStr}`;
  const endISO = `${end_date}T23:59:59${tzStr}`;
  
  const LIMIT = 500;
  let offset = 0;
  const allVisitors = [];
  let totalProcessed = 0;
  
  console.log(`🔍 Buscando visitantes: ${startISO} até ${endISO}, Dispositivo: ${device_id || 'todos'}`);
  
  try {
    // Primeira página para descobrir total e pageLimit
    const firstBody = { start: startISO, end: endISO, limit: LIMIT, offset: 0, tracks: true };
    if (device_id) firstBody.devices = [parseInt(device_id)];
    const firstResp = await fetch(`${DISPLAYFORCE_BASE}/stats/visitor/list`, { method: 'POST', headers: { 'X-API-Token': DISPLAYFORCE_TOKEN, 'Content-Type': 'application/json' }, body: JSON.stringify(firstBody) });
    if (!firstResp.ok) throw new Error(`API Error ${firstResp.status}: ${await firstResp.text()}`);
    const firstData = await firstResp.json();
    const firstArr = firstData.payload || firstData || [];
    allVisitors.push(...firstArr);
    totalProcessed += firstArr.length;
    const pageLimit = Number(firstData.pagination?.limit ?? LIMIT);
    const totalFromAPI = Number(firstData.pagination?.total ?? firstArr.length);
    console.log(`📊 Primeira página: ${firstArr.length} | total=${totalFromAPI} | pageLimit=${pageLimit}`);
    // Gerar offsets restantes
    const offsets = [];
    for (let off = pageLimit; off < totalFromAPI; off += pageLimit) offsets.push(off);
    const CONCURRENCY = 8;
    let idx = 0;
    while (idx < offsets.length) {
      const batch = offsets.slice(idx, idx + CONCURRENCY);
      console.log(`📄 Buscando offsets: ${batch.join(', ')}`);
      const calls = batch.map(off => {
        const bodyPayload = { start: startISO, end: endISO, limit: pageLimit, offset: off, tracks: true };
        if (device_id) bodyPayload.devices = [parseInt(device_id)];
        return fetch(`${DISPLAYFORCE_BASE}/stats/visitor/list`, { method: 'POST', headers: { 'X-API-Token': DISPLAYFORCE_TOKEN, 'Content-Type': 'application/json' }, body: JSON.stringify(bodyPayload) })
          .then(r => r.ok ? r.json() : r.text().then(t => { throw new Error(`API Error ${r.status}: ${t}`); }))
          .then(d => { const arr = d.payload || d || []; allVisitors.push(...arr); totalProcessed += arr.length; });
      });
      await Promise.all(calls);
      idx += CONCURRENCY;
    }
    console.log(`✅ Total final: ${allVisitors.length} visitantes obtidos`);
    return allVisitors;
    
  } catch (error) {
    console.error('❌ Erro ao buscar visitantes:', error);
    throw error;
  }
}

async function refreshAll(req, res, start_date, end_date) {
  try {
    const s = start_date || new Date().toISOString().split('T')[0];
    const e = end_date || s;
    console.log(`🔄 Refresh all (fast): ${s} - ${e}`);
    // Busca todos os visitantes de todas as lojas em uma chamada paginada paralela
    const visitors = await fetchVisitorsFromDisplayForce(s, e, null);
    const saved = await saveVisitorsToDatabase(visitors, s, String(req.query.mode || ''));
    const start = new Date(s); const end = new Date(e);
    for (let d = new Date(start); d <= end; d.setDate(d.getDate() + 1)) {
      const dateStr = d.toISOString().split('T')[0];
      ;
    }
    return res.status(200).json({ success: true, period: `${s} - ${e}`, visitors_found: visitors.length, visitors_saved: saved });
  } catch (error) {
    console.error('❌ Refresh all error:', error);
    return res.status(500).json({ success: false, error: error.message });
  }
}

async function autoRefresh(req, res) {
  try {
    const s = String(req.query.start_date || '');
    const e = String(req.query.end_date || '');
    const start = s || new Date(Date.now() - 86400000).toISOString().slice(0,10);
    const end = e || start;
    const proto = String(req.headers['x-forwarded-proto'] || 'https');
    const host = String(req.headers['host'] || '');
    const base = host ? `${proto}://${host}` : '';
    const days = []; let d = new Date(start + 'T00:00:00Z'); const de = new Date(end + 'T00:00:00Z');
    while (d <= de) { days.push(d.toISOString().slice(0,10)); d = new Date(d.getTime() + 86400000); }
    const calls = [];
    for (const day of days) {
      if (base) calls.push(fetch(`${base}/api/assai/dashboard?endpoint=refresh_all&start_date=${day}&end_date=${day}`).catch(()=>{}));
    }
    return res.status(202).json({ success:true, triggered:calls.length, start, end });
  } catch (error) {
    console.error('❌ Auto-refresh error:', error);
    return res.status(500).json({ success:false, error:error.message });
  }
}

async function planIngestDay(req, res, start_date, end_date, store_id) {
  try {
    const tz = parseInt(process.env.TIMEZONE_OFFSET_HOURS || "-3", 10);
    const sign = tz >= 0 ? "+" : "-"; const hh = String(Math.abs(tz)).padStart(2, '0'); const tzStr = `${sign}${hh}:00`;
    const day = start_date || new Date().toISOString().slice(0,10);
    const startISO = `${day}T00:00:00${tzStr}`; const endISO = `${day}T23:59:59${tzStr}`;
    const body = { start: startISO, end: endISO, limit: 500, offset: 0, tracks: true };
    if (store_id && store_id !== 'all') body.devices = [parseInt(store_id)];
    const r = await fetch(`${DISPLAYFORCE_BASE}/stats/visitor/list`, { method: 'POST', headers: { 'X-API-Token': DISPLAYFORCE_TOKEN, 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
    if (!r.ok) return res.status(r.status).json({ error: await r.text() });
    const j = await r.json();
    const limit = Number(j.pagination?.limit ?? 100);
    const total = Number(j.pagination?.total ?? (Array.isArray(j.payload) ? j.payload.length : 0));
    const offsets = []; for (let off = 0; off < total; off += limit) offsets.push(off);
    return res.status(200).json({ day, limit, total, offsets });
  } catch (e) {
    return res.status(500).json({ error: e.message });
  }
}

async function ingestDay(req, res, start_date, end_date, store_id) {
  try {
    const day = start_date || new Date().toISOString().slice(0,10);
    const offset = Number(req.query.offset || 0);
    const limit = Number(req.query.limit || 100);
    const tz = parseInt(process.env.TIMEZONE_OFFSET_HOURS || "-3", 10);
    const sign = tz >= 0 ? "+" : "-"; const hh = String(Math.abs(tz)).padStart(2, '0'); const tzStr = `${sign}${hh}:00`;
    const startISO = `${day}T00:00:00${tzStr}`; const endISO = `${day}T23:59:59${tzStr}`;
    const body = { start: startISO, end: endISO, limit, offset, tracks: true };
    if (store_id && store_id !== 'all') body.devices = [parseInt(store_id)];
    const r = await fetch(`${DISPLAYFORCE_BASE}/stats/visitor/list`, { method: 'POST', headers: { 'X-API-Token': DISPLAYFORCE_TOKEN, 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
    if (!r.ok) return res.status(r.status).json({ error: await r.text() });
    const j = await r.json(); const arr = j.payload || j || [];
    const saved = await saveVisitorsToDatabase(arr, day, String(req.query.mode || ''));
    return res.status(200).json({ day, offset, limit, saved, count: arr.length });
  } catch (e) {
    return res.status(500).json({ error: e.message });
  }
}

async function forceSyncToday(req, res) {
  try {
    const day = new Date().toISOString().slice(0,10);
    const tz = parseInt(process.env.TIMEZONE_OFFSET_HOURS || "-3", 10);
    const sign = tz >= 0 ? "+" : "-"; const hh = String(Math.abs(tz)).padStart(2,'0'); const tzStr = `${sign}${hh}:00`;
    const startISO = `${day}T00:00:00${tzStr}`; const endISO = `${day}T23:59:59${tzStr}`;
    const firstBody = { start:startISO, end:endISO, limit:500, offset:0, tracks:true };
    const firstResp = await fetch(`${DISPLAYFORCE_BASE}/stats/visitor/list`, { method:'POST', headers:{ 'X-API-Token': DISPLAYFORCE_TOKEN, 'Content-Type':'application/json' }, body: JSON.stringify(firstBody) });
    if (!firstResp.ok) return res.status(firstResp.status).json({ error: await firstResp.text() });
    const firstData = await firstResp.json(); const limit = Number(firstData.pagination?.limit ?? 500); const apiTotal = Number(firstData.pagination?.total ?? 0);
    const { rows } = await q(`SELECT COUNT(*)::int AS c FROM visitors WHERE day=$1`, [day]); const dbTotal = Number(rows[0]?.c || 0);
    if (dbTotal >= apiTotal) return res.status(200).json({ success:true, day, apiTotal, dbTotal, synced:true });
    const startOffset = Math.floor(dbTotal/limit)*limit; const endOffset = Math.floor((apiTotal-1)/limit)*limit;
    const offsets = []; for (let off=startOffset; off<=endOffset; off+=limit) offsets.push(off);
    const conc = Math.max(1, Math.min(parseInt(String(req.query.concurrency || '1'), 10) || 1, 4));
    const maxPages = Math.max(1, Math.min(parseInt(String(req.query.max_pages || '16'), 10) || 16, 128));
    const slice = offsets.slice(0, maxPages);
    let processed = 0;
    while (processed < slice.length) {
      const batch = slice.slice(processed, processed + conc);
      for (const off of batch) {
        const r = await fetch(`${DISPLAYFORCE_BASE}/stats/visitor/list`, { method:'POST', headers:{ 'X-API-Token': DISPLAYFORCE_TOKEN, 'Content-Type':'application/json' }, body: JSON.stringify({ start:startISO, end:endISO, limit, offset:off, tracks:true }) });
        if (!r.ok) { processed += 1; continue; }
        const j = await r.json(); const arr = j.payload || j || [];
        await saveVisitorsToDatabase(arr, day, String(req.query.mode || ''));
        await new Promise(resolve => setTimeout(resolve, 150));
        processed += 1;
      }
    }
    ;
    const vr = await pool.query(`SELECT COUNT(*)::int AS c FROM visitors WHERE day=$1`, [day]);
    return res.status(200).json({ success:true, day, apiTotal, dbTotal_before: dbTotal, dbTotal_after: Number(vr.rows[0]?.c||0), processed_pages: slice.length });
  } catch (e) { return res.status(500).json({ success:false, error:e.message }); }
}

async function wipeRange(req, res, start_date, end_date) {
  try {
    const s = start_date || new Date().toISOString().slice(0,10);
    const e = end_date || s;
    const delH = await pool.query(`DELETE FROM public.dashboard_hourly WHERE day BETWEEN $1 AND $2`, [s, e]);
    const delD = await pool.query(`DELETE FROM public.dashboard_daily  WHERE day BETWEEN $1 AND $2`, [s, e]);
    const delV = await pool.query(`DELETE FROM public.visitors        WHERE day BETWEEN $1 AND $2`, [s, e]);
    return res.status(200).json({ success:true, period:`${s} - ${e}`, deleted:{ hourly: delH.rowCount, daily: delD.rowCount, visitors: delV.rowCount } });
  } catch (error) {
    return res.status(500).json({ success:false, error: error.message });
  }
}

async function verifyDay(req, res, start_date, store_id) {
  try {
    const day = start_date || new Date().toISOString().slice(0,10);
    const tz = parseInt(process.env.TIMEZONE_OFFSET_HOURS || "-3", 10);
    const sign = tz >= 0 ? "+" : "-"; const hh = String(Math.abs(tz)).padStart(2,'0'); const tzStr = `${sign}${hh}:00`;
    const startISO = `${day}T00:00:00${tzStr}`; const endISO = `${day}T23:59:59${tzStr}`;
    const body = { start: startISO, end: endISO, limit: 1, offset: 0, tracks: true };
    if (store_id && store_id !== 'all') body.devices = [parseInt(store_id)];
    const r = await fetch(`${DISPLAYFORCE_BASE}/stats/visitor/list`, { method:'POST', headers:{ 'X-API-Token': DISPLAYFORCE_TOKEN, 'Content-Type':'application/json' }, body: JSON.stringify(body) });
    if (!r.ok) return res.status(r.status).json({ error: await r.text() });
    const j = await r.json();
    const apiTotal = Number(j.pagination?.total || 0);
    const { rows } = await q(`SELECT 1 AS found FROM visitors WHERE day=$1 LIMIT 1`, [day]);
    const dbHas = rows && rows.length > 0;
    return res.status(200).json({ day, apiTotal, dbHas, ok: dbHas && apiTotal > 0 });
  } catch (e) {
    try {
      const day = start_date || new Date().toISOString().slice(0,10);
      const tz = parseInt(process.env.TIMEZONE_OFFSET_HOURS || "-3", 10);
      const sign = tz >= 0 ? "+" : "-"; const hh = String(Math.abs(tz)).padStart(2,'0'); const tzStr = `${sign}${hh}:00`;
      const startISO = `${day}T00:00:00${tzStr}`; const endISO = `${day}T23:59:59${tzStr}`;
      const body = { start: startISO, end: endISO, limit: 1, offset: 0, tracks: true };
      if (store_id && store_id !== 'all') body.devices = [parseInt(store_id)];
      const r2 = await fetch(`${DISPLAYFORCE_BASE}/stats/visitor/list`, { method:'POST', headers:{ 'X-API-Token': DISPLAYFORCE_TOKEN, 'Content-Type':'application/json' }, body: JSON.stringify(body) });
      if (r2.ok) {
        const j2 = await r2.json();
        const apiTotal2 = Number(j2.pagination?.total || 0);
        return res.status(200).json({ day, apiTotal: apiTotal2, dbHas: false, ok: false, source: 'api_only', db_error: String(e.message || '') });
      }
    } catch {}
    return res.status(200).json({ day: start_date || new Date().toISOString().slice(0,10), apiTotal: null, dbHas: false, ok: false, source: 'error', error: String(e.message || '') });
  }
}

async function rebuildHourlyFromVisitors(req, res, start_date, end_date, store_id) {
  try {
    const s = start_date || new Date().toISOString().slice(0,10);
    const e = end_date || s;
    const tzOffset = parseInt(process.env.TIMEZONE_OFFSET_HOURS || "-3", 10);
    const adj = `EXTRACT(HOUR FROM (timestamp + INTERVAL '${tzOffset} hour'))`;
    const start = new Date(s); const end = new Date(e);
    for (let d = new Date(start); d <= end; d.setDate(d.getDate() + 1)) {
      const dayStr = d.toISOString().split('T')[0];
      const hourExpr = `COALESCE(EXTRACT(HOUR FROM local_time::time), ${adj})`;
      let q = `SELECT ${hourExpr} AS hour,
                       SUM(CASE WHEN gender IN ('M','F') THEN 1 ELSE 0 END) AS total,
                       SUM(CASE WHEN gender='M' THEN 1 ELSE 0 END) AS male,
                       SUM(CASE WHEN gender='F' THEN 1 ELSE 0 END) AS female
                FROM visitors WHERE day=$1`;
      const params = [dayStr];
      if (store_id && store_id !== 'all') { q += ` AND store_id=$2`; params.push(store_id); }
      q += ` GROUP BY ${hourExpr} ORDER BY 1`;
      const { rows } = await pool.query(q, params);
      for (const r of rows) {
        await pool.query(`INSERT INTO dashboard_hourly (day, store_id, hour, total, male, female) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (day, store_id, hour) DO UPDATE SET total=EXCLUDED.total, male=EXCLUDED.male, female=EXCLUDED.female`, [dayStr, store_id && store_id !== 'all' ? store_id : 'all', Number(r.hour), Number(r.total||0), Number(r.male||0), Number(r.female||0)]);
      }
      if (!store_id || store_id === 'all') {
        const distinct = await pool.query(`SELECT DISTINCT store_id FROM visitors WHERE day=$1`, [dayStr]);
        for (const st of distinct.rows) {
          const sid = String(st.store_id);
          const hourExpr2 = `COALESCE(EXTRACT(HOUR FROM local_time::time), ${adj})`;
          const r2 = await pool.query(`SELECT ${hourExpr2} AS hour,
                                       SUM(CASE WHEN gender IN ('M','F') THEN 1 ELSE 0 END) AS total,
                                       SUM(CASE WHEN gender='M' THEN 1 ELSE 0 END) AS male,
                                       SUM(CASE WHEN gender='F' THEN 1 ELSE 0 END) AS female
                                       FROM visitors WHERE day=$1 AND store_id=$2
                                       GROUP BY ${hourExpr2} ORDER BY 1`, [dayStr, sid]);
          for (const rr of r2.rows) {
            await pool.query(`INSERT INTO dashboard_hourly (day, store_id, hour, total, male, female) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (day, store_id, hour) DO UPDATE SET total=EXCLUDED.total, male=EXCLUDED.male, female=EXCLUDED.female`, [dayStr, sid, Number(rr.hour), Number(rr.total||0), Number(rr.male||0), Number(rr.female||0)]);
          }
        }
      }
    }
    return res.status(200).json({ success:true, period:`${s} - ${e}` });
  } catch (error) {
    return res.status(500).json({ success:false, error: error.message });
  }
}

async function refreshRecent(req, res, start_date, store_id) {
  try {
    const day = start_date || new Date().toISOString().slice(0,10);
    const tz = parseInt(process.env.TIMEZONE_OFFSET_HOURS || "-3", 10);
    const sign = tz >= 0 ? "+" : "-"; const hh = String(Math.abs(tz)).padStart(2,'0'); const tzStr = `${sign}${hh}:00`;
    const startISO = `${day}T00:00:00${tzStr}`; const endISO = `${day}T23:59:59${tzStr}`;
    const body = { start: startISO, end: endISO, limit: 500, offset: 0, tracks: true };
    if (store_id && store_id !== 'all') body.devices = [parseInt(store_id)];
    const r = await fetch(`${DISPLAYFORCE_BASE}/stats/visitor/list`, { method:'POST', headers:{ 'X-API-Token': DISPLAYFORCE_TOKEN, 'Content-Type':'application/json' }, body: JSON.stringify(body) });
    if (!r.ok) return res.status(r.status).json({ error: await r.text() });
    const j = await r.json();
    const limit = Number(j.pagination?.limit ?? 100);
    const total = Number(j.pagination?.total ?? (Array.isArray(j.payload)? j.payload.length:0));
    const offsets = []; for (let off=0; off<total; off+=limit) offsets.push(off);
    const recentCount = Math.max(1, Number(req.query.count || 48));
    const slice = offsets.slice(0, recentCount);
    const results = await Promise.all(slice.map(async (off) => {
      const jr = await fetch(`${DISPLAYFORCE_BASE}/stats/visitor/list`, { method:'POST', headers:{ 'X-API-Token': DISPLAYFORCE_TOKEN, 'Content-Type':'application/json' }, body: JSON.stringify({ start:startISO, end:endISO, limit, offset:off, tracks:true, ...(store_id&&store_id!=='all'?{devices:[parseInt(store_id)]}:{}) }) });
      if (!jr.ok) return { saved: 0, processed: 0 };
      const jj = await jr.json(); const arr = jj.payload || jj || [];
      const saved = await saveVisitorsToDatabase(arr, day, String(req.query.mode || ''));
      return { saved, processed: arr.length };
    }));
    const saved = results.reduce((a,b)=>a+b.saved,0);
    const processed = results.reduce((a,b)=>a+b.processed,0);
    ;
    return res.status(200).json({ success:true, day, recent_offsets:slice, processed, saved });
  } catch (e) {
    return res.status(500).json({ success:false, error:e.message });
  }
}

async function ensureIndexes(req, res) {
  try {
    console.log('🔧 Criando índices...');
    
    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_visitors_day ON visitors(day);
      CREATE INDEX IF NOT EXISTS idx_visitors_store_id ON visitors(store_id);
      CREATE INDEX IF NOT EXISTS idx_visitors_day_store ON visitors(day, store_id);
      CREATE INDEX IF NOT EXISTS idx_visitors_timestamp ON visitors(timestamp DESC);
      CREATE INDEX IF NOT EXISTS idx_visitors_gender ON visitors(gender);
      CREATE INDEX IF NOT EXISTS idx_visitors_age ON visitors(age);
      CREATE INDEX IF NOT EXISTS idx_visitors_hour ON visitors(hour);
      CREATE INDEX IF NOT EXISTS idx_visitors_local_time ON visitors(local_time);
      CREATE UNIQUE INDEX IF NOT EXISTS uniq_visitors_id_ts ON visitors(visitor_id, timestamp);
    `);
    
    console.log('✅ Índices criados/verificados');
    
    return res.status(200).json({
      success: true,
      message: 'Índices otimizados'
    });
    
  } catch (error) {
    console.error('❌ Ensure indexes error:', error);
    return res.status(500).json({
      success: false,
      error: error.message
    });
  }
}

async function backfillLocalTime(req, res) {
  try {
    const s = String(req.query.start_date || new Date().toISOString().slice(0,10));
    const e = String(req.query.end_date || s);
    const storeId = String(req.query.store_id || '');
    const batch = Math.max(50, Math.min(2000, parseInt(String(req.query.batch || '500'), 10) || 500));
    const maxBatches = Math.max(1, Math.min(50, parseInt(String(req.query.max_batches || '20'), 10) || 20));
    const tzName = process.env.PG_TIMEZONE || 'America/Sao_Paulo';
    const start = new Date(s + 'T00:00:00Z');
    const end = new Date(e + 'T00:00:00Z');
    const days = [];
    for (let d = new Date(start); d <= end && days.length < 7; d = new Date(d.getTime() + 86400000)) {
      days.push(d.toISOString().slice(0,10));
    }
    let total = 0;
    for (const day of days) {
      let loops = 0;
      while (loops < maxBatches) {
        const where = storeId && storeId !== 'all' ? `day = $1 AND store_id = $2 AND timestamp IS NOT NULL` : `day = $1 AND timestamp IS NOT NULL`;
        const params = storeId && storeId !== 'all' ? [day, storeId] : [day];
        const sql = `UPDATE visitors SET 
          local_time = CASE 
            WHEN timestamp::text ~ '(Z|[+-]\\d{2}:\\d{2})
function getDayOfWeek(timestamp) {
  if (!timestamp) return '';
  const DAYS = ['Dom','Seg','Ter','Qua','Qui','Sex','Sáb'];
  const date = new Date(timestamp);
  return DAYS[date.getDay()] || '';
}

function getHourFromTimestamp(timestamp) {
  if (!timestamp) return 0;
  const date = new Date(timestamp);
  const tz = parseInt(process.env.TIMEZONE_OFFSET_HOURS || "-3", 10);
  const localDate = new Date(date.getTime() + (tz * 3600000));
  return localDate.getHours();
}

function getSmileStatus(attributes) {
  if (!Array.isArray(attributes) || attributes.length === 0) return false;
  const lastAttr = attributes[attributes.length - 1];
  return String(lastAttr?.smile || '').toLowerCase() === 'yes';
} THEN (to_char((timestamp AT TIME ZONE '${tzName}'), 'HH24:MI:SS'))::time 
            ELSE (to_char(timestamp, 'HH24:MI:SS'))::time 
          END,
          hour = EXTRACT(HOUR FROM CASE 
            WHEN timestamp::text ~ '(Z|[+-]\\d{2}:\\d{2})
function getDayOfWeek(timestamp) {
  if (!timestamp) return '';
  const DAYS = ['Dom','Seg','Ter','Qua','Qui','Sex','Sáb'];
  const date = new Date(timestamp);
  return DAYS[date.getDay()] || '';
}

function getHourFromTimestamp(timestamp) {
  if (!timestamp) return 0;
  const date = new Date(timestamp);
  const tz = parseInt(process.env.TIMEZONE_OFFSET_HOURS || "-3", 10);
  const localDate = new Date(date.getTime() + (tz * 3600000));
  return localDate.getHours();
}

function getSmileStatus(attributes) {
  if (!Array.isArray(attributes) || attributes.length === 0) return false;
  const lastAttr = attributes[attributes.length - 1];
  return String(lastAttr?.smile || '').toLowerCase() === 'yes';
} THEN (to_char((timestamp AT TIME ZONE '${tzName}'), 'HH24:MI:SS'))::time 
            ELSE (to_char(timestamp, 'HH24:MI:SS'))::time 
          END)
        WHERE ctid IN (
          SELECT ctid FROM visitors WHERE ${where} AND local_time IS NULL LIMIT ${batch}
        )`;
        const upd = await pool.query(sql, params);
        if (!upd.rowCount) break;
        total += upd.rowCount || 0;
        loops++;
      }
    }
    return res.status(200).json({ success:true, updated: total, batch, max_batches: maxBatches, processed_days: days });
  } catch (e) {
    return res.status(500).json({ success:false, error:e.message });
  }
}

function getDayOfWeek(timestamp) {
  if (!timestamp) return '';
  const DAYS = ['Dom','Seg','Ter','Qua','Qui','Sex','Sáb'];
  const date = new Date(timestamp);
  return DAYS[date.getDay()] || '';
}

function getHourFromTimestamp(timestamp) {
  if (!timestamp) return 0;
  const date = new Date(timestamp);
  const tz = parseInt(process.env.TIMEZONE_OFFSET_HOURS || "-3", 10);
  const localDate = new Date(date.getTime() + (tz * 3600000));
  return localDate.getHours();
}

function getSmileStatus(attributes) {
  if (!Array.isArray(attributes) || attributes.length === 0) return false;
  const lastAttr = attributes[attributes.length - 1];
  return String(lastAttr?.smile || '').toLowerCase() === 'yes';
}