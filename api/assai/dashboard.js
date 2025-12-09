// api/assai/dashboard.js - API CORRIGIDA PARA DISPLAYFORCE
import { Pool } from 'pg';

// Configurar conexão com PostgreSQL
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

// Configurações DisplayForce
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
    console.log(`📊 Endpoint: ${endpoint}, Dates: ${start_date} - ${end_date}, Store: ${store_id}`);
    
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
      
      case 'sync_now':
        return await syncNow(req, res);
      
      case 'test':
        return res.status(200).json({
          success: true,
          message: 'API Assaí está funcionando!',
          endpoints: ['visitors', 'summary', 'stores', 'devices', 'refresh', 'refresh_all', 'auto_refresh', 'optimize', 'sync_now', 'test'],
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
            'refresh_all - Atualiza todas as lojas',
            'auto_refresh - Atualiza automaticamente',
            'optimize - Cria índices',
            'sync_now - Sincronização manual',
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
// 1. FUNÇÃO PRINCIPAL DE SINCRONIZAÇÃO
// ===========================================
async function syncNow(req, res) {
  try {
    const yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);
    const yesterdayStr = yesterday.toISOString().split('T')[0];
    
    console.log(`🚀 Sincronização manual iniciada para ${yesterdayStr}`);
    
    // Busca todos os dispositivos
    const devices = await fetchDisplayForceDevices();
    
    console.log(`📱 ${devices.length} dispositivos encontrados`);
    
    // Sincroniza cada dispositivo
    const results = [];
    for (const device of devices) {
      const deviceId = device.id;
      console.log(`🔄 Sincronizando dispositivo ${deviceId}...`);
      
      try {
        // Busca visitantes do dispositivo
        const visitors = await fetchVisitorsFromDisplayForce(
          yesterdayStr,
          yesterdayStr,
          deviceId
        );
        
        console.log(`📊 ${visitors.length} visitantes para dispositivo ${deviceId}`);
        
        // Salva no banco
        const saved = await saveVisitorsToDatabase(visitors);
        
        results.push({
          device_id: deviceId,
          visitors_found: visitors.length,
          visitors_saved: saved,
          success: true
        });
        
        // Atualiza agregados
        await updateAggregatesForDevice(yesterdayStr, deviceId);
        
      } catch (deviceError) {
        console.error(`❌ Erro no dispositivo ${deviceId}:`, deviceError.message);
        results.push({
          device_id: deviceId,
          error: deviceError.message,
          success: false
        });
      }
    }
    
    // Atualiza agregado geral
    await updateAggregatesForDevice(yesterdayStr, 'all');
    
    return res.status(200).json({
      success: true,
      message: 'Sincronização concluída',
      date: yesterdayStr,
      results: results,
      total_devices: devices.length,
      timestamp: new Date().toISOString()
    });
    
  } catch (error) {
    console.error('❌ Sync error:', error);
    return res.status(500).json({
      success: false,
      error: error.message
    });
  }
}

// ===========================================
// 2. BUSCA VISITANTES DA DISPLAYFORCE
// ===========================================
async function fetchVisitorsFromDisplayForce(start_date, end_date, device_id = null) {
  const tz = parseInt(process.env.TIMEZONE_OFFSET_HOURS || "-3", 10);
  const sign = tz >= 0 ? "+" : "-";
  const hh = String(Math.abs(tz)).padStart(2, "0");
  const tzStr = `${sign}${hh}:00`;
  const startISO = `${start_date}T00:00:00${tzStr}`;
  const endISO = `${end_date}T23:59:59${tzStr}`;
  
  const LIMIT = 100;
  let offset = 0;
  const allVisitors = [];
  let totalPages = 1;
  let currentPage = 0;
  
  console.log(`🔍 Buscando visitantes: ${startISO} até ${endISO}, Dispositivo: ${device_id || 'todos'}`);
  
  try {
    while (true) {
      const bodyPayload = {
        start: startISO,
        end: endISO,
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
      
      console.log(`📄 Página ${currentPage + 1}, offset: ${offset}`);
      
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
      
      // Verifica paginação
      if (data.pagination) {
        const total = data.pagination.total || 0;
        totalPages = Math.ceil(total / LIMIT);
        console.log(`📊 Total: ${total}, Página ${currentPage + 1}/${totalPages}`);
        
        if (allVisitors.length >= total) {
          break;
        }
      }
      
      // Se não há mais dados
      if (visitors.length < LIMIT) {
        break;
      }
      
      offset += LIMIT;
      currentPage++;
      
      // Limite de segurança
      if (currentPage >= 50) {
        console.warn('⚠️ Limite de páginas atingido (50)');
        break;
      }
      
      // Pequena pausa para evitar rate limiting
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    
    console.log(`✅ ${allVisitors.length} visitantes encontrados`);
    return allVisitors;
    
  } catch (error) {
    console.error('❌ Erro ao buscar visitantes:', error);
    throw error;
  }
}

// ===========================================
// 3. SALVA VISITANTES NO BANCO
// ===========================================
async function saveVisitorsToDatabase(visitors) {
  if (!visitors || !Array.isArray(visitors) || visitors.length === 0) {
    console.log('ℹ️ Nenhum visitante para salvar');
    return 0;
  }
  
  const DAYS = ['Dom','Seg','Ter','Qua','Qui','Sex','Sáb'];
  const tz = parseInt(process.env.TIMEZONE_OFFSET_HOURS || "-3", 10);
  let savedCount = 0;
  let errorCount = 0;
  
  console.log(`💾 Salvando ${visitors.length} visitantes no banco...`);
  
  for (const visitor of visitors) {
    try {
      // Valida dados básicos
      if (!visitor.visitor_id && !visitor.session_id) {
        console.warn('⚠️ Visitante sem ID válido:', visitor);
        continue;
      }
      
      const visitorId = visitor.visitor_id || visitor.session_id;
      const timestamp = visitor.start || visitor.tracks?.[0]?.start || new Date().toISOString();
      const dateObj = new Date(timestamp);
      
      if (isNaN(dateObj.getTime())) {
        console.warn('⚠️ Data inválida para visitante:', visitorId);
        continue;
      }
      
      // Calcula hora local
      const localDate = new Date(dateObj.getTime() + (tz * 3600000));
      const hour = localDate.getHours();
      const dateStr = localDate.toISOString().split('T')[0];
      const dayOfWeek = DAYS[localDate.getDay()];
      
      // Extrai device_id
      let deviceId = 'unknown';
      if (visitor.tracks && visitor.tracks.length > 0) {
        deviceId = String(visitor.tracks[0].device_id || '');
      } else if (visitor.devices && visitor.devices.length > 0) {
        deviceId = String(visitor.devices[0] || '');
      }
      
      // Processa gênero
      let gender = 'U';
      if (visitor.sex === 1) gender = 'M';
      else if (visitor.sex === 2) gender = 'F';
      
      // Processa idade
      let age = 0;
      if (typeof visitor.age === 'number') {
        age = Math.max(0, visitor.age);
      }
      
      // Processa smile
      let smile = false;
      const attrs = visitor.additional_atributes || visitor.additional_attributes || [];
      if (attrs.length > 0) {
        const lastAttr = attrs[attrs.length - 1];
        smile = String(lastAttr?.smile || '').toLowerCase() === 'yes';
      }
      
      // Insere no banco
      await pool.query(
        `INSERT INTO visitors (
          visitor_id, day, store_id, store_name, 
          timestamp, gender, age, day_of_week, smile, hour
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        ON CONFLICT (visitor_id, timestamp) 
        DO UPDATE SET
          day = EXCLUDED.day,
          store_id = EXCLUDED.store_id,
          store_name = EXCLUDED.store_name,
          gender = EXCLUDED.gender,
          age = EXCLUDED.age,
          day_of_week = EXCLUDED.day_of_week,
          smile = EXCLUDED.smile,
          hour = EXCLUDED.hour`,
        [
          visitorId,
          dateStr,
          deviceId,
          `Loja ${deviceId}`,
          timestamp,
          gender,
          age,
          dayOfWeek,
          smile,
          hour
        ]
      );
      
      savedCount++;
      
    } catch (error) {
      errorCount++;
      console.error('❌ Erro ao salvar visitante:', error.message);
      if (errorCount <= 5) {
        console.error('❌ Dados do visitante:', JSON.stringify(visitor, null, 2));
      }
    }
  }
  
  console.log(`✅ ${savedCount} visitantes salvos, ${errorCount} erros`);
  return savedCount;
}

// ===========================================
// 4. ATUALIZA AGREGADOS
// ===========================================
async function updateAggregatesForDevice(date, device_id) {
  try {
    console.log(`📈 Atualizando agregados para ${date}, dispositivo: ${device_id}`);
    
    // Calcula estatísticas do dia
    const stats = await calculateDailyStats(date, device_id);
    
    // Salva em dashboard_daily
    await pool.query(
      `INSERT INTO dashboard_daily (
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
        updated_at = EXCLUDED.updated_at`,
      [
        date,
        device_id,
        stats.total_visitors,
        stats.male,
        stats.female,
        stats.avg_age_sum,
        stats.avg_age_count,
        stats.age_18_25,
        stats.age_26_35,
        stats.age_36_45,
        stats.age_46_60,
        stats.age_60_plus,
        stats.monday,
        stats.tuesday,
        stats.wednesday,
        stats.thursday,
        stats.friday,
        stats.saturday,
        stats.sunday
      ]
    );
    
    // Atualiza dados por hora
    await updateHourlyStats(date, device_id);
    
    console.log(`✅ Agregados atualizados para ${date}, dispositivo: ${device_id}`);
    
  } catch (error) {
    console.error(`❌ Erro ao atualizar agregados para ${date}, ${device_id}:`, error);
  }
}

async function calculateDailyStats(date, device_id) {
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

async function updateHourlyStats(date, device_id) {
  try {
    // Limpa dados existentes
    await pool.query(
      'DELETE FROM dashboard_hourly WHERE day = $1 AND store_id = $2',
      [date, device_id]
    );
    
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
    
    const params = [date];
    
    if (device_id !== 'all') {
      query += ` AND store_id = $2`;
      params.push(device_id);
    }
    
    query += ` GROUP BY hour ORDER BY hour`;
    
    const result = await pool.query(query, params);
    
    // Insere dados por hora
    for (const row of result.rows) {
      await pool.query(
        `INSERT INTO dashboard_hourly (day, store_id, hour, total, male, female)
         VALUES ($1, $2, $3, $4, $5, $6)`,
        [
          date,
          device_id,
          Number(row.hour),
          Number(row.total || 0),
          Number(row.male || 0),
          Number(row.female || 0)
        ]
      );
    }
    
  } catch (error) {
    console.error(`❌ Erro ao atualizar hourly stats:`, error);
  }
}

// ===========================================
// 5. BUSCA DISPOSITIVOS DA DISPLAYFORCE
// ===========================================
async function fetchDisplayForceDevices() {
  try {
    console.log('🌐 Buscando dispositivos da DisplayForce...');
    
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
    
    console.log(`✅ ${devices.length} dispositivos encontrados`);
    
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

// ===========================================
// 6. GET VISITORS (CORRIGIDO)
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
    
    const result = await pool.query(query, params);
    
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
        hour: row.hour
      })),
      count: result.rows.length,
      source: 'database'
    });
    
  } catch (error) {
    console.error("❌ Visitors error:", error);
    return res.status(500).json({
      success: false,
      error: error.message
    });
  }
}

// ===========================================
// 7. GET SUMMARY (CORRIGIDO)
// ===========================================
async function getSummary(req, res, start_date, end_date, store_id) {
  try {
    console.log(`📊 Summary request: ${start_date} - ${end_date}, store: ${store_id}`);
    
    // Primeiro, sincroniza se necessário
    if (req.query.sync === 'true') {
      await syncData(start_date, end_date, store_id);
    }
    
    // Busca dados agregados
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
    
    // Se não tem dados, calcula na hora
    if (Number(row.total_visitors || 0) === 0) {
      console.log("📊 Calculando summary em tempo real...");
      return await calculateRealTimeSummary(res, start_date, end_date, store_id);
    }
    
    // Calcula idade média
    const avgAgeCount = Number(row.avg_age_count || 0);
    const averageAge = avgAgeCount > 0 ? Math.round(Number(row.avg_age_sum || 0) / avgAgeCount) : 0;
    
    // Busca dados por hora
    const hourlyData = await getHourlyAggregates(start_date, end_date, store_id);
    
    // Busca distribuição por idade e gênero
    const ageGenderData = await getAgeGenderDistribution(start_date, end_date, store_id);
    
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
      source: 'dashboard_aggregates'
    };
    
    return res.status(200).json(response);
    
  } catch (error) {
    console.error("❌ Summary error:", error);
    return await calculateRealTimeSummary(res, start_date, end_date, store_id);
  }
}

async function syncData(start_date, end_date, store_id) {
  try {
    console.log(`🔄 Sincronizando dados para ${start_date} - ${end_date}`);
    
    const visitors = await fetchVisitorsFromDisplayForce(start_date, end_date, store_id);
    await saveVisitorsToDatabase(visitors);
    
    // Atualiza agregados para cada dia
    const start = new Date(start_date);
    const end = new Date(end_date);
    
    for (let d = new Date(start); d <= end; d.setDate(d.getDate() + 1)) {
      const dateStr = d.toISOString().split('T')[0];
      await updateAggregatesForDevice(dateStr, store_id || 'all');
    }
    
  } catch (error) {
    console.error('❌ Sync data error:', error);
  }
}

async function calculateRealTimeSummary(res, start_date, end_date, store_id) {
  try {
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
    
    const result = await pool.query(query, params);
    const row = result.rows[0] || {};
    
    // Calcula idade média
    const avgAgeCount = Number(row.avg_age_count || 0);
    const averageAge = avgAgeCount > 0 ? Math.round(Number(row.avg_age_sum || 0) / avgAgeCount) : 0;
    
    // Busca dados por hora
    const hourlyData = await getRealTimeHourlyData(start_date, end_date, store_id);
    
    // Busca distribuição por idade e gênero
    const ageGenderData = await getAgeGenderDistribution(start_date, end_date, store_id);
    
    const response = {
      success: true,
      totalVisitors: Number(row.total_visitors || 0),
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
      source: 'realtime_calculation'
    };
    
    return res.status(200).json(response);
    
  } catch (error) {
    console.error("❌ Real-time summary error:", error);
    return res.status(200).json(createEmptySummary());
  }
}

async function getHourlyAggregates(start_date, end_date, store_id) {
  try {
    let query = `
      SELECT 
        hour,
        COALESCE(SUM(total), 0) AS total,
        COALESCE(SUM(male), 0) AS male,
        COALESCE(SUM(female), 0) AS female
      FROM dashboard_hourly
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
    
    query += ` GROUP BY hour ORDER BY hour ASC`;
    
    const result = await pool.query(query, params);
    
    const byHour = {};
    const byGenderHour = { male: {}, female: {} };
    
    // Inicializa
    for (let h = 0; h < 24; h++) {
      byHour[h] = 0;
      byGenderHour.male[h] = 0;
      byGenderHour.female[h] = 0;
    }
    
    // Preenche
    for (const row of result.rows) {
      const hour = Number(row.hour);
      byHour[hour] = Number(row.total || 0);
      byGenderHour.male[hour] = Number(row.male || 0);
      byGenderHour.female[hour] = Number(row.female || 0);
    }
    
    return { byHour, byGenderHour };
  } catch (error) {
    console.error("❌ Hourly aggregates error:", error);
    return createEmptyHourlyData();
  }
}

async function getRealTimeHourlyData(start_date, end_date, store_id) {
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
    
    // Inicializa
    for (let h = 0; h < 24; h++) {
      byHour[h] = 0;
      byGenderHour.male[h] = 0;
      byGenderHour.female[h] = 0;
    }
    
    // Preenche
    for (const row of result.rows) {
      const hour = Number(row.hour);
      byHour[hour] = Number(row.total || 0);
      byGenderHour.male[hour] = Number(row.male || 0);
      byGenderHour.female[hour] = Number(row.female || 0);
    }
    
    return { byHour, byGenderHour };
  } catch (error) {
    console.error("❌ Real-time hourly error:", error);
    return createEmptyHourlyData();
  }
}

async function getAgeGenderDistribution(start_date, end_date, store_id) {
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
// 8. FUNÇÕES AUXILIARES
// ===========================================
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

// ===========================================
// 9. OUTROS ENDPOINTS
// ===========================================
async function getStores(req, res) {
  try {
    const devices = await fetchDisplayForceDevices();
    
    // Adiciona contagem de visitantes de cada dispositivo
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
    
    // Fallback
    return res.status(200).json({
      success: true,
      stores: [
        { id: '15267', name: 'Loja Principal', visitor_count: 0, status: 'active' },
        { id: '15268', name: 'Loja Norte', visitor_count: 0, status: 'active' },
        { id: '15269', name: 'Loja Sul', visitor_count: 0, status: 'active' }
      ],
      isFallback: true
    });
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
      await updateAggregatesForDevice(dateStr, store_id || 'all');
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

async function refreshAll(req, res, start_date, end_date) {
  try {
    const s = start_date || new Date().toISOString().split('T')[0];
    const e = end_date || s;
    
    console.log(`🔄 Refresh all: ${s} - ${e}`);
    
    const devices = await fetchDisplayForceDevices();
    const results = [];
    
    for (const device of devices) {
      try {
        const visitors = await fetchVisitorsFromDisplayForce(s, e, device.id);
        const saved = await saveVisitorsToDatabase(visitors);
        
        // Atualiza agregados para este dispositivo
        const start = new Date(s);
        const end = new Date(e);
        
        for (let d = new Date(start); d <= end; d.setDate(d.getDate() + 1)) {
          const dateStr = d.toISOString().split('T')[0];
          await updateAggregatesForDevice(dateStr, device.id);
        }
        
        results.push({
          device_id: device.id,
          visitors_found: visitors.length,
          visitors_saved: saved,
          success: true
        });
        
      } catch (deviceError) {
        console.error(`❌ Erro no dispositivo ${device.id}:`, deviceError.message);
        results.push({
          device_id: device.id,
          error: deviceError.message,
          success: false
        });
      }
    }
    
    // Atualiza agregado geral
    const start = new Date(s);
    const end = new Date(e);
    
    for (let d = new Date(start); d <= end; d.setDate(d.getDate() + 1)) {
      const dateStr = d.toISOString().split('T')[0];
      await updateAggregatesForDevice(dateStr, 'all');
    }
    
    return res.status(200).json({
      success: true,
      message: 'Refresh all concluído',
      period: `${s} - ${e}`,
      devices: devices.length,
      results: results
    });
    
  } catch (error) {
    console.error('❌ Refresh all error:', error);
    return res.status(500).json({
      success: false,
      error: error.message
    });
  }
}

async function autoRefresh(req, res) {
  try {
    const yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);
    const dateStr = yesterday.toISOString().split('T')[0];
    
    console.log(`🤖 Auto-refresh para ${dateStr}`);
    
    // Chama refresh para ontem
    await refreshRange({}, res, dateStr, dateStr, 'all');
    
    return res.status(202).json({
      success: true,
      message: 'Auto-refresh iniciado',
      date: dateStr
    });
    
  } catch (error) {
    console.error('❌ Auto-refresh error:', error);
    return res.status(500).json({
      success: false,
      error: error.message
    });
  }
}

async function ensureIndexes(req, res) {
  try {
    console.log('🔧 Criando índices...');
    
    // Índices para visitors
    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_visitors_day ON visitors(day);
      CREATE INDEX IF NOT EXISTS idx_visitors_store_id ON visitors(store_id);
      CREATE INDEX IF NOT EXISTS idx_visitors_day_store ON visitors(day, store_id);
      CREATE INDEX IF NOT EXISTS idx_visitors_timestamp ON visitors(timestamp DESC);
      CREATE INDEX IF NOT EXISTS idx_visitors_gender ON visitors(gender);
      CREATE INDEX IF NOT EXISTS idx_visitors_age ON visitors(age);
      CREATE INDEX IF NOT EXISTS idx_visitors_hour ON visitors(hour);
    `);
    
    // Índices para dashboard_daily
    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_dashboard_daily_day ON dashboard_daily(day);
      CREATE INDEX IF NOT EXISTS idx_dashboard_daily_store ON dashboard_daily(store_id);
      CREATE INDEX IF NOT EXISTS idx_dashboard_daily_day_store ON dashboard_daily(day, store_id);
    `);
    
    // Índices para dashboard_hourly
    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_dashboard_hourly_day ON dashboard_hourly(day);
      CREATE INDEX IF NOT EXISTS idx_dashboard_hourly_store ON dashboard_hourly(store_id);
      CREATE INDEX IF NOT EXISTS idx_dashboard_hourly_hour ON dashboard_hourly(hour);
      CREATE INDEX IF NOT EXISTS idx_dashboard_hourly_day_store_hour ON dashboard_hourly(day, store_id, hour);
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