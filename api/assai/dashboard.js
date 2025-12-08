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

// Cache para nomes de dispositivos
let deviceCache = {};
let cacheTimestamp = 0;
const CACHE_DURATION = 3600000; // 1 hora

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
    console.log(`📊 API: ${endpoint} - ${start_date} to ${end_date} - store: ${store_id}`);
    
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
      
      case 'sync':
        return await syncFromDisplayForce(req, res, start_date, end_date, store_id);
      
      case 'test':
        return await testConnection(req, res);
      
      default:
        return res.status(200).json({
          success: true,
          message: 'API Assaí Dashboard',
          endpoints: ['visitors', 'summary', 'stores', 'devices', 'refresh', 'sync', 'test']
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
// 1. VISITANTES - SEMPRE DA DISPLAYFORCE
// ===========================================
async function getVisitors(req, res, start_date, end_date, store_id) {
  try {
    console.log(`📋 Buscando visitantes da DisplayForce...`);
    
    // Se source não for displayforce, verificar se temos no banco
    if (source !== 'displayforce') {
      const dbVisitors = await getVisitorsFromDB(start_date, end_date, store_id);
      if (dbVisitors.length > 0) {
        return res.status(200).json({
          success: true,
          data: dbVisitors,
          count: dbVisitors.length,
          source: 'database'
        });
      }
    }
    
    // Buscar da DisplayForce
    const visitors = await fetchVisitorsFromDisplayForce(start_date, end_date, store_id);
    
    // Salvar no banco para cache
    await saveVisitorsToDB(visitors);
    
    return res.status(200).json({
      success: true,
      data: visitors,
      count: visitors.length,
      source: 'displayforce'
    });
    
  } catch (error) {
    console.error("❌ Visitors error:", error.message);
    
    // Fallback para banco de dados
    try {
      const dbVisitors = await getVisitorsFromDB(start_date, end_date, store_id);
      return res.status(200).json({
        success: true,
        data: dbVisitors,
        count: dbVisitors.length,
        source: 'database_fallback',
        error: error.message
      });
    } catch (dbError) {
      return res.status(200).json({
        success: false,
        data: [],
        error: `DisplayForce: ${error.message}, Database: ${dbError.message}`
      });
    }
  }
}

// Buscar visitantes da DisplayForce
async function fetchVisitorsFromDisplayForce(start_date, end_date, store_id) {
  const tz = parseInt(process.env.TIMEZONE_OFFSET_HOURS || "-3", 10);
  const sign = tz >= 0 ? "+" : "-";
  const hh = String(Math.abs(tz)).padStart(2, "0");
  const tzStr = `${sign}${hh}:00`;
  
  const startISO = start_date ? `${start_date}T00:00:00${tzStr}` : 
    `${new Date().toISOString().split('T')[0]}T00:00:00${tzStr}`;
  const endISO = end_date ? `${end_date}T23:59:59${tzStr}` : 
    `${new Date().toISOString().split('T')[0]}T23:59:59${tzStr}`;
  
  let allVisitors = [];
  let offset = 0;
  const limit = 500;
  
  console.log(`🔄 Fetching from DisplayForce: ${startISO} to ${endISO}`);
  
  while (true) {
    const body = {
      start: startISO,
      end: endISO,
      limit: limit,
      offset: offset
    };
    
    if (store_id && store_id !== 'all') {
      body.devices = [store_id];
    }
    
    const response = await fetch(`${DISPLAYFORCE_BASE}/stats/visitor/list`, {
      method: 'POST',
      headers: {
        'X-API-Token': DISPLAYFORCE_TOKEN,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(body)
    });
    
    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`DisplayForce API error: ${response.status} - ${errorText}`);
    }
    
    const data = await response.json();
    const visitors = data.payload || data.data || [];
    
    allVisitors = allVisitors.concat(visitors);
    
    // Verificar se há mais páginas
    const pagination = data.pagination;
    if (!pagination || visitors.length < limit) {
      break;
    }
    
    offset += limit;
  }
  
  console.log(`✅ Fetched ${allVisitors.length} visitors from DisplayForce`);
  
  // Converter para formato padrão
  return allVisitors.map(visitor => ({
    id: visitor.visitor_id || visitor.session_id || `visitor_${Date.now()}_${Math.random()}`,
    day: new Date(visitor.start || visitor.tracks?.[0]?.start || new Date()).toISOString().split('T')[0],
    store_id: visitor.tracks?.[0]?.device_id || visitor.devices?.[0] || store_id || 'unknown',
    store_name: `Loja ${visitor.tracks?.[0]?.device_id || visitor.devices?.[0] || store_id || 'unknown'}`,
    timestamp: visitor.start || visitor.tracks?.[0]?.start || new Date().toISOString(),
    gender: visitor.sex === 1 ? 'M' : 'F',
    age: parseInt(visitor.age || 0),
    day_of_week: getDayOfWeekPT(new Date(visitor.start || visitor.tracks?.[0]?.start || new Date())),
    smile: String(visitor.smile || '').toLowerCase() === 'yes'
  }));
}

// ===========================================
// 2. RESUMO DO DASHBOARD - DADOS REAIS
// ===========================================
async function getSummary(req, res, start_date, end_date, store_id) {
  try {
    console.log(`📊 Buscando resumo da DisplayForce...`);
    
    const startDate = start_date || new Date().toISOString().split('T')[0];
    const endDate = end_date || startDate;
    
    // Buscar visitantes da DisplayForce
    const visitors = await fetchVisitorsFromDisplayForce(startDate, endDate, store_id);
    
    // Processar estatísticas
    const stats = processVisitorStats(visitors, store_id);
    
    // Buscar nomes dos dispositivos
    const deviceNames = await getDeviceNames();
    
    return res.status(200).json({
      success: true,
      ...stats,
      storeName: store_id === 'all' ? 'Todas as Lojas' : 
                 deviceNames[store_id] || `Loja ${store_id}`,
      source: 'displayforce',
      query: { start_date: startDate, end_date: endDate, store_id }
    });
    
  } catch (error) {
    console.error("❌ Summary error:", error.message);
    
    // Tentar buscar do banco
    try {
      const dbSummary = await getSummaryFromDB(start_date, end_date, store_id);
      return res.status(200).json({
        ...dbSummary,
        source: 'database_fallback',
        error: error.message
      });
    } catch (dbError) {
      return res.status(200).json({
        success: false,
        error: `DisplayForce: ${error.message}, Database: ${dbError.message}`
      });
    }
  }
}

// Processar estatísticas dos visitantes
function processVisitorStats(visitors, store_id) {
  let totalVisitors = 0;
  let totalMale = 0;
  let totalFemale = 0;
  let avgAgeSum = 0;
  let avgAgeCount = 0;
  
  const byAgeGroup = {
    "18-25": 0,
    "26-35": 0,
    "36-45": 0,
    "46-60": 0,
    "60+": 0
  };
  
  const visitsByDay = {
    Monday: 0, Tuesday: 0, Wednesday: 0, Thursday: 0,
    Friday: 0, Saturday: 0, Sunday: 0
  };
  
  const byHour = {};
  const byGenderHour = { male: {}, female: {} };
  
  // Inicializar horas
  for (let h = 0; h < 24; h++) {
    byHour[h] = 0;
    byGenderHour.male[h] = 0;
    byGenderHour.female[h] = 0;
  }
  
  // Processar cada visitante
  visitors.forEach(visitor => {
    // Filtrar por loja se necessário
    if (store_id && store_id !== 'all' && visitor.store_id !== store_id) {
      return;
    }
    
    totalVisitors++;
    
    // Gênero
    if (visitor.gender === 'M') {
      totalMale++;
    } else {
      totalFemale++;
    }
    
    // Idade
    if (visitor.age > 0) {
      avgAgeSum += visitor.age;
      avgAgeCount++;
      
      if (visitor.age >= 18 && visitor.age <= 25) {
        byAgeGroup["18-25"]++;
      } else if (visitor.age >= 26 && visitor.age <= 35) {
        byAgeGroup["26-35"]++;
      } else if (visitor.age >= 36 && visitor.age <= 45) {
        byAgeGroup["36-45"]++;
      } else if (visitor.age >= 46 && visitor.age <= 60) {
        byAgeGroup["46-60"]++;
      } else if (visitor.age > 60) {
        byAgeGroup["60+"]++;
      }
    }
    
    // Dia da semana
    const date = new Date(visitor.timestamp);
    const dayOfWeek = date.getDay();
    const days = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
    visitsByDay[days[dayOfWeek]]++;
    
    // Hora
    const hour = date.getHours();
    byHour[hour]++;
    if (visitor.gender === 'M') {
      byGenderHour.male[hour]++;
    } else {
      byGenderHour.female[hour]++;
    }
  });
  
  // Calcular média de idade
  const averageAge = avgAgeCount > 0 ? Math.round(avgAgeSum / avgAgeCount) : 0;
  
  // Calcular distribuição idade/gênero
  const byAgeGender = calculateAgeGenderDistribution(visitors, byAgeGroup, totalMale, totalFemale);
  
  return {
    success: true,
    totalVisitors,
    totalMale,
    totalFemale,
    averageAge,
    visitsByDay,
    byAgeGroup,
    byHour,
    byGenderHour,
    byAgeGender
  };
}

// ===========================================
// 3. LOJAS - DISPOSITIVOS DA DISPLAYFORCE
// ===========================================
async function getStores(req, res) {
  try {
    console.log('🏪 Buscando lojas da DisplayForce...');
    
    // Buscar dispositivos da DisplayForce
    const devices = await fetchDevicesFromDisplayForce();
    
    // Buscar estatísticas de visitantes
    const visitors = await fetchVisitorsFromDisplayForce(
      new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString().split('T')[0], // Últimos 30 dias
      new Date().toISOString().split('T')[0]
    );
    
    // Calcular contagem de visitantes por loja
    const storeCounts = {};
    visitors.forEach(visitor => {
      const storeId = visitor.store_id;
      if (storeId && storeId !== 'unknown') {
        storeCounts[storeId] = (storeCounts[storeId] || 0) + 1;
      }
    });
    
    // Criar lista de lojas
    const stores = devices.map(device => ({
      id: String(device.id || device.device_id || ''),
      name: device.name || `Dispositivo ${device.id}`,
      visitor_count: storeCounts[String(device.id || device.device_id || '')] || 0,
      status: device.status || 'active',
      location: device.location || 'Assaí Atacadista'
    }));
    
    // Adicionar "Todas as Lojas"
    const totalVisitors = Object.values(storeCounts).reduce((sum, count) => sum + count, 0);
    stores.unshift({
      id: 'all',
      name: 'Todas as Lojas',
      visitor_count: totalVisitors,
      status: 'active',
      location: 'Todas as unidades'
    });
    
    console.log(`✅ Encontradas ${stores.length} lojas`);
    
    return res.status(200).json({
      success: true,
      stores: stores,
      count: stores.length,
      source: 'displayforce'
    });
    
  } catch (error) {
    console.error('❌ Stores error:', error.message);
    return res.status(200).json({
      success: false,
      error: error.message,
      stores: [],
      isFallback: true
    });
  }
}

// ===========================================
// 4. DISPOSITIVOS
// ===========================================
async function getDevices(req, res) {
  try {
    console.log('🌐 Buscando dispositivos da DisplayForce...');
    
    const devices = await fetchDevicesFromDisplayForce();
    
    return res.status(200).json({
      success: true,
      devices: devices,
      count: devices.length,
      source: 'displayforce'
    });
    
  } catch (error) {
    console.error('❌ Devices error:', error.message);
    return res.status(200).json({
      success: false,
      error: error.message,
      devices: [],
      isFallback: true
    });
  }
}

// Buscar dispositivos da DisplayForce
async function fetchDevicesFromDisplayForce() {
  // Verificar cache
  const now = Date.now();
  if (now - cacheTimestamp < CACHE_DURATION && Object.keys(deviceCache).length > 0) {
    console.log('📦 Usando cache de dispositivos');
    return Object.values(deviceCache);
  }
  
  console.log('🔄 Buscando dispositivos da API...');
  
  const response = await fetch(`${DISPLAYFORCE_BASE}/device/list`, {
    method: 'GET',
    headers: {
      'X-API-Token': DISPLAYFORCE_TOKEN,
      'Accept': 'application/json'
    }
  });
  
  if (!response.ok) {
    throw new Error(`DisplayForce devices error: ${response.status}`);
  }
  
  const data = await response.json();
  const devices = data.devices || data.data || [];
  
  // Atualizar cache
  deviceCache = {};
  devices.forEach(device => {
    const deviceId = String(device.id || device.device_id || '');
    deviceCache[deviceId] = {
      id: deviceId,
      name: device.name || `Dispositivo ${deviceId}`,
      status: device.status || 'active',
      location: device.location || 'Assaí Atacadista',
      last_seen: device.last_seen || new Date().toISOString()
    };
  });
  
  cacheTimestamp = now;
  
  console.log(`✅ Encontrados ${devices.length} dispositivos`);
  return devices;
}

// ===========================================
// 5. SINCRONIZAÇÃO COMPLETA
// ===========================================
async function syncFromDisplayForce(req, res, start_date, end_date, store_id) {
  try {
    console.log(`🔄 Sincronização completa da DisplayForce...`);
    
    const startDate = start_date || new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
    const endDate = end_date || new Date().toISOString().split('T')[0];
    
    // 1. Buscar dispositivos
    const devices = await fetchDevicesFromDisplayForce();
    
    // 2. Buscar visitantes
    const visitors = await fetchVisitorsFromDisplayForce(startDate, endDate, store_id);
    
    // 3. Salvar no banco
    await saveVisitorsToDB(visitors);
    
    // 4. Atualizar estatísticas agregadas
    await updateAggregatedStats(visitors);
    
    return res.status(200).json({
      success: true,
      message: 'Sincronização completa realizada',
      stats: {
        devices: devices.length,
        visitors: visitors.length,
        period: `${startDate} a ${endDate}`,
        store: store_id || 'Todas as lojas'
      },
      timestamp: new Date().toISOString()
    });
    
  } catch (error) {
    console.error('❌ Sync error:', error.message);
    return res.status(500).json({
      success: false,
      error: error.message
    });
  }
}

// ===========================================
// 6. REFRESH (PARA COMPATIBILIDADE)
// ===========================================
async function refreshRange(req, res, start_date, end_date, store_id) {
  return await syncFromDisplayForce(req, res, start_date, end_date, store_id);
}

// ===========================================
// FUNÇÕES AUXILIARES
// ===========================================

// Obter nomes dos dispositivos
async function getDeviceNames() {
  try {
    const devices = await fetchDevicesFromDisplayForce();
    const names = {};
    devices.forEach(device => {
      const deviceId = String(device.id || device.device_id || '');
      names[deviceId] = device.name || `Loja ${deviceId}`;
    });
    return names;
  } catch (error) {
    console.error('Erro ao buscar nomes:', error.message);
    return {};
  }
}

// Converter dia da semana para português
function getDayOfWeekPT(date) {
  const days = ['Dom', 'Seg', 'Ter', 'Qua', 'Qui', 'Sex', 'Sáb'];
  return days[date.getDay()];
}

// Calcular distribuição idade/gênero
function calculateAgeGenderDistribution(visitors, byAgeGroup, totalMale, totalFemale) {
  const byAgeGender = {
    "<20": { male: 0, female: 0 },
    "20-29": { male: 0, female: 0 },
    "30-45": { male: 0, female: 0 },
    ">45": { male: 0, female: 0 }
  };
  
  // Distribuir proporcionalmente (simplificado)
  const totalVisitors = visitors.length;
  if (totalVisitors > 0) {
    const maleRatio = totalMale / totalVisitors;
    const femaleRatio = totalFemale / totalVisitors;
    
    // Supor que 18-25 inclui <20 e 20-29
    const youngTotal = byAgeGroup["18-25"] + byAgeGroup["26-35"];
    byAgeGender["<20"].male = Math.round(youngTotal * 0.3 * maleRatio);
    byAgeGender["<20"].female = Math.round(youngTotal * 0.3 * femaleRatio);
    byAgeGender["20-29"].male = Math.round(youngTotal * 0.7 * maleRatio);
    byAgeGender["20-29"].female = Math.round(youngTotal * 0.7 * femaleRatio);
    
    // 30-45
    const middleTotal = byAgeGroup["36-45"];
    byAgeGender["30-45"].male = Math.round(middleTotal * maleRatio);
    byAgeGender["30-45"].female = Math.round(middleTotal * femaleRatio);
    
    // >45
    const olderTotal = byAgeGroup["46-60"] + byAgeGroup["60+"];
    byAgeGender[">45"].male = Math.round(olderTotal * maleRatio);
    byAgeGender[">45"].female = Math.round(olderTotal * femaleRatio);
  }
  
  return byAgeGender;
}

// ===========================================
// FUNÇÕES DE BANCO DE DADOS (CACHE)
// ===========================================

// Salvar visitantes no banco
async function saveVisitorsToDB(visitors) {
  try {
    // Criar tabela se não existir
    await pool.query(`
      CREATE TABLE IF NOT EXISTS visitors_cache (
        visitor_id VARCHAR(255) PRIMARY KEY,
        day DATE NOT NULL,
        timestamp TIMESTAMP NOT NULL,
        store_id VARCHAR(100),
        store_name VARCHAR(255),
        gender CHAR(1),
        age INTEGER,
        day_of_week VARCHAR(10),
        smile BOOLEAN,
        source VARCHAR(50) DEFAULT 'displayforce',
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW()
      )
    `);
    
    // Inserir/atualizar visitantes
    for (const visitor of visitors.slice(0, 1000)) { // Limitar para não sobrecarregar
      await pool.query(`
        INSERT INTO visitors_cache 
        (visitor_id, day, timestamp, store_id, store_name, gender, age, day_of_week, smile, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())
        ON CONFLICT (visitor_id) DO UPDATE SET
          day = EXCLUDED.day,
          timestamp = EXCLUDED.timestamp,
          store_id = EXCLUDED.store_id,
          store_name = EXCLUDED.store_name,
          gender = EXCLUDED.gender,
          age = EXCLUDED.age,
          day_of_week = EXCLUDED.day_of_week,
          smile = EXCLUDED.smile,
          updated_at = NOW()
      `, [
        visitor.id,
        visitor.day,
        visitor.timestamp,
        visitor.store_id,
        visitor.store_name,
        visitor.gender,
        visitor.age,
        visitor.day_of_week,
        visitor.smile
      ]);
    }
    
    console.log(`💾 Salvo ${Math.min(visitors.length, 1000)} visitantes no cache`);
  } catch (error) {
    console.error('Erro ao salvar no banco:', error.message);
  }
}

// Buscar visitantes do banco
async function getVisitorsFromDB(start_date, end_date, store_id) {
  try {
    let query = `
      SELECT 
        visitor_id as id,
        day,
        store_id,
        store_name,
        timestamp,
        gender,
        age,
        day_of_week,
        smile
      FROM visitors_cache
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
    
    query += ` ORDER BY timestamp DESC LIMIT 500`;
    
    const result = await pool.query(query, params);
    return result.rows;
  } catch (error) {
    console.error('Erro ao buscar do banco:', error.message);
    return [];
  }
}

// Buscar resumo do banco
async function getSummaryFromDB(start_date, end_date, store_id) {
  try {
    const visitors = await getVisitorsFromDB(start_date, end_date, store_id);
    return processVisitorStats(visitors, store_id);
  } catch (error) {
    throw error;
  }
}

// Atualizar estatísticas agregadas
async function updateAggregatedStats(visitors) {
  try {
    // Criar tabela de estatísticas diárias
    await pool.query(`
      CREATE TABLE IF NOT EXISTS dashboard_stats (
        day DATE NOT NULL,
        store_id VARCHAR(100) NOT NULL,
        total_visitors INTEGER DEFAULT 0,
        male INTEGER DEFAULT 0,
        female INTEGER DEFAULT 0,
        avg_age FLOAT DEFAULT 0,
        updated_at TIMESTAMP DEFAULT NOW(),
        PRIMARY KEY (day, store_id)
      )
    `);
    
    // Agrupar por dia e loja
    const statsByDayStore = {};
    
    visitors.forEach(visitor => {
      const key = `${visitor.day}_${visitor.store_id}`;
      if (!statsByDayStore[key]) {
        statsByDayStore[key] = {
          day: visitor.day,
          store_id: visitor.store_id,
          total: 0,
          male: 0,
          female: 0,
          ageSum: 0,
          ageCount: 0
        };
      }
      
      const stat = statsByDayStore[key];
      stat.total++;
      
      if (visitor.gender === 'M') {
        stat.male++;
      } else {
        stat.female++;
      }
      
      if (visitor.age > 0) {
        stat.ageSum += visitor.age;
        stat.ageCount++;
      }
    });
    
    // Salvar no banco
    for (const key in statsByDayStore) {
      const stat = statsByDayStore[key];
      const avgAge = stat.ageCount > 0 ? stat.ageSum / stat.ageCount : 0;
      
      await pool.query(`
        INSERT INTO dashboard_stats (day, store_id, total_visitors, male, female, avg_age, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, NOW())
        ON CONFLICT (day, store_id) DO UPDATE SET
          total_visitors = EXCLUDED.total_visitors,
          male = EXCLUDED.male,
          female = EXCLUDED.female,
          avg_age = EXCLUDED.avg_age,
          updated_at = NOW()
      `, [stat.day, stat.store_id, stat.total, stat.male, stat.female, avgAge]);
    }
    
    console.log(`📊 Estatísticas atualizadas para ${Object.keys(statsByDayStore).length} combinações dia/loja`);
  } catch (error) {
    console.error('Erro ao atualizar estatísticas:', error.message);
  }
}

// ===========================================
// TESTE DE CONEXÃO
// ===========================================
async function testConnection(req, res) {
  try {
    // Testar conexão com DisplayForce
    const devicesResponse = await fetch(`${DISPLAYFORCE_BASE}/device/list`, {
      method: 'GET',
      headers: {
        'X-API-Token': DISPLAYFORCE_TOKEN,
        'Accept': 'application/json'
      }
    });
    
    const devicesStatus = devicesResponse.ok ? 'OK' : `Erro ${devicesResponse.status}`;
    
    // Testar API de visitantes
    const today = new Date().toISOString().split('T')[0];
    const tz = parseInt(process.env.TIMEZONE_OFFSET_HOURS || "-3", 10);
    const sign = tz >= 0 ? "+" : "-";
    const hh = String(Math.abs(tz)).padStart(2, "0");
    const tzStr = `${sign}${hh}:00`;
    
    const visitorsResponse = await fetch(`${DISPLAYFORCE_BASE}/stats/visitor/list`, {
      method: 'POST',
      headers: {
        'X-API-Token': DISPLAYFORCE_TOKEN,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        start: `${today}T00:00:00${tzStr}`,
        end: `${today}T23:59:59${tzStr}`,
        limit: 10
      })
    });
    
    const visitorsStatus = visitorsResponse.ok ? 'OK' : `Erro ${visitorsResponse.status}`;
    
    // Testar banco de dados
    let dbStatus = 'OK';
    try {
      await pool.query('SELECT NOW()');
    } catch (dbError) {
      dbStatus = `Erro: ${dbError.message}`;
    }
    
    return res.status(200).json({
      success: true,
      connections: {
        displayforce_devices: devicesStatus,
        displayforce_visitors: visitorsStatus,
        database: dbStatus
      },
      environment: {
        timezone_offset: process.env.TIMEZONE_OFFSET_HOURS || '-3',
        api_token: DISPLAYFORCE_TOKEN ? 'Configurado' : 'Não configurado',
        database_url: process.env.DATABASE_URL ? 'Configurado' : 'Não configurado'
      },
      timestamp: new Date().toISOString()
    });
    
  } catch (error) {
    return res.status(200).json({
      success: false,
      error: error.message,
      timestamp: new Date().toISOString()
    });
  }
}