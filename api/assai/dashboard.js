// api/assai/dashboard.js - CÓDIGO CORRIGIDO
import { Pool } from 'pg';

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

const DISPLAYFORCE_TOKEN = process.env.DISPLAYFORCE_API_TOKEN || '4AUH-BX6H-G2RJ-G7PB';
const DISPLAYFORCE_BASE = process.env.DISPLAYFORCE_API_URL || 'https://api.displayforce.ai/public/v1';

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  
  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }
  
  const { endpoint, start_date, end_date, store_id } = req.query;
  
  try {
    console.log(`📊 API: ${endpoint} - ${start_date} to ${end_date}`);
    
    switch (endpoint) {
      case 'summary':
        return await getSummary(req, res, start_date, end_date, store_id);
      
      case 'stores':
        return await getStores(req, res);
      
      case 'sync':
        return await syncData(req, res, start_date, end_date, store_id);
      
      case 'test':
        return await testAPI(req, res);
      
      default:
        return res.status(200).json({
          success: true,
          message: 'API Assaí Dashboard',
          endpoints: ['summary', 'stores', 'sync', 'test']
        });
    }
    
  } catch (error) {
    console.error('🔥 API Error:', error);
    return res.status(500).json({
      success: false,
      error: error.message
    });
  }
}

// ===========================================
// BUSCAR DISPOSITIVOS (CORRIGIDO)
// ===========================================
async function getDevices() {
  try {
    console.log('🌐 Buscando dispositivos...');
    
    // Tentar POST primeiro (método correto)
    const response = await fetch(`${DISPLAYFORCE_BASE}/device/list`, {
      method: 'POST',
      headers: {
        'X-API-Token': DISPLAYFORCE_TOKEN,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
      },
      body: JSON.stringify({})  // Corpo vazio
    });
    
    // Se POST falhar, tentar GET
    if (!response.ok) {
      console.log('⚠️ POST falhou, tentando GET...');
      const getResponse = await fetch(`${DISPLAYFORCE_BASE}/device/list`, {
        method: 'GET',
        headers: {
          'X-API-Token': DISPLAYFORCE_TOKEN,
          'Accept': 'application/json'
        }
      });
      
      if (!getResponse.ok) {
        throw new Error(`GET também falhou: ${getResponse.status}`);
      }
      
      const data = await getResponse.json();
      console.log(`✅ ${data.devices?.length || data.data?.length || 0} dispositivos via GET`);
      return data.devices || data.data || [];
    }
    
    const data = await response.json();
    console.log(`✅ ${data.devices?.length || data.data?.length || 0} dispositivos via POST`);
    return data.devices || data.data || [];
    
  } catch (error) {
    console.error('❌ Erro ao buscar dispositivos:', error.message);
    throw error;
  }
}

// ===========================================
// BUSCAR VISITANTES
// ===========================================
async function getVisitorsFromDisplayForce(start_date, end_date, store_id) {
  const tz = parseInt(process.env.TIMEZONE_OFFSET_HOURS || "-3", 10);
  const sign = tz >= 0 ? "+" : "-";
  const hh = String(Math.abs(tz)).padStart(2, "0");
  const tzStr = `${sign}${hh}:00`;
  
  const startISO = start_date ? `${start_date}T00:00:00${tzStr}` : 
    `${new Date().toISOString().split('T')[0]}T00:00:00${tzStr}`;
  const endISO = end_date ? `${end_date}T23:59:59${tzStr}` : 
    `${new Date().toISOString().split('T')[0]}T23:59:59${tzStr}`;
  
  console.log(`🔄 Buscando visitantes: ${startISO} a ${endISO}`);
  
  try {
    const body = {
      start: startISO,
      end: endISO,
      limit: 1000,
      offset: 0
    };
    
    if (store_id && store_id !== 'all') {
      body.devices = [store_id];
    }
    
    const response = await fetch(`${DISPLAYFORCE_BASE}/stats/visitor/list`, {
      method: 'POST',
      headers: {
        'X-API-Token': DISPLAYFORCE_TOKEN,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
      },
      body: JSON.stringify(body)
    });
    
    if (!response.ok) {
      const errorText = await response.text();
      console.error(`❌ API Visitantes erro: ${response.status} - ${errorText}`);
      
      // Tentar com Content-Type diferente
      const formResponse = await fetch(`${DISPLAYFORCE_BASE}/stats/visitor/list`, {
        method: 'POST',
        headers: {
          'X-API-Token': DISPLAYFORCE_TOKEN,
          'Content-Type': 'application/x-www-form-urlencoded',
          'Accept': 'application/json'
        },
        body: `start=${encodeURIComponent(startISO)}&end=${encodeURIComponent(endISO)}&limit=1000&offset=0`
      });
      
      if (!formResponse.ok) {
        throw new Error(`Ambos métodos falharam: ${response.status}, ${formResponse.status}`);
      }
      
      const data = await formResponse.json();
      return data.payload || data.data || [];
    }
    
    const data = await response.json();
    console.log(`✅ ${data.payload?.length || data.data?.length || 0} visitantes encontrados`);
    return data.payload || data.data || [];
    
  } catch (error) {
    console.error('❌ Erro na API de visitantes:', error.message);
    throw error;
  }
}

// ===========================================
// RESUMO DO DASHBOARD
// ===========================================
async function getSummary(req, res, start_date, end_date, store_id) {
  try {
    const startDate = start_date || new Date().toISOString().split('T')[0];
    const endDate = end_date || startDate;
    
    console.log(`📊 Processando resumo: ${startDate} a ${endDate}, loja: ${store_id || 'all'}`);
    
    // Buscar visitantes
    const rawVisitors = await getVisitorsFromDisplayForce(startDate, endDate, store_id);
    
    // Processar estatísticas
    const stats = processStats(rawVisitors, store_id);
    
    return res.status(200).json({
      success: true,
      ...stats,
      source: 'displayforce',
      period: `${startDate} a ${endDate}`
    });
    
  } catch (error) {
    console.error('❌ Summary error:', error.message);
    return res.status(200).json({
      success: false,
      error: error.message,
      source: 'error'
    });
  }
}

// ===========================================
// LOJAS
// ===========================================
async function getStores(req, res) {
  try {
    console.log('🏪 Buscando lojas...');
    
    // Buscar dispositivos
    const devices = await getDevices();
    
    // Buscar visitantes dos últimos 7 dias para estatísticas
    const endDate = new Date().toISOString().split('T')[0];
    const startDate = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
    
    let visitors = [];
    try {
      visitors = await getVisitorsFromDisplayForce(startDate, endDate, 'all');
    } catch (e) {
      console.log('⚠️ Não foi possível buscar estatísticas de visitantes');
    }
    
    // Calcular contagem por dispositivo
    const visitorCounts = {};
    visitors.forEach(v => {
      const deviceId = v.tracks?.[0]?.device_id || v.devices?.[0];
      if (deviceId) {
        visitorCounts[deviceId] = (visitorCounts[deviceId] || 0) + 1;
      }
    });
    
    // Criar lista de lojas
    const stores = devices.map(device => ({
      id: String(device.id || device.device_id || ''),
      name: device.name || `Loja ${device.id}`,
      visitor_count: visitorCounts[String(device.id || device.device_id)] || 0,
      status: device.status || 'active',
      location: device.location || 'Assaí Atacadista'
    }));
    
    // Adicionar "Todas as Lojas"
    const totalVisitors = Object.values(visitorCounts).reduce((a, b) => a + b, 0);
    stores.unshift({
      id: 'all',
      name: 'Todas as Lojas',
      visitor_count: totalVisitors,
      status: 'active',
      location: 'Todas as unidades'
    });
    
    console.log(`✅ ${stores.length} lojas encontradas`);
    
    return res.status(200).json({
      success: true,
      stores: stores,
      count: stores.length,
      source: 'displayforce'
    });
    
  } catch (error) {
    console.error('❌ Stores error:', error.message);
    
    // Fallback com lojas de exemplo
    return res.status(200).json({
      success: true,
      stores: [
        { id: 'all', name: 'Todas as Lojas', visitor_count: 0, status: 'active', location: 'Todas' },
        { id: '15287', name: 'Assaí Cajamar', visitor_count: 0, status: 'active', location: 'SP' },
        { id: '15286', name: 'Assaí São Paulo', visitor_count: 0, status: 'active', location: 'SP' }
      ],
      isFallback: true,
      error: error.message
    });
  }
}

// ===========================================
// SINCRONIZAR DADOS
// ===========================================
async function syncData(req, res, start_date, end_date, store_id) {
  try {
    const startDate = start_date || new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString().split('T')[0];
    const endDate = end_date || new Date().toISOString().split('T')[0];
    
    console.log(`🔄 Sincronizando: ${startDate} a ${endDate}`);
    
    // Buscar visitantes
    const visitors = await getVisitorsFromDisplayForce(startDate, endDate, store_id);
    
    // Buscar dispositivos
    const devices = await getDevices();
    
    return res.status(200).json({
      success: true,
      message: 'Sincronização realizada',
      stats: {
        visitors: visitors.length,
        devices: devices.length,
        period: `${startDate} a ${endDate}`
      },
      timestamp: new Date().toISOString()
    });
    
  } catch (error) {
    console.error('❌ Sync error:', error.message);
    return res.status(200).json({
      success: false,
      error: error.message
    });
  }
}

// ===========================================
// TESTAR API
// ===========================================
async function testAPI(req, res) {
  try {
    console.log('🧪 Testando conexões...');
    
    // Testar dispositivos
    let devicesStatus = 'OK';
    try {
      const devices = await getDevices();
      devicesStatus = `OK (${devices.length} dispositivos)`;
    } catch (e) {
      devicesStatus = `ERRO: ${e.message}`;
    }
    
    // Testar visitantes
    let visitorsStatus = 'OK';
    try {
      const today = new Date().toISOString().split('T')[0];
      const visitors = await getVisitorsFromDisplayForce(today, today, 'all');
      visitorsStatus = `OK (${visitors.length} visitantes hoje)`;
    } catch (e) {
      visitorsStatus = `ERRO: ${e.message}`;
    }
    
    return res.status(200).json({
      success: true,
      connections: {
        displayforce_devices: devicesStatus,
        displayforce_visitors: visitorsStatus
      },
      environment: {
        timezone: process.env.TIMEZONE_OFFSET_HOURS || '-3',
        token_configured: DISPLAYFORCE_TOKEN ? 'Sim' : 'Não',
        base_url: DISPLAYFORCE_BASE
      },
      timestamp: new Date().toISOString()
    });
    
  } catch (error) {
    return res.status(200).json({
      success: false,
      error: error.message
    });
  }
}

// ===========================================
// PROCESSAR ESTATÍSTICAS
// ===========================================
function processStats(visitors, store_id) {
  let total = 0;
  let male = 0;
  let female = 0;
  let ageSum = 0;
  let ageCount = 0;
  
  const byAge = { "18-25": 0, "26-35": 0, "36-45": 0, "46-60": 0, "60+": 0 };
  const byDay = { Monday: 0, Tuesday: 0, Wednesday: 0, Thursday: 0, Friday: 0, Saturday: 0, Sunday: 0 };
  const byHour = {};
  const byGenderHour = { male: {}, female: {} };
  
  // Inicializar horas
  for (let h = 0; h < 24; h++) {
    byHour[h] = 0;
    byGenderHour.male[h] = 0;
    byGenderHour.female[h] = 0;
  }
  
  // Processar cada visitante
  visitors.forEach(v => {
    const deviceId = v.tracks?.[0]?.device_id || v.devices?.[0];
    
    // Filtrar por loja se necessário
    if (store_id && store_id !== 'all' && deviceId !== store_id) {
      return;
    }
    
    total++;
    
    // Gênero
    if (v.sex === 1) {
      male++;
    } else {
      female++;
    }
    
    // Idade
    const age = parseInt(v.age || 0);
    if (age > 0) {
      ageSum += age;
      ageCount++;
      
      if (age >= 18 && age <= 25) byAge["18-25"]++;
      else if (age >= 26 && age <= 35) byAge["26-35"]++;
      else if (age >= 36 && age <= 45) byAge["36-45"]++;
      else if (age >= 46 && age <= 60) byAge["46-60"]++;
      else if (age > 60) byAge["60+"]++;
    }
    
    // Data e hora
    const timestamp = v.start || v.tracks?.[0]?.start;
    if (timestamp) {
      const date = new Date(timestamp);
      const day = date.getDay();
      const days = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
      byDay[days[day]]++;
      
      const hour = date.getHours();
      byHour[hour]++;
      if (v.sex === 1) {
        byGenderHour.male[hour]++;
      } else {
        byGenderHour.female[hour]++;
      }
    }
  });
  
  // Idade por gênero (estimado)
  const byAgeGender = {
    "<20": { male: Math.round(byAge["18-25"] * 0.3 * (male / total)), female: Math.round(byAge["18-25"] * 0.3 * (female / total)) },
    "20-29": { male: Math.round(byAge["18-25"] * 0.7 * (male / total)), female: Math.round(byAge["18-25"] * 0.7 * (female / total)) },
    "30-45": { male: Math.round((byAge["26-35"] + byAge["36-45"]) * (male / total)), female: Math.round((byAge["26-35"] + byAge["36-45"]) * (female / total)) },
    ">45": { male: Math.round((byAge["46-60"] + byAge["60+"]) * (male / total)), female: Math.round((byAge["46-60"] + byAge["60+"]) * (female / total)) }
  };
  
  return {
    totalVisitors: total,
    totalMale: male,
    totalFemale: female,
    averageAge: ageCount > 0 ? Math.round(ageSum / ageCount) : 0,
    visitsByDay: byDay,
    byAgeGroup: byAge,
    byHour: byHour,
    byGenderHour: byGenderHour,
    byAgeGender: byAgeGender
  };
}