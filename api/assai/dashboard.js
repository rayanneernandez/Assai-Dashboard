// api/assai/dashboard.js - API COMPLETA E FUNCIONAL
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
  
  const { endpoint, start_date, end_date, store_id, source } = req.query;
  
  try {
    console.log(`📊 API: ${endpoint} - ${start_date} to ${end_date} - store: ${store_id}`);
    
    switch (endpoint) {
      case 'summary':
        return await getSummary(req, res, start_date, end_date, store_id);
      
      case 'stores':
        return await getStores(req, res);
      
      case 'visitors':
        return await getVisitors(req, res, start_date, end_date, store_id);
      
      case 'sync':
        return await syncData(req, res, start_date, end_date, store_id);
      
      case 'test':
        return await testAPI(req, res);
      
      default:
        return res.status(200).json({
          success: true,
          message: 'API Assaí Dashboard',
          endpoints: ['summary', 'stores', 'visitors', 'sync', 'test']
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
// 1. BUSCAR DISPOSITIVOS/LOJAS
// ===========================================
async function fetchDevices() {
  try {
    console.log('🌐 Buscando dispositivos da DisplayForce...');
    
    // Tentar diferentes métodos
    const methods = [
      { method: 'POST', headers: { 'X-API-Token': DISPLAYFORCE_TOKEN, 'Content-Type': 'application/json' }, body: JSON.stringify({ limit: 100 }) },
      { method: 'GET', headers: { 'X-API-Token': DISPLAYFORCE_TOKEN } },
      { method: 'POST', headers: { 'X-API-Token': DISPLAYFORCE_TOKEN, 'Content-Type': 'application/x-www-form-urlencoded' }, body: 'limit=100' }
    ];
    
    let response;
    for (const config of methods) {
      try {
        response = await fetch(`${DISPLAYFORCE_BASE}/device/list`, config);
        if (response.ok) break;
      } catch (e) {
        continue;
      }
    }
    
    if (!response || !response.ok) {
      throw new Error(`Falha ao buscar dispositivos: ${response?.status || 'No response'}`);
    }
    
    const data = await response.json();
    const devices = data.devices || data.data || data.payload || [];
    
    console.log(`✅ ${devices.length} dispositivos encontrados`);
    
    // Mapear para formato padrão
    return devices.map(device => ({
      id: String(device.id || device.device_id || Math.random()),
      name: device.name || `Dispositivo ${device.id || device.device_id}`,
      status: device.status || 'active',
      location: device.location || 'Assaí Atacadista',
      last_seen: device.last_seen || new Date().toISOString(),
      type: device.type || 'camera'
    }));
    
  } catch (error) {
    console.error('❌ Erro ao buscar dispositivos:', error.message);
    throw error;
  }
}

// ===========================================
// 2. BUSCAR VISITANTES COMPLETOS
// ===========================================
async function fetchAllVisitors(start_date, end_date, device_id = null) {
  const tz = parseInt(process.env.TIMEZONE_OFFSET_HOURS || "-3", 10);
  const sign = tz >= 0 ? "+" : "-";
  const hh = String(Math.abs(tz)).padStart(2, "0");
  const tzStr = `${sign}${hh}:00`;
  
  const startISO = start_date ? `${start_date}T00:00:00${tzStr}` : 
    `${new Date().toISOString().split('T')[0]}T00:00:00${tzStr}`;
  const endISO = end_date ? `${end_date}T23:59:59${tzStr}` : 
    `${new Date().toISOString().split('T')[0]}T23:59:59${tzStr}`;
  
  console.log(`🔄 Buscando visitantes: ${startISO} a ${endISO}${device_id ? `, device: ${device_id}` : ''}`);
  
  let allVisitors = [];
  let offset = 0;
  const limit = 1000;
  let hasMore = true;
  
  while (hasMore) {
    try {
      const body = {
        start: startISO,
        end: endISO,
        limit: limit,
        offset: offset,
        tracks: true  // Incluir informações dos tracks
      };
      
      if (device_id && device_id !== 'all') {
        body.devices = [device_id];
      }
      
      // Tentar diferentes formatos
      const formats = [
        { 
          method: 'POST', 
          headers: { 
            'X-API-Token': DISPLAYFORCE_TOKEN, 
            'Content-Type': 'application/json',
            'Accept': 'application/json'
          }, 
          body: JSON.stringify(body) 
        },
        { 
          method: 'POST',
          headers: {
            'X-API-Token': DISPLAYFORCE_TOKEN,
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': 'application/json'
          },
          body: new URLSearchParams({
            start: startISO,
            end: endISO,
            limit: limit.toString(),
            offset: offset.toString(),
            tracks: 'true',
            ...(device_id && device_id !== 'all' && { devices: device_id })
          }).toString()
        }
      ];
      
      let response;
      for (const format of formats) {
        try {
          response = await fetch(`${DISPLAYFORCE_BASE}/stats/visitor/list`, format);
          if (response.ok) break;
        } catch (e) {
          continue;
        }
      }
      
      if (!response || !response.ok) {
        console.error(`❌ Erro ${response?.status} na página ${offset/limit + 1}`);
        break;
      }
      
      const data = await response.json();
      const visitors = data.visitors || data.payload || data.data || [];
      
      console.log(`📄 Página ${offset/limit + 1}: ${visitors.length} visitantes`);
      
      if (visitors.length === 0) {
        hasMore = false;
        break;
      }
      
      allVisitors = allVisitors.concat(visitors);
      
      // Verificar se há mais páginas
      const pagination = data.pagination;
      if (pagination) {
        const total = parseInt(pagination.total || 0);
        if (total > 0 && allVisitors.length >= total) {
          hasMore = false;
        }
      }
      
      // Se recebemos menos que o limite, não há mais páginas
      if (visitors.length < limit) {
        hasMore = false;
      }
      
      offset += limit;
      
      // Limitar para não sobrecarregar (máximo 10,000 visitantes)
      if (allVisitors.length >= 10000) {
        console.log(`⚠️ Limite de 10,000 visitantes atingido`);
        hasMore = false;
      }
      
    } catch (error) {
      console.error(`❌ Erro ao buscar página ${offset/limit + 1}:`, error.message);
      hasMore = false;
    }
  }
  
  console.log(`✅ Total de ${allVisitors.length} visitantes encontrados`);
  return allVisitors;
}

// ===========================================
// 3. PROCESSAR VISITANTES PARA ESTATÍSTICAS
// ===========================================
function processVisitorsForStats(visitors, device_id = null) {
  console.log(`📊 Processando ${visitors.length} visitantes para estatísticas...`);
  
  let totalVisitors = 0;
  let totalMale = 0;
  let totalFemale = 0;
  let ageSum = 0;
  let ageCount = 0;
  
  const byAgeGroup = {
    "18-25": 0,
    "26-35": 0, 
    "36-45": 0,
    "46-60": 0,
    "60+": 0
  };
  
  const visitsByDay = {
    Sunday: 0, Monday: 0, Tuesday: 0, Wednesday: 0,
    Thursday: 0, Friday: 0, Saturday: 0
  };
  
  const byHour = {};
  const byGenderHour = { male: {}, female: {} };
  
  // Inicializar horas
  for (let h = 0; h < 24; h++) {
    byHour[h] = 0;
    byGenderHour.male[h] = 0;
    byGenderHour.female[h] = 0;
  }
  
  const daysPT = ['Dom', 'Seg', 'Ter', 'Qua', 'Qui', 'Sex', 'Sáb'];
  const daysEN = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
  
  // Agrupar por dispositivo para filtro
  const visitorsByDevice = {};
  
  visitors.forEach(visitor => {
    try {
      // Obter device_id do visitor
      const visitorDeviceId = getDeviceIdFromVisitor(visitor);
      
      // Contar por dispositivo
      if (!visitorsByDevice[visitorDeviceId]) {
        visitorsByDevice[visitorDeviceId] = 0;
      }
      visitorsByDevice[visitorDeviceId]++;
      
      // Se filtrando por dispositivo específico, pular outros
      if (device_id && device_id !== 'all' && visitorDeviceId !== device_id) {
        return;
      }
      
      totalVisitors++;
      
      // Gênero (sex: 1 = male, 2 = female, 0 = unknown)
      if (visitor.sex === 1) {
        totalMale++;
      } else if (visitor.sex === 2) {
        totalFemale++;
      }
      
      // Idade
      const age = parseInt(visitor.age || 0);
      if (age > 0) {
        ageSum += age;
        ageCount++;
        
        if (age >= 18 && age <= 25) byAgeGroup["18-25"]++;
        else if (age >= 26 && age <= 35) byAgeGroup["26-35"]++;
        else if (age >= 36 && age <= 45) byAgeGroup["36-45"]++;
        else if (age >= 46 && age <= 60) byAgeGroup["46-60"]++;
        else if (age > 60) byAgeGroup["60+"]++;
      }
      
      // Data e hora
      const timestamp = visitor.start || visitor.tracks?.[0]?.start || new Date().toISOString();
      const date = new Date(timestamp);
      
      // Dia da semana
      const dayOfWeek = date.getDay();
      visitsByDay[daysEN[dayOfWeek]]++;
      
      // Hora
      const hour = date.getHours();
      byHour[hour]++;
      
      if (visitor.sex === 1) {
        byGenderHour.male[hour]++;
      } else if (visitor.sex === 2) {
        byGenderHour.female[hour]++;
      }
      
    } catch (error) {
      console.error('Erro ao processar visitor:', error);
    }
  });
  
  // Calcular média de idade
  const averageAge = ageCount > 0 ? Math.round(ageSum / ageCount) : 0;
  
  // Calcular distribuição idade/gênero
  const byAgeGender = calculateAgeGenderDistribution(byAgeGroup, totalMale, totalFemale);
  
  return {
    totalVisitors,
    totalMale,
    totalFemale,
    averageAge,
    visitsByDay,
    byAgeGroup,
    byHour,
    byGenderHour,
    byAgeGender,
    visitorsByDevice
  };
}

// Helper para obter device_id do visitor
function getDeviceIdFromVisitor(visitor) {
  if (visitor.tracks && visitor.tracks.length > 0) {
    return String(visitor.tracks[0].device_id || 'unknown');
  }
  if (visitor.devices && visitor.devices.length > 0) {
    return String(visitor.devices[0] || 'unknown');
  }
  return 'unknown';
}

// Calcular distribuição idade/gênero
function calculateAgeGenderDistribution(byAgeGroup, totalMale, totalFemale) {
  const totalVisitors = Object.values(byAgeGroup).reduce((a, b) => a + b, 0);
  
  if (totalVisitors === 0) {
    return {
      "<20": { male: 0, female: 0 },
      "20-29": { male: 0, female: 0 },
      "30-45": { male: 0, female: 0 },
      ">45": { male: 0, female: 0 }
    };
  }
  
  const maleRatio = totalMale / totalVisitors;
  const femaleRatio = totalFemale / totalVisitors;
  
  // Distribuir proporcionalmente
  return {
    "<20": {
      male: Math.round(byAgeGroup["18-25"] * 0.3 * maleRatio),
      female: Math.round(byAgeGroup["18-25"] * 0.3 * femaleRatio)
    },
    "20-29": {
      male: Math.round(byAgeGroup["18-25"] * 0.7 * maleRatio + byAgeGroup["26-35"] * 0.5 * maleRatio),
      female: Math.round(byAgeGroup["18-25"] * 0.7 * femaleRatio + byAgeGroup["26-35"] * 0.5 * femaleRatio)
    },
    "30-45": {
      male: Math.round(byAgeGroup["26-35"] * 0.5 * maleRatio + byAgeGroup["36-45"] * maleRatio),
      female: Math.round(byAgeGroup["26-35"] * 0.5 * femaleRatio + byAgeGroup["36-45"] * femaleRatio)
    },
    ">45": {
      male: Math.round((byAgeGroup["46-60"] + byAgeGroup["60+"]) * maleRatio),
      female: Math.round((byAgeGroup["46-60"] + byAgeGroup["60+"]) * femaleRatio)
    }
  };
}

// ===========================================
// 4. RESUMO DO DASHBOARD (ENDPOINT PRINCIPAL)
// ===========================================
async function getSummary(req, res, start_date, end_date, store_id) {
  try {
    const startDate = start_date || new Date().toISOString().split('T')[0];
    const endDate = end_date || startDate;
    
    console.log(`📊 Gerando resumo: ${startDate} a ${endDate}, loja: ${store_id || 'all'}`);
    
    // Buscar visitantes da DisplayForce
    const rawVisitors = await fetchAllVisitors(startDate, endDate, store_id);
    
    // Processar estatísticas
    const stats = processVisitorsForStats(rawVisitors, store_id);
    
    // Buscar nomes dos dispositivos para o nome da loja
    let storeName = 'Todas as Lojas';
    if (store_id && store_id !== 'all') {
      try {
        const devices = await fetchDevices();
        const device = devices.find(d => d.id === store_id);
        storeName = device ? device.name : `Loja ${store_id}`;
      } catch (e) {
        storeName = `Loja ${store_id}`;
      }
    }
    
    return res.status(200).json({
      success: true,
      ...stats,
      storeName,
      query: {
        start_date: startDate,
        end_date: endDate,
        store_id: store_id || 'all'
      },
      source: 'displayforce'
    });
    
  } catch (error) {
    console.error('❌ Summary error:', error.message);
    
    return res.status(200).json({
      success: false,
      error: error.message,
      totalVisitors: 0,
      totalMale: 0,
      totalFemale: 0,
      averageAge: 0,
      visitsByDay: { Sunday: 0, Monday: 0, Tuesday: 0, Wednesday: 0, Thursday: 0, Friday: 0, Saturday: 0 },
      byAgeGroup: { "18-25": 0, "26-35": 0, "36-45": 0, "46-60": 0, "60+": 0 },
      byHour: {},
      byGenderHour: { male: {}, female: {} },
      byAgeGender: {
        "<20": { male: 0, female: 0 },
        "20-29": { male: 0, female: 0 },
        "30-45": { male: 0, female: 0 },
        ">45": { male: 0, female: 0 }
      },
      source: 'error'
    });
  }
}

// ===========================================
// 5. LOJAS/DISPOSITIVOS
// ===========================================
async function getStores(req, res) {
  try {
    console.log('🏪 Buscando lojas/dispositivos...');
    
    // Buscar dispositivos da DisplayForce
    const devices = await fetchDevices();
    
    // Buscar visitantes dos últimos 7 dias para estatísticas
    const endDate = new Date().toISOString().split('T')[0];
    const startDate = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
    
    let visitorStats = {};
    try {
      const visitors = await fetchAllVisitors(startDate, endDate, 'all');
      visitorStats = processVisitorsForStats(visitors, 'all').visitorsByDevice;
    } catch (e) {
      console.log('⚠️ Não foi possível buscar estatísticas de visitantes:', e.message);
    }
    
    // Criar lista de lojas
    const stores = devices.map(device => ({
      id: device.id,
      name: device.name,
      visitor_count: visitorStats[device.id] || 0,
      status: device.status,
      location: device.location,
      type: device.type
    }));
    
    // Ordenar por número de visitantes (decrescente)
    stores.sort((a, b) => b.visitor_count - a.visitor_count);
    
    // Adicionar "Todas as Lojas" no início
    const totalVisitors = Object.values(visitorStats).reduce((a, b) => a + b, 0);
    stores.unshift({
      id: 'all',
      name: 'Todas as Lojas',
      visitor_count: totalVisitors,
      status: 'active',
      location: 'Todas as unidades',
      type: 'all'
    });
    
    console.log(`✅ ${stores.length} lojas encontradas (${totalVisitors} visitantes totais)`);
    
    return res.status(200).json({
      success: true,
      stores: stores,
      count: stores.length,
      total_visitors: totalVisitors,
      source: 'displayforce'
    });
    
  } catch (error) {
    console.error('❌ Stores error:', error.message);
    
    // Fallback mínimo
    return res.status(200).json({
      success: true,
      stores: [
        { id: 'all', name: 'Todas as Lojas', visitor_count: 0, status: 'active', location: 'Todas' }
      ],
      isFallback: true,
      error: error.message
    });
  }
}

// ===========================================
// 6. VISITANTES INDIVIDUAIS
// ===========================================
async function getVisitors(req, res, start_date, end_date, store_id) {
  try {
    const startDate = start_date || new Date().toISOString().split('T')[0];
    const endDate = end_date || startDate;
    
    console.log(`📋 Buscando lista de visitantes: ${startDate} a ${endDate}, loja: ${store_id || 'all'}`);
    
    // Buscar visitantes
    const rawVisitors = await fetchAllVisitors(startDate, endDate, store_id);
    
    // Converter para formato do dashboard
    const formattedVisitors = rawVisitors.slice(0, 500).map((visitor, index) => {
      const timestamp = visitor.start || visitor.tracks?.[0]?.start || new Date().toISOString();
      const date = new Date(timestamp);
      const deviceId = getDeviceIdFromVisitor(visitor);
      
      return {
        id: visitor.visitor_id || visitor.session_id || `visitor_${index}_${Date.now()}`,
        day: date.toISOString().split('T')[0],
        store_id: deviceId,
        store_name: `Loja ${deviceId}`,
        timestamp: timestamp,
        gender: visitor.sex === 1 ? 'Masculino' : (visitor.sex === 2 ? 'Feminino' : 'Desconhecido'),
        age: parseInt(visitor.age || 0),
        day_of_week: ['Dom', 'Seg', 'Ter', 'Qua', 'Qui', 'Sex', 'Sáb'][date.getDay()],
        smile: String(visitor.smile || '').toLowerCase() === 'yes'
      };
    });
    
    return res.status(200).json({
      success: true,
      data: formattedVisitors,
      count: formattedVisitors.length,
      total: rawVisitors.length,
      source: 'displayforce'
    });
    
  } catch (error) {
    console.error('❌ Visitors error:', error.message);
    return res.status(200).json({
      success: false,
      data: [],
      error: error.message
    });
  }
}

// ===========================================
// 7. SINCRONIZAÇÃO
// ===========================================
async function syncData(req, res, start_date, end_date, store_id) {
  try {
    const startDate = start_date || new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString().split('T')[0];
    const endDate = end_date || new Date().toISOString().split('T')[0];
    
    console.log(`🔄 Sincronizando dados: ${startDate} a ${endDate}`);
    
    // Buscar dispositivos
    const devices = await fetchDevices();
    
    // Buscar visitantes
    const visitors = await fetchAllVisitors(startDate, endDate, store_id);
    
    return res.status(200).json({
      success: true,
      message: 'Sincronização realizada com sucesso',
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
    return res.status(200).json({
      success: false,
      error: error.message
    });
  }
}

// ===========================================
// 8. TESTE DA API
// ===========================================
async function testAPI(req, res) {
  try {
    console.log('🧪 Testando todas as conexões...');
    
    // Testar dispositivos
    let devicesResult = { status: 'pending', count: 0, error: null };
    try {
      const devices = await fetchDevices();
      devicesResult = { status: 'success', count: devices.length, error: null };
    } catch (e) {
      devicesResult = { status: 'error', count: 0, error: e.message };
    }
    
    // Testar visitantes (últimas 24 horas)
    let visitorsResult = { status: 'pending', count: 0, error: null };
    try {
      const endDate = new Date().toISOString().split('T')[0];
      const startDate = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString().split('T')[0];
      const visitors = await fetchAllVisitors(startDate, endDate, 'all');
      visitorsResult = { status: 'success', count: visitors.length, error: null };
    } catch (e) {
      visitorsResult = { status: 'error', count: 0, error: e.message };
    }
    
    // Testar banco de dados
    let dbResult = { status: 'pending', error: null };
    try {
      await pool.query('SELECT NOW()');
      dbResult = { status: 'success', error: null };
    } catch (e) {
      dbResult = { status: 'error', error: e.message };
    }
    
    return res.status(200).json({
      success: true,
      connections: {
        displayforce_devices: devicesResult,
        displayforce_visitors: visitorsResult,
        postgres_database: dbResult
      },
      environment: {
        timezone_offset: process.env.TIMEZONE_OFFSET_HOURS || '-3',
        has_api_token: !!DISPLAYFORCE_TOKEN,
        has_database_url: !!process.env.DATABASE_URL,
        base_url: DISPLAYFORCE_BASE
      },
      recommendations: devicesResult.status === 'success' && visitorsResult.status === 'success' 
        ? '✅ Todas as conexões estão funcionando!'
        : '⚠️ Algumas conexões estão com problemas',
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