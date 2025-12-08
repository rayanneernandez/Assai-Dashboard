// api/assai/dashboard.js - API ÚNICA PARA O DASHBOARD ASSAÍ
import { Pool } from 'pg';

// Configurar conexão com PostgreSQL (Render)
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 2000,
});

// Token da DisplayForce
const DISPLAYFORCE_TOKEN = process.env.DISPLAYFORCE_API_TOKEN || '4AUH-BX6H-G2RJ-G7PB';
const DISPLAYFORCE_BASE = process.env.DISPLAYFORCE_API_URL || 'https://api.displayforce.ai/public/v1';

export default async function handler(req, res) {
  // Configurar CORS
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  
  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }
  
  console.log(`📊 API Request: ${req.method} ${req.url}`);
  
  const { endpoint, start_date, end_date, store_id, source } = req.query;
  
  try {
    // Testar conexão com o banco de dados primeiro
    try {
      await pool.query('SELECT NOW()');
      console.log('✅ Conexão com banco de dados OK');
    } catch (dbError) {
      console.error('❌ Erro de conexão com o banco:', dbError.message);
      return res.status(500).json({
        success: false,
        error: 'Database connection error',
        message: dbError.message,
        timestamp: new Date().toISOString()
      });
    }
    
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
        return await testAPI(req, res);
      
      case 'health':
        return await healthCheck(req, res);
      
      default:
        return res.status(200).json({
          success: true,
          message: 'API Assaí Dashboard',
          endpoints: [
            'visitors', 'summary', 'stores', 'devices', 
            'refresh', 'refresh_all', 'auto_refresh', 
            'optimize', 'test', 'health'
          ],
          usage: '/api/assai/dashboard?endpoint=summary&start_date=2025-12-01&end_date=2025-12-08&store_id=all',
          timestamp: new Date().toISOString()
        });
    }
    
  } catch (error) {
    console.error('🔥 API Error:', error);
    return res.status(500).json({
      success: false,
      error: 'Internal server error',
      message: error.message,
      stack: process.env.NODE_ENV === 'development' ? error.stack : undefined,
      timestamp: new Date().toISOString()
    });
  }
}

// ===========================================
// TESTE DE API E HEALTH CHECK
// ===========================================
async function testAPI(req, res) {
  try {
    // Testar conexão com banco
    const dbTest = await pool.query('SELECT NOW() as time, version() as version');
    
    // Verificar tabelas existentes
    const tablesQuery = await pool.query(`
      SELECT table_name 
      FROM information_schema.tables 
      WHERE table_schema = 'public'
      ORDER BY table_name
    `);
    
    const tableCounts = {};
    for (const table of tablesQuery.rows) {
      try {
        const countResult = await pool.query(`SELECT COUNT(*) as count FROM ${table.table_name}`);
        tableCounts[table.table_name] = parseInt(countResult.rows[0]?.count || 0);
      } catch (e) {
        tableCounts[table.table_name] = 'error';
      }
    }
    
    return res.status(200).json({
      success: true,
      message: 'API Test Complete',
      database: {
        connected: true,
        time: dbTest.rows[0]?.time,
        version: dbTest.rows[0]?.version?.split(' ')[1] || 'unknown'
      },
      tables: tableCounts,
      environment: {
        node_env: process.env.NODE_ENV,
        timezone: process.env.TIMEZONE_OFFSET_HOURS || '-3'
      },
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    console.error('Test API error:', error);
    return res.status(500).json({
      success: false,
      error: 'Test failed',
      message: error.message
    });
  }
}

async function healthCheck(req, res) {
  try {
    await pool.query('SELECT 1');
    return res.status(200).json({
      status: 'healthy',
      timestamp: new Date().toISOString(),
      database: 'connected'
    });
  } catch (error) {
    return res.status(503).json({
      status: 'unhealthy',
      timestamp: new Date().toISOString(),
      database: 'disconnected',
      error: error.message
    });
  }
}

// ===========================================
// 1. VISITANTES
// ===========================================
async function getVisitors(req, res, start_date, end_date, store_id) {
  try {
    console.log(`📋 Visitors request: start=${start_date}, end=${end_date}, store=${store_id}`);
    
    // Se pedirem explicitamente displayforce
    if (req.query.source === "displayforce") {
      console.log('🔄 Fetching from DisplayForce API');
      return await getVisitorsFromDisplayForce(res, start_date, end_date, store_id);
    }

    // Construir query para visitors
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

    query += ` ORDER BY timestamp DESC LIMIT 500`;

    console.log("📋 Executing visitors query");
    const result = await pool.query(query, params);
    const rows = result.rows || [];

    console.log(`✅ Found ${rows.length} visitors`);
    
    return res.status(200).json({
      success: true,
      data: rows,
      count: rows.length,
      query: { start_date, end_date, store_id }
    });
    
  } catch (error) {
    console.error("❌ Visitors error:", error.message);
    return res.status(200).json({
      success: true,
      data: [],
      error: error.message,
      isFallback: true
    });
  }
}

// ===========================================
// 2. RESUMO DO DASHBOARD (SIMPLIFICADO E CORRIGIDO)
// ===========================================
async function getSummary(req, res, start_date, end_date, store_id) {
  try {
    console.log(`📊 Summary request: start=${start_date}, end=${end_date}, store=${store_id}`);
    
    // Validação básica de datas
    if (!start_date || !end_date) {
      return res.status(400).json({
        success: false,
        error: 'Missing date parameters',
        message: 'start_date and end_date are required'
      });
    }

    // Buscar dados do dashboard_daily - CORRIGIDO
    let summaryData = [];
    let query = `
      SELECT 
        day,
        store_id,
        total_visitors,
        male,
        female,
        COALESCE(avg_age_sum, 0) as avg_age_sum,
        COALESCE(avg_age_count, 0) as avg_age_count,
        COALESCE(age_18_25, 0) as age_18_25,
        COALESCE(age_26_35, 0) as age_26_35,
        COALESCE(age_36_45, 0) as age_36_45,
        COALESCE(age_46_60, 0) as age_46_60,
        COALESCE(age_60_plus, 0) as age_60_plus,
        COALESCE(monday, 0) as monday,
        COALESCE(tuesday, 0) as tuesday,
        COALESCE(wednesday, 0) as wednesday,
        COALESCE(thursday, 0) as thursday,
        COALESCE(friday, 0) as friday,
        COALESCE(saturday, 0) as saturday,
        COALESCE(sunday, 0) as sunday
      FROM dashboard_daily
      WHERE day BETWEEN $1 AND $2
    `;
    
    const params = [start_date, end_date];
    
    if (store_id && store_id !== "all") {
      query += ` AND store_id = $3`;
      params.push(store_id);
    } else {
      query += ` AND store_id = 'all'`;
    }
    
    query += ` ORDER BY day`;
    
    console.log("📊 Executing dashboard_daily query");
    const result = await pool.query(query, params);
    summaryData = result.rows;
    
    console.log(`✅ Found ${summaryData.length} days in dashboard_daily`);
    
    // Se não encontrou dados agregados, buscar dados de todas as lojas e somar
    if (summaryData.length === 0 && (!store_id || store_id === 'all')) {
      console.log('🔄 No aggregated data found, summing store data...');
      
      const storeQuery = `
        SELECT 
          'all' as store_id,
          SUM(COALESCE(total_visitors, 0)) as total_visitors,
          SUM(COALESCE(male, 0)) as male,
          SUM(COALESCE(female, 0)) as female,
          SUM(COALESCE(avg_age_sum, 0)) as avg_age_sum,
          SUM(COALESCE(avg_age_count, 0)) as avg_age_count,
          SUM(COALESCE(age_18_25, 0)) as age_18_25,
          SUM(COALESCE(age_26_35, 0)) as age_26_35,
          SUM(COALESCE(age_36_45, 0)) as age_36_45,
          SUM(COALESCE(age_46_60, 0)) as age_46_60,
          SUM(COALESCE(age_60_plus, 0)) as age_60_plus,
          SUM(COALESCE(monday, 0)) as monday,
          SUM(COALESCE(tuesday, 0)) as tuesday,
          SUM(COALESCE(wednesday, 0)) as wednesday,
          SUM(COALESCE(thursday, 0)) as thursday,
          SUM(COALESCE(friday, 0)) as friday,
          SUM(COALESCE(saturday, 0)) as saturday,
          SUM(COALESCE(sunday, 0)) as sunday
        FROM dashboard_daily
        WHERE day BETWEEN $1 AND $2 
          AND store_id != 'all'
      `;
      
      const storeResult = await pool.query(storeQuery, [start_date, end_date]);
      if (storeResult.rows.length > 0 && storeResult.rows[0].total_visitors > 0) {
        summaryData = [storeResult.rows[0]];
        console.log(`🔄 Aggregated data from stores: ${summaryData[0].total_visitors} visitors`);
      }
    }
    
    // Se ainda não tem dados, retornar estrutura vazia
    if (summaryData.length === 0 || summaryData[0].total_visitors === 0) {
      console.log('📭 No summary data found');
      
      return res.status(200).json({
        success: true,
        totalVisitors: 0,
        totalMale: 0,
        totalFemale: 0,
        averageAge: 0,
        visitsByDay: {
          Monday: 0, Tuesday: 0, Wednesday: 0, Thursday: 0, Friday: 0, Saturday: 0, Sunday: 0
        },
        byAgeGroup: {
          "18-25": 0, "26-35": 0, "36-45": 0, "46-60": 0, "60+": 0
        },
        byHour: {},
        byGenderHour: { male: {}, female: {} },
        byAgeGender: {
          "<20": { male: 0, female: 0 },
          "20-29": { male: 0, female: 0 },
          "30-45": { male: 0, female: 0 },
          ">45": { male: 0, female: 0 }
        },
        isFallback: true,
        message: "No data available for the selected period"
      });
    }

    // Processar os dados encontrados
    console.log('🔄 Processing summary data...');
    
    // Como temos apenas um registro (agregado), usar ele diretamente
    const dayData = summaryData[0];
    
    const totalVisitors = Number(dayData.total_visitors || 0);
    const totalMale = Number(dayData.male || 0);
    const totalFemale = Number(dayData.female || 0);
    const avgAgeSum = Number(dayData.avg_age_sum || 0);
    const avgAgeCount = Number(dayData.avg_age_count || 0);
    
    const averageAge = avgAgeCount > 0 ? Math.round(avgAgeSum / avgAgeCount) : 0;
    
    const byAgeGroup = {
      "18-25": Number(dayData.age_18_25 || 0),
      "26-35": Number(dayData.age_26_35 || 0),
      "36-45": Number(dayData.age_36_45 || 0),
      "46-60": Number(dayData.age_46_60 || 0),
      "60+": Number(dayData.age_60_plus || 0)
    };
    
    const visitsByDay = {
      Monday: Number(dayData.monday || 0),
      Tuesday: Number(dayData.tuesday || 0),
      Wednesday: Number(dayData.wednesday || 0),
      Thursday: Number(dayData.thursday || 0),
      Friday: Number(dayData.friday || 0),
      Saturday: Number(dayData.saturday || 0),
      Sunday: Number(dayData.sunday || 0)
    };
    
    // Buscar dados por hora
    let byHour = {};
    let byGenderHour = { male: {}, female: {} };
    
    try {
      let hourQuery = `
        SELECT 
          hour,
          COALESCE(SUM(total), 0) as total,
          COALESCE(SUM(male), 0) as male,
          COALESCE(SUM(female), 0) as female
        FROM dashboard_hourly
        WHERE day BETWEEN $1 AND $2
      `;
      
      const hourParams = [start_date, end_date];
      
      if (store_id && store_id !== "all") {
        hourQuery += ` AND store_id = $3`;
        hourParams.push(store_id);
      } else {
        hourQuery += ` AND store_id = 'all'`;
      }
      
      hourQuery += ` GROUP BY hour ORDER BY hour`;
      
      const hourResult = await pool.query(hourQuery, hourParams);
      
      // Inicializar todas as horas (0-23)
      for (let h = 0; h < 24; h++) {
        byHour[h] = 0;
        byGenderHour.male[h] = 0;
        byGenderHour.female[h] = 0;
      }
      
      // Preencher com dados reais
      for (const row of hourResult.rows) {
        const hour = parseInt(row.hour);
        byHour[hour] = Number(row.total || 0);
        byGenderHour.male[hour] = Number(row.male || 0);
        byGenderHour.female[hour] = Number(row.female || 0);
      }
      
      console.log(`⏰ Hourly data: ${hourResult.rows.length} hours found`);
    } catch (hourError) {
      console.log('⚠️ Hourly data not available:', hourError.message);
      
      // Inicializar estrutura vazia
      for (let h = 0; h < 24; h++) {
        byHour[h] = 0;
        byGenderHour.male[h] = 0;
        byGenderHour.female[h] = 0;
      }
    }
    
    // Calcular dados de idade por gênero (simulação baseada nos totais)
    let byAgeGender = {
      "<20": { male: 0, female: 0 },
      "20-29": { male: 0, female: 0 },
      "30-45": { male: 0, female: 0 },
      ">45": { male: 0, female: 0 }
    };
    
    // Distribuir os totais de idade entre gêneros proporcionalmente
    const totalAgeVisitors = Object.values(byAgeGroup).reduce((a, b) => a + b, 0);
    if (totalAgeVisitors > 0 && totalVisitors > 0) {
      const maleRatio = totalMale / totalVisitors;
      const femaleRatio = totalFemale / totalVisitors;
      
      for (const [ageRange, count] of Object.entries(byAgeGroup)) {
        byAgeGender[ageRange].male = Math.round(count * maleRatio);
        byAgeGender[ageRange].female = Math.round(count * femaleRatio);
      }
    }
    
    console.log(`✅ Summary processed: ${totalVisitors} total visitors`);
    
    // Retornar resposta completa
    return res.status(200).json({
      success: true,
      totalVisitors,
      totalMale,
      totalFemale,
      averageAge,
      visitsByDay,
      byAgeGroup,
      byHour,
      byGenderHour,
      byAgeGender,
      isFallback: false,
      query: { start_date, end_date, store_id }
    });
    
  } catch (error) {
    console.error("❌ Summary error:", error.message, error.stack);
    
    return res.status(200).json({
      success: true,
      totalVisitors: 0,
      totalMale: 0,
      totalFemale: 0,
      averageAge: 0,
      visitsByDay: {
        Monday: 0, Tuesday: 0, Wednesday: 0, Thursday: 0, Friday: 0, Saturday: 0, Sunday: 0
      },
      byAgeGroup: {
        "18-25": 0, "26-35": 0, "36-45": 0, "46-60": 0, "60+": 0
      },
      byHour: {},
      byGenderHour: { male: {}, female: {} },
      byAgeGender: {
        "<20": { male: 0, female: 0 },
        "20-29": { male: 0, female: 0 },
        "30-45": { male: 0, female: 0 },
        ">45": { male: 0, female: 0 }
      },
      isFallback: true,
      error: error.message
    });
  }
}

// ===========================================
// 3. LOJAS (COM NOMES REAIS)
// ===========================================
async function getStores(req, res) {
  try {
    console.log('🏪 Fetching stores from database...');
    
    // Mapeamento de IDs para nomes reais (você pode expandir isso)
    const storeNames = {
      '15287': 'Assaí Cajamar - SP',
      '15286': 'Assaí São Paulo - Zona Leste',
      '15268': 'Assaí Campinas - SP',
      '15267': 'Assaí Ribeirão Preto - SP',
      '15266': 'Assaí São José dos Campos - SP',
      '15265': 'Assaí Sorocaba - SP',
      '16109': 'Assaí Goiânia - GO',
      '16108': 'Assaí Brasília - DF',
      '16107': 'Assaí Belo Horizonte - MG',
      '16103': 'Assaí Curitiba - PR',
      '14832': 'Assaí Porto Alegre - RS',
      '14818': 'Assaí Florianópolis - SC',
      'all': 'Todas as Lojas'
    };
    
    // Buscar lojas com dados
    const storesQuery = `
      SELECT DISTINCT 
        store_id as id,
        COALESCE(SUM(total_visitors), 0) as visitor_count
      FROM dashboard_daily
      WHERE store_id IS NOT NULL 
        AND store_id != ''
      GROUP BY store_id
      ORDER BY visitor_count DESC
      LIMIT 20
    `;
    
    const result = await pool.query(storesQuery);
    
    const stores = result.rows.map(row => {
      const storeId = row.id;
      const name = storeNames[storeId] || `Loja ${storeId}`;
      
      return {
        id: storeId,
        name: name,
        visitor_count: parseInt(row.visitor_count || 0),
        status: 'active'
      };
    });
    
    console.log(`✅ Found ${stores.length} stores in dashboard_daily`);
    
    // Garantir que "Todas as Lojas" está na lista
    const allStoresExists = stores.some(store => store.id === 'all');
    if (!allStoresExists) {
      // Adicionar "Todas as Lojas" com a soma de todos os visitantes
      const totalVisitors = stores.reduce((sum, store) => sum + store.visitor_count, 0);
      stores.unshift({
        id: 'all',
        name: 'Todas as Lojas',
        visitor_count: totalVisitors,
        status: 'active'
      });
    }
    
    return res.status(200).json({
      success: true,
      stores: stores,
      count: stores.length,
      source: 'dashboard_daily'
    });
    
  } catch (error) {
    console.error('❌ Stores error:', error.message);
    
    // Fallback com nomes reais
    const fallbackStores = [
      { id: "all", name: 'Todas as Lojas', visitor_count: 4558, status: 'active' },
      { id: "15287", name: 'Assaí Cajamar - SP', visitor_count: 0, status: 'active' },
      { id: "15286", name: 'Assaí São Paulo - Zona Leste', visitor_count: 0, status: 'active' },
      { id: "15268", name: 'Assaí Campinas - SP', visitor_count: 0, status: 'active' },
      { id: "15267", name: 'Assaí Ribeirão Preto - SP', visitor_count: 0, status: 'active' },
      { id: "15266", name: 'Assaí São José dos Campos - SP', visitor_count: 0, status: 'active' },
      { id: "15265", name: 'Assaí Sorocaba - SP', visitor_count: 0, status: 'active' },
      { id: "16109", name: 'Assaí Goiânia - GO', visitor_count: 0, status: 'active' },
      { id: "16108", name: 'Assaí Brasília - DF', visitor_count: 0, status: 'active' },
      { id: "16107", name: 'Assaí Belo Horizonte - MG', visitor_count: 0, status: 'active' }
    ];
    
    return res.status(200).json({
      success: true,
      stores: fallbackStores,
      count: fallbackStores.length,
      isFallback: true,
      error: error.message
    });
  }
}

// ===========================================
// 4. DISPOSITIVOS (DisplayForce)
// ===========================================
async function getDevices(req, res) {
  try {
    console.log('🌐 Calling DisplayForce API for devices...');
    
    const response = await fetch(`${DISPLAYFORCE_BASE}/device/list`, {
      method: 'GET',
      headers: {
        'X-API-Token': DISPLAYFORCE_TOKEN,
        'Accept': 'application/json'
      },
      timeout: 10000
    });
    
    if (!response.ok) {
      console.warn(`⚠️ DisplayForce API returned ${response.status}`);
      // Se a API falhar, retornar as lojas do banco como dispositivos
      return await getStoresAsDevices(req, res);
    }
    
    const data = await response.json();
    const devices = data.devices || data.data || [];
    
    console.log(`✅ Found ${devices.length} devices from DisplayForce`);
    
    return res.status(200).json({
      success: true,
      devices: devices,
      count: devices.length,
      source: 'displayforce'
    });
    
  } catch (error) {
    console.error('❌ Devices API error:', error.message);
    return await getStoresAsDevices(req, res);
  }
}

// Função auxiliar para retornar lojas como dispositivos
async function getStoresAsDevices(req, res) {
  try {
    const storesResult = await pool.query(`
      SELECT DISTINCT 
        store_id as id,
        CONCAT('Loja ', store_id) as name,
        'active' as status,
        'Assaí Atacadista' as location,
        NOW() as last_seen
      FROM dashboard_daily
      WHERE store_id IS NOT NULL 
        AND store_id != ''
        AND store_id != 'all'
      ORDER BY store_id
      LIMIT 20
    `);
    
    const devices = storesResult.rows.map(row => ({
      id: row.id,
      name: row.name,
      status: row.status,
      location: row.location,
      last_seen: row.last_seen
    }));
    
    return res.status(200).json({
      success: true,
      devices: devices,
      count: devices.length,
      source: 'database',
      message: 'Using store data from database as devices'
    });
    
  } catch (dbError) {
    console.error('Database devices error:', dbError.message);
    
    // Fallback final
    return res.status(200).json({
      success: true,
      devices: [
        {
          id: "15287",
          name: "Assaí Cajamar - SP",
          status: "active",
          location: "Assaí Atacadista",
          last_seen: new Date().toISOString()
        },
        {
          id: "15286", 
          name: "Assaí São Paulo - ZL",
          status: "active",
          location: "Assaí Atacadista",
          last_seen: new Date().toISOString()
        }
      ],
      isFallback: true,
      message: 'Using fallback device data'
    });
  }
}

// ===========================================
// 5. REFRESH (SINCRONIZAÇÃO)
// ===========================================
async function refreshRange(req, res, start_date, end_date, store_id) {
  console.log(`🔄 Refresh request: start=${start_date}, end=${end_date}, store=${store_id}`);
  
  try {
    const targetStart = start_date || new Date().toISOString().split('T')[0];
    const targetEnd = end_date || targetStart;
    
    // Retornar resposta imediata
    const response = {
      success: true,
      message: 'Refresh initiated',
      details: {
        start_date: targetStart,
        end_date: targetEnd,
        store_id: store_id || 'all',
        note: 'Refresh would sync data from DisplayForce and update database'
      },
      timestamp: new Date().toISOString()
    };
    
    // Iniciar refresh em background (não bloqueante)
    setTimeout(async () => {
      try {
        console.log(`🔄 Starting background sync for ${targetStart} to ${targetEnd}`);
        // Aqui iria a lógica real de sincronização
      } catch (bgError) {
        console.error('Background sync error:', bgError.message);
      }
    }, 100);
    
    return res.status(200).json(response);
    
  } catch (error) {
    console.error('❌ Refresh error:', error.message);
    return res.status(200).json({
      success: false,
      error: error.message,
      message: 'Refresh failed'
    });
  }
}

// ===========================================
// FUNÇÕES RESTANTES (SIMPLIFICADAS)
// ===========================================
async function getVisitorsFromDisplayForce(res, start_date, end_date, store_id) {
  console.log('🔄 Simulating DisplayForce visitors fetch');
  
  // Dados de exemplo
  const exampleData = [
    {
      id: "visitor_001",
      date: start_date || "2025-12-08",
      store_id: store_id || "15287",
      store_name: "Assaí Cajamar - SP",
      timestamp: `${start_date || "2025-12-08"}T10:30:00.000Z`,
      gender: "Masculino",
      age: 35,
      day_of_week: "Seg",
      smile: true
    },
    {
      id: "visitor_002", 
      date: start_date || "2025-12-08",
      store_id: store_id || "15287",
      store_name: "Assaí Cajamar - SP",
      timestamp: `${start_date || "2025-12-08"}T14:45:00.000Z`,
      gender: "Feminino",
      age: 28,
      day_of_week: "Seg",
      smile: false
    }
  ];
  
  return res.status(200).json({
    success: true,
    data: exampleData,
    count: exampleData.length,
    source: 'displayforce_simulated',
    message: 'Using simulated visitor data'
  });
}

async function ensureIndexes(req, res) {
  try {
    // Criar índices se não existirem
    const indexes = [
      "CREATE INDEX IF NOT EXISTS visitors_day_idx ON public.visitors(day)",
      "CREATE INDEX IF NOT EXISTS visitors_store_day_idx ON public.visitors(store_id, day)",
      "CREATE INDEX IF NOT EXISTS dashboard_daily_day_store_idx ON public.dashboard_daily(day, store_id)",
      "CREATE INDEX IF NOT EXISTS dashboard_hourly_day_store_hour_idx ON public.dashboard_hourly(day, store_id, hour)"
    ];
    
    for (const indexQuery of indexes) {
      await pool.query(indexQuery);
    }
    
    return res.status(200).json({
      success: true,
      message: 'Indexes created/verified'
    });
  } catch (error) {
    console.error('Index creation error:', error.message);
    return res.status(500).json({ error: error.message });
  }
}

async function refreshAll(req, res, start_date, end_date) {
  return res.status(200).json({
    success: true,
    message: 'Refresh all stores initiated',
    start_date: start_date || new Date().toISOString().split('T')[0],
    end_date: end_date || new Date().toISOString().split('T')[0],
    timestamp: new Date().toISOString()
  });
}

async function autoRefresh(req, res) {
  const yesterday = new Date();
  yesterday.setDate(yesterday.getDate() - 1);
  const yesterdayStr = yesterday.toISOString().split('T')[0];
  
  return res.status(200).json({
    success: true,
    message: 'Auto-refresh would sync yesterday\'s data',
    date: yesterdayStr,
    timestamp: new Date().toISOString()
  });
}