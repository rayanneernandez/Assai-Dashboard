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
    // Se pedirem explicitamente displayforce, busca direto da API
    if (req.query.source === "displayforce") {
      console.log("📥 Buscando visitantes diretamente da DisplayForce...");
      return await getVisitorsFromDisplayForce(res, start_date, end_date, store_id);
    }

    // Busca do banco de dados local
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

    console.log("📋 Visitors query:", query, params);

    const result = await pool.query(query, params);

    return res.status(200).json({
      success: true,
      data: result.rows.map(row => ({
        id: row.visitor_id,
        date: row.day,
        store_id: row.store_id,
        store_name: row.store_name,
        timestamp: row.timestamp,
        gender: row.gender === 'M' ? 'Masculino' : 'Feminino',
        age: row.age,
        day_of_week: row.day_of_week,
        smile: row.smile,
        hour: row.hour
      })),
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
  try {
    const tz = parseInt(process.env.TIMEZONE_OFFSET_HOURS || "-3", 10);
    const sign = tz >= 0 ? "+" : "-";
    const hh = String(Math.abs(tz)).padStart(2, "0");
    const tzStr = `${sign}${hh}:00`;
    const startISO = `${start_date || new Date().toISOString().split('T')[0]}T00:00:00${tzStr}`;
    const endISO = `${end_date || new Date().toISOString().split('T')[0]}T23:59:59${tzStr}`;
    
    const LIMIT_REQ = 100; // A API retorna 100 por página
    let offset = 0;
    const allVisitors = [];
    let totalPages = 1;
    let currentPage = 0;
    
    console.log(`📥 Buscando visitantes da DisplayForce: ${startISO} até ${endISO}`);
    
    // Busca todas as páginas
    while (true) {
      const bodyPayload = {
        start: startISO,
        end: endISO,
        limit: LIMIT_REQ,
        offset: offset,
        tracks: true
      };
      
      if (store_id && store_id !== 'all') {
        bodyPayload.devices = [parseInt(store_id)];
      }
      
      console.log(`📄 Buscando página ${currentPage + 1}, offset: ${offset}`);
      
      const response = await fetch(`${DISPLAYFORCE_BASE}/stats/visitor/list`, {
        method: 'POST',
        headers: { 
          'X-API-Token': DISPLAYFORCE_TOKEN, 
          'Content-Type': 'application/json' 
        },
        body: JSON.stringify(bodyPayload)
      });
      
      if (!response.ok) {
        const body = await response.text().catch(() => '');
        throw new Error(`DisplayForce API: ${response.status} ${response.statusText} - ${body}`);
      }
      
      const data = await response.json();
      
      // Verifica a estrutura da resposta
      console.log("📊 Resposta da API:", {
        hasPayload: !!data.payload,
        payloadLength: data.payload ? data.payload.length : 0,
        pagination: data.pagination
      });
      
      const visitors = data.payload || [];
      allVisitors.push(...visitors);
      
      const pagination = data.pagination;
      if (pagination) {
        totalPages = Math.ceil(pagination.total / LIMIT_REQ);
        console.log(`📊 Total: ${pagination.total}, Página ${currentPage + 1}/${totalPages}`);
      }
      
      // Se não há mais dados ou atingimos o total
      if (!visitors.length || visitors.length < LIMIT_REQ) {
        break;
      }
      
      offset += LIMIT_REQ;
      currentPage++;
      
      // Limita para evitar muitas requisições
      if (currentPage >= 10) { // Máximo 1000 visitantes
        console.log("⚠️ Limite de páginas atingido (10 páginas)");
        break;
      }
    }
    
    console.log(`✅ Total de visitantes encontrados: ${allVisitors.length}`);
    
    // Processa os visitantes
    const DAYS = ['Dom','Seg','Ter','Qua','Qui','Sex','Sáb'];
    const processedVisitors = [];
    
    for (const visitor of allVisitors) {
      try {
        const ts = visitor.start || visitor.tracks?.[0]?.start || new Date().toISOString();
        const d = new Date(ts);
        const isoUTC = d.toISOString();
        
        // Ajusta para o fuso horário local
        const local = new Date(d.getTime() + tz * 3600000);
        const di = local.getDay();
        const day_of_week = DAYS[di];
        const hour = local.getHours();
        const dateStr = local.toISOString().split('T')[0];
        
        // Extrai atributos adicionais
        const attrs = visitor.additional_atributes || [];
        const lastAttr = attrs.length > 0 ? attrs[attrs.length - 1] : {};
        
        const smile = String(lastAttr?.smile || '').toLowerCase() === 'yes';
        const age = Number(visitor.age || 0);
        
        // Pega o device_id
        let deviceId = 'unknown';
        if (visitor.tracks && visitor.tracks.length > 0) {
          deviceId = String(visitor.tracks[0].device_id || '');
        } else if (visitor.devices && visitor.devices.length > 0) {
          deviceId = String(visitor.devices[0] || '');
        }
        
        const gender = visitor.sex === 1 ? 'M' : visitor.sex === 2 ? 'F' : 'U';
        
        const processedVisitor = {
          id: visitor.visitor_id || visitor.session_id || `temp_${Date.now()}`,
          date: dateStr,
          store_id: deviceId,
          store_name: `Loja ${deviceId}`,
          timestamp: isoUTC,
          gender: gender === 'M' ? 'Masculino' : gender === 'F' ? 'Feminino' : 'Desconhecido',
          age: age,
          day_of_week: day_of_week,
          smile: smile,
          hour: hour
        };
        
        processedVisitors.push(processedVisitor);
        
        // Insere no banco de dados
        try {
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
              processedVisitor.id,
              processedVisitor.date,
              processedVisitor.store_id,
              processedVisitor.store_name,
              processedVisitor.timestamp,
              gender, // Salva como M/F/U no banco
              processedVisitor.age,
              processedVisitor.day_of_week,
              processedVisitor.smile,
              processedVisitor.hour
            ]
          );
        } catch (dbError) {
          console.error('❌ Erro ao inserir no banco:', dbError.message);
        }
        
      } catch (visitorError) {
        console.error('❌ Erro ao processar visitante:', visitorError.message);
      }
    }
    
    // Após processar todos os visitantes, atualiza as estatísticas agregadas
    if (processedVisitors.length > 0) {
      try {
        await updateDashboardAggregates(start_date || dateStr, end_date || dateStr, store_id);
      } catch (aggError) {
        console.error('❌ Erro ao atualizar agregados:', aggError.message);
      }
    }
    
    return res.status(200).json({ 
      success: true, 
      data: processedVisitors, 
      count: processedVisitors.length, 
      source: 'displayforce',
      query: { start_date, end_date, store_id } 
    });
    
  } catch (error) {
    console.error('❌ Erro ao buscar visitantes da DisplayForce:', error);
    return res.status(500).json({
      success: false,
      error: error.message,
      message: 'Erro ao buscar dados da DisplayForce'
    });
  }
}

// ===========================================
// 2. RESUMO DO DASHBOARD
// ===========================================
async function getSummary(req, res, start_date, end_date, store_id) {
  try {
    // Primeiro tenta buscar dos dados agregados
    const summary = await getSummaryFromAggregates(start_date, end_date, store_id);
    
    // Se encontrou dados nos agregados, retorna
    if (summary.totalVisitors > 0) {
      return res.status(200).json({
        ...summary,
        success: true,
        source: 'dashboard_aggregates'
      });
    }
    
    // Se não encontrou dados nos agregados, busca direto dos visitantes
    console.log("📊 Nenhum dado encontrado nos agregados, buscando de visitors...");
    return await getSummaryFromVisitors(res, start_date, end_date, store_id);
    
  } catch (error) {
    console.error("❌ Summary error:", error);
    return await getSummaryFromVisitors(res, start_date, end_date, store_id);
  }
}

async function getSummaryFromAggregates(start_date, end_date, store_id) {
  try {
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

    console.log("📊 Query agregados:", query, params);
    
    const result = await pool.query(query, params);
    const row = result.rows[0] || {};
    
    // Se não tem dados, retorna zeros
    if (Number(row.total_visitors || 0) === 0) {
      return createEmptySummary();
    }
    
    // Calcula idade média
    const avgAgeCount = Number(row.avg_age_count || 0);
    const averageAge = avgAgeCount > 0 ? Math.round(Number(row.avg_age_sum || 0) / avgAgeCount) : 0;
    
    // Busca dados por hora
    const hourlyData = await getHourlyDataFromAggregates(start_date, end_date, store_id);
    
    // Busca distribuição por idade e gênero
    const ageGenderData = await getAgeGenderData(start_date, end_date, store_id);
    
    return {
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
      source: 'dashboard_aggregates'
    };
    
  } catch (error) {
    console.error("❌ Error getting summary from aggregates:", error);
    return createEmptySummary();
  }
}

async function getSummaryFromVisitors(res, start_date, end_date, store_id) {
  try {
    console.log("📊 Calculando resumo a partir da tabela visitors...");
    
    // Primeiro, busca todos os visitantes do período
    let query = `
      SELECT 
        gender,
        age,
        day_of_week,
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

    console.log("📊 Query visitors para resumo:", query, params);
    
    const result = await pool.query(query, params);
    const visitors = result.rows || [];
    
    console.log(`📊 Total de visitantes encontrados: ${visitors.length}`);
    
    if (visitors.length === 0) {
      console.log("📊 Nenhum visitante encontrado, retornando dados vazios");
      return res.status(200).json({
        ...createEmptySummary(),
        success: true,
        source: 'visitors_table_empty'
      });
    }
    
    // Calcula estatísticas
    let totalVisitors = visitors.length;
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
      Sunday: 0,
      Monday: 0,
      Tuesday: 0,
      Wednesday: 0,
      Thursday: 0,
      Friday: 0,
      Saturday: 0
    };
    
    const dayMap = {
      'Dom': 'Sunday',
      'Seg': 'Monday',
      'Ter': 'Tuesday',
      'Qua': 'Wednesday',
      'Qui': 'Thursday',
      'Sex': 'Friday',
      'Sáb': 'Saturday'
    };
    
    const byHour = {};
    const byGenderHour = { male: {}, female: {} };
    
    // Inicializa horas
    for (let h = 0; h < 24; h++) {
      byHour[h] = 0;
      byGenderHour.male[h] = 0;
      byGenderHour.female[h] = 0;
    }
    
    // Processa cada visitante
    for (const visitor of visitors) {
      // Gênero
      if (visitor.gender === 'M') {
        totalMale++;
      } else if (visitor.gender === 'F') {
        totalFemale++;
      }
      
      // Idade
      const age = Number(visitor.age || 0);
      if (age > 0) {
        ageSum += age;
        ageCount++;
        
        if (age >= 18 && age <= 25) byAgeGroup["18-25"]++;
        else if (age >= 26 && age <= 35) byAgeGroup["26-35"]++;
        else if (age >= 36 && age <= 45) byAgeGroup["36-45"]++;
        else if (age >= 46 && age <= 60) byAgeGroup["46-60"]++;
        else if (age > 60) byAgeGroup["60+"]++;
      }
      
      // Dia da semana
      const dayPt = visitor.day_of_week;
      if (dayPt && dayMap[dayPt]) {
        visitsByDay[dayMap[dayPt]]++;
      }
      
      // Hora
      const hour = Number(visitor.hour);
      if (hour >= 0 && hour < 24) {
        byHour[hour]++;
        if (visitor.gender === 'M') {
          byGenderHour.male[hour]++;
        } else if (visitor.gender === 'F') {
          byGenderHour.female[hour]++;
        }
      }
    }
    
    // Calcula idade média
    const averageAge = ageCount > 0 ? Math.round(ageSum / ageCount) : 0;
    
    // Calcula distribuição por idade e gênero
    const byAgeGender = await getAgeGenderData(start_date, end_date, store_id);
    
    const summary = {
      success: true,
      totalVisitors: totalVisitors,
      totalMale: totalMale,
      totalFemale: totalFemale,
      averageAge: averageAge,
      visitsByDay: visitsByDay,
      byAgeGroup: byAgeGroup,
      byAgeGender: byAgeGender,
      byHour: byHour,
      byGenderHour: byGenderHour,
      isFallback: true,
      source: 'visitors_table_calculated'
    };
    
    // Atualiza os agregados para próxima vez
    try {
      await updateDashboardAggregates(start_date, end_date, store_id);
    } catch (aggError) {
      console.error('❌ Erro ao atualizar agregados após cálculo:', aggError.message);
    }
    
    return res.status(200).json(summary);
    
  } catch (error) {
    console.error("❌ Error getting summary from visitors:", error);
    return res.status(200).json({
      ...createEmptySummary(),
      success: true,
      source: 'error_fallback'
    });
  }
}

function createEmptySummary() {
  const byHour = {};
  const byGenderHour = { male: {}, female: {} };
  
  for (let h = 0; h < 24; h++) {
    byHour[h] = 0;
    byGenderHour.male[h] = 0;
    byGenderHour.female[h] = 0;
  }
  
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
    byAgeGender: {
      "<20": { male: 0, female: 0 },
      "20-29": { male: 0, female: 0 },
      "30-45": { male: 0, female: 0 },
      ">45": { male: 0, female: 0 }
    },
    byHour: byHour,
    byGenderHour: byGenderHour,
    isFallback: true
  };
}

async function getHourlyDataFromAggregates(start_date, end_date, store_id) {
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
    console.error("❌ Hourly data from aggregates error:", error);
    
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
// 3. ATUALIZAÇÃO DE AGREGADOS
// ===========================================
async function updateDashboardAggregates(start_date, end_date, store_id) {
  try {
    console.log(`🔄 Atualizando agregados para ${start_date} até ${end_date}, loja: ${store_id || 'all'}`);
    
    // Para cada dia no intervalo
    const start = new Date(start_date);
    const end = new Date(end_date);
    const days = [];
    
    for (let d = new Date(start); d <= end; d.setDate(d.getDate() + 1)) {
      days.push(d.toISOString().split('T')[0]);
    }
    
    for (const day of days) {
      await updateDailyAggregatesForDay(day, store_id);
    }
    
    console.log(`✅ Agregados atualizados para ${days.length} dias`);
    
  } catch (error) {
    console.error('❌ Erro ao atualizar agregados:', error);
  }
}

async function updateDailyAggregatesForDay(day, store_id) {
  try {
    // Calcula estatísticas para o dia específico
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
    
    if (store_id && store_id !== 'all') {
      query += ` AND store_id = $2`;
      params.push(store_id);
    }

    const result = await pool.query(query, params);
    const stats = result.rows[0] || {};
    
    // Determina o store_id para salvar
    const saveStoreId = store_id && store_id !== 'all' ? store_id : 'all';
    
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
      saveStoreId,
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
    await updateHourlyAggregatesForDay(day, store_id);
    
    console.log(`✅ Agregados atualizados para ${day}, loja: ${saveStoreId}`);
    
  } catch (error) {
    console.error(`❌ Erro ao atualizar agregados para ${day}:`, error);
  }
}

async function updateHourlyAggregatesForDay(day, store_id) {
  try {
    // Primeiro, deleta dados existentes para este dia/loja
    const deleteQuery = `DELETE FROM dashboard_hourly WHERE day = $1 AND store_id = $2`;
    const saveStoreId = store_id && store_id !== 'all' ? store_id : 'all';
    await pool.query(deleteQuery, [day, saveStoreId]);
    
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
    
    if (store_id && store_id !== 'all') {
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
        saveStoreId,
        Number(row.hour),
        Number(row.total || 0),
        Number(row.male || 0),
        Number(row.female || 0)
      ]);
    }
    
  } catch (error) {
    console.error(`❌ Erro ao atualizar dados por hora para ${day}:`, error);
  }
}

// ===========================================
// 4. LOJAS
// ===========================================
async function getStores(req, res) {
  try {
    // Primeiro tenta buscar dispositivos da DisplayForce
    try {
      const response = await fetch(`${DISPLAYFORCE_BASE}/device/list`, {
        method: 'POST',
        headers: {
          'X-API-Token': DISPLAYFORCE_TOKEN,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({})
      });
      
      if (response.ok) {
        const data = await response.json();
        const devices = data.devices || data.data || [];
        
        const stores = devices.map(device => ({
          id: device.id || device.device_id,
          name: device.name || `Dispositivo ${device.id || device.device_id}`,
          location: device.location || 'Local desconhecido',
          status: device.status || 'active',
          visitor_count: 0
        }));
        
        return res.status(200).json({
          success: true,
          stores: stores,
          count: stores.length,
          source: 'displayforce'
        });
      }
    } catch (error) {
      console.log('⚠️ Não foi possível buscar dispositivos da DisplayForce, usando banco local');
    }
    
    // Fallback para banco local
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
      count: stores.length,
      source: 'database'
    });
    
  } catch (error) {
    console.error('❌ Stores error:', error);
    
    return res.status(200).json({
      success: true,
      stores: [
        { id: 15267, name: 'Loja Principal', visitor_count: 5000, status: 'active' },
        { id: 15268, name: 'Loja Norte', visitor_count: 1500, status: 'active' },
        { id: 15269, name: 'Loja Sul', visitor_count: 966, status: 'active' }
      ],
      isFallback: true
    });
  }
}

// ===========================================
// 5. DISPOSITIVOS (DisplayForce)
// ===========================================
async function getDevices(req, res) {
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
      throw new Error(`DisplayForce: ${response.status} ${response.statusText}`);
    }
    
    const data = await response.json();
    
    const devices = data.devices || data.data || [];
    
    console.log(`✅ ${devices.length} dispositivos encontrados`);
    
    return res.status(200).json({
      success: true,
      devices: devices,
      count: devices.length,
      source: 'displayforce'
    });
    
  } catch (error) {
    console.error('❌ Devices error:', error);
    
    return res.status(200).json({
      success: true,
      devices: [
        {
          id: 15267,
          name: "Sensor Entrada",
          status: "active",
          location: "Loja Principal",
          last_seen: new Date().toISOString()
        },
        {
          id: 15268,
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
// 6. REFRESH - ATUALIZAÇÃO DE DADOS
// ===========================================
async function refreshRange(req, res, start_date, end_date, store_id) {
  try {
    const s = start_date || new Date().toISOString().slice(0, 10);
    const e = end_date || s;
    
    console.log(`🔄 Iniciando refresh: ${s} até ${e}, loja: ${store_id || 'all'}`);
    
    // Busca dados da DisplayForce
    await getVisitorsFromDisplayForceForRefresh(s, e, store_id);
    
    // Atualiza agregados
    await updateDashboardAggregates(s, e, store_id);
    
    return res.status(200).json({
      success: true,
      message: `Dados atualizados com sucesso`,
      period: `${s} até ${e}`,
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

async function getVisitorsFromDisplayForceForRefresh(start_date, end_date, store_id) {
  const tz = parseInt(process.env.TIMEZONE_OFFSET_HOURS || "-3", 10);
  const sign = tz >= 0 ? "+" : "-";
  const hh = String(Math.abs(tz)).padStart(2, "0");
  const tzStr = `${sign}${hh}:00`;
  const startISO = `${start_date}T00:00:00${tzStr}`;
  const endISO = `${end_date}T23:59:59${tzStr}`;
  
  const LIMIT_REQ = 100;
  let offset = 0;
  const allVisitors = [];
  let totalVisitors = 0;
  
  console.log(`📥 Buscando visitantes para refresh: ${startISO} até ${endISO}`);
  
  // Busca todas as páginas
  while (true) {
    const bodyPayload = {
      start: startISO,
      end: endISO,
      limit: LIMIT_REQ,
      offset: offset,
      tracks: true
    };
    
    if (store_id && store_id !== 'all') {
      bodyPayload.devices = [parseInt(store_id)];
    }
    
    console.log(`📄 Buscando página, offset: ${offset}`);
    
    const response = await fetch(`${DISPLAYFORCE_BASE}/stats/visitor/list`, {
      method: 'POST',
      headers: { 
        'X-API-Token': DISPLAYFORCE_TOKEN, 
        'Content-Type': 'application/json' 
      },
      body: JSON.stringify(bodyPayload)
    });
    
    if (!response.ok) {
      const body = await response.text().catch(() => '');
      throw new Error(`DisplayForce API: ${response.status} ${response.statusText} - ${body}`);
    }
    
    const data = await response.json();
    const visitors = data.payload || [];
    
    allVisitors.push(...visitors);
    
    const pagination = data.pagination;
    if (pagination) {
      totalVisitors = pagination.total;
      console.log(`📊 Total na API: ${totalVisitors}, Obtidos: ${allVisitors.length}`);
    }
    
    // Se não há mais dados ou atingimos o total
    if (!visitors.length || visitors.length < LIMIT_REQ || allVisitors.length >= totalVisitors) {
      break;
    }
    
    offset += LIMIT_REQ;
    
    // Limita para evitar muitas requisições
    if (offset >= 1000) { // Máximo 1000 visitantes
      console.log("⚠️ Limite de visitantes atingido (1000)");
      break;
    }
  }
  
  console.log(`✅ ${allVisitors.length} visitantes obtidos da DisplayForce`);
  
  // Processa e salva no banco
  const DAYS = ['Dom','Seg','Ter','Qua','Qui','Sex','Sáb'];
  let savedCount = 0;
  
  for (const visitor of allVisitors) {
    try {
      const ts = visitor.start || visitor.tracks?.[0]?.start || new Date().toISOString();
      const d = new Date(ts);
      const isoUTC = d.toISOString();
      
      // Ajusta para o fuso horário local
      const local = new Date(d.getTime() + tz * 3600000);
      const di = local.getDay();
      const day_of_week = DAYS[di];
      const hour = local.getHours();
      const dateStr = local.toISOString().split('T')[0];
      
      // Extrai atributos adicionais
      const attrs = visitor.additional_atributes || [];
      const lastAttr = attrs.length > 0 ? attrs[attrs.length - 1] : {};
      
      const smile = String(lastAttr?.smile || '').toLowerCase() === 'yes';
      const age = Number(visitor.age || 0);
      
      // Pega o device_id
      let deviceId = 'unknown';
      if (visitor.tracks && visitor.tracks.length > 0) {
        deviceId = String(visitor.tracks[0].device_id || '');
      } else if (visitor.devices && visitor.devices.length > 0) {
        deviceId = String(visitor.devices[0] || '');
      }
      
      const gender = visitor.sex === 1 ? 'M' : visitor.sex === 2 ? 'F' : 'U';
      
      // Insere no banco
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
          visitor.visitor_id || visitor.session_id || `temp_${Date.now()}_${savedCount}`,
          dateStr,
          deviceId,
          `Loja ${deviceId}`,
          isoUTC,
          gender,
          age,
          day_of_week,
          smile,
          hour
        ]
      );
      
      savedCount++;
      
    } catch (visitorError) {
      console.error('❌ Erro ao salvar visitante:', visitorError.message);
    }
  }
  
  console.log(`💾 ${savedCount} visitantes salvos no banco de dados`);
  return savedCount;
}

// ===========================================
// 7. REFRESH ALL - TODAS AS LOJAS
// ===========================================
async function refreshAll(req, res, start_date, end_date) {
  try {
    const s = start_date || new Date().toISOString().slice(0, 10);
    const e = end_date || s;
    
    console.log(`🔄 Refresh All: período ${s} até ${e}`);
    
    // Primeiro, busca todas as lojas/dispositivos
    const devices = await getDeviceIds();
    
    console.log(`📱 ${devices.length} dispositivos encontrados`);
    
    // Atualiza cada dispositivo
    for (const deviceId of devices) {
      try {
        console.log(`🔄 Atualizando dispositivo ${deviceId}...`);
        await getVisitorsFromDisplayForceForRefresh(s, e, deviceId);
        await updateDashboardAggregates(s, e, deviceId);
        console.log(`✅ Dispositivo ${deviceId} atualizado`);
      } catch (deviceError) {
        console.error(`❌ Erro no dispositivo ${deviceId}:`, deviceError.message);
      }
    }
    
    // Atualiza agregado 'all'
    console.log(`🔄 Atualizando agregado 'all'...`);
    await updateDashboardAggregates(s, e, 'all');
    
    return res.status(200).json({
      success: true,
      message: `Refresh All concluído`,
      devices: devices.length,
      period: `${s} até ${e}`,
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

async function getDeviceIds() {
  try {
    const response = await fetch(`${DISPLAYFORCE_BASE}/device/list`, {
      method: 'POST',
      headers: {
        'X-API-Token': DISPLAYFORCE_TOKEN,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({})
    });
    
    if (!response.ok) {
      throw new Error(`DisplayForce API: ${response.status}`);
    }
    
    const data = await response.json();
    const devices = data.devices || data.data || [];
    
    return devices
      .map(device => String(device.id || device.device_id || ''))
      .filter(id => id && id !== 'all' && !isNaN(parseInt(id)));
      
  } catch (error) {
    console.error('❌ Erro ao buscar dispositivos:', error);
    
    // Fallback
    return ['15267', '15268', '15269'];
  }
}

// ===========================================
// 8. AUTO REFRESH
// ===========================================
async function autoRefresh(req, res) {
  try {
    // Data de ontem
    const yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);
    const dateStr = yesterday.toISOString().slice(0, 10);
    
    console.log(`🤖 Auto-refresh para ${dateStr}`);
    
    // Busca todos os dispositivos
    const devices = await getDeviceIds();
    
    // Para cada dispositivo, atualiza dados de ontem
    for (const deviceId of devices) {
      try {
        await getVisitorsFromDisplayForceForRefresh(dateStr, dateStr, deviceId);
        await updateDashboardAggregates(dateStr, dateStr, deviceId);
        console.log(`✅ Auto-refresh para dispositivo ${deviceId} concluído`);
      } catch (deviceError) {
        console.error(`❌ Erro no auto-refresh para ${deviceId}:`, deviceError.message);
      }
    }
    
    // Atualiza agregado 'all'
    await updateDashboardAggregates(dateStr, dateStr, 'all');
    
    return res.status(200).json({
      success: true,
      message: `Auto-refresh executado para ${dateStr}`,
      date: dateStr,
      devices: devices.length,
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
// 9. OTIMIZAÇÃO DE ÍNDICES
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
      CREATE INDEX IF NOT EXISTS idx_visitors_timestamp ON visitors(timestamp DESC);
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
    
    console.log('✅ Índices criados/verificados com sucesso');
    
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