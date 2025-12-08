import { createClient } from '@supabase/supabase-js';
import cron from 'node-cron';

const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_SERVICE_KEY; // Use service role key
const supabase = createClient(supabaseUrl, supabaseKey);

const DISPLAYFORCE_API_KEY = process.env.DISPLAYFORCE_API_KEY;

// Agendar atualização a cada hora
cron.schedule('0 * * * *', async () => {
  console.log('Iniciando atualização dos dados...');
  await updateAllData();
});

async function updateAllData() {
  try {
    // 1. Atualizar dados das lojas
    await updateStoresData();
    
    // 2. Atualizar métricas de visitantes
    await updateVisitorMetrics();
    
    // 3. Atualizar dados demográficos
    await updateDemographics();
    
    // 4. Atualizar dados semanais
    await updateWeeklyData();
    
    console.log('Atualização concluída com sucesso!');
  } catch (error) {
    console.error('Erro na atualização:', error);
  }
}

async function updateStoresData() {
  try {
    const response = await fetch('https://api.displayforce.ai/public/v1/device/list', {
      headers: {
        'Authorization': `Bearer ${DISPLAYFORCE_API_KEY}`,
        'Content-Type': 'application/json'
      }
    });
    
    const data = await response.json();
    
    // Aqui você pode processar e salvar os dados das lojas se necessário
    console.log(`${data.data.length} lojas encontradas na API`);
    
  } catch (error) {
    console.error('Erro ao atualizar dados das lojas:', error);
  }
}

async function updateVisitorMetrics() {
  const today = new Date().toISOString().split('T')[0];
  
  try {
    // Simulação: Obter dados da API de analytics
    // Substitua por sua lógica real de obtenção de dados
    const mockData = {
      visitor_count: Math.floor(Math.random() * 1000) + 500,
      peak_hour: `${Math.floor(Math.random() * 24)}:${Math.floor(Math.random() * 60)}`
    };
    
    // Salvar no banco de dados
    const { error } = await supabase
      .from('visitor_metrics')
      .upsert({
        store_id: 'all',
        date: today,
        visitor_count: mockData.visitor_count,
        peak_hour: mockData.peak_hour,
        updated_at: new Date()
      });
    
    if (error) throw error;
    
  } catch (error) {
    console.error('Erro ao atualizar métricas de visitantes:', error);
  }
}

// Funções similares para updateDemographics() e updateWeeklyData()

export { updateAllData };