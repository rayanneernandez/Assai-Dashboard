import { useState, useEffect } from 'react';
import DatePicker from 'react-datepicker';
import "react-datepicker/dist/react-datepicker.css";
import { 
  Box, 
  Container, 
  Grid, 
  Card, 
  CardContent, 
  Typography, 
  Select, 
  MenuItem, 
  FormControl,
  InputLabel,
  CircularProgress,
  Alert,
  Button,
  Paper
} from '@mui/material';
import RefreshIcon from '@mui/icons-material/Refresh';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
  Tooltip,
  Legend,
  ArcElement,
  PointElement,
  LineElement
} from 'chart.js';
import { Bar, Doughnut, Line } from 'react-chartjs-2';

// Registrar componentes do Chart.js
ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
  Tooltip,
  Legend,
  ArcElement,
  PointElement,
  LineElement
);

export default function Dashboard() {
  const [stores, setStores] = useState([]);
  const [selectedStore, setSelectedStore] = useState('all');
  const [selectedDate, setSelectedDate] = useState(new Date('2025-12-08'));
  const [dashboardData, setDashboardData] = useState(null);
  const [visitorsData, setVisitorsData] = useState([]);
  const [loading, setLoading] = useState({
    stores: true,
    dashboard: false,
    visitors: false
  });
  const [error, setError] = useState(null);
  const [refreshCount, setRefreshCount] = useState(0);

  // Carregar lojas iniciais
  useEffect(() => {
    loadStores();
  }, []);

  // Carregar dados quando seleções mudarem
  useEffect(() => {
    if (stores.length > 0) {
      loadDashboardData();
      loadVisitorsData();
    }
  }, [selectedStore, selectedDate, refreshCount]);

  const loadStores = async () => {
    setLoading(prev => ({ ...prev, stores: true }));
    setError(null);
    
    try {
      const response = await fetch('/api/assai/dashboard?endpoint=stores');
      
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      
      const result = await response.json();
      
      if (result.success) {
        setStores(result.stores);
        console.log(`Carregadas ${result.stores.length} lojas`);
      } else {
        throw new Error(result.error || 'Falha ao carregar lojas');
      }
    } catch (error) {
      console.error('Erro ao carregar lojas:', error);
      setError(`Erro ao carregar lojas: ${error.message}`);
      
      // Dados de fallback
      setStores(getFallbackStores());
    } finally {
      setLoading(prev => ({ ...prev, stores: false }));
    }
  };

  const loadDashboardData = async () => {
    setLoading(prev => ({ ...prev, dashboard: true }));
    
    try {
      const dateStr = selectedDate.toISOString().split('T')[0];
      const url = `/api/assai/dashboard?endpoint=dashboard-data&storeId=${selectedStore}&date=${dateStr}`;
      
      const response = await fetch(url);
      
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      
      const result = await response.json();
      
      if (result.success) {
        setDashboardData(result.data);
      } else {
        throw new Error(result.error || 'Falha ao carregar dados do dashboard');
      }
    } catch (error) {
      console.error('Erro ao carregar dados do dashboard:', error);
      setDashboardData(getFallbackDashboardData());
    } finally {
      setLoading(prev => ({ ...prev, dashboard: false }));
    }
  };

  const loadVisitorsData = async () => {
    setLoading(prev => ({ ...prev, visitors: true }));
    
    try {
      const startDate = new Date(selectedDate);
      startDate.setDate(startDate.getDate() - 7);
      const startDateStr = startDate.toISOString().split('T')[0];
      const endDateStr = selectedDate.toISOString().split('T')[0];
      
      const url = `/api/assai/dashboard?endpoint=visitors&start_date=${startDateStr}&end_date=${endDateStr}&store_id=${selectedStore}`;
      
      const response = await fetch(url);
      
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      
      const result = await response.json();
      
      if (result.success) {
        setVisitorsData(result.visitors || []);
      } else {
        throw new Error(result.error || 'Falha ao carregar dados de visitantes');
      }
    } catch (error) {
      console.error('Erro ao carregar dados de visitantes:', error);
      setVisitorsData(getFallbackVisitorsData());
    } finally {
      setLoading(prev => ({ ...prev, visitors: false }));
    }
  };

  const handleRefresh = async () => {
    try {
      const response = await fetch('/api/assai/dashboard?endpoint=refresh');
      if (response.ok) {
        setRefreshCount(prev => prev + 1);
        loadStores();
      }
    } catch (error) {
      console.error('Erro ao atualizar:', error);
    }
  };

  // Dados de fallback
  const getFallbackStores = () => [
    {
      id: 'all',
      name: 'Todas as Lojas',
      visitor_count: 10000,
      status: 'active',
      location: 'Todas as unidades',
      type: 'all'
    },
    {
      id: '15267',
      name: 'Assai: Aricanduva - Entrada',
      visitor_count: 4306,
      status: 'active',
      location: 'Assaí Atacadista',
      type: 'camera'
    },
    {
      id: '15268',
      name: 'Assaí Aricanduva - Gondula Açougue',
      visitor_count: 2110,
      status: 'active',
      location: 'Assaí Atacadista',
      type: 'camera'
    },
    {
      id: '15265',
      name: 'Assaí: Aricanduva - Gondula Caixa',
      visitor_count: 1676,
      status: 'active',
      location: 'Assaí Atacadista',
      type: 'camera'
    }
  ];

  const getFallbackDashboardData = () => ({
    total_visitors: 3995,
    peak_time: '18:45',
    table_number: 3995,
    gender_distribution: { male: 68.2, female: 31.8 },
    weekly_visits: { seg: 1250, ter: 1320, qua: 1400, qui: 1380, sex: 1550, sab: 2100, dom: 1850 },
    selected_date: selectedDate.toISOString().split('T')[0],
    selected_store: selectedStore
  });

  const getFallbackVisitorsData = () => {
    const data = [];
    for (let i = 6; i >= 0; i--) {
      const date = new Date(selectedDate);
      date.setDate(date.getDate() - i);
      const dateStr = date.toISOString().split('T')[0];
      const visitors = 500 + Math.floor(Math.random() * 500);
      data.push({ date: dateStr, visitors: visitors });
    }
    return data;
  };

  // Configurações dos gráficos
  const weeklyChartData = {
    labels: ['Seg', 'Ter', 'Qua', 'Qui', 'Sex', 'Sáb', 'Dom'],
    datasets: [{
      label: 'Visitas',
      data: dashboardData ? [
        dashboardData.weekly_visits.seg,
        dashboardData.weekly_visits.ter,
        dashboardData.weekly_visits.qua,
        dashboardData.weekly_visits.qui,
        dashboardData.weekly_visits.sex,
        dashboardData.weekly_visits.sab,
        dashboardData.weekly_visits.dom
      ] : [0, 0, 0, 0, 0, 0, 0],
      backgroundColor: 'rgba(54, 162, 235, 0.6)',
      borderColor: 'rgba(54, 162, 235, 1)',
      borderWidth: 1
    }]
  };

  const genderChartData = {
    labels: ['Masculino', 'Feminino'],
    datasets: [{
      data: dashboardData ? [
        dashboardData.gender_distribution.male,
        dashboardData.gender_distribution.female
      ] : [68.2, 31.8],
      backgroundColor: [
        'rgba(54, 162, 235, 0.6)',
        'rgba(255, 99, 132, 0.6)'
      ],
      borderColor: [
        'rgba(54, 162, 235, 1)',
        'rgba(255, 99, 132, 1)'
      ],
      borderWidth: 1
    }]
  };

  const visitorsChartData = {
    labels: visitorsData.map(item => {
      const date = new Date(item.date);
      return `${date.getDate()}/${date.getMonth() + 1}`;
    }),
    datasets: [{
      label: 'Visitantes',
      data: visitorsData.map(item => item.visitors),
      borderColor: 'rgb(75, 192, 192)',
      backgroundColor: 'rgba(75, 192, 192, 0.2)',
      tension: 0.4
    }]
  };

  if (loading.stores && stores.length === 0) {
    return (
      <Container sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '100vh' }}>
        <CircularProgress />
        <Typography sx={{ ml: 2 }}>Carregando lojas...</Typography>
      </Container>
    );
  }

  return (
    <Container maxWidth="xl" sx={{ mt: 4, mb: 4 }}>
      {/* Cabeçalho */}
      <Paper sx={{ p: 3, mb: 4, backgroundColor: '#f5f5f5' }}>
        <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
          <Typography variant="h4" component="h1" fontWeight="bold">
            Assaí Atacadista - Dashboard de Análise
          </Typography>
          <Button
            variant="contained"
            startIcon={<RefreshIcon />}
            onClick={handleRefresh}
            disabled={loading.stores || loading.dashboard}
          >
            Atualizar Dados
          </Button>
        </Box>

        {error && (
          <Alert severity="error" sx={{ mb: 2 }} onClose={() => setError(null)}>
            {error}
          </Alert>
        )}

        {/* Filtros */}
        <Grid container spacing={3}>
          <Grid item xs={12} md={6}>
            <FormControl fullWidth>
              <InputLabel>Loja</InputLabel>
              <Select
                value={selectedStore}
                label="Loja"
                onChange={(e) => setSelectedStore(e.target.value)}
                disabled={loading.stores}
              >
                {stores.map((store) => (
                  <MenuItem key={store.id} value={store.id}>
                    {store.name} ({store.visitor_count?.toLocaleString()} visitantes)
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
          </Grid>
          <Grid item xs={12} md={6}>
            <FormControl fullWidth>
              <InputLabel shrink>Data</InputLabel>
              <DatePicker
                selected={selectedDate}
                onChange={(date) => setSelectedDate(date)}
                dateFormat="dd/MM/yyyy"
                customInput={
                  <input style={{
                    width: '100%',
                    padding: '16.5px 14px',
                    border: '1px solid rgba(0, 0, 0, 0.23)',
                    borderRadius: '4px',
                    fontSize: '1rem',
                    fontFamily: 'Roboto, Helvetica, Arial, sans-serif'
                  }} />
                }
              />
            </FormControl>
          </Grid>
        </Grid>
      </Paper>

      {/* Métricas Principais */}
      <Grid container spacing={3} sx={{ mb: 4 }}>
        <Grid item xs={12} md={4}>
          <Card>
            <CardContent>
              <Typography color="textSecondary" gutterBottom>
                Total de Visitantes
              </Typography>
              {loading.dashboard ? (
                <CircularProgress size={24} />
              ) : (
                <Typography variant="h3" fontWeight="bold">
                  {dashboardData?.total_visitors?.toLocaleString() || '0'}
                </Typography>
              )}
              <Typography variant="body2" color="textSecondary">
                Período selecionado
              </Typography>
            </CardContent>
          </Card>
        </Grid>
        
        <Grid item xs={12} md={4}>
          <Card>
            <CardContent>
              <Typography color="textSecondary" gutterBottom>
                Pico de Movimentação
              </Typography>
              {loading.dashboard ? (
                <CircularProgress size={24} />
              ) : (
                <Typography variant="h3" fontWeight="bold">
                  {dashboardData?.peak_time || '--:--'}
                </Typography>
              )}
              <Typography variant="body2" color="textSecondary">
                Horário de maior fluxo
              </Typography>
            </CardContent>
          </Card>
        </Grid>
        
        <Grid item xs={12} md={4}>
          <Card>
            <CardContent>
              <Typography color="textSecondary" gutterBottom>
                Mesa do Caixa
              </Typography>
              {loading.dashboard ? (
                <CircularProgress size={24} />
              ) : (
                <Typography variant="h3" fontWeight="bold">
                  {dashboardData?.table_number || 'N/A'}
                </Typography>
              )}
              <Typography variant="body2" color="textSecondary">
                Número de referência
              </Typography>
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      {/* Gráficos */}
      <Grid container spacing={3}>
        {/* Gráfico de Visitas por Dia da Semana */}
        <Grid item xs={12} md={8}>
          <Card sx={{ p: 2, height: '100%' }}>
            <Typography variant="h6" gutterBottom>
              Visitas por Dia da Semana
            </Typography>
            <Box sx={{ height: 300 }}>
              <Bar 
                data={weeklyChartData}
                options={{
                  responsive: true,
                  maintainAspectRatio: false,
                  plugins: {
                    legend: { display: false }
                  },
                  scales: {
                    y: {
                      beginAtZero: true,
                      title: {
                        display: true,
                        text: 'Número de Visitas'
                      }
                    }
                  }
                }}
              />
            </Box>
          </Card>
        </Grid>

        {/* Gráfico de Distribuição por Gênero */}
        <Grid item xs={12} md={4}>
          <Card sx={{ p: 2, height: '100%' }}>
            <Typography variant="h6" gutterBottom>
              Distribuição por Gênero
            </Typography>
            <Box sx={{ height: 250, position: 'relative' }}>
              <Doughnut 
                data={genderChartData}
                options={{
                  responsive: true,
                  maintainAspectRatio: false,
                  plugins: {
                    legend: {
                      position: 'bottom'
                    }
                  }
                }}
              />
            </Box>
            <Box textAlign="center" mt={2}>
              <Typography variant="body1">
                <strong>Masculino:</strong> {dashboardData?.gender_distribution?.male?.toFixed(1) || '68.2'}%
              </Typography>
              <Typography variant="body1">
                <strong>Feminino:</strong> {dashboardData?.gender_distribution?.female?.toFixed(1) || '31.8'}%
              </Typography>
            </Box>
          </Card>
        </Grid>

        {/* Gráfico de Tendência de Visitantes */}
        <Grid item xs={12}>
          <Card sx={{ p: 2, mt: 3 }}>
            <Typography variant="h6" gutterBottom>
              Tendência de Visitantes (Últimos 7 Dias)
            </Typography>
            <Box sx={{ height: 300 }}>
              {loading.visitors ? (
                <Box display="flex" justifyContent="center" alignItems="center" height="100%">
                  <CircularProgress />
                </Box>
              ) : (
                <Line 
                  data={visitorsChartData}
                  options={{
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                      legend: { display: false }
                    },
                    scales: {
                      y: {
                        beginAtZero: true,
                        title: {
                          display: true,
                          text: 'Número de Visitantes'
                        }
                      },
                      x: {
                        title: {
                          display: true,
                          text: 'Data'
                        }
                      }
                    }
                  }}
                />
              )}
            </Box>
          </Card>
        </Grid>
      </Grid>

      {/* Informações de Debug (apenas em desenvolvimento) */}
      {process.env.NODE_ENV === 'development' && (
        <Paper sx={{ p: 2, mt: 4, backgroundColor: '#fffde7' }}>
          <Typography variant="subtitle2" color="textSecondary">
            Informações de Debug:
          </Typography>
          <Typography variant="body2">
            Loja selecionada: {selectedStore} | 
            Data: {selectedDate.toLocaleDateString('pt-BR')} | 
            Total lojas: {stores.length} | 
            Dados carregados: {dashboardData ? 'Sim' : 'Não'}
          </Typography>
        </Paper>
      )}
    </Container>
  );
}