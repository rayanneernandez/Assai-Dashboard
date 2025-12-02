// api/health.js
export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  
  return res.status(200).json({
    status: 'healthy',
    service: 'Assaí Dashboard API',
    timestamp: new Date().toISOString(),
    endpoints: {
      main: '/api/assai/dashboard',
      health: '/api/health',
      usage: '?endpoint=summary&start_date=2025-12-01&end_date=2025-12-02'
    }
  });
}