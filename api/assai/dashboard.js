// /pages/test-api.js - Para testar se a API está funcionando
import { useState } from 'react';

export default function TestAPI() {
  const [results, setResults] = useState({});
  const [loading, setLoading] = useState(false);

  const testEndpoint = async (endpoint) => {
    setLoading(true);
    try {
      const url = `/api/assai/dashboard?endpoint=${endpoint}`;
      console.log(`Testando: ${url}`);
      
      const response = await fetch(url);
      const data = await response.json();
      
      setResults(prev => ({
        ...prev,
        [endpoint]: {
          status: response.status,
          ok: response.ok,
          data: data
        }
      }));
      
      console.log(`✅ ${endpoint}:`, data);
    } catch (error) {
      console.error(`❌ ${endpoint}:`, error);
      setResults(prev => ({
        ...prev,
        [endpoint]: {
          error: error.message
        }
      }));
    } finally {
      setLoading(false);
    }
  };

  const testAll = () => {
    ['stores', 'dashboard-data', 'visitors', 'refresh'].forEach(testEndpoint);
  };

  return (
    <div style={{ padding: 20, fontFamily: 'Arial' }}>
      <h1>Teste da API Dashboard</h1>
      
      <button 
        onClick={testAll}
        disabled={loading}
        style={{ padding: '10px 20px', margin: '10px' }}
      >
        Testar Todos os Endpoints
      </button>
      
      {loading && <p>Testando...</p>}
      
      {Object.entries(results).map(([endpoint, result]) => (
        <div key={endpoint} style={{ 
          margin: '20px 0', 
          padding: '10px', 
          border: '1px solid #ccc',
          backgroundColor: result.ok ? '#e8f5e9' : '#ffebee'
        }}>
          <h3>{endpoint}</h3>
          <p>Status: {result.status} {result.ok ? '✅' : '❌'}</p>
          <pre style={{ 
            background: '#f5f5f5', 
            padding: '10px', 
            overflow: 'auto',
            fontSize: '12px'
          }}>
            {JSON.stringify(result.data || result.error, null, 2)}
          </pre>
        </div>
      ))}
    </div>
  );
}