// api/visitors/list.js
const API_TOKEN = process.env.DISPLAYFORCE_API_TOKEN || process.env.DISPLAYFORCE_TOKEN || '4AUH-BX6H-G2RJ-G7PB';
const API_BASE_URL = process.env.DISPLAYFORCE_API_URL || 'https://api.displayforce.ai/public/v1';
export default async function handler(req, res) {
  const { start, end, page = 1, pageSize = 20 } = req.query;
  try {
    // Chamar API real da DisplayForce
    const response = await fetch(
      `${API_BASE_URL}/stats/visitor/list?start_date=${start}&end_date=${end}&page=${page}&limit=${pageSize}`,
      {
        headers: {
          'Authorization': `Bearer ${API_TOKEN}`,
          'Content-Type': 'application/json'
        }
      }
    );
    
    if (!response.ok) {
      throw new Error(`DisplayForce API returned ${response.status}`);
    }
    
    const data = await response.json();
    
    // Formatar resposta para seu frontend
    const formattedData = {
      success: true,
      data: data.visitors || [],
      pagination: {
        page: parseInt(page),
        pageSize: parseInt(pageSize),
        total: data.total_count || 0,
        totalPages: Math.ceil((data.total_count || 0) / pageSize)
      },
      summary: {
        totalVisitors: data.total_count || 0,
        totalMale: data.gender_stats?.male || 0,
        totalFemale: data.gender_stats?.female || 0,
        averageAge: data.average_age || 0
      }
    };
    
    res.status(200).json(formattedData);
    
  } catch (error) {
    console.error('Error fetching visitors:', error);
    res.status(500).json({ 
      success: false,
      error: 'Failed to fetch visitors',
      message: error.message,
      details: 'Check if the DisplayForce API is accessible'
    });
  }
}