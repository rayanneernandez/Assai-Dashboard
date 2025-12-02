// api/displayforce/device/list.js
const API_TOKEN = process.env.DISPLAYFORCE_API_TOKEN || process.env.DISPLAYFORCE_TOKEN || '4AUH-BX6H-G2RJ-G7PB';
const API_BASE_URL = process.env.DISPLAYFORCE_API_URL || 'https://api.displayforce.ai/public/v1';
export default async function handler(req, res) {
  try {
    // Chamar API real da DisplayForce
    const response = await fetch(
      `${API_BASE_URL}/device/list`,
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
    
    // Formatar resposta
    const formattedData = {
      success: true,
      devices: data.devices || [],
      stats: {
        totalDevices: data.total_devices || 0,
        activeDevices: data.active_devices || 0,
        inactiveDevices: data.inactive_devices || 0
      }
    };
    
    res.status(200).json(formattedData);
    
  } catch (error) {
    console.error('Error fetching devices:', error);
    res.status(500).json({ 
      success: false,
      error: 'Failed to fetch devices',
      message: error.message
    });
  }
}