// api/displayforce/device/list.js
export default async function handler(req, res) {
  try {
    const mockDevices = {
      devices: [
        { id: 1, name: "Sensor 1", status: "active" },
        { id: 2, name: "Sensor 2", status: "active" },
        { id: 3, name: "Sensor 3", status: "inactive" },
      ],
      total: 3
    };
    
    return res.status(200).json(mockDevices);
  } catch (error) {
    return res.status(500).json({ 
      error: "Failed to fetch devices",
      details: error.message 
    });
  }
}