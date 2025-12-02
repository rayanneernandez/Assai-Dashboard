// api/visitors/list.js
export default async function handler(req, res) {
  const { start, end, page = 1, pageSize = 20 } = req.query;
  
  try {
    // Aqui você pode:
    // 1. Conectar a um banco de dados real (Neon, Supabase, etc.)
    // 2. Ou usar dados mock temporariamente
    
    const mockData = {
      data: [
        { id: 1, name: "Visitante 1", age: 30, gender: "M" },
        { id: 2, name: "Visitante 2", age: 25, gender: "F" },
        // ... mais dados
      ],
      total: 7466,
      page: parseInt(page),
      pageSize: parseInt(pageSize)
    };
    
    return res.status(200).json(mockData);
  } catch (error) {
    return res.status(500).json({ 
      error: "Internal server error",
      details: error.message 
    });
  }
}