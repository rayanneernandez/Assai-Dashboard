// api/stats/visitors.js - VERSÃO SUPER SIMPLES
export default async function handler(req, res) {
  // Simula dados para teste
  const { start, end } = req.query;
  
  res.json({
    success: true,
    message: 'Endpoint de stats funcionando',
    start,
    end,
    data: {
      total: 150,
      men: 90,
      women: 60,
      averageAge: 35,
      byDayOfWeek: { Seg: 20, Ter: 30, Qua: 25, Qui: 35, Sex: 30, Sáb: 5, Dom: 5 },
      byAgeGroup: { "18-25": 30, "26-35": 50, "36-45": 40, "46-60": 25, "60+": 5 }
    }
  });
}