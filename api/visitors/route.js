export default async function handler(req, res) {
  if (req.method === 'GET') {
    // Sua lógica aqui
    return res.status(200).json({ visitors: 100 });
  }
  res.status(405).json({ error: 'Method not allowed' });
}