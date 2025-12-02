const API_TOKEN = '4AUH-BX6H-G2RJ-G7PB';

async function testMethods() {
  const urls = [
    'https://api.displayforce.ai/public/v1/device/list',
    'https://api.displayforce.ai/public/v1/stats/visitor/list'
  ];

  for (const url of urls) {
    console.log(`\n=== Testando ${url} ===`);
    
    // Testar GET
    try {
      const res = await fetch(url, {
        method: 'GET',
        headers: {
          'Authorization': `Bearer ${API_TOKEN}`,
          'Content-Type': 'application/json'
        }
      });
      console.log(`GET: Status ${res.status} - ${res.statusText}`);
    } catch (err) {
      console.log(`GET Error: ${err.message}`);
    }

    // Testar POST
    try {
      const res = await fetch(url, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${API_TOKEN}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({ limit: 10 })
      });
      console.log(`POST: Status ${res.status} - ${res.statusText}`);
    } catch (err) {
      console.log(`POST Error: ${err.message}`);
    }
  }
}

testMethods();