// app.js
const express = require('express');
const axios = require('axios');
const cron = require('node-cron');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());

// In-memory storage
let stats = { newProducts: 0, inventoryUpdates: 0, discontinued: 0, errors: 0, lastSync: null };
let logs = [];
let systemPaused = false;

function addLog(message, type = 'info') {
  const log = { timestamp: new Date().toISOString(), message, type };
  logs.unshift(log);
  if (logs.length > 50) logs = logs.slice(0, 50);
  console.log(`[${log.timestamp}] ${message}`);
}

// Configuration
const config = {
  apify: {
    token: process.env.APIFY_TOKEN,
    actorId: process.env.APIFY_ACTOR_ID || 'autofacts~shopify',
    baseUrl: 'https://api.apify.com/v2'
  },
  shopify: {
    domain: process.env.SHOPIFY_DOMAIN,
    accessToken: process.env.SHOPIFY_ACCESS_TOKEN,
    locationId: process.env.SHOPIFY_LOCATION_ID,
    baseUrl: `https://${process.env.SHOPIFY_DOMAIN}/admin/api/2024-01`
  }
};

// API clients
const apifyClient = axios.create({ baseURL: config.apify.baseUrl, timeout: 30000 });
const shopifyClient = axios.create({
  baseURL: config.shopify.baseUrl,
  headers: {
    'X-Shopify-Access-Token': config.shopify.accessToken,
    'Content-Type': 'application/json'
  },
  timeout: 35000
});

// Handle extractor
function extractHandleFromCanonicalUrl(item, index) {
  const canonicalUrl = item.canonicalUrl || item['source/canonicalUrl'] || item.source?.canonicalUrl;

  if (index < 3) {
    addLog(`Debug product ${index}: canonicalUrl="${canonicalUrl}", title="${item.title}"`, 'info');
  }

  if (canonicalUrl && canonicalUrl !== 'undefined') {
    const handle = canonicalUrl.replace('https://www.manchesterwholesale.co.uk/products/', '');
    if (handle && handle.length > 0) return handle;
  }

  const titleForHandle = item.title || `product-${index}`;
  return titleForHandle.toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-|-$/g, '');
}

// Dummy placeholders for logic functions (replace with your actual implementations)
async function getApifyProducts() { return []; }
async function getShopifyProducts() { return []; }
async function createNewProducts() { return { created: 0 }; }
async function updateInventory() {
  addLog('Running inventory update...', 'info');
  const apifyProducts = await getApifyProducts();
  const shopifyProducts = await getShopifyProducts();

  const shopifyMap = new Map(shopifyProducts.map(p => [p.handle, p]));

  let unmatched = [];
  apifyProducts.forEach((ap, i) => {
    const handle = extractHandleFromCanonicalUrl(ap, i);
    if (!shopifyMap.has(handle)) unmatched.push(handle);
  });

  if (unmatched.length > 0) {
    addLog(`Unmatched Apify handles (first 20): ${unmatched.slice(0,20).join(', ')}`, 'warning');
    addLog(`Total unmatched: ${unmatched.length}`, 'warning');
  }

  return { updated: shopifyProducts.length };
}
async function handleDiscontinuedProducts() { return { discontinued: 0 }; }

// Dashboard route
app.get('/', (req, res) => {
  res.send(`
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Shopify Sync Dashboard</title>
  <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-gray-50 min-h-screen">
  <div class="container mx-auto px-4 py-8">
    <h1 class="text-3xl font-bold text-gray-900 mb-6">Shopify Sync Dashboard</h1>

    <div class="grid grid-cols-1 md:grid-cols-4 gap-6 mb-8">
      <div class="bg-white rounded-xl shadow-sm border border-gray-100 p-6">
        <h3 class="text-lg font-semibold text-blue-600">New Products</h3>
        <p class="text-3xl font-bold text-gray-900" id="newProducts">${stats.newProducts}</p>
      </div>
      <div class="bg-white rounded-xl shadow-sm border border-gray-100 p-6">
        <h3 class="text-lg font-semibold text-green-600">Inventory Updates</h3>
        <p class="text-3xl font-bold text-gray-900" id="inventoryUpdates">${stats.inventoryUpdates}</p>
      </div>
      <div class="bg-white rounded-xl shadow-sm border border-gray-100 p-6">
        <h3 class="text-lg font-semibold text-orange-600">Discontinued</h3>
        <p class="text-3xl font-bold text-gray-900" id="discontinued">${stats.discontinued}</p>
      </div>
      <div class="bg-white rounded-xl shadow-sm border border-gray-100 p-6">
        <h3 class="text-lg font-semibold text-red-600">Errors</h3>
        <p class="text-3xl font-bold text-gray-900" id="errors">${stats.errors}</p>
      </div>
    </div>

    <div class="bg-white rounded-xl shadow-sm border border-gray-100 p-6">
      <h2 class="text-xl font-semibold text-gray-900 mb-4">Activity Log</h2>
      <div class="bg-black rounded-lg p-4 h-64 overflow-y-auto font-mono text-sm" id="logContainer">
        ${logs.map(log => 
          '<div class="' +
          (log.type === 'success' ? 'text-green-400' :
           log.type === 'error' ? 'text-red-400' :
           log.type === 'warning' ? 'text-yellow-400' :
           'text-gray-300') +
          '">[' + new Date(log.timestamp).toLocaleTimeString() + '] ' +
          log.message + '</div>'
        ).join('')}
      </div>
    </div>
  </div>
</body>
</html>
  `);
});

// API routes
app.get('/api/status', (req, res) => {
  res.json({
    ...stats,
    systemPaused,
    logs: logs.slice(0, 10),
    uptime: process.uptime(),
    environment: {
      apifyToken: config.apify.token ? 'SET' : 'MISSING',
      shopifyToken: config.shopify.accessToken ? 'SET' : 'MISSING',
      shopifyDomain: config.shopify.domain || 'MISSING'
    }
  });
});

app.post('/api/pause', (req, res) => {
  systemPaused = !systemPaused;
  addLog(`System ${systemPaused ? 'paused' : 'resumed'}`, 'info');
  res.json({ success: true, paused: systemPaused });
});

app.post('/api/sync/products', async (req, res) => {
  try {
    addLog('Manual product sync triggered', 'info');
    const result = await createNewProducts();
    res.json({ success: true, message: `Created ${result.created} products`, data: result });
  } catch (error) {
    addLog(`Product sync failed: ${error.message}`, 'error');
    res.status(500).json({ success: false, error: error.message });
  }
});

app.post('/api/sync/inventory', async (req, res) => {
  try {
    addLog('Manual inventory sync triggered', 'info');
    const result = await updateInventory();
    res.json({ success: true, message: `Updated ${result.updated} products`, data: result });
  } catch (error) {
    addLog(`Inventory sync failed: ${error.message}`, 'error');
    res.status(500).json({ success: false, error: error.message });
  }
});

app.post('/api/sync/discontinued', async (req, res) => {
  try {
    addLog('Manual discontinued check triggered', 'info');
    const result = await handleDiscontinuedProducts();
    res.json({ success: true, message: `Discontinued ${result.discontinued} products`, data: result });
  } catch (error) {
    addLog(`Discontinued check failed: ${error.message}`, 'error');
    res.status(500).json({ success: false, error: error.message });
  }
});

// Scheduled jobs
cron.schedule('*/30 * * * *', async () => {
  if (systemPaused) {
    addLog('Scheduled inventory update skipped - system paused', 'warning');
    return;
  }
  addLog('Starting scheduled inventory update', 'info');
  try { await updateInventory(); } catch (error) { addLog(`Scheduled inventory update failed: ${error.message}`, 'error'); }
});

cron.schedule('0 */6 * * *', async () => {
  if (systemPaused) {
    addLog('Scheduled product creation skipped - system paused', 'warning');
    return;
  }
  addLog('Starting scheduled product creation', 'info');
  try { 
    await createNewProducts(); 
    await handleDiscontinuedProducts();
  } catch (error) { addLog(`Scheduled product creation failed: ${error.message}`, 'error'); }
});

app.listen(PORT, () => {
  addLog(`Shopify Sync Service started on port ${PORT}`, 'success');
  addLog('Inventory updates: Every 30 minutes', 'info');
  addLog('Product creation: Every 6 hours', 'info');
  if (!config.apify.token) addLog('WARNING: APIFY_TOKEN not set', 'error');
  if (!config.shopify.accessToken) addLog('WARNING: SHOPIFY_ACCESS_TOKEN not set', 'error');
  if (!config.shopify.domain) addLog('WARNING: SHOPIFY_DOMAIN not set', 'error');
});
