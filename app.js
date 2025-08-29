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

// ... (all your business logic functions: getApifyProducts, getShopifyProducts, processApifyProducts, etc.)

// Routes
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
        <div class="bg-white rounded-xl shadow-sm border border-gray-100 p-6 mb-8">
            <h1 class="text-3xl font-bold text-gray-900">Shopify Sync Dashboard</h1>
            <p class="text-gray-600 mt-1">Automated product synchronization from Apify with SEO optimization</p>
        </div>

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

        <div class="bg-white rounded-xl shadow-sm border border-gray-100 p-6 mb-8">
            <h2 class="text-xl font-semibold text-gray-900 mb-4">System Controls</h2>
            <!-- Pause/Resume Toggle -->
            <div class="mb-4 p-4 rounded-lg ${systemPaused ? 'bg-red-50 border border-red-200' : 'bg-green-50 border border-green-200'}">
                <div class="flex items-center justify-between">
                    <div>
                        <h3 class="font-medium ${systemPaused ? 'text-red-800' : 'text-green-800'}">
                            System Status: ${systemPaused ? 'PAUSED' : 'ACTIVE'}
                        </h3>
                        <p class="text-sm ${systemPaused ? 'text-red-600' : 'text-green-600'}">
                            ${systemPaused ? 'Automatic syncing is disabled' : 'Automatic syncing every 30min (inventory) & 6hrs (products)'}
                        </p>
                    </div>
                    <button onclick="togglePause()" 
                            class="${systemPaused ? 'bg-green-600 hover:bg-green-700' : 'bg-red-600 hover:bg-red-700'} text-white px-4 py-2 rounded-lg transition-colors">
                        ${systemPaused ? 'Resume System' : 'Pause System'}
                    </button>
                </div>
            </div>
            <div class="flex flex-wrap gap-4">
                <button onclick="triggerSync('products')" class="bg-blue-600 text-white px-6 py-3 rounded-lg hover:bg-blue-700 transition-colors">Create New Products</button>
                <button onclick="triggerSync('inventory')" class="bg-green-600 text-white px-6 py-3 rounded-lg hover:bg-green-700 transition-colors">Update Inventory</button>
                <button onclick="triggerSync('discontinued')" class="bg-orange-600 text-white px-6 py-3 rounded-lg hover:bg-orange-700 transition-colors">Check Discontinued</button>
            </div>
            <div class="mt-4 p-4 bg-gray-50 rounded-lg">
                <p class="text-sm text-gray-600"><strong>Manual controls work even when system is paused</strong></p>
                <p class="text-sm text-green-600 mt-1"><strong>New:</strong> Discontinued product detection - marks missing supplier products as out of stock</p>
            </div>
        </div>

        <div class="bg-white rounded-xl shadow-sm border border-gray-100 p-6">
            <h2 class="text-xl font-semibold text-gray-900 mb-4">Activity Log</h2>
            <div class="bg-black rounded-lg p-4 h-64 overflow-y-auto font-mono text-sm" id="logContainer">
                ${logs.map(log => (
                  '<div class="' +
                  (log.type === 'success' ? 'text-green-400' :
                   log.type === 'error' ? 'text-red-400' :
                   log.type === 'warning' ? 'text-yellow-400' :
                   'text-gray-300') +
                  '">[' + new Date(log.timestamp).toLocaleTimeString() + '] ' +
                  log.message + '</div>'
                )).join('')}
            </div>
        </div>
    </div>

    <script>
        async function triggerSync(type) {
            let endpoint;
            if (type === 'products') endpoint = '/api/sync/products';
            else if (type === 'inventory') endpoint = '/api/sync/inventory';
            else if (type === 'discontinued') endpoint = '/api/sync/discontinued';
            try {
                const response = await fetch(endpoint, { method: 'POST' });
                const result = await response.json();
                if (result.success) {
                    addLogEntry(`✅ ${result.message}`, 'success');
                    setTimeout(() => location.reload(), 2000);
                } else {
                    addLogEntry(`❌ ${result.message || 'Sync failed'}`, 'error');
                }
            } catch {
                addLogEntry(`❌ Failed to trigger ${type} sync`, 'error');
            }
        }
        async function togglePause() {
            try {
                const response = await fetch('/api/pause', { method: 'POST' });
                const result = await response.json();
                if (result.success) {
                    addLogEntry(`🔄 System ${result.paused ? 'paused' : 'resumed'}`, 'info');
                    setTimeout(() => location.reload(), 1000);
                } else {
                    addLogEntry('❌ Failed to toggle pause', 'error');
                }
            } catch {
                addLogEntry('❌ Failed to toggle pause', 'error');
            }
        }
        function addLogEntry(message, type) {
            const logContainer = document.getElementById('logContainer');
            const time = new Date().toLocaleTimeString();
            const color = type === 'success' ? 'text-green-400' : type === 'error' ? 'text-red-400' : type === 'warning' ? 'text-yellow-400' : 'text-gray-300';
            const newLog = `<div class="${color}">[${time}] ${message}</div>`;
            logContainer.innerHTML = newLog + logContainer.innerHTML;
        }
    </script>
</body>
</html>
  `);
});

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
