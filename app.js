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
const apifyClient = axios.create({ baseURL: config.apify.baseUrl, timeout: 60000 });
const shopifyClient = axios.create({
  baseURL: config.shopify.baseUrl,
  headers: {
    'X-Shopify-Access-Token': config.shopify.accessToken,
    'Content-Type': 'application/json'
  },
  timeout: 60000
});

// --- helpers (getApifyProducts, getShopifyProducts, processApifyProducts, etc.) ---
// [unchanged from your last version, except deduplicated handle extractor]

function extractHandleFromCanonicalUrl(item, index) {
  if (index < 3) {
    addLog(`Debug product ${index}: canonicalUrl="${item.canonicalUrl}", title="${item.title}"`, 'info');
  }

  if (item.canonicalUrl) {
    const handle = item.canonicalUrl.replace('https://www.manchesterwholesale.co.uk/products/', '');
    if (handle && handle !== item.canonicalUrl && handle.length > 0) return handle;
  }

  const titleForHandle = item.title || `product-${index}`;
  return titleForHandle.toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-|-$/g, '')
    .substring(0, 50);
}

// --- updateInventory patched with unmatched handle logging ---
async function updateInventory() {
  if (systemPaused) {
    addLog('Inventory update skipped - system is paused', 'warning');
    return { updated: 0, errors: 0, total: 0 };
  }

  let updated = 0, errors = 0;
  try {
    addLog('=== STARTING INVENTORY UPDATE WORKFLOW ===', 'info');
    const [apifyData, shopifyData] = await Promise.all([getApifyProducts(), getShopifyProducts()]);

    addLog(`Data comparison: ${apifyData.length} Apify products vs ${shopifyData.length} Shopify products`, 'info');

    const processedProducts = processApifyProducts(apifyData);
    addLog(`Processed ${processedProducts.length} valid Apify products after filtering`, 'info');

    const shopifyMap = new Map(shopifyData.map(p => [p.handle, p]));

    let matchedCount = 0;
    let skippedSameInventory = 0;
    let inventoryUpdates = [];

    processedProducts.forEach((apifyProduct, index) => {
      const shopifyProduct = shopifyMap.get(apifyProduct.handle);
      if (!shopifyProduct) return;
      matchedCount++;

      if (!shopifyProduct.variants || !shopifyProduct.variants[0]) return;

      const currentInventory = shopifyProduct.variants[0].inventory_quantity || 0;
      const targetInventory = apifyProduct.inventory;
      if (currentInventory === targetInventory) {
        skippedSameInventory++;
        return;
      }

      inventoryUpdates.push({
        handle: apifyProduct.handle,
        title: shopifyProduct.title,
        currentInventory,
        newInventory: targetInventory,
        inventoryItemId: shopifyProduct.variants[0].inventory_item_id
      });
    });

    // 🔍 log first 20 unmatched handles
    const apifyHandles = new Set(processedProducts.map(p => p.handle));
    const shopifyHandles = new Set(shopifyData.map(p => p.handle));
    const unmatched = [...apifyHandles].filter(h => !shopifyHandles.has(h));
    addLog(`Unmatched handles (first 20): ${unmatched.slice(0,20).join(', ')}`, 'warning');

    addLog(`Matched products: ${matchedCount}`, 'info');
    addLog(`Skipped (same inventory): ${skippedSameInventory}`, 'info');
    addLog(`Updates needed: ${inventoryUpdates.length}`, 'info');

    for (const update of inventoryUpdates) {
      try {
        await shopifyClient.post('/inventory_levels/set.json', {
          location_id: config.shopify.locationId,
          inventory_item_id: update.inventoryItemId,
          available: update.newInventory
        });
        addLog(`✓ Updated: ${update.title} (${update.currentInventory} → ${update.newInventory})`, 'success');
        updated++;
        await new Promise(r => setTimeout(r, 500));
      } catch (err) {
        errors++;
        addLog(`✗ Failed: ${update.title} - ${err.message}`, 'error');
      }
    }

    stats.inventoryUpdates += updated;
    stats.errors += errors;
    stats.lastSync = new Date().toISOString();
    addLog(`=== INVENTORY UPDATE COMPLETE ===`, 'info');
    addLog(`Result: ${updated} updated, ${errors} errors out of ${inventoryUpdates.length} needed`, 'info');
    return { updated, errors, total: inventoryUpdates.length };
  } catch (e) {
    addLog(`Inventory update workflow failed: ${e.message}`, 'error');
    return { updated, errors: errors + 1, total: 0 };
  }
}

// --- Dashboard Route (FULL with buttons restored) ---
app.get('/', (req, res) => {
  res.send(`<!DOCTYPE html>
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
          <button onclick="togglePause()" class="${systemPaused ? 'bg-green-600 hover:bg-green-700' : 'bg-red-600 hover:bg-red-700'} text-white px-4 py-2 rounded-lg transition-colors">
            ${systemPaused ? 'Resume System' : 'Pause System'}
          </button>
        </div>
      </div>
      <div class="flex flex-wrap gap-4">
        <button onclick="triggerSync('products')" class="bg-blue-600 text-white px-6 py-3 rounded-lg hover:bg-blue-700 transition-colors">Create New Products</button>
        <button onclick="triggerSync('inventory')" class="bg-green-600 text-white px-6 py-3 rounded-lg hover:bg-green-700 transition-colors">Update Inventory</button>
        <button onclick="triggerSync('discontinued')" class="bg-orange-600 text-white px-6 py-3 rounded-lg hover:bg-orange-700 transition-colors">Check Discontinued</button>
      </div>
    </div>

    <div class="bg-white rounded-xl shadow-sm border border-gray-100 p-6">
      <h2 class="text-xl font-semibold text-gray-900 mb-4">Activity Log</h2>
      <div class="bg-black rounded-lg p-4 h-64 overflow-y-auto font-mono text-sm" id="logContainer">
        ${logs.map(log => '<div class="' + (log.type === 'success' ? 'text-green-400' : log.type === 'error' ? 'text-red-400' : log.type === 'warning' ? 'text-yellow-400' : 'text-gray-300') + '">[' + new Date(log.timestamp).toLocaleTimeString() + '] ' + log.message + '</div>').join('')}
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
        const res = await fetch(endpoint, { method: 'POST' });
        const result = await res.json();
        if (result.success) {
          addLogEntry('✅ ' + result.message, 'success');
          setTimeout(() => location.reload(), 2000);
        } else {
          addLogEntry('❌ ' + (result.message || 'Sync failed'), 'error');
        }
      } catch {
        addLogEntry('❌ Failed to trigger ' + type + ' sync', 'error');
      }
    }
    async function togglePause() {
      try {
        const res = await fetch('/api/pause', { method: 'POST' });
        const result = await res.json();
        if (result.success) {
          addLogEntry('🔄 System ' + (result.paused ? 'paused' : 'resumed'), 'info');
          setTimeout(() => location.reload(), 1000);
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
</html>`);
});

// --- API routes + cron jobs remain same ---

app.listen(PORT, () => {
  addLog(`Shopify Sync Service started on port ${PORT}`, 'success');
  addLog('Inventory updates: Every 30 minutes', 'info');
  addLog('Product creation: Every 6 hours', 'info');
});
