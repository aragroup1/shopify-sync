
const express = require('express');
const axios = require('axios');
const cron = require('node-cron');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());

// In-memory storage
let stats = { newProducts: 0, inventoryUpdates: 0, errors: 0, lastSync: null };
let logs = [];

function addLog(message, type = 'info') {
  const log = { timestamp: new Date().toISOString(), message, type };
  logs.unshift(log);
  if (logs.length > 50) logs = logs.slice(0, 50);
  console.log(`[${log.timestamp}] ${message}`);
}

// Configuration from environment variables
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
  timeout: 15000
});

// Helper functions
async function getApifyProducts() {
  const response = await apifyClient.get(
    `/acts/${config.apify.actorId}/runs/last/dataset/items?token=${config.apify.token}&limit=100`
  );
  return response.data;
}

async function getShopifyProducts() {
  const response = await shopifyClient.get('/products.json?limit=250&fields=id,handle,title,variants,tags');
  return response.data.products.filter(p => p.tags && p.tags.includes('Supplier:Apify'));
}

function processApifyProducts(apifyData) {
  return apifyData.map((item, index) => {
    let handle = '';
    if (item.canonicalUrl) {
      const parts = item.canonicalUrl.split('/');
      handle = parts[parts.length - 1] || '';
    } else {
      const titleForHandle = item.title || `product-${index}`;
      handle = titleForHandle.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '').substring(0, 50);
    }

    if (!handle) return null;

    let price = 0;
    if (item.variants && item.variants.length > 0) {
      const variant = item.variants[0];
      if (variant.price) {
        price = typeof variant.price === 'object' ? parseFloat(variant.price.current || 0) : parseFloat(variant.price);
      }
    }
    if (price === 0) price = parseFloat(item.price || 0);
    if (price > 100) price = price / 100;

    let finalPrice = 0;
    if (price <= 1) finalPrice = price + 6;
    else if (price <= 2) finalPrice = price + 7;
    else if (price <= 3) finalPrice = price + 8;
    else if (price <= 5) finalPrice = price + 9;
    else if (price <= 8) finalPrice = price + 10;
    else if (price <= 15) finalPrice = price * 2;
    else finalPrice = price * 2.2;

    if (finalPrice === 0) { price = 5.00; finalPrice = 15.00; }

    let cleanTitle = item.title || 'Untitled Product';
    cleanTitle = cleanTitle.replace(/\b\d{4}\b/g, '').replace(/\s*(large letter rate|parcel rate|big parcel rate|letter rate)\s*/gi, '').replace(/\s+/g, ' ').trim();

    const images = [];
    if (item.medias && Array.isArray(item.medias)) {
      for (let i = 0; i < Math.min(3, item.medias.length); i++) {
        if (item.medias[i] && item.medias[i].url) {
          images.push(item.medias[i].url);
        }
      }
    }

    let inventory = 10;
    if (item.variants && item.variants[0] && item.variants[0].price && item.variants[0].price.stockStatus) {
      inventory = item.variants[0].price.stockStatus === 'IN_STOCK' ? 10 : 0;
    }

    return {
      handle, title: cleanTitle,
      description: item.description || `${cleanTitle}\n\nHigh-quality product from LandOfEssentials.`,
      sku: item.sku || '', originalPrice: price.toFixed(2), price: finalPrice.toFixed(2),
      compareAtPrice: (finalPrice * 1.2).toFixed(2), inventory, images, vendor: 'LandOfEssentials'
    };
  }).filter(Boolean);
}

async function createNewProducts() {
  let created = 0, errors = 0;
  try {
    addLog('Fetching data for product creation...', 'info');
    const [apifyData, shopifyData] = await Promise.all([getApifyProducts(), getShopifyProducts()]);
    const processedProducts = processApifyProducts(apifyData);
    const shopifyHandles = new Set(shopifyData.map(p => p.handle));
    const newProducts = processedProducts.filter(p => !shopifyHandles.has(p.handle));

    addLog(`Found ${newProducts.length} new products to create`, 'info');

    for (const product of newProducts) {
      try {
        const shopifyProduct = {
          title: product.title,
          body_html: product.description.replace(/\n/g, '<br>'),
          handle: product.handle, vendor: product.vendor, product_type: 'General',
          tags: `Supplier:Apify,Cost:${product.originalPrice},SKU:${product.sku},Auto-Sync`,
          variants: [{
            price: product.price, compare_at_price: product.compareAtPrice,
            sku: product.sku, inventory_management: 'shopify', inventory_policy: 'deny'
          }]
        };

        const response = await shopifyClient.post('/products.json', { product: shopifyProduct });
        const createdProduct = response.data.product;
        addLog(`Created: ${product.title} (£${product.price})`, 'success');

        for (const imageUrl of product.images) {
          try {
            await shopifyClient.post(`/products/${createdProduct.id}/images.json`, { image: { src: imageUrl } });
            await new Promise(resolve => setTimeout(resolve, 200));
          } catch (imageError) {
            addLog(`Image failed for ${product.title}`, 'warning');
          }
        }

        if (createdProduct.variants && createdProduct.variants[0]) {
          await shopifyClient.post('/inventory_levels/set.json', {
            location_id: config.shopify.locationId,
            inventory_item_id: createdProduct.variants[0].inventory_item_id,
            available: product.inventory
          });
        }

        created++;
        await new Promise(resolve => setTimeout(resolve, 1000));
      } catch (error) {
        errors++;
        addLog(`Failed to create ${product.title}: ${error.message}`, 'error');
      }
    }

    stats.newProducts += created;
    stats.errors += errors;
    stats.lastSync = new Date().toISOString();
    addLog(`Product creation completed: ${created} created, ${errors} errors`, created > 0 ? 'success' : 'info');
    return { created, errors, total: newProducts.length };
  } catch (error) {
    addLog(`Product creation workflow failed: ${error.message}`, 'error');
    return { created, errors: errors + 1, total: 0 };
  }
}

async function updateInventory() {
  let updated = 0, errors = 0;
  try {
    addLog('Fetching data for inventory updates...', 'info');
    const [apifyData, shopifyData] = await Promise.all([getApifyProducts(), getShopifyProducts()]);
    const processedProducts = processApifyProducts(apifyData);
    const shopifyMap = new Map(shopifyData.map(p => [p.handle, p]));
    
    const inventoryUpdates = [];
    processedProducts.forEach(apifyProduct => {
      const shopifyProduct = shopifyMap.get(apifyProduct.handle);
      if (shopifyProduct && shopifyProduct.variants && shopifyProduct.variants[0]) {
        const currentInventory = shopifyProduct.variants[0].inventory_quantity || 0;
        if (currentInventory !== apifyProduct.inventory) {
          inventoryUpdates.push({
            handle: apifyProduct.handle, title: shopifyProduct.title,
            currentInventory, newInventory: apifyProduct.inventory,
            inventoryItemId: shopifyProduct.variants[0].inventory_item_id
          });
        }
      }
    });

    addLog(`Found ${inventoryUpdates.length} inventory updates needed`, 'info');

    for (const update of inventoryUpdates) {
      try {
        await shopifyClient.post('/inventory_levels/set.json', {
          location_id: config.shopify.locationId,
          inventory_item_id: update.inventoryItemId,
          available: update.newInventory
        });
        addLog(`Updated: ${update.title} (${update.currentInventory} → ${update.newInventory})`, 'success');
        updated++;
        await new Promise(resolve => setTimeout(resolve, 200));
      } catch (error) {
        errors++;
        addLog(`Failed to update ${update.title}: ${error.message}`, 'error');
      }
    }

    stats.inventoryUpdates += updated;
    stats.errors += errors;
    stats.lastSync = new Date().toISOString();
    addLog(`Inventory update completed: ${updated} updated, ${errors} errors`, updated > 0 ? 'success' : 'info');
    return { updated, errors, total: inventoryUpdates.length };
  } catch (error) {
    addLog(`Inventory update workflow failed: ${error.message}`, 'error');
    return { updated, errors: errors + 1, total: 0 };
  }
}

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
            <p class="text-gray-600 mt-1">Automated product synchronization from Apify</p>
        </div>

        <div class="grid grid-cols-1 md:grid-cols-3 gap-6 mb-8">
            <div class="bg-white rounded-xl shadow-sm border border-gray-100 p-6">
                <h3 class="text-lg font-semibold text-blue-600">New Products</h3>
                <p class="text-3xl font-bold text-gray-900" id="newProducts">${stats.newProducts}</p>
            </div>
            <div class="bg-white rounded-xl shadow-sm border border-gray-100 p-6">
                <h3 class="text-lg font-semibold text-green-600">Inventory Updates</h3>
                <p class="text-3xl font-bold text-gray-900" id="inventoryUpdates">${stats.inventoryUpdates}</p>
            </div>
            <div class="bg-white rounded-xl shadow-sm border border-gray-100 p-6">
                <h3 class="text-lg font-semibold text-red-600">Errors</h3>
                <p class="text-3xl font-bold text-gray-900" id="errors">${stats.errors}</p>
            </div>
        </div>

        <div class="bg-white rounded-xl shadow-sm border border-gray-100 p-6 mb-8">
            <h2 class="text-xl font-semibold text-gray-900 mb-4">Manual Controls</h2>
            <div class="flex gap-4">
                <button onclick="triggerSync('products')" 
                        class="bg-blue-600 text-white px-6 py-3 rounded-lg hover:bg-blue-700 transition-colors">
                    Create New Products
                </button>
                <button onclick="triggerSync('inventory')" 
                        class="bg-green-600 text-white px-6 py-3 rounded-lg hover:bg-green-700 transition-colors">
                    Update Inventory
                </button>
            </div>
            <div class="mt-4 p-4 bg-gray-50 rounded-lg">
                <p class="text-sm text-gray-600">
                    <strong>Automatic Schedule:</strong> Inventory updates every 30 minutes • New products every 6 hours
                </p>
            </div>
        </div>

        <div class="bg-white rounded-xl shadow-sm border border-gray-100 p-6">
            <h2 class="text-xl font-semibold text-gray-900 mb-4">Activity Log</h2>
            <div class="bg-black rounded-lg p-4 h-64 overflow-y-auto font-mono text-sm" id="logContainer">
                ${logs.map(log => `
                    <div class="${log.type === 'success' ? 'text-green-400' : log.type === 'error' ? 'text-red-400' : 'text-gray-300'}">
                        [${new Date(log.timestamp).toLocaleTimeString()}] ${log.message}
                    </div>
                `).join('')}
            </div>
        </div>
    </div>

    <script>
        async function triggerSync(type) {
            const endpoint = type === 'products' ? '/api/sync/products' : '/api/sync/inventory';
            try {
                const response = await fetch(endpoint, { method: 'POST' });
                const result = await response.json();
                if (result.success) {
                    addLogEntry(\`✅ \${result.message}\`, 'success');
                    setTimeout(() => location.reload(), 2000);
                } else {
                    addLogEntry(\`❌ \${result.message || 'Sync failed'}\`, 'error');
                }
            } catch (error) {
                addLogEntry(\`❌ Failed to trigger \${type} sync\`, 'error');
            }
        }

        function addLogEntry(message, type) {
            const logContainer = document.getElementById('logContainer');
            const time = new Date().toLocaleTimeString();
            const color = type === 'success' ? 'text-green-400' : type === 'error' ? 'text-red-400' : 'text-gray-300';
            const newLog = \`<div class="\${color}">[\${time}] \${message}</div>\`;
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
    logs: logs.slice(0, 10),
    uptime: process.uptime(),
    environment: {
      apifyToken: config.apify.token ? 'SET' : 'MISSING',
      shopifyToken: config.shopify.accessToken ? 'SET' : 'MISSING',
      shopifyDomain: config.shopify.domain || 'MISSING'
    }
  });
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

// Scheduled jobs
cron.schedule('*/30 * * * *', async () => {
  addLog('Starting scheduled inventory update', 'info');
  try { await updateInventory(); } catch (error) { addLog(`Scheduled inventory update failed: ${error.message}`, 'error'); }
});

cron.schedule('0 */6 * * *', async () => {
  addLog('Starting scheduled product creation', 'info');
  try { await createNewProducts(); } catch (error) { addLog(`Scheduled product creation failed: ${error.message}`, 'error'); }
});

app.listen(PORT, () => {
  addLog(`Shopify Sync Service started on port ${PORT}`, 'success');
  addLog('Inventory updates: Every 30 minutes', 'info');
  addLog('Product creation: Every 6 hours', 'info');
  
  // Environment check
  if (!config.apify.token) addLog('WARNING: APIFY_TOKEN not set', 'error');
  if (!config.shopify.accessToken) addLog('WARNING: SHOPIFY_ACCESS_TOKEN not set', 'error');
  if (!config.shopify.domain) addLog('WARNING: SHOPIFY_DOMAIN not set', 'error');
});
