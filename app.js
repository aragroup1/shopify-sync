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
  timeout: 35000
});

// Helper functions
async function getApifyProducts() {
  let allItems = [];
  let offset = 0;
  let pageCount = 0;
  const limit = 500;
  
  addLog('Starting Apify product fetch...', 'info');
  
  while (true) {
    const response = await apifyClient.get(
      `/acts/${config.apify.actorId}/runs/last/dataset/items?token=${config.apify.token}&limit=${limit}&offset=${offset}`
    );
    const items = response.data;
    allItems.push(...items);
    pageCount++;
    
    addLog(`Apify page ${pageCount}: fetched ${items.length} products (total: ${allItems.length})`, 'info');
    
    if (items.length < limit) break;
    offset += limit;
    await new Promise(resolve => setTimeout(resolve, 1000));
  }
  
  addLog(`Apify fetch complete: ${allItems.length} total products`, 'info');
  
  // Log sample products for debugging
  addLog(`Sample Apify products: ${allItems.slice(0, 3).map(p => `${p.title} (${p.sku || 'no-sku'})`).join(', ')}`, 'info');
  
  return allItems;
}

async function getShopifyProducts() {
  let allProducts = [];
  let sinceId = null;
  let pageCount = 0;
  const limit = 250;
  const fields = 'id,handle,title,variants,tags';
  
  addLog('Starting Shopify product fetch...', 'info');
  
  while (true) {
    let url = `/products.json?limit=${limit}&fields=${fields}`;
    if (sinceId) url += `&since_id=${sinceId}`;
    
    const response = await shopifyClient.get(url);
    const products = response.data.products;
    allProducts.push(...products);
    pageCount++;
    
    addLog(`Shopify page ${pageCount}: fetched ${products.length} products (total: ${allProducts.length})`, 'info');
    
    if (products.length < limit) break;
    sinceId = products[products.length - 1].id;
    await new Promise(resolve => setTimeout(resolve, 1000));
  }
  
  const apifyTaggedProducts = allProducts.filter(p => p.tags && p.tags.includes('Supplier:Apify'));
  addLog(`Shopify fetch complete: ${allProducts.length} total products, ${apifyTaggedProducts.length} with Supplier:Apify tag`, 'info');
  
  // Log sample products for debugging
  addLog(`Sample Shopify products: ${apifyTaggedProducts.slice(0, 3).map(p => `${p.handle} (inv: ${p.variants?.[0]?.inventory_quantity || 'N/A'})`).join(', ')}`, 'info');
  
  return apifyTaggedProducts;
}

// Generate SEO-friendly description
function generateSEODescription(product) {
  const title = product.title;
  const originalDescription = product.description || '';
  
  const features = [];
  if (title.toLowerCase().includes('halloween')) features.push('perfect for Halloween celebrations');
  if (title.toLowerCase().includes('kids') || title.toLowerCase().includes('children')) features.push('designed for children');
  if (title.toLowerCase().includes('game') || title.toLowerCase().includes('puzzle')) features.push('entertaining and educational');
  if (title.toLowerCase().includes('mask')) features.push('comfortable and easy to wear');
  if (title.toLowerCase().includes('decoration')) features.push('enhances any space');
  if (title.toLowerCase().includes('toy')) features.push('safe and durable construction');
  
  let seoDescription = `${title} - Premium quality ${title.toLowerCase()} available at LandOfEssentials. `;
  
  if (originalDescription && originalDescription.length > 20) {
    seoDescription += `${originalDescription.substring(0, 150)}... `;
  }
  
  if (features.length > 0) {
    seoDescription += `This product is ${features.join(', ')}. `;
  }
  
  seoDescription += `Order now for fast delivery. `;
  seoDescription += `Shop with confidence at LandOfEssentials - your trusted online retailer for quality products.`;
  
  const keywords = [];
  if (title.toLowerCase().includes('halloween')) keywords.push('halloween costumes', 'party supplies', 'trick or treat');
  if (title.toLowerCase().includes('game')) keywords.push('family games', 'entertainment', 'fun activities');
  if (title.toLowerCase().includes('decoration')) keywords.push('home decor', 'decorative items', 'interior design');
  if (title.toLowerCase().includes('toy')) keywords.push('children toys', 'educational toys', 'safe toys');
  
  if (keywords.length > 0) {
    seoDescription += ` Perfect for: ${keywords.join(', ')}.`;
  }
  
  return seoDescription;
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

    const productData = {
      handle, title: cleanTitle,
      description: item.description || `${cleanTitle}\n\nHigh-quality product from LandOfEssentials.`,
      sku: item.sku || '', originalPrice: price.toFixed(2), price: finalPrice.toFixed(2),
      compareAtPrice: (finalPrice * 1.2).toFixed(2), inventory, images, vendor: 'LandOfEssentials'
    };

    // Generate SEO-friendly description
    productData.seoDescription = generateSEODescription(productData);

    return productData;
  }).filter(Boolean);
}

async function createNewProducts() {
  if (systemPaused) {
    addLog('Product creation skipped - system is paused', 'warning');
    return { created: 0, errors: 0, total: 0 };
  }

  let created = 0, errors = 0;
  try {
    addLog('Fetching data for product creation...', 'info');
    const [apifyData, shopifyData] = await Promise.all([getApifyProducts(), getShopifyProducts()]);
    const processedProducts = processApifyProducts(apifyData);
    const shopifyHandles = new Set(shopifyData.map(p => p.handle));
    const newProducts = processedProducts.filter(p => !shopifyHandles.has(p.handle));

    addLog(`Found ${newProducts.length} new products to create`, 'info');

    // Process products in smaller batches with longer delays
    const batchSize = 5; // Only 5 products at a time
    for (let i = 0; i < newProducts.length; i += batchSize) {
      const batch = newProducts.slice(i, i + batchSize);
      
      for (const product of batch) {
        try {
          const shopifyProduct = {
            title: product.title,
            body_html: product.seoDescription.replace(/\n/g, '<br>'),
            handle: product.handle, 
            vendor: product.vendor, 
            product_type: 'General',
            tags: `Supplier:Apify,Cost:${product.originalPrice},SKU:${product.sku},Auto-Sync`,
            variants: [{
              price: product.price, 
              compare_at_price: product.compareAtPrice,
              sku: product.sku, 
              inventory_management: 'shopify', 
              inventory_policy: 'deny'
            }]
          };

          const response = await shopifyClient.post('/products.json', { product: shopifyProduct });
          const createdProduct = response.data.product;
          addLog(`Created: ${product.title} (£${product.price})`, 'success');

          // Add images with longer delays
          for (const imageUrl of product.images) {
            try {
              await shopifyClient.post(`/products/${createdProduct.id}/images.json`, { image: { src: imageUrl } });
              await new Promise(resolve => setTimeout(resolve, 1000)); // 1 second between images
            } catch (imageError) {
              addLog(`Image failed for ${product.title}`, 'warning');
            }
          }

          // Set inventory
          if (createdProduct.variants && createdProduct.variants[0]) {
            await shopifyClient.post('/inventory_levels/set.json', {
              location_id: config.shopify.locationId,
              inventory_item_id: createdProduct.variants[0].inventory_item_id,
              available: product.inventory
            });
          }

          created++;
          await new Promise(resolve => setTimeout(resolve, 5000)); // 5 seconds between products
        } catch (error) {
          errors++;
          addLog(`Failed to create ${product.title}: ${error.message}`, 'error');
          
          // If we hit rate limit, wait longer
          if (error.response?.status === 429) {
            addLog('Rate limit hit - waiting 30 seconds', 'warning');
            await new Promise(resolve => setTimeout(resolve, 30000));
          }
        }
      }
      
      // Wait between batches
      if (i + batchSize < newProducts.length) {
        addLog(`Completed batch ${Math.floor(i/batchSize) + 1}, waiting before next batch...`, 'info');
        await new Promise(resolve => setTimeout(resolve, 10000)); // 10 seconds between batches
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

async function handleDiscontinuedProducts() {
  let discontinued = 0, errors = 0;
  try {
    addLog('Checking for discontinued products...', 'info');
    const [apifyData, shopifyData] = await Promise.all([getApifyProducts(), getShopifyProducts()]);
    
    const processedApifyProducts = processApifyProducts(apifyData);
    const currentApifyHandles = new Set(processedApifyProducts.map(p => p.handle));
    
    const discontinuedProducts = shopifyData.filter(shopifyProduct => 
      shopifyProduct.tags && 
      shopifyProduct.tags.includes('Supplier:Apify') && 
      !currentApifyHandles.has(shopifyProduct.handle)
    );
    
    addLog(`Found ${discontinuedProducts.length} potentially discontinued products`, 'info');
    
    for (const product of discontinuedProducts) {
      try {
        if (product.variants && product.variants[0]) {
          const currentInventory = product.variants[0].inventory_quantity || 0;
          
          if (currentInventory > 0) {
            await shopifyClient.post('/inventory_levels/set.json', {
              location_id: config.shopify.locationId,
              inventory_item_id: product.variants[0].inventory_item_id,
              available: 0
            });
            
            addLog(`Discontinued: ${product.title} (${currentInventory} → 0)`, 'success');
            discontinued++;
          }
        }
        
        await new Promise(resolve => setTimeout(resolve, 500));
      } catch (error) {
        errors++;
        addLog(`Failed to discontinue ${product.title}: ${error.message}`, 'error');
      }
    }
    
    stats.discontinued += discontinued;
    addLog(`Discontinued product check completed: ${discontinued} discontinued, ${errors} errors`, discontinued > 0 ? 'success' : 'info');
    return { discontinued, errors, total: discontinuedProducts.length };
    
  } catch (error) {
    addLog(`Discontinued product workflow failed: ${error.message}`, 'error');
    return { discontinued, errors: errors + 1, total: 0 };
  }
}

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
    addLog(`Created Shopify lookup map with ${shopifyMap.size} products`, 'info');
    
    // Detailed matching analysis
    let matchedCount = 0;
    let skippedNoVariants = 0;
    let skippedSameInventory = 0;
    
    const inventoryUpdates = [];
    processedProducts.forEach((apifyProduct, index) => {
      const shopifyProduct = shopifyMap.get(apifyProduct.handle);
      
      if (!shopifyProduct) {
        if (index < 5) addLog(`No Shopify match for: ${apifyProduct.handle}`, 'info');
        return;
      }
      
      matchedCount++;
      
      if (!shopifyProduct.variants || !shopifyProduct.variants[0]) {
        skippedNoVariants++;
        if (index < 5) addLog(`No variants for: ${shopifyProduct.handle}`, 'warning');
        return;
      }
      
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
      
      // Log first few updates for debugging
      if (inventoryUpdates.length <= 5) {
        addLog(`Update needed: ${apifyProduct.handle} (${currentInventory} → ${targetInventory})`, 'info');
      }
    });

    addLog(`=== MATCHING ANALYSIS ===`, 'info');
    addLog(`Matched products: ${matchedCount}`, 'info');
    addLog(`Skipped (no variants): ${skippedNoVariants}`, 'info');
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
        await new Promise(resolve => setTimeout(resolve, 500));
      } catch (error) {
        errors++;
        addLog(`✗ Failed: ${update.title} - ${error.message}`, 'error');
      }
    }

    stats.inventoryUpdates += updated;
    stats.errors += errors;
    stats.lastSync = new Date().toISOString();
    addLog(`=== INVENTORY UPDATE COMPLETE ===`, 'info');
    addLog(`Result: ${updated} updated, ${errors} errors out of ${inventoryUpdates.length} needed`, updated > 0 ? 'success' : 'info');
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
                <button onclick="triggerSync('products')" 
                        class="bg-blue-600 text-white px-6 py-3 rounded-lg hover:bg-blue-700 transition-colors">
                    Create New Products
                </button>
                <button onclick="triggerSync('inventory')" 
                        class="bg-green-600 text-white px-6 py-3 rounded-lg hover:bg-green-700 transition-colors">
                    Update Inventory
                </button>
                <button onclick="triggerSync('discontinued')" 
                        class="bg-orange-600 text-white px-6 py-3 rounded-lg hover:bg-orange-700 transition-colors">
                    Check Discontinued
                </button>
            </div>
            <div class="mt-4 p-4 bg-gray-50 rounded-lg">
                <p class="text-sm text-gray-600">
                    <strong>Manual controls work even when system is paused</strong>
                </p>
                <p class="text-sm text-green-600 mt-1">
                    <strong>New:</strong> Discontinued product detection - marks missing supplier products as out of stock
                </p>
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
                    addLogEntry(\`✅ \${result.message}\`, 'success');
                    setTimeout(() => location.reload(), 2000);
                } else {
                    addLogEntry(\`❌ \${result.message || 'Sync failed'}\`, 'error');
                }
            } catch (error) {
                addLogEntry(\`❌ Failed to trigger \${type} sync\`, 'error');
            }
        }

        async function togglePause() {
            try {
                const response = await fetch('/api/pause', { method: 'POST' });
                const result = await response.json();
                if (result.success) {
                    addLogEntry(\`🔄 System \${result.paused ? 'paused' : 'resumed'}\`, 'info');
                    setTimeout(() => location.reload(), 1000);
                } else {
                    addLogEntry('❌ Failed to toggle pause', 'error');
                }
            } catch (error) {
                addLogEntry('❌ Failed to toggle pause', 'error');
            }
        }

        function addLogEntry(message, type) {
            const logContainer = document.getElementById('logContainer');
            const time = new Date().toLocaleTimeString();
            const color = type === 'success' ? 'text-green-400' : type === 'error' ? 'text-red-400' : type === 'warning' ? 'text-yellow-400' : 'text-gray-300';
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
    // Also run discontinued check after product creation
    await handleDiscontinuedProducts();
  } catch (error) { addLog(`Scheduled product creation failed: ${error.message}`, 'error'); }
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
