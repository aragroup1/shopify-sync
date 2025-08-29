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
    baseUrl: 'https://api.apify.com/v2',
    urlPrefix: process.env.URL_PREFIX || 'https://www.manchesterwholesale.co.uk/products/'
  },
  shopify: {
    domain: process.env.SHOPIFY_DOMAIN,
    accessToken: process.env.SHOPIFY_ACCESS_TOKEN,
    locationId: process.env.SHOPIFY_LOCATION_ID,
    baseUrl: `https://${process.env.SHOPIFY_DOMAIN}/admin/api/2024-01`
  }
};

// API clients
const apifyClient = axios.create({ baseURL: config.apify.baseUrl, timeout: 120000 });
const shopifyClient = axios.create({
  baseURL: config.shopify.baseUrl,
  headers: {
    'X-Shopify-Access-Token': config.shopify.accessToken,
    'Content-Type': 'application/json'
  },
  timeout: 120000
});

// Helper functions
async function getApifyProducts() {
  let allItems = [];
  let offset = 0;
  let pageCount = 0;
  const limit = 500;
  
  addLog('Starting Apify product fetch...', 'info');
  
  while (true) {
    try {
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
    } catch (error) {
      addLog(`Apify fetch error: ${error.message}`, 'error');
      stats.errors++;
      throw error;
    }
  }
  
  addLog(`Apify fetch complete: ${allItems.length} total products`, 'info');
  if (allItems.length > 0) {
    addLog(`Sample Apify products: ${allItems.slice(0, 3).map(p => `${p.title} (${p.sku || 'no-sku'})`).join(', ')}`, 'info');
  }
  
  return allItems;
}

async function getShopifyProducts() {
  let allProducts = [];
  let sinceId = null;
  let pageCount = 0;
  const limit = 250;
  const fields = 'id,handle,title,variants,tags';
  
  addLog('Starting Shopify product fetch (Supplier:Apify only)...', 'info');
  
  while (true) {
    try {
      let url = `/products.json?limit=${limit}&fields=${fields}&tags=Supplier:Apify`;
      if (sinceId) url += `&since_id=${sinceId}`;
      
      const response = await shopifyClient.get(url);
      const products = response.data.products;
      allProducts.push(...products);
      pageCount++;
      
      addLog(`Shopify page ${pageCount}: fetched ${products.length} products (total: ${allProducts.length})`, 'info');
      
      if (products.length < limit) break;
      sinceId = products[products.length - 1].id;
      await new Promise(resolve => setTimeout(resolve, 500));
    } catch (error) {
      addLog(`Shopify fetch error: ${error.message}`, 'error');
      stats.errors++;
      throw error;
    }
  }
  
  addLog(`Shopify fetch complete: ${allProducts.length} products with Supplier:Apify tag`, 'info');
  if (allProducts.length > 0) {
    addLog(`Sample Shopify products: ${allProducts.slice(0, 3).map(p => `${p.handle} (inv: ${p.variants?.[0]?.inventory_quantity || 'N/A'})`).join(', ')}`, 'info');
  }
  
  return allProducts;
}

function normalizeHandle(input, index, isTitle = false) {
  let handle = input;
  
  if (!isTitle && handle && handle !== 'undefined') {
    handle = handle.replace(config.apify.urlPrefix, '');
    if (handle !== input && handle.length > 0) {
      if (index < 3) addLog(`Handle from URL: "${input}" → "${handle}"`, 'info');
      return handle;
    }
  }
  
  // Fallback to title-based handle
  handle = (isTitle ? input : `product-${index}`).toLowerCase()
    .replace(/\b\d{4}\b/g, '') // Remove 4-digit years
    .replace(/\s*(large letter rate|parcel rate|big parcel rate|letter rate)\s*/gi, '') // Remove suffixes
    .replace(/[^a-z0-9]+/g, '-') // Replace non-alphanumeric with dash
    .replace(/^-|-$/g, ''); // Trim leading/trailing dashes
  
  if (index < 3) addLog(`Fallback handle: "${input}" → "${handle}"`, 'info');
  
  return handle;
}

function extractHandleFromCanonicalUrl(item, index) {
  const urlFields = [item.canonicalUrl, item.url, item.productUrl, item.source?.url];
  const validUrl = urlFields.find(url => url && url !== 'undefined');
  
  if (index < 3) {
    addLog(`Debug product ${index}: canonicalUrl="${item.canonicalUrl}", title="${item.title}"`, 'info');
  }
  
  return normalizeHandle(validUrl || item.title, index, !validUrl);
}

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
    const handle = extractHandleFromCanonicalUrl(item, index);
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

    let inventory = item.variants?.[0]?.stockQuantity || item.stock || 10;
    if (item.variants && item.variants[0] && item.variants[0].price && item.variants[0].price.stockStatus) {
      inventory = item.variants[0].price.stockStatus === 'IN_STOCK' ? 10 : 0;
    }

    const productData = {
      handle, title: cleanTitle,
      description: item.description || `${cleanTitle}\n\nHigh-quality product from LandOfEssentials.`,
      sku: item.sku || '', originalPrice: price.toFixed(2), price: finalPrice.toFixed(2),
      compareAtPrice: (finalPrice * 1.2).toFixed(2), inventory, images, vendor: 'LandOfEssentials'
    };

    productData.seoDescription = generateSEODescription(productData);
    return productData;
  }).filter(Boolean);
}

async function createNewProducts(productsToCreate) {
  if (systemPaused) {
    addLog('Product creation skipped - system is paused', 'warning');
    return { created: 0, errors: 0, total: productsToCreate.length };
  }

  let created = 0, errors = 0;
  const batchSize = 5;
  
  for (let i = 0; i < productsToCreate.length; i += batchSize) {
    const batch = productsToCreate.slice(i, i + batchSize);
    
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

        for (const imageUrl of product.images) {
          try {
            await shopifyClient.post(`/products/${createdProduct.id}/images.json`, { image: { src: imageUrl } });
            await new Promise(resolve => setTimeout(resolve, 1000));
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
        stats.newProducts++;
        await new Promise(resolve => setTimeout(resolve, 5000));
      } catch (error) {
        errors++;
        stats.errors++;
        addLog(`Failed to create ${product.title}: ${error.message}`, 'error');
        if (error.response?.status === 429) {
          addLog('Rate limit hit - waiting 30 seconds', 'warning');
          await new Promise(resolve => setTimeout(resolve, 30000));
        }
      }
    }
    
    if (i + batchSize < productsToCreate.length) {
      addLog(`Completed batch ${Math.floor(i/batchSize) + 1}, waiting before next batch...`, 'info');
      await new Promise(resolve => setTimeout(resolve, 10000));
    }
  }

  addLog(`Product creation completed: ${created} created, ${errors} errors`, created > 0 ? 'success' : 'info');
  return { created, errors, total: productsToCreate.length };
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
            stats.discontinued++;
          }
        }
        
        await new Promise(resolve => setTimeout(resolve, 500));
      } catch (error) {
        errors++;
        stats.errors++;
        addLog(`Failed to discontinue ${product.title}: ${error.message}`, 'error');
      }
    }
    
    addLog(`Discontinued product check completed: ${discontinued} discontinued, ${errors} errors`, discontinued > 0 ? 'success' : 'info');
    return { discontinued, errors, total: discontinuedProducts.length };
  } catch (error) {
    addLog(`Discontinued product workflow failed: ${error.message}`, 'error');
    stats.errors++;
    return { discontinued, errors: errors + 1, total: 0 };
  }
}

async function updateInventory() {
  if (systemPaused) {
    addLog('Inventory update skipped - system is paused', 'warning');
    return { updated: 0, created: 0, errors: 0, total: 0 };
  }

  let updated = 0, created = 0, errors = 0;
  try {
    addLog('=== STARTING INVENTORY UPDATE WORKFLOW ===', 'info');
    const [apifyData, shopifyData] = await Promise.all([getApifyProducts(), getShopifyProducts()]);
    
    addLog(`Data comparison: ${apifyData.length} Apify products vs ${shopifyData.length} Shopify products`, 'info');
    
    const processedProducts = processApifyProducts(apifyData);
    addLog(`Processed ${processedProducts.length} valid Apify products after filtering`, 'info');
    
    const shopifyMap = new Map(shopifyData.map(p => [p.handle, p]));
    addLog(`Created Shopify lookup map with ${shopifyMap.size} products`, 'info');
    
    let matchedCount = 0;
    let skippedNoVariants = 0;
    let skippedSameInventory = 0;
    const inventoryUpdates = [];
    const newProducts = [];
    
    processedProducts.forEach((apifyProduct, index) => {
      const shopifyProduct = shopifyMap.get(apifyProduct.handle);
      
      if (!shopifyProduct) {
        if (index < 5) addLog(`No Shopify match for: ${apifyProduct.handle}`, 'info');
        newProducts.push(apifyProduct);
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
      
      if (inventoryUpdates.length <= 5) {
        addLog(`Update needed: ${apifyProduct.handle} (${currentInventory} → ${targetInventory})`, 'info');
      }
    });

    addLog(`=== MATCHING ANALYSIS ===`, 'info');
    addLog(`Matched products: ${matchedCount}`, 'info');
    addLog(`Skipped (no variants): ${skippedNoVariants}`, 'info');
    addLog(`Skipped (same inventory): ${skippedSameInventory}`, 'info');
    addLog(`Updates needed: ${inventoryUpdates.length}`, 'info');
    addLog(`New products to create: ${newProducts.length}`, 'info');

    for (const update of inventoryUpdates) {
      try {
        await shopifyClient.post('/inventory_levels/set.json', {
          location_id: config.shopify.locationId,
          inventory_item_id: update.inventoryItemId,
          available: update.newInventory
        });
        addLog(`✓ Updated: ${update.title} (${update.currentInventory} → ${update.newInventory})`, 'success');
        updated++;
        stats.inventoryUpdates++;
        await new Promise(resolve => setTimeout(resolve, 500));
      } catch (error) {
        errors++;
        stats.errors++;
        addLog(`✗ Failed: ${update.title} - ${error.message}`, 'error');
      }
    }

    if (newProducts.length > 0) {
      addLog('Creating new products for unmatched Apify products...', 'info');
      const createResult = await createNewProducts(newProducts);
      created = createResult.created;
      errors += createResult.errors;
    }

    stats.lastSync = new Date().toISOString();
    addLog(`=== INVENTORY UPDATE COMPLETE ===`, 'info');
    addLog(`Result: ${updated} updated, ${created} created, ${errors} errors`, (updated + created) > 0 ? 'success' : 'info');
    return { updated, created, errors, total: inventoryUpdates.length + newProducts.length };
  } catch (error) {
    addLog(`Inventory update workflow failed: ${error.message}`, 'error');
    stats.errors++;
    return { updated, created, errors: errors + 1, total: 0 };
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
    <style>
        body {
            transition: background-color 0.3s, color 0.3s;
        }
        .dark body {
            background-color: #1f2937;
        }
        .gradient-bg {
            background: linear-gradient(135deg, #6ee7b7 0%, #3b82f6 100%);
        }
        .card-hover:hover {
            transform: translateY(-4px);
            box-shadow: 0 8px 20px rgba(0, 0, 0, 0.15);
        }
        .btn-hover {
            transition: all 0.3s ease;
        }
        .btn-hover:hover {
            transform: scale(1.05);
        }
        .fade-in {
            animation: fadeIn 0.5s ease-in;
        }
        @keyframes fadeIn {
            from { opacity: 0; transform: translateY(10px); }
            to { opacity: 1; transform: translateY(0); }
        }
    </style>
</head>
<body class="min-h-screen font-sans transition-colors">
    <div class="container mx-auto px-4 py-8">
        <div class="relative bg-white dark:bg-gray-800 rounded-2xl shadow-lg p-8 mb-8 gradient-bg text-white fade-in">
            <h1 class="text-4xl font-extrabold tracking-tight">Shopify Sync Dashboard</h1>
            <p class="mt-2 text-lg opacity-90">Seamless product synchronization with Apify, optimized for SEO</p>
            <button onclick="toggleDarkMode()" class="absolute top-4 right-4 bg-gray-200 dark:bg-gray-700 text-gray-800 dark:text-gray-200 px-3 py-1 rounded-full text-sm btn-hover">
                Toggle Dark Mode
            </button>
        </div>

        <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-6 mb-8">
            <div class="bg-white dark:bg-gray-800 rounded-2xl shadow-md p-6 card-hover fade-in">
                <h3 class="text-lg font-semibold text-blue-500 dark:text-blue-400">New Products</h3>
                <p class="text-3xl font-bold text-gray-900 dark:text-gray-100" id="newProducts">${stats.newProducts}</p>
            </div>
            <div class="bg-white dark:bg-gray-800 rounded-2xl shadow-md p-6 card-hover fade-in">
                <h3 class="text-lg font-semibold text-green-500 dark:text-green-400">Inventory Updates</h3>
                <p class="text-3xl font-bold text-gray-900 dark:text-gray-100" id="inventoryUpdates">${stats.inventoryUpdates}</p>
            </div>
            <div class="bg-white dark:bg-gray-800 rounded-2xl shadow-md p-6 card-hover fade-in">
                <h3 class="text-lg font-semibold text-orange-500 dark:text-orange-400">Discontinued</h3>
                <p class="text-3xl font-bold text-gray-900 dark:text-gray-100" id="discontinued">${stats.discontinued}</p>
            </div>
            <div class="bg-white dark:bg-gray-800 rounded-2xl shadow-md p-6 card-hover fade-in">
                <h3 class="text-lg font-semibold text-red-500 dark:text-red-400">Errors</h3>
                <p class="text-3xl font-bold text-gray-900 dark:text-gray-100" id="errors">${stats.errors}</p>
            </div>
        </div>

        <div class="bg-white dark:bg-gray-800 rounded-2xl shadow-md p-6 mb-8 fade-in">
            <h2 class="text-2xl font-semibold text-gray-900 dark:text-gray-100 mb-4">System Controls</h2>
            <div class="mb-6 p-4 rounded-lg ${systemPaused ? 'bg-red-100 dark:bg-red-900 border-red-300 dark:border-red-700' : 'bg-green-100 dark:bg-green-900 border-green-300 dark:border-green-700'} border">
                <div class="flex items-center justify-between">
                    <div>
                        <h3 class="font-medium ${systemPaused ? 'text-red-800 dark:text-red-300' : 'text-green-800 dark:text-green-300'}">
                            System Status: ${systemPaused ? 'PAUSED' : 'ACTIVE'}
                        </h3>
                        <p class="text-sm ${systemPaused ? 'text-red-600 dark:text-red-400' : 'text-green-600 dark:text-green-400'}">
                            ${systemPaused ? 'Automatic syncing is disabled' : 'Automatic syncing every 30min (inventory) & 6hrs (products)'}
                        </p>
                    </div>
                    <button onclick="togglePause()" 
                            class="${systemPaused ? 'bg-green-500 hover:bg-green-600' : 'bg-red-500 hover:bg-red-600'} text-white px-4 py-2 rounded-lg btn-hover">
                        ${systemPaused ? 'Resume System' : 'Pause System'}
                    </button>
                </div>
            </div>
            <div class="flex flex-wrap gap-4">
                <button onclick="triggerSync('products')" class="bg-blue-500 text-white px-6 py-3 rounded-lg btn-hover">Create New Products</button>
                <button onclick="triggerSync('inventory')" class="bg-green-500 text-white px-6 py-3 rounded-lg btn-hover">Update Inventory</button>
                <button onclick="triggerSync('discontinued')" class="bg-orange-500 text-white px-6 py-3 rounded-lg btn-hover">Check Discontinued</button>
            </div>
            <div class="mt-4 p-4 bg-gray-100 dark:bg-gray-700 rounded-lg">
                <p class="text-sm text-gray-600 dark:text-gray-400"><strong>Manual controls work even when system is paused</strong></p>
                <p class="text-sm text-blue-600 dark:text-blue-400 mt-1"><strong>Updated:</strong> Improved handle matching and automatic product creation</p>
            </div>
        </div>

        <div class="bg-white dark:bg-gray-800 rounded-2xl shadow-md p-6 fade-in">
            <h2 class="text-2xl font-semibold text-gray-900 dark:text-gray-100 mb-4">Activity Log</h2>
            <div class="bg-gray-900 rounded-lg p-4 h-80 overflow-y-auto font-mono text-sm" id="logContainer">
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

    <script>
        function toggleDarkMode() {
            document.documentElement.classList.toggle('dark');
            localStorage.setItem('darkMode', document.documentElement.classList.contains('dark'));
        }
        if (localStorage.getItem('darkMode') === 'true') {
            document.documentElement.classList.add('dark');
        }
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
            } catch {
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
            } catch {
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
    const apifyData = await getApifyProducts();
    const processedProducts = processApifyProducts(apifyData);
    const shopifyData = await getShopifyProducts();
    const shopifyHandles = new Set(shopifyData.map(p => p.handle));
    const newProducts = processedProducts.filter(p => !shopifyHandles.has(p.handle));
    const result = await createNewProducts(newProducts);
    res.json({ success: true, message: `Created ${result.created} products`, data: result });
  } catch (error) {
    addLog(`Product sync failed: ${error.message}`, 'error');
    stats.errors++;
    res.status(500).json({ success: false, error: error.message });
  }
});

app.post('/api/sync/inventory', async (req, res) => {
  try {
    addLog('Manual inventory sync triggered', 'info');
    const result = await updateInventory();
    res.json({ success: true, message: `Updated ${result.updated} products, created ${result.created} products`, data: result });
  } catch (error) {
    addLog(`Inventory sync failed: ${error.message}`, 'error');
    stats.errors++;
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
    stats.errors++;
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
  try { await updateInventory(); } catch (error) { addLog(`Scheduled inventory update failed: ${error.message}`, 'error'); stats.errors++; }
});

cron.schedule('0 */6 * * *', async () => {
  if (systemPaused) {
    addLog('Scheduled product creation skipped - system paused', 'warning');
    return;
  }
  addLog('Starting scheduled product creation', 'info');
  try { 
    const apifyData = await getApifyProducts();
    const processedProducts = processApifyProducts(apifyData);
    const shopifyData = await getShopifyProducts();
    const shopifyHandles = new Set(shopifyData.map(p => p.handle));
    const newProducts = processedProducts.filter(p => !shopifyHandles.has(p.handle));
    await createNewProducts(newProducts);
    await handleDiscontinuedProducts();
  } catch (error) { 
    addLog(`Scheduled product creation failed: ${error.message}`, 'error'); 
    stats.errors++; 
  }
});

app.listen(PORT, () => {
  const requiredEnv = ['APIFY_TOKEN', 'SHOPIFY_ACCESS_TOKEN', 'SHOPIFY_DOMAIN'];
  const missing = requiredEnv.filter(key => !process.env[key]);
  if (missing.length > 0) {
    addLog(`FATAL: Missing environment variables: ${missing.join(', ')}`, 'error');
    process.exit(1);
  }
  addLog(`Shopify Sync Service started on port ${PORT}`, 'success');
  addLog('Inventory updates: Every 30 minutes', 'info');
  addLog('Product creation: Every 6 hours', 'info');
});
