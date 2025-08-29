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
  addLog(`Sample Apify products: ${allItems.slice(0, 3).map(p => `${p.title} (${p.sku || 'no-sku'})`).join(', ')}`, 'info');
  
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
    let url = `/products.json?limit=${limit}&fields=${fields}`;
    if (sinceId) url += `&since_id=${sinceId}`;
    
    const response = await shopifyClient.get(url);
    const products = response.data.products;
    
    // Filter immediately to reduce memory usage
    const apifyProducts = products.filter(p => p.tags && p.tags.includes('Supplier:Apify'));
    allProducts.push(...apifyProducts);
    pageCount++;
    
    addLog(`Shopify page ${pageCount}: ${apifyProducts.length}/${products.length} relevant products (total relevant: ${allProducts.length})`, 'info');
    
    if (products.length < limit) break;
    sinceId = products[products.length - 1].id;
    await new Promise(resolve => setTimeout(resolve, 500));
  }
  
  addLog(`Shopify fetch complete: ${allProducts.length} products with Supplier:Apify tag`, 'info');
  addLog(`Sample Shopify products: ${allProducts.slice(0, 3).map(p => `${p.handle} (inv: ${p.variants?.[0]?.inventory_quantity || 'N/A'})`).join(', ')}`, 'info');
  
  return allProducts;
}

function extractHandleFromCanonicalUrl(item, index) {
  if (item.canonicalUrl && item.canonicalUrl !== 'undefined' || item.url || item.productUrl || item.source?.url) {
    const urlToUse = item.canonicalUrl || item.url || item.productUrl || item.source?.url;
    const handle = urlToUse.replace('https://www.manchesterwholesale.co.uk/products/', '');
    
    if (index < 3) {
      addLog(`Debug product ${index}: canonicalUrl="${urlToUse}" → handle="${handle}"`, 'info');
    }
    
    if (handle && handle !== urlToUse && handle.length > 0) {
      return handle;
    }
  }
  
  // Fallback to title-based handle
  const titleForHandle = item.title || `product-${index}`;
  const fallbackHandle = titleForHandle.toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-|-$/g, '');
    
  if (index < 3) {
    addLog(`  Using fallback handle: "${fallbackHandle}"`, 'info');
  }
  
  return fallbackHandle;
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

    productData.seoDescription = generateSEODescription(productData);
    return productData;
  }).filter(Boolean);
}

// [The rest of the script: createNewProducts, handleDiscontinuedProducts, updateInventory, routes, and cron jobs]

// ------------------------
// Shopify Operations
// ------------------------
async function createOrUpdateProduct(product) {
  try {
    // Check if product exists
    const existing = await shopifyClient.get(`/products.json?handle=${product.handle}`);
    let shopifyProduct;
    
    if (existing.data.products && existing.data.products.length > 0) {
      shopifyProduct = existing.data.products[0];
      // Update inventory
      if (shopifyProduct.variants && shopifyProduct.variants[0]) {
        await shopifyClient.put(`/variants/${shopifyProduct.variants[0].id}.json`, {
          variant: { inventory_quantity: product.inventory }
        });
        addLog(`Updated inventory for ${product.handle} to ${product.inventory}`);
      }
    } else {
      // Create new product
      const newProduct = {
        product: {
          title: product.title,
          body_html: product.description,
          vendor: product.vendor,
          handle: product.handle,
          tags: 'Supplier:Apify',
          variants: [{
            price: product.price,
            sku: product.sku,
            inventory_quantity: product.inventory
          }],
          images: product.images.map(url => ({ src: url })),
          metafields: [
            {
              namespace: 'seo',
              key: 'description',
              value: product.seoDescription,
              type: 'single_line_text_field'
            }
          ]
        }
      };
      await shopifyClient.post('/products.json', newProduct);
      stats.newProducts++;
      addLog(`Created new product: ${product.handle}`);
    }
  } catch (err) {
    stats.errors++;
    addLog(`Error creating/updating product ${product.handle}: ${err.message}`, 'error');
  }
}

// ------------------------
// Inventory Update
// ------------------------
async function updateInventory(apifyProducts) {
  try {
    const shopifyProducts = await getShopifyProducts();
    const shopifyMap = new Map(shopifyProducts.map(p => [p.handle, p]));

    let updates = 0;

    for (const product of apifyProducts) {
      const shopifyProduct = shopifyMap.get(product.handle);
      if (!shopifyProduct) {
        await createOrUpdateProduct(product);
        continue;
      }

      const variant = shopifyProduct.variants[0];
      if (variant && variant.inventory_quantity !== product.inventory) {
        await shopifyClient.put(`/variants/${variant.id}.json`, {
          variant: { inventory_quantity: product.inventory }
        });
        stats.inventoryUpdates++;
        updates++;
        addLog(`Updated inventory for ${product.handle}: ${variant.inventory_quantity} → ${product.inventory}`);
      }
    }
    addLog(`Inventory update complete: ${updates} products updated.`);
  } catch (err) {
    addLog(`Error updating inventory: ${err.message}`, 'error');
  }
}

// ------------------------
// Discontinued Products
// ------------------------
async function handleDiscontinuedProducts(apifyProducts) {
  try {
    const apifyHandles = new Set(apifyProducts.map(p => p.handle));
    const shopifyProducts = await getShopifyProducts();
    for (const product of shopifyProducts) {
      if (!apifyHandles.has(product.handle)) {
        const variant = product.variants[0];
        if (variant && variant.inventory_quantity !== 0) {
          await shopifyClient.put(`/variants/${variant.id}.json`, { variant: { inventory_quantity: 0 } });
          stats.discontinued++;
          addLog(`Marked discontinued product ${product.handle} inventory → 0`);
        }
      }
    }
  } catch (err) {
    addLog(`Error handling discontinued products: ${err.message}`, 'error');
  }
}

// ------------------------
// Dashboard Route
// ------------------------
app.get('/', (req, res) => {
  res.send(`
  <!DOCTYPE html>
  <html lang="en">
  <head>
    <meta charset="UTF-8">
    <title>Inventory Dashboard</title>
    <style>
      body { font-family: Arial, sans-serif; background:#f2f2f2; margin:0; padding:0; }
      header { background:#2c3e50; color:#fff; padding:1rem; text-align:center; }
      main { padding:1rem; }
      .stats { display:flex; gap:1rem; flex-wrap:wrap; margin-bottom:1rem; }
      .stat { background:#fff; padding:1rem; border-radius:8px; flex:1; min-width:150px; box-shadow:0 0 5px rgba(0,0,0,0.1);}
      .logs { background:#fff; padding:1rem; border-radius:8px; max-height:400px; overflow-y:auto; box-shadow:0 0 5px rgba(0,0,0,0.1);}
      .log-entry { padding:0.2rem 0; border-bottom:1px solid #eee; font-size:0.9rem;}
      .log-entry.error { color:red; }
      button { padding:0.5rem 1rem; border:none; border-radius:5px; background:#2980b9; color:#fff; cursor:pointer; margin-right:0.5rem; }
      button:disabled { background:#ccc; cursor:not-allowed; }
    </style>
  </head>
  <body>
    <header><h1>Inventory Dashboard</h1></header>
    <main>
      <div class="stats">
        <div class="stat">New Products: <span id="newProducts">${stats.newProducts}</span></div>
        <div class="stat">Inventory Updates: <span id="inventoryUpdates">${stats.inventoryUpdates}</span></div>
        <div class="stat">Discontinued: <span id="discontinued">${stats.discontinued}</span></div>
        <div class="stat">Errors: <span id="errors">${stats.errors}</span></div>
        <div class="stat">Last Sync: <span id="lastSync">${stats.lastSync || 'Never'}</span></div>
      </div>
      <div>
        <button id="syncBtn">Run Sync Now</button>
        <button id="pauseBtn">${systemPaused ? 'Resume' : 'Pause'}</button>
      </div>
      <h2>Logs</h2>
      <div class="logs" id="logs">
        ${logs.map(log => `<div class="log-entry ${log.type}">[${log.timestamp}] ${log.message}</div>`).join('')}
      </div>
    </main>
    <script>
      const syncBtn = document.getElementById('syncBtn');
      const pauseBtn = document.getElementById('pauseBtn');
      syncBtn.addEventListener('click', async () => {
        syncBtn.disabled = true;
        await fetch('/sync', { method: 'POST' });
        syncBtn.disabled = false;
        window.location.reload();
      });
      pauseBtn.addEventListener('click', async () => {
        const res = await fetch('/togglePause', { method: 'POST' });
        window.location.reload();
      });
    </script>
  </body>
  </html>
  `);
});

// ------------------------
// API Routes
// ------------------------
app.post('/sync', async (req, res) => {
  if (systemPaused) return res.status(403).send({ message: 'System is paused' });
  try {
    const apifyData = await getApifyProducts();
    const processedProducts = processApifyProducts(apifyData);
    await updateInventory(processedProducts);
    await handleDiscontinuedProducts(processedProducts);
    stats.lastSync = new Date().toISOString();
    res.send({ message: 'Sync complete' });
  } catch (err) {
    addLog(`Sync failed: ${err.message}`, 'error');
    res.status(500).send({ error: err.message });
  }
});

app.post('/togglePause', (req, res) => {
  systemPaused = !systemPaused;
  addLog(`System ${systemPaused ? 'paused' : 'resumed'}`, 'info');
  res.send({ paused: systemPaused });
});

// ------------------------
// Cron Job - Run Every 30 mins
// ------------------------
cron.schedule('*/30 * * * *', async () => {
  if (systemPaused) return;
  addLog('Running scheduled sync...', 'info');
  const apifyData = await getApifyProducts();
  const processedProducts = processApifyProducts(apifyData);
  await updateInventory(processedProducts);
  await handleDiscontinuedProducts(processedProducts);
  stats.lastSync = new Date().toISOString();
});

// ------------------------
// Start Server
// ------------------------
app.listen(PORT, () => addLog(`Server running on port ${PORT}`, 'info'));
