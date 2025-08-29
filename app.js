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

//
// 🔹 Fetch products from Apify
//
async function getApifyProducts() {
  try {
    const response = await apifyClient.get(`/actors/${config.apify.actorId}/runs/last/dataset/items`, {
      params: { token: config.apify.token }
    });
    return response.data; // array of items
  } catch (err) {
    addLog(`Failed to fetch Apify products: ${err.message}`, 'error');
    return [];
  }
}

//
// 🔹 Fetch products from Shopify (with cursor-based pagination)
//
async function getShopifyProducts(includeAll = false) {
  let allProducts = [];
  let pageInfo = null;

  try {
    do {
      const params = { limit: 250 };
      if (pageInfo) params.page_info = pageInfo;

      const response = await shopifyClient.get('/products.json', { params });
      allProducts = allProducts.concat(response.data.products);

      const linkHeader = response.headers['link'];
      const nextPageMatch = linkHeader && linkHeader.match(/<([^>]+)>;\s*rel="next"/);
      pageInfo = nextPageMatch ? new URL(nextPageMatch[1]).searchParams.get('page_info') : null;
    } while (pageInfo);

    return includeAll
      ? allProducts
      : allProducts.filter(p =>
          p.tags &&
          p.tags.toLowerCase().includes('supplier:apify')
        );
  } catch (err) {
    addLog(`Failed to fetch Shopify products: ${err.message}`, 'error');
    return [];
  }
}

//
// 🔹 SEO Description Generator
//
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
    seoDescription += `${originalDescription.replace(/<[^>]+>/g, '').substring(0, 150)}... `;
  }
  if (features.length > 0) seoDescription += `This product is ${features.join(', ')}. `;
  seoDescription += `Order now for fast delivery. Shop with confidence at LandOfEssentials - your trusted online retailer.`;

  return seoDescription;
}

//
// 🔹 Convert Apify item → Shopify product object
//
function extractHandleFromCanonicalUrl(item, index) {
  if (item.canonicalUrl) {
    const handle = item.canonicalUrl.replace('https://www.manchesterwholesale.co.uk/products/', '');
    if (handle && handle !== item.canonicalUrl && handle.length > 0) return handle;
  }
  const titleForHandle = item.title || `product-${index}`;
  return titleForHandle.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '').substring(0, 50);
}

function processApifyProducts(apifyData) {
  return apifyData.map((item, index) => {
    const handle = extractHandleFromCanonicalUrl(item, index);
    if (!handle) return null;

    let price = 0;
    if (item.variants?.length > 0) {
      const variant = item.variants[0];
      if (variant.price) {
        price = typeof variant.price === 'object'
          ? parseFloat(variant.price.current || 0)
          : parseFloat(variant.price);
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
    if (finalPrice === 0) { price = 5.0; finalPrice = 15.0; }

    let cleanTitle = (item.title || 'Untitled Product')
      .replace(/\b\d{4}\b/g, '')
      .replace(/\s*(large letter rate|parcel rate|big parcel rate|letter rate)\s*/gi, '')
      .replace(/\s+/g, ' ')
      .trim();

    const images = item.medias?.slice(0, 3).map(m => m.url).filter(Boolean) || [];
    let inventory = item.variants?.[0]?.price?.stockStatus === 'IN_STOCK' ? 10 : 0;

    const productData = {
      handle,
      title: cleanTitle,
      description: item.description || `${cleanTitle}\n\nHigh-quality product from LandOfEssentials.`,
      sku: item.sku || '',
      originalPrice: price.toFixed(2),
      price: finalPrice.toFixed(2),
      compareAtPrice: (finalPrice * 1.2).toFixed(2),
      inventory,
      images,
      vendor: 'LandOfEssentials'
    };
    productData.seoDescription = generateSEODescription(productData);

    return productData;
  }).filter(Boolean);
}

//
// 🔹 Create new products on Shopify
//
async function createNewProducts() {
  if (systemPaused) {
    addLog('Product creation skipped - system paused', 'warning');
    return { created: 0, errors: 0, total: 0 };
  }

  let created = 0, errors = 0;
  try {
    addLog('Fetching data for product creation...', 'info');
    const [apifyData, shopifyData] = await Promise.all([getApifyProducts(), getShopifyProducts()]);
    const processedProducts = processApifyProducts(apifyData);
    const shopifyHandles = new Set(shopifyData.map(p => p.handle));
    const newProducts = processedProducts.filter(p => !shopifyHandles.has(p.handle));

    addLog(`Found ${newProducts.length} new products`, 'info');

    for (const product of newProducts) {
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
            await new Promise(r => setTimeout(r, 1000));
          } catch {
            addLog(`Image failed for ${product.title}`, 'warning');
          }
        }

        if (createdProduct.variants?.[0]) {
          await shopifyClient.post('/inventory_levels/set.json', {
            location_id: config.shopify.locationId,
            inventory_item_id: createdProduct.variants[0].inventory_item_id,
            available: product.inventory
          });
        }

        created++;
        await new Promise(r => setTimeout(r, 5000));
      } catch (error) {
        errors++;
        addLog(`Failed to create ${product.title}: ${error.message}`, 'error');
      }
    }

    stats.newProducts += created;
    stats.errors += errors;
    stats.lastSync = new Date().toISOString();
    return { created, errors, total: newProducts.length };
  } catch (err) {
    addLog(`Product creation workflow failed: ${err.message}`, 'error');
    return { created, errors: errors + 1, total: 0 };
  }
}

//
// 🔹 Handle discontinued products
//
async function handleDiscontinuedProducts() {
  let discontinued = 0, errors = 0;
  try {
    addLog('Checking discontinued products...', 'info');
    const [apifyData, shopifyData] = await Promise.all([getApifyProducts(), getShopifyProducts()]);
    const processedApifyProducts = processApifyProducts(apifyData);
    const currentApifyHandles = new Set(processedApifyProducts.map(p => p.handle));

    const discontinuedProducts = shopifyData.filter(p =>
      p.tags?.includes('Supplier:Apify') &&
      !currentApifyHandles.has(p.handle)
    );

    for (const product of discontinuedProducts) {
      try {
        if (product.variants?.[0]) {
          await shopifyClient.post('/inventory_levels/set.json', {
            location_id: config.shopify.locationId,
            inventory_item_id: product.variants[0].inventory_item_id,
            available: 0
          });
          addLog(`Discontinued: ${product.title}`, 'success');
          discontinued++;
        }
      } catch (err) {
        errors++;
        addLog(`Failed to discontinue ${product.title}: ${err.message}`, 'error');
      }
    }

    stats.discontinued += discontinued;
    return { discontinued, errors, total: discontinuedProducts.length };
  } catch (err) {
    addLog(`Discontinued workflow failed: ${err.message}`, 'error');
    return { discontinued, errors: errors + 1, total: 0 };
  }
}

//
// 🔹 Update inventory
//
async function updateInventory() {
  if (systemPaused) {
    addLog('Inventory update skipped - system paused', 'warning');
    return { updated: 0, errors: 0, total: 0 };
  }

  let updated = 0, errors = 0;
  try {
    const [apifyData, shopifyData] = await Promise.all([getApifyProducts(), getShopifyProducts()]);
    const processedProducts = processApifyProducts(apifyData);
    const shopifyMap = new Map(shopifyData.map(p => [p.handle, p]));

    const inventoryUpdates = processedProducts.map(apifyProduct => {
      const shopifyProduct = shopifyMap.get(apifyProduct.handle);
      if (!shopifyProduct?.variants?.[0]) return null;
      const currentInventory = shopifyProduct.variants[0].inventory_quantity || 0;
      if (currentInventory === apifyProduct.inventory) return null;
      return {
        title: shopifyProduct.title,
        inventoryItemId: shopifyProduct.variants[0].inventory_item_id,
        newInventory: apifyProduct.inventory
      };
    }).filter(Boolean);

    for (const update of inventoryUpdates) {
      try {
        await shopifyClient.post('/inventory_levels/set.json', {
          location_id: config.shopify.locationId,
          inventory_item_id: update.inventoryItemId,
          available: update.newInventory
        });
        addLog(`Updated: ${update.title} → ${update.newInventory}`, 'success');
        updated++;
        await new Promise(r => setTimeout(r, 500));
      } catch (err) {
        errors++;
        addLog(`Failed to update ${update.title}: ${err.message}`, 'error');
      }
    }

    stats.inventoryUpdates += updated;
    stats.errors += errors;
    stats.lastSync = new Date().toISOString();
    return { updated, errors, total: inventoryUpdates.length };
  } catch (err) {
    addLog(`Inventory update failed: ${err.message}`, 'error');
    return { updated, errors: errors + 1, total: 0 };
  }
}

//
// 🔹 Routes (dashboard + API)
//
app.get('/', (req, res) => {
  res.send('<h1>Shopify Sync Dashboard</h1><p>Go to /api/status for details.</p>');
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
    const result = await createNewProducts();
    res.json({ success: true, message: `Created ${result.created} products`, data: result });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.post('/api/sync/inventory', async (req, res) => {
  try {
    const result = await updateInventory();
    res.json({ success: true, message: `Updated ${result.updated} products`, data: result });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.post('/api/sync/discontinued', async (req, res) => {
  try {
    const result = await handleDiscontinuedProducts();
    res.json({ success: true, message: `Discontinued ${result.discontinued} products`, data: result });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

//
// 🔹 Cron jobs
//
cron.schedule('*/30 * * * *', async () => {
  if (systemPaused) return;
  addLog('Scheduled inventory update', 'info');
  await updateInventory();
});

cron.schedule('0 */6 * * *', async () => {
  if (systemPaused) return;
  addLog('Scheduled product creation', 'info');
  await createNewProducts();
  await handleDiscontinuedProducts();
});

//
// 🔹 Start server
//
app.listen(PORT, () => {
  addLog(`Shopify Sync Service running on port ${PORT}`, 'success');
});
