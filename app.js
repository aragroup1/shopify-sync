const express = require('express');
const axios = require('axios');
const cron = require('node-cron');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());

// -------------------- In-memory storage --------------------
let stats = { newProducts: 0, inventoryUpdates: 0, discontinued: 0, errors: 0, lastSync: null };
let logs = [];
let systemPaused = false;

function addLog(message, type = 'info') {
  const log = { timestamp: new Date().toISOString(), message, type };
  logs.unshift(log);
  if (logs.length > 50) logs = logs.slice(0, 50);
  console.log(`[${log.timestamp}] ${message}`);
}

// -------------------- Configuration --------------------
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

// -------------------- API Clients --------------------
const apifyClient = axios.create({ baseURL: config.apify.baseUrl, timeout: 60000 });
const shopifyClient = axios.create({
  baseURL: config.shopify.baseUrl,
  headers: {
    'X-Shopify-Access-Token': config.shopify.accessToken,
    'Content-Type': 'application/json'
  },
  timeout: 60000
});

// -------------------- Apify Fetch --------------------
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

// -------------------- Shopify Fetch --------------------
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

// -------------------- Handle Extraction --------------------
function extractHandleFromCanonicalUrl(item, index) {
  if (index < 3) {
    addLog(`Debug product ${index}: canonicalUrl="${item.canonicalUrl}", title="${item.title}"`, 'info');
  }

  const urlToUse = item.canonicalUrl && item.canonicalUrl !== 'undefined'
    ? item.canonicalUrl
    : item.url || item.productUrl || item.source?.url;

  if (urlToUse) {
    const handle = urlToUse.replace('https://www.manchesterwholesale.co.uk/products/', '');
    if (index < 3) addLog(`  After URL removal: "${handle}"`, 'info');
    if (handle && handle !== urlToUse && handle.length > 0) return handle;
  }

  const titleForHandle = item.title || `product-${index}`;
  const fallbackHandle = titleForHandle.toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-|-$/g, '');

  if (index < 3) addLog(`  Using fallback handle: "${fallbackHandle}"`, 'info');
  return fallbackHandle;
}

// -------------------- SEO Description --------------------
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
  if (originalDescription && originalDescription.length > 20) seoDescription += `${originalDescription.substring(0, 150)}... `;
  if (features.length > 0) seoDescription += `This product is ${features.join(', ')}. `;
  seoDescription += `Order now for fast delivery. Shop with confidence at LandOfEssentials - your trusted online retailer for quality products.`;

  const keywords = [];
  if (title.toLowerCase().includes('halloween')) keywords.push('halloween costumes', 'party supplies', 'trick or treat');
  if (title.toLowerCase().includes('game')) keywords.push('family games', 'entertainment', 'fun activities');
  if (title.toLowerCase().includes('decoration')) keywords.push('home decor', 'decorative items', 'interior design');
  if (title.toLowerCase().includes('toy')) keywords.push('children toys', 'educational toys', 'safe toys');
  if (keywords.length > 0) seoDescription += ` Perfect for: ${keywords.join(', ')}.`;

  return seoDescription;
}

// -------------------- Process Apify Products --------------------
function processApifyProducts(apifyData) {
  return apifyData.map((item, index) => {
    const handle = extractHandleFromCanonicalUrl(item, index);
    if (!handle) return null;

    let price = 0;
    if (item.variants && item.variants.length > 0) {
      const variant = item.variants[0];
      if (variant.price) price = typeof variant.price === 'object' ? parseFloat(variant.price.current || 0) : parseFloat(variant.price);
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
        if (item.medias[i] && item.medias[i].url) images.push(item.medias[i].url);
      }
    }

    let inventory = 10;
    if (item.variants && item.variants[0] && item.variants[0].price && item.variants[0].price.stockStatus) {
      inventory = item.variants[0].price.stockStatus === 'IN_STOCK' ? 10 : 0;
    }

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

// -------------------- Product Creation --------------------
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

    const batchSize = 5;
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
          await new Promise(resolve => setTimeout(resolve, 5000));
        } catch (error) {
          errors++;
          addLog(`Failed to create ${product.title}: ${error.message}`, 'error');

          if (error.response?.status === 429) {
            addLog('Rate limit hit - waiting 30 seconds', 'warning');
            await new Promise(resolve => setTimeout(resolve, 30000));
          }
        }
      }

      if (i + batchSize < newProducts.length) {
        addLog(`Completed batch ${Math.floor(i/batchSize) + 1}, waiting before next batch...`, 'info');
        await new Promise(resolve => setTimeout(resolve, 10000));
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

// -------------------- Inventory Update --------------------
async function updateInventory() {
  addLog('Starting inventory update...', 'info');
  try {
    const [apifyData, shopifyData] = await Promise.all([getApifyProducts(), getShopifyProducts()]);
    const shopifyMap = new Map(shopifyData.map(p => [p.handle, p]));
    let updates = 0;

    for (const item of apifyData) {
      const handle = extractHandleFromCanonicalUrl(item, 0);
      const shopifyProduct = shopifyMap.get(handle);
      if (!shopifyProduct) continue;

      const currentInventory = shopifyProduct.variants?.[0]?.inventory_quantity || 0;
      const desiredInventory = item.variants && item.variants[0] && item.variants[0].price?.stockStatus === 'IN_STOCK' ? 10 : 0;

      if (currentInventory !== desiredInventory) {
        try {
          await shopifyClient.post('/inventory_levels/set.json', {
            location_id: config.shopify.locationId,
            inventory_item_id: shopifyProduct.variants[0].inventory_item_id,
            available: desiredInventory
          });
          updates++;
          addLog(`Updated inventory: ${handle} (${currentInventory} → ${desiredInventory})`, 'success');
        } catch (err) {
          stats.errors++;
          addLog(`Inventory update failed for ${handle}: ${err.message}`, 'error');
        }
      }
    }

    stats.inventoryUpdates += updates;
    stats.lastSync = new Date().toISOString();
    addLog(`Inventory update complete: ${updates} updated`, 'info');
  } catch (error) {
    addLog(`Inventory update workflow failed: ${error.message}`, 'error');
  }
}

// -------------------- Discontinued Products --------------------
async function checkDiscontinued() {
  addLog('Checking for discontinued products...', 'info');
  try {
    const [apifyData, shopifyData] = await Promise.all([getApifyProducts(), getShopifyProducts()]);
    const apifyHandles = new Set(apifyData.map((p, idx) => extractHandleFromCanonicalUrl(p, idx)));
    let discontinuedCount = 0;

    for (const product of shopifyData) {
      if (!apifyHandles.has(product.handle)) {
        try {
          await shopifyClient.delete(`/products/${product.id}.json`);
          discontinuedCount++;
          addLog(`Deleted discontinued product: ${product.handle}`, 'success');
        } catch (err) {
          stats.errors++;
          addLog(`Failed to delete discontinued product ${product.handle}: ${err.message}`, 'error');
        }
      }
    }

    stats.discontinued += discontinuedCount;
    stats.lastSync = new Date().toISOString();
    addLog(`Discontinued check complete: ${discontinuedCount} deleted`, 'info');
  } catch (error) {
    addLog(`Discontinued check failed: ${error.message}`, 'error');
  }
}

// -------------------- Cron Jobs --------------------
cron.schedule('*/30 * * * *', async () => {
  addLog('Starting scheduled full sync (products, inventory, discontinued)...', 'info');
  await createNewProducts();
  await updateInventory();
  await checkDiscontinued();
  addLog('Scheduled sync completed', 'info');
});

// -------------------- Dashboard Routes --------------------
app.get('/dashboard', (req, res) => {
  res.json({ stats, logs });
});

app.post('/pause', (req, res) => {
  systemPaused = true;
  addLog('System paused', 'warning');
  res.json({ message: 'System paused' });
});

app.post('/resume', (req, res) => {
  systemPaused = false;
  addLog('System resumed', 'success');
  res.json({ message: 'System resumed' });
});

// -------------------- Start Server --------------------
app.listen(PORT, () => {
  addLog(`Server started on port ${PORT}`, 'info');
});
