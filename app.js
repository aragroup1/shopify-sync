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
        if (item.medias[i] && item.medias[i].url) images.push(item.medias[i].url);
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

    let matchedCount = 0;
    let skippedNoVariants = 0;
    let skippedSameInventory = 0;
    let unmatched = [];
    const inventoryUpdates = [];

    processedProducts.forEach((apifyProduct, index) => {
      const shopifyProduct = shopifyMap.get(apifyProduct.handle);
      if (!shopifyProduct) {
        unmatched.push(apifyProduct.handle);
        return;
      }
      matchedCount++;

      if (!shopifyProduct.variants || !shopifyProduct.variants[0]) {
        skippedNoVariants++;
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
    });

    addLog(`=== MATCHING ANALYSIS ===`, 'info');
    addLog(`Matched products: ${matchedCount}`, 'info');
    addLog(`Skipped (no variants): ${skippedNoVariants}`, 'info');
    addLog(`Skipped (same inventory): ${skippedSameInventory}`, 'info');
    addLog(`Updates needed: ${inventoryUpdates.length}`, 'info');
    addLog(`Total unmatched Apify products: ${unmatched.length}`, 'warning');
    if (unmatched.length > 0) addLog(`First 20 unmatched: ${unmatched.slice(0,20).join(', ')}`, 'warning');

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

// (Routes + other functions unchanged from your original — kept for brevity)
// ... rest of your Express routes and cron jobs ...

app.listen(PORT, () => {
  addLog(`Shopify Sync Service started on port ${PORT}`, 'success');
  addLog('Inventory updates: Every 30 minutes', 'info');
  addLog('Product creation: Every 6 hours', 'info');
  if (!config.apify.token) addLog('WARNING: APIFY_TOKEN not set', 'error');
  if (!config.shopify.accessToken) addLog('WARNING: SHOPIFY_ACCESS_TOKEN not set', 'error');
  if (!config.shopify.domain) addLog('WARNING: SHOPIFY_DOMAIN not set', 'error');
});
