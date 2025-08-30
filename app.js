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
let mismatches = [];
let failsafeTriggered = false;
let failsafeReason = '';

// Telegram configuration
const TELEGRAM_CONFIG = {
  botToken: process.env.TELEGRAM_BOT_TOKEN || '7937540772:AAE8TV8FHYVogI-F4P4p08hLRqnovBV4O1Q',
  chatId: process.env.TELEGRAM_CHAT_ID || '1596350649',
  enabled: false
};

// Check if Telegram is configured
if (TELEGRAM_CONFIG.botToken && TELEGRAM_CONFIG.chatId) {
  TELEGRAM_CONFIG.enabled = true;
  console.log('Telegram notifications enabled for chat ID:', TELEGRAM_CONFIG.chatId);
}

// Failsafe configuration with higher limits for initial setup
const FAILSAFE_LIMITS = {
  MIN_APIFY_PRODUCTS: 100,
  MIN_SHOPIFY_PRODUCTS: 100,
  MAX_CHANGE_PERCENTAGE: 30,
  MAX_ERROR_RATE: 20,
  MAX_DISCONTINUED_AT_ONCE: 50,
  MAX_NEW_PRODUCTS_AT_ONCE: 500, // Raised for initial setup
  MAX_NEW_PRODUCTS_TOTAL: 7000, // Allow creating up to 7000 products total
  FETCH_TIMEOUT: 300000
};

// Track last known good state
let lastKnownGoodState = {
  apifyCount: 0,
  shopifyCount: 0,
  timestamp: null
};

function addLog(message, type = 'info') {
  const log = { timestamp: new Date().toISOString(), message, type };
  logs.unshift(log);
  if (logs.length > 100) logs = logs.slice(0, 100);
  console.log(`[${log.timestamp}] ${message}`);
}

// Send Telegram notification
async function sendTelegramNotification(message) {
  if (!TELEGRAM_CONFIG.enabled) return;
  
  try {
    const url = `https://api.telegram.org/bot${TELEGRAM_CONFIG.botToken}/sendMessage`;
    await axios.post(url, {
      chat_id: TELEGRAM_CONFIG.chatId,
      text: message,
      parse_mode: 'HTML'
    });
    addLog('Telegram notification sent', 'info');
  } catch (error) {
    addLog(`Failed to send Telegram notification: ${error.message}`, 'warning');
  }
}

// Global failsafe check
function checkGlobalFailsafe() {
  if (failsafeTriggered) {
    addLog('Operation blocked - failsafe is active', 'warning');
    return false;
  }
  return true;
}

// Failsafe check function
function checkFailsafeConditions(context, data = {}) {
  const checks = [];
  
  switch(context) {
    case 'fetch':
      if (data.apifyCount < FAILSAFE_LIMITS.MIN_APIFY_PRODUCTS) {
        checks.push(`Apify products too low: ${data.apifyCount} < ${FAILSAFE_LIMITS.MIN_APIFY_PRODUCTS}`);
      }
      if (data.shopifyCount < FAILSAFE_LIMITS.MIN_SHOPIFY_PRODUCTS) {
        checks.push(`Shopify products too low: ${data.shopifyCount} < ${FAILSAFE_LIMITS.MIN_SHOPIFY_PRODUCTS}`);
      }
      if (lastKnownGoodState.apifyCount > 0) {
        const changePercent = Math.abs((data.apifyCount - lastKnownGoodState.apifyCount) / lastKnownGoodState.apifyCount * 100);
        if (changePercent > FAILSAFE_LIMITS.MAX_CHANGE_PERCENTAGE) {
          checks.push(`Apify product count changed by ${changePercent.toFixed(1)}% (was ${lastKnownGoodState.apifyCount}, now ${data.apifyCount})`);
        }
      }
      break;
      
    case 'inventory':
      if (data.updatesNeeded > data.totalProducts * (FAILSAFE_LIMITS.MAX_CHANGE_PERCENTAGE / 100)) {
        checks.push(`Too many inventory changes: ${data.updatesNeeded} > ${Math.floor(data.totalProducts * (FAILSAFE_LIMITS.MAX_CHANGE_PERCENTAGE / 100))}`);
      }
      if (data.errorRate > FAILSAFE_LIMITS.MAX_ERROR_RATE) {
        checks.push(`Error rate too high: ${data.errorRate.toFixed(1)}% > ${FAILSAFE_LIMITS.MAX_ERROR_RATE}%`);
      }
      break;
      
    case 'discontinued':
      if (data.toDiscontinue > FAILSAFE_LIMITS.MAX_DISCONTINUED_AT_ONCE) {
        checks.push(`Too many products to discontinue: ${data.toDiscontinue} > ${FAILSAFE_LIMITS.MAX_DISCONTINUED_AT_ONCE}`);
      }
      if (data.totalProducts > 0 && data.toDiscontinue > data.totalProducts * 0.1) {
        checks.push(`Attempting to discontinue ${data.toDiscontinue} products (>10% of ${data.totalProducts} total)`);
      }
      break;
      
    case 'products':
      // Check if this is initial setup
      if (data.isInitialSetup) {
        addLog('Initial setup mode - allowing larger product creation', 'info');
        return true;
      }
      
      if (data.toCreate > FAILSAFE_LIMITS.MAX_NEW_PRODUCTS_AT_ONCE) {
        checks.push(`Too many new products to create at once: ${data.toCreate} > ${FAILSAFE_LIMITS.MAX_NEW_PRODUCTS_AT_ONCE}`);
      }
      break;
  }
  
  if (checks.length > 0) {
    failsafeTriggered = true;
    failsafeReason = checks.join('; ');
    systemPaused = true;
    addLog(`⚠️ FAILSAFE TRIGGERED: ${failsafeReason}`, 'error');
    addLog('System automatically paused to prevent potential damage', 'error');
    
    // Send Telegram notification
    const telegramMessage = `🚨 <b>FAILSAFE TRIGGERED</b>\n\n` +
      `<b>Context:</b> ${context}\n` +
      `<b>Reason:</b> ${failsafeReason}\n\n` +
      `System has been automatically paused.\n` +
      `Check the dashboard immediately.`;
    sendTelegramNotification(telegramMessage);
    
    return false;
  }
  
  return true;
}

// Configuration
const config = {
  apify: {
    token: process.env.APIFY_TOKEN,
    actorId: process.env.APIFY_ACTOR_ID || 'autofacts~shopify',
    baseUrl: 'https://api.telegram.org/bot7937540772:AAE8TV8FHYVogI-F4P4p08hLRqnovBV4O1Q/getUpdates',
    urlPrefix: process.env.URL_PREFIX || 'https://www.manchesterwholesale.co.uk/products/'
  },
  shopify: {
    domain: process.env.SHOPIFY_DOMAIN,
    accessToken: process.env.SHOPIFY_ACCESS_TOKEN,
    locationId: process.env.SHOPIFY_LOCATION_ID,
    baseUrl: `https://${process.env.SHOPIFY_DOMAIN}/admin/api/2024-01`
  }
};

// API clients with timeout
const apifyClient = axios.create({ 
  baseURL: config.apify.baseUrl, 
  timeout: FAILSAFE_LIMITS.FETCH_TIMEOUT 
});

const shopifyClient = axios.create({
  baseURL: config.shopify.baseUrl,
  headers: {
    'X-Shopify-Access-Token': config.shopify.accessToken,
    'Content-Type': 'application/json'
  },
  timeout: FAILSAFE_LIMITS.FETCH_TIMEOUT
});

// Helper functions with failsafe
async function getApifyProducts() {
  if (!checkGlobalFailsafe()) {
    throw new Error('Operation blocked by failsafe');
  }
  
  let allItems = [];
  let offset = 0;
  let pageCount = 0;
  const limit = 500;
  
  addLog('Starting Apify product fetch...', 'info');
  
  try {
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
  } catch (error) {
    addLog(`Apify fetch error: ${error.message}`, 'error');
    stats.errors++;
    
    failsafeTriggered = true;
    failsafeReason = `Apify fetch failed: ${error.message}`;
    systemPaused = true;
    addLog('⚠️ FAILSAFE: System paused due to Apify fetch failure', 'error');
    
    sendTelegramNotification(`🚨 <b>FAILSAFE: Apify Fetch Failed</b>\n\n${error.message}\n\nSystem paused.`);
    throw error;
  }
  
  addLog(`Apify fetch complete: ${allItems.length} total products`, 'info');
  
  if (!checkFailsafeConditions('fetch', { apifyCount: allItems.length })) {
    throw new Error('Failsafe triggered: Apify product count anomaly');
  }
  
  return allItems;
}

async function getShopifyProducts() {
  if (!checkGlobalFailsafe()) {
    throw new Error('Operation blocked by failsafe');
  }
  
  let allProducts = [];
  let sinceId = null;
  let pageCount = 0;
  const limit = 250;
  const fields = 'id,handle,title,variants,tags';
  
  addLog('Starting Shopify product fetch...', 'info');
  
  try {
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
      await new Promise(resolve => setTimeout(resolve, 500));
    }
  } catch (error) {
    addLog(`Shopify fetch error: ${error.message}`, 'error');
    stats.errors++;
    
    failsafeTriggered = true;
    failsafeReason = `Shopify fetch failed: ${error.message}`;
    systemPaused = true;
    addLog('⚠️ FAILSAFE: System paused due to Shopify fetch failure', 'error');
    
    sendTelegramNotification(`🚨 <b>FAILSAFE: Shopify Fetch Failed</b>\n\n${error.message}\n\nSystem paused.`);
    throw error;
  }
  
  const filteredProducts = allProducts.filter(p => p.tags && p.tags.includes('Supplier:Apify'));
  addLog(`Shopify fetch complete: ${allProducts.length} total products, ${filteredProducts.length} with Supplier:Apify tag`, 'info');
  
  if (!checkFailsafeConditions('fetch', { shopifyCount: filteredProducts.length })) {
    throw new Error('Failsafe triggered: Shopify product count anomaly');
  }
  
  return filteredProducts;
}

function normalizeHandle(input, index, isTitle = false) {
  let handle = input || '';
  
  if (!isTitle && handle && handle !== 'undefined' && handle !== 'null') {
    handle = handle.replace(config.apify.urlPrefix, '')
      .replace(/\.html$/, '')
      .replace(/\s*KATEX_INLINE_OPEN.*?KATEX_INLINE_CLOSE\s*/g, '')
      .replace(/[^a-z0-9-]+/g, '-')
      .replace(/-+/g, '-')
      .replace(/^-|-$/g, '')
      .toLowerCase();
    
    if (handle !== input && handle.length > 0) {
      if (index < 5) addLog(`Handle from URL: "${input}" → "${handle}"`, 'info');
      return handle;
    }
  }
  
  let baseText = (isTitle ? input : `product-${index}`);
  
  if (/^\d+$/.test(baseText.trim()) || baseText.trim().length < 3) {
    baseText = `product-${baseText.trim()}`;
  }
  
  handle = baseText.toLowerCase()
    .replace(/\b\d{4}\b/g, '')
    .replace(/\s*(large letter rate|parcel rate|big parcel rate|letter rate)\s*/gi, '')
    .replace(/\s*KATEX_INLINE_OPEN.*?KATEX_INLINE_CLOSE\s*/g, '')
    .replace(/[^a-z0-9-]+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '');
  
  if (!handle || handle === '-' || handle.length < 2) {
    handle = `product-${index}-${Date.now()}`;
  }
  
  if (index < 5) addLog(`Generated handle: "${input}" → "${handle}"`, 'info');
  
  return handle;
}

function extractHandleFromCanonicalUrl(item, index) {
  const urlFields = [item.canonicalUrl, item.url, item.productUrl, item.source?.url];
  const validUrl = urlFields.find(url => url && url !== 'undefined' && url !== 'null');
  
  if (index < 5) {
    addLog(`Debug product ${index}: canonicalUrl="${item.canonicalUrl}", url="${item.url}", productUrl="${item.productUrl}", title="${item.title}", sku="${item.sku}"`, 'info');
  }
  
  if (validUrl) {
    const handle = normalizeHandle(validUrl, index, false);
    if (handle && handle.length > 2) {
      return handle;
    }
  }
  
  let titleForHandle = item.title || `product-${index}`;
  titleForHandle = titleForHandle
    .replace(/\s*KATEX_INLINE_OPEN.*?(rate|Rate)KATEX_INLINE_CLOSE\s*$/gi, '')
    .replace(/\s*KATEX_INLINE_OPEN.*?KATEX_INLINE_CLOSE\s*$/g, '')
    .trim();
  
  const handle = normalizeHandle(titleForHandle, index, true);
  
  if (!handle || handle.length < 2) {
    return `product-${index}-${Date.now()}`;
  }
  
  return handle;
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

function processApifyProducts(apifyData, options = { processPrice: true }) {
  return apifyData.map((item, index) => {
    const handle = extractHandleFromCanonicalUrl(item, index);
    if (!handle) {
      addLog(`Failed to generate handle for ${item.title || 'unknown'}`, 'error');
      stats.errors++;
      return null;
    }

    let cleanTitle = item.title || 'Untitled Product';
    cleanTitle = cleanTitle.replace(/\b\d{4}\b/g, '')
      .replace(/\s*(large letter rate|parcel rate|big parcel rate|letter rate)\s*/gi, '')
      .replace(/\s*KATEX_INLINE_OPEN.*?KATEX_INLINE_CLOSE\s*/g, '')
      .replace(/\s+/g, ' ')
      .trim();

    let inventory = item.variants?.[0]?.stockQuantity || item.stock || 20;
    const stockStatus = item.variants?.[0]?.price?.stockStatus;
    if (stockStatus === 'OUT_OF_STOCK' || stockStatus === 'OutOfStock') {
      inventory = 0;
    } else if (stockStatus === 'IN_STOCK' && inventory === 0) {
      inventory = 20;
    }

    if (index < 5) {
      addLog(`Inventory debug for ${cleanTitle}: stockQuantity=${item.variants?.[0]?.stockQuantity || 'N/A'}, stock=${item.stock || 'N/A'}, stockStatus=${stockStatus || 'N/A'}, final=${inventory}`, 'info');
    }

    let price = 0;
    let finalPrice = 0;
    let compareAtPrice = 0;
    
    if (options.processPrice) {
      if (item.variants && item.variants.length > 0) {
        const variant = item.variants[0];
        if (variant.price) {
          price = typeof variant.price === 'object' ? parseFloat(variant.price.current || 0) : parseFloat(variant.price);
        }
      }
      if (price === 0) price = parseFloat(item.price || 0);
      
      const originalPrice = price;
      
      if (price > 0 && price < 10 && price.toString().includes('.')) {
        if (index < 5) {
          addLog(`Price appears to be in pounds already: ${price}`, 'info');
        }
      } 
      else if (price > 50 && !price.toString().includes('.')) {
        price = price / 100;
        if (index < 5) {
          addLog(`Price converted from pence: ${originalPrice} → ${price}`, 'info');
        }
      }
      else if (price > 1000) {
        price = price / 100;
        if (index < 5) {
          addLog(`Price converted from pence (high value): ${originalPrice} → ${price}`, 'info');
        }
      }
      
      if (price <= 1) finalPrice = price + 6;
      else if (price <= 2) finalPrice = price + 7;
      else if (price <= 3) finalPrice = price + 8;
      else if (price <= 5) finalPrice = price + 9;
      else if (price <= 8) finalPrice = price + 10;
      else if (price <= 15) finalPrice = price * 2;
      else if (price <= 30) finalPrice = price * 2.2;
      else if (price <= 50) finalPrice = price * 1.8;
      else if (price <= 75) finalPrice = price * 1.6;
      else finalPrice = price * 1.4;

      if (finalPrice === 0 || finalPrice < 5) { 
        addLog(`Minimum price applied for ${item.title}: ${price.toFixed(2)} → 15.00`, 'warning');
        price = 5.00; 
        finalPrice = 15.00; 
      }
      
      compareAtPrice = (finalPrice * 1.2).toFixed(2);

      if (index < 5) {
        addLog(`Price debug for ${cleanTitle}: original=${originalPrice}, base=${price.toFixed(2)}, final=${finalPrice.toFixed(2)}`, 'info');
      }
    }

    const images = [];
    if (item.medias && Array.isArray(item.medias)) {
      for (let i = 0; i < Math.min(3, item.medias.length); i++) {
        if (item.medias[i] && item.medias[i].url) {
          images.push(item.medias[i].url);
        }
      }
    }

    const productData = {
      handle, 
      title: cleanTitle,
      description: item.description || `${cleanTitle}\n\nHigh-quality product from LandOfEssentials.`,
      sku: item.sku || '', 
      originalPrice: price.toFixed(2), 
      price: finalPrice.toFixed(2),
      compareAtPrice,
      inventory, 
      images, 
      vendor: 'LandOfEssentials'
    };

    if (options.processPrice) {
      productData.seoDescription = generateSEODescription(productData);
    }

    return productData;
  }).filter(Boolean);
}

async function enableInventoryTracking(productId, variantId, inventoryItemId) {
  try {
    const updateData = {
      variant: {
        id: variantId,
        inventory_management: 'shopify',
        inventory_policy: 'deny'
      }
    };
    
    await shopifyClient.put(`/variants/${variantId}.json`, updateData);
    addLog(`Enabled inventory tracking for variant ${variantId}`, 'success');
    return true;
  } catch (error) {
    addLog(`Failed to enable inventory tracking for variant ${variantId}: ${error.message}`, 'error');
    return false;
  }
}

async function createNewProducts(productsToCreate) {
  if (!checkGlobalFailsafe()) {
    return { created: 0, errors: 0, total: productsToCreate.length, blocked: true };
  }
  
  if (systemPaused) {
    addLog('Product creation skipped - system is paused', 'warning');
    return { created: 0, errors: 0, total: productsToCreate.length };
  }

  // Check if this is initial setup
  const shopifyData = await getShopifyProducts();
  const existingApifyProducts = shopifyData.filter(p => p.tags && p.tags.includes('Supplier:Apify')).length;
  const isInitialSetup = existingApifyProducts < 1000;
  
  if (isInitialSetup) {
    addLog(`Initial setup detected: ${existingApifyProducts} existing Apify products in Shopify`, 'info');
    addLog(`Total products to create: ${productsToCreate.length}`, 'info');
    
    // Limit batch size but don't trigger failsafe
    const batchLimit = FAILSAFE_LIMITS.MAX_NEW_PRODUCTS_AT_ONCE;
    if (productsToCreate.length > batchLimit) {
      productsToCreate = productsToCreate.slice(0, batchLimit);
      addLog(`Processing batch of ${batchLimit} products (run multiple times to complete)`, 'info');
      
      sendTelegramNotification(
        `📦 <b>Initial Setup Progress</b>\n\n` +
        `Creating batch of ${batchLimit} products.\n` +
        `Existing Apify products: ${existingApifyProducts}\n` +
        `Run "Create New Products" multiple times to complete setup.`
      );
    }
  } else {
    // Normal failsafe check
    if (!checkFailsafeConditions('products', { 
      toCreate: productsToCreate.length, 
      existingCount: shopifyData.length,
      isInitialSetup: false
    })) {
      return { created: 0, errors: 0, total: productsToCreate.length };
    }
  }

  let created = 0, errors = 0;
  const batchSize = 5;
  
  for (let i = 0; i < productsToCreate.length; i += batchSize) {
    if (!checkGlobalFailsafe()) {
      addLog('Product creation stopped - failsafe triggered', 'error');
      break;
    }
    
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
            inventory_policy: 'deny',
            inventory_quantity: product.inventory,
            fulfillment_service: 'manual',
            requires_shipping: true
          }]
        };

        const response = await shopifyClient.post('/products.json', { product: shopifyProduct });
        const createdProduct = response.data.product;
        addLog(`Created: ${product.title} (£${product.price}) with inventory tracking enabled`, 'success');

        for (const imageUrl of product.images) {
          try {
            await shopifyClient.post(`/products/${createdProduct.id}/images.json`, { image: { src: imageUrl } });
            await new Promise(resolve => setTimeout(resolve, 1000));
          } catch (imageError) {
            addLog(`Image failed for ${product.title}: ${imageError.message}`, 'warning');
          }
        }

        if (createdProduct.variants && createdProduct.variants[0] && createdProduct.variants[0].inventory_item_id) {
          try {
            await shopifyClient.post('/inventory_levels/connect.json', {
              location_id: parseInt(config.shopify.locationId),
              inventory_item_id: createdProduct.variants[0].inventory_item_id
            }).catch(() => {});
            
            await shopifyClient.post('/inventory_levels/set.json', {
              location_id: parseInt(config.shopify.locationId),
              inventory_item_id: createdProduct.variants[0].inventory_item_id,
              available: product.inventory
            });
            addLog(`Set inventory for ${product.title}: ${product.inventory} units`, 'info');
          } catch (invError) {
            addLog(`Inventory set failed for ${product.title}: ${invError.message}`, 'warning');
          }
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
  
  if (created > 0) {
    sendTelegramNotification(
      `✅ <b>Products Created</b>\n\n` +
      `Successfully created: ${created} products\n` +
      `Errors: ${errors}\n` +
      `Total processed: ${productsToCreate.length}`
    );
  }
  
  return { created, errors, total: productsToCreate.length };
}

async function handleDiscontinuedProducts() {
  if (!checkGlobalFailsafe()) {
    return { discontinued: 0, errors: 0, total: 0, blocked: true };
  }
  
  let discontinued = 0, errors = 0;
  try {
    addLog('Checking for discontinued products...', 'info');
    const [apifyData, shopifyData] = await Promise.all([getApifyProducts(), getShopifyProducts()]);
    
    const processedApifyProducts = processApifyProducts(apifyData, { processPrice: false });
    
    const currentApifyHandles = new Set();
    processedApifyProducts.forEach(p => {
      currentApifyHandles.add(p.handle.toLowerCase());
      const withoutNumbers = p.handle.replace(/-\d+$/, '');
      if (withoutNumbers !== p.handle) {
        currentApifyHandles.add(withoutNumbers.toLowerCase());
      }
    });
    
    const apifyTitles = new Set();
    processedApifyProducts.forEach(p => {
      const cleanTitle = p.title.toLowerCase()
        .replace(/\b\d{4}\b/g, '')
        .replace(/\s+/g, ' ')
        .trim();
      apifyTitles.add(cleanTitle);
    });
    
    const discontinuedProducts = shopifyData.filter(shopifyProduct => {
      if (!shopifyProduct.tags || !shopifyProduct.tags.includes('Supplier:Apify')) {
        return false;
      }
      
      if (currentApifyHandles.has(shopifyProduct.handle.toLowerCase())) {
        return false;
      }
      
      const shopifyCleanTitle = shopifyProduct.title.toLowerCase()
        .replace(/\b\d{4}\b/g, '')
        .replace(/\s+/g, ' ')
        .trim();
      
      if (apifyTitles.has(shopifyCleanTitle)) {
        return false;
      }
      
      return true;
    });
    
    addLog(`Found ${discontinuedProducts.length} potentially discontinued products`, 'info');
    
    if (!checkFailsafeConditions('discontinued', { 
      toDiscontinue: discontinuedProducts.length,
      totalProducts: shopifyData.length 
    })) {
      return { discontinued: 0, errors: 0, total: discontinuedProducts.length };
    }
    
    if (discontinuedProducts.length > 0 && discontinuedProducts.length <= 10) {
      addLog('Products to be discontinued:', 'warning');
      discontinuedProducts.slice(0, 5).forEach(p => {
        addLog(`  - ${p.title} (${p.handle})`, 'warning');
      });
    }
    
    for (const product of discontinuedProducts) {
      if (!checkGlobalFailsafe()) {
        addLog('Discontinuation stopped - failsafe triggered', 'error');
        break;
      }
      
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
  if (!checkGlobalFailsafe()) {
    return { updated: 0, created: 0, errors: 0, total: 0, blocked: true };
  }
  
  if (systemPaused) {
    addLog('Inventory update skipped - system is paused', 'warning');
    return { updated: 0, created: 0, errors: 0, total: 0 };
  }

  let updated = 0, errors = 0, trackingEnabled = 0;
  mismatches = [];
  
  try {
    addLog('=== STARTING INVENTORY UPDATE WORKFLOW ===', 'info');
    const [apifyData, shopifyData] = await Promise.all([getApifyProducts(), getShopifyProducts()]);
    
    if (apifyData.length > FAILSAFE_LIMITS.MIN_APIFY_PRODUCTS && 
        shopifyData.length > FAILSAFE_LIMITS.MIN_SHOPIFY_PRODUCTS) {
      lastKnownGoodState = {
        apifyCount: apifyData.length,
        shopifyCount: shopifyData.length,
        timestamp: new Date().toISOString()
      };
    }
    
    addLog(`Data comparison: ${apifyData.length} Apify products vs ${shopifyData.length} Shopify products`, 'info');
    
    const processedProducts = processApifyProducts(apifyData, { processPrice: false });
    addLog(`Processed ${processedProducts.length} valid Apify products after filtering`, 'info');
    
    const shopifyHandleMap = new Map(shopifyData.map(p => [p.handle.toLowerCase(), p]));
    const shopifyTitleMap = new Map();
    const shopifySkuMap = new Map();
    
    shopifyData.forEach(product => {
      const cleanTitle = product.title.toLowerCase()
        .replace(/\b\d{4}\b/g, '')
        .replace(/\s*(large letter rate|parcel rate|big parcel rate|letter rate)\s*/gi, '')
        .replace(/\s*KATEX_INLINE_OPEN.*?KATEX_INLINE_CLOSE\s*/g, '')
        .replace(/[^a-z0-9]+/g, ' ')
        .trim();
      shopifyTitleMap.set(cleanTitle, product);
      
      if (product.variants && product.variants[0] && product.variants[0].sku) {
        shopifySkuMap.set(product.variants[0].sku.toLowerCase(), product);
      }
      
      const altHandle = product.handle.replace(/-\d+$/, '');
      if (altHandle !== product.handle) {
        shopifyHandleMap.set(altHandle.toLowerCase(), product);
      }
    });
    
    addLog(`Created Shopify lookup maps: ${shopifyHandleMap.size} handles, ${shopifyTitleMap.size} titles, ${shopifySkuMap.size} SKUs`, 'info');
    
    let matchedCount = 0;
    let matchedByHandle = 0;
    let matchedByTitle = 0;
    let matchedBySku = 0;
    let matchedByPartial = 0;
    let skippedNoVariants = 0;
    let skippedSameInventory = 0;
    let skippedUnreliableZero = 0;
    let skippedNoInventoryItemId = 0;
    const inventoryUpdates = [];
    const debugSample = [];
    
    processedProducts.forEach((apifyProduct, index) => {
      let shopifyProduct = null;
      let matchType = '';
      
      shopifyProduct = shopifyHandleMap.get(apifyProduct.handle.toLowerCase());
      if (shopifyProduct) {
        matchType = 'handle';
        matchedByHandle++;
      }
      
      if (!shopifyProduct && apifyProduct.sku) {
        shopifyProduct = shopifySkuMap.get(apifyProduct.sku.toLowerCase());
        if (shopifyProduct) {
          matchType = 'SKU';
          matchedBySku++;
        }
      }
      
      if (!shopifyProduct) {
        const cleanApifyTitle = apifyProduct.title.toLowerCase()
          .replace(/\b\d{4}\b/g, '')
          .replace(/\s*(large letter rate|parcel rate|big parcel rate|letter rate)\s*/gi, '')
          .replace(/\s*KATEX_INLINE_OPEN.*?KATEX_INLINE_CLOSE\s*/g, '')
          .replace(/[^a-z0-9]+/g, ' ')
          .trim();
        shopifyProduct = shopifyTitleMap.get(cleanApifyTitle);
        if (shopifyProduct) {
          matchType = 'title';
          matchedByTitle++;
        }
      }
      
      if (!shopifyProduct) {
        const handleParts = apifyProduct.handle.split('-').filter(p => p.length > 3);
        for (const [shopifyHandle, product] of shopifyHandleMap) {
          if (handleParts.length > 2) {
            const matchingParts = handleParts.filter(part => shopifyHandle.includes(part));
            if (matchingParts.length >= handleParts.length * 0.6) {
              shopifyProduct = product;
              matchType = 'partial-handle';
              matchedByPartial++;
              break;
            }
          }
        }
      }
      
      if (index < 10) {
        debugSample.push({
          apifyTitle: apifyProduct.title,
          apifyHandle: apifyProduct.handle,
          apifySku: apifyProduct.sku,
          matched: !!shopifyProduct,
          matchType: matchType || 'none',
          shopifyHandle: shopifyProduct?.handle || 'N/A'
        });
      }
      
      if (!shopifyProduct) {
        if (mismatches.length < 100) {
          mismatches.push({
            apifyTitle: apifyProduct.title,
            apifyHandle: apifyProduct.handle,
            apifyUrl: apifyProduct.url || apifyProduct.canonicalUrl || 'N/A',
            shopifyHandle: 'NOT FOUND'
          });
        }
        return;
      }
      
      matchedCount++;
      
      if (!shopifyProduct.variants || !shopifyProduct.variants[0]) {
        skippedNoVariants++;
        return;
      }
      
      if (!shopifyProduct.variants[0].inventory_item_id) {
        skippedNoInventoryItemId++;
        if (index < 20) {
          addLog(`No inventory_item_id for: ${shopifyProduct.title}`, 'warning');
        }
        return;
      }
      
      const currentInventory = shopifyProduct.variants[0].inventory_quantity || 0;
      const targetInventory = apifyProduct.inventory;
      
      if (currentInventory === targetInventory) {
        skippedSameInventory++;
        return;
      }
      
      if (targetInventory === 0 && currentInventory > 0 && (!apifyProduct.stockStatus || apifyProduct.stockStatus !== 'OUT_OF_STOCK')) {
        skippedUnreliableZero++;
        return;
      }
      
      inventoryUpdates.push({
        handle: apifyProduct.handle,
        title: shopifyProduct.title,
        currentInventory,
        newInventory: targetInventory,
        inventoryItemId: shopifyProduct.variants[0].inventory_item_id,
        matchType,
        productId: shopifyProduct.id,
        variantId: shopifyProduct.variants[0].id
      });
      
      if (inventoryUpdates.length <= 5) {
        addLog(`Update needed: ${apifyProduct.handle} (${currentInventory} → ${targetInventory}) [matched by ${matchType}]`, 'info');
      }
    });
    
    addLog('=== MATCHING DEBUG SAMPLE ===', 'info');
    debugSample.forEach(item => {
      addLog(`${item.matched ? '✓' : '✗'} ${item.apifyTitle} | Handle: ${item.apifyHandle} | SKU: ${item.apifySku || 'none'} | Match: ${item.matchType} | Shopify: ${item.shopifyHandle}`, item.matched ? 'info' : 'warning');
    });

    addLog(`=== MATCHING ANALYSIS ===`, 'info');
    addLog(`Total matched: ${matchedCount} (${((matchedCount/processedProducts.length)*100).toFixed(1)}%)`, 'info');
    addLog(`- Matched by handle: ${matchedByHandle}`, 'info');
    addLog(`- Matched by SKU: ${matchedBySku}`, 'info');
    addLog(`- Matched by title: ${matchedByTitle}`, 'info');
    addLog(`- Matched by partial: ${matchedByPartial}`, 'info');
    addLog(`Skipped (no variants): ${skippedNoVariants}`, 'info');
    addLog(`Skipped (no inventory_item_id): ${skippedNoInventoryItemId}`, 'info');
    addLog(`Skipped (same inventory): ${skippedSameInventory}`, 'info');
    addLog(`Skipped (unreliable zero): ${skippedUnreliableZero}`, 'info');
    addLog(`Updates needed: ${inventoryUpdates.length}`, 'info');
    addLog(`Mismatches: ${processedProducts.length - matchedCount}`, 'warning');

    const errorRate = errors > 0 ? (errors / inventoryUpdates.length * 100) : 0;
    if (!checkFailsafeConditions('inventory', {
      updatesNeeded: inventoryUpdates.length,
      totalProducts: processedProducts.length,
      errorRate
    })) {
      return { updated: 0, created: 0, errors: 0, total: inventoryUpdates.length };
    }

    if (!config.shopify.locationId) {
      addLog('ERROR: SHOPIFY_LOCATION_ID environment variable is not set!', 'error');
      return { updated: 0, created: 0, errors: 1, total: inventoryUpdates.length };
    }

    const delayBetweenCalls = 600;

    for (const update of inventoryUpdates) {
      if (!checkGlobalFailsafe()) {
        addLog('Inventory update stopped - failsafe triggered', 'error');
        break;
      }
      
      try {
        const variantResponse = await shopifyClient.get(`/products/${update.productId}.json?fields=variants`);
        const variant = variantResponse.data.product.variants[0];
        
        if (!variant.inventory_management || variant.inventory_management === 'blank') {
          addLog(`Enabling inventory tracking for: ${update.title}`, 'info');
          const enabled = await enableInventoryTracking(update.productId, variant.id, update.inventoryItemId);
          
          if (!enabled) {
            errors++;
            stats.errors++;
            continue;
          }
          trackingEnabled++;
          await new Promise(resolve => setTimeout(resolve, 1000));
        }
        
        const checkResponse = await shopifyClient.get(
          `/inventory_levels.json?inventory_item_ids=${update.inventoryItemId}&location_ids=${config.shopify.locationId}`
        ).catch(err => null);
        
        if (!checkResponse || !checkResponse.data.inventory_levels || checkResponse.data.inventory_levels.length === 0) {
          try {
            await shopifyClient.post('/inventory_levels/connect.json', {
              location_id: parseInt(config.shopify.locationId),
              inventory_item_id: update.inventoryItemId
            });
            addLog(`Connected inventory item to location for: ${update.title}`, 'info');
            await new Promise(resolve => setTimeout(resolve, 250));
          } catch (connectErr) {
            // Already connected
          }
        }
        
        await shopifyClient.post('/inventory_levels/set.json', {
          location_id: parseInt(config.shopify.locationId),
          inventory_item_id: update.inventoryItemId,
          available: update.newInventory
        });
        
        addLog(`✓ Updated: ${update.title} (${update.currentInventory} → ${update.newInventory}) [${update.matchType}]`, 'success');
        updated++;
        stats.inventoryUpdates++;
        await new Promise(resolve => setTimeout(resolve, delayBetweenCalls));
      } catch (error) {
        errors++;
        stats.errors++;
        
        if (error.response?.data?.errors?.[0]?.includes('inventory tracking')) {
          addLog(`✗ ${update.title} - Inventory tracking not enabled. Attempting to enable...`, 'warning');
          
          try {
            const productResponse = await shopifyClient.get(`/products/${update.productId}.json`);
            const product = productResponse.data.product;
            if (product.variants && product.variants[0]) {
              await enableInventoryTracking(product.id, product.variants[0].id, product.variants[0].inventory_item_id);
              trackingEnabled++;
              
              await new Promise(resolve => setTimeout(resolve, 1000));
              await shopifyClient.post('/inventory_levels/set.json', {
                location_id: parseInt(config.shopify.locationId),
                inventory_item_id: update.inventoryItemId,
                available: update.newInventory
              });
              
              addLog(`✓ Retry successful: ${update.title} (${update.currentInventory} → ${update.newInventory})`, 'success');
              updated++;
              errors--;
            }
          } catch (retryError) {
            addLog(`✗ Failed to enable tracking and update: ${update.title} - ${retryError.message}`, 'error');
          }
        } else if (error.response) {
          const errorDetails = error.response.data?.errors || error.response.data?.error || error.message;
          addLog(`✗ Failed: ${update.title} - Status ${error.response.status}: ${JSON.stringify(errorDetails)}`, 'error');
          
          if (error.response.status === 422) {
            addLog(`  → Inventory Item ID: ${update.inventoryItemId}, Location ID: ${config.shopify.locationId}`, 'error');
          }
        } else {
          addLog(`✗ Failed: ${update.title} - ${error.message}`, 'error');
        }
        
        if (error.response?.status === 429) {
          addLog('Rate limit hit - waiting 30 seconds', 'warning');
          await new Promise(resolve => setTimeout(resolve, 30000));
        } else {
          await new Promise(resolve => setTimeout(resolve, 1000));
        }
      }
    }

    stats.lastSync = new Date().toISOString();
    addLog(`=== INVENTORY UPDATE COMPLETE ===`, 'info');
    addLog(`Result: ${updated} updated, ${trackingEnabled} tracking enabled, ${errors} errors out of ${inventoryUpdates.length} attempts`, updated > 0 ? 'success' : 'info');
    
    if (errors > inventoryUpdates.length * 0.5) {
      addLog('⚠️ High error rate detected. Common issues:', 'warning');
      addLog('  - Products created without inventory tracking enabled', 'warning');
      addLog('  - Use "Fix Inventory Tracking" button to enable for all products', 'warning');
    }
    
    if (updated > 50) {
      sendTelegramNotification(
        `📊 <b>Inventory Update Complete</b>\n\n` +
        `Updated: ${updated} products\n` +
        `Tracking enabled: ${trackingEnabled}\n` +
        `Errors: ${errors}\n` +
        `Total processed: ${inventoryUpdates.length}`
      );
    }
    
    return { updated, created: 0, errors, total: inventoryUpdates.length, trackingEnabled };
  } catch (error) {
    addLog(`Inventory update workflow failed: ${error.message}`, 'error');
    stats.errors++;
    return { updated, created: 0, errors: errors + 1, total: 0 };
  }
}

// API Routes - Continuing from previous...
app.get('/', (req, res) => {
  res.send(`
<!DOCTYPE html>
<html lang="en" class="dark">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Shopify Sync Dashboard</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <style>
        body {
            background: linear-gradient(to bottom right, #1a1a2e, #16213e);
        }
        .gradient-bg {
            background: linear-gradient(135deg, #ff6e7f 0%, #bfe9ff 100%);
            box-shadow: 0 10px 20px rgba(255, 110, 127, 0.4);
            position: relative;
            overflow: hidden;
        }
        .gradient-bg::before {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            background: linear-gradient(45deg, rgba(255,255,255,0.1), rgba(255,255,255,0.3));
            opacity: 0;
            transition: opacity 0.5s;
        }
        .gradient-bg:hover::before {
            opacity: 1;
        }
        .card-hover {
            transition: all 0.3s ease-in-out;
            background: rgba(31, 41, 55, 0.2);
            backdrop-filter: blur(12px);
            border: 1px solid rgba(255, 255, 255, 0.1);
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.4);
        }
        .card-hover:hover {
            transform: translateY(-8px) scale(1.03);
            box-shadow: 0 12px 40px rgba(0, 0, 0, 0.3);
            border-color: rgba(255, 110, 127, 0.5);
        }
        .btn-hover {
            transition: all 0.3s ease-in-out;
            position: relative;
            overflow: hidden;
            z-index: 1;
        }
        .btn-hover::before {
            content: '';
            position: absolute;
            top: 50%;
            left: 50%;
            width: 0;
            height: 0;
            background: rgba(255, 255, 255, 0.3);
            border-radius: 50%;
            transform: translate(-50%, -50%);
            transition: width 0.6s ease, height 0.6s ease;
            z-index: -1;
        }
        .btn-hover:hover::before {
            width: 300px;
            height: 300px;
        }
        .btn-hover:hover {
            transform: scale(1.1);
            box-shadow: 0 0 20px rgba(255, 110, 127, 0.7);
        }
        .fade-in {
            animation: fadeIn 0.6s ease-in-out;
        }
        @keyframes fadeIn {
            from { opacity: 0; transform: translateY(30px); }
            to { opacity: 1; transform: translateY(0); }
        }
        .spinner {
            display: none;
            border: 4px solid rgba(255, 255, 255, 0.3);
            border-top: 4px solid #ff6e7f;
            border-radius: 50%;
            width: 24px;
            height: 24px;
            animation: spin 1s linear infinite;
            margin-left: 8px;
            display: inline-block;
        }
        .spinner.hidden {
            display: none !important;
        }
        @keyframes spin {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
        }
        .mismatch-table th {
            cursor: pointer;
            transition: all 0.3s ease;
            background: rgba(255, 255, 255, 0.1);
            backdrop-filter: blur(8px);
        }
        .mismatch-table th:hover {
            background: rgba(255, 110, 127, 0.2);
        }
        .mismatch-table tr:nth-child(even) {
            background: rgba(31, 41, 55, 0.1);
        }
        .mismatch-table tr:hover {
            background: rgba(255, 110, 127, 0.2);
        }
        .log-container {
            background: rgba(17, 24, 39, 0.95);
            backdrop-filter: blur(10px);
            box-shadow: 0 0 30px rgba(0, 0, 0, 0.3);
        }
        .log-container:hover {
            box-shadow: 0 0 40px rgba(255, 110, 127, 0.5);
        }
        .failsafe-alert {
            animation: pulse 2s infinite;
        }
        @keyframes pulse {
            0% { transform: scale(1); }
            50% { transform: scale(1.02); }
            100% { transform: scale(1); }
        }
    </style>
</head>
<body class="min-h-screen font-sans">
    <div class="container mx-auto px-4 py-8">
        <div class="relative bg-gray-800 rounded-2xl shadow-lg p-8 mb-8 gradient-bg text-white fade-in">
            <h1 class="text-4xl font-extrabold tracking-tight">Shopify Sync Dashboard</h1>
            <p class="mt-2 text-lg opacity-90">Seamless product synchronization with Apify, optimized for SEO</p>
        </div>

        <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-6 mb-8">
            <div class="rounded-2xl p-6 card-hover fade-in">
                <h3 class="text-lg font-semibold text-blue-400">New Products</h3>
                <p class="text-3xl font-bold text-gray-100" id="newProducts">${stats.newProducts}</p>
            </div>
            <div class="rounded-2xl p-6 card-hover fade-in">
                <h3 class="text-lg font-semibold text-green-400">Inventory Updates</h3>
                <p class="text-3xl font-bold text-gray-100" id="inventoryUpdates">${stats.inventoryUpdates}</p>
            </div>
            <div class="rounded-2xl p-6 card-hover fade-in">
                <h3 class="text-lg font-semibold text-orange-400">Discontinued</h3>
                <p class="text-3xl font-bold text-gray-100" id="discontinued">${stats.discontinued}</p>
            </div>
            <div class="rounded-2xl p-6 card-hover fade-in">
                <h3 class="text-lg font-semibold text-red-400">Errors</h3>
                <p class="text-3xl font-bold text-gray-100" id="errors">${stats.errors}</p>
            </div>
        </div>

        <div class="rounded-2xl p-6 card-hover mb-8 fade-in">
            <h2 class="text-2xl font-semibold text-gray-100 mb-4">System Controls</h2>
            
            ${failsafeTriggered ? `
            <div class="mb-4 p-4 rounded-lg bg-red-900 border-2 border-red-500 failsafe-alert">
                <div class="flex items-center justify-between">
                    <div>
                        <h3 class="font-bold text-red-300">
                            🚨 FAILSAFE TRIGGERED
                        </h3>
                        <p class="text-sm text-red-400">
                            ${failsafeReason}
                        </p>
                    </div>
                    <button onclick="clearFailsafe()" 
                            class="bg-red-500 hover:bg-red-600 text-white px-4 py-2 rounded-lg btn-hover">
                        Clear Failsafe
                    </button>
                </div>
            </div>
            ` : ''}
            
            ${mismatches.length > 100 ? `
            <div class="mb-4 p-4 rounded-lg bg-yellow-900 border border-yellow-700">
                <div class="flex items-center justify-between">
                    <div>
                        <h3 class="font-medium text-yellow-300">
                            ⚠️ ${mismatches.length} Unmatched Products
                        </h3>
                        <p class="text-sm text-yellow-400">
                            These Apify products don't exist in Shopify yet. Create them to sync inventory.
                        </p>
                    </div>
                    <button onclick="triggerSync('mismatches')" 
                            class="bg-yellow-500 hover:bg-yellow-600 text-white px-4 py-2 rounded-lg btn-hover">
                        Create First 50
                    </button>
                </div>
            </div>
            ` : ''}
            
            <div class="mb-6 p-4 rounded-lg ${systemPaused ? 'bg-red-900 border-red-700' : 'bg-green-900 border-green-700'} border">
                <div class="flex items-center justify-between">
                    <div>
                        <h3 class="font-medium ${systemPaused ? 'text-red-300' : 'text-green-300'}" id="systemStatus">
                            System Status: ${systemPaused ? 'PAUSED' : 'ACTIVE'}
                        </h3>
                        <p class="text-sm ${systemPaused ? 'text-red-400' : 'text-green-400'}" id="systemStatusDesc">
                            ${systemPaused ? 'Automatic syncing is disabled' : 'Automatic syncing every 30min (inventory) & 6hrs (products)'}
                        </p>
                    </div>
                    <div class="flex items-center">
                        <button onclick="togglePause()" id="pauseButton"
                                class="${systemPaused ? 'bg-green-500 hover:bg-green-600' : 'bg-red-500 hover:bg-red-600'} text-white px-4 py-2 rounded-lg btn-hover">
                            ${systemPaused ? 'Resume System' : 'Pause System'}
                        </button>
                        <div id="pauseSpinner" class="spinner hidden"></div>
                    </div>
                </div>
            </div>
            
            <div class="flex flex-wrap gap-4">
                <div class="flex items-center">
                    <button onclick="triggerSync('products')" class="bg-blue-500 text-white px-6 py-3 rounded-lg btn-hover">Create New Products</button>
                    <div id="productsSpinner" class="spinner hidden"></div>
                </div>
                <div class="flex items-center">
                    <button onclick="triggerSync('inventory')" class="bg-green-500 text-white px-6 py-3 rounded-lg btn-hover">Update Inventory</button>
                    <div id="inventorySpinner" class="spinner hidden"></div>
                </div>
                <div class="flex items-center">
                    <button onclick="triggerSync('discontinued')" class="bg-orange-500 text-white px-6 py-3 rounded-lg btn-hover">Check Discontinued</button>
                    <div id="discontinuedSpinner" class="spinner hidden"></div>
                </div>
                <div class="flex items-center">
                    <button onclick="fixInventoryTracking()" class="bg-purple-500 text-white px-6 py-3 rounded-lg btn-hover">Fix Inventory Tracking</button>
                    <div id="fixSpinner" class="spinner hidden"></div>
                </div>
                ${mismatches.length > 0 ? `
                <div class="flex items-center">
                    <button onclick="triggerSync('mismatches')" class="bg-yellow-500 text-white px-6 py-3 rounded-lg btn-hover">Create from Mismatches</button>
                    <div id="mismatchesSpinner" class="spinner hidden"></div>
                </div>
                ` : ''}
            </div>
            
            <div class="mt-4 p-4 rounded-lg bg-gray-700">
                <p class="text-sm text-gray-400"><strong>Failsafe Protection Active:</strong></p>
                <ul class="text-xs text-gray-400 mt-2">
                    <li>• Auto-pauses if Apify/Shopify fetch fails</li>
                    <li>• Prevents large unexpected changes (>30%)</li>
                    <li>• Stops if error rate exceeds 20%</li>
                    <li>• Limits bulk discontinuations (max 50)</li>
                    <li>• Initial setup allows up to 500 products per batch</li>
                    ${TELEGRAM_CONFIG.enabled ? '<li>• <span class="text-green-400">✓ Telegram notifications active</span></li>' : '<li>• <span class="text-yellow-400">⚠ Telegram not configured</span></li>'}
                </ul>
            </div>
        </div>

        <div class="rounded-2xl p-6 card-hover mb-8 fade-in">
            <h2 class="text-2xl font-semibold text-gray-100 mb-4">Mismatch Report</h2>
            <div class="overflow-x-auto">
                <table class="mismatch-table w-full text-sm text-left text-gray-400">
                    <thead class="text-xs text-gray-400 uppercase bg-gray-700">
                        <tr>
                            <th class="px-4 py-2" onclick="sortTable(0)">Apify Title</th>
                            <th class="px-4 py-2" onclick="sortTable(1)">Apify Handle</th>
                            <th class="px-4 py-2" onclick="sortTable(2)">Apify URL</th>
                            <th class="px-4 py-2" onclick="sortTable(3)">Shopify Handle</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${mismatches.slice(0, 20).map(mismatch => `
                            <tr class="border-b border-gray-700">
                                <td class="px-4 py-2">${mismatch.apifyTitle || ''}</td>
                                <td class="px-4 py-2">${mismatch.apifyHandle || ''}</td>
                                <td class="px-4 py-2">${mismatch.apifyUrl || ''}</td>
                                <td class="px-4 py-2">${mismatch.shopifyHandle || ''}</td>
                            </tr>
                        `).join('')}
                    </tbody>
                </table>
            </div>
        </div>

        <div class="rounded-2xl p-6 card-hover fade-in log-container">
            <h2 class="text-2xl font-semibold text-gray-100 mb-4">Activity Log</h2>
            <div class="bg-gray-900 rounded-lg p-4 h-96 overflow-y-auto font-mono text-sm" id="logContainer">
                ${logs.map(log => 
                    `<div class="${
                        log.type === 'success' ? 'text-green-400' :
                        log.type === 'error' ? 'text-red-400' :
                        log.type === 'warning' ? 'text-yellow-400' :
                        'text-gray-300'
                    }">[${new Date(log.timestamp).toLocaleTimeString()}] ${log.message}</div>`
                ).join('')}
            </div>
        </div>
    </div>

    <script>
        let systemPaused = ${systemPaused};
        
        async function togglePause() {
            const button = document.getElementById('pauseButton');
            const spinner = document.getElementById('pauseSpinner');
            
            button.disabled = true;
            spinner.classList.remove('hidden');
            
            try {
                const response = await fetch('/api/pause', { method: 'POST' });
                const result = await response.json();
                
                if (result.success) {
                    systemPaused = result.paused;
                    updateSystemStatus();
                    addLogEntry('🔄 System ' + (result.paused ? 'paused' : 'resumed'), 'info');
                } else {
                    addLogEntry('❌ Failed to toggle pause', 'error');
                }
            } catch (error) {
                console.error('Pause toggle error:', error);
                addLogEntry('❌ Failed to toggle pause', 'error');
            } finally {
                button.disabled = false;
                spinner.classList.add('hidden');
            }
        }
        
        function updateSystemStatus() {
            const statusEl = document.getElementById('systemStatus');
            const statusDescEl = document.getElementById('systemStatusDesc');
            const buttonEl = document.getElementById('pauseButton');
            const containerEl = buttonEl.closest('.rounded-lg');
            
            if (systemPaused) {
                statusEl.textContent = 'System Status: PAUSED';
                statusEl.className = 'font-medium text-red-300';
                statusDescEl.textContent = 'Automatic syncing is disabled';
                statusDescEl.className = 'text-sm text-red-400';
                buttonEl.textContent = 'Resume System';
                buttonEl.className = 'bg-green-500 hover:bg-green-600 text-white px-4 py-2 rounded-lg btn-hover';
                containerEl.className = 'mb-6 p-4 rounded-lg bg-red-900 border-red-700 border';
            } else {
                statusEl.textContent = 'System Status: ACTIVE';
                statusEl.className = 'font-medium text-green-300';
                statusDescEl.textContent = 'Automatic syncing every 30min (inventory) & 6hrs (products)';
                statusDescEl.className = 'text-sm text-green-400';
                buttonEl.textContent = 'Pause System';
                buttonEl.className = 'bg-red-500 hover:bg-red-600 text-white px-4 py-2 rounded-lg btn-hover';
                containerEl.className = 'mb-6 p-4 rounded-lg bg-green-900 border-green-700 border';
            }
        }
        
        async function clearFailsafe() {
            try {
                const response = await fetch('/api/failsafe/clear', { method: 'POST' });
                const result = await response.json();
                
                if (result.success) {
                    addLogEntry('✅ Failsafe cleared', 'success');
                    setTimeout(() => location.reload(), 1000);
                }
            } catch (error) {
                console.error('Clear failsafe error:', error);
                addLogEntry('❌ Failed to clear failsafe', 'error');
            }
        }
        
        async function fixInventoryTracking() {
            const button = event.target;
            const spinner = document.getElementById('fixSpinner');
            
            if (!confirm('This will enable inventory tracking for all products that don\\'t have it. Continue?')) {
                return;
            }
            
            button.disabled = true;
            spinner.classList.remove('hidden');
            
            try {
                const response = await fetch('/api/fix/inventory-tracking', { method: 'POST' });
                const result = await response.json();
                
                if (result.success) {
                    addLogEntry('✅ ' + result.message, 'success');
                    setTimeout(() => location.reload(), 2000);
                } else {
                    addLogEntry('❌ Failed to fix inventory tracking', 'error');
                }
            } catch (error) {
                console.error('Fix error:', error);
                addLogEntry('❌ Failed to fix inventory tracking', 'error');
            } finally {
                button.disabled = false;
                spinner.classList.add('hidden');
            }
        }
        
        async function triggerSync(type) {
            const button = event.target;
            const spinner = document.getElementById(type + 'Spinner');
            
            button.disabled = true;
            spinner.classList.remove('hidden');
            
            try {
                const response = await fetch('/api/sync/' + type, { method: 'POST' });
                const result = await response.json();
                
                if (result.success) {
                    addLogEntry('✅ ' + result.message, 'success');
                    if (result.data) {
                        updateStats();
                    }
                } else {
                    addLogEntry('❌ ' + (result.message || 'Sync failed'), 'error');
                }
            } catch (error) {
                console.error('Sync error:', error);
                addLogEntry('❌ Failed to trigger ' + type + ' sync', 'error');
            } finally {
                button.disabled = false;
                spinner.classList.add('hidden');
            }
        }
        
        function addLogEntry(message, type) {
            const logContainer = document.getElementById('logContainer');
            const time = new Date().toLocaleTimeString();
            const color = type === 'success' ? 'text-green-400' : 
                         type === 'error' ? 'text-red-400' : 
                         type === 'warning' ? 'text-yellow-400' : 
                         'text-gray-300';
            const newLog = document.createElement('div');
            newLog.className = color;
            newLog.textContent = '[' + time + '] ' + message;
            logContainer.insertBefore(newLog, logContainer.firstChild);
        }
        
        function sortTable(n) {
            const table = document.querySelector('.mismatch-table tbody');
            const rows = Array.from(table.getElementsByTagName('tr'));
            const isAsc = table.dataset.sortDir !== 'asc';
            table.dataset.sortDir = isAsc ? 'asc' : 'desc';
            
            rows.sort((a, b) => {
                const aText = a.getElementsByTagName('td')[n].textContent;
                const bText = b.getElementsByTagName('td')[n].textContent;
                return isAsc ? aText.localeCompare(bText) : bText.localeCompare(aText);
            });
            
            while (table.firstChild) {
                table.removeChild(table.firstChild);
            }
            rows.forEach(row => table.appendChild(row));
        }
        
        async function updateStats() {
            try {
                const response = await fetch('/api/status');
                const data = await response.json();
                
                document.getElementById('newProducts').textContent = data.newProducts;
                document.getElementById('inventoryUpdates').textContent = data.inventoryUpdates;
                document.getElementById('discontinued').textContent = data.discontinued;
                document.getElementById('errors').textContent = data.errors;
                
                if (data.systemPaused !== systemPaused) {
                    systemPaused = data.systemPaused;
                    updateSystemStatus();
                }
            } catch (error) {
                console.error('Status update error:', error);
            }
        }
        
        // Auto-refresh stats every 30 seconds
        setInterval(updateStats, 30000);
    </script>
</body>
</html>
  `);
});

// Continue with remaining API endpoints...
app.get('/api/status', (req, res) => {
  res.json({
    ...stats,
    systemPaused,
    failsafeTriggered,
    failsafeReason,
    logs: logs.slice(0, 20),
    mismatches: mismatches.slice(0, 20),
    uptime: process.uptime(),
    lastKnownGoodState,
    telegramEnabled: TELEGRAM_CONFIG.enabled,
    environment: {
      apifyToken: config.apify.token ? 'SET' : 'MISSING',
      shopifyToken: config.shopify.accessToken ? 'SET' : 'MISSING',
      shopifyDomain: config.shopify.domain || 'MISSING'
    }
  });
});

app.post('/api/failsafe/clear', (req, res) => {
  failsafeTriggered = false;
  failsafeReason = '';
  addLog('Failsafe cleared manually', 'info');
  sendTelegramNotification('✅ Failsafe has been cleared manually. System can resume operations.');
  res.json({ success: true });
});

app.get('/api/mismatches', (req, res) => {
  res.json({ mismatches });
});

app.get('/api/locations', async (req, res) => {
  try {
    const response = await shopifyClient.get('/locations.json');
    const locations = response.data.locations;
    
    res.json({
      currentLocationId: config.shopify.locationId,
      availableLocations: locations.map(loc => ({
        id: loc.id,
        name: loc.name,
        active: loc.active
      }))
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.get('/api/diagnose-handles', async (req, res) => {
  try {
    addLog('Running handle diagnostic...', 'info');
    
    const apifyData = await getApifyProducts();
    const shopifyData = await getShopifyProducts();
    
    const apifySample = apifyData.slice(0, 20);
    const shopifySample = shopifyData.filter(p => p.tags && p.tags.includes('Supplier:Apify')).slice(0, 20);
    
    const processedApify = processApifyProducts(apifySample, { processPrice: false });
    
    const comparison = processedApify.map(apify => {
      const shopifyMatch = shopifySample.find(s => 
        s.handle.toLowerCase() === apify.handle.toLowerCase() ||
        s.title.toLowerCase().includes(apify.title.toLowerCase().substring(0, 20))
      );
      
      return {
        apifyTitle: apify.title,
        apifyHandle: apify.handle,
        shopifyTitle: shopifyMatch?.title || 'NOT
