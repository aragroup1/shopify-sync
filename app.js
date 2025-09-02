const express = require('express');
const axios = require('axios');
const cron = require('node-cron');

const app = express();
const PORT = process.env.PORT || 3000;
app.use(express.json());

// In-memory storage
let stats = { newProducts: 0, inventoryUpdates: 0, discontinued: 0, errors: 0 };
let lastRun = { inventory: {}, products: { createdItems: [] }, discontinued: { discontinuedItems: [] }, deduplicate: {}, mapSkus: {} };
let logs = [];
let systemPaused = false;
let failsafeTriggered = false;
let failsafeReason = '';
let pendingFailsafeAction = null;
const missingCounters = new Map();
let errorSummary = new Map();

const jobLocks = { inventory: false, products: false, discontinued: false, mapSkus: false, deduplicate: false };
let abortVersion = 0;
const getJobToken = () => abortVersion;
const shouldAbort = (token) => systemPaused || failsafeTriggered || token !== abortVersion;

// Configuration
const FAILSAFE_LIMITS = { MAX_INVENTORY_UPDATE_PERCENTAGE: Number(process.env.MAX_INVENTORY_UPDATE_PERCENTAGE || 5), MAX_DISCONTINUE_PERCENTAGE: Number(process.env.MAX_DISCONTINUE_PERCENTAGE || 5), MAX_NEW_PRODUCTS_AT_ONCE: Number(process.env.MAX_NEW_PRODUCTS_AT_ONCE || 100), FETCH_TIMEOUT: Number(process.env.FETCH_TIMEOUT || 300000) };
const DISCONTINUE_MISS_RUNS = Number(process.env.DISCONTINUE_MISS_RUNS || 3);
const MAX_CREATE_PER_RUN = Number(process.env.MAX_CREATE_PER_RUN || 200);
const SUPPLIER_TAG = process.env.SUPPLIER_TAG || 'Autofacts'; // IMPORTANT: Set this to your supplier's tag
const config = { apify: { token: process.env.APIFY_TOKEN, actorId: process.env.APIFY_ACTOR_ID || 'autofacts~shopify', baseUrl: 'https://api.apify.com/v2' }, shopify: { domain: process.env.SHOPIFY_DOMAIN, accessToken: process.env.SHOPIFY_ACCESS_TOKEN, locationId: process.env.SHOPIFY_LOCATION_ID, baseUrl: `https://${process.env.SHOPIFY_DOMAIN}/admin/api/2024-01` } };
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID;

const apifyClient = axios.create({ baseURL: config.apify.baseUrl, timeout: FAILSAFE_LIMITS.FETCH_TIMEOUT });
const shopifyClient = axios.create({ baseURL: config.shopify.baseUrl, headers: { 'X-Shopify-Access-Token': config.shopify.accessToken, 'Content-Type': 'application/json' }, timeout: FAILSAFE_LIMITS.FETCH_TIMEOUT });

// --- Helper Functions ---
function addLog(message, type = 'info', error = null) {
const log = { timestamp: new Date().toISOString(), message, type };
logs.unshift(log);
if (logs.length > 200) logs.length = 200;
console.log(`[${new Date(log.timestamp).toLocaleTimeString()}] ${message}`);

if (type === 'error') {
stats.errors++;
let normalizedError = (error?.message || message).replace(/"[^"]+"/g, '"{VAR}"').replace(/\b\d{5,}\b/g, '{ID}');
errorSummary.set(normalizedError, (errorSummary.get(normalizedError) || 0) + 1);
}
}
async function notifyTelegram(text) { if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) return; try { await axios.post(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`, { chat_id: TELEGRAM_CHAT_ID, text, parse_mode: 'HTML' }, { timeout: 15000 }); } catch (e) { addLog(`Telegram notify failed: ${e.message}`, 'warning', e); } }
function startBackgroundJob(key, name, fn) { if (jobLocks[key]) { addLog(`${name} already running; ignoring duplicate start`, 'warning'); return false; } jobLocks[key] = true; const token = getJobToken(); addLog(`Started background job: ${name}`, 'info'); setImmediate(async () => { try { await fn(token); } catch (e) { addLog(`Unhandled error in ${name}: ${e.message}\n${e.stack}`, 'error', e); } finally { jobLocks[key] = false; addLog(`${name} job finished`, 'info'); } }); return true; }
function getWordOverlap(str1, str2) { const words1 = new Set(str1.split(' ')); const words2 = new Set(str2.split(' ')); const intersection = new Set([...words1].filter(x => words2.has(x))); return (intersection.size / Math.max(words1.size, words2.size)) * 100; }

// --- Data Fetching & Processing ---
async function getApifyProducts() { let allItems = []; let offset = 0; addLog('Starting Apify product fetch...', 'info'); try { while (true) { const { data } = await apifyClient.get(`/acts/${config.apify.actorId}/runs/last/dataset/items?token=${config.apify.token}&limit=1000&offset=${offset}`); allItems.push(...data); if (data.length < 1000) break; offset += 1000; } } catch (error) { addLog(`Apify fetch error: ${error.message}`, 'error', error); throw error; } addLog(`Apify fetch complete: ${allItems.length} total products.`, 'info'); return allItems; }
async function getShopifyProducts({ fields = 'id,handle,title,variants,tags,status,created_at' } = {}) { let allProducts = []; addLog(`Starting Shopify fetch...`, 'info'); try { let url = `/products.json?limit=250&fields=${fields}`; while (url) { const response = await shopifyClient.get(url); allProducts.push(...response.data.products); const linkHeader = response.headers.link; url = null; if (linkHeader) { const nextLink = linkHeader.split(',').find(s => s.includes('rel="next"')); if (nextLink) { const pageInfoMatch = nextLink.match(/page_info=([^>]+)>/); if (pageInfoMatch) url = `/products.json?limit=250&fields=${fields}&page_info=${pageInfoMatch[1]}`; } } await new Promise(r => setTimeout(r, 500)); } } catch (error) { addLog(`Shopify fetch error: ${error.message}`, 'error', error); throw error; } addLog(`Shopify fetch complete: ${allProducts.length} total products.`, 'info'); return allProducts; }

// Get inventory levels for products
async function getShopifyInventoryLevels(inventoryItemIds) {
  const inventoryMap = new Map();
  if (!inventoryItemIds.length) return inventoryMap;
  
  try {
    const chunks = [];
    for (let i = 0; i < inventoryItemIds.length; i += 50) {
      chunks.push(inventoryItemIds.slice(i, i + 50));
    }
    
    for (const chunk of chunks) {
      const { data } = await shopifyClient.get(`/inventory_levels.json?inventory_item_ids=${chunk.join(',')}&location_ids=${config.shopify.locationId}`);
      for (const level of data.inventory_levels) {
        inventoryMap.set(level.inventory_item_id, level.available || 0);
      }
      await new Promise(r => setTimeout(r, 500));
    }
  } catch (e) {
    addLog(`Error fetching inventory levels: ${e.message}`, 'error', e);
  }
  
  return inventoryMap;
}

// --- COMPLETELY REWRITTEN NORMALIZE FUNCTION ---
function normalizeForMatching(text = '') {
  return String(text)
    .toLowerCase()
    .replace(/\s*KATEX_INLINE_OPEN.*?KATEX_INLINE_CLOSE\s*/g, ' ') // Remove content in parentheses


    .replace(/-(parcel|large-letter|letter)-rate$/i, '')
    .replace(/-p\d+$/i, '')
    .replace(/\b(a|an|the|of|in|on|at|to|for|with|by)\b/g, '')
    .replace(/[^a-z0-9]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function calculateRetailPrice(supplierCostString) { const cost = parseFloat(supplierCostString); if (isNaN(cost) || cost < 0) return '0.00'; let finalPrice; if (cost <= 1) finalPrice = cost + 5.5; else if (cost <= 2) finalPrice = cost + 5.95; else if (cost <= 3) finalPrice = cost + 6.99; else if (cost <= 5) finalPrice = cost * 3.2; else if (cost <= 7) finalPrice = cost * 2.5; else if (cost <= 9) finalPrice = cost * 2.2; else if (cost <= 12) finalPrice = cost * 2; else if (cost <= 20) finalPrice = cost * 1.9; else finalPrice = cost * 1.8; return finalPrice.toFixed(2); }
function processApifyProducts(apifyData, { processPrice = true } = {}) { return apifyData.map(item => { if (!item || !item.title) return null; const handle = normalizeForMatching(item.handle || item.title).replace(/ /g, '-'); let inventory = item.variants?.[0]?.stockQuantity || item.stock || 20; if (String(item.variants?.[0]?.price?.stockStatus || item.stockStatus || item.availability || '').toLowerCase().includes('out')) inventory = 0; let sku = item.sku || item.variants?.[0]?.sku || ''; let price = '0.00'; if (item.variants?.[0]?.price?.value) { price = String(item.variants[0].price.value); if (processPrice) price = calculateRetailPrice(price); } const body_html = item.description || item.bodyHtml || ''; const images = item.images ? item.images.map(img => ({ src: img.src || img.url })).filter(img => img.src) : []; return { handle, title: item.title, inventory, sku, price, body_html, images, normalizedTitle: normalizeForMatching(item.title) }; }).filter(p => p && p.sku); }
function buildShopifyMaps(shopifyData) { const skuMap = new Map(); const strippedHandleMap = new Map(); const normalizedTitleMap = new Map(); for (const product of shopifyData) { const sku = product.variants?.[0]?.sku; if (sku) skuMap.set(sku.toLowerCase(), product); strippedHandleMap.set(normalizeForMatching(product.handle).replace(/ /g, '-'), product); normalizedTitleMap.set(normalizeForMatching(product.title), product); } return { skuMap, strippedHandleMap, normalizedTitleMap, all: shopifyData }; }
function findBestMatch(apifyProduct, shopifyMaps) { if (shopifyMaps.skuMap.has(apifyProduct.sku.toLowerCase())) return { product: shopifyMaps.skuMap.get(apifyProduct.sku.toLowerCase()), matchType: 'sku' }; if (shopifyMaps.strippedHandleMap.has(apifyProduct.handle)) return { product: shopifyMaps.strippedHandleMap.get(apifyProduct.handle), matchType: 'handle' }; if (shopifyMaps.normalizedTitleMap.has(apifyProduct.normalizedTitle)) return { product: shopifyMaps.normalizedTitleMap.get(apifyProduct.normalizedTitle), matchType: 'title' }; let bestTitleMatch = null; let maxOverlap = 60; for (const shopifyProduct of shopifyMaps.all) { const overlap = getWordOverlap(apifyProduct.normalizedTitle, normalizeForMatching(shopifyProduct.title)); if (overlap > maxOverlap) { maxOverlap = overlap; bestTitleMatch = shopifyProduct; } } if (bestTitleMatch) return { product: bestTitleMatch, matchType: `title-fuzzy-${Math.round(maxOverlap)}%` }; return { product: null, matchType: 'none' }; }
function matchShopifyProductBySku(apifyProduct, skuMap) { const product = skuMap.get(apifyProduct.sku.toLowerCase()); return product ? { product, matchType: 'sku' } : { product: null, matchType: 'none' }; }

// --- CORE JOB LOGIC ---
async function deduplicateProductsJob(token) { 
  addLog('--- Starting One-Time Duplicate Cleanup Job ---', 'warning'); 
  let deletedCount = 0, errors = 0; 
  try { 
    const allShopifyProducts = await getShopifyProducts(); 
    if (shouldAbort(token)) return; 
    
    const productsByTitle = new Map(); 
    for (const product of allShopifyProducts) { 
      const normalized = normalizeForMatching(product.title); 
      if (!productsByTitle.has(normalized)) 
        productsByTitle.set(normalized, []); 
      productsByTitle.get(normalized).push(product); 
    } 
    
    const toDeleteIds = []; 
    for (const products of productsByTitle.values()) { 
      if (products.length > 1) { 
        addLog(`Found ${products.length} duplicates for title: "${products[0].title}"`, 'warning'); 
        products.sort((a, b) => new Date(a.created_at) - new Date(b.created_at)); 
        const toKeep = products.shift(); 
        addLog(` Keeping ORIGINAL product ID ${toKeep.id} (created at ${toKeep.created_at})`, 'info'); 
        
        products.forEach(p => { 
          addLog(` Marking NEWER duplicate for deletion ID ${p.id} (created at ${p.created_at})`, 'info'); 
          toDeleteIds.push(p.id); 
        }); 
      } 
    } 
    
    if (toDeleteIds.length > 0) { 
      addLog(`Preparing to delete ${toDeleteIds.length} newer duplicate products...`, 'warning'); 
      for (const id of toDeleteIds) { 
        if (shouldAbort(token)) break; 
        try { 
          await shopifyClient.delete(`/products/${id}.json`); 
          deletedCount++; 
          addLog(` ✓ Deleted product ID ${id}`, 'success'); 
        } catch (e) { 
          errors++; 
          addLog(` ✗ Error deleting product ID ${id}: ${e.message}`, 'error', e); 
        } 
        await new Promise(r => setTimeout(r, 600)); 
      } 
    } else { 
      addLog('No duplicate products found to delete.', 'success'); 
    } 
  } catch (e) { 
    addLog(`Critical error in deduplication job: ${e.message}`, 'error', e); 
    errors++; 
  } 
  
  lastRun.deduplicate = { at: new Date().toISOString(), deleted: deletedCount, errors }; 
}

async function mapSkusJob(token) { 
  addLog('--- Starting Aggressive SKU Mapping Job ---', 'warning'); 
  let updated = 0, errors = 0, alreadyMatched = 0, noMatch = 0; 
  
  try { 
    const [apifyData, shopifyData] = await Promise.all([getApifyProducts(), getShopifyProducts()]); 
    if (shouldAbort(token)) return;
    
    const apifyProcessed = processApifyProducts(apifyData, { processPrice: false }); 
    const shopifyMaps = buildShopifyMaps(shopifyData); 
    const usedShopifyIds = new Set(); 
    
    for (const apifyProduct of apifyProcessed) { 
      if (shouldAbort(token)) break; 
      
      const { product: shopifyProduct, matchType } = findBestMatch(apifyProduct, shopifyMaps); 
      
      if (shopifyProduct) { 
        if (usedShopifyIds.has(shopifyProduct.id)) { 
          addLog(`Skipping duplicate match for Shopify ID ${shopifyProduct.id}`, 'warning'); 
          continue; 
        } 
        
        const shopifyVariant = shopifyProduct.variants?.[0]; 
        if (!shopifyVariant) continue; 
        
        if (shopifyVariant.sku?.toLowerCase() === apifyProduct.sku.toLowerCase()) { 
          alreadyMatched++; 
        } else { 
          try { 
            addLog(`[${matchType}] Mapping "${shopifyProduct.title}". Old SKU: "${shopifyVariant.sku}" -> New SKU: "${apifyProduct.sku}"`, 'info'); 
            await shopifyClient.put(`/variants/${shopifyVariant.id}.json`, { 
              variant: { id: shopifyVariant.id, sku: apifyProduct.sku } 
            }); 
            updated++; 
          } catch (e) { 
            errors++; 
            addLog(`Error updating SKU for "${shopifyProduct.title}": ${e.message}`, 'error', e); 
          } 
          await new Promise(r => setTimeout(r, 600)); 
        } 
        
        usedShopifyIds.add(shopifyProduct.id); 
      } else { 
        noMatch++; 
      } 
    } 
  } catch (e) { 
    addLog(`Critical error in SKU mapping job: ${e.message}`, 'error', e); 
    errors++; 
  } 
  
  lastRun.mapSkus = { 
    at: new Date().toISOString(), 
    updated, 
    errors, 
    alreadyMatched, 
    noMatch 
  }; 
}

// Add the missing updateInventoryJob
async function updateInventoryJob(token) {
  addLog('Starting inventory sync (SKU-only)...', 'info');
  let updated = 0, errors = 0, inSync = 0, notFound = 0;
  
  try {
    const [apifyData, shopifyData] = await Promise.all([
      getApifyProducts(),
      getShopifyProducts({ fields: 'id,title,variants,tags,status' })
    ]);
    
    if (shouldAbort(token)) return;
    
    // Create SKU map for fast lookups
    const skuMap = new Map();
    for (const product of shopifyData) {
      const sku = product.variants?.[0]?.sku;
      if (sku) skuMap.set(sku.toLowerCase(), product);
    }
    
    // Get inventory item IDs for all products
    const inventoryItemIds = shopifyData
      .map(p => p.variants?.[0]?.inventory_item_id)
      .filter(id => id);
    
    // Get current inventory levels
    const inventoryLevels = await getShopifyInventoryLevels(inventoryItemIds);
    
    // Process Apify products
    const apifyProcessed = processApifyProducts(apifyData, { processPrice: false });
    
    // Prepare inventory updates
    const updates = [];
    
    for (const apifyProd of apifyProcessed) {
      const matchResult = matchShopifyProductBySku(apifyProd, skuMap);
      
      if (matchResult.product) {
        const shopifyProd = matchResult.product;
        const variant = shopifyProd.variants?.[0];
        
        if (!variant?.inventory_item_id) continue;
        
        const currentInventory = inventoryLevels.get(variant.inventory_item_id) ?? 0;
        const targetInventory = apifyProd.inventory;
        
        if (currentInventory === targetInventory) {
          inSync++;
        } else {
          updates.push({
            inventory_item_id: variant.inventory_item_id,
            location_id: config.shopify.locationId,
            available: targetInventory,
            title: shopifyProd.title,
            current: currentInventory
          });
        }
      } else {
        notFound++;
      }
    }
    
    // Summary before updates
    addLog(`Inventory Summary: Updates needed: ${updates.length}, In Sync: ${inSync}, Not Found: ${notFound}`, 'info');
    
    // Apply updates in chunks to avoid rate limits
    if (updates.length > 0) {
      // Failsafe check - don't update too many items at once
      const updatePercentage = (updates.length / shopifyData.length) * 100;
      
      if (updatePercentage > FAILSAFE_LIMITS.MAX_INVENTORY_UPDATE_PERCENTAGE) {
        const msg = `Inventory update percentage (${updatePercentage.toFixed(1)}%) exceeds failsafe limit (${FAILSAFE_LIMITS.MAX_INVENTORY_UPDATE_PERCENTAGE}%)`;
        addLog(msg, 'error');
        throw new Error(msg);
      }
      
      // Process updates in chunks
      const chunks = [];
      for (let i = 0; i < updates.length; i += 50) {
        chunks.push(updates.slice(i, i + 50));
      }
      
      for (const chunk of chunks) {
        if (shouldAbort(token)) break;
        
        for (const update of chunk) {
          try {
            await shopifyClient.post('/inventory_levels/set.json', update);
            addLog(`Updated inventory for "${update.title}" from ${update.current} to ${update.available}`, 'info');
            updated++;
          } catch (e) {
            errors++;
            addLog(`Error updating inventory for "${update.title}": ${e.message}`, 'error', e);
          }
          await new Promise(r => setTimeout(r, 200));
        }
      }
    }
    
    // Final summary
    addLog(`Inventory update complete: Updated: ${updated}, Errors: ${errors}, In Sync: ${inSync}, Not Found: ${notFound}`, 'success');
    
  } catch (e) {
    addLog(`Critical error in inventory update job: ${e.message}`, 'error', e);
    errors++;
  }
  
  lastRun.inventory = {
    at: new Date().toISOString(),
    updated,
    errors,
    inSync,
    notFound
  };
  
  stats.inventoryUpdates += updated;
}

// Add the missing createNewProductsJob
async function createNewProductsJob(token) {
  addLog('Starting new product creation (SKU-based check)...', 'info');
  let created = 0, errors = 0, skipped = 0;
  
  try {
    const [apifyData, shopifyData] = await Promise.all([
      getApifyProducts(),
      getShopifyProducts()
    ]);
    
    if (shouldAbort(token)) return;
    
    // Process Apify products with price calculation
    const apifyProcessed = processApifyProducts(apifyData, { processPrice: true });
    
    // Create SKU map for fast lookups
    const skuMap = new Map();
    for (const product of shopifyData) {
      const sku = product.variants?.[0]?.sku;
      if (sku) skuMap.set(sku.toLowerCase(), product);
    }
    
    // Find products to create (not in Shopify yet)
    const toCreate = apifyProcessed.filter(p => !matchShopifyProductBySku(p, skuMap).product);
    
    addLog(`Found ${toCreate.length} new products to create.`, 'info');
    
    if (toCreate.length > 0) {
      // Failsafe check - don't create too many products at once
      const maxToCreate = Math.min(toCreate.length, FAILSAFE_LIMITS.MAX_NEW_PRODUCTS_AT_ONCE, MAX_CREATE_PER_RUN);
      
      if (toCreate.length > maxToCreate) {
        addLog(`Limiting creation to ${maxToCreate} products out of ${toCreate.length} found.`, 'warning');
      }
      
      // Create products one by one
      const createdItems = [];
      
      for (let i = 0; i < maxToCreate; i++) {
        if (shouldAbort(token)) break;
        
        const apifyProd = toCreate[i];
        
        try {
          // Prepare product data
          const newProduct = {
            product: {
              title: apifyProd.title,
              body_html: apifyProd.body_html || '',
              vendor: 'Imported',
              product_type: '',
              tags: SUPPLIER_TAG,
              status: 'active',
              variants: [{
                price: apifyProd.price,
                sku: apifyProd.sku,
                inventory_management: 'shopify'
              }],
              images: apifyProd.images
            }
          };
          
          // Create product
          const { data } = await shopifyClient.post('/products.json', newProduct);
          const newProductId = data.product.id;
          const inventoryItemId = data.product.variants[0].inventory_item_id;
          
          // Set inventory
          await shopifyClient.post('/inventory_levels/set.json', {
            inventory_item_id: inventoryItemId,
            location_id: config.shopify.locationId,
            available: apifyProd.inventory
          });
          
          created++;
          createdItems.push({
            id: newProductId,
            title: apifyProd.title,
            sku: apifyProd.sku
          });
          
          addLog(`Created product: "${apifyProd.title}" with SKU: ${apifyProd.sku}`, 'success');
          
        } catch (e) {
          errors++;
          addLog(`Error creating product "${apifyProd.title}": ${e.message}`, 'error', e);
        }
        
        await new Promise(r => setTimeout(r, 1000)); // Longer delay for product creation
      }
      
      lastRun.products = {
        at: new Date().toISOString(),
        created,
        errors,
        skipped: toCreate.length - created,
        createdItems
      };
      
    } else {
      addLog('No new products to create.', 'success');
      lastRun.products = {
        at: new Date().toISOString(),
        created: 0,
        errors: 0,
        skipped: 0,
        createdItems: []
      };
    }
    
  } catch (e) {
    addLog(`Critical error in product creation job: ${e.message}`, 'error', e);
    errors++;
    
    lastRun.products = {
      at: new Date().toISOString(),
      created,
      errors,
      skipped,
      createdItems: []
    };
  }
  
  stats.newProducts += created;
}

// Add the missing handleDiscontinuedProductsJob
async function handleDiscontinuedProductsJob(token) {
  addLog('Starting discontinued check (SKU-only)...', 'info');
  let discontinued = 0, errors = 0, skipped = 0;
  
  try {
    const [apifyData, shopifyData] = await Promise.all([
      getApifyProducts(),
      getShopifyProducts({ fields: 'id,title,variants,tags,status' })
    ]);
    
    if (shouldAbort(token)) return;
    
    // Get all SKUs from Apify (currently available products)
    const apifySkus = new Set(
      processApifyProducts(apifyData, { processPrice: false })
        .map(p => p.sku.toLowerCase())
    );
    
    // Find products in Shopify that may be discontinued (not in Apify anymore)
    const potentialDiscontinued = shopifyData.filter(p => {
      const sku = p.variants?.[0]?.sku?.toLowerCase();
      // Only consider products with the supplier tag and that have a SKU
      return sku && 
             p.tags && 
             p.tags.includes(SUPPLIER_TAG) && 
             !apifySkus.has(sku) &&
             p.status === 'active'; // Only consider active products
    });
    
    addLog(`Found ${potentialDiscontinued.length} potential discontinued products.`, 'info');
    
    if (potentialDiscontinued.length > 0) {
      // Failsafe check - don't discontinue too many products at once
      const discontinuePercentage = (potentialDiscontinued.length / shopifyData.length) * 100;
      
      if (discontinuePercentage > FAILSAFE_LIMITS.MAX_DISCONTINUE_PERCENTAGE) {
        const msg = `Discontinue percentage (${discontinuePercentage.toFixed(1)}%) exceeds failsafe limit (${FAILSAFE_LIMITS.MAX_DISCONTINUE_PERCENTAGE}%)`;
        addLog(msg, 'error');
        throw new Error(msg);
      }
      
      // Track products that have been discontinued
      const discontinuedItems = [];
      
      // Process discontinued products in chunks
      const chunks = [];
      for (let i = 0; i < potentialDiscontinued.length; i += 50) {
        chunks.push(potentialDiscontinued.slice(i, i + 50));
      }
      
      for (const chunk of chunks) {
        if (shouldAbort(token)) break;
        
        for (const product of chunk) {
          try {
            // Set inventory to 0
            const inventoryItemId = product.variants[0].inventory_item_id;
            
            await shopifyClient.post('/inventory_levels/set.json', {
              inventory_item_id: inventoryItemId,
              location_id: config.shopify.locationId,
              available: 0
            });
            
            // Update product to draft status
            await shopifyClient.put(`/products/${product.id}.json`, {
              product: {
                id: product.id,
                status: 'draft'
              }
            });
            
            discontinued++;
            discontinuedItems.push({
              id: product.id,
              title: product.title,
              sku: product.variants[0].sku
            });
            
            addLog(`Discontinued product: "${product.title}" with SKU: ${product.variants[0].sku}`, 'success');
            
          } catch (e) {
            errors++;
            addLog(`Error discontinuing product "${product.title}": ${e.message}`, 'error', e);
          }
          
          await new Promise(r => setTimeout(r, 600));
        }
      }
      
      lastRun.discontinued = {
        at: new Date().toISOString(),
        discontinued,
        errors,
        skipped: potentialDiscontinued.length - discontinued,
        discontinuedItems
      };
      
    } else {
      addLog('No products to discontinue.', 'success');
      lastRun.discontinued = {
        at: new Date().toISOString(),
        discontinued: 0,
        errors: 0,
        skipped: 0,
        discontinuedItems: []
      };
    }
    
  } catch (e) {
    addLog(`Critical error in discontinued products job: ${e.message}`, 'error', e);
    errors++;
    
    lastRun.discontinued = {
      at: new Date().toISOString(),
      discontinued,
      errors,
      skipped,
      discontinuedItems: []
    };
  }
  
  stats.discontinued += discontinued;
}

// Error reporting function
async function generateAndSendErrorReport() {
  try {
    if (errorSummary.size === 0) {
      addLog('No errors to report.', 'info');
      return;
    }
    
    const sortedErrors = [...errorSummary.entries()]
      .sort((a, b) => b[1] - a[1])
      .slice(0, 10);
    
    let report = `<b>Weekly Error Report</b>\n\n`;
    report += `Total errors since last report: ${stats.errors}\n\n`;
    report += `<b>Top errors:</b>\n`;
    
    sortedErrors.forEach(([error, count], index) => {
      report += `${index + 1}. <code>${error}</code> (${count} occurrences)\n`;
    });
    
    await notifyTelegram(report);
    errorSummary.clear();
    stats.errors = 0;
    addLog('Error report sent successfully.', 'success');
    
  } catch (e) {
    addLog(`Failed to send error report: ${e.message}`, 'error', e);
  }
}

// --- UI AND API ---
app.get('/', (req, res) => {
  const status = systemPaused ? 'PAUSED' : (failsafeTriggered ? 'FAILSAFE' : 'RUNNING');
  
  res.send(`
    <!DOCTYPE html>
    <html>
    <head>
      <title>Shopify Sync</title>
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0-alpha1/dist/css/bootstrap.min.css" rel="stylesheet">
      <style>
        body { padding: 20px; }
        pre { background: #f8f9fa; padding: 15px; border-radius: 5px; }
        .status-badge { font-size: 1.2em; padding: 5px 10px; }
        .log-entry { margin-bottom: 5px; border-bottom: 1px solid #eee; padding-bottom: 5px; }
        .log-error { color: #dc3545; }
        .log-warning { color: #ffc107; }
        .log-success { color: #198754; }
      </style>
    </head>
    <body>
      <div class="container-fluid">
        <h1>Shopify Sync Dashboard</h1>
        
        <div class="row mb-4">
          <div class="col-md-6">
            <div class="card">
              <div class="card-header d-flex justify-content-between align-items-center">
                <h5 class="mb-0">System Status</h5>
                <span class="badge ${status === 'RUNNING' ? 'bg-success' : (status === 'PAUSED' ? 'bg-warning' : 'bg-danger')} status-badge">${status}</span>
              </div>
              <div class="card-body">
                <div class="row">
                  <div class="col-6">
                    <p><strong>New Products:</strong> ${stats.newProducts}</p>
                    <p><strong>Inventory Updates:</strong> ${stats.inventoryUpdates}</p>
                  </div>
                  <div class="col-6">
                    <p><strong>Discontinued:</strong> ${stats.discontinued}</p>
                    <p><strong>Errors:</strong> ${stats.errors}</p>
                  </div>
                </div>
                
                <div class="d-flex gap-2 mt-3">
                  ${systemPaused ? 
                    `<button class="btn btn-success" onclick="fetch('/api/pause', {method: 'POST', body: JSON.stringify({paused: false}), headers: {'Content-Type': 'application/json'}}).then(() => location.reload())">Resume System</button>` : 
                    `<button class="btn btn-warning" onclick="fetch('/api/pause', {method: 'POST', body: JSON.stringify({paused: true}), headers: {'Content-Type': 'application/json'}}).then(() => location.reload())">Pause System</button>`
                  }
                  
                  ${failsafeTriggered ? 
                    `<button class="btn btn-danger" onclick="fetch('/api/failsafe/clear', {method: 'POST'}).then(() => location.reload())">Clear Failsafe</button>` : 
                    ``
                  }
                </div>
                
                ${failsafeTriggered ? 
                  `<div class="alert alert-danger mt-3">
                    <strong>Failsafe triggered:</strong> ${failsafeReason}
                  </div>` : 
                  ``
                }
              </div>
            </div>
          </div>
          
          <div class="col-md-6">
            <div class="card">
              <div class="card-header">
                <h5 class="mb-0">Manual Actions</h5>
              </div>
              <div class="card-body">
                <div class="d-flex flex-wrap gap-2">
                  <button class="btn btn-primary" onclick="runSync('inventory')">Sync Inventory</button>
                  <button class="btn btn-primary" onclick="runSync('products')">Sync New Products</button>
                  <button class="btn btn-primary" onclick="runSync('discontinued')">Check Discontinued</button>
                  <button class="btn btn-secondary" onclick="runSync('deduplicate')">Find & Delete Duplicates</button>
                  <button class="btn btn-secondary" onclick="runSync('map-skus')">Map SKUs</button>
                </div>
              </div>
            </div>
          </div>
        </div>
        
        <div class="row">
          <div class="col-md-12">
            <div class="card">
              <div class="card-header">
                <h5 class="mb-0">Logs</h5>
              </div>
              <div class="card-body">
                <div style="max-height: 500px; overflow-y: auto;">
                  ${logs.map(log => `
                    <div class="log-entry ${log.type === 'error' ? 'log-error' : (log.type === 'warning' ? 'log-warning' : (log.type === 'success' ? 'log-success' : ''))}">
                      <small>${new Date(log.timestamp).toLocaleString()}</small>
                      <span>${log.message}</span>
                    </div>
                  `).join('')}
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
      
      <script>
        function runSync(type) {
          fetch('/api/sync/' + type, { method: 'POST' })
            .then(response => response.json())
            .then(data => {
              if (data.s === 1) {
                alert('Job started successfully!');
                setTimeout(() => location.reload(), 1000);
              } else {
                alert('Job already running. Try again later.');
              }
            })
            .catch(error => {
              alert('Error: ' + error);
            });
        }
        
        // Auto-refresh page every 30 seconds
        setTimeout(() => location.reload(), 30000);
      </script>
    </body>
    </html>
  `);
});

app.get('/api/status', (req, res) => res.json({ stats, lastRun, logs, systemPaused, failsafeTriggered, failsafeReason, errorSummary: Array.from(errorSummary.entries()).map(([msg, count]) => ({msg, count})) }));

app.post('/api/pause', (req, res) => {
  const { paused } = req.body;
  if (paused === undefined) return res.status(400).json({ s: 0, msg: 'Missing paused parameter' });
  
  systemPaused = Boolean(paused);
  addLog(`System ${systemPaused ? 'paused' : 'resumed'} by user`, 'warning');
  
  if (!systemPaused) {
    // Increment abort version to prevent old jobs from continuing
    abortVersion++;
  }
  
  return res.json({ s: 1 });
});

app.post('/api/failsafe/clear', (req, res) => {
  if (!failsafeTriggered) return res.status(400).json({ s: 0, msg: 'No failsafe triggered' });
  
  addLog('Failsafe cleared by user', 'warning');
  failsafeTriggered = false;
  failsafeReason = '';
  pendingFailsafeAction = null;
  abortVersion++; // Increment to allow new jobs to start
  
  return res.json({ s: 1 });
});

app.post('/api/failsafe/confirm', (req, res) => {
  if (!failsafeTriggered || !pendingFailsafeAction) {
    return res.status(400).json({ s: 0, msg: 'No pending failsafe action' });
  }
  
  const action = pendingFailsafeAction;
  pendingFailsafeAction = null;
  
  addLog('Failsafe action confirmed by user', 'warning');
  
  // Execute the pending action
  action();
  
  return res.json({ s: 1 });
});

app.post('/api/failsafe/abort', (req, res) => {
  if (!failsafeTriggered) {
    return res.status(400).json({ s: 0, msg: 'No failsafe triggered' });
  }
  
  addLog('Failsafe action aborted by user', 'warning');
  failsafeTriggered = false;
  failsafeReason = '';
  pendingFailsafeAction = null;
  
  return res.json({ s: 1 });
});

app.post('/api/sync/deduplicate', (req, res) => {
  return startBackgroundJob('deduplicate', 'Find & Delete Duplicates', t => deduplicateProductsJob(t)) 
    ? res.json({s: 1}) 
    : res.status(409).json({s: 0});
});

app.post('/api/sync/map-skus', (req, res) => {
  return startBackgroundJob('mapSkus', 'Map & Override SKUs', t => mapSkusJob(t)) 
    ? res.json({s: 1}) 
    : res.status(409).json({s: 0});
});

app.post('/api/sync/:type', (req, res) => {
  const jobs = {
    inventory: updateInventoryJob,
    products: createNewProductsJob,
    discontinued: handleDiscontinuedProductsJob
  };
  
  const { type } = req.params;
  
  if (!jobs[type]) {
    return res.status(400).json({s: 0, msg: 'Invalid job type'});
  }
  
  return startBackgroundJob(type, `Manual ${type} sync`, t => jobs[type](t)) 
    ? res.json({s: 1}) 
    : res.status(409).json({s: 0});
});

// Scheduled jobs & Server Start
cron.schedule('0 1 * * *', () => {
  if (!systemPaused && !failsafeTriggered) {
    startBackgroundJob('inventory', 'Scheduled inventory sync', t => updateInventoryJob(t));
  }
});

cron.schedule('0 2 * * 5', () => {
  if (!systemPaused && !failsafeTriggered) {
    startBackgroundJob('products', 'Scheduled Product Sync', t => createNewProductsJob(t));
  }
});

cron.schedule('0 3 * * *', () => {
  if (!systemPaused && !failsafeTriggered) {
    startBackgroundJob('discontinued', 'Scheduled Discontinued Check', t => handleDiscontinuedProductsJob(t));
  }
});

cron.schedule('0 9 * * 0', () => {
  startBackgroundJob('errorReport', 'Weekly Error Report', () => generateAndSendErrorReport());
});

const server = app.listen(PORT, () => {
  addLog(`Server started on port ${PORT}`, 'success');
  
  const missing = ['APIFY_TOKEN', 'SHOPIFY_ACCESS_TOKEN', 'SHOPIFY_DOMAIN', 'SHOPIFY_LOCATION_ID']
    .filter(key => !process.env[key]);
    
  if (missing.length > 0) {
    addLog(`FATAL: Missing environment variables: ${missing.join(', ')}`, 'error');
    process.exit(1);
  }
});

process.on('SIGTERM', () => {
  addLog('SIGTERM received...', 'warning');
  server.close(() => process.exit(0));
});

process.on('SIGINT', () => {
  addLog('SIGINT received...', 'warning');
  server.close(() => process.exit(0));
});
