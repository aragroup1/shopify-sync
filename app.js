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

function normalizeForMatching(text = '') {
    if (typeof text !== 'string') return '';
    return text
        .toLowerCase()
        .replace(/\s*KATEX_INLINE_OPEN.*?KATEX_INLINE_CLOSE\s*/g, ' ')       // Removes content in parentheses
        .replace(/\s*```math[\s\S]*?```\s*/g, ' ') // Removes content in brackets (single and multi-line)
        .replace(/-(parcel|large-letter|letter)-rate$/i, '')
        .replace(/-p\d+$/i, '')
        .replace(/\b(a|an|the|of|in|on|at|to|for|with|by)\b/g, '')
        .replace(/[^a-z0-9]+/g, ' ')
        .replace(/\s+/g, ' ')
        .trim();
}

function calculateRetailPrice(supplierCostString) { const cost = parseFloat(supplierCostString); if (isNaN(cost) || cost < 0) return '0.00'; let finalPrice; if (cost <= 1) finalPrice = cost + 5.5; else if (cost <= 2) finalPrice = cost + 5.95; else if (cost <= 3) finalPrice = cost + 6.99; else if (cost <= 5) finalPrice = cost * 3.2; else if (cost <= 7) finalPrice = cost * 2.5; else if (cost <= 9) finalPrice = cost * 2.2; else if (cost <= 12) finalPrice = cost * 2; else if (cost <= 20) finalPrice = cost * 1.9; else finalPrice = cost * 1.8; return finalPrice.toFixed(2); }
function processApifyProducts(apifyData, { processPrice = true } = {}) { return apifyData.map(item => { if (!item || !item.title) return null; const handle = normalizeForMatching(item.handle || item.title).replace(/ /g, '-'); let inventory = item.variants?.[0]?.stockQuantity || item.stock || 20; if (String(item.variants?.[0]?.price?.stockStatus || item.stockStatus || item.availability || '').toLowerCase().includes('out')) inventory = 0; let sku = item.sku || item.variants?.[0]?.sku || ''; let price = '0.00'; if (item.variants?.[0]?.price?.value) { price = String(item.variants[0].price.value); if (processPrice) price = calculateRetailPrice(price); } const body_html = item.description || item.bodyHtml || ''; const images = item.images ? item.images.map(img => ({ src: img.src || img.url })).filter(img => img.src) : []; return { handle, title: item.title, inventory, sku, price, body_html, images, normalizedTitle: normalizeForMatching(item.title) }; }).filter(p => p && p.sku); }
function matchShopifyProductBySku(apifyProduct, skuMap) { const product = skuMap.get(apifyProduct.sku.toLowerCase()); return product ? { product, matchType: 'sku' } : { product: null, matchType: 'none' }; }

// --- CORE JOB LOGIC ---
async function improvedMapSkusJob(token) { 
    addLog('--- Starting Comprehensive SKU Mapping Job ---', 'warning'); 
    let updated = 0, errors = 0, alreadyMatched = 0, noMatch = 0;
    let matchedByHandle = 0, matchedByTitle = 0, matchedByFuzzy = 0;
    
    try { 
        const [apifyData, allShopifyProducts] = await Promise.all([getApifyProducts(), getShopifyProducts()]); 
        if (shouldAbort(token)) return;

        // --- THIS IS THE FIX ---
        const supplierShopifyProducts = allShopifyProducts.filter(p => p.tags && p.tags.includes(SUPPLIER_TAG));
        addLog(`Found ${supplierShopifyProducts.length} Shopify products with tag '${SUPPLIER_TAG}' to process for SKU mapping.`, 'info');

        const apifyProcessed = processApifyProducts(apifyData, { processPrice: false }); 
        const usedShopifyIds = new Set();
        const apifySkuMap = new Map();
        
        for (const apifyProd of apifyProcessed) {
            apifySkuMap.set(apifyProd.sku.toLowerCase(), apifyProd);
        }
        
        for (const shopifyProd of supplierShopifyProducts) {
            const variant = shopifyProd.variants?.[0];
            if (!variant) continue;
            
            const shopifySku = variant.sku?.toLowerCase();
            if (shopifySku && apifySkuMap.has(shopifySku)) {
                alreadyMatched++;
                usedShopifyIds.add(shopifyProd.id);
            }
        }
        
        addLog(`Found ${alreadyMatched} products already correctly mapped by SKU`, 'info');
        
        const toProcess = supplierShopifyProducts.filter(p => !usedShopifyIds.has(p.id));
        addLog(`Processing ${toProcess.length} products that need SKU mapping...`, 'info');
        
        const updateCandidates = [];
        
        for (const shopifyProd of toProcess) {
            if (shouldAbort(token)) break;
            
            const shopifyNormalizedTitle = normalizeForMatching(shopifyProd.title);
            const shopifyNormalizedHandle = normalizeForMatching(shopifyProd.handle);
            let bestMatch = null;
            let matchType = '';
            
            for (const apifyProd of apifyProcessed) {
                const apifyNormalizedHandle = normalizeForMatching(apifyProd.handle);
                if (shopifyNormalizedHandle === apifyNormalizedHandle || shopifyProd.handle.toLowerCase() === apifyProd.handle.toLowerCase()) {
                    bestMatch = apifyProd;
                    matchType = 'handle';
                    break;
                }
            }
            
            if (!bestMatch) {
                for (const apifyProd of apifyProcessed) {
                    if (shopifyNormalizedTitle === apifyProd.normalizedTitle) {
                        bestMatch = apifyProd;
                        matchType = 'title';
                        break;
                    }
                }
            }
            
            if (!bestMatch) {
                let maxOverlap = 70;
                let fuzzyCandidate = null;
                for (const apifyProd of apifyProcessed) {
                    const overlap = getWordOverlap(shopifyNormalizedTitle, apifyProd.normalizedTitle);
                    if (overlap > maxOverlap) {
                        maxOverlap = overlap;
                        fuzzyCandidate = apifyProd;
                    }
                }
                if (fuzzyCandidate) {
                    bestMatch = fuzzyCandidate;
                    matchType = `fuzzy-${Math.round(maxOverlap)}%`;
                }
            }
            
            if (bestMatch) {
                if(matchType === 'handle') matchedByHandle++;
                if(matchType === 'title') matchedByTitle++;
                if(matchType.startsWith('fuzzy')) matchedByFuzzy++;

                const shopifyVariant = shopifyProd.variants?.[0];
                if (!shopifyVariant) continue;
                
                if (shopifyVariant.sku?.toLowerCase() !== bestMatch.sku.toLowerCase()) {
                    updateCandidates.push({
                        shopifyId: shopifyProd.id,
                        variantId: shopifyVariant.id,
                        oldSku: shopifyVariant.sku || '(none)',
                        newSku: bestMatch.sku,
                        title: shopifyProd.title,
                        matchType
                    });
                }
            } else {
                noMatch++;
            }
        }
        
        addLog(`SKU mapping summary:`, 'info');
        addLog(`- Already matched: ${alreadyMatched}`, 'info');
        addLog(`- To be matched by handle: ${matchedByHandle}`, 'info');
        addLog(`- To be matched by title: ${matchedByTitle}`, 'info');
        addLog(`- To be matched by fuzzy: ${matchedByFuzzy}`, 'info');
        addLog(`- Total to update: ${updateCandidates.length}`, 'info');
        addLog(`- No match found: ${noMatch}`, 'info');
        
        if (updateCandidates.length > 0) {
            addLog(`Examples of SKUs to update:`, 'warning');
            for (let i = 0; i < Math.min(5, updateCandidates.length); i++) {
                const item = updateCandidates[i];
                addLog(`  - [${item.matchType}] "${item.title}": ${item.oldSku} -> ${item.newSku}`, 'warning');
            }
            
            for (const item of updateCandidates) {
                if (shouldAbort(token)) break;
                
                try {
                    await shopifyClient.put(`/variants/${item.variantId}.json`, { variant: { id: item.variantId, sku: item.newSku } });
                    updated++;
                    addLog(`Updated SKU for "${item.title}" from ${item.oldSku} to ${item.newSku} (${item.matchType})`, 'success');
                } catch (e) {
                    errors++;
                    addLog(`Error updating SKU for "${item.title}": ${e.message}`, 'error', e);
                }
                await new Promise(r => setTimeout(r, 500));
            }
        }
        
        addLog(`SKU mapping complete: Updated ${updated}, Errors ${errors}, Already matched ${alreadyMatched}, No match ${noMatch}`, 'success');
        
    } catch (e) {
        addLog(`Critical error in SKU mapping job: ${e.message}`, 'error', e);
        errors++;
    }
  
    lastRun.mapSkus = { at: new Date().toISOString(), updated, errors, alreadyMatched, noMatch, matchedByHandle, matchedByTitle, matchedByFuzzy };
}

async function updateInventoryJob(token) {
    addLog('Starting inventory sync (SKU-only)...', 'info');
    let updated = 0, errors = 0, inSync = 0, notFound = 0;
    const notFoundItems = [];
    
    try {
        const [apifyData, allShopifyProducts] = await Promise.all([ getApifyProducts(), getShopifyProducts({ fields: 'id,title,variants,tags,status' }) ]);
        if (shouldAbort(token)) return;

        const supplierShopifyProducts = allShopifyProducts.filter(p => p.tags && p.tags.includes(SUPPLIER_TAG));
        addLog(`Found ${supplierShopifyProducts.length} Shopify products with tag '${SUPPLIER_TAG}' to process for inventory.`, 'info');

        const skuMap = new Map();
        for (const product of supplierShopifyProducts) {
            const sku = product.variants?.[0]?.sku;
            if (sku) skuMap.set(sku.toLowerCase(), product);
        }
        
        const inventoryItemIds = supplierShopifyProducts.map(p => p.variants?.[0]?.inventory_item_id).filter(id => id);
        const inventoryLevels = await getShopifyInventoryLevels(inventoryItemIds);
        const apifyProcessed = processApifyProducts(apifyData, { processPrice: false });
        const updates = [];
        
        for (const apifyProd of apifyProcessed) {
            const shopifyProd = skuMap.get(apifyProd.sku.toLowerCase());
            
            if (shopifyProd) {
                const variant = shopifyProd.variants?.[0];
                if (!variant?.inventory_item_id) continue;
                
                const currentInventory = inventoryLevels.get(variant.inventory_item_id) ?? 0;
                const targetInventory = apifyProd.inventory;
                
                if (currentInventory === targetInventory) {
                    inSync++;
                } else {
                    updates.push({
                        inventory_item_id: variant.inventory_item_id,
                        available: targetInventory,
                        title: shopifyProd.title,
                        current: currentInventory
                    });
                }
            } else {
                notFound++;
                if (notFoundItems.length < 100) {
                    notFoundItems.push({ title: apifyProd.title, sku: apifyProd.sku, inventory: apifyProd.inventory });
                }
            }
        }
        
        addLog(`Inventory Summary: Updates needed: ${updates.length}, In Sync: ${inSync}, Not Found: ${notFound}`, 'info');
        
        if (notFoundItems.length > 0) {
            addLog(`Sample of items not found (showing ${Math.min(10, notFoundItems.length)} of ${notFound}):`, 'warning');
            for (let i = 0; i < Math.min(10, notFoundItems.length); i++) {
                const item = notFoundItems[i];
                addLog(`  - SKU: ${item.sku}, Title: "${item.title}", Inventory: ${item.inventory}`, 'warning');
            }
            addLog(`Run "Comprehensive SKU Mapping" or "Sync New Products" to resolve these.`, 'info');
        }
        
        if (updates.length > 0) {
            const updatePercentage = supplierShopifyProducts.length > 0 ? (updates.length / supplierShopifyProducts.length) * 100 : 0;
            if (updatePercentage > FAILSAFE_LIMITS.MAX_INVENTORY_UPDATE_PERCENTAGE) {
                const msg = `Inventory update percentage (${updatePercentage.toFixed(1)}%) exceeds failsafe limit (${FAILSAFE_LIMITS.MAX_INVENTORY_UPDATE_PERCENTAGE}%)`;
                addLog(msg, 'error');
                throw new Error(msg);
            }
            
            for (const update of updates) {
                if (shouldAbort(token)) break;
                try {
                    await shopifyClient.post('/inventory_levels/set.json', {
                        inventory_item_id: update.inventory_item_id,
                        location_id: config.shopify.locationId,
                        available: update.available
                    });
                    addLog(`Updated inventory for "${update.title}" from ${update.current} to ${update.available}`, 'info');
                    updated++;
                } catch (e) {
                    errors++;
                    addLog(`Error updating inventory for "${update.title}": ${e.message}`, 'error', e);
                }
                await new Promise(r => setTimeout(r, 200));
            }
        }
        
        addLog(`Inventory update complete: Updated: ${updated}, Errors: ${errors}, In Sync: ${inSync}, Not Found: ${notFound}`, 'success');
        
    } catch (e) {
        addLog(`Critical error in inventory update job: ${e.message}`, 'error', e);
        errors++;
    }
    
    lastRun.inventory = { at: new Date().toISOString(), updated, errors, inSync, notFound, notFoundSample: notFoundItems.slice(0, 100) };
    stats.inventoryUpdates += updated;
}

async function createNewProductsJob(token) {
    addLog('Starting new product creation (SKU-based check)...', 'info');
    let created = 0, errors = 0;
    
    try {
        const [apifyData, allShopifyProducts] = await Promise.all([ getApifyProducts(), getShopifyProducts() ]);
        if (shouldAbort(token)) return;

        const supplierShopifyProducts = allShopifyProducts.filter(p => p.tags && p.tags.includes(SUPPLIER_TAG));
        
        const apifyProcessed = processApifyProducts(apifyData, { processPrice: true });
        
        const skuMap = new Map();
        for (const product of supplierShopifyProducts) {
            const sku = product.variants?.[0]?.sku;
            if (sku) skuMap.set(sku.toLowerCase(), product);
        }
        
        const toCreate = apifyProcessed.filter(p => !skuMap.has(p.sku.toLowerCase()));
        addLog(`Found ${toCreate.length} new products to create.`, 'info');
        
        if (toCreate.length > 0) {
            const maxToCreate = Math.min(toCreate.length, FAILSAFE_LIMITS.MAX_NEW_PRODUCTS_AT_ONCE, MAX_CREATE_PER_RUN);
            if (toCreate.length > maxToCreate) {
                addLog(`Limiting creation to ${maxToCreate} products out of ${toCreate.length} found.`, 'warning');
            }
            
            const createdItems = [];
            for (let i = 0; i < maxToCreate; i++) {
                if (shouldAbort(token)) break;
                
                const apifyProd = toCreate[i];
                try {
                    const newProduct = { product: { title: apifyProd.title, body_html: apifyProd.body_html || '', vendor: 'Imported', product_type: '', tags: SUPPLIER_TAG, status: 'active', variants: [{ price: apifyProd.price, sku: apifyProd.sku, inventory_management: 'shopify' }], images: apifyProd.images } };
                    const { data } = await shopifyClient.post('/products.json', newProduct);
                    const inventoryItemId = data.product.variants[0].inventory_item_id;
                    
                    await shopifyClient.post('/inventory_levels/set.json', { inventory_item_id: inventoryItemId, location_id: config.shopify.locationId, available: apifyProd.inventory });
                    
                    created++;
                    createdItems.push({ id: data.product.id, title: apifyProd.title, sku: apifyProd.sku });
                    addLog(`Created product: "${apifyProd.title}" with SKU: ${apifyProd.sku}`, 'success');
                } catch (e) {
                    errors++;
                    addLog(`Error creating product "${apifyProd.title}": ${e.message}`, 'error', e);
                }
                await new Promise(r => setTimeout(r, 1000));
            }
            lastRun.products = { at: new Date().toISOString(), created, errors, skipped: toCreate.length - created, createdItems };
        } else {
            addLog('No new products to create.', 'success');
            lastRun.products = { at: new Date().toISOString(), created: 0, errors: 0, skipped: 0, createdItems: [] };
        }
    } catch (e) {
        addLog(`Critical error in product creation job: ${e.message}`, 'error', e);
        errors++;
    }
    stats.newProducts += created;
}

async function handleDiscontinuedProductsJob(token) {
    addLog('Starting discontinued check (SKU-only)...', 'info');
    let discontinued = 0, errors = 0;
    
    try {
        const [apifyData, allShopifyProducts] = await Promise.all([ getApifyProducts(), getShopifyProducts({ fields: 'id,title,variants,tags,status' }) ]);
        if (shouldAbort(token)) return;

        const supplierShopifyProducts = allShopifyProducts.filter(p => p.tags && p.tags.includes(SUPPLIER_TAG));

        const apifySkus = new Set(processApifyProducts(apifyData, { processPrice: false }).map(p => p.sku.toLowerCase()));
        
        const potentialDiscontinued = supplierShopifyProducts.filter(p => {
            const sku = p.variants?.[0]?.sku?.toLowerCase();
            return sku && !apifySkus.has(sku) && p.status === 'active';
        });
        
        addLog(`Found ${potentialDiscontinued.length} potential discontinued products.`, 'info');
        
        if (potentialDiscontinued.length > 0) {
            const discontinuePercentage = supplierShopifyProducts.length > 0 ? (potentialDiscontinued.length / supplierShopifyProducts.length) * 100 : 0;
            if (discontinuePercentage > FAILSAFE_LIMITS.MAX_DISCONTINUE_PERCENTAGE) {
                const msg = `Discontinue percentage (${discontinuePercentage.toFixed(1)}%) exceeds failsafe limit (${FAILSAFE_LIMITS.MAX_DISCONTINUE_PERCENTAGE}%)`;
                addLog(msg, 'error');
                throw new Error(msg);
            }
            
            const discontinuedItems = [];
            for (const product of potentialDiscontinued) {
                if (shouldAbort(token)) break;
                try {
                    const inventoryItemId = product.variants[0].inventory_item_id;
                    await shopifyClient.post('/inventory_levels/set.json', { inventory_item_id: inventoryItemId, location_id: config.shopify.locationId, available: 0 });
                    await shopifyClient.put(`/products/${product.id}.json`, { product: { id: product.id, status: 'draft' } });
                    
                    discontinued++;
                    discontinuedItems.push({ id: product.id, title: product.title, sku: product.variants[0].sku });
                    addLog(`Discontinued product: "${product.title}" with SKU: ${product.variants[0].sku}`, 'success');
                } catch (e) {
                    errors++;
                    addLog(`Error discontinuing product "${product.title}": ${e.message}`, 'error', e);
                }
                await new Promise(r => setTimeout(r, 600));
            }
            lastRun.discontinued = { at: new Date().toISOString(), discontinued, errors, skipped: potentialDiscontinued.length - discontinued, discontinuedItems };
        } else {
            addLog('No products to discontinue.', 'success');
            lastRun.discontinued = { at: new Date().toISOString(), discontinued: 0, errors: 0, skipped: 0, discontinuedItems: [] };
        }
    } catch (e) {
        addLog(`Critical error in discontinued products job: ${e.message}`, 'error', e);
        errors++;
    }
    stats.discontinued += discontinued;
}

// --- UI AND API ---
app.get('/', (req, res) => { res.send(`
    <!DOCTYPE html>
    <html>
    <head>
      <title>Shopify Sync</title>
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0-alpha1/dist/css/bootstrap.min.css" rel="stylesheet">
      <style> body { padding: 20px; } pre { background: #f8f9fa; padding: 15px; border-radius: 5px; } .status-badge { font-size: 1.2em; padding: 5px 10px; } .log-entry { margin-bottom: 5px; border-bottom: 1px solid #eee; padding-bottom: 5px; } .log-error { color: #dc3545; } .log-warning { color: #ffc107; } .log-success { color: #198754; } </style>
    </head>
    <body>
      <div class="container-fluid">
        <h1>Shopify Sync Dashboard</h1>
        <div class="row mb-4">
          <div class="col-md-6"><div class="card">
            <div class="card-header d-flex justify-content-between align-items-center">
              <h5 class="mb-0">System Status</h5>
              <span class="badge ${systemPaused ? 'bg-warning' : (failsafeTriggered ? 'bg-danger' : 'bg-success')} status-badge">${systemPaused ? 'PAUSED' : (failsafeTriggered ? 'FAILSAFE' : 'RUNNING')}</span>
            </div>
            <div class="card-body">
              <div class="row"><div class="col-6"><p><strong>New Products:</strong> ${stats.newProducts}</p><p><strong>Inventory Updates:</strong> ${stats.inventoryUpdates}</p></div><div class="col-6"><p><strong>Discontinued:</strong> ${stats.discontinued}</p><p><strong>Errors:</strong> ${stats.errors}</p></div></div>
              <div class="d-flex gap-2 mt-3">
                <button class="btn ${systemPaused ? 'btn-success' : 'btn-warning'}" onclick="fetch('/api/pause', {method: 'POST'}).then(() => location.reload())">${systemPaused ? 'Resume' : 'Pause'} System</button>
                ${failsafeTriggered ? `<button class="btn btn-danger" onclick="fetch('/api/failsafe/clear', {method: 'POST'}).then(() => location.reload())">Clear Failsafe</button>` : ''}
              </div>
              ${failsafeTriggered ? `<div class="alert alert-danger mt-3"><strong>Failsafe triggered:</strong> ${failsafeReason}</div>` : ''}
            </div>
          </div></div>
          <div class="col-md-6"><div class="card">
            <div class="card-header"><h5 class="mb-0">Manual Actions</h5></div>
            <div class="card-body"><div class="d-flex flex-wrap gap-2">
              <button class="btn btn-warning" onclick="runSync('improved-map-skus')">Comprehensive SKU Mapping</button>
              <button class="btn btn-primary" onclick="runSync('inventory')">Sync Inventory</button>
              <button class="btn btn-primary" onclick="runSync('products')">Sync New Products</button>
              <button class="btn btn-primary" onclick="runSync('discontinued')">Check Discontinued</button>
              <button class="btn btn-secondary" onclick="runSync('deduplicate')">Deduplicate Products</button>
            </div></div>
          </div></div>
        </div>
        <div class="row"><div class="col-md-12"><div class="card">
          <div class="card-header"><h5 class="mb-0">Logs</h5></div>
          <div class="card-body"><div style="max-height: 500px; overflow-y: auto;">
            ${logs.map(log => `<div class="log-entry ${log.type === 'error' ? 'log-error' : (log.type === 'warning' ? 'log-warning' : (log.type === 'success' ? 'log-success' : ''))}"><small>${new Date(log.timestamp).toLocaleString()}</small> <span>${log.message}</span></div>`).join('')}
          </div></div>
        </div></div></div>
      </div>
      <script>
        function runSync(type) { if(confirm('Are you sure you want to run ' + type + '?')) { fetch('/api/sync/' + type, { method: 'POST' }).then(res => res.json()).then(data => { if(data.s === 1) { alert('Job started successfully.'); setTimeout(() => location.reload(), 1000); } else { alert('Job is already running.'); } }).catch(err => alert('Error starting job.')); } }
        setTimeout(() => location.reload(), 30000);
      </script>
    </body>
    </html>
  `);});

app.get('/api/status', (req, res) => res.json({ stats, lastRun, logs, systemPaused, failsafeTriggered, failsafeReason, errorSummary: Array.from(errorSummary.entries()).map(([msg, count]) => ({msg, count})) }));
app.post('/api/pause', (req, res) => { systemPaused = !systemPaused; addLog(`System ${systemPaused ? 'paused' : 'resumed'} by user`, 'warning'); abortVersion++; res.json({ s: 1 }); });
app.post('/api/failsafe/clear', (req, res) => { failsafeTriggered = false; failsafeReason = ''; pendingFailsafeAction = null; addLog('Failsafe cleared by user', 'warning'); res.json({ s: 1 }); });
app.post('/api/sync/improved-map-skus', (req, res) => startBackgroundJob('mapSkus', 'Comprehensive SKU Mapping', t => improvedMapSkusJob(t)) ? res.json({s: 1}) : res.status(409).json({s: 0, msg: 'Job already running'}));
app.post('/api/sync/:type', (req, res) => {
  const jobs = { inventory: updateInventoryJob, products: createNewProductsJob, discontinued: handleDiscontinuedProductsJob, deduplicate: deduplicateProductsJob };
  const { type } = req.params;
  if (!jobs[type]) return res.status(400).json({s: 0, msg: 'Invalid job type'});
  startBackgroundJob(type, `Manual ${type} sync`, t => jobs[type](t)) ? res.json({s: 1}) : res.status(409).json({s: 0, msg: 'Job already running'});
});

// Scheduled jobs & Server Start
cron.schedule('0 1 * * *', () => { if (!systemPaused && !failsafeTriggered) startBackgroundJob('inventory', 'Scheduled inventory sync', t => updateInventoryJob(t)); });
cron.schedule('0 2 * * 5', () => { if (!systemPaused && !failsafeTriggered) startBackgroundJob('products', 'Scheduled Product Sync', t => createNewProductsJob(t)); });
cron.schedule('0 3 * * *', () => { if (!systemPaused && !failsafeTriggered) startBackgroundJob('discontinued', 'Scheduled Discontinued Check', t => handleDiscontinuedProductsJob(t)); });
const server = app.listen(PORT, () => {
  addLog(`Server started on port ${PORT}`, 'success');
  const missing = ['APIFY_TOKEN', 'SHOPIFY_ACCESS_TOKEN', 'SHOPIFY_DOMAIN', 'SHOPIFY_LOCATION_ID'].filter(key => !process.env[key]);
  if (missing.length > 0) { addLog(`FATAL: Missing environment variables: ${missing.join(', ')}`, 'error'); process.exit(1); }
});
process.on('SIGTERM', () => { addLog('SIGTERM received...', 'warning'); server.close(() => process.exit(0)); });
process.on('SIGINT', () => { addLog('SIGINT received...', 'warning'); server.close(() => process.exit(0)); });
