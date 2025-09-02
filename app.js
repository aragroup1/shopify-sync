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
const SUPPLIER_TAG = process.env.SUPPLIER_TAG || 'Supplier:Apify';
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
async function getShopifyProducts({ fields = 'id,handle,title,variants,tags,status,created_at' } = {}) {
    let allProducts = [];
    addLog(`Starting Shopify fetch (including active, draft, & archived)...`, 'info');
    const statuses = ['active', 'draft', 'archived'];
    try {
        for (const status of statuses) {
            let pageInfo = null;
            let hasNextPage = true;
            let count = 0;
            while (hasNextPage) {
                const url = `/products.json?limit=250&fields=${fields}&status=${status}${pageInfo ? `&page_info=${pageInfo}` : ''}`;
                const response = await shopifyClient.get(url);
                allProducts.push(...response.data.products);
                count += response.data.products.length;

                const linkHeader = response.headers.link;
                if (linkHeader) {
                    const nextLink = linkHeader.split(',').find(s => s.includes('rel="next"'));
                    if (nextLink) {
                        const pageInfoMatch = nextLink.match(/page_info=([^>]+)>/);
                        pageInfo = pageInfoMatch ? pageInfoMatch[1] : null;
                    } else {
                        hasNextPage = false;
                    }
                } else {
                    hasNextPage = false;
                }
                await new Promise(r => setTimeout(r, 500));
            }
            addLog(`Fetched ${count} products with status: ${status}`, 'info');
        }
    } catch (error) {
        addLog(`Shopify fetch error: ${error.message}`, 'error', error);
        throw error;
    }
    addLog(`Shopify fetch complete: ${allProducts.length} total products.`, 'info');
    return allProducts;
}
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
  } catch (e) { addLog(`Error fetching inventory levels: ${e.message}`, 'error', e); }
  return inventoryMap;
}

// --- **NEW & IMPROVED** NORMALIZE FUNCTION ---
function normalizeForMatching(text = '') {
    if (typeof text !== 'string') return '';
    return text
        .toLowerCase()
        // Specifically remove noise words found in logs
        .replace(/KATEX_INLINE_OPENparcel rateKATEX_INLINE_CLOSE/g, '')
        .replace(/KATEX_INLINE_OPENlarge letter rateKATEX_INLINE_CLOSE/g, '')
        .replace(/xtra value/g, '')
        .replace(/value pack of \d+/g, '')
        // General cleanup
        .replace(/[^a-z0-9\s]/g, '') // Keep only letters, numbers, and spaces
        .replace(/\s+/g, ' ') // Collapse multiple spaces
        .trim();
}
function calculateRetailPrice(supplierCostString) { const cost = parseFloat(supplierCostString); if (isNaN(cost) || cost < 0) return '0.00'; let finalPrice; if (cost <= 1) finalPrice = cost + 5.5; else if (cost <= 2) finalPrice = cost + 5.95; else if (cost <= 3) finalPrice = cost + 6.99; else if (cost <= 5) finalPrice = cost * 3.2; else if (cost <= 7) finalPrice = cost * 2.5; else if (cost <= 9) finalPrice = cost * 2.2; else if (cost <= 12) finalPrice = cost * 2; else if (cost <= 20) finalPrice = cost * 1.9; else finalPrice = cost * 1.8; return finalPrice.toFixed(2); }
function processApifyProducts(apifyData, { processPrice = true } = {}) { return apifyData.map(item => { if (!item || !item.title) return null; const handle = normalizeForMatching(item.handle || item.title).replace(/ /g, '-'); let inventory = String(item.variants?.[0]?.price?.stockStatus || item.stockStatus || item.availability || '').toLowerCase().includes('out') ? 0 : 20; let sku = item.sku || item.variants?.[0]?.sku || ''; let price = '0.00'; if (item.variants?.[0]?.price?.value) { price = String(item.variants[0].price.value); if (processPrice) price = calculateRetailPrice(price); } const body_html = item.description || item.bodyHtml || ''; const images = item.images ? item.images.map(img => ({ src: img.src || img.url })).filter(img => img.src) : []; return { handle, title: item.title, inventory, sku, price, body_html, images, normalizedTitle: normalizeForMatching(item.title) }; }).filter(p => p && p.sku); }
function matchShopifyProductBySku(apifyProduct, skuMap) { const product = skuMap.get(apifyProduct.sku.toLowerCase()); return product ? { product, matchType: 'sku' } : { product: null, matchType: 'none' }; }

// --- CORE JOB LOGIC ---
// This is your most important job right now. It is designed to fix the SKU mismatches.
async function improvedMapSkusJob(token) {
    addLog(`--- Starting Comprehensive SKU Mapping Job with Debug Info ---`, 'warning');
    let updated = 0, errors = 0, alreadyMatched = 0, noMatch = 0;
    let matchedByHandle = 0, matchedByTitle = 0, matchedByFuzzy = 0;
    let skippedNonSupplier = 0;

    try {
        const [apifyData, shopifyData] = await Promise.all([getApifyProducts(), getShopifyProducts()]);
        if (shouldAbort(token)) return;

        const apifyProcessed = processApifyProducts(apifyData, { processPrice: false });
        const apifySkuMap = new Map();
        for (const apifyProd of apifyProcessed) {
            apifySkuMap.set(apifyProd.sku.toLowerCase(), apifyProd);
        }

        const supplierProducts = shopifyData.filter(p => p.tags && p.tags.includes(SUPPLIER_TAG));
        addLog(`Found ${supplierProducts.length} Shopify products with tag '${SUPPLIER_TAG}' to process.`, 'info');
        skippedNonSupplier = shopifyData.length - supplierProducts.length;

        const usedShopifyIds = new Set();
        const usedApifySkus = new Set();
        for (const shopifyProd of supplierProducts) {
            const shopifySku = shopifyProd.variants?.[0]?.sku?.toLowerCase();
            if (shopifySku && apifySkuMap.has(shopifySku)) {
                alreadyMatched++;
                usedShopifyIds.add(shopifyProd.id);
                usedApifySkus.add(shopifySku);
            }
        }
        addLog(`Found ${alreadyMatched} products already correctly mapped by SKU`, 'info');

        const toProcess = supplierProducts.filter(p => !usedShopifyIds.has(p.id));
        const updateCandidates = [];
        const remainingApifyProducts = apifyProcessed.filter(p => !usedApifySkus.has(p.sku.toLowerCase()));
        addLog(`Processing ${toProcess.length} products that need SKU mapping...`, 'info');

        for (const shopifyProd of toProcess) {
            if (shouldAbort(token)) break;
            const shopifyVariant = shopifyProd.variants?.[0];
            if (!shopifyVariant) continue;

            const shopifyNormalizedTitle = normalizeForMatching(shopifyProd.title);
            const shopifyNormalizedHandle = normalizeForMatching(shopifyProd.handle);
            let bestMatch = null;
            let matchType = '';

            // 1. Try Handle Match
            for (const apifyProd of remainingApifyProducts) {
                if (shopifyNormalizedHandle === normalizeForMatching(apifyProd.handle)) {
                    bestMatch = apifyProd;
                    matchType = 'handle';
                    break;
                }
            }

            // 2. Try Title Match
            if (!bestMatch) {
                for (const apifyProd of remainingApifyProducts) {
                    if (shopifyNormalizedTitle === apifyProd.normalizedTitle) {
                        bestMatch = apifyProd;
                        matchType = 'title';
                        break;
                    }
                }
            }

            // 3. Try Fuzzy Title Match
            if (!bestMatch) {
                let maxOverlap = 65; // Lowered threshold to 65%
                for (const apifyProd of remainingApifyProducts) {
                    const overlap = getWordOverlap(shopifyNormalizedTitle, apifyProd.normalizedTitle);
                    if (overlap > maxOverlap) {
                        maxOverlap = overlap;
                        bestMatch = apifyProd;
                        matchType = `fuzzy-${Math.round(maxOverlap)}%`;
                    }
                }
            }

            if (bestMatch) {
                if (matchType === 'handle') matchedByHandle++;
                if (matchType === 'title') matchedByTitle++;
                if (matchType.startsWith('fuzzy')) matchedByFuzzy++;

                updateCandidates.push({
                    variantId: shopifyVariant.id,
                    oldSku: shopifyVariant.sku || '(none)',
                    newSku: bestMatch.sku,
                    title: shopifyProd.title,
                    matchType
                });
                const index = remainingApifyProducts.findIndex(p => p.sku === bestMatch.sku);
                if (index !== -1) remainingApifyProducts.splice(index, 1);
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
    lastRun.mapSkus = { at: new Date().toISOString(), updated, errors, alreadyMatched, noMatch, matchedByHandle, matchedByTitle, matchedByFuzzy, skippedNonSupplier };
}

// UPDATED: Set products with stock to active status
async function updateInventoryJob(token) {
  addLog('Starting inventory sync...', 'info');
  let updated = 0, errors = 0, inSync = 0, notFound = 0;
  let statusChanged = 0;

  try {
    const [apifyData, shopifyData] = await Promise.all([getApifyProducts(), getShopifyProducts()]);
    if (shouldAbort(token)) return;

    const supplierProducts = shopifyData.filter(p => p.tags && p.tags.includes(SUPPLIER_TAG));
    addLog(`Found ${supplierProducts.length} products with tag '${SUPPLIER_TAG}' out of ${shopifyData.length} total`, 'info');

    const skuMap = new Map();
    for (const product of supplierProducts) {
      const sku = product.variants?.[0]?.sku;
      if (sku) skuMap.set(sku.toLowerCase(), product);
    }
    
    const inventoryItemIds = supplierProducts.map(p => p.variants?.[0]?.inventory_item_id).filter(id => id);
    const inventoryLevels = await getShopifyInventoryLevels(inventoryItemIds);
    const apifyProcessed = processApifyProducts(apifyData, { processPrice: false });
    
    const updates = [];
    const statusUpdates = [];

    for (const apifyProd of apifyProcessed) {
        const shopifyProd = skuMap.get(apifyProd.sku.toLowerCase());
        if (shopifyProd) {
            const variant = shopifyProd.variants?.[0];
            if (!variant?.inventory_item_id) continue;

            const currentInventory = inventoryLevels.get(variant.inventory_item_id) ?? 0;
            const targetInventory = apifyProd.inventory;

            if (currentInventory !== targetInventory) {
                updates.push({ inventory_item_id: variant.inventory_item_id, location_id: config.shopify.locationId, available: targetInventory, title: shopifyProd.title, current: currentInventory });
            } else {
                inSync++;
            }
            if (targetInventory > 0 && shopifyProd.status !== 'active') {
                statusUpdates.push({ id: shopifyProd.id, title: shopifyProd.title, oldStatus: shopifyProd.status, newStatus: 'active' });
            }
        } else {
            notFound++;
        }
    }

    addLog(`Inventory Summary: Updates needed: ${updates.length}, Status changes: ${statusUpdates.length}, In Sync: ${inSync}, Not Found: ${notFound}`, 'info');
    
    if (updates.length > 0) {
      addLog(`Processing ${updates.length} inventory updates...`, 'info');
      for (const update of updates) {
        if (shouldAbort(token)) break;
        try {
            await shopifyClient.post('/inventory_levels/set.json', { inventory_item_id: update.inventory_item_id, location_id: update.location_id, available: update.available });
            updated++;
        } catch (e) { errors++; addLog(`Error updating inventory for "${update.title}": ${e.message}`, 'error', e); }
        await new Promise(r => setTimeout(r, 200));
      }
    }

    if (statusUpdates.length > 0) {
        addLog(`Updating status for ${statusUpdates.length} products...`, 'info');
        for (const update of statusUpdates) {
            if (shouldAbort(token)) break;
            try {
                await shopifyClient.put(`/products/${update.id}.json`, { product: { id: update.id, status: update.newStatus } });
                statusChanged++;
            } catch (e) { errors++; addLog(`Error updating status for "${update.title}": ${e.message}`, 'error', e); }
            await new Promise(r => setTimeout(r, 500));
        }
    }
    addLog(`Inventory update complete: Updated: ${updated}, Status Changed: ${statusChanged}, Errors: ${errors}, In Sync: ${inSync}, Not Found: ${notFound}`, 'success');

  } catch (e) {
    addLog(`Critical error in inventory update job: ${e.message}`, 'error', e);
    errors++;
  }
  
  lastRun.inventory = { at: new Date().toISOString(), updated, statusChanged, errors, inSync, notFound };
  stats.inventoryUpdates += updated;
}

// Add the createNewProductsJob
async function createNewProductsJob(token) {
    addLog('Starting new product creation...', 'info');
    let created = 0, errors = 0;
    try {
        const [apifyData, shopifyData] = await Promise.all([getApifyProducts(), getShopifyProducts()]);
        if (shouldAbort(token)) return;

        const apifyProcessed = processApifyProducts(apifyData);
        const skuMap = new Map();
        for (const product of shopifyData) {
            const sku = product.variants?.[0]?.sku;
            if (sku) skuMap.set(sku.toLowerCase(), product);
        }

        const toCreate = apifyProcessed.filter(p => !skuMap.has(p.sku.toLowerCase()));
        addLog(`Found ${toCreate.length} new products to create.`, 'info');

        if (toCreate.length > 0) {
            const maxToCreate = Math.min(toCreate.length, FAILSAFE_LIMITS.MAX_NEW_PRODUCTS_AT_ONCE, MAX_CREATE_PER_RUN);
            if (toCreate.length > maxToCreate) {
                addLog(`Limiting creation to ${maxToCreate} products.`, 'warning');
            }

            for (let i = 0; i < maxToCreate; i++) {
                if (shouldAbort(token)) break;
                const apifyProd = toCreate[i];
                try {
                    const newProduct = {
                        product: {
                            title: apifyProd.title,
                            body_html: apifyProd.body_html || '',
                            vendor: SUPPLIER_TAG,
                            tags: SUPPLIER_TAG,
                            status: apifyProd.inventory > 0 ? 'active' : 'draft',
                            variants: [{ price: apifyProd.price, sku: apifyProd.sku, inventory_management: 'shopify' }],
                            images: apifyProd.images
                        }
                    };
                    const { data } = await shopifyClient.post('/products.json', newProduct);
                    await shopifyClient.post('/inventory_levels/set.json', { inventory_item_id: data.product.variants[0].inventory_item_id, location_id: config.shopify.locationId, available: apifyProd.inventory });
                    created++;
                    addLog(`Created product: "${apifyProd.title}"`, 'success');
                } catch (e) {
                    errors++;
                    addLog(`Error creating product "${apifyProd.title}": ${e.message}`, 'error', e);
                }
                await new Promise(r => setTimeout(r, 1000));
            }
        }
    } catch (e) { addLog(`Critical error in product creation job: ${e.message}`, 'error', e); errors++; }
    stats.newProducts += created;
    lastRun.products = { at: new Date().toISOString(), created, errors };
}

// Add the handleDiscontinuedProductsJob
async function handleDiscontinuedProductsJob(token) {
    addLog('Starting discontinued check...', 'info');
    let discontinued = 0, errors = 0;
    try {
        const [apifyData, shopifyData] = await Promise.all([getApifyProducts(), getShopifyProducts()]);
        if (shouldAbort(token)) return;

        const apifySkus = new Set(processApifyProducts(apifyData, { processPrice: false }).map(p => p.sku.toLowerCase()));
        const potentialDiscontinued = shopifyData.filter(p => {
            const sku = p.variants?.[0]?.sku?.toLowerCase();
            return sku && p.tags && p.tags.includes(SUPPLIER_TAG) && !apifySkus.has(sku) && p.status === 'active';
        });

        addLog(`Found ${potentialDiscontinued.length} potential discontinued products.`, 'info');
        if (potentialDiscontinued.length > 0) {
            const supplierProductCount = shopifyData.filter(p => p.tags && p.tags.includes(SUPPLIER_TAG)).length;
            const discontinuePercentage = (potentialDiscontinued.length / supplierProductCount) * 100;
            if (discontinuePercentage > FAILSAFE_LIMITS.MAX_DISCONTINUE_PERCENTAGE) {
                throw new Error(`Discontinue percentage (${discontinuePercentage.toFixed(1)}%) exceeds failsafe limit.`);
            }

            for (const product of potentialDiscontinued) {
                if (shouldAbort(token)) break;
                try {
                    await shopifyClient.post('/inventory_levels/set.json', { inventory_item_id: product.variants[0].inventory_item_id, location_id: config.shopify.locationId, available: 0 });
                    await shopifyClient.put(`/products/${product.id}.json`, { product: { id: product.id, status: 'draft' } });
                    discontinued++;
                    addLog(`Discontinued product: "${product.title}"`, 'success');
                } catch (e) {
                    errors++;
                    addLog(`Error discontinuing product "${product.title}": ${e.message}`, 'error', e);
                }
                await new Promise(r => setTimeout(r, 600));
            }
        }
    } catch (e) { addLog(`Critical error in discontinued job: ${e.message}`, 'error', e); errors++; }
    stats.discontinued += discontinued;
    lastRun.discontinued = { at: new Date().toISOString(), discontinued, errors };
}

// --- UI AND API ---
app.get('/', (req, res) => {
  const status = systemPaused ? 'PAUSED' : (failsafeTriggered ? 'FAILSAFE' : 'RUNNING');
  res.send(`
    <!DOCTYPE html>
    <html lang="en">
    <head>
      <title>Shopify Sync Dashboard</title>
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <link rel="preconnect" href="https://fonts.googleapis.com">
      <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
      <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
      <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.1.1/css/all.min.css">
      <style>
        :root {
            --bg-color: #1a1d21;
            --card-bg: #252a30;
            --text-color: #e0e0e0;
            --text-muted: #8c96a5;
            --border-color: #363c45;
            --primary: #5e72e4;
            --success: #2dce89;
            --warning: #fb6340;
            --danger: #f5365c;
            --info: #11cdef;
        }
        body {
            font-family: 'Inter', sans-serif;
            background-color: var(--bg-color);
            color: var(--text-color);
            padding: 2rem;
            margin: 0;
        }
        .container-fluid { max-width: 1400px; }
        .card {
            border: 1px solid var(--border-color);
            border-radius: 12px;
            background-color: var(--card-bg);
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
            margin-bottom: 2rem;
        }
        .card-header {
            background-color: transparent;
            border-bottom: 1px solid var(--border-color);
            padding: 1.25rem 1.5rem;
            font-weight: 600;
        }
        .card-body { padding: 1.5rem; }
        .stat-card {
            text-align: center;
            padding: 1rem;
        }
        .stat-card p {
            margin: 0;
            font-size: 0.9rem;
            color: var(--text-muted);
            font-weight: 500;
            text-transform: uppercase;
        }
        .stat-card h3 {
            font-size: 2.25rem;
            font-weight: 700;
            margin: 0.5rem 0;
            color: var(--text-color);
        }
        .status-badge {
            font-size: 1rem;
            padding: 0.5rem 1rem;
            border-radius: 50px;
            font-weight: 600;
            color: white;
        }
        .bg-running { background-color: var(--success); }
        .bg-paused { background-color: var(--warning); }
        .bg-failsafe { background-color: var(--danger); }
        .btn {
            border-radius: 8px;
            font-weight: 600;
            transition: all 0.2s ease;
            padding: 0.75rem 1.25rem;
        }
        .btn:hover { transform: translateY(-2px); box-shadow: 0 7px 14px rgba(50,50,93,.1),0 3px 6px rgba(0,0,0,.08); }
        .btn-primary { background-color: var(--primary); border-color: var(--primary); }
        .btn-warning { background-color: var(--warning); border-color: var(--warning); }
        .btn-success { background-color: var(--success); border-color: var(--success); }
        .btn-danger { background-color: var(--danger); border-color: var(--danger); }
        .btn-info { background-color: var(--info); border-color: var(--info); }
        .logs-container { max-height: 500px; overflow-y: auto; background-color: var(--bg-color); padding: 1rem; border-radius: 8px; }
        .log-entry { font-family: 'Menlo', 'Consolas', monospace; font-size: 0.85rem; padding: 0.5rem; border-radius: 4px; margin-bottom: 0.25rem; }
        .log-entry small { color: var(--text-muted); margin-right: 1rem; }
        .log-error { background-color: rgba(245, 54, 92, 0.1); color: var(--danger); }
        .log-warning { background-color: rgba(251, 99, 64, 0.1); color: var(--warning); }
        .log-success { background-color: rgba(45, 206, 137, 0.1); color: var(--success); }
        .log-info { color: var(--info); }
      </style>
    </head>
    <body>
      <div class="container-fluid">
        <h1 class="my-4"><i class="fas fa-sync-alt me-2"></i> Shopify Sync Dashboard</h1>
        <div class="row">
          <div class="col-lg-7">
            <div class="card">
              <div class="card-header d-flex justify-content-between align-items-center">
                <h5 class="mb-0"><i class="fas fa-cogs me-2"></i>Manual Actions</h5>
              </div>
              <div class="card-body">
                <div class="d-flex flex-wrap gap-2">
                  <button class="btn btn-warning text-white" onclick="runSync('improved-map-skus')"><i class="fas fa-magic me-1"></i> Comprehensive SKU Mapping</button>
                  <button class="btn btn-primary" onclick="runSync('inventory')"><i class="fas fa-boxes me-1"></i> Sync Inventory</button>
                  <button class="btn btn-primary" onclick="runSync('products')"><i class="fas fa-plus-circle me-1"></i> Sync New Products</button>
                  <button class="btn btn-primary" onclick="runSync('discontinued')"><i class="fas fa-archive me-1"></i> Check Discontinued</button>
                  <button class="btn btn-info" onclick="runSync('deduplicate')"><i class="fas fa-clone me-1"></i> Find & Delete Duplicates</button>
                </div>
              </div>
            </div>
          </div>
          <div class="col-lg-5">
            <div class="card">
              <div class="card-header d-flex justify-content-between align-items-center">
                <h5 class="mb-0"><i class="fas fa-tachometer-alt me-2"></i>System Status</h5>
                <span class="badge ${status.toLowerCase()}">${status}</span>
              </div>
              <div class="card-body">
                <div class="d-flex flex-wrap gap-2">
                    ${systemPaused ? `<button class="btn btn-success" onclick="fetch('/api/pause', {method: 'POST', body: JSON.stringify({paused: false}), headers: {'Content-Type': 'application/json'}}).then(() => location.reload())"><i class="fas fa-play"></i> Resume</button>` : `<button class="btn btn-warning text-white" onclick="fetch('/api/pause', {method: 'POST', body: JSON.stringify({paused: true}), headers: {'Content-Type': 'application/json'}}).then(() => location.reload())"><i class="fas fa-pause"></i> Pause</button>`}
                    ${failsafeTriggered ? `<button class="btn btn-danger" onclick="fetch('/api/failsafe/clear', {method: 'POST'}).then(() => location.reload())"><i class="fas fa-shield-alt"></i> Clear Failsafe</button>` : ''}
                </div>
                ${failsafeTriggered ? `<div class="alert alert-danger mt-3" style="background-color: var(--danger); color: white;"><strong>Failsafe:</strong> ${failsafeReason}</div>` : ''}
              </div>
            </div>
          </div>
        </div>
        <div class="row">
          <div class="col-12">
            <div class="card">
                <div class="card-header"><h5 class="mb-0"><i class="fas fa-chart-bar me-2"></i>Lifetime Stats</h5></div>
                <div class="card-body">
                    <div class="row">
                      <div class="col-md-3"><div class="stat-card"><p>New Products</p><h3>${stats.newProducts}</h3></div></div>
                      <div class="col-md-3"><div class="stat-card"><p>Inventory Updates</p><h3>${stats.inventoryUpdates}</h3></div></div>
                      <div class="col-md-3"><div class="stat-card"><p>Discontinued</p><h3>${stats.discontinued}</h3></div></div>
                      <div class="col-md-3"><div class="stat-card"><p>Errors</p><h3>${stats.errors}</h3></div></div>
                    </div>
                </div>
            </div>
          </div>
        </div>
        <div class="row">
          <div class="col-md-12">
            <div class="card">
              <div class="card-header"><h5 class="mb-0"><i class="fas fa-stream me-2"></i>Logs</h5></div>
              <div class="card-body">
                <div class="logs-container">
                  ${logs.map(log => `<div class="log-entry log-${log.type}"><small>${new Date(log.timestamp).toLocaleString()}</small><span>${log.message.replace(/</g, "&lt;").replace(/>/g, "&gt;")}</span></div>`).join('')}
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
      <script>
        function runSync(type) { if(confirm('Are you sure you want to run the ' + type + ' job?')) { fetch('/api/sync/' + type, { method: 'POST' }).then(res => res.json()).then(data => { if (data.s === 1) { alert('Job started successfully.'); setTimeout(() => location.reload(), 1000); } else { alert('Job failed to start. It might be already running.'); } }).catch(err => alert('Error: ' + err)); } }
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
  if (!systemPaused) { abortVersion++; }
  return res.json({ s: 1 });
});

app.post('/api/failsafe/clear', (req, res) => {
  if (!failsafeTriggered) return res.status(400).json({ s: 0, msg: 'No failsafe triggered' });
  addLog('Failsafe cleared by user', 'warning');
  failsafeTriggered = false;
  failsafeReason = '';
  pendingFailsafeAction = null;
  abortVersion++;
  return res.json({ s: 1 });
});

app.post('/api/sync/:type', (req, res) => {
  const jobs = {
    inventory: updateInventoryJob,
    products: createNewProductsJob,
    discontinued: handleDiscontinuedProductsJob,
    'improved-map-skus': improvedMapSkusJob,
    deduplicate: deduplicateProductsJob
  };
  const { type } = req.params;
  if (!jobs[type]) { return res.status(400).json({s: 0, msg: 'Invalid job type'}); }
  return startBackgroundJob(type, `Manual ${type} sync`, t => jobs[type](t)) ? res.json({s: 1}) : res.status(409).json({s: 0});
});

// Scheduled jobs & Server Start
cron.schedule('0 1 * * *', () => { if (!systemPaused && !failsafeTriggered) startBackgroundJob('inventory', 'Scheduled inventory sync', t => updateInventoryJob(t)); });
cron.schedule('0 2 * * 5', () => { if (!systemPaused && !failsafeTriggered) startBackgroundJob('products', 'Scheduled Product Sync', t => createNewProductsJob(t)); });
cron.schedule('0 3 * * *', () => { if (!systemPaused && !failsafeTriggered) startBackgroundJob('discontinued', 'Scheduled Discontinued Check', t => handleDiscontinuedProductsJob(t)); });

const server = app.listen(PORT, () => {
  addLog(`Server started on port ${PORT}`, 'success');
  const missing = ['APIFY_TOKEN', 'SHOPIFY_ACCESS_TOKEN', 'SHOPIFY_DOMAIN', 'SHOPIFY_LOCATION_ID'].filter(key => !process.env[key]);
  if (missing.length > 0) {
    addLog(`FATAL: Missing environment variables: ${missing.join(', ')}`, 'error');
    process.exit(1);
  }
});

process.on('SIGTERM', () => { addLog('SIGTERM received...', 'warning'); server.close(() => process.exit(0)); });
process.on('SIGINT', () => { addLog('SIGINT received...', 'warning'); server.close(() => process.exit(0)); });
