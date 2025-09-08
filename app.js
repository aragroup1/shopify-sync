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

const jobLocks = { inventory: false, products: false, discontinued: false, mapSkus: false, deduplicate: false, errorReport: false, 'cleanse-unmatched': false };
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
function addLog(message, type = 'info', error = null) { const log = { timestamp: new Date().toISOString(), message, type }; logs.unshift(log); if (logs.length > 200) logs.length = 200; console.log(`[${new Date(log.timestamp).toLocaleTimeString()}] ${message}`); if (type === 'error') { stats.errors++; let normalizedError = (error?.message || message).replace(/"[^"]+"/g, '"{VAR}"').replace(/\b\d{5,}\b/g, '{ID}'); errorSummary.set(normalizedError, (errorSummary.get(normalizedError) || 0) + 1); } }
async function notifyTelegram(text) { if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) return; try { await axios.post(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`, { chat_id: TELEGRAM_CHAT_ID, text, parse_mode: 'HTML' }, { timeout: 15000 }); } catch (e) { addLog(`Telegram notify failed: ${e.message}`, 'warning', e); } }
function startBackgroundJob(key, name, fn) { if (jobLocks[key]) { addLog(`${name} already running; ignoring duplicate start`, 'warning'); return false; } if (failsafeTriggered) { addLog(`System in failsafe mode. Cannot start job: ${name}`, 'warning'); return false; } jobLocks[key] = true; const token = getJobToken(); addLog(`Started background job: ${name}`, 'info'); setImmediate(async () => { try { await fn(token); } catch (e) { addLog(`Unhandled error in ${name}: ${e.message}\n${e.stack}`, 'error', e); } finally { jobLocks[key] = false; addLog(`${name} job finished`, 'info'); } }); return true; }
function getWordOverlap(str1, str2) { const words1 = new Set(str1.split(' ')); const words2 = new Set(str2.split(' ')); const intersection = new Set([...words1].filter(x => words2.has(x))); return (intersection.size / Math.max(words1.size, words2.size)) * 100; }

// [+] IMPROVED HELPER FUNCTION: Cleans product titles for display
function cleanProductTitle(title) {
  if (!title) return '';
  return title
    .replace(/\b[A-Z]?\d{4,}\b/gi, '') // Removes codes like R38864, SK28659, or 1433
    .replace(/\s*KATEX_INLINE_OPEN.*?KATEX_INLINE_CLOSE\s*/g, '')      // Removes text in brackets like (Large Letter Rate)
    .replace(/\s+/g, ' ')               // Cleans up double spaces
    .trim();                            // Trims leading/trailing whitespace
}

// ENHANCED Helper for robust Shopify API calls with more patient rate-limit handling
async function shopifyRequestWithRetry(requestFn, ...args) {
    const maxRetries = 7;
    for (let attempt = 0; attempt < maxRetries; attempt++) {
        try {
            return await requestFn(...args);
        } catch (error) {
            if (error.response && error.response.status === 429) {
                const retryAfter = error.response.headers['retry-after'] ? parseFloat(error.response.headers['retry-after']) * 1000 : (2 ** attempt) * 2000 + Math.random() * 1000;
                addLog(`Shopify rate limit hit. Retrying in ${Math.round(retryAfter / 1000)}s... (Attempt ${attempt + 1}/${maxRetries})`, 'warning');
                await new Promise(res => setTimeout(res, retryAfter));
            } else {
                throw error;
            }
        }
    }
    throw new Error(`Shopify request failed after ${maxRetries} retries.`);
}

// --- Data Fetching & Processing ---
async function getApifyProducts() { let allItems = []; let offset = 0; addLog('Starting Apify product fetch...', 'info'); try { while (true) { const { data } = await apifyClient.get(`/acts/${config.apify.actorId}/runs/last/dataset/items?token=${config.apify.token}&limit=1000&offset=${offset}`); if (!data || data.length === 0) break; allItems.push(...data); if (data.length < 1000) break; offset += 1000; } } catch (error) { addLog(`Apify fetch error: ${error.message}`, 'error', error); throw error; } addLog(`Apify fetch complete: ${allItems.length} total products.`, 'info'); return allItems; }
async function getShopifyProducts({ fields = 'id,handle,title,variants,tags,status,created_at' } = {}) { let allProducts = []; addLog(`Starting Shopify fetch...`, 'info'); try { let url = `/products.json?limit=250&fields=${fields}`; while (url) { const response = await shopifyRequestWithRetry(shopifyClient.get, url); allProducts.push(...response.data.products); const linkHeader = response.headers.link; url = null; if (linkHeader) { const nextLink = linkHeader.split(',').find(s => s.includes('rel="next"')); if (nextLink) { const pageInfoMatch = nextLink.match(/page_info=([^>]+)>/); if (pageInfoMatch) url = `/products.json?limit=250&fields=${fields}&page_info=${pageInfoMatch[1]}`; } } await new Promise(r => setTimeout(r, 500)); } const activeCount = allProducts.filter(p => p.status === 'active').length; const draftCount = allProducts.filter(p => p.status === 'draft').length; const archivedCount = allProducts.filter(p => p.status === 'archived').length; addLog(`Shopify fetch complete: ${allProducts.length} total products (${activeCount} active, ${draftCount} draft, ${archivedCount} archived).`, 'info'); } catch (error) { addLog(`Shopify fetch error: ${error.message}`, 'error', error); throw error; } return allProducts; }
async function getShopifyInventoryLevels(inventoryItemIds) { const inventoryMap = new Map(); if (!inventoryItemIds.length) return inventoryMap; try { for (let i = 0; i < inventoryItemIds.length; i += 50) { const chunk = inventoryItemIds.slice(i, i + 50); const { data } = await shopifyRequestWithRetry(shopifyClient.get, `/inventory_levels.json?inventory_item_ids=${chunk.join(',')}&location_ids=${config.shopify.locationId}`); for (const level of data.inventory_levels) { inventoryMap.set(level.inventory_item_id, level.available || 0); } await new Promise(r => setTimeout(r, 500)); } } catch (e) { addLog(`Error fetching inventory levels: ${e.message}`, 'error', e); } return inventoryMap; }
function normalizeForMatching(text = '') { return String(text).toLowerCase().replace(/\s*KATEX_INLINE_OPEN.*?KATEX_INLINE_CLOSE\s*/g, ' ').replace(/\s*KATEX_INLINE_OPEN.*?KATEX_INLINE_CLOSE\s*/g, ' ').replace(/```math.*?```/gs, ' ').replace(/-parcel-large-letter-rate$/i, '').replace(/-p\d+$/i, '').replace(/\b(a|an|the|of|in|on|at|to|for|with|by)\b/g, '').replace(/[^a-z0-9]+/g, ' ').replace(/\s+/g, ' ').trim(); }
function calculateRetailPrice(supplierCostString) { const cost = parseFloat(supplierCostString); if (isNaN(cost) || cost < 0) return '0.00'; let finalPrice; if (cost <= 1) finalPrice = cost + 5.5; else if (cost <= 2) finalPrice = cost + 5.95; else if (cost <= 3) finalPrice = cost + 6.99; else if (cost <= 5) finalPrice = cost * 3.2; else if (cost <= 7) finalPrice = cost * 2.5; else if (cost <= 9) finalPrice = cost * 2.2; else if (cost <= 12) finalPrice = cost * 2; else if (cost <= 20) finalPrice = cost * 1.9; else finalPrice = cost * 1.8; return finalPrice.toFixed(2); }

// [+] CORRECTED: Title is now cleaned here, at the source.
function processApifyProducts(apifyData, { processPrice = true } = {}) {
    return apifyData.map(item => {
        if (!item || !item.title) return null;

        const cleanedTitle = cleanProductTitle(item.title); // Clean the title immediately
        const handle = normalizeForMatching(item.handle || cleanedTitle).replace(/ /g, '-');
        let inventory = 20;
        if (item.variants?.[0]?.price?.stockStatus === 'OutOfStock') inventory = 0;
        let sku = item.variants?.[0]?.sku || item.sku || '';
        const body_html = item.description || '';
        let price = '0.00';
        if (item.variants?.[0]?.price?.current) {
            const priceInPence = parseFloat(item.variants[0].price.current);
            if (!isNaN(priceInPence)) {
                const priceInPounds = priceInPence / 100;
                price = processPrice ? calculateRetailPrice(priceInPounds.toString()) : priceInPounds.toFixed(2);
            }
        }
        let images = [];
        if (item.medias && Array.isArray(item.medias)) {
            images = item.medias
                .filter(media => media.type === 'Image' && media.url)
                .map(media => ({ src: media.url }));
        }
        return { 
            handle, 
            title: cleanedTitle, // Use the cleaned title from now on
            inventory, 
            sku, 
            price, 
            body_html, 
            images, 
            normalizedTitle: normalizeForMatching(item.title) // Keep original normalization for matching purposes
        };
    }).filter(p => p && p.sku);
}

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
            if (!productsByTitle.has(normalized)) productsByTitle.set(normalized, []);
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
                    await shopifyRequestWithRetry(shopifyClient.delete, `/products/${id}.json`);
                    deletedCount++;
                    addLog(` ✓ Deleted product ID ${id}`, 'success');
                } catch (e) {
                    errors++;
                    addLog(` ✗ Error deleting product ID ${id}: ${e.message}`, 'error', e);
                }
            }
        } else {
            addLog('No duplicate products found to delete.', 'success');
        }
        addLog(`Deduplication job complete: Deleted ${deletedCount} products. Errors: ${errors}.`, 'success');
    } catch (e) {
        addLog(`Critical error in deduplication job: ${e.message}`, 'error', e);
        errors++;
    }
    lastRun.deduplicate = { at: new Date().toISOString(), deleted: deletedCount, errors };
}
async function improvedMapSkusJob(token) { addLog(`--- Starting Comprehensive SKU Mapping Job with Debug Info ---`, 'warning'); let updated = 0, errors = 0, alreadyMatched = 0, noMatch = 0; let matchedByHandle = 0, matchedByTitle = 0, matchedByFuzzy = 0; let matchedByPartialSkuInSku = 0, matchedByPartialSkuInHandle = 0, matchedByPartialSkuInTitle = 0; let skippedNonSupplier = 0; try { const [apifyData, shopifyData] = await Promise.all([getApifyProducts(), getShopifyProducts()]); if (shouldAbort(token)) return; const apifyProcessed = processApifyProducts(apifyData, { processPrice: false }); const apifySkuMap = new Map(); for (const apifyProd of apifyProcessed) { apifySkuMap.set(apifyProd.sku.toLowerCase(), apifyProd); } const supplierProducts = shopifyData.filter(p => p.tags && p.tags.includes(SUPPLIER_TAG)); addLog(`Found ${supplierProducts.length} Shopify products with tag '${SUPPLIER_TAG}' to process.`, 'info'); skippedNonSupplier = shopifyData.length - supplierProducts.length; const usedShopifyIds = new Set(); const usedApifySkus = new Set(); for (const shopifyProd of supplierProducts) { const variant = shopifyProd.variants?.[0]; if (!variant) continue; const shopifySku = variant.sku?.toLowerCase(); if (shopifySku && apifySkuMap.has(shopifySku)) { alreadyMatched++; usedShopifyIds.add(shopifyProd.id); usedApifySkus.add(shopifySku); } } addLog(`Found ${alreadyMatched} products already correctly mapped by SKU`, 'info'); const toProcess = supplierProducts.filter(p => !usedShopifyIds.has(p.id)); const updateCandidates = []; const remainingApifyProducts = apifyProcessed.filter(p => !usedApifySkus.has(p.sku.toLowerCase())); addLog(`Processing ${toProcess.length} products that need SKU mapping...`, 'info'); for (const shopifyProd of toProcess) { if (shouldAbort(token)) break; const shopifyVariant = shopifyProd.variants?.[0]; if (!shopifyVariant) continue; const shopifyNormalizedTitle = normalizeForMatching(shopifyProd.title); let bestMatch = null, matchType = ''; for (const apifyProd of remainingApifyProducts) { if (shopifyProd.handle.toLowerCase() === apifyProd.handle.toLowerCase()) { bestMatch = apifyProd; matchType = 'handle'; break; } } if (bestMatch) { matchedByHandle++; } else { for (const apifyProd of remainingApifyProducts) { if (shopifyNormalizedTitle === apifyProd.normalizedTitle) { bestMatch = apifyProd; matchType = 'title'; break; } } if (bestMatch) { matchedByTitle++; } else { const shopifySku = shopifyVariant.sku; if (shopifySku && /^\d+$/.test(shopifySku) && shopifySku.length >= 4) { for (const apifyProd of remainingApifyProducts) { if (apifyProd.sku.includes(shopifySku)) { bestMatch = apifyProd; matchType = 'partial-sku-in-full-sku'; matchedByPartialSkuInSku++; break; } if (apifyProd.handle.includes(shopifySku)) { bestMatch = apifyProd; matchType = 'partial-sku-in-handle'; matchedByPartialSkuInHandle++; break; } if (apifyProd.title.includes(shopifySku)) { bestMatch = apifyProd; matchType = 'partial-sku-in-title'; matchedByPartialSkuInTitle++; break; } } } if(!bestMatch) { let maxOverlap = 70; for (const apifyProd of remainingApifyProducts) { const overlap = getWordOverlap(shopifyNormalizedTitle, apifyProd.normalizedTitle); if (overlap > maxOverlap) { maxOverlap = overlap; bestMatch = apifyProd; matchType = `fuzzy-${Math.round(maxOverlap)}%`; } } if (bestMatch) { matchedByFuzzy++; } } } } if (bestMatch) { const variant = shopifyProd.variants?.[0]; if (!variant) continue; updateCandidates.push({ variantId: variant.id, oldSku: variant.sku || '(none)', newSku: bestMatch.sku, title: shopifyProd.title, matchType }); const index = remainingApifyProducts.findIndex(p => p.sku === bestMatch.sku); if (index !== -1) remainingApifyProducts.splice(index, 1); } else { noMatch++; } } addLog(`SKU mapping summary:`, 'info'); addLog(`- Already matched: ${alreadyMatched}`, 'info'); addLog(`- Matched by handle: ${matchedByHandle}`, 'info'); addLog(`- Matched by title: ${matchedByTitle}`, 'info'); addLog(`- Matched by Partial SKU (in SKU/Handle/Title): ${matchedByPartialSkuInSku}/${matchedByPartialSkuInHandle}/${matchedByPartialSkuInTitle}`, 'info'); addLog(`- Matched by fuzzy: ${matchedByFuzzy}`, 'info'); addLog(`- Total to update: ${updateCandidates.length}`, 'info'); addLog(`- No match found: ${noMatch}`, 'info'); if (updateCandidates.length > 0) { addLog(`Examples of SKUs to update:`, 'warning'); for (let i = 0; i < Math.min(5, updateCandidates.length); i++) { const item = updateCandidates[i]; addLog(`  - [${item.matchType}] "${item.title}": ${item.oldSku} -> ${item.newSku}`, 'warning'); } for (const item of updateCandidates) { if (shouldAbort(token)) break; try { await shopifyRequestWithRetry(shopifyClient.put, `/variants/${item.variantId}.json`, { variant: { id: item.variantId, sku: item.newSku } }); updated++; addLog(`Updated SKU for "${item.title}" from ${item.oldSku} to ${item.newSku} (${item.matchType})`, 'success'); } catch (e) { errors++; addLog(`Error updating SKU for "${item.title}": ${e.message}`, 'error', e); } } } addLog(`SKU mapping complete: Updated ${updated}, Errors ${errors}, Already matched ${alreadyMatched}, No match ${noMatch}`, 'success'); } catch (e) { addLog(`Critical error in SKU mapping job: ${e.message}`, 'error', e); errors++; } lastRun.mapSkus = { at: new Date().toISOString(), updated, errors, alreadyMatched, noMatch, matchedByHandle, matchedByTitle, matchedByPartialSkuInSku, matchedByPartialSkuInHandle, matchedByPartialSkuInTitle, matchedByFuzzy, skippedNonSupplier }; }
async function updateInventoryJob(token, { overrideFailsafe = false } = {}) {
    addLog('Starting inventory sync (SKU-only)...', 'info');
    let updated = 0, errors = 0, inSync = 0, notFound = 0, statusChanged = 0;
    const notFoundItems = [];
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
        for (const apifyProd of apifyProcessed) { const matchResult = matchShopifyProductBySku(apifyProd, skuMap); if (matchResult.product) { const shopifyProd = matchResult.product; const variant = shopifyProd.variants?.[0]; if (!variant?.inventory_item_id) continue; const currentInventory = inventoryLevels.get(variant.inventory_item_id) ?? 0; const targetInventory = apifyProd.inventory; if (currentInventory !== targetInventory) { updates.push({ inventory_item_id: variant.inventory_item_id, location_id: config.shopify.locationId, available: targetInventory, title: shopifyProd.title, current: currentInventory }); } else { inSync++; } if (targetInventory > 0 && shopifyProd.status !== 'active') { statusUpdates.push({ id: shopifyProd.id, title: shopifyProd.title, oldStatus: shopifyProd.status, newStatus: 'active' }); } } else { notFound++; if (notFoundItems.length < 100) notFoundItems.push({ title: apifyProd.title, sku: apifyProd.sku, inventory: apifyProd.inventory }); } }
        addLog(`Inventory Summary: Updates needed: ${updates.length}, Status changes: ${statusUpdates.length}, In Sync: ${inSync}, Not Found: ${notFound}`, 'info');
        
        if (updates.length > 0) {
            const updatePercentage = supplierProducts.length > 0 ? (updates.length / supplierProducts.length) * 100 : 0;
            if (!overrideFailsafe && updatePercentage > FAILSAFE_LIMITS.MAX_INVENTORY_UPDATE_PERCENTAGE) {
                const msg = `Inventory update percentage (${updatePercentage.toFixed(1)}%) exceeds failsafe limit (${FAILSAFE_LIMITS.MAX_INVENTORY_UPDATE_PERCENTAGE}%).`;
                throw new Error(msg);
            }
            if(overrideFailsafe) addLog(`OVERRIDE: Proceeding with high inventory update count.`, 'warning');
            for (const update of updates) { if (shouldAbort(token)) break; try { await shopifyRequestWithRetry(shopifyClient.post, '/inventory_levels/set.json', update); addLog(`Updated inventory for "${update.title}" from ${update.current} to ${update.available}`, 'info'); updated++; } catch (e) { errors++; addLog(`Error updating inventory for "${update.title}": ${e.message}`, 'error', e); } }
        }
        if (statusUpdates.length > 0) { for (const update of statusUpdates) { if (shouldAbort(token)) break; try { await shopifyRequestWithRetry(shopifyClient.put, `/products/${update.id}.json`, { product: { id: update.id, status: update.newStatus } }); addLog(`Updated status for "${update.title}" from ${update.oldStatus} to ${update.newStatus}`, 'success'); statusChanged++; } catch (e) { errors++; addLog(`Error updating status for "${update.title}": ${e.message}`, 'error', e); } } }
        addLog(`Inventory update complete: Updated: ${updated}, Status Changed: ${statusChanged}, Errors: ${errors}, In Sync: ${inSync}, Not Found: ${notFound}`, 'success');
        stats.inventoryUpdates += updated;
    } catch (e) {
        if (e.message.includes('exceeds failsafe limit')) {
            failsafeTriggered = true;
            failsafeReason = e.message;
            pendingFailsafeAction = () => startBackgroundJob('inventory', 'Inventory Sync (OVERRIDE)', t => updateInventoryJob(t, { overrideFailsafe: true }));
            const msg = `<b>APIFY SYNC: FAILSAFE TRIGGERED</b>\n${e.message}\nThe system is paused. Please review the logs and choose to proceed or abort in the dashboard.`;
            addLog(e.message, 'error');
            await notifyTelegram(msg);
            return;
        }
        addLog(`Critical error in inventory update job: ${e.message}`, 'error', e);
        errors++;
    }
    lastRun.inventory = { at: new Date().toISOString(), updated, statusChanged, errors, inSync, notFound, notFoundSample: notFoundItems.slice(0, 100) };
}
async function createNewProductsJob(token, { overrideFailsafe = false } = {}) {
    addLog('Starting new product creation...', 'info');
    let created = 0, errors = 0, skipped = 0;
    const createdItems = [];
    try {
        const [apifyData, shopifyData] = await Promise.all([getApifyProducts(), getShopifyProducts()]);
        if (shouldAbort(token)) return;
        const apifyProcessed = processApifyProducts(apifyData, { processPrice: true });
        const skuMap = new Map();
        for (const product of shopifyData) { const sku = product.variants?.[0]?.sku; if (sku) skuMap.set(sku.toLowerCase(), product); }
        const toCreate = apifyProcessed.filter(p => !matchShopifyProductBySku(p, skuMap).product);
        addLog(`Found ${toCreate.length} new products to create.`, 'info');
        
        if (!overrideFailsafe && toCreate.length > FAILSAFE_LIMITS.MAX_NEW_PRODUCTS_AT_ONCE) {
            const msg = `Found ${toCreate.length} new products, which exceeds the failsafe limit of ${FAILSAFE_LIMITS.MAX_NEW_PRODUCTS_AT_ONCE}.`;
            throw new Error(msg);
        }
        if (overrideFailsafe) addLog(`OVERRIDE: Proceeding with creating ${toCreate.length} products.`, 'warning');

        if (toCreate.length > 0) {
            const maxToCreate = overrideFailsafe ? toCreate.length : Math.min(toCreate.length, MAX_CREATE_PER_RUN);
            if (toCreate.length > maxToCreate && !overrideFailsafe) {
                addLog(`Limiting creation to ${maxToCreate} products per run. To create all, approve the failsafe.`, 'warning');
            }
            
            for (let i = 0; i < maxToCreate; i++) {
                if (shouldAbort(token)) break;
                const apifyProd = toCreate[i];
                
                // [+] CORRECTED: The title is now pre-cleaned by processApifyProducts. No extra cleaning is needed here.
                try {
                    const newProduct = { product: { 
                        title: apifyProd.title, // This is now the clean title
                        body_html: apifyProd.body_html || '', 
                        vendor: 'Imported', 
                        tags: SUPPLIER_TAG, 
                        status: apifyProd.inventory > 0 ? 'active' : 'draft', 
                        variants: [{ price: apifyProd.price, sku: apifyProd.sku, inventory_management: 'shopify' }], 
                        images: apifyProd.images 
                    }};
                    const { data } = await shopifyRequestWithRetry(shopifyClient.post, '/products.json', newProduct);
                    const inventoryItemId = data.product.variants[0].inventory_item_id;
                    if (inventoryItemId) {
                       await shopifyRequestWithRetry(shopifyClient.post, '/inventory_levels/set.json', { inventory_item_id: inventoryItemId, location_id: config.shopify.locationId, available: apifyProd.inventory });
                    }
                    created++;
                    createdItems.push({ id: data.product.id, title: apifyProd.title, sku: apifyProd.sku });
                    addLog(`Created product: "${apifyProd.title}" (SKU: ${apifyProd.sku}, Status: ${newProduct.product.status})`, 'success');
                } catch (e) {
                    errors++;
                    addLog(`Error creating product "${apifyProd.title}": ${e.message}`, 'error', e);
                }
                await new Promise(r => setTimeout(r, 2000));
            }
            skipped = toCreate.length - created;
        } else {
            addLog('No new products to create.', 'success');
        }
        stats.newProducts += created;
    } catch (e) {
        if (e.message.includes('exceeds the failsafe limit')) {
            failsafeTriggered = true;
            failsafeReason = e.message;
            pendingFailsafeAction = () => startBackgroundJob('products', 'New Product Sync (OVERRIDE)', t => createNewProductsJob(t, { overrideFailsafe: true }));
            const msg = `<b>APIFY SYNC: FAILSAFE TRIGGERED</b>\n${e.message}\nThe system is paused. Please review the logs and choose to proceed or abort in the dashboard.`;
            addLog(e.message, 'error');
            await notifyTelegram(msg);
            return;
        }
        addLog(`Critical error in product creation job: ${e.message}`, 'error', e);
        errors++;
    }
    lastRun.products = { at: new Date().toISOString(), created, errors, skipped, createdItems };
}
async function handleDiscontinuedProductsJob(token, { overrideFailsafe = false } = {}) {
    addLog('Starting discontinued check (SKU-only)...', 'info');
    let discontinued = 0, errors = 0, skipped = 0;
    const discontinuedItems = [];
    try {
        const [apifyData, shopifyData] = await Promise.all([getApifyProducts(), getShopifyProducts({ fields: 'id,title,variants,tags,status' })]);
        if (shouldAbort(token)) return;
        
        const apifySkus = new Set(processApifyProducts(apifyData, { processPrice: false }).map(p => p.sku.toLowerCase()));
        const supplierProducts = shopifyData.filter(p => p.tags && p.tags.includes(SUPPLIER_TAG));
        
        addLog(`Found ${apifySkus.size} unique SKUs in the latest Apify data.`, 'info');
        addLog(`Found ${supplierProducts.length} total Shopify products with tag '${SUPPLIER_TAG}'.`, 'info');

        const activeSupplierProducts = supplierProducts.filter(p => p.status === 'active');
        addLog(`Checking ${activeSupplierProducts.length} of them which are 'active' against Apify SKUs...`, 'info');

        const potentialDiscontinued = activeSupplierProducts.filter(p => {
            const sku = p.variants?.[0]?.sku?.toLowerCase();
            return sku && !apifySkus.has(sku);
        });
        
        addLog(`Found ${potentialDiscontinued.length} active products in Shopify that are no longer in the Apify data. These will be set to 0 inventory and moved to Draft.`, 'info');
        
        if (potentialDiscontinued.length > 0) {
            const discontinuePercentage = supplierProducts.length > 0 ? (potentialDiscontinued.length / supplierProducts.length) * 100 : 0;
            if (!overrideFailsafe && discontinuePercentage > FAILSAFE_LIMITS.MAX_DISCONTINUE_PERCENTAGE) {
                const msg = `Discontinue percentage (${discontinuePercentage.toFixed(1)}%) exceeds failsafe limit (${FAILSAFE_LIMITS.MAX_DISCONTINUE_PERCENTAGE}%).`;
                throw new Error(msg);
            }
            if(overrideFailsafe) addLog(`OVERRIDE: Proceeding with discontinuing ${potentialDiscontinued.length} products.`, 'warning');

            for (const product of potentialDiscontinued) { 
                if (shouldAbort(token)) break; 
                try { 
                    const variant = product.variants?.[0];
                    if (!variant) continue;
                    
                    if (variant.inventory_item_id) { 
                        await shopifyRequestWithRetry(shopifyClient.post, '/inventory_levels/set.json', { inventory_item_id: variant.inventory_item_id, location_id: config.shopify.locationId, available: 0 }); 
                    } 
                    await shopifyRequestWithRetry(shopifyClient.put, `/products/${product.id}.json`, { product: { id: product.id, status: 'draft' } }); 
                    
                    discontinued++; 
                    discontinuedItems.push({ id: product.id, title: product.title, sku: variant.sku }); 
                    addLog(`Discontinued product: "${product.title}" (SKU: ${variant.sku}) - Set to 0 inventory and status to Draft.`, 'success'); 
                } catch (e) { 
                    errors++; 
                    addLog(`Error discontinuing product "${product.title}": ${e.message}`, 'error', e); 
                } 
            }
            skipped = potentialDiscontinued.length - discontinued;
        } else { addLog('No products to discontinue.', 'success'); }
        stats.discontinued += discontinued;
    } catch (e) {
        if (e.message.includes('exceeds failsafe limit')) {
            failsafeTriggered = true;
            failsafeReason = e.message;
            pendingFailsafeAction = () => startBackgroundJob('discontinued', 'Discontinued Check (OVERRIDE)', t => handleDiscontinuedProductsJob(t, { overrideFailsafe: true }));
            const msg = `<b>APIFY SYNC: FAILSAFE TRIGGERED</b>\n${e.message}\nThe system is paused. Please review the logs and choose to proceed or abort in the dashboard.`;
            addLog(e.message, 'error');
            await notifyTelegram(msg);
            return;
        }
        addLog(`Critical error in discontinued products job: ${e.message}`, 'error', e);
        errors++;
    }
    lastRun.discontinued = { at: new Date().toISOString(), discontinued, errors, skipped, discontinuedItems };
}
async function cleanseUnmatchedProductsJob(token, { overrideFailsafe = false } = {}) {
    addLog('--- Starting One-Time Unmatched Product Cleanup Job ---', 'error');
    let deletedCount = 0, errors = 0, toDeleteCount = 0;
    try {
        const [apifyData, shopifyData] = await Promise.all([getApifyProducts(), getShopifyProducts({ fields: 'id,handle,title,variants,tags' })]);
        if (shouldAbort(token)) return;
        
        if (!overrideFailsafe && (!apifyData || apifyData.length < 1000)) {
            const msg = `Apify fetch returned only ${apifyData ? apifyData.length : 0} products. This is too low to safely perform a deletion. Aborting cleanse job.`;
            throw new Error(msg);
        }
        if (overrideFailsafe) addLog(`OVERRIDE: Proceeding with cleanse despite low Apify product count.`, 'warning');
        
        const apifyProcessed = processApifyProducts(apifyData, { processPrice: false }); const apifySkuMap = new Map(apifyProcessed.map(p => [p.sku.toLowerCase(), p])); const supplierProducts = shopifyData.filter(p => p.tags && p.tags.includes(SUPPLIER_TAG)); addLog(`Found ${supplierProducts.length} Shopify products with tag '${SUPPLIER_TAG}' to check.`, 'info'); const matchedShopifyIds = new Set(); for (const shopifyProd of supplierProducts) { const variant = shopifyProd.variants?.[0]; if (!variant) continue; const shopifySku = variant.sku?.toLowerCase(); if (shopifySku && apifySkuMap.has(shopifySku)) { matchedShopifyIds.add(shopifyProd.id); continue; } const partialSku = variant.sku; if (partialSku && /^\d+$/.test(partialSku) && partialSku.length >= 4) { const partialMatch = apifyProcessed.find(p => p.sku.includes(partialSku) || p.handle.includes(partialSku) || p.title.includes(partialSku)); if (partialMatch) { matchedShopifyIds.add(shopifyProd.id); continue; } } const normTitle = normalizeForMatching(shopifyProd.title); const fuzzyMatch = apifyProcessed.find(p => getWordOverlap(normTitle, p.normalizedTitle) > 65); if (fuzzyMatch) { matchedShopifyIds.add(shopifyProd.id); continue; } } const toDelete = supplierProducts.filter(p => !matchedShopifyIds.has(p.id)); toDeleteCount = toDelete.length; addLog(`Found ${matchedShopifyIds.size} matched products.`, 'info'); addLog(`${toDeleteCount} products have no match and will be deleted.`, 'warning'); if (toDeleteCount > 0) { addLog(`Examples of products to be deleted:`, 'warning'); toDelete.slice(0, 5).forEach(p => addLog(`  - "${p.title}" (SKU: ${p.variants?.[0]?.sku || '(none)'})`, 'warning')); addLog('Deletion will begin in 5 seconds...', 'error'); await new Promise(r => setTimeout(r, 5000)); for (const product of toDelete) { if (shouldAbort(token)) break; try { await shopifyRequestWithRetry(shopifyClient.delete, `/products/${product.id}.json`); deletedCount++; addLog(` ✓ Deleted product ID ${product.id} ("${product.title}")`, 'success'); } catch (e) { errors++; addLog(` ✗ Error deleting product ID ${product.id}: ${e.message}`, 'error', e); } } } else { addLog('No unmatched products found to delete.', 'success'); }
    } catch (e) {
        if (e.message.includes('too low to safely perform a deletion')) {
            failsafeTriggered = true;
            failsafeReason = e.message;
            pendingFailsafeAction = () => startBackgroundJob('cleanse-unmatched', 'Cleanse Unmatched (OVERRIDE)', t => cleanseUnmatchedProductsJob(t, { overrideFailsafe: true }));
            const msg = `<b>APIFY SYNC: FAILSAFE TRIGGERED</b>\n${e.message}\nThe system is paused. Please review the logs and choose to proceed or abort in the dashboard.`;
            addLog(e.message, 'error');
            await notifyTelegram(msg);
            return;
        }
        addLog(`Critical error in cleanse job: ${e.message}`, 'error', e);
        errors++;
    }
    addLog(`Cleanse job complete: Deleted ${deletedCount} of ${toDeleteCount}, Errors ${errors}`, 'success');
}
async function generateAndSendErrorReport() { try { if (errorSummary.size === 0) { addLog('No errors to report.', 'info'); return; } const sortedErrors = [...errorSummary.entries()].sort((a, b) => b[1] - a[1]).slice(0, 10); let report = `<b>Weekly Error Report</b>\n\n`; report += `Total errors since last report: ${stats.errors}\n\n`; report += `<b>Top errors:</b>\n`; sortedErrors.forEach(([error, count], index) => { report += `${index + 1}. <code>${error}</code> (${count} occurrences)\n`; }); await notifyTelegram(report); errorSummary.clear(); stats.errors = 0; addLog('Error report sent successfully.', 'success'); } catch (e) { addLog(`Failed to send error report: ${e.message}`, 'error', e); } }

// --- UI AND API ---
app.get('/', (req, res) => {
  const status = systemPaused ? 'PAUSED' : (failsafeTriggered ? 'FAILSAFE' : 'RUNNING');
  res.send(`<!DOCTYPE html><html><head><title>Shopify Sync</title><meta name="viewport" content="width=device-width, initial-scale=1"><link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0-alpha1/dist/css/bootstrap.min.css" rel="stylesheet"><link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css"><style>:root{--primary-color:#4361ee;--secondary-color:#3a0ca3;--success-color:#4cc9f0;--warning-color:#f72585;--info-color:#4895ef;--light-color:#f8f9fa;--dark-color:#212529}body{font-family:'Inter',-apple-system,BlinkMacSystemFont,sans-serif;background-color:#f7f7f9;color:#333;line-height:1.6;padding:0;margin:0}.navbar{background:linear-gradient(90deg,var(--primary-color),var(--secondary-color));box-shadow:0 4px 12px rgba(0,0,0,.1);padding:1rem 2rem;color:white;margin-bottom:2rem}.navbar h1{margin:0;font-weight:700;font-size:1.5rem}.card{border:none;border-radius:12px;box-shadow:0 5px 15px rgba(0,0,0,.05);transition:transform .3s,box-shadow .3s;margin-bottom:25px;overflow:hidden}.card:hover{transform:translateY(-5px);box-shadow:0 10px 20px rgba(0,0,0,.1)}.card-header{background:white;padding:1.25rem 1.5rem;font-weight:600;border-bottom:1px solid rgba(0,0,0,.05);display:flex;align-items:center;justify-content:space-between}.card-body{padding:1.5rem}.status-badge{font-size:.9rem;padding:.5rem 1rem;border-radius:50px;font-weight:500;text-transform:uppercase;letter-spacing:.5px;box-shadow:0 3px 8px rgba(0,0,0,.1)}.btn{border-radius:50px;padding:.6rem 1.5rem;font-weight:500;text-transform:capitalize;letter-spacing:.3px;transition:all .3s;box-shadow:0 3px 6px rgba(0,0,0,.1);margin:.25rem}.btn:hover{transform:translateY(-2px);box-shadow:0 5px 10px rgba(0,0,0,.15)}.btn i{margin-right:5px}.btn-primary{background:linear-gradient(45deg,var(--primary-color),var(--info-color));border:none}.btn-warning{background:linear-gradient(45deg,#f72585,#ff9e00);border:none;color:white}.btn-success{background:linear-gradient(45deg,#06d6a0,#1b9aaa);border:none}.btn-danger{background:linear-gradient(45deg,#ef476f,#ffd166);border:none}.btn-secondary{background:linear-gradient(45deg,#8338ec,#3a86ff);border:none;color:white}.stat-card{padding:1rem;border-radius:10px;box-shadow:0 3px 10px rgba(0,0,0,.05);background:white;text-align:center;transition:all .3s}.stat-card:hover{transform:translateY(-3px);box-shadow:0 5px 15px rgba(0,0,0,.1)}.stat-card h3{font-size:2rem;font-weight:700;margin:.5rem 0;background:linear-gradient(45deg,var(--primary-color),var(--info-color));-webkit-background-clip:text;background-clip:text;-webkit-text-fill-color:transparent}.stat-card p{margin:0;font-size:.9rem;color:#6c757d;font-weight:500}.logs-container{max-height:500px;overflow-y:auto;border-radius:8px;background:#f8f9fa;padding:1rem}.log-entry{padding:.75rem 1rem;margin-bottom:.5rem;border-radius:6px;background:white;box-shadow:0 2px 5px rgba(0,0,0,.02);border-left:4px solid #dee2e6;font-size:.9rem}.log-entry small{display:block;font-size:.75rem;opacity:.7;margin-bottom:.25rem}.log-error{border-left-color:#ef476f;background:#fff5f7}.log-warning{border-left-color:#ffd166;background:#fff9eb}.log-success{border-left-color:#06d6a0;background:#f0fff4}.alert{border-radius:10px;padding:1rem 1.5rem;border:none;box-shadow:0 3px 10px rgba(0,0,0,.05)}.bg-running{background:linear-gradient(-45deg,#06d6a0,#1b9aaa,#4cc9f0,#3a86ff);background-size:400% 400%;animation:gradient 3s ease infinite}.bg-paused{background:linear-gradient(-45deg,#ffd166,#ffbd00,#ff9e00,#ff7700);background-size:400% 400%;animation:gradient 3s ease infinite}.bg-failsafe{background:linear-gradient(-45deg,#ef476f,#f72585,#b5179e,#7209b7);background-size:400% 400%;animation:gradient 3s ease infinite}@keyframes gradient{0%{background-position:0 50%}50%{background-position:100% 50%}100%{background-position:0 50%}}</style></head><body><nav class="navbar"><div class="container-fluid"><h1><i class="fas fa-sync-alt me-2"></i> Shopify Sync Dashboard</h1></div></nav><div class="container"><div class="row mb-4"><div class="col-md-6"><div class="card"><div class="card-header"><h5 class="mb-0"><i class="fas fa-tachometer-alt me-2"></i>System Status</h5><span class="badge ${status==='RUNNING'?'bg-running':status==='PAUSED'?'bg-paused':'bg-failsafe'} status-badge"><i class="fas ${status==='RUNNING'?'fa-play-circle':status==='PAUSED'?'fa-pause-circle':'fa-exclamation-triangle'} me-1"></i>${status}</span></div><div class="card-body"><div class="row mb-4"><div class="col-6 col-md-3"><div class="stat-card"><p>New Products</p><h3>${stats.newProducts}</h3></div></div><div class="col-6 col-md-3"><div class="stat-card"><p>Inventory Updates</p><h3>${stats.inventoryUpdates}</h3></div></div><div class="col-6 col-md-3"><div class="stat-card"><p>Discontinued</p><h3>${stats.discontinued}</h3></div></div><div class="col-6 col-md-3"><div class="stat-card"><p>Errors</p><h3>${stats.errors}</h3></div></div></div><div class="d-flex flex-wrap justify-content-center gap-2 mt-3">${systemPaused?`<button class="btn btn-success" onclick="fetch('/api/pause',{method:'POST',body:JSON.stringify({paused:!1}),headers:{'Content-Type':'application/json'}}).then(()=>location.reload())"><i class="fas fa-play"></i> Resume System</button>`:`<button class="btn btn-warning" onclick="fetch('/api/pause',{method:'POST',body:JSON.stringify({paused:!0}),headers:{'Content-Type':'application/json'}}).then(()=>location.reload())"><i class="fas fa-pause"></i> Pause System</button>`}</div>${failsafeTriggered?`<div class="alert alert-danger mt-3 text-center"><h5 class="alert-heading"><i class="fas fa-exclamation-triangle me-2"></i>FAILSAFE TRIGGERED</h5><p>${failsafeReason}</p><hr><p class="mb-0">You must resolve this before the system can continue.</p><div class="d-flex justify-content-center gap-2 mt-3"><button class="btn btn-success" onclick="fetch('/api/failsafe/confirm',{method:'POST'}).then(()=>location.reload())"><i class="fas fa-check-circle"></i> Proceed Anyway</button><button class="btn btn-danger" onclick="fetch('/api/failsafe/abort',{method:'POST'}).then(()=>location.reload())"><i class="fas fa-times-circle"></i> Abort & Clear Failsafe</button></div></div>`:``}</div></div></div><div class="col-md-6"><div class="card"><div class="card-header"><h5 class="mb-0"><i class="fas fa-tools me-2"></i>Manual Actions</h5></div><div class="card-body"><div class="d-flex flex-wrap justify-content-center gap-2"><button class="btn btn-warning" onclick="runSync('improved-map-skus')" ${failsafeTriggered ? 'disabled' : ''}><i class="fas fa-map-signs"></i> Comprehensive SKU Mapping</button><button class="btn btn-primary" onclick="runSync('inventory')" ${failsafeTriggered ? 'disabled' : ''}><i class="fas fa-boxes"></i> Sync Inventory</button><button class="btn btn-primary" onclick="runSync('products')" ${failsafeTriggered ? 'disabled' : ''}><i class="fas fa-plus-circle"></i> Sync New Products</button><button class="btn btn-primary" onclick="runSync('discontinued')" ${failsafeTriggered ? 'disabled' : ''}><i class="fas fa-archive"></i> Check Discontinued</button><button class="btn btn-secondary" onclick="runSync('deduplicate')" ${failsafeTriggered ? 'disabled' : ''}><i class="fas fa-clone"></i> Find & Delete Duplicates</button><button class="btn btn-danger" onclick="if(confirm('DANGER: This will permanently delete all supplier products that cannot be matched. This cannot be undone. Are you sure you want to proceed?')) runSync('cleanse-unmatched')" ${failsafeTriggered ? 'disabled' : ''}><i class="fas fa-trash-alt"></i> Cleanse Unmatched Products</button></div></div></div></div></div><div class="row"><div class="col-md-12"><div class="card"><div class="card-header"><h5 class="mb-0"><i class="fas fa-list-alt me-2"></i>Logs</h5></div><div class="card-body"><div class="logs-container">${logs.map(log=>`<div class="log-entry ${log.type==='error'?'log-error':log.type==='warning'?'log-warning':log.type==='success'?'log-success':''}"><small>${new Date(log.timestamp).toLocaleString()}</small><span>${log.message}</span></div>`).join('')}</div></div></div></div></div></div><script>function runSync(t){fetch("/api/sync/"+t,{method:"POST"}).then(t=>t.json()).then(t=>{1===t.s?(alert("Job started successfully!"),setTimeout(()=>location.reload(),1e3)):alert(t.msg||"Job already running. Try again later.")}).catch(t=>{alert("Error: "+t)})}setTimeout(()=>location.reload(),3e4);</script></body></html>`);
});
app.get('/api/status', (req, res) => res.json({ stats, lastRun, logs, systemPaused, failsafeTriggered, failsafeReason, errorSummary: Array.from(errorSummary.entries()).map(([msg, count]) => ({msg, count})) }));
app.post('/api/pause', (req, res) => { const { paused } = req.body; if (paused === undefined) return res.status(400).json({ s: 0, msg: 'Missing paused parameter' }); systemPaused = Boolean(paused); addLog(`System ${systemPaused ? 'paused' : 'resumed'} by user`, 'warning'); if (!systemPaused) { abortVersion++; } return res.json({ s: 1 }); });
app.post('/api/failsafe/confirm', (req, res) => {
    if (!failsafeTriggered || !pendingFailsafeAction) {
        return res.status(400).json({ s: 0, msg: 'No pending failsafe action to confirm.' });
    }
    addLog('Failsafe action confirmed by user. Proceeding with override...', 'warning');
    const actionToRun = pendingFailsafeAction;
    failsafeTriggered = false;
    failsafeReason = '';
    pendingFailsafeAction = null;
    abortVersion++;
    
    actionToRun();
    
    return res.json({ s: 1 });
});
app.post('/api/failsafe/abort', (req, res) => {
    if (!failsafeTriggered) {
        return res.status(400).json({ s: 0, msg: 'No failsafe is currently triggered.' });
    }
    addLog('Failsafe aborted by user. System returning to normal.', 'warning');
    failsafeTriggered = false;
    failsafeReason = '';
    pendingFailsafeAction = null;
    abortVersion++;
    
    return res.json({ s: 1 });
});
app.post('/api/sync/:type', (req, res) => {
  if (failsafeTriggered) {
    return res.status(409).json({s: 0, msg: 'System is in failsafe mode. Resolve the failsafe before starting new jobs.'});
  }
  const jobMap = {
    inventory: { name: 'Inventory Sync', fn: updateInventoryJob },
    products: { name: 'New Product Sync', fn: createNewProductsJob },
    discontinued: { name: 'Discontinued Check', fn: handleDiscontinuedProductsJob },
    'improved-map-skus': { name: 'Comprehensive SKU Mapping', fn: improvedMapSkusJob },
    deduplicate: { name: 'Find & Delete Duplicates', fn: deduplicateProductsJob },
    'cleanse-unmatched': { name: 'Cleanse Unmatched Products', fn: cleanseUnmatchedProductsJob }
  };
  const { type } = req.params;
  const job = jobMap[type];
  if (!job) {
    return res.status(400).json({s: 0, msg: 'Invalid job type'});
  }
  return startBackgroundJob(type, `Manual ${job.name}`, t => job.fn(t)) 
    ? res.json({s: 1}) 
    : res.status(409).json({s: 0, msg: 'Job already running'});
});
// Scheduled jobs & Server Start
cron.schedule('0 1 * * *', () => { if (!systemPaused && !failsafeTriggered) startBackgroundJob('inventory', 'Scheduled Inventory Sync', t => updateInventoryJob(t)); });
cron.schedule('0 2 * * 5', () => { if (!systemPaused && !failsafeTriggered) startBackgroundJob('products', 'Scheduled Product Sync', t => createNewProductsJob(t)); });
cron.schedule('0 3 * * *', () => { if (!systemPaused && !failsafeTriggered) startBackgroundJob('discontinued', 'Scheduled Discontinued Check', t => handleDiscontinuedProductsJob(t)); });
cron.schedule('0 9 * * 0', () => startBackgroundJob('errorReport', 'Weekly Error Report', () => generateAndSendErrorReport()));
const server = app.listen(PORT, () => {
  addLog(`Server started on port ${PORT}`, 'success');
  const missing = ['APIFY_TOKEN', 'SHOPIFY_ACCESS_TOKEN', 'SHOPIFY_DOMAIN', 'SHOPIFY_LOCATION_ID'].filter(key => !process.env[key]);
  if (missing.length > 0) {
    addLog(`FATAL: Missing environment variables: ${missing.join(', ')}`, 'error');
    process.exit(1);
  }
});
process.on('SIGTERM', () => { addLog('SIGTERM received, shutting down...', 'warning'); server.close(() => process.exit(0)); });
process.on('SIGINT', () => { addLog('SIGINT received, shutting down...', 'warning'); server.close(() => process.exit(0)); });
