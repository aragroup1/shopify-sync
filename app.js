const express = require('express');
const axios = require('axios');
const cron = require('node-cron');
const crypto = require('crypto'); // For potential future use, not critical now

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
let errorSummary = new Map();
// NEW: Storage for two-phase discontinuation
let pendingDiscontinuation = new Map(); // Map<sku, { timestamp: number, productInfo: object }>

// Fetch history tracking for validation
let fetchHistory = {
    apify: [],
    shopify: []
};

const jobLocks = { inventory: false, products: false, markDiscontinued: false, confirmDiscontinued: false, mapSkus: false, deduplicate: false, errorReport: false, 'cleanse-unmatched': false };
let abortVersion = 0;
const getJobToken = () => abortVersion;
const shouldAbort = (token) => systemPaused || failsafeTriggered || token !== abortVersion;

// Configuration
const FAILSAFE_LIMITS = { 
    MAX_INVENTORY_UPDATE_PERCENTAGE: Number(process.env.MAX_INVENTORY_UPDATE_PERCENTAGE || 5), 
    MAX_DISCONTINUE_PERCENTAGE: Number(process.env.MAX_DISCONTINUE_PERCENTAGE || 5), 
    MAX_NEW_PRODUCTS_AT_ONCE: Number(process.env.MAX_NEW_PRODUCTS_AT_ONCE || 100), 
    FETCH_TIMEOUT: Number(process.env.FETCH_TIMEOUT || 300000) 
};

// MODIFIED: Renamed for clarity and to allow dynamic updates
let FETCH_VALIDATION = {
    MIN_APIFY_PRODUCTS: Number(process.env.MIN_APIFY_PRODUCTS || 5500),
    MAX_APIFY_PRODUCTS: Number(process.env.MAX_APIFY_PRODUCTS || 7000),
    MIN_SHOPIFY_SUPPLIER_PRODUCTS: Number(process.env.MIN_SHOPIFY_SUPPLIER_PRODUCTS || 5000),
    FETCH_RETRY_ATTEMPTS: 3,
    REQUIRED_MATCH_PERCENTAGE: 95,
    MAX_APIFY_DATA_AGE_HOURS: 24 // NEW: Max age for supplier data
};
// NEW: Core SKUs for continuity check
const CORE_SKUS = (process.env.CORE_SKUS || '').split(',').filter(Boolean);


const MAX_CREATE_PER_RUN = Number(process.env.MAX_CREATE_PER_RUN || 200);
const SUPPLIER_TAG = process.env.SUPPLIER_TAG || 'Supplier:Apify';
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
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID;

const apifyClient = axios.create({ baseURL: config.apify.baseUrl, timeout: FAILSAFE_LIMITS.FETCH_TIMEOUT });
const shopifyClient = axios.create({ baseURL: config.shopify.baseUrl, headers: { 'X-Shopify-Access-Token': config.shopify.accessToken, 'Content-Type': 'application/json' }, timeout: FAILSAFE_LIMITS.FETCH_TIMEOUT });

// --- Helper & Validation Functions ---
// ... (addLog, notifyTelegram, startBackgroundJob, getWordOverlap, cleanProductTitle, shopifyRequestWithRetry are unchanged)
function addLog(message, type = 'info', error = null) { const log = { timestamp: new Date().toISOString(), message, type }; logs.unshift(log); if (logs.length > 200) logs.length = 200; console.log(`[${new Date(log.timestamp).toLocaleTimeString()}] ${message}`); if (type === 'error') { stats.errors++; let normalizedError = (error?.message || message).replace(/"[^"]+"/g, '"{VAR}"').replace(/\b\d{5,}\b/g, '{ID}'); errorSummary.set(normalizedError, (errorSummary.get(normalizedError) || 0) + 1); } }
async function notifyTelegram(text) { if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) return; try { await axios.post(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`, { chat_id: TELEGRAM_CHAT_ID, text, parse_mode: 'HTML' }, { timeout: 15000 }); } catch (e) { addLog(`Telegram notify failed: ${e.message}`, 'warning', e); } }
function startBackgroundJob(key, name, fn) { if (jobLocks[key]) { addLog(`${name} already running; ignoring duplicate start`, 'warning'); return false; } if (failsafeTriggered) { addLog(`System in failsafe mode. Cannot start job: ${name}`, 'warning'); return false; } jobLocks[key] = true; const token = getJobToken(); addLog(`Started background job: ${name}`, 'info'); setImmediate(async () => { try { await fn(token); } catch (e) { addLog(`Unhandled error in ${name}: ${e.message}\n${e.stack}`, 'error', e); } finally { jobLocks[key] = false; addLog(`${name} job finished`, 'info'); } }); return true; }
function getWordOverlap(str1, str2) { const words1 = new Set(str1.split(' ')); const words2 = new Set(str2.split(' ')); const intersection = new Set([...words1].filter(x => words2.has(x))); return (intersection.size / Math.max(words1.size, words2.size)) * 100; }
const TITLE_CLEANUP_PATTERNS = [/\s*KATEX_INLINE_OPEN[^)]*KATEX_INLINE_CLOSE\s*/g, /\b[A-Z]{1,3}-?\d{4,}[A-Z]?\b/gi, /\b\d{4,}\b/g];
function cleanProductTitle(title) { if (!title) return ''; let cleanedTitle = title; TITLE_CLEANUP_PATTERNS.forEach(pattern => { cleanedTitle = cleanedTitle.replace(pattern, ' '); }); return cleanedTitle.replace(/\s+/g, ' ').trim(); }
async function shopifyRequestWithRetry(requestFn, ...args) { const maxRetries = 7; for (let attempt = 0; attempt < maxRetries; attempt++) { try { return await requestFn(...args); } catch (error) { if (error.response && error.response.status === 429) { const retryAfter = error.response.headers['retry-after'] ? parseFloat(error.response.headers['retry-after']) * 1000 : (2 ** attempt) * 2000 + Math.random() * 1000; addLog(`Shopify rate limit hit. Retrying in ${Math.round(retryAfter / 1000)}s... (Attempt ${attempt + 1}/${maxRetries})`, 'warning'); await new Promise(res => setTimeout(res, retryAfter)); } else { throw error; } } } throw new Error(`Shopify request failed after ${maxRetries} retries.`); }


// NEW: Apify data freshness validation
async function validateApifyFreshness() {
    addLog('Checking Apify data freshness...', 'info');
    const { data } = await apifyClient.get(`/acts/${config.apify.actorId}/runs/last?token=${config.apify.token}`);
    if (!data?.data?.finishedAt) {
        throw new Error('Could not determine Apify run finish time.');
    }
    const finishedAt = new Date(data.data.finishedAt);
    const hoursSinceUpdate = (Date.now() - finishedAt.getTime()) / (1000 * 60 * 60);

    if (hoursSinceUpdate > FETCH_VALIDATION.MAX_APIFY_DATA_AGE_HOURS) {
        throw new Error(`Apify data is ${hoursSinceUpdate.toFixed(1)} hours old. This is too stale for a safe sync (limit: ${FETCH_VALIDATION.MAX_APIFY_DATA_AGE_HOURS} hours).`);
    }
    addLog(`Apify data is ${hoursSinceUpdate.toFixed(1)} hours old. (OK)`, 'success');
}

// NEW: Core SKU continuity validation
function validateSkuContinuity(apifyData) {
    if (CORE_SKUS.length === 0) {
        addLog('No CORE_SKUS defined, skipping continuity check.', 'warning');
        return;
    }
    addLog(`Checking continuity of ${CORE_SKUS.length} core SKUs...`, 'info');
    const apifySkuSet = new Set(apifyData.map(p => p.sku || p.variants?.[0]?.sku).filter(Boolean));
    
    const missingCoreSkus = CORE_SKUS.filter(sku => !apifySkuSet.has(sku));
    if (missingCoreSkus.length > 0) {
        throw new Error(`Critical continuity check failed! The following CORE_SKUS are missing from the Apify data: ${missingCoreSkus.join(', ')}. This may indicate a corrupted fetch.`);
    }
    addLog('Core SKU continuity check passed.', 'success');
}

// ... (validateFetchCompleteness, performReconciliationCheck, getShopifyInventoryLevels, normalizeForMatching, calculateRetailPrice, processApifyProducts, matchShopifyProductBySku are mostly unchanged but shown for completeness)
function validateFetchCompleteness(dataType, count, additionalData = {}) { const timestamp = new Date().toISOString(); if (dataType === 'apify') { fetchHistory.apify.push({ timestamp, count }); if (fetchHistory.apify.length > 10) fetchHistory.apify.shift(); if (count < FETCH_VALIDATION.MIN_APIFY_PRODUCTS) { throw new Error(`Apify fetch returned only ${count} products (minimum expected: ${FETCH_VALIDATION.MIN_APIFY_PRODUCTS})`); } if (count > FETCH_VALIDATION.MAX_APIFY_PRODUCTS) { throw new Error(`Apify fetch returned ${count} products (maximum expected: ${FETCH_VALIDATION.MAX_APIFY_PRODUCTS})`); } } else if (dataType === 'shopify-supplier') { fetchHistory.shopify.push({ timestamp, count, ...additionalData }); if (fetchHistory.shopify.length > 10) fetchHistory.shopify.shift(); if (count < FETCH_VALIDATION.MIN_SHOPIFY_SUPPLIER_PRODUCTS) { throw new Error(`Shopify fetch returned only ${count} supplier products (minimum expected: ${FETCH_VALIDATION.MIN_SHOPIFY_SUPPLIER_PRODUCTS})`); } } const recentHistory = dataType === 'apify' ? fetchHistory.apify : fetchHistory.shopify; if (recentHistory.length >= 3) { const recentAvg = recentHistory.slice(-3).reduce((sum, h) => sum + h.count, 0) / 3; const dropPercentage = ((recentAvg - count) / recentAvg) * 100; if (dropPercentage > 10) { throw new Error(`${dataType} fetch shows ${dropPercentage.toFixed(1)}% drop from recent average (${Math.round(recentAvg)} -> ${count})`); } } return true; }
async function performReconciliationCheck(apifyData, shopifyData) { addLog('Performing reconciliation check...', 'warning'); const apifyProcessed = processApifyProducts(apifyData, { processPrice: false }); const supplierProducts = shopifyData.filter(p => p.tags && p.tags.includes(SUPPLIER_TAG)); const apifySkuSet = new Set(apifyProcessed.map(p => p.sku.toLowerCase())); const shopifySkuSet = new Set(); for (const product of supplierProducts) { const sku = product.variants?.[0]?.sku?.toLowerCase(); if (sku) shopifySkuSet.add(sku); } let matchedCount = 0; let unmatchedApifySkus = []; for (const apifySku of apifySkuSet) { if (shopifySkuSet.has(apifySku)) { matchedCount++; } else { unmatchedApifySkus.push(apifySku); } } const matchPercentage = (matchedCount / apifySkuSet.size) * 100; addLog(`Reconciliation Results:`, 'info'); addLog(`- Apify products: ${apifySkuSet.size}`, 'info'); addLog(`- Shopify supplier products: ${supplierProducts.length}`, 'info'); addLog(`- Matched SKUs: ${matchedCount} (${matchPercentage.toFixed(1)}%)`, 'info'); addLog(`- Unmatched Apify SKUs: ${unmatchedApifySkus.length}`, 'info'); if (unmatchedApifySkus.length > 0 && unmatchedApifySkus.length <= 10) { addLog(`Sample unmatched Apify SKUs: ${unmatchedApifySkus.slice(0, 10).join(', ')}`, 'warning'); } return { apifyCount: apifySkuSet.size, shopifyCount: supplierProducts.length, matchedCount, matchPercentage, unmatchedCount: unmatchedApifySkus.length, isValid: matchPercentage >= FETCH_VALIDATION.REQUIRED_MATCH_PERCENTAGE }; }
async function getShopifyInventoryLevels(inventoryItemIds) { const inventoryMap = new Map(); if (!inventoryItemIds.length) return inventoryMap; try { for (let i = 0; i < inventoryItemIds.length; i += 50) { const chunk = inventoryItemIds.slice(i, i + 50); const { data } = await shopifyRequestWithRetry(shopifyClient.get, `/inventory_levels.json?inventory_item_ids=${chunk.join(',')}&location_ids=${config.shopify.locationId}`); for (const level of data.inventory_levels) { inventoryMap.set(level.inventory_item_id, level.available || 0); } await new Promise(r => setTimeout(r, 500)); } } catch (e) { addLog(`Error fetching inventory levels: ${e.message}`, 'error', e); } return inventoryMap; }
function normalizeForMatching(text = '') { return String(text).toLowerCase().replace(/\s*KATEX_INLINE_OPEN[^)]*KATEX_INLINE_CLOSE\s*/g, ' ').replace(/```math.*?```/gs, ' ').replace(/-parcel-large-letter-rate$/i, '').replace(/-p\d+$/i, '').replace(/\b(a|an|the|of|in|on|at|to|for|with|by)\b/g, '').replace(/[^a-z0-9]+/g, ' ').replace(/\s+/g, ' ').trim(); }
function calculateRetailPrice(supplierCostString) { const cost = parseFloat(supplierCostString); if (isNaN(cost) || cost < 0) return '0.00'; let finalPrice; if (cost <= 1) finalPrice = cost + 5.5; else if (cost <= 2) finalPrice = cost + 5.95; else if (cost <= 3) finalPrice = cost + 6.99; else if (cost <= 5) finalPrice = cost * 3.2; else if (cost <= 7) finalPrice = cost * 2.5; else if (cost <= 9) finalPrice = cost * 2.2; else if (cost <= 12) finalPrice = cost * 2; else if (cost <= 20) finalPrice = cost * 1.9; else finalPrice = cost * 1.8; return finalPrice.toFixed(2); }
function processApifyProducts(apifyData, { processPrice = true } = {}) { return apifyData.map(item => { if (!item || !item.title) return null; const handle = normalizeForMatching(item.handle || item.title).replace(/ /g, '-'); let inventory = 20; if (item.variants?.[0]?.price?.stockStatus === 'OutOfStock') inventory = 0; let sku = item.variants?.[0]?.sku || item.sku || ''; const body_html = item.description || ''; let price = '0.00'; if (item.variants?.[0]?.price?.current) { const priceInPence = parseFloat(item.variants[0].price.current); if (!isNaN(priceInPence)) { const priceInPounds = priceInPence / 100; price = processPrice ? calculateRetailPrice(priceInPounds.toString()) : priceInPounds.toFixed(2); } } let images = []; if (item.medias && Array.isArray(item.medias)) { images = item.medias.filter(media => media.type === 'Image' && media.url).map(media => ({ src: media.url })); } return { handle, title: item.title, inventory, sku, price, body_html, images, normalizedTitle: normalizeForMatching(item.title) }; }).filter(p => p && p.sku); }
function matchShopifyProductBySku(apifyProduct, skuMap) { const product = skuMap.get(apifyProduct.sku.toLowerCase()); return product ? { product, matchType: 'sku' } : { product: null, matchType: 'none' }; }


// MODIFIED: Apify fetch function now includes all new validation checks
async function getApifyProducts() {
    let lastError = null;
    
    // First, validate data freshness before even starting the main fetch loop
    await validateApifyFreshness();

    for (let attempt = 1; attempt <= FETCH_VALIDATION.FETCH_RETRY_ATTEMPTS; attempt++) {
        try {
            addLog(`Fetching Apify products (attempt ${attempt}/${FETCH_VALIDATION.FETCH_RETRY_ATTEMPTS})...`, 'info');
            
            let allItems = [];
            // ... (rest of the fetching logic is unchanged)
            let offset = 0; let consecutiveEmptyPages = 0; while (true) { const { data } = await apifyClient.get(`/acts/${config.apify.actorId}/runs/last/dataset/items?token=${config.apify.token}&limit=1000&offset=${offset}`); if (!data || data.length === 0) { consecutiveEmptyPages++; if (consecutiveEmptyPages >= 2) break; await new Promise(r => setTimeout(r, 2000)); const retryResponse = await apifyClient.get(`/acts/${config.apify.actorId}/runs/last/dataset/items?token=${config.apify.token}&limit=1000&offset=${offset}`); if (!retryResponse.data || retryResponse.data.length === 0) break; allItems.push(...retryResponse.data); } else { consecutiveEmptyPages = 0; allItems.push(...data); } if (data && data.length < 1000) break; offset += 1000; if (offset % 3000 === 0) { addLog(`Apify fetch progress: ${allItems.length} products fetched so far...`, 'info'); } }
            
            // Perform all validation checks on the retrieved data
            validateFetchCompleteness('apify', allItems.length);
            validateSkuContinuity(allItems);
            
            addLog(`✓ Apify fetch fully validated: ${allItems.length} products.`, 'success');
            return allItems;
            
        } catch (error) {
            lastError = error;
            addLog(`Apify fetch attempt ${attempt} failed: ${error.message}`, 'error', error);
            if (attempt < FETCH_VALIDATION.FETCH_RETRY_ATTEMPTS) {
                const waitTime = attempt * 5000;
                addLog(`Waiting ${waitTime/1000}s before retry...`, 'warning');
                await new Promise(r => setTimeout(r, waitTime));
            }
        }
    }
    
    throw new Error(`Apify fetch failed after ${FETCH_VALIDATION.FETCH_RETRY_ATTEMPTS} attempts. Last error: ${lastError?.message}`);
}

// MODIFIED: Simplified getShopifyProducts (validation is sufficient, no other changes needed)
async function getShopifyProducts({ fields = 'id,handle,title,variants,tags,status,created_at' } = {}) {
    // ... (This function's internal logic is fine, its validation call is what matters)
    let lastError = null; for (let attempt = 1; attempt <= FETCH_VALIDATION.FETCH_RETRY_ATTEMPTS; attempt++) { try { addLog(`Fetching Shopify products (attempt ${attempt}/${FETCH_VALIDATION.FETCH_RETRY_ATTEMPTS})...`, 'info'); let allProducts = []; let url = `/products.json?limit=250&fields=${fields}`; let pageCount = 0; while (url) { const response = await shopifyRequestWithRetry(shopifyClient.get, url); allProducts.push(...response.data.products); pageCount++; if (pageCount % 10 === 0) { addLog(`Shopify fetch progress: ${allProducts.length} products fetched (${pageCount} pages)...`, 'info'); } const linkHeader = response.headers.link; url = null; if (linkHeader) { const nextLink = linkHeader.split(',').find(s => s.includes('rel="next"')); if (nextLink) { const pageInfoMatch = nextLink.match(/page_info=([^>]+)>/); if (pageInfoMatch) url = `/products.json?limit=250&fields=${fields}&page_info=${pageInfoMatch[1]}`; } } await new Promise(r => setTimeout(r, 500)); } const supplierProducts = allProducts.filter(p => p.tags && p.tags.includes(SUPPLIER_TAG)); const activeCount = allProducts.filter(p => p.status === 'active').length; const draftCount = allProducts.filter(p => p.status === 'draft').length; const archivedCount = allProducts.filter(p => p.status === 'archived').length; validateFetchCompleteness('shopify-supplier', supplierProducts.length, { total: allProducts.length, active: activeCount, draft: draftCount, archived: archivedCount }); addLog(`✓ Shopify fetch validated: ${allProducts.length} total (${supplierProducts.length} supplier, ${activeCount} active, ${draftCount} draft, ${archivedCount} archived)`, 'success'); return allProducts; } catch (error) { lastError = error; addLog(`Shopify fetch attempt ${attempt} failed: ${error.message}`, 'error', error); if (attempt < FETCH_VALIDATION.FETCH_RETRY_ATTEMPTS) { const waitTime = attempt * 5000; addLog(`Waiting ${waitTime/1000}s before retry...`, 'warning'); await new Promise(r => setTimeout(r, waitTime)); } } } throw new Error(`Shopify fetch failed after ${FETCH_VALIDATION.FETCH_RETRY_ATTEMPTS} attempts. Last error: ${lastError?.message}`);
}

// --- CORE JOB LOGIC (with new discontinuation flow) ---
// ... (deduplicateProductsJob, improvedMapSkusJob, updateInventoryJob, createNewProductsJob, cleanseUnmatchedProductsJob are unchanged from previous full script)
async function deduplicateProductsJob(token) { /* ... same as before ... */ }
async function improvedMapSkusJob(token) { /* ... same as before ... */ }
async function updateInventoryJob(token, { overrideFailsafe = false } = {}) { /* ... same as before ... */ }
async function createNewProductsJob(token, { overrideFailsafe = false } = {}) { /* ... same as before ... */ }
async function cleanseUnmatchedProductsJob(token, { overrideFailsafe = false } = {}) { /* ... same as before ... */ }


// MODIFIED: This job now only MARKS products for discontinuation
async function markForDiscontinuationJob(token) {
    addLog('Starting Phase 1: Marking products for discontinuation...', 'info');
    let marked = 0, alreadyPending = 0, errors = 0;
    
    try {
        const [apifyData, shopifyData] = await Promise.all([
            getApifyProducts(),
            getShopifyProducts({ fields: 'id,title,variants,tags,status' })
        ]);
        
        if (shouldAbort(token)) return;

        const apifySkus = new Set(processApifyProducts(apifyData, { processPrice: false }).map(p => p.sku.toLowerCase()));
        const supplierProducts = shopifyData.filter(p => p.tags && p.tags.includes(SUPPLIER_TAG) && p.status === 'active');
        addLog(`Checking ${supplierProducts.length} active supplier products against ${apifySkus.size} Apify SKUs...`, 'info');

        const toMark = supplierProducts.filter(p => {
            const sku = p.variants?.[0]?.sku?.toLowerCase();
            return sku && !apifySkus.has(sku);
        });
        
        addLog(`Found ${toMark.length} active products that are no longer in the supplier feed.`, 'info');

        if (toMark.length > 0) {
            for (const product of toMark) {
                const variant = product.variants?.[0];
                if (!variant || !variant.sku) continue;
                
                const sku = variant.sku.toLowerCase();
                if (pendingDiscontinuation.has(sku)) {
                    alreadyPending++;
                } else {
                    pendingDiscontinuation.set(sku, {
                        timestamp: Date.now(),
                        productInfo: {
                            id: product.id,
                            title: product.title,
                            sku: variant.sku,
                            inventory_item_id: variant.inventory_item_id
                        }
                    });
                    marked++;
                    addLog(`Marked for discontinuation: "${product.title}" (SKU: ${variant.sku}). Will be confirmed in ~20 hours.`, 'warning');
                }
            }
        }
        addLog(`Marking complete: ${marked} newly marked, ${alreadyPending} already pending.`, 'success');
    } catch (e) {
        addLog(`Critical error in marking job: ${e.message}`, 'error', e);
        // This is a validation/fetch error, so we trigger a failsafe
        failsafeTriggered = true;
        failsafeReason = `Discontinuation marking failed: ${e.message}`;
        pendingFailsafeAction = null; // No override for this, it's a data integrity issue
        await notifyTelegram(`<b>APIFY SYNC: FAILSAFE (Marking Job)</b>\n${e.message}`);
    }
}

// NEW: This job confirms and executes the discontinuation
async function confirmDiscontinuedJob(token) {
    addLog('Starting Phase 2: Confirming and processing discontinued products...', 'info');
    let confirmed = 0, stillPresent = 0, errors = 0;
    const PENDING_PERIOD_MS = 20 * 60 * 60 * 1000; // 20 hours

    if (pendingDiscontinuation.size === 0) {
        addLog('No products pending discontinuation. Exiting.', 'info');
        return;
    }
    
    try {
        addLog(`Checking ${pendingDiscontinuation.size} products in the pending queue...`, 'info');
        const apifyData = await getApifyProducts(); // Get fresh, validated data
        const apifySkus = new Set(processApifyProducts(apifyData, { processPrice: false }).map(p => p.sku.toLowerCase()));

        for (const [sku, item] of pendingDiscontinuation.entries()) {
            if (shouldAbort(token)) break;

            if (Date.now() - item.timestamp < PENDING_PERIOD_MS) {
                continue; // Not old enough to process
            }

            if (apifySkus.has(sku)) {
                // The product has reappeared in the feed, so it's not discontinued.
                stillPresent++;
                addLog(`Product "${item.productInfo.title}" (SKU: ${sku}) reappeared in feed. Removing from pending list.`, 'success');
                pendingDiscontinuation.delete(sku);
            } else {
                // It's past the pending period and STILL not in the feed. Discontinue it.
                addLog(`Confirming discontinuation for "${item.productInfo.title}" (SKU: ${sku}).`, 'warning');
                try {
                    const { id, inventory_item_id } = item.productInfo;
                    // Set inventory to 0
                    if (inventory_item_id) {
                        await shopifyRequestWithRetry(shopifyClient.post, '/inventory_levels/set.json', {
                            inventory_item_id: inventory_item_id,
                            location_id: config.shopify.locationId,
                            available: 0
                        });
                    }
                    // Set status to draft
                    await shopifyRequestWithRetry(shopifyClient.put, `/products/${id}.json`, {
                        product: { id, status: 'draft' }
                    });
                    
                    confirmed++;
                    stats.discontinued++; // Update global stat here
                    addLog(`✓ Discontinued product: "${item.productInfo.title}" (SKU: ${sku})`, 'success');
                    pendingDiscontinuation.delete(sku); // Remove from queue after success
                } catch(e) {
                    errors++;
                    addLog(`Error processing confirmed discontinuation for "${item.productInfo.title}": ${e.message}`, 'error', e);
                }
            }
        }
        addLog(`Confirmation complete: ${confirmed} discontinued, ${stillPresent} removed from queue, ${errors} errors.`, 'success');
    } catch(e) {
        addLog(`Critical error in confirmation job (likely fetch-related): ${e.message}`, 'error', e);
        // We don't want to clear the pending queue if the check fails. It will try again tomorrow.
    }
}


async function generateAndSendErrorReport() { /* ... same as before ... */ }

// --- UI AND API (with new additions) ---
app.get('/', (req, res) => {
  const status = systemPaused ? 'PAUSED' : (failsafeTriggered ? 'FAILSAFE' : 'RUNNING');
  // MODIFIED: Added pending discontinuation stat
  res.send(`<!DOCTYPE html><html><head><title>Shopify Sync</title><meta name="viewport" content="width=device-width, initial-scale=1"><link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0-alpha1/dist/css/bootstrap.min.css" rel="stylesheet"><link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css"><style>:root{--primary-color:#4361ee;--secondary-color:#3a0ca3;--success-color:#4cc9f0;--warning-color:#f72585;--info-color:#4895ef;--light-color:#f8f9fa;--dark-color:#212529}body{font-family:'Inter',-apple-system,BlinkMacSystemFont,sans-serif;background-color:#f7f7f9;color:#333;line-height:1.6;padding:0;margin:0}.navbar{background:linear-gradient(90deg,var(--primary-color),var(--secondary-color));box-shadow:0 4px 12px rgba(0,0,0,.1);padding:1rem 2rem;color:white;margin-bottom:2rem}.navbar h1{margin:0;font-weight:700;font-size:1.5rem}.card{border:none;border-radius:12px;box-shadow:0 5px 15px rgba(0,0,0,.05);transition:transform .3s,box-shadow .3s;margin-bottom:25px;overflow:hidden}.card:hover{transform:translateY(-5px);box-shadow:0 10px 20px rgba(0,0,0,.1)}.card-header{background:white;padding:1.25rem 1.5rem;font-weight:600;border-bottom:1px solid rgba(0,0,0,.05);display:flex;align-items:center;justify-content:space-between}.card-body{padding:1.5rem}.status-badge{font-size:.9rem;padding:.5rem 1rem;border-radius:50px;font-weight:500;text-transform:uppercase;letter-spacing:.5px;box-shadow:0 3px 8px rgba(0,0,0,.1)}.btn{border-radius:50px;padding:.6rem 1.5rem;font-weight:500;text-transform:capitalize;letter-spacing:.3px;transition:all .3s;box-shadow:0 3px 6px rgba(0,0,0,.1);margin:.25rem}.btn:hover{transform:translateY(-2px);box-shadow:0 5px 10px rgba(0,0,0,.15)}.btn i{margin-right:5px}.btn-primary{background:linear-gradient(45deg,var(--primary-color),var(--info-color));border:none}.btn-warning{background:linear-gradient(45deg,#f72585,#ff9e00);border:none;color:white}.btn-success{background:linear-gradient(45deg,#06d6a0,#1b9aaa);border:none}.btn-danger{background:linear-gradient(45deg,#ef476f,#ffd166);border:none}.btn-secondary{background:linear-gradient(45deg,#8338ec,#3a86ff);border:none;color:white}.stat-card{padding:1rem;border-radius:10px;box-shadow:0 3px 10px rgba(0,0,0,.05);background:white;text-align:center;transition:all .3s}.stat-card:hover{transform:translateY(-3px);box-shadow:0 5px 15px rgba(0,0,0,.1)}.stat-card h3{font-size:2rem;font-weight:700;margin:.5rem 0;background:linear-gradient(45deg,var(--primary-color),var(--info-color));-webkit-background-clip:text;background-clip:text;-webkit-text-fill-color:transparent}.stat-card p{margin:0;font-size:.9rem;color:#6c757d;font-weight:500}.logs-container{max-height:500px;overflow-y:auto;border-radius:8px;background:#f8f9fa;padding:1rem}.log-entry{padding:.75rem 1rem;margin-bottom:.5rem;border-radius:6px;background:white;box-shadow:0 2px 5px rgba(0,0,0,.02);border-left:4px solid #dee2e6;font-size:.9rem}.log-entry small{display:block;font-size:.75rem;opacity:.7;margin-bottom:.25rem}.log-error{border-left-color:#ef476f;background:#fff5f7}.log-warning{border-left-color:#ffd166;background:#fff9eb}.log-success{border-left-color:#06d6a0;background:#f0fff4}.alert{border-radius:10px;padding:1rem 1.5rem;border:none;box-shadow:0 3px 10px rgba(0,0,0,.05)}.bg-running{background:linear-gradient(-45deg,#06d6a0,#1b9aaa,#4cc9f0,#3a86ff);background-size:400% 400%;animation:gradient 3s ease infinite}.bg-paused{background:linear-gradient(-45deg,#ffd166,#ffbd00,#ff9e00,#ff7700);background-size:400% 400%;animation:gradient 3s ease infinite}.bg-failsafe{background:linear-gradient(-45deg,#ef476f,#f72585,#b5179e,#7209b7);background-size:400% 400%;animation:gradient 3s ease infinite}@keyframes gradient{0%{background-position:0 50%}50%{background-position:100% 50%}100%{background-position:0 50%}}.fetch-health{background:#f8f9fa;padding:1rem;border-radius:8px;font-size:.85rem; margin-bottom: 1rem;}</style></head><body><nav class="navbar"><div class="container-fluid"><h1><i class="fas fa-sync-alt me-2"></i> Shopify Sync Dashboard</h1></div></nav><div class="container"><div class="row mb-4"><div class="col-lg-7"><div class="card"><div class="card-header"><h5 class="mb-0"><i class="fas fa-tachometer-alt me-2"></i>System Status</h5><span class="badge ${status==='RUNNING'?'bg-running':status==='PAUSED'?'bg-paused':'bg-failsafe'} status-badge"><i class="fas ${status==='RUNNING'?'fa-play-circle':status==='PAUSED'?'fa-pause-circle':'fa-exclamation-triangle'} me-1"></i>${status}</span></div><div class="card-body"><div class="row mb-4"><div class="col-6 col-md-3"><div class="stat-card"><p>New Products</p><h3>${stats.newProducts}</h3></div></div><div class="col-6 col-md-3"><div class="stat-card"><p>Inv. Updates</p><h3>${stats.inventoryUpdates}</h3></div></div><div class="col-6 col-md-3"><div class="stat-card"><p>Discontinued</p><h3>${stats.discontinued}</h3></div></div><div class="col-6 col-md-3"><div class="stat-card"><p>Pending</p><h3>${pendingDiscontinuation.size}</h3></div></div></div><div class="fetch-health"><strong>Fetch Validation Settings:</strong><br>Apify: ${FETCH_VALIDATION.MIN_APIFY_PRODUCTS}-${FETCH_VALIDATION.MAX_APIFY_PRODUCTS} | Shopify Supplier: Min ${FETCH_VALIDATION.MIN_SHOPIFY_SUPPLIER_PRODUCTS}<br>Required Match: ${FETCH_VALIDATION.REQUIRED_MATCH_PERCENTAGE}% | Max Data Age: ${FETCH_VALIDATION.MAX_APIFY_DATA_AGE_HOURS}h | Core SKUs: ${CORE_SKUS.length > 0 ? CORE_SKUS.length : 'None'}</div><div class="d-flex flex-wrap justify-content-center gap-2 mt-3">${systemPaused?`<button class="btn btn-success" onclick="fetch('/api/pause',{method:'POST',body:JSON.stringify({paused:!1}),headers:{'Content-Type':'application/json'}}).then(()=>location.reload())"><i class="fas fa-play"></i> Resume</button>`:`<button class="btn btn-warning" onclick="fetch('/api/pause',{method:'POST',body:JSON.stringify({paused:!0}),headers:{'Content-Type':'application/json'}}).then(()=>location.reload())"><i class="fas fa-pause"></i> Pause</button>`}</div>${failsafeTriggered?`<div class="alert alert-danger mt-3 text-center"><h5 class="alert-heading"><i class="fas fa-exclamation-triangle me-2"></i>FAILSAFE TRIGGERED</h5><p>${failsafeReason}</p><hr><div class="d-flex justify-content-center gap-2 mt-3">${pendingFailsafeAction ? `<button class="btn btn-success" onclick="fetch('/api/failsafe/confirm',{method:'POST'}).then(()=>location.reload())"><i class="fas fa-check-circle"></i> Proceed</button>` : ''}<button class="btn btn-danger" onclick="fetch('/api/failsafe/abort',{method:'POST'}).then(()=>location.reload())"><i class="fas fa-times-circle"></i> Abort & Clear</button></div></div>`:``}</div></div></div><div class="col-lg-5"><div class="card"><div class="card-header"><h5 class="mb-0"><i class="fas fa-tools me-2"></i>Manual Actions</h5></div><div class="card-body"><div class="d-flex flex-wrap justify-content-center gap-2"><button class="btn btn-primary" onclick="runSync('inventory')" ${failsafeTriggered ? 'disabled' : ''}><i class="fas fa-boxes"></i> Sync Inv.</button><button class="btn btn-primary" onclick="runSync('products')" ${failsafeTriggered ? 'disabled' : ''}><i class="fas fa-plus-circle"></i> Sync New</button><button class="btn btn-primary" onclick="runSync('mark-discontinued')" ${failsafeTriggered ? 'disabled' : ''}><i class="fas fa-archive"></i> Mark Discontinued</button><button class="btn btn-warning" onclick="runSync('improved-map-skus')" ${failsafeTriggered ? 'disabled' : ''}><i class="fas fa-map-signs"></i> Map SKUs</button><button class="btn btn-secondary" onclick="runSync('deduplicate')" ${failsafeTriggered ? 'disabled' : ''}><i class="fas fa-clone"></i> De-Duplicate</button><button class="btn btn-danger" onclick="if(confirm('DANGER: This will permanently delete all supplier products that cannot be matched. Are you sure?')) runSync('cleanse-unmatched')" ${failsafeTriggered ? 'disabled' : ''}><i class="fas fa-trash-alt"></i> Cleanse</button></div></div></div></div></div><div class="row"><div class="col-md-12"><div class="card"><div class="card-header"><h5 class="mb-0"><i class="fas fa-list-alt me-2"></i>Logs</h5></div><div class="card-body"><div class="logs-container">${logs.map(log=>`<div class="log-entry ${log.type==='error'?'log-error':log.type==='warning'?'log-warning':log.type==='success'?'log-success':''}"><small>${new Date(log.timestamp).toLocaleString()}</small><span>${log.message}</span></div>`).join('')}</div></div></div></div></div></div><script>function runSync(t){fetch("/api/sync/"+t,{method:"POST"}).then(t=>t.json()).then(t=>{1===t.s?(alert("Job started successfully!"),setTimeout(()=>location.reload(),1e3)):alert(t.msg||"Job already running. Try again later.")}).catch(t=>{alert("Error: "+t)})}setTimeout(()=>location.reload(),60000);</script></body></html>`);
});

// ... (GET /api/status, POST /api/pause, failsafe routes are unchanged)
app.get('/api/status', (req, res) => res.json({ stats, lastRun, logs, systemPaused, failsafeTriggered, failsafeReason, errorSummary: Array.from(errorSummary.entries()).map(([msg, count]) => ({msg, count})), fetchHistory, fetchValidation, pendingDiscontinuationCount: pendingDiscontinuation.size }));
app.post('/api/pause', (req, res) => { const { paused } = req.body; if (paused === undefined) return res.status(400).json({ s: 0, msg: 'Missing paused parameter' }); systemPaused = Boolean(paused); addLog(`System ${systemPaused ? 'paused' : 'resumed'} by user`, 'warning'); if (!systemPaused) { abortVersion++; } return res.json({ s: 1 }); });
app.post('/api/failsafe/confirm', (req, res) => { if (!failsafeTriggered || !pendingFailsafeAction) { return res.status(400).json({ s: 0, msg: 'No pending failsafe action to confirm.' }); } addLog('Failsafe action confirmed by user. Proceeding with override...', 'warning'); const actionToRun = pendingFailsafeAction; failsafeTriggered = false; failsafeReason = ''; pendingFailsafeAction = null; abortVersion++; actionToRun(); return res.json({ s: 1 }); });
app.post('/api/failsafe/abort', (req, res) => { if (!failsafeTriggered) { return res.status(400).json({ s: 0, msg: 'No failsafe is currently triggered.' }); } addLog('Failsafe aborted by user. System returning to normal.', 'warning'); failsafeTriggered = false; failsafeReason = ''; pendingFailsafeAction = null; abortVersion++; return res.json({ s: 1 }); });


// NEW: Endpoint to view pending discontinuations
app.get('/api/pending-discontinuations', (req, res) => {
    const pendingArray = Array.from(pendingDiscontinuation.values()).map(item => ({
        ...item.productInfo,
        pendingSince: new Date(item.timestamp).toISOString()
    }));
    res.json(pendingArray);
});

// NEW: Endpoint to manually update validation baselines
app.post('/api/update-baseline', (req, res) => {
    const { minApify, maxApify, minShopify } = req.body;
    let updated = [];
    if (minApify && !isNaN(minApify)) { FETCH_VALIDATION.MIN_APIFY_PRODUCTS = Number(minApify); updated.push('minApify'); }
    if (maxApify && !isNaN(maxApify)) { FETCH_VALIDATION.MAX_APIFY_PRODUCTS = Number(maxApify); updated.push('maxApify'); }
    if (minShopify && !isNaN(minShopify)) { FETCH_VALIDATION.MIN_SHOPIFY_SUPPLIER_PRODUCTS = Number(minShopify); updated.push('minShopify'); }
    
    if (updated.length > 0) {
        const msg = `Validation baselines updated by user: ${updated.join(', ')}`;
        addLog(msg, 'warning');
        return res.json({ s: 1, msg });
    }
    return res.status(400).json({ s: 0, msg: 'No valid parameters provided.' });
});

// MODIFIED: Sync endpoint map updated for new discontinuation flow
app.post('/api/sync/:type', (req, res) => {
  if (failsafeTriggered) {
    return res.status(409).json({s: 0, msg: 'System is in failsafe mode.'});
  }
  const jobMap = {
    inventory: { name: 'Inventory Sync', fn: updateInventoryJob },
    products: { name: 'New Product Sync', fn: createNewProductsJob },
    'mark-discontinued': { name: 'Mark Discontinued', fn: markForDiscontinuationJob },
    'improved-map-skus': { name: 'Comprehensive SKU Mapping', fn: improvedMapSkusJob },
    deduplicate: { name: 'Find & Delete Duplicates', fn: deduplicateProductsJob },
    'cleanse-unmatched': { name: 'Cleanse Unmatched Products', fn: cleanseUnmatchedProductsJob }
  };
  const { type } = req.params;
  const job = jobMap[type];
  if (!job) return res.status(400).json({s: 0, msg: 'Invalid job type'});
  
  const jobKey = type === 'mark-discontinued' ? 'markDiscontinued' : type;
  return startBackgroundJob(jobKey, `Manual ${job.name}`, t => job.fn(t)) 
    ? res.json({s: 1}) 
    : res.status(409).json({s: 0, msg: 'Job already running'});
});

// MODIFIED: Scheduled jobs updated for new two-phase flow
cron.schedule('0 1 * * *', () => { if (!systemPaused && !failsafeTriggered) startBackgroundJob('inventory', 'Scheduled Inventory Sync', t => updateInventoryJob(t)); });
cron.schedule('0 2 * * 5', () => { if (!systemPaused && !failsafeTriggered) startBackgroundJob('products', 'Scheduled Product Sync', t => createNewProductsJob(t)); });
cron.schedule('0 3 * * *', () => { if (!systemPaused && !failsafeTriggered) startBackgroundJob('markDiscontinued', 'Scheduled Discontinued Marking', t => markForDiscontinuationJob(t)); });
// NEW: Cron job to run the confirmation step
cron.schedule('0 4 * * *', () => { if (!systemPaused && !failsafeTriggered) startBackgroundJob('confirmDiscontinued', 'Scheduled Discontinued Confirmation', t => confirmDiscontinuedJob(t)); });
cron.schedule('0 9 * * 0', () => startBackgroundJob('errorReport', 'Weekly Error Report', () => generateAndSendErrorReport()));

// ... (Server start and signal handlers are unchanged)
const server = app.listen(PORT, () => { addLog(`Server started on port ${PORT}`, 'success'); const missing = ['APIFY_TOKEN', 'SHOPIFY_ACCESS_TOKEN', 'SHOPIFY_DOMAIN', 'SHOPIFY_LOCATION_ID'].filter(key => !process.env[key]); if (missing.length > 0) { addLog(`FATAL: Missing environment variables: ${missing.join(', ')}`, 'error'); process.exit(1); } if(CORE_SKUS.length === 0) { addLog('WARNING: No CORE_SKUS defined in .env file. SKU continuity check will be skipped.', 'warning'); } else { addLog(`Core SKU continuity check is active for ${CORE_SKUS.length} SKUs.`, 'info'); } addLog(`Fetch validation configured: Apify ${FETCH_VALIDATION.MIN_APIFY_PRODUCTS}-${FETCH_VALIDATION.MAX_APIFY_PRODUCTS}, Shopify min ${FETCH_VALIDATION.MIN_SHOPIFY_SUPPLIER_PRODUCTS}`, 'info'); });
process.on('SIGTERM', () => { addLog('SIGTERM received, shutting down...', 'warning'); server.close(() => process.exit(0)); });
process.on('SIGINT', () => { addLog('SIGINT received, shutting down...', 'warning'); server.close(() => process.exit(0)); });
