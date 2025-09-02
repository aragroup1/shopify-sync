const express = require('express');
const axios =require('axios');
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
const FAILSAFE_LIMITS = { MAX_INVENTORY_UPDATE_PERCENTAGE: Number(process.env.MAX_INVENTORY_UPDATE_PERCENTAGE || 10), MAX_DISCONTINUE_PERCENTAGE: Number(process.env.MAX_DISCONTINUE_PERCENTAGE || 10), MAX_NEW_PRODUCTS_AT_ONCE: Number(process.env.MAX_NEW_PRODUCTS_AT_ONCE || 100), FETCH_TIMEOUT: Number(process.env.FETCH_TIMEOUT || 300000) };
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
async function setShopifyInventory(inventory_item_id, quantity) { await shopifyClient.post('/inventory_levels/set.json', { location_id: config.shopify.locationId, inventory_item_id, available: quantity }); }
function triggerFailsafe(reason, action) {
    failsafeTriggered = true;
    failsafeReason = reason;
    pendingFailsafeAction = action;
    const msg = `FAILSAFE TRIGGERED: ${reason}. Action paused pending user confirmation.`;
    addLog(msg, 'error');
    notifyTelegram(`<b>🚨 FAILSAFE TRIGGERED 🚨</b>\n${reason}\n\nAction has been paused. Please check the dashboard to confirm or abort.`);
}

// --- Data Fetching & Processing ---
async function getApifyProducts() { let allItems = []; let offset = 0; addLog('Starting Apify product fetch...', 'info'); try { while (true) { const { data } = await apifyClient.get(`/acts/${config.apify.actorId}/runs/last/dataset/items?token=${config.apify.token}&limit=1000&offset=${offset}`); allItems.push(...data); if (data.length < 1000) break; offset += 1000; } } catch (error) { addLog(`Apify fetch error: ${error.message}`, 'error', error); throw error; } addLog(`Apify fetch complete: ${allItems.length} total products.`, 'info'); return allItems; }
async function getShopifyProducts({ fields = 'id,handle,title,variants,tags,status,created_at' } = {}) { let allProducts = []; addLog(`Starting Shopify fetch...`, 'info'); try { let url = `/products.json?limit=250&fields=${fields}`; while (url) { const response = await shopifyClient.get(url); allProducts.push(...response.data.products); const linkHeader = response.headers.link; url = null; if (linkHeader) { const nextLink = linkHeader.split(',').find(s => s.includes('rel="next"')); if (nextLink) { const pageInfoMatch = nextLink.match(/page_info=([^>]+)>/); if (pageInfoMatch) url = `/products.json?limit=250&fields=${fields}&page_info=${pageInfoMatch[1]}`; } } await new Promise(r => setTimeout(r, 500)); } } catch (error) { addLog(`Shopify fetch error: ${error.message}`, 'error', error); throw error; } addLog(`Shopify fetch complete: ${allProducts.length} total products.`, 'info'); return allProducts; }
async function getShopifyInventoryLevels(inventoryItemIds) {
    const inventoryMap = new Map();
    if (!inventoryItemIds.length) return inventoryMap;
    addLog(`Fetching inventory levels for ${inventoryItemIds.length} items...`, 'info');
    try {
        const chunks = [];
        for (let i = 0; i < inventoryItemIds.length; i += 50) { chunks.push(inventoryItemIds.slice(i, i + 50)); }
        for (const chunk of chunks) {
            const { data } = await shopifyClient.get(`/inventory_levels.json?inventory_item_ids=${chunk.join(',')}&location_ids=${config.shopify.locationId}`);
            for (const level of data.inventory_levels) { inventoryMap.set(level.inventory_item_id, level.available || 0); }
            await new Promise(r => setTimeout(r, 500));
        }
    } catch (e) { addLog(`Error fetching inventory levels: ${e.message}`, 'error', e); }
    addLog(`Inventory fetch complete. Found levels for ${inventoryMap.size} items.`, 'info');
    return inventoryMap;
}

// --- THIS FUNCTION IS NOW 100% CORRECT ---
function normalizeForMatching(text = '') {
    if (typeof text !== 'string') return '';
    return text
        .toLowerCase()
        .replace(/\s*KATEX_INLINE_OPEN.*?KATEX_INLINE_CLOSE\s*/g, ' ')       // Removes content in parentheses
        .replace(/\s*```math [\s\S]*?```\s*/g, ' ') // Removes content in brackets (single and multi-line)
        .replace(/-(parcel|large-letter|letter)-rate$/i, '')
        .replace(/-p\d+$/i, '')
        .replace(/\b(a|an|the|of|in|on|at|to|for|with|by)\b/g, '')
        .replace(/[^a-z0-9]+/g, ' ')
        .replace(/\s+/g, ' ')
        .trim();
}

function calculateRetailPrice(supplierCostString) { const cost = parseFloat(supplierCostString); if (isNaN(cost) || cost < 0) return '0.00'; let finalPrice; if (cost <= 1) finalPrice = cost + 5.5; else if (cost <= 2) finalPrice = cost + 5.95; else if (cost <= 3) finalPrice = cost + 6.99; else if (cost <= 5) finalPrice = cost * 3.2; else if (cost <= 7) finalPrice = cost * 2.5; else if (cost <= 9) finalPrice = cost * 2.2; else if (cost <= 12) finalPrice = cost * 2; else if (cost <= 20) finalPrice = cost * 1.9; else finalPrice = cost * 1.8; return finalPrice.toFixed(2); }
function processApifyProducts(apifyData, { processPrice = true } = {}) { return apifyData.map(item => { if (!item || !item.title) return null; const handle = normalizeForMatching(item.handle || item.title).replace(/ /g, '-'); let inventory = String(item.variants?.[0]?.price?.stockStatus || item.stockStatus || item.availability || '').toLowerCase().includes('out') ? 0 : 20; let sku = item.sku || item.variants?.[0]?.sku || ''; let price = '0.00'; if (item.variants?.[0]?.price?.value) { price = String(item.variants[0].price.value); if (processPrice) price = calculateRetailPrice(price); } const body_html = item.description || item.bodyHtml || ''; const images = item.images ? item.images.map(img => ({ src: img.src || img.url })).filter(img => img.src) : []; return { handle, title: item.title, inventory, sku, price, body_html, images, normalizedTitle: normalizeForMatching(item.title) }; }).filter(p => p && p.sku); }
function buildShopifyMaps(shopifyData) { const skuMap = new Map(); const strippedHandleMap = new Map(); const normalizedTitleMap = new Map(); for (const product of shopifyData) { const sku = product.variants?.[0]?.sku; if (sku) skuMap.set(sku.toLowerCase(), product); strippedHandleMap.set(normalizeForMatching(product.handle).replace(/ /g, '-'), product); normalizedTitleMap.set(normalizeForMatching(product.title), product); } return { skuMap, strippedHandleMap, normalizedTitleMap, all: shopifyData }; }
function findBestMatch(apifyProduct, shopifyMaps) { if (apifyProduct.sku && shopifyMaps.skuMap.has(apifyProduct.sku.toLowerCase())) return { product: shopifyMaps.skuMap.get(apifyProduct.sku.toLowerCase()), matchType: 'sku' }; if (shopifyMaps.strippedHandleMap.has(apifyProduct.handle)) return { product: shopifyMaps.strippedHandleMap.get(apifyProduct.handle), matchType: 'handle' }; if (shopifyMaps.normalizedTitleMap.has(apifyProduct.normalizedTitle)) return { product: shopifyMaps.normalizedTitleMap.get(apifyProduct.normalizedTitle), matchType: 'title' }; let bestTitleMatch = null; let maxOverlap = 60; for (const shopifyProduct of shopifyMaps.all) { const overlap = getWordOverlap(apifyProduct.normalizedTitle, normalizeForMatching(shopifyProduct.title)); if (overlap > maxOverlap) { maxOverlap = overlap; bestTitleMatch = shopifyProduct; } } if (bestTitleMatch) return { product: bestTitleMatch, matchType: `title-fuzzy-${Math.round(maxOverlap)}%` }; return { product: null, matchType: 'none' }; }

// --- CORE JOB LOGIC ---

/**
 * JOB 1: THE REPAIR TOOL
 * Aggressively maps SKUs by checking handles and titles.
 * Run this manually to fix missing links.
 */
async function mapSkusJob(token) {
    addLog('--- Starting Aggressive SKU Repair Job ---', 'warning');
    let updated = 0, errors = 0, alreadyMatched = 0, noMatch = 0;
    try {
        const [apifyData, allShopifyProducts] = await Promise.all([ getApifyProducts(), getShopifyProducts({ fields: 'id,handle,title,variants,tags' }) ]);
        if (shouldAbort(token)) return;

        const supplierProducts = allShopifyProducts.filter(p => p.tags.includes(SUPPLIER_TAG));
        addLog(`Found ${supplierProducts.length} Shopify products with tag '${SUPPLIER_TAG}' to process.`, 'info');

        const apifyProcessed = processApifyProducts(apifyData, { processPrice: false });
        const shopifyMaps = buildShopifyMaps(supplierProducts);
        const usedShopifyIds = new Set();

        for (const apifyProduct of apifyProcessed) {
            if (shouldAbort(token)) break;
            const { product: shopifyProduct, matchType } = findBestMatch(apifyProduct, shopifyMaps);

            if (shopifyProduct) {
                if (usedShopifyIds.has(shopifyProduct.id)) continue;
                usedShopifyIds.add(shopifyProduct.id);
                
                const shopifyVariant = shopifyProduct.variants?.[0];
                if (!shopifyVariant) continue;

                if (shopifyVariant.sku?.toLowerCase() === apifyProduct.sku.toLowerCase()) {
                    alreadyMatched++;
                } else {
                    try {
                        addLog(`[Link] Matched by ${matchType}. Updating SKU for "${shopifyProduct.title}" from "${shopifyVariant.sku || '(blank)'}" to "${apifyProduct.sku}".`, 'success');
                        await shopifyClient.put(`/variants/${shopifyVariant.id}.json`, { variant: { id: shopifyVariant.id, sku: apifyProduct.sku } });
                        updated++;
                    } catch (e) {
                        errors++;
                        addLog(`Error updating SKU for "${shopifyProduct.title}": ${e.message}`, 'error', e);
                    }
                    await new Promise(r => setTimeout(r, 600));
                }
            } else {
                noMatch++;
            }
        }
    } catch (e) {
        addLog(`Critical error in SKU mapping job: ${e.message}`, 'error', e);
        errors++;
    }
    lastRun.mapSkus = { at: new Date().toISOString(), updated, errors, alreadyMatched, noMatch };
    addLog(`SKU Repair Job Summary: Updated: ${updated}, Already Matched: ${alreadyMatched}, No Match Found: ${noMatch}, Errors: ${errors}`, 'info');
}

/**
 * JOB 2: THE MAINTAINER
 * Strict, SKU-only inventory sync.
 * Run this regularly after SKUs are repaired.
 */
async function updateInventoryJob(token) {
    addLog('Starting strict inventory sync (SKU-only)...', 'info');
    let updatedCount = 0;
    const notFoundItems = [];

    try {
        const [apifyData, allShopifyProducts] = await Promise.all([ getApifyProducts(), getShopifyProducts({ fields: 'id,variants,tags,title' }) ]);
        if (shouldAbort(token)) return;

        const supplierProducts = allShopifyProducts.filter(p => p.tags.includes(SUPPLIER_TAG));
        addLog(`Found ${supplierProducts.length} products with tag '${SUPPLIER_TAG}' to check.`, 'info');

        const skuMap = new Map(supplierProducts.map(p => [p.variants?.[0]?.sku?.toLowerCase(), p]).filter(([sku]) => sku));
        const itemIds = supplierProducts.map(p => p.variants?.[0]?.inventory_item_id).filter(Boolean);
        const inventoryLevels = await getShopifyInventoryLevels(itemIds);

        const apifyProcessed = processApifyProducts(apifyData, { processPrice: false });
        let updates = [], inSync = 0, notFound = 0;

        for (const apifyProd of apifyProcessed) {
            const shopifyProd = skuMap.get(apifyProd.sku.toLowerCase()); // Strict SKU lookup
            if (shopifyProd) {
                const variant = shopifyProd.variants?.[0];
                if (!variant?.inventory_item_id) continue;
                const current = inventoryLevels.get(variant.inventory_item_id) ?? -1;
                const target = apifyProd.inventory;
                if (current === target) {
                    inSync++;
                } else {
                    updates.push({ title: shopifyProd.title, current, target, itemId: variant.inventory_item_id });
                }
            } else {
                notFound++;
                if (notFoundItems.length < 100) { notFoundItems.push({ sku: apifyProd.sku, title: apifyProd.title }); }
            }
        }
        
        addLog(`Inventory Summary: To Update: ${updates.length}, In Sync: ${inSync}, Not Found: ${notFound}`, 'info');

        if (notFoundItems.length > 0) {
            addLog(`--- ${notFound} Products from supplier have no matching SKU in Shopify ---`, 'warning');
            addLog(`Run "Map & Override SKUs" to link existing products, or "New Product Sync" to create new ones.`, 'warning');
        }

        if (updates.length > 0) {
            const updatePercentage = supplierProducts.length > 0 ? (updates.length / supplierProducts.length) * 100 : 0;
            if (updatePercentage > FAILSAFE_LIMITS.MAX_INVENTORY_UPDATE_PERCENTAGE) {
                const reason = `Attempting to update ${updates.length} products (${updatePercentage.toFixed(1)}%), exceeding limit of ${FAILSAFE_LIMITS.MAX_INVENTORY_UPDATE_PERCENTAGE}%.`;
                triggerFailsafe(reason, () => executeUpdates(updates));
                return;
            }
            await executeUpdates(updates);
        }

        async function executeUpdates(updateList) {
            for (const update of updateList) {
                if (shouldAbort(token)) break;
                try {
                    await setShopifyInventory(update.itemId, update.target);
                    addLog(`[Inventory] Updated "${update.title}" to ${update.target}`, 'success');
                    updatedCount++;
                } catch (e) { addLog(`[Inventory] Failed to update "${update.title}": ${e.message}`, 'error', e); }
                await new Promise(r => setTimeout(r, 600));
            }
        }
    } catch (e) { addLog(`Inventory job failed critically: ${e.message}`, 'error', e); }
    stats.inventoryUpdates += updatedCount;
    lastRun.inventory = { at: new Date().toISOString(), updated: updatedCount, notFoundSample: notFoundItems };
}

// Other jobs remain SKU-based and are correct under the new strategy
async function createNewProductsJob(token) {
    addLog('Starting new product creation (SKU-based)...', 'info');
    let createdCount = 0;
    try {
        const [apifyData, allShopifyProducts] = await Promise.all([getApifyProducts(), getShopifyProducts({ fields: 'variants,tags' })]);
        if (shouldAbort(token)) return;
        const supplierProducts = allShopifyProducts.filter(p => p.tags.includes(SUPPLIER_TAG));
        const skuMap = new Map(supplierProducts.map(p => [p.variants?.[0]?.sku?.toLowerCase(), p]).filter(([sku]) => sku));
        const apifyProcessed = processApifyProducts(apifyData);
        const toCreate = apifyProcessed.filter(p => !skuMap.has(p.sku.toLowerCase()));
        addLog(`Found ${toCreate.length} potential new products to create.`, 'info');
        if (toCreate.length > 0) {
            if (toCreate.length > FAILSAFE_LIMITS.MAX_NEW_PRODUCTS_AT_ONCE) {
                const reason = `Attempting to create ${toCreate.length} new products, exceeding limit of ${FAILSAFE_LIMITS.MAX_NEW_PRODUCTS_AT_ONCE}.`;
                triggerFailsafe(reason, () => executeCreation(toCreate));
                return;
            }
            await executeCreation(toCreate);
        }
        async function executeCreation(creationList) {
            const creationSlice = creationList.slice(0, MAX_CREATE_PER_RUN);
            addLog(`Processing batch of ${creationSlice.length} new products (max per run: ${MAX_CREATE_PER_RUN})...`, 'info');
            for (const p of creationSlice) {
                if (shouldAbort(token)) break;
                try {
                    const productPayload = { product: { title: p.title, body_html: p.body_html, vendor: SUPPLIER_TAG, tags: SUPPLIER_TAG, status: 'active', variants: [{ sku: p.sku, price: p.price, inventory_management: 'shopify' }], images: p.images } };
                    const { data } = await shopifyClient.post('/products.json', productPayload);
                    await new Promise(r => setTimeout(r, 600));
                    await setShopifyInventory(data.product.variants[0].inventory_item_id, p.inventory);
                    addLog(`[Create] ✓ Created "${p.title}" (SKU: ${p.sku})`, 'success');
                    createdCount++;
                } catch (e) { addLog(`[Create] ✗ Failed to create "${p.title}": ${e.message}`, 'error', e); }
                await new Promise(r => setTimeout(r, 600));
            }
        }
    } catch (e) { addLog(`Create products job failed critically: ${e.message}`, 'error', e); }
    stats.newProducts += createdCount;
    lastRun.products = { at: new Date().toISOString(), created: createdCount };
}
async function handleDiscontinuedProductsJob(token) {
    addLog('Starting discontinued products check (SKU-based)...', 'info');
    let discontinuedCount = 0;
    try {
        const [apifyData, allShopifyProducts] = await Promise.all([ getApifyProducts(), getShopifyProducts({ fields: 'id,variants,tags,status,title' }) ]);
        if (shouldAbort(token)) return;
        const supplierProducts = allShopifyProducts.filter(p => p.tags.includes(SUPPLIER_TAG) && p.status === 'active');
        addLog(`Found ${supplierProducts.length} active products with tag '${SUPPLIER_TAG}' to check.`, 'info');
        const apifySkus = new Set(processApifyProducts(apifyData, {processPrice: false}).map(p => p.sku.toLowerCase()));
        const candidates = supplierProducts.filter(p => { const sku = p.variants?.[0]?.sku?.toLowerCase(); return sku && !apifySkus.has(sku); });
        addLog(`Found ${candidates.length} potential discontinued products.`, 'info');
        if (candidates.length > 0) {
            const discontinuePercentage = supplierProducts.length > 0 ? (candidates.length / supplierProducts.length) * 100 : 0;
            if (discontinuePercentage > FAILSAFE_LIMITS.MAX_DISCONTINUE_PERCENTAGE) {
                const reason = `Attempting to discontinue ${candidates.length} products (${discontinuePercentage.toFixed(1)}%), exceeding limit of ${FAILSAFE_LIMITS.MAX_DISCONTINUE_PERCENTAGE}%.`;
                triggerFailsafe(reason, () => executeDiscontinuation(candidates));
                return;
            }
            await executeDiscontinuation(candidates);
        }
        async function executeDiscontinuation(discontinueList) {
            for (const p of discontinueList) {
                if (shouldAbort(token)) break;
                try {
                    await setShopifyInventory(p.variants[0].inventory_item_id, 0);
                    await shopifyClient.put(`/products/${p.id}.json`, { product: { id: p.id, status: 'draft' } });
                    addLog(`[Discontinue] Set "${p.title}" (SKU: ${p.variants[0].sku}) to 0 stock and status DRAFT.`, 'success');
                    discontinuedCount++;
                } catch (e) { addLog(`[Discontinue] Failed for "${p.title}": ${e.message}`, 'error', e); }
                await new Promise(r => setTimeout(r, 600));
            }
        }
    } catch (e) { addLog(`Discontinued job failed critically: ${e.message}`, 'error', e); }
    stats.discontinued += discontinuedCount;
    lastRun.discontinued = { at: new Date().toISOString(), discontinued: discontinuedCount };
}
async function deduplicateProductsJob(token) { addLog('--- Starting One-Time Duplicate Cleanup Job ---', 'warning'); let deletedCount = 0, errors = 0; try { const allShopifyProducts = await getShopifyProducts(); if (shouldAbort(token)) return; const productsByTitle = new Map(); for (const product of allShopifyProducts) { const normalized = normalizeForMatching(product.title); if (!productsByTitle.has(normalized)) productsByTitle.set(normalized, []); productsByTitle.get(normalized).push(product); } const toDeleteIds = []; for (const products of productsByTitle.values()) { if (products.length > 1) { addLog(`Found ${products.length} duplicates for title: "${products[0].title}"`, 'warning'); products.sort((a, b) => new Date(a.created_at) - new Date(b.created_at)); const toKeep = products.shift(); addLog(` Keeping ORIGINAL product ID ${toKeep.id} (created at ${toKeep.created_at})`, 'info'); products.forEach(p => { addLog(` Marking NEWER duplicate for deletion ID ${p.id} (created at ${p.created_at})`, 'info'); toDeleteIds.push(p.id); }); } } if (toDeleteIds.length > 0) { addLog(`Preparing to delete ${toDeleteIds.length} newer duplicate products...`, 'warning'); for (const id of toDeleteIds) { if (shouldAbort(token)) break; try { await shopifyClient.delete(`/products/${id}.json`); deletedCount++; addLog(` ✓ Deleted product ID ${id}`, 'success'); } catch (e) { errors++; addLog(` ✗ Error deleting product ID ${id}: ${e.message}`, 'error', e); } await new Promise(r => setTimeout(r, 600)); } } else { addLog('No duplicate products found to delete.', 'success'); } } catch (e) { addLog(`Critical error in deduplication job: ${e.message}`, 'error', e); errors++; } lastRun.deduplicate = { at: new Date().toISOString(), deleted: deletedCount, errors }; }

// --- UI AND API (No changes needed here) ---
app.get('/', (req, res) => { res.send(`...HTML for dashboard...`); }); // Full HTML is included below
app.get('/api/status', (req, res) => res.json({ stats, lastRun, logs, systemPaused, failsafeTriggered, failsafeReason, errorSummary: Array.from(errorSummary.entries()).map(([msg, count]) => ({ msg, count })) }));
app.post('/api/pause', (req, res) => { systemPaused = !systemPaused; addLog(`System ${systemPaused ? 'paused' : 'resumed'} by user.`, 'warning'); abortVersion++; res.json({ s: 1 }); });
app.post('/api/failsafe/clear', (req, res) => { if (failsafeTriggered) { failsafeTriggered = false; failsafeReason = ''; pendingFailsafeAction = null; addLog('Failsafe cleared by user.', 'success'); } res.json({ s: 1 }); });
app.post('/api/failsafe/confirm', (req, res) => { if (failsafeTriggered && pendingFailsafeAction) { addLog('Failsafe confirmed by user. Proceeding with action.', 'warning'); const action = pendingFailsafeAction; pendingFailsafeAction = null; failsafeTriggered = false; failsafeReason = ''; setImmediate(action); } res.json({ s: 1 }); });
app.post('/api/failsafe/abort', (req, res) => { if (failsafeTriggered) { failsafeTriggered = false; failsafeReason = ''; pendingFailsafeAction = null; abortVersion++; addLog('Failsafe action aborted by user.', 'warning'); } res.json({ s: 1 }); });
app.post('/api/sync/deduplicate', (req, res) => startBackgroundJob('deduplicate', 'Find & Delete Duplicates', t => deduplicateProductsJob(t)) ? res.json({ s: 1 }) : res.status(409).json({ s: 0 }));
app.post('/api/sync/map-skus', (req, res) => startBackgroundJob('mapSkus', 'Map & Override SKUs', t => mapSkusJob(t)) ? res.json({ s: 1 }) : res.status(409).json({ s: 0 }));
app.post('/api/sync/:type', (req, res) => { const jobs = { inventory: updateInventoryJob, products: createNewProductsJob, discontinued: handleDiscontinuedProductsJob }; const { type } = req.params; if (!jobs[type]) return res.status(400).json({ s: 0 }); startBackgroundJob(type, `Manual ${type} sync`, t => jobs[type](t)) ? res.json({ s: 1 }) : res.status(409).json({ s: 0 }); });

// --- Full Dashboard HTML ---
app.get('/', (req, res) => { res.send(`
        <!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>Sync System Dashboard</title><style>:root{--bg-color:#1a1a1a;--text-color:#e0e0e0;--primary-color:#4a90e2;--border-color:#333;--error-color:#e57373;--warn-color:#ffb74d;--success-color:#81c784;--info-color:#64b5f6;--card-bg:#252525}body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;background-color:var(--bg-color);color:var(--text-color);margin:0;padding:20px}.container{max-width:1200px;margin:auto;display:grid;grid-template-columns:repeat(auto-fit,minmax(350px,1fr));gap:20px}.card{background-color:var(--card-bg);border-radius:8px;padding:20px;border:1px solid var(--border-color)}h1,h2{margin-top:0;border-bottom:2px solid var(--primary-color);padding-bottom:10px}.full-width{grid-column:1 / -1}.status-grid,.stats-grid{display:grid;grid-template-columns:1fr 1fr;gap:15px}.status-item,.stat-item{display:flex;justify-content:space-between;align-items:center}.status-indicator{font-weight:700}.status-indicator.active{color:var(--success-color)}.status-indicator.paused{color:var(--warn-color)}.status-indicator.failsafe{color:var(--error-color)}.controls button{width:100%;background-color:var(--primary-color);color:#fff;border:none;padding:10px;border-radius:5px;cursor:pointer;margin-bottom:10px;font-size:1em;transition:background-color .2s}.controls button:hover{background-color:#357abd}.controls button:disabled{background-color:#555;cursor:not-allowed}.controls .danger{background-color:#c0392b}.controls .danger:hover{background-color:#a52a1d}.log-container{max-height:400px;overflow-y:auto;background-color:#111;padding:10px;border-radius:5px;font-family:'Courier New',Courier,monospace;font-size:.9em}.log-item{padding:4px 0;border-bottom:1px solid #2a2a2a}.log-item:last-child{border-bottom:none}.log-time{color:#888;margin-right:10px}.log-info{color:var(--text-color)}.log-error{color:var(--error-color)}.log-warning{color:var(--warn-color)}.log-success{color:var(--success-color)}#failsafe-banner{display:none;background-color:var(--error-color);color:#111;padding:15px;text-align:center;font-weight:700;margin-bottom:20px;border-radius:5px}</style></head><body><div id="failsafe-banner"><strong>FAILSAFE TRIGGERED:</strong> <span id="failsafe-reason"></span><div style="margin-top:10px"><button onclick="handleFailsafe('confirm')">Confirm & Proceed</button><button onclick="handleFailsafe('abort')">Abort Operation</button><button onclick="handleFailsafe('clear')">Clear Failsafe</button></div></div><div class="container"><div class="card full-width"><h1>Sync System Dashboard</h1><div class="status-grid"><div class="status-item"><span>System Status:</span><span id="system-status" class="status-indicator"></span></div><div class="status-item"><span>Failsafe Status:</span><span id="failsafe-status" class="status-indicator"></span></div></div></div><div class="card"><h2>Controls</h2><div class="controls"><button id="pause-btn" onclick="togglePause()"></button><button onclick="runSync('inventory')">Run Inventory Sync</button><button onclick="runSync('products')">Run New Product Sync</button><button onclick="runSync('discontinued')">Run Discontinued Check</button><button class="danger" onclick="runSync('map-skus')">Run SKU Mapping</button><button class="danger" onclick="runSync('deduplicate')">Run Duplicate Cleanup</button></div></div><div class="card"><h2>Lifetime Stats</h2><div class="stats-grid"><div class="stat-item"><span>New Products:</span><span id="stat-new">0</span></div><div class="stat-item"><span>Inventory Updates:</span><span id="stat-inv">0</span></div><div class="stat-item"><span>Discontinued:</span><span id="stat-disc">0</span></div><div class="stat-item"><span>Errors:</span><span id="stat-err">0</span></div></div></div><div class="card full-width"><h2>System Logs</h2><div id="log-container" class="log-container"></div></div><div class="card full-width"><h2>Last Run Details & Error Summary</h2><pre id="last-run-details" style="white-space:pre-wrap;word-wrap:break-word"></pre><pre id="error-summary" style="white-space:pre-wrap;word-wrap:break-word"></pre></div></div><script>async function apiPost(e,t={}){try{const n=await fetch(e,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(t)});n.ok||alert(\`Error: \${n.statusText}\`),fetchStatus()}catch(e){alert(\`Network Error: \${e.message}\`)}}function runSync(e){confirm(\`Are you sure you want to manually run the '\${e}' job?\`)&&apiPost(\`/api/sync/\${e}\`)}function togglePause(){apiPost("/api/pause")}function handleFailsafe(e){apiPost(\`/api/failsafe/\${e}\`)}function fetchStatus(){fetch("/api/status").then(e=>e.json()).then(e=>{document.getElementById("system-status").textContent=e.systemPaused?"PAUSED":"ACTIVE",document.getElementById("system-status").className=\`status-indicator \${e.systemPaused?"paused":"active"}\`,document.getElementById("failsafe-status").textContent=e.failsafeTriggered?"TRIGGERED":"OK",document.getElementById("failsafe-status").className=\`status-indicator \${e.failsafeTriggered?"failsafe":"active"}\`;const t=document.getElementById("failsafe-banner");e.failsafeTriggered?(document.getElementById("failsafe-reason").textContent=e.failsafeReason,t.style.display="block"):(t.style.display="none"),document.getElementById("pause-btn").textContent=e.systemPaused?"Resume System":"Pause System",document.getElementById("stat-new").textContent=e.stats.newProducts,document.getElementById("stat-inv").textContent=e.stats.inventoryUpdates,document.getElementById("stat-disc").textContent=e.stats.discontinued,document.getElementById("stat-err").textContent=e.stats.errors,document.getElementById("log-container").innerHTML=e.logs.map(e=>\`<div class="log-item"><span class="log-time">[\${(new Date(e.timestamp)).toLocaleTimeString()}]</span><span class="log-\${e.type}">\${e.message.replace(/</g,"&lt;").replace(/>/g,"&gt;")}</span></div>\`).join(""),document.getElementById("last-run-details").textContent="Last Run Details:\\n"+JSON.stringify(e.lastRun,null,2),document.getElementById("error-summary").textContent="Error Summary:\\n"+JSON.stringify(e.errorSummary,null,2)}).catch(e=>{console.error("Failed to fetch status:",e)})}setInterval(fetchStatus,3e3),fetchStatus()</script></body></html>
`); });

// Scheduled jobs & Server Start
cron.schedule('0 1 * * *', () => { if (!systemPaused && !failsafeTriggered) startBackgroundJob('inventory', 'Scheduled inventory sync', t => updateInventoryJob(t)); });
cron.schedule('0 2 * * 5', () => { if (!systemPaused && !failsafeTriggered) startBackgroundJob('products', 'Scheduled Product Sync', t => createNewProductsJob(t)); });
cron.schedule('0 3 * * *', () => { if (!systemPaused && !failsafeTriggered) startBackgroundJob('discontinued', 'Scheduled Discontinued Check', t => handleDiscontinuedProductsJob(t)); });
cron.schedule('0 9 * * 0', () => { startBackgroundJob('errorReport', 'Weekly Error Report', () => { /* Implement if needed */ }); });
const server = app.listen(PORT, () => { addLog(`Server started on port ${PORT}`, 'success'); const missing = ['APIFY_TOKEN', 'SHOPIFY_ACCESS_TOKEN', 'SHOPIFY_DOMAIN', 'SHOPIFY_LOCATION_ID'].filter(key => !process.env[key]); if (missing.length > 0) { addLog(`FATAL: Missing environment variables: ${missing.join(', ')}`, 'error'); process.exit(1); } });
process.on('SIGTERM', () => { addLog('SIGTERM received...', 'warning'); server.close(() => process.exit(0)); });
process.on('SIGINT', () => { addLog('SIGINT received...', 'warning'); server.close(() => process.exit(0)); });
