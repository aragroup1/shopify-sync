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
const SUPPLIER_TAG = process.env.SUPPLIER_TAG || 'Supplier:Apify'; // CORRECTED: The proper supplier tag
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

// FIXED: Fetch all products (active, draft, and archived) using separate calls
async function getShopifyProducts({ fields = 'id,handle,title,variants,tags,status,created_at' } = {}) {
    let allProducts = [];
    addLog(`Starting Shopify fetch (including active, draft, & archived)...`, 'info');

    const fetchByStatus = async (status) => {
        let products = [];
        let url = `/products.json?limit=250&fields=${fields}&status=${status}`;
        while (url) {
            const response = await shopifyClient.get(url);
            products.push(...response.data.products);
            const linkHeader = response.headers.link;
            url = null;
            if (linkHeader) {
                const nextLink = linkHeader.split(',').find(s => s.includes('rel="next"'));
                if (nextLink) {
                    const pageInfoMatch = nextLink.match(/page_info=([^>]+)>/);
                    if (pageInfoMatch) {
                        url = `/products.json?limit=250&fields=${fields}&status=${status}&page_info=${pageInfoMatch[1]}`;
                    }
                }
            }
            await new Promise(r => setTimeout(r, 500));
        }
        return products;
    };

    try {
        const activeProducts = await fetchByStatus('active');
        const draftProducts = await fetchByStatus('draft');
        const archivedProducts = await fetchByStatus('archived');

        allProducts = [...activeProducts, ...draftProducts, ...archivedProducts];
        addLog(`Shopify fetch complete: ${allProducts.length} total products (${activeProducts.length} active, ${draftProducts.length} draft, ${archivedProducts.length} archived).`, 'info');
        return allProducts;
    } catch (error) {
        addLog(`Shopify fetch error: ${error.message}`, 'error', error);
        throw error;
    }
}


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

// --- FIXED NORMALIZE FUNCTION WITHOUT PROBLEMATIC REGEX ---
function normalizeForMatching(text = '') {
  return String(text)
    .toLowerCase()
    .replace(/\s*KATEX_INLINE_OPEN.*?KATEX_INLINE_CLOSE\s*/g, ' ') // Remove content in parentheses
    .replace(/\s*```math
.*?```\s*/g, ' ') // Remove content in brackets
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

// IMPROVED SKU mapping job with detailed debugging for unmatched products
async function improvedMapSkusJob(token) { 
  addLog(`--- Starting Comprehensive SKU Mapping Job with Debug Info ---`, 'warning'); 
  let updated = 0, errors = 0, alreadyMatched = 0, noMatch = 0;
  let matchedByHandle = 0, matchedByTitle = 0, matchedByFuzzy = 0;
  let skippedNonSupplier = 0;
  
  // Debug info collections
  const unmatchedSamples = {
    noHandle: [],
    shortTitle: [],
    badFormat: [],
    closeMatches: [],
    noCloseMatches: []
  };
  
  try { 
    const [apifyData, shopifyData] = await Promise.all([getApifyProducts(), getShopifyProducts()]); 
    if (shouldAbort(token)) return;
    
    const apifyProcessed = processApifyProducts(apifyData, { processPrice: false }); 
    const apifySkuMap = new Map();
    
    // Create map of Apify SKUs for quick lookups
    for (const apifyProd of apifyProcessed) {
      apifySkuMap.set(apifyProd.sku.toLowerCase(), apifyProd);
    }
    
    // Filter Shopify products to only include those with the supplier tag
    const supplierProducts = shopifyData.filter(p => {
      if (!p.tags) return false;
      return p.tags.includes(SUPPLIER_TAG);
    });
    
    addLog(`Found ${supplierProducts.length} Shopify products with tag '${SUPPLIER_TAG}' to process for SKU mapping.`, 'info');
    skippedNonSupplier = shopifyData.length - supplierProducts.length;
    
    // First pass: Match by SKU
    const usedShopifyIds = new Set();
    const usedApifySkus = new Set();
    
    for (const shopifyProd of supplierProducts) {
      const variant = shopifyProd.variants?.[0];
      if (!variant) continue;
      
      const shopifySku = variant.sku?.toLowerCase();
      if (shopifySku && apifySkuMap.has(shopifySku)) {
        alreadyMatched++;
        usedShopifyIds.add(shopifyProd.id);
        usedApifySkus.add(shopifySku);
      }
    }
    
    addLog(`Found ${alreadyMatched} products already correctly mapped by SKU`, 'info');
    
    // Second pass: Look for products to match by other methods
    const toProcess = supplierProducts.filter(p => !usedShopifyIds.has(p.id));
    const updateCandidates = [];
    
    // Track remaining Apify products that don't have a match yet
    const remainingApifyProducts = apifyProcessed.filter(p => !usedApifySkus.has(p.sku.toLowerCase()));
    
    addLog(`Processing ${toProcess.length} products that need SKU mapping...`, 'info');
    
    // For each remaining product, try to find a match
    for (const shopifyProd of toProcess) {
      if (shouldAbort(token)) break;
      
      const shopifyVariant = shopifyProd.variants?.[0];
      if (!shopifyVariant) continue;
      
      const shopifyNormalizedTitle = normalizeForMatching(shopifyProd.title);
      const shopifyNormalizedHandle = normalizeForMatching(shopifyProd.handle);
      let bestMatch = null;
      let matchType = '';
      let bestOverlap = 0;
      let debugInfo = {
        shopifyTitle: shopifyProd.title,
        shopifyHandle: shopifyProd.handle,
        shopifySku: shopifyVariant.sku || '(none)',
        normalizedTitle: shopifyNormalizedTitle,
        normalizedHandle: shopifyNormalizedHandle,
        bestMatchTitle: '',
        bestMatchSku: '',
        bestOverlap: 0,
        potentialMatches: []
      };
      
      // Try to match by handle
      for (const apifyProd of remainingApifyProducts) {
        const apifyNormalizedHandle = normalizeForMatching(apifyProd.handle);
        
        if (shopifyNormalizedHandle === apifyNormalizedHandle || 
            shopifyProd.handle.toLowerCase() === apifyProd.handle.toLowerCase()) {
          bestMatch = apifyProd;
          matchType = 'handle';
          debugInfo.bestMatchTitle = apifyProd.title;
          debugInfo.bestMatchSku = apifyProd.sku;
          break;
        }
      }
      
      if (bestMatch) {
        matchedByHandle++;
      } else {
        // Try to match by title
        for (const apifyProd of remainingApifyProducts) {
          if (shopifyNormalizedTitle === apifyProd.normalizedTitle) {
            bestMatch = apifyProd;
            matchType = 'title';
            debugInfo.bestMatchTitle = apifyProd.title;
            debugInfo.bestMatchSku = apifyProd.sku;
            break;
          }
        }
        
        if (bestMatch) {
          matchedByTitle++;
        } else {
          // Try fuzzy match as last resort
          let maxOverlap = 70; // Higher threshold for confidence
          
          // Find closest matches for debugging
          for (const apifyProd of remainingApifyProducts) {
            const overlap = getWordOverlap(shopifyNormalizedTitle, apifyProd.normalizedTitle);
            
            // Track top 3 closest matches for debugging
            if (overlap > bestOverlap) {
              bestOverlap = overlap;
              debugInfo.bestMatchTitle = apifyProd.title;
              debugInfo.bestMatchSku = apifyProd.sku;
              debugInfo.bestOverlap = overlap;
            }
            
            if (overlap > 60) {
              // Store potential close matches for debugging
              debugInfo.potentialMatches.push({
                title: apifyProd.title,
                sku: apifyProd.sku,
                overlap: Math.round(overlap)
              });
            }
            
            if (overlap > maxOverlap) {
              maxOverlap = overlap;
              bestMatch = apifyProd;
              matchType = `fuzzy-${Math.round(maxOverlap)}%`;
            }
          }
          
          if (bestMatch) {
            matchedByFuzzy++;
          }
        }
      }
      
      // If we found a match, prepare to update
      if (bestMatch) {
        const shopifyVariant = shopifyProd.variants?.[0];
        if (!shopifyVariant) continue;
        
        updateCandidates.push({
          shopifyId: shopifyProd.id,
          variantId: shopifyVariant.id,
          oldSku: shopifyVariant.sku || '(none)',
          newSku: bestMatch.sku,
          title: shopifyProd.title,
          matchType
        });
        
        // Remove this Apify product from consideration for future matches
        const index = remainingApifyProducts.findIndex(p => p.sku === bestMatch.sku);
        if (index !== -1) remainingApifyProducts.splice(index, 1);
      } else {
        noMatch++;
        
        // Collect diagnostic information about why it didn't match
        if (!shopifyProd.handle || shopifyProd.handle.length < 3) {
          // No/short handle
          if (unmatchedSamples.noHandle.length < 5) {
            unmatchedSamples.noHandle.push(debugInfo);
          }
        } else if (shopifyNormalizedTitle.length < 10) {
          // Very short title
          if (unmatchedSamples.shortTitle.length < 5) {
            unmatchedSamples.shortTitle.push(debugInfo);
          }
        } else if (shopifyVariant.sku && shopifyVariant.sku.match(/^\d+$/)) {
          // Numeric SKU format - likely just a partial SKU
          if (unmatchedSamples.badFormat.length < 5) {
            unmatchedSamples.badFormat.push(debugInfo);
          }
        } else if (debugInfo.potentialMatches.length > 0) {
          // Had close matches but not close enough
          if (unmatchedSamples.closeMatches.length < 5) {
            unmatchedSamples.closeMatches.push(debugInfo);
          }
        } else {
          // No close matches at all
          if (unmatchedSamples.noCloseMatches.length < 5) {
            unmatchedSamples.noCloseMatches.push(debugInfo);
          }
        }
      }
    }
    
    // Log summary
    addLog(`SKU mapping summary:`, 'info');
    addLog(`- Already matched: ${alreadyMatched}`, 'info');
    addLog(`- To be matched by handle: ${matchedByHandle}`, 'info');
    addLog(`- To be matched by title: ${matchedByTitle}`, 'info');
    addLog(`- To be matched by fuzzy: ${matchedByFuzzy}`, 'info');
    addLog(`- Total to update: ${updateCandidates.length}`, 'info');
    addLog(`- No match found: ${noMatch}`, 'info');
    
    // Log diagnostic information about unmatched products
    addLog(`\n===== DIAGNOSTIC INFORMATION FOR UNMATCHED PRODUCTS =====`, 'warning');
    
    // Calculate statistics about unmatched products
    const numericSkuCount = toProcess.filter(p => {
      const sku = p.variants?.[0]?.sku;
      return sku && /^\d+$/.test(sku);
    }).length;
    
    const shortTitleCount = toProcess.filter(p => 
      normalizeForMatching(p.title).length < 10
    ).length;
    
    const noHandleCount = toProcess.filter(p => 
      !p.handle || p.handle.length < 3
    ).length;
    
    addLog(`Numeric-only SKUs (likely partial SKUs): ${numericSkuCount} (${Math.round(numericSkuCount/noMatch*100)}% of unmatched)`, 'warning');
    addLog(`Very short titles: ${shortTitleCount} (${Math.round(shortTitleCount/noMatch*100)}% of unmatched)`, 'warning');
    addLog(`Missing/short handles: ${noHandleCount} (${Math.round(noHandleCount/noMatch*100)}% of unmatched)`, 'warning');
    
    // Log examples of unmatched products by category
    if (unmatchedSamples.badFormat.length > 0) {
      addLog(`\nProducts with numeric-only SKUs (examples):`, 'warning');
      unmatchedSamples.badFormat.forEach((item, i) => {
        addLog(`  ${i+1}. "${item.shopifyTitle}" - Current SKU: ${item.shopifySku} - Best potential match: "${item.bestMatchTitle}" (${item.bestOverlap}% overlap)`, 'warning');
      });
    }
    
    if (unmatchedSamples.closeMatches.length > 0) {
      addLog(`\nProducts with close but not matching titles:`, 'warning');
      unmatchedSamples.closeMatches.forEach((item, i) => {
        addLog(`  ${i+1}. "${item.shopifyTitle}" - Current SKU: ${item.shopifySku}`, 'warning');
        const topMatches = item.potentialMatches.sort((a, b) => b.overlap - a.overlap).slice(0, 3);
        topMatches.forEach((match, j) => {
          addLog(`     Close match ${j+1}: "${match.title}" (${match.overlap}% overlap) - SKU: ${match.sku}`, 'warning');
        });
      });
    }
    
    if (unmatchedSamples.noCloseMatches.length > 0) {
      addLog(`\nProducts with no close matches at all:`, 'warning');
      unmatchedSamples.noCloseMatches.forEach((item, i) => {
        addLog(`  ${i+1}. "${item.shopifyTitle}" - Current SKU: ${item.shopifySku}`, 'warning');
      });
    }
    
    addLog(`\n===== END DIAGNOSTIC INFORMATION =====`, 'warning');
    
    // Perform the updates
    if (updateCandidates.length > 0) {
      addLog(`Examples of SKUs to update:`, 'warning');
      for (let i = 0; i < Math.min(5, updateCandidates.length); i++) {
        const item = updateCandidates[i];
        addLog(`  - [${item.matchType}] "${item.title}": ${item.oldSku} -> ${item.newSku}`, 'warning');
      }
      
      for (const item of updateCandidates) {
        if (shouldAbort(token)) break;
        
        try {
          await shopifyClient.put(`/variants/${item.variantId}.json`, {
            variant: {
              id: item.variantId,
              sku: item.newSku
            }
          });
          updated++;
          addLog(`Updated SKU for "${item.title}" from ${item.oldSku} to ${item.newSku} (${item.matchType})`, 'success');
        } catch (e) {
          errors++;
          addLog(`Error updating SKU for "${item.title}": ${e.message}`, 'error', e);
        }
        
        await new Promise(r => setTimeout(r, 500)); // Rate limit
      }
    }
    
    // Final summary
    addLog(`SKU mapping complete: Updated ${updated}, Errors ${errors}, Already matched ${alreadyMatched}, No match ${noMatch}`, 'success');
    
    // Save diagnostics for reference
    lastRun.mapSkus.unmatchedDiagnostics = {
      numericSkuCount,
      shortTitleCount,
      noHandleCount,
      examples: unmatchedSamples
    };
    
  } catch (e) {
    addLog(`Critical error in SKU mapping job: ${e.message}`, 'error', e);
    errors++;
  }
  
  lastRun.mapSkus = {
    at: new Date().toISOString(),
    updated,
    errors,
    alreadyMatched,
    noMatch,
    matchedByHandle,
    matchedByTitle,
    matchedByFuzzy,
    skippedNonSupplier
  };
}

// UPDATED: Set products with stock to active status
async function updateInventoryJob(token) {
  addLog('Starting inventory sync (SKU-only)...', 'info');
  let updated = 0, errors = 0, inSync = 0, notFound = 0;
  let statusChanged = 0;
  const notFoundItems = []; // Track items not found
  
  try {
    const [apifyData, shopifyData] = await Promise.all([
      getApifyProducts(),
      getShopifyProducts()
    ]);
    
    if (shouldAbort(token)) return;
    
    // Filter for only supplier tag products
    const supplierProducts = shopifyData.filter(p => {
      if (!p.tags) return false;
      return p.tags.includes(SUPPLIER_TAG);
    });
    
    addLog(`Found ${supplierProducts.length} products with tag '${SUPPLIER_TAG}' out of ${shopifyData.length} total`, 'info');
    
    // Create SKU map for fast lookups
    const skuMap = new Map();
    for (const product of supplierProducts) {
      const sku = product.variants?.[0]?.sku;
      if (sku) skuMap.set(sku.toLowerCase(), product);
    }
    
    // Get inventory item IDs for all products
    const inventoryItemIds = supplierProducts
      .map(p => p.variants?.[0]?.inventory_item_id)
      .filter(id => id);
    
    // Get current inventory levels
    const inventoryLevels = await getShopifyInventoryLevels(inventoryItemIds);
    
    // Process Apify products
    const apifyProcessed = processApifyProducts(apifyData, { processPrice: false });
    
    // Prepare inventory updates
    const updates = [];
    const statusUpdates = []; // Track products that need status updates
    
    for (const apifyProd of apifyProcessed) {
      const matchResult = matchShopifyProductBySku(apifyProd, skuMap);
      
      if (matchResult.product) {
        const shopifyProd = matchResult.product;
        const variant = shopifyProd.variants?.[0];
        
        if (!variant?.inventory_item_id) continue;
        
        const currentInventory = inventoryLevels.get(variant.inventory_item_id) ?? 0;
        const targetInventory = apifyProd.inventory;
        
        // Check if we need to update inventory
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
        
        // Check if we need to update product status
        // If inventory > 0, product should be active
        // If inventory = 0, leave status as is (might be draft from discontinued job)
        if (targetInventory > 0 && shopifyProd.status !== 'active') {
          statusUpdates.push({
            id: shopifyProd.id,
            title: shopifyProd.title,
            oldStatus: shopifyProd.status,
            newStatus: 'active'
          });
        }
      } else {
        // Track unmatched products for analysis
        notFound++;
        if (notFoundItems.length < 100) { // Limit to prevent memory issues
          notFoundItems.push({
            title: apifyProd.title,
            sku: apifyProd.sku,
            inventory: apifyProd.inventory
          });
        }
      }
    }
    
    // Summary before updates
    addLog(`Inventory Summary: Updates needed: ${updates.length}, Status changes: ${statusUpdates.length}, In Sync: ${inSync}, Not Found: ${notFound}`, 'info');
    
    // Log sample of not found items for analysis
    if (notFoundItems.length > 0) {
      addLog(`Sample of items not found (showing ${Math.min(10, notFoundItems.length)} of ${notFound}):`, 'warning');
      for (let i = 0; i < Math.min(10, notFoundItems.length); i++) {
        const item = notFoundItems[i];
        addLog(`  - SKU: ${item.sku}, Title: "${item.title}", Inventory: ${item.inventory}`, 'warning');
      }
      
      // Suggest next steps
      addLog(`These ${notFound} products likely need to have their SKUs mapped or be created in your Shopify store.`, 'info');
      addLog(`Try running the "Comprehensive SKU Mapping" job first, then "Sync New Products" for any remaining items.`, 'info');
    }
    
    // Apply inventory updates in chunks to avoid rate limits
    if (updates.length > 0) {
      // Failsafe check - don't update too many items at once
      const updatePercentage = (updates.length / supplierProducts.length) * 100;
      
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
    
    // Apply status updates (activate products with stock)
    if (statusUpdates.length > 0) {
      addLog(`Updating status for ${statusUpdates.length} products...`, 'info');
      
      for (const update of statusUpdates) {
        if (shouldAbort(token)) break;
        
        try {
          await shopifyClient.put(`/products/${update.id}.json`, {
            product: {
              id: update.id,
              status: update.newStatus
            }
          });
          addLog(`Updated status for "${update.title}" from ${update.oldStatus} to ${update.newStatus}`, 'success');
          statusChanged++;
        } catch (e) {
          errors++;
          addLog(`Error updating status for "${update.title}": ${e.message}`, 'error', e);
        }
        
        await new Promise(r => setTimeout(r, 500)); // Rate limit
      }
    }
    
    // Final summary
    addLog(`Inventory update complete: Updated: ${updated}, Status Changed: ${statusChanged}, Errors: ${errors}, In Sync: ${inSync}, Not Found: ${notFound}`, 'success');
    
  } catch (e) {
    addLog(`Critical error in inventory update job: ${e.message}`, 'error', e);
    errors++;
  }
  
  lastRun.inventory = {
    at: new Date().toISOString(),
    updated,
    statusChanged,
    errors,
    inSync,
    notFound,
    notFoundSample: notFoundItems.slice(0, 100) // Store sample for reference
  };
  
  stats.inventoryUpdates += updated;
}

// Add the createNewProductsJob
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
    
    // Create SKU map for fast lookups - include all products
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
              tags: SUPPLIER_TAG, // Use the correct supplier tag
              status: apifyProd.inventory > 0 ? 'active' : 'draft', // Set status based on inventory
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
          
          addLog(`Created product: "${apifyProd.title}" with SKU: ${apifyProd.sku} (Status: ${apifyProd.inventory > 0 ? 'active' : 'draft'})`, 'success');
          
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

// Add the handleDiscontinuedProductsJob
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
      const discontinuePercentage = (potentialDiscontinued.length / shopifyData.filter(p => p.tags && p.tags.includes(SUPPLIER_TAG)).length) * 100;
      
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
      <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css">
      <style>
        :root {
          --primary-color: #4361ee;
          --secondary-color: #3a0ca3;
          --success-color: #4cc9f0;
          --warning-color: #f72585;
          --info-color: #4895ef;
          --light-color: #f8f9fa;
          --dark-color: #212529;
        }
        
        body {
          font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
          background-color: #f7f7f9;
          color: #333;
          line-height: 1.6;
          padding: 0;
          margin: 0;
        }
        
        .navbar {
          background: linear-gradient(90deg, var(--primary-color), var(--secondary-color));
          box-shadow: 0 4px 12px rgba(0,0,0,0.1);
          padding: 1rem 2rem;
          color: white;
          margin-bottom: 2rem;
        }
        
        .navbar h1 {
          margin: 0;
          font-weight: 700;
          font-size: 1.5rem;
        }
        
        .card {
          border: none;
          border-radius: 12px;
          box-shadow: 0 5px 15px rgba(0,0,0,0.05);
          transition: transform 0.3s, box-shadow 0.3s;
          margin-bottom: 25px;
          overflow: hidden;
        }
        
        .card:hover {
          transform: translateY(-5px);
          box-shadow: 0 10px 20px rgba(0,0,0,0.1);
        }
        
        .card-header {
          background: white;
          padding: 1.25rem 1.5rem;
          font-weight: 600;
          border-bottom: 1px solid rgba(0,0,0,0.05);
          display: flex;
          align-items: center;
          justify-content: space-between;
        }
        
        .card-body {
          padding: 1.5rem;
        }
        
        .status-badge {
          font-size: 0.9rem;
          padding: 0.5rem 1rem;
          border-radius: 50px;
          font-weight: 500;
          text-transform: uppercase;
          letter-spacing: 0.5px;
          box-shadow: 0 3px 8px rgba(0,0,0,0.1);
        }
        
        .btn {
          border-radius: 50px;
          padding: 0.6rem 1.5rem;
          font-weight: 500;
          text-transform: capitalize;
          letter-spacing: 0.3px;
          transition: all 0.3s;
          box-shadow: 0 3px 6px rgba(0,0,0,0.1);
          margin: 0.25rem;
        }
        
        .btn:hover {
          transform: translateY(-2px);
          box-shadow: 0 5px 10px rgba(0,0,0,0.15);
        }
        
        .btn i {
          margin-right: 5px;
        }
        
        .btn-primary {
          background: linear-gradient(45deg, var(--primary-color), var(--info-color));
          border: none;
        }
        
        .btn-warning {
          background: linear-gradient(45deg, #f72585, #ff9e00);
          border: none;
          color: white;
        }
        
        .btn-success {
          background: linear-gradient(45deg, #06d6a0, #1b9aaa);
          border: none;
        }
        
        .btn-danger {
          background: linear-gradient(45deg, #ef476f, #ffd166);
          border: none;
        }
        
        .btn-secondary {
          background: linear-gradient(45deg, #8338ec, #3a86ff);
          border: none;
          color: white;
        }
        
        .stat-card {
          padding: 1rem;
          border-radius: 10px;
          box-shadow: 0 3px 10px rgba(0,0,0,0.05);
          background: white;
          text-align: center;
          transition: all 0.3s;
        }
        
        .stat-card:hover {
          transform: translateY(-3px);
          box-shadow: 0 5px 15px rgba(0,0,0,0.1);
        }
        
        .stat-card h3 {
          font-size: 2rem;
          font-weight: 700;
          margin: 0.5rem 0;
          background: linear-gradient(45deg, var(--primary-color), var(--info-color));
          -webkit-background-clip: text;
          background-clip: text;
          -webkit-text-fill-color: transparent;
        }
        
        .stat-card p {
          margin: 0;
          font-size: 0.9rem;
          color: #6c757d;
          font-weight: 500;
        }
        
        .logs-container {
          max-height: 500px;
          overflow-y: auto;
          border-radius: 8px;
          background: #f8f9fa;
          padding: 1rem;
        }
        
        .log-entry {
          padding: 0.75rem 1rem;
          margin-bottom: 0.5rem;
          border-radius: 6px;
          background: white;
          box-shadow: 0 2px 5px rgba(0,0,0,0.02);
          border-left: 4px solid #dee2e6;
          font-size: 0.9rem;
        }
        
        .log-entry small {
          display: block;
          font-size: 0.75rem;
          opacity: 0.7;
          margin-bottom: 0.25rem;
        }
        
        .log-error {
          border-left-color: #ef476f;
          background: #fff5f7;
        }
        
        .log-warning {
          border-left-color: #ffd166;
          background: #fff9eb;
        }
        
        .log-success {
          border-left-color: #06d6a0;
          background: #f0fff4;
        }
        
        .alert {
          border-radius: 10px;
          padding: 1rem 1.5rem;
          border: none;
          box-shadow: 0 3px 10px rgba(0,0,0,0.05);
        }
        
        /* Animated background for status */
        .bg-running {
          background: linear-gradient(-45deg, #06d6a0, #1b9aaa, #4cc9f0, #3a86ff);
          background-size: 400% 400%;
          animation: gradient 3s ease infinite;
        }
        
        .bg-paused {
          background: linear-gradient(-45deg, #ffd166, #ffbd00, #ff9e00, #ff7700);
          background-size: 400% 400%;
          animation: gradient 3s ease infinite;
        }
        
        .bg-failsafe {
          background: linear-gradient(-45deg, #ef476f, #f72585, #b5179e, #7209b7);
          background-size: 400% 400%;
          animation: gradient 3s ease infinite;
        }
        
        @keyframes gradient {
          0% { background-position: 0% 50%; }
          50% { background-position: 100% 50%; }
          100% { background-position: 0% 50%; }
        }
      </style>
    </head>
    <body>
      <nav class="navbar">
        <div class="container-fluid">
          <h1><i class="fas fa-sync-alt me-2"></i> Shopify Sync Dashboard</h1>
        </div>
      </nav>
      
      <div class="container">
        <div class="row mb-4">
          <div class="col-md-6">
            <div class="card">
              <div class="card-header">
                <h5 class="mb-0"><i class="fas fa-tachometer-alt me-2"></i>System Status</h5>
                <span class="badge ${status === 'RUNNING' ? 'bg-running' : (status === 'PAUSED' ? 'bg-paused' : 'bg-failsafe')} status-badge">
                  <i class="fas ${status === 'RUNNING' ? 'fa-play-circle' : (status === 'PAUSED' ? 'fa-pause-circle' : 'fa-exclamation-triangle')} me-1"></i>
                  ${status}
                </span>
              </div>
              <div class="card-body">
                <div class="row mb-4">
                  <div class="col-6 col-md-3">
                    <div class="stat-card">
                      <p>New Products</p>
                      <h3>${stats.newProducts}</h3>
                    </div>
                  </div>
                  <div class="col-6 col-md-3">
                    <div class="stat-card">
                      <p>Inventory Updates</p>
                      <h3>${stats.inventoryUpdates}</h3>
                    </div>
                  </div>
                  <div class="col-6 col-md-3">
                    <div class="stat-card">
                      <p>Discontinued</p>
                      <h3>${stats.discontinued}</h3>
                    </div>
                  </div>
                  <div class="col-6 col-md-3">
                    <div class="stat-card">
                      <p>Errors</p>
                      <h3>${stats.errors}</h3>
                    </div>
                  </div>
                </div>
                
                <div class="d-flex flex-wrap justify-content-center gap-2 mt-3">
                  ${systemPaused ? 
                    `<button class="btn btn-success" onclick="fetch('/api/pause', {method: 'POST', body: JSON.stringify({paused: false}), headers: {'Content-Type': 'application/json'}}).then(() => location.reload())">
                      <i class="fas fa-play"></i> Resume System
                    </button>` : 
                    `<button class="btn btn-warning" onclick="fetch('/api/pause', {method: 'POST', body: JSON.stringify({paused: true}), headers: {'Content-Type': 'application/json'}}).then(() => location.reload())">
                      <i class="fas fa-pause"></i> Pause System
                    </button>`
                  }
                  
                  ${failsafeTriggered ? 
                    `<button class="btn btn-danger" onclick="fetch('/api/failsafe/clear', {method: 'POST'}).then(() => location.reload())">
                      <i class="fas fa-exclamation-triangle"></i> Clear Failsafe
                    </button>` : 
                    ``
                  }
                </div>
                
                ${failsafeTriggered ? 
                  `<div class="alert alert-danger mt-3">
                    <i class="fas fa-exclamation-circle me-2"></i>
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
                <h5 class="mb-0"><i class="fas fa-tools me-2"></i>Manual Actions</h5>
              </div>
              <div class="card-body">
                <div class="d-flex flex-wrap justify-content-center gap-2">
                  <button class="btn btn-warning" onclick="runSync('improved-map-skus')">
                    <i class="fas fa-map-signs"></i> Comprehensive SKU Mapping
                  </button>
                  <button class="btn btn-primary" onclick="runSync('inventory')">
                    <i class="fas fa-boxes"></i> Sync Inventory
                  </button>
                  <button class="btn btn-primary" onclick="runSync('products')">
                    <i class="fas fa-plus-circle"></i> Sync New Products
                  </button>
                  <button class="btn btn-primary" onclick="runSync('discontinued')">
                    <i class="fas fa-archive"></i> Check Discontinued
                  </button>
                  <button class="btn btn-secondary" onclick="runSync('deduplicate')">
                    <i class="fas fa-clone"></i> Find & Delete Duplicates
                  </button>
                </div>
              </div>
            </div>
          </div>
        </div>
        
        <div class="row">
          <div class="col-md-12">
            <div class="card">
              <div class="card-header">
                <h5 class="mb-0"><i class="fas fa-list-alt me-2"></i>Logs</h5>
              </div>
              <div class="card-body">
                <div class="logs-container">
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

app.post('/api/sync/improved-map-skus', (req, res) => {
  return startBackgroundJob('mapSkus', 'Comprehensive SKU Mapping', t => improvedMapSkusJob(t)) 
    ? res.json({s: 1}) 
    : res.status(409).json({s: 0});
});

app.post('/api/sync/:type', (req, res) => {
  const jobs = {
    inventory: updateInventoryJob,
    products: createNewProductsJob,
    discontinued: handleDiscontinuedProductsJob,
    'improved-map-skus': improvedMapSkusJob
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
