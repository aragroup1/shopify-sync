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

// --- THIS FUNCTION IS NOW FIXED ---
function normalizeForMatching(text = '') {
return String(text)
.toLowerCase()
.replace(/\s*KATEX_INLINE_OPEN.*?KATEX_INLINE_CLOSE\s*/g, ' ') // Remove content in parentheses
.replace(/\s*```math
.*?```\s*/g, ' ') // Remove content in brackets
.replace(/\s*```math
\s*[\s\S]*?\s*```\s*/g, ' ') // CORRECTED: Handles multi-line math blocks
.replace(/-(parcel|large-letter|letter)-rate$/i, '')
.replace(/-p\d+$/i, '')
.replace(/\b(a|an|the|of|in|on|at|to|for|with|by)\b/g, '')
.replace(/[^a-z0-9]+/g, ' ')
.replace(/\s+/g, ' ')
.trim();
}
function calculateRetailPrice(supplierCostString) { const cost = parseFloat(supplierCostString); if (isNaN(cost) || cost < 0) return '0.00'; let finalPrice; if (cost <= 1) finalPrice = cost + 5.5; else if (cost <= 2) finalPrice = cost + 5.95; else if (cost <= 3) finalPrice = cost + 6.99; else if (cost <= 5) finalPrice = cost * 3.2; else if (cost <= 7) finalPrice = cost * 2.5; else if (cost <= 9) finalPrice = cost * 2.2; else if (cost <= 12) finalPrice = cost * 2; else if (cost <= 20) finalPrice = cost * 1.9; else finalPrice = cost * 1.8; return finalPrice.toFixed(2); }
function processApifyProducts(apifyData, { processPrice = true } = {}) { return apifyData.map(item => { if (!item || !item.title) return null; const handle = normalizeForMatching(item.handle || item.title).replace(/ /g, '-'); let inventory = item.variants?.[0]?.stockQuantity || item.stock || 20; if (String(item.variants?.[0]?.price?.stockStatus || item.stockStatus || item.availability || '').toLowerCase().includes('out')) inventory = 0; let sku = item.sku || item.variants?.[0]?.sku || ''; let price = '0.00'; if (item.variants?.[0]?.price?.value) { price = String(item.variants[0].price.value); if (processPrice) price = calculateRetailPrice(price); } const body_html = item.description || item.bodyHtml || ''; const images = item.images ? item.images.map(img => ({ src: img.src || img.url })).filter(img => img.src) : []; return { handle, title: item.title, inventory, sku, price, body_html, images, normalizedTitle: normalizeForMatching(item.title) }; }).filter(p => p.sku); }
function buildShopifyMaps(shopifyData) { const skuMap = new Map(); const strippedHandleMap = new Map(); const normalizedTitleMap = new Map(); for (const product of shopifyData) { const sku = product.variants?.[0]?.sku; if (sku) skuMap.set(sku.toLowerCase(), product); strippedHandleMap.set(normalizeForMatching(product.handle).replace(/ /g, '-'), product); normalizedTitleMap.set(normalizeForMatching(product.title), product); } return { skuMap, strippedHandleMap, normalizedTitleMap, all: shopifyData }; }
function findBestMatch(apifyProduct, shopifyMaps) { if (shopifyMaps.skuMap.has(apifyProduct.sku.toLowerCase())) return { product: shopifyMaps.skuMap.get(apifyProduct.sku.toLowerCase()), matchType: 'sku' }; if (shopifyMaps.strippedHandleMap.has(apifyProduct.handle)) return { product: shopifyMaps.strippedHandleMap.get(apifyProduct.handle), matchType: 'handle' }; if (shopifyMaps.normalizedTitleMap.has(apifyProduct.normalizedTitle)) return { product: shopifyMaps.normalizedTitleMap.get(apifyProduct.normalizedTitle), matchType: 'title' }; let bestTitleMatch = null; let maxOverlap = 60; for (const shopifyProduct of shopifyMaps.all) { const overlap = getWordOverlap(apifyProduct.normalizedTitle, normalizeForMatching(shopifyProduct.title)); if (overlap > maxOverlap) { maxOverlap = overlap; bestTitleMatch = shopifyProduct; } } if (bestTitleMatch) return { product: bestTitleMatch, matchType: `title-fuzzy-${Math.round(maxOverlap)}%` }; return { product: null, matchType: 'none' }; }
function matchShopifyProductBySku(apifyProduct, skuMap) { const product = skuMap.get(apifyProduct.sku.toLowerCase()); return product ? { product, matchType: 'sku' } : { product: null, matchType: 'none' }; }

// --- CORE JOB LOGIC ---
async function deduplicateProductsJob(token) { addLog('--- Starting One-Time Duplicate Cleanup Job ---', 'warning'); let deletedCount = 0, errors = 0; try { const allShopifyProducts = await getShopifyProducts(); if (shouldAbort(token)) return; const productsByTitle = new Map(); for (const product of allShopifyProducts) { const normalized = normalizeForMatching(product.title); if (!productsByTitle.has(normalized)) productsByTitle.set(normalized, []); productsByTitle.get(normalized).push(product); } const toDeleteIds = []; for (const products of productsByTitle.values()) { if (products.length > 1) { addLog(`Found ${products.length} duplicates for title: "${products[0].title}"`, 'warning'); products.sort((a, b) => new Date(a.created_at) - new Date(b.created_at)); const toKeep = products.shift(); addLog(` Keeping ORIGINAL product ID ${toKeep.id} (created at ${toKeep.created_at})`, 'info'); products.forEach(p => { addLog(` Marking NEWER duplicate for deletion ID ${p.id} (created at ${p.created_at})`, 'info'); toDeleteIds.push(p.id); }); } } if (toDeleteIds.length > 0) { addLog(`Preparing to delete ${toDeleteIds.length} newer duplicate products...`, 'warning'); for (const id of toDeleteIds) { if (shouldAbort(token)) break; try { await shopifyClient.delete(`/products/${id}.json`); deletedCount++; addLog(` ✓ Deleted product ID ${id}`, 'success'); } catch (e) { errors++; addLog(` ✗ Error deleting product ID ${id}: ${e.message}`, 'error', e); } await new Promise(r => setTimeout(r, 600)); } } else { addLog('No duplicate products found to delete.', 'success'); } } catch (e) { addLog(`Critical error in deduplication job: ${e.message}`, 'error', e); errors++; } lastRun.deduplicate = { at: new Date().toISOString(), deleted: deletedCount, errors }; }
async function mapSkusJob(token) { addLog('--- Starting Aggressive SKU Mapping Job ---', 'warning'); let updated = 0, errors = 0, alreadyMatched = 0, noMatch = 0; try { const [apifyData, shopifyData] = await Promise.all([ getApifyProducts(), getShopifyProducts() ]); if (shouldAbort(token)) return; const apifyProcessed = processApifyProducts(apifyData, { processPrice: false }); const shopifyMaps = buildShopifyMaps(shopifyData); const usedShopifyIds = new Set(); for (const apifyProduct of apifyProcessed) { if (shouldAbort(token)) break; const { product: shopifyProduct, matchType } = findBestMatch(apifyProduct, shopifyMaps); if (shopifyProduct) { if (usedShopifyIds.has(shopifyProduct.id)) { addLog(`Skipping duplicate match for Shopify ID ${shopifyProduct.id}`, 'warning'); continue; } const shopifyVariant = shopifyProduct.variants?.[0]; if (!shopifyVariant) continue; if (shopifyVariant.sku?.toLowerCase() === apifyProduct.sku.toLowerCase()) { alreadyMatched++; } else { try { addLog(`[${matchType}] Mapping "${shopifyProduct.title}". Old SKU: "${shopifyVariant.sku}" -> New SKU: "${apifyProduct.sku}"`, 'info'); await shopifyClient.put(`/variants/${shopifyVariant.id}.json`, { variant: { id: shopify
