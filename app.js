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

// --- HANDLE NORMALIZATION ---
function normalizeHandle(urlOrTitle) {
  if (!urlOrTitle) return '';
  return urlOrTitle.toLowerCase()
    .replace('https://www.manchesterwholesale.co.uk/products/', '')
    .replace(/\s*\(parcel rate\)|\s*\(big parcel rate\)|\s*\(large letter rate\)/gi, '')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-|-$/g, '');
}

function extractHandle(item, index) {
  const source = item.canonicalUrl || item.url || item.productUrl || item.source?.url || item.title || `product-${index}`;
  const handle = normalizeHandle(source);
  if (index < 3) {
    addLog(`Normalized handle ${index}: "${handle}" (title: "${item.title}")`, 'info');
  }
  return handle;
}

// --- HELPER FUNCTIONS ---
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

// --- PROCESS APIFY PRODUCTS ---
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
  if (originalDescription.length > 20) seoDescription += `${originalDescription.substring(0, 150)}... `;
  if (features.length > 0) seoDescription += `This product is ${features.join(', ')}. `;
  seoDescription += `Order now for fast delivery. Shop with confidence at LandOfEssentials - your trusted online retailer for quality products.`;

  const keywords = [];
  if (title.toLowerCase().includes('halloween')) keywords.push('halloween costumes', 'party supplies', 'trick or treat');
  if (title.toLowerCase().includes('game')) keywords.push('family games', 'entertainment', 'fun activities');
  if (title.toLowerCase().includes('decoration')) keywords.push('home decor', 'decorative items', 'interior design');
  if (title.toLowerCase().includes('toy')) keywords.push('children toys', 'educational toys', 'safe toys');

  if (keywords.length > 0) seoDescription += ` Perfect for: ${keywords.join(', ')}.`;
  return seoDescription;
}

function processApifyProducts(apifyData) {
  return apifyData.map((item, index) => {
    const handle = extractHandle(item, index);
    if (!handle) return null;

    // Price calculation
    let price = 0;
    if (item.variants && item.variants.length > 0) {
      const variant = item.variants[0];
      if (variant.price) price = typeof variant.price === 'object' ? parseFloat(variant.price.current || 0) : parseFloat(variant.price);
    }
    if (price === 0) price = parseFloat(item.price || 0);
    if (price > 100) price = price / 100;

    let finalPrice = price <= 1 ? price + 6 :
                     price <= 2 ? price + 7 :
                     price <= 3 ? price + 8 :
                     price <= 5 ? price + 9 :
                     price <= 8 ? price + 10 :
                     price <= 15 ? price * 2 :
                     price * 2.2;

    if (finalPrice === 0) { price = 5; finalPrice = 15; }

    // Clean title
    let cleanTitle = item.title || 'Untitled Product';
    cleanTitle = cleanTitle.replace(/\b\d{4}\b/g, '')
                           .replace(/\s*(large letter rate|parcel rate|big parcel rate|letter rate)\s*/gi, '')
                           .replace(/\s+/g, ' ')
                           .trim();

    // Images
    const images = [];
    if (item.medias && Array.isArray(item.medias)) {
      for (let i = 0; i < Math.min(3, item.medias.length); i++) {
        if (item.medias[i]?.url) images.push(item.medias[i].url);
      }
    }

    // Inventory
    let inventory = 10;
    if (item.variants?.[0]?.price?.stockStatus) inventory = item.variants[0].price.stockStatus === 'IN_STOCK' ? 10 : 0;

    const productData = {
      handle,
      title: cleanTitle,
      description: item.description || `${cleanTitle}\n\nHigh-quality product from LandOfEssentials.`,
      sku: item.sku || '',
      originalPrice: price.toFixed(2),
      price: finalPrice.toFixed(2),
      compareAtPrice: (finalPrice * 1.2).toFixed(2),
      inventory,
      images,
      vendor: 'LandOfEssentials',
      seoDescription: generateSEODescription({ title: cleanTitle, description: item.description || '' })
    };

    return productData;
  }).filter(Boolean);
}

// --- CREATE NEW PRODUCTS ---
async function createNewProducts() {
  if (systemPaused) { addLog('Product creation skipped - system is paused', 'warning'); return { created: 0, errors: 0, total: 0 }; }
  let created = 0, errors = 0;
  try {
    addLog('Fetching data for product creation...', 'info');
    const [apifyData, shopifyData] = await Promise.all([getApifyProducts(), getShopifyProducts()]);
    const processedProducts = processApifyProducts(apifyData);
    const shopifyHandles = new Set(shopifyData.map(p => p.handle));
    const new
