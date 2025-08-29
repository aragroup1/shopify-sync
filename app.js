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

function addLog(message, type = 'info') {
  const log = { timestamp: new Date().toISOString(), message, type };
  logs.unshift(log);
  if (logs.length > 100) logs = logs.slice(0, 100);
  console.log(`[${log.timestamp}] ${message}`);
}

// Configuration
const config = {
  apify: {
    token: process.env.APIFY_TOKEN,
    actorId: process.env.APIFY_ACTOR_ID || 'autofacts~shopify',
    baseUrl: 'https://api.apify.com/v2',
    urlPrefix: process.env.URL_PREFIX || 'https://www.manchesterwholesale.co.uk/products/'
  },
  shopify: {
    domain: process.env.SHOPIFY_DOMAIN,
    accessToken: process.env.SHOPIFY_ACCESS_TOKEN,
    locationId: process.env.SHOPIFY_LOCATION_ID,
    baseUrl: `https://${process.env.SHOPIFY_DOMAIN}/admin/api/2024-01`
  }
};

// API clients
const apifyClient = axios.create({ baseURL: config.apify.baseUrl, timeout: 120000 });
const shopifyClient = axios.create({
  baseURL: config.shopify.baseUrl,
  headers: {
    'X-Shopify-Access-Token': config.shopify.accessToken,
    'Content-Type': 'application/json'
  },
  timeout: 120000
});

// Helper functions
async function getApifyProducts() {
  let allItems = [];
  let offset = 0;
  let pageCount = 0;
  const limit = 500;
  
  addLog('Starting Apify product fetch...', 'info');
  
  while (true) {
    try {
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
    } catch (error) {
      addLog(`Apify fetch error: ${error.message}`, 'error');
      stats.errors++;
      throw error;
    }
  }
  
  addLog(`Apify fetch complete: ${allItems.length} total products`, 'info');
  if (allItems.length > 0) {
    addLog(`Sample Apify products: ${allItems.slice(0, 3).map(p => `${p.title} (${p.sku || 'no-sku'})`).join(', ')}`, 'info');
  }
  
  return allItems;
}

async function getShopifyProducts() {
  let allProducts = [];
  let sinceId = null;
  let pageCount = 0;
  const limit = 250;
  const fields = 'id,handle,title,variants,tags';
  
  addLog('Starting Shopify product fetch...', 'info');
  
  while (true) {
    try {
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
    } catch (error) {
      addLog(`Shopify fetch error: ${error.message}`, 'error');
      stats.errors++;
      throw error;
    }
  }
  
  const filteredProducts = allProducts.filter(p => p.tags && p.tags.includes('Supplier:Apify'));
  addLog(`Shopify fetch complete: ${allProducts.length} total products, ${filteredProducts.length} with Supplier:Apify tag`, 'info');
  if (filteredProducts.length > 0) {
    addLog(`Sample Shopify products: ${filteredProducts.slice(0, 5).map(p => `${p.handle} (inv: ${p.variants?.[0]?.inventory_quantity || 'N/A'})`).join(', ')}`, 'info');
  }
  
  return filteredProducts;
}

function normalizeHandle(input, index, isTitle = false) {
  let handle = input || '';
  
  if (!isTitle && handle && handle !== 'undefined') {
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
  
  handle = (isTitle ? input : `product-${index}`).toLowerCase()
    .replace(/\b\d{4}\b/g, '')
    .replace(/\s*(large letter rate|parcel rate|big parcel rate|letter rate)\s*/gi, '')
    .replace(/\s*KATEX_INLINE_OPEN.*?KATEX_INLINE_CLOSE\s*/g, '')
    .replace(/[^a-z0-9-]+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '');
  
  if (index < 5) addLog(`Fallback handle: "${input}" → "${handle}"`, 'info');
  
  return handle;
}

function extractHandleFromCanonicalUrl(item, index) {
  const urlFields = [item.canonicalUrl, item.url, item.productUrl, item.source?.url];
  const validUrl = urlFields.find(url => url && url !== 'undefined');
  
  if (index < 5) {
    addLog(`Debug product ${index}: canonicalUrl="${item.canonicalUrl}", url="${item.url}", productUrl="${item.productUrl}", title="${item.title}"`, 'info');
  }
  
  return normalizeHandle(validUrl || item.title, index, !validUrl);
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

    // Clean title
    let cleanTitle = item.title || 'Untitled Product';
    cleanTitle = cleanTitle.replace(/\b\d{4}\b/g, '')
      .replace(/\s*(large letter rate|parcel rate|big parcel rate|letter rate)\s*/gi, '')
      .replace(/\s*KATEX_INLINE_OPEN.*?KATEX_INLINE_CLOSE\s*/g, '')
      .replace(/\s+/g, ' ')
      .trim();

    // Process inventory
    let inventory = item.variants?.[0]?.stockQuantity || item.stock || 20;
    const stockStatus = item.variants?.[0]?.price?.stockStatus;
    if (stockStatus === 'OUT_OF_STOCK') {
      inventory = 0;
    } else if (stockStatus === 'IN_STOCK' && inventory === 0) {
      inventory = 20;
    }

    if (index < 5) {
      addLog(`Inventory debug for ${cleanTitle}: stockQuantity=${item.variants?.[0]?.stockQuantity || 'N/A'}, stock=${item.stock || 'N/A'}, stockStatus=${stockStatus || 'N/A'}, final=${inventory}`, 'info');
    }

    // Process price only if needed
    let price = 0;
    let finalPrice = 0;
    let compareAtPrice = 0;
    
    if (options.processPrice) {
      // Extract price from various possible locations
      if (item.variants && item.variants.length > 0) {
        const variant = item.variants[0];
        if (variant.price) {
          price = typeof variant.price === 'object' ? parseFloat(variant.price.current || 0) : parseFloat(variant.price);
        }
      }
      if (price === 0) price = parseFloat(item.price || 0);
      
      // Improved currency detection
      const originalPrice = price;
      
      // If price has decimals and is less than 10, it's likely already in pounds
      if (price > 0 && price < 10 && price.toString().includes('.')) {
        // Already in pounds, no conversion needed
        if (index < 5) {
          addLog(`Price appears to be in pounds already: ${price}`, 'info');
        }
      } 
      // If price is a whole number > 50, it's likely in pence
      else if (price > 50 && !price.toString().includes('.')) {
        price = price / 100;
        if (index < 5) {
          addLog(`Price converted from pence: ${originalPrice} → ${price}`, 'info');
        }
      }
      // If price > 1000, definitely pence
      else if (price > 1000) {
        price = price / 100;
        if (index < 5) {
          addLog(`Price converted from pence (high value): ${originalPrice} → ${price}`, 'info');
        }
      }
      
      // Apply markup based on price ranges
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

      // Ensure minimum price
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

    // Process images
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

async function createNewProducts(productsToCreate) {
  if (systemPaused) {
    addLog('Product creation skipped - system is paused', 'warning');
    return { created: 0, errors: 0, total: productsToCreate.length };
  }

  let created = 0, errors = 0;
  const batchSize = 5;
  
  for (let i = 0; i < productsToCreate.length; i += batchSize) {
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
            inventory_policy: 'deny'
          }]
        };

        const response = await shopifyClient.post('/products.json', { product: shopifyProduct });
        const createdProduct = response.data.product;
        addLog(`Created: ${product.title} (£${product.price})`, 'success');

        for (const imageUrl of product.images) {
          try {
            await shopifyClient.post(`/products/${createdProduct.id}/images.json`, { image: { src: imageUrl } });
            await new Promise(resolve => setTimeout(resolve, 1000));
          } catch (imageError) {
            addLog(`Image failed for ${product.title}: ${imageError.message}`, 'warning');
          }
        }

        if (createdProduct.variants && createdProduct.variants[0]) {
          await shopifyClient.post('/inventory_levels/set.json', {
            location_id: config.shopify.locationId,
            inventory_item_id: createdProduct.variants[0].inventory_item_id,
            available: product.inventory
          });
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
  return { created, errors, total: productsToCreate.length };
}

async function handleDiscontinuedProducts() {
  let discontinued = 0, errors = 0;
  try {
    addLog('Checking for discontinued products...', 'info');
    const [apifyData, shopifyData] = await Promise.all([getApifyProducts(), getShopifyProducts()]);
    
    const processedApifyProducts = processApifyProducts(apifyData, { processPrice: false });
    const currentApifyHandles = new Set(processedApifyProducts.map(p => p.handle));
    
    const discontinuedProducts = shopifyData.filter(shopifyProduct => 
      shopifyProduct.tags && 
      shopifyProduct.tags.includes('Supplier:Apify') && 
      !currentApifyHandles.has(shopifyProduct.handle)
    );
    
    addLog(`Found ${discontinuedProducts.length} potentially discontinued products`, 'info');
    
    for (const product of discontinuedProducts) {
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
  if (systemPaused) {
    addLog('Inventory update skipped - system is paused', 'warning');
    return { updated: 0, created: 0, errors: 0, total: 0 };
  }

  let updated = 0, errors = 0;
  mismatches = [];
  try {
    addLog('=== STARTING INVENTORY UPDATE WORKFLOW ===', 'info');
    const [apifyData, shopifyData] = await Promise.all([getApifyProducts(), getShopifyProducts()]);
    
    addLog(`Data comparison: ${apifyData.length} Apify products vs ${shopifyData.length} Shopify products`, 'info');
    
    // Process products WITHOUT price processing for inventory updates
    const processedProducts = processApifyProducts(apifyData, { processPrice: false });
    addLog(`Processed ${processedProducts.length} valid Apify products after filtering`, 'info');
    
    // Create multiple lookup maps for better matching
    const shopifyHandleMap = new Map(shopifyData.map(p => [p.handle.toLowerCase(), p]));
    const shopifyTitleMap = new Map();
    const shopifySkuMap = new Map();
    
    // Build additional maps for fallback matching
    shopifyData.forEach(product => {
      // Clean title for matching
      const cleanTitle = product.title.toLowerCase()
        .replace(/\b\d{4}\b/g, '')
        .replace(/\s*(large letter rate|parcel rate|big parcel rate|letter rate)\s*/gi, '')
        .replace(/\s*KATEX_INLINE_OPEN.*?KATEX_INLINE_CLOSE\s*/g, '')
        .replace(/[^a-z0-9]+/g, ' ')
        .trim();
      shopifyTitleMap.set(cleanTitle, product);
      
      // Map by SKU if available
      if (product.variants && product.variants[0] && product.variants[0].sku) {
        shopifySkuMap.set(product.variants[0].sku.toLowerCase(), product);
      }
      
      // Also try alternative handle formats
      const altHandle = product.handle.replace(/-\d+$/, ''); // Remove trailing numbers
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
      
      // Try to match by handle first
      shopifyProduct = shopifyHandleMap.get(apifyProduct.handle.toLowerCase());
      if (shopifyProduct) {
        matchType = 'handle';
        matchedByHandle++;
      }
      
      // Try to match by SKU if handle didn't work
      if (!shopifyProduct && apifyProduct.sku) {
        shopifyProduct = shopifySkuMap.get(apifyProduct.sku.toLowerCase());
        if (shopifyProduct) {
          matchType = 'SKU';
          matchedBySku++;
        }
      }
      
      // Try to match by cleaned title if still no match
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
      
      // Try partial handle match as last resort
      if (!shopifyProduct) {
        // Try to find a product whose handle contains our handle or vice versa
        const handleParts = apifyProduct.handle.split('-').filter(p => p.length > 3);
        for (const [shopifyHandle, product] of shopifyHandleMap) {
          // Check if handles share significant parts
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
      
      // Log debug info for first few products
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
        if (mismatches.length < 100) { // Only store first 100 mismatches
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
      
      // Check if we have inventory_item_id
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
        productId: shopifyProduct.id
      });
      
      if (inventoryUpdates.length <= 5) {
        addLog(`Update needed: ${apifyProduct.handle} (${currentInventory} → ${targetInventory}) [matched by ${matchType}]`, 'info');
      }
    });
    
    // Log debug sample
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

    // Verify location ID is set
    if (!config.shopify.locationId) {
      addLog('ERROR: SHOPIFY_LOCATION_ID environment variable is not set!', 'error');
      return { updated: 0, created: 0, errors: 1, total: inventoryUpdates.length };
    }

    // Process updates with better error handling
    for (const update of inventoryUpdates) {
      try {
        // First, verify the inventory level exists
        const checkResponse = await shopifyClient.get(
          `/inventory_levels.json?inventory_item_ids=${update.inventoryItemId}&location_ids=${config.shopify.locationId}`
        ).catch(err => null);
        
        if (!checkResponse || !checkResponse.data.inventory_levels || checkResponse.data.inventory_levels.length === 0) {
          // Need to connect the inventory item to the location first
          try {
            await shopifyClient.post('/inventory_levels/connect.json', {
              location_id: parseInt(config.shopify.locationId),
              inventory_item_id: update.inventoryItemId
            });
            addLog(`Connected inventory item to location for: ${update.title}`, 'info');
            await new Promise(resolve => setTimeout(resolve, 250));
          } catch (connectErr) {
            // Item might already be connected, continue
          }
        }
        
        // Now update the inventory
        await shopifyClient.post('/inventory_levels/set.json', {
          location_id: parseInt(config.shopify.locationId),
          inventory_item_id: update.inventoryItemId,
          available: update.newInventory
        });
        
        addLog(`✓ Updated: ${update.title} (${update.currentInventory} → ${update.newInventory}) [${update.matchType}]`, 'success');
        updated++;
        stats.inventoryUpdates++;
        await new Promise(resolve => setTimeout(resolve, 500));
      } catch (error) {
        errors++;
        stats.errors++;
        
        // Log detailed error information
        if (error.response) {
          const errorDetails = error.response.data?.errors || error.response.data?.error || error.message;
          addLog(`✗ Failed: ${update.title} - Status ${error.response.status}: ${JSON.stringify(errorDetails)}`, 'error');
          
          if (error.response.status === 422) {
            addLog(`  → Inventory Item ID: ${update.inventoryItemId}, Location ID: ${config.shopify.locationId}`, 'error');
          }
        } else {
          addLog(`✗ Failed: ${update.title} - ${error.message}`, 'error');
        }
        
        // Add delay after errors to avoid rate limiting
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
    addLog(`Result: ${updated} updated, ${errors} errors out of ${inventoryUpdates.length} attempts`, updated > 0 ? 'success' : 'info');
    
    // If we had many errors, suggest checking configuration
    if (errors > inventoryUpdates.length * 0.5) {
      addLog('⚠️ High error rate detected. Please verify:', 'warning');
      addLog(`  - SHOPIFY_LOCATION_ID is correct: ${config.shopify.locationId}`, 'warning');
      addLog('  - Products have inventory tracking enabled', 'warning');
      addLog('  - API permissions include inventory management', 'warning');
    }
    
    return { updated, created: 0, errors, total: inventoryUpdates.length };
  } catch (error) {
    addLog(`Inventory update workflow failed: ${error.message}`, 'error');
    stats.errors++;
    return { updated, created: 0, errors: errors + 1, total: 0 };
  }
}

// API Routes
app.get('/', (req, res) => {
  res.send(`
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Shopify Sync Dashboard</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <style>
        body {
            transition: background-color 0.3s, color 0.3s;
            background: linear-gradient(to bottom right, #ff6e7f, #bfe9ff);
            animation: gradientShift 15s ease infinite;
        }
        .dark body {
            background: linear-gradient(to bottom right, #1a1a2e, #16213e);
            animation: gradientShiftDark 15s ease infinite;
        }
        @keyframes gradientShift {
            0% { background-position: 0% 50%; }
            50% { background-position: 100% 50%; }
            100% { background-position: 0% 50%; }
        }
        @keyframes gradientShiftDark {
            0% { background-position: 0% 50%; }
            50% { background-position: 100% 50%; }
            100% { background-position: 0% 50%; }
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
            background: rgba(255, 255, 255, 0.1);
            backdrop-filter: blur(12px);
            border: 1px solid rgba(255, 255, 255, 0.2);
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.2);
        }
        .dark .card-hover {
            background: rgba(31, 41, 55, 0.2);
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
            animation: glow 1.5s infinite;
        }
        @keyframes glow {
            0% { box-shadow: 0 0 5px rgba(255, 110, 127, 0.7); }
            50% { box-shadow: 0 0 20px rgba(255, 110, 127, 1); }
            100% { box-shadow: 0 0 5px rgba(255, 110, 127, 0.7); }
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
        }
        @keyframes spin {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
        }
        .loading-overlay {
            display: none;
            position: fixed;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            background: rgba(0, 0, 0, 0.8);
            z-index: 1000;
            align-items: center;
            justify-content: center;
        }
        .loading-overlay.active {
            display: flex;
        }
        .loading-spinner {
            width: 60px;
            height: 60px;
            border: 8px solid rgba(255, 255, 255, 0.2);
            border-top: 8px solid #ff6e7f;
            border-radius: 50%;
            animation: spin 1s linear infinite, pulseSpinner 2s infinite;
            transform-style: preserve-3d;
        }
        @keyframes pulseSpinner {
            0% { transform: scale(1) rotateX(0deg); }
            50% { transform: scale(1.2) rotateX(10deg); }
            100% { transform: scale(1) rotateX(0deg); }
        }
        .mismatch-table th {
            cursor: pointer;
            transition: all 0.3s ease;
            background: rgba(255, 255, 255, 0.1);
            backdrop-filter: blur(8px);
        }
        .mismatch-table th:hover {
            background: rgba(255, 110, 127, 0.3);
            transform: scale(1.02);
        }
        .dark .mismatch-table th:hover {
            background: rgba(255, 110, 127, 0.2);
        }
        .mismatch-table tr {
            transition: background-color 0.3s;
        }
        .mismatch-table tr:nth-child(even) {
            background: rgba(255, 255, 255, 0.05);
        }
        .dark .mismatch-table tr:nth-child(even) {
            background: rgba(31, 41, 55, 0.1);
        }
        .mismatch-table tr:hover {
            background: rgba(255, 110, 127, 0.2);
            transform: scale(1.01);
        }
        .log-container {
            background: rgba(17, 24, 39, 0.9);
            backdrop-filter: blur(10px);
            box-shadow: 0 0 30px rgba(0, 0, 0, 0.3);
            transition: box-shadow 0.3s;
        }
        .log-container:hover {
            box-shadow: 0 0 40px rgba(255, 110, 127, 0.5);
        }
        .dark .log-container {
            background: rgba(17, 24, 39, 0.95);
        }
    </style>
</head>
<body class="min-h-screen font-sans transition-colors">
    <div class="loading-overlay" id="loadingOverlay">
        <div class="loading-spinner"></div>
    </div>
    <div class="container mx-auto px-4 py-8">
        <div class="relative bg-white dark:bg-gray-800 rounded-2xl shadow-lg p-8 mb-8 gradient-bg text-white fade-in">
            <h1 class="text-4xl font-extrabold tracking-tight">Shopify Sync Dashboard</h1>
            <p class="mt-2 text-lg opacity-90">Seamless product synchronization with Apify, optimized for SEO</p>
            <button onclick="toggleDarkMode()" class="absolute top-4 right-4 bg-gray-200 dark:bg-gray-700 text-gray-800 dark:text-gray-200 px-3 py-1 rounded-full text-sm btn-hover">
                Toggle Dark Mode
            </button>
        </div>

        <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-6 mb-8">
            <div class="rounded-2xl p-6 card-hover fade-in">
                <h3 class="text-lg font-semibold text-blue-500 dark:text-blue-400">New Products</h3>
                <p class="text-3xl font-bold text-gray-900 dark:text-gray-100" id="newProducts">${stats.newProducts}</p>
            </div>
            <div class="rounded-2xl p-6 card-hover fade-in">
                <h3 class="text-lg font-semibold text-green-500 dark:text-green-400">Inventory Updates</h3>
                <p class="text-3xl font-bold text-gray-900 dark:text-gray-100" id="inventoryUpdates">${stats.inventoryUpdates}</p>
            </div>
            <div class="rounded-2xl p-6 card-hover fade-in">
                <h3 class="text-lg font-semibold text-orange-500 dark:text-orange-400">Discontinued</h3>
                <p class="text-3xl font-bold text-gray-900 dark:text-gray-100" id="discontinued">${stats.discontinued}</p>
            </div>
            <div class="rounded-2xl p-6 card-hover fade-in">
                <h3 class="text-lg font-semibold text-red-500 dark:text-red-400">Errors</h3>
                <p class="text-3xl font-bold text-gray-900 dark:text-gray-100" id="errors">${stats.errors}</p>
            </div>
        </div>

        <div class="rounded-2xl p-6 card-hover mb-8 fade-in">
            <h2 class="text-2xl font-semibold text-gray-900 dark:text-gray-100 mb-4">System Controls</h2>
            <div class="mb-6 p-4 rounded-lg ${systemPaused ? 'bg-red-100 dark:bg-red-900 border-red-300 dark:border-red-700' : 'bg-green-100 dark:bg-green-900 border-green-300 dark:border-green-700'} border">
                <div class="flex items-center justify-between">
                    <div>
                        <h3 class="font-medium ${systemPaused ? 'text-red-800 dark:text-red-300' : 'text-green-800 dark:text-green-300'}">
                            System Status: ${systemPaused ? 'PAUSED' : 'ACTIVE'}
                        </h3>
                        <p class="text-sm ${systemPaused ? 'text-red-600 dark:text-red-400' : 'text-green-600 dark:text-green-400'}">
                            ${systemPaused ? 'Automatic syncing is disabled' : 'Automatic syncing every 30min (inventory) & 6hrs (products)'}
                        </p>
                    </div>
                    <div class="flex items-center">
                        <button onclick="togglePause()" 
                                class="${systemPaused ? 'bg-green-500 hover:bg-green-600' : 'bg-red-500 hover:bg-red-600'} text-white px-4 py-2 rounded-lg btn-hover">
                            ${systemPaused ? 'Resume System' : 'Pause System'}
                        </button>
                        <div id="pauseSpinner" class="spinner"></div>
                    </div>
                </div>
            </div>
            <div class="flex flex-wrap gap-4">
                <div class="flex items-center">
                    <button onclick="triggerSync('products')" class="bg-blue-500 text-white px-6 py-3 rounded-lg btn-hover">Create New Products</button>
                    <div id="productsSpinner" class="spinner"></div>
                </div>
                <div class="flex items-center">
                    <button onclick="triggerSync('inventory')" class="bg-green-500 text-white px-6 py-3 rounded-lg btn-hover">Update Inventory</button>
                    <div id="inventorySpinner" class="spinner"></div>
                </div>
                <div class="flex items-center">
                    <button onclick="triggerSync('discontinued')" class="bg-orange-500 text-white px-6 py-3 rounded-lg btn-hover">Check Discontinued</button>
                    <div id="discontinuedSpinner" class="spinner"></div>
                </div>
            </div>
            <div class="mt-4 p-4 rounded-lg bg-gray-100 dark:bg-gray-700">
                <p class="text-sm text-gray-600 dark:text-gray-400"><strong>Manual controls work even when system is paused</strong></p>
                <p class="text-sm text-blue-600 dark:text-blue-400 mt-1"><strong>Updated:</strong> Fixed price detection for decimal values</p>
            </div>
        </div>

        <div class="rounded-2xl p-6 card-hover mb-8 fade-in">
            <h2 class="text-2xl font-semibold text-gray-900 dark:text-gray-100 mb-4">Mismatch Report</h2>
            <div class="overflow-x-auto">
                <table class="mismatch-table w-full text-sm text-left text-gray-500 dark:text-gray-400">
                    <thead class="text-xs text-gray-700 uppercase bg-gray-50 dark:bg-gray-700 dark:text-gray-400">
                        <tr>
                            <th class="px-4 py-2" onclick="sortTable(0)">Apify Title</th>
                            <th class="px-4 py-2" onclick="sortTable(1)">Apify Handle</th>
                            <th class="px-4 py-2" onclick="sortTable(2)">Apify URL</th>
                            <th class="px-4 py-2" onclick="sortTable(3)">Shopify Handle</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${mismatches.slice(0, 20).map(mismatch => `
                            <tr class="border-b dark:border-gray-700">
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
            <h2 class="text-2xl font-semibold text-gray-900 dark:text-gray-100 mb-4">Activity Log</h2>
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
        function toggleDarkMode() {
            document.documentElement.classList.toggle('dark');
            localStorage.setItem('darkMode', document.documentElement.classList.contains('dark'));
        }
        
        if (localStorage.getItem('darkMode') === 'true') {
            document.documentElement.classList.add('dark');
        }
        
        async function triggerSync(type) {
            const button = event.target;
            const spinner = document.getElementById(type + 'Spinner');
            const overlay = document.getElementById('loadingOverlay');
            
            button.disabled = true;
            spinner.style.display = 'inline-block';
            overlay.classList.add('active');
            
            try {
                const response = await fetch('/api/sync/' + type, { method: 'POST' });
                const result = await response.json();
                
                if (result.success) {
                    addLogEntry('✅ ' + result.message, 'success');
                    setTimeout(() => location.reload(), 2000);
                } else {
                    addLogEntry('❌ ' + (result.message || 'Sync failed'), 'error');
                }
            } catch (error) {
                console.error('Sync error:', error);
                addLogEntry('❌ Failed to trigger ' + type + ' sync', 'error');
            } finally {
                button.disabled = false;
                spinner.style.display = 'none';
                overlay.classList.remove('active');
            }
        }
        
        async function togglePause() {
            const button = event.target;
            const spinner = document.getElementById('pauseSpinner');
            const overlay = document.getElementById('loadingOverlay');
            
            button.disabled = true;
            spinner.style.display = 'inline-block';
            overlay.classList.add('active');
            
            try {
                const response = await fetch('/api/pause', { method: 'POST' });
                const result = await response.json();
                
                if (result.success) {
                    addLogEntry('🔄 System ' + (result.paused ? 'paused' : 'resumed'), 'info');
                    setTimeout(() => location.reload(), 1000);
                } else {
                    addLogEntry('❌ Failed to toggle pause', 'error');
                }
            } catch (error) {
                console.error('Pause toggle error:', error);
                addLogEntry('❌ Failed to toggle pause', 'error');
            } finally {
                button.disabled = false;
                spinner.style.display = 'none';
                overlay.classList.remove('active');
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
    </script>
</body>
</html>
  `);
});

app.get('/api/status', (req, res) => {
  res.json({
    ...stats,
    systemPaused,
    logs: logs.slice(0, 20),
    mismatches: mismatches.slice(0, 20),
    uptime: process.uptime(),
    environment: {
      apifyToken: config.apify.token ? 'SET' : 'MISSING',
      shopifyToken: config.shopify.accessToken ? 'SET' : 'MISSING',
      shopifyDomain: config.shopify.domain || 'MISSING'
    }
  });
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

app.get('/api/diagnose', async (req, res) => {
  try {
    addLog('Running diagnostic check...', 'info');
    
    const [apifyData, shopifyData] = await Promise.all([
      getApifyProducts(),
      getShopifyProducts()
    ]);
    
    // Sample some products for analysis
    const apifySample = apifyData.slice(0, 10);
    const shopifySample = shopifyData.slice(0, 10);
    
    const diagnosis = {
      apifyTotal: apifyData.length,
      shopifyTotal: shopifyData.length,
      shopifyWithApifyTag: shopifyData.filter(p => p.tags && p.tags.includes('Supplier:Apify')).length,
      apifySample: apifySample.map(item => ({
        title: item.title,
        url: item.url || item.canonicalUrl || 'none',
        sku: item.sku || 'none',
        generatedHandle: extractHandleFromCanonicalUrl(item, 0)
      })),
      shopifySample: shopifySample.map(item => ({
        title: item.title,
        handle: item.handle,
        sku: item.variants?.[0]?.sku || 'none',
        tags: item.tags
      }))
    };
    
    res.json(diagnosis);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.post('/api/pause', (req, res) => {
  systemPaused = !systemPaused;
  addLog(`System ${systemPaused ? 'paused' : 'resumed'}`, 'info');
  res.json({ success: true, paused: systemPaused });
});

app.post('/api/sync/products', async (req, res) => {
  try {
    addLog('Manual product sync triggered', 'info');
    const apifyData = await getApifyProducts();
    const processedProducts = processApifyProducts(apifyData, { processPrice: true });
    const shopifyData = await getShopifyProducts();
    const shopifyHandles = new Set(shopifyData.map(p => p.handle.toLowerCase()));
    const newProducts = processedProducts.filter(p => !shopifyHandles.has(p.handle.toLowerCase()));
    const result = await createNewProducts(newProducts);
    res.json({ success: true, message: `Created ${result.created} products`, data: result });
  } catch (error) {
    addLog(`Product sync failed: ${error.message}`, 'error');
    stats.errors++;
    res.status(500).json({ success: false, error: error.message });
  }
});

app.post('/api/sync/inventory', async (req, res) => {
  try {
    addLog('Manual inventory sync triggered', 'info');
    const result = await updateInventory();
    res.json({ success: true, message: `Updated ${result.updated} products`, data: result });
  } catch (error) {
    addLog(`Inventory sync failed: ${error.message}`, 'error');
    stats.errors++;
    res.status(500).json({ success: false, error: error.message });
  }
});

app.post('/api/sync/discontinued', async (req, res) => {
  try {
    addLog('Manual discontinued check triggered', 'info');
    const result = await handleDiscontinuedProducts();
    res.json({ success: true, message: `Discontinued ${result.discontinued} products`, data: result });
  } catch (error) {
    addLog(`Discontinued check failed: ${error.message}`, 'error');
    stats.errors++;
    res.status(500).json({ success: false, error: error.message });
  }
});

// Scheduled jobs
cron.schedule('*/30 * * * *', async () => {
  if (systemPaused) {
    addLog('Scheduled inventory update skipped - system paused', 'warning');
    return;
  }
  addLog('Starting scheduled inventory update', 'info');
  try { await updateInventory(); } catch (error) { 
    addLog(`Scheduled inventory update failed: ${error.message}`, 'error'); 
    stats.errors++; 
  }
});

cron.schedule('0 */6 * * *', async () => {
  if (systemPaused) {
    addLog('Scheduled product creation skipped - system paused', 'warning');
    return;
  }
  addLog('Starting scheduled product creation', 'info');
  try { 
    const apifyData = await getApifyProducts();
    const processedProducts = processApifyProducts(apifyData, { processPrice: true });
    const shopifyData = await getShopifyProducts();
    const shopifyHandles = new Set(shopifyData.map(p => p.handle.toLowerCase()));
    const newProducts = processedProducts.filter(p => !shopifyHandles.has(p.handle.toLowerCase()));
    await createNewProducts(newProducts);
    await handleDiscontinuedProducts();
  } catch (error) { 
    addLog(`Scheduled product creation failed: ${error.message}`, 'error'); 
    stats.errors++; 
  }
});

app.listen(PORT, () => {
  const requiredEnv = ['APIFY_TOKEN', 'SHOPIFY_ACCESS_TOKEN', 'SHOPIFY_DOMAIN'];
  const missing = requiredEnv.filter(key => !process.env[key]);
  if (missing.length > 0) {
    addLog(`FATAL: Missing environment variables: ${missing.join(', ')}`, 'error');
    process.exit(1);
  }
  addLog(`Shopify Sync Service started on port ${PORT}`, 'success');
  addLog('Inventory updates: Every 30 minutes', 'info');
  addLog('Product creation: Every 6 hours', 'info');
});
