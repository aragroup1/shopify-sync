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
      .replace(/\s*\(.*?\)\s*/g, '')
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
    .replace(/\s*\(.*?\)\s*/g, '')
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

function processApifyProducts(apifyData) {
  return apifyData.map((item, index) => {
    const handle = extractHandleFromCanonicalUrl(item, index);
    if (!handle) {
      addLog(`Failed to generate handle for ${item.title || 'unknown'}`, 'error');
      stats.errors++;
      return null;
    }

    let price = 0;
    if (item.variants && item.variants.length > 0) {
      const variant = item.variants[0];
      if (variant.price) {
        price = typeof variant.price === 'object' ? parseFloat(variant.price.current || 0) : parseFloat(variant.price);
      }
    }
    if (price === 0) price = parseFloat(item.price || 0);
    if (price > 100) price = price / 100;

    let finalPrice = 0;
    if (price <= 1) finalPrice = price + 6;
    else if (price <= 2) finalPrice = price + 7;
    else if (price <= 3) finalPrice = price + 8;
    else if (price <= 5) finalPrice = price + 9;
    else if (price <= 8) finalPrice = price + 10;
    else if (price <= 15) finalPrice = price * 2;
    else finalPrice = price * 2.2;

    if (finalPrice === 0) { 
      addLog(`Default price applied for ${item.title}: 5.00 → 15.00`, 'warning');
      price = 5.00; 
      finalPrice = 15.00; 
    }
    if (finalPrice > 100) {
      addLog(`Price capped for ${item.title}: ${finalPrice.toFixed(2)} → 100.00`, 'warning');
      finalPrice = 100.00;
    }

    let cleanTitle = item.title || 'Untitled Product';
    cleanTitle = cleanTitle.replace(/\b\d{4}\b/g, '')
      .replace(/\s*(large letter rate|parcel rate|big parcel rate|letter rate)\s*/gi, '')
      .replace(/\s*\(.*?\)\s*/g, '')
      .replace(/\s+/g, ' ')
      .trim();

    const images = [];
    if (item.medias && Array.isArray(item.medias)) {
      for (let i = 0; i < Math.min(3, item.medias.length); i++) {
        if (item.medias[i] && item.medias[i].url) {
          images.push(item.medias[i].url);
        }
      }
    }

    let inventory = item.variants?.[0]?.stockQuantity || item.stock || 10;
    const stockStatus = item.variants?.[0]?.price?.stockStatus;
    if (stockStatus === 'OUT_OF_STOCK') {
      inventory = 0;
    } else if (stockStatus === 'IN_STOCK' && inventory === 0) {
      inventory = 10;
    }

    if (index < 5) {
      addLog(`Inventory debug for ${cleanTitle}: stockQuantity=${item.variants?.[0]?.stockQuantity || 'N/A'}, stock=${item.stock || 'N/A'}, stockStatus=${stockStatus || 'N/A'}, final=${inventory}`, 'info');
      addLog(`Price debug for ${cleanTitle}: base=${price.toFixed(2)}, final=${finalPrice.toFixed(2)}`, 'info');
    }

    const productData = {
      handle, title: cleanTitle,
      description: item.description || `${cleanTitle}\n\nHigh-quality product from LandOfEssentials.`,
      sku: item.sku || '', originalPrice: price.toFixed(2), price: finalPrice.toFixed(2),
      compareAtPrice: (finalPrice * 1.2).toFixed(2), inventory, images, vendor: 'LandOfEssentials'
    };

    productData.seoDescription = generateSEODescription(productData);
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
    
    const processedApifyProducts = processApifyProducts(apifyData);
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

  let updated = 0, created = 0, errors = 0;
  mismatches = [];
  try {
    addLog('=== STARTING INVENTORY UPDATE WORKFLOW ===', 'info');
    const [apifyData, shopifyData] = await Promise.all([getApifyProducts(), getShopifyProducts()]);
    
    addLog(`Data comparison: ${apifyData.length} Apify products vs ${shopifyData.length} Shopify products`, 'info');
    
    const processedProducts = processApifyProducts(apifyData);
    addLog(`Processed ${processedProducts.length} valid Apify products after filtering`, 'info');
    
    const shopifyMap = new Map(shopifyData.map(p => [p.handle.toLowerCase(), p]));
    addLog(`Created Shopify lookup map with ${shopifyMap.size} products`, 'info');
    
    let matchedCount = 0;
    let skippedNoVariants = 0;
    let skippedSameInventory = 0;
    let skippedUnreliableZero = 0;
    const inventoryUpdates = [];
    const newProducts = [];
    
    processedProducts.forEach((apifyProduct, index) => {
      const shopifyProduct = shopifyMap.get(apifyProduct.handle.toLowerCase());
      
      if (!shopifyProduct) {
        addLog(`No Shopify match for: ${apifyProduct.handle} (title: ${apifyProduct.title}, url: ${apifyProduct.url || apifyProduct.canonicalUrl || 'N/A'})`, 'warning');
        mismatches.push({
          apifyTitle: apifyProduct.title,
          apifyHandle: apifyProduct.handle,
          apifyUrl: apifyProduct.url || apifyProduct.canonicalUrl || 'N/A',
          shopifyHandle: 'N/A'
        });
        newProducts.push(apifyProduct);
        return;
      }
      
      matchedCount++;
      
      if (!shopifyProduct.variants || !shopifyProduct.variants[0]) {
        skippedNoVariants++;
        addLog(`No variants for: ${shopifyProduct.handle}`, 'warning');
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
        addLog(`Skipped unreliable zero inventory for: ${apifyProduct.handle} (current: ${currentInventory}, stockStatus: ${apifyProduct.stockStatus || 'N/A'})`, 'warning');
        return;
      }
      
      inventoryUpdates.push({
        handle: apifyProduct.handle,
        title: shopifyProduct.title,
        currentInventory,
        newInventory: targetInventory,
        inventoryItemId: shopifyProduct.variants[0].inventory_item_id
      });
      
      if (inventoryUpdates.length <= 5) {
        addLog(`Update needed: ${apifyProduct.handle} (${currentInventory} → ${targetInventory})`, 'info');
      }
    });

    addLog(`=== MATCHING ANALYSIS ===`, 'info');
    addLog(`Matched products: ${matchedCount}`, 'info');
    addLog(`Skipped (no variants): ${skippedNoVariants}`, 'info');
    addLog(`Skipped (same inventory): ${skippedSameInventory}`, 'info');
    addLog(`Skipped (unreliable zero): ${skippedUnreliableZero}`, 'info');
    addLog(`Updates needed: ${inventoryUpdates.length}`, 'info');
    addLog(`New products to create: ${newProducts.length}`, 'info');

    for (const update of inventoryUpdates) {
      try {
        await shopifyClient.post('/inventory_levels/set.json', {
          location_id: config.shopify.locationId,
          inventory_item_id: update.inventoryItemId,
          available: update.newInventory
        });
        addLog(`✓ Updated: ${update.title} (${update.currentInventory} → ${update.newInventory})`, 'success');
        updated++;
        stats.inventoryUpdates++;
        await new Promise(resolve => setTimeout(resolve, 500));
      } catch (error) {
        errors++;
        stats.errors++;
        addLog(`✗ Failed: ${update.title} - ${error.message}`, 'error');
      }
    }

    if (newProducts.length > 0) {
      addLog('Creating new products for unmatched Apify products...', 'info');
      const createResult = await createNewProducts(newProducts);
      created = createResult.created;
      errors += createResult.errors;
    }

    stats.lastSync = new Date().toISOString();
    addLog(`=== INVENTORY UPDATE COMPLETE ===`, 'info');
    addLog(`Result: ${updated} updated, ${created} created, ${errors} errors`, (updated + created) > 0 ? 'success' : 'info');
    return { updated, created, errors, total: inventoryUpdates.length + newProducts.length };
  } catch (error) {
    addLog(`Inventory update workflow failed: ${error.message}`, 'error');
    stats.errors++;
    return { updated, created, errors: errors + 1, total: 0 };
  }
}

// Routes
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
        /* Your CSS styles here */
    </style>
</head>
<body class="min-h-screen font-sans transition-colors">
    <!-- Your HTML content here -->

    <script>
        function toggleDarkMode() {
            document.documentElement.classList.toggle('dark');
            localStorage.setItem('darkMode', document.documentElement.classList.contains('dark'));
        }
        if (localStorage.getItem('darkMode') === 'true') {
            document.documentElement.classList.add('dark');
        }
        async function triggerSync(type) {
            const button = document.querySelector('button[onclick="triggerSync(\'' + type + '\')"]');
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
                addLogEntry('❌ Failed to trigger ' + type + ' sync', 'error');
            } finally {
                button.disabled = false;
                spinner.style.display = 'none';
                overlay.classList.remove('active');
            }
        }
        async function togglePause() {
            const button = document.querySelector('button[onclick="togglePause()"]');
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
            const color = type === 'success' ? 'text-green-400' : type === 'error' ? 'text-red-400' : type === 'warning' ? 'text-yellow-400' : 'text-gray-300';
            const newLog = '<div class="' + color + '">[' + time + '] ' + message + '</div>';
            logContainer.innerHTML = newLog + logContainer.innerHTML;
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

app.post('/api/pause', (req, res) => {
  systemPaused = !systemPaused;
  addLog(`System ${systemPaused ? 'paused' : 'resumed'}`, 'info');
  res.json({ success: true, paused: systemPaused });
});

app.post('/api/sync/products', async (req, res) => {
  try {
    addLog('Manual product sync triggered', 'info');
    const apifyData = await getApifyProducts();
    const processedProducts = processApifyProducts(apifyData);
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
    res.json({ success: true, message: `Updated ${result.updated} products, created ${result.created} products`, data: result });
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
    const processedProducts = processApifyProducts(apifyData);
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
