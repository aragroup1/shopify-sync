const express = require('express');
const axios = require('axios');
const cron = require('node-cron');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());

// In-memory storage
let stats = { newProducts: 0, inventoryUpdates: 0, discontinued: 0, errors: 0, lastSync: null };
let runHistory = []; 
let errorLog = []; 
let logs = [];
let systemPaused = false;
let mismatches = [];
const missingCounters = new Map();

// MODIFIED: Failsafe state management
let failsafeTriggered = false; // Can be: false, 'pending', true
let failsafeReason = '';
let pendingFailsafeAction = null; // Stores the action to be confirmed

const jobLocks = { inventory: false, products: false, discontinued: false, fixTracking: false };
let abortVersion = 0;
const getJobToken = () => abortVersion;
// MODIFIED: shouldAbort now also checks for pending failsafe
const shouldAbort = (token) => systemPaused || failsafeTriggered === true || failsafeTriggered === 'pending' || token !== abortVersion;

// Failsafe configuration
const FAILSAFE_LIMITS = {
  MIN_APIFY_PRODUCTS: Number(process.env.MIN_APIFY_PRODUCTS || 100),
  MIN_SHOPIFY_PRODUCTS: Number(process.env.MIN_SHOPIFY_PRODUCTS || 100),
  MAX_CHANGE_PERCENTAGE: Number(process.env.MAX_CHANGE_PERCENTAGE || 30),
  MAX_INVENTORY_UPDATE_PERCENTAGE: Number(process.env.MAX_INVENTORY_UPDATE_PERCENTAGE || 5),
  MAX_ERROR_RATE: Number(process.env.MAX_ERROR_RATE || 20),
  MAX_DISCONTINUED_AT_ONCE: Number(process.env.MAX_DISCONTINUED_AT_ONCE || 100),
  FETCH_TIMEOUT: Number(process.env.FETCH_TIMEOUT || 300000)
};
const DISCONTINUE_MISS_RUNS = Number(process.env.DISCONTINUE_MISS_RUNS || 3);
const MAX_CREATE_PER_RUN = Number(process.env.MAX_CREATE_PER_RUN || 200);

const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID || '1596350649';
const TELEGRAM_WEBHOOK_SECRET = process.env.TELEGRAM_WEBHOOK_SECRET || '';
let lastFailsafeNotified = '';

async function notifyTelegram(text) {
  // ... (implementation remains the same)
  if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) return;
  try {
    await axios.post(
      `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`,
      { chat_id: TELEGRAM_CHAT_ID, text, parse_mode: 'HTML' },
      { timeout: 15000 }
    );
  } catch (e) {
    addLog(`Telegram notify failed: ${e.message}`, 'warning');
  }
}

let lastKnownGoodState = { apifyCount: 0, shopifyCount: 0, timestamp: null };

function addLog(message, type = 'info', jobType = 'system') {
  // ... (implementation remains the same)
}

function addToHistory(type, data) {
    // ... (implementation remains the same)
}

function startBackgroundJob(key, name, fn) {
  // ... (implementation remains the same)
}

// MODIFIED: triggerFailsafe now handles 'pending' state
function triggerFailsafe(msg, contextData = {}, isConfirmable = false, action = null) {
  if (failsafeTriggered) return; // Don't trigger a new failsafe if one is already active/pending

  failsafeReason = msg;
  systemPaused = true;
  abortVersion++;
  addLog(`⚠️ APIFY (MH) FAILSAFE TRIGGERED: ${failsafeReason}`, 'error', 'failsafe');

  if (isConfirmable && action) {
    failsafeTriggered = 'pending';
    pendingFailsafeAction = action;
    addLog('System paused, waiting for user confirmation.', 'warning', 'failsafe');
    const notification = `
⚠️ <b>APIFY Failsafe Warning - Confirmation Required</b> ⚠️

<b>Reason:</b>
<pre>${msg}</pre>

An unusual operation is about to be performed. The system is now paused. Please review and decide:

- To proceed with this action, reply: <code>/confirm</code>
- To abort this action and keep the system paused, reply: <code>/abort</code>
        `;
    notifyTelegram(notification);
  } else {
    failsafeTriggered = true;
    addLog('System automatically paused to prevent potential damage.', 'error', 'failsafe');
    if (lastFailsafeNotified !== msg) {
        lastFailsafeNotified = msg;
        notifyTelegram(`🚨 <b>Failsafe Triggered & System Paused</b> 🚨\n\n<b>Reason:</b>\n<pre>${msg}</pre>`);
    }
  }
}

// MODIFIED: checkFailsafeConditions now returns an object and can trigger a pending state
function checkFailsafeConditions(context, data = {}, actionToConfirm = null) {
  const checks = [];
  let isConfirmable = false;

  switch (context) {
    case 'inventory': {
      isConfirmable = true; // Inventory updates are confirmable
      if (data.totalApifyProducts > 0 && data.updatesNeeded > data.totalApifyProducts * (FAILSAFE_LIMITS.MAX_INVENTORY_UPDATE_PERCENTAGE / 100)) {
        checks.push(`Too many inventory changes: ${data.updatesNeeded} > ${Math.floor(data.totalApifyProducts * (FAILSAFE_LIMITS.MAX_INVENTORY_UPDATE_PERCENTAGE / 100))} (${FAILSAFE_LIMITS.MAX_INVENTORY_UPDATE_PERCENTAGE}% of ${data.totalApifyProducts})`);
      }
      break;
    }
    // ... other cases like 'discontinued', 'products' could also be made confirmable
    // For now, let's keep them as hard failsafes for simplicity
  }

  if (checks.length > 0) {
    const reason = checks.join('; ');
    triggerFailsafe(reason, { checkContext: context, checkData: data }, isConfirmable, actionToConfirm);
    return { proceed: false, reason: isConfirmable ? 'pending_confirmation' : 'hard_stop' };
  }
  return { proceed: true };
}


// ... (Configuration, API clients, and all helper functions remain the same)

// NEW: Extracted execution logic so it can be called on confirmation
async function executeInventoryUpdates(updates, token) {
  let updated = 0, errors = 0;
  if (!updates || updates.length === 0) return { updated, errors };
  
  addLog(`Executing ${updates.length} inventory updates...`, 'info', 'inventory');

  for (const update of updates) {
      if (shouldAbort(token)) { addLog('Aborting inventory execution...', 'warning', 'inventory'); break; }
      try {
          await shopifyClient.post('/inventory_levels/set.json', { location_id: parseInt(config.shopify.locationId), inventory_item_id: update.inventoryItemId, available: update.newInventory });
          addLog(`✓ Updated: ${update.title} (${update.currentInventory} → ${update.newInventory})`, 'success', 'inventory');
          updated++;
          stats.inventoryUpdates++;
          await new Promise(r => setTimeout(r, 400));
      } catch (error) {
          errors++;
          stats.errors++;
          addLog(`✗ Failed to update ${update.title}: ${error.message}`, 'error', 'inventory');
      }
  }
  addLog(`Execution finished: ${updated} updated, ${errors} errors.`, 'info', 'inventory');
  return { updated, errors };
}

// MODIFIED: updateInventoryJob now has a "prepare" and "execute" phase
async function updateInventoryJob(token) {
  if (systemPaused) return { updated: 0, errors: 0, total: 0 };
  let errors = 0;

  try {
    const [apifyData, shopifyData] = await Promise.all([getApifyProducts(), getShopifyProducts()]);
    if (shouldAbort(token)) return { updated: 0, errors: 0 };
    
    // PREPARE PHASE: Calculate all changes
    const processedProducts = processApifyProducts(apifyData, { processPrice: false });
    const maps = buildShopifyMaps(shopifyData);
    const inventoryUpdates = [];
    processedProducts.forEach((apifyProduct) => {
        const { product: shopifyProduct } = matchShopifyProduct(apifyProduct, maps);
        if (!shopifyProduct || !shopifyProduct.variants?.[0]?.inventory_item_id) return;
        if ((shopifyProduct.variants[0].inventory_quantity || 0) === apifyProduct.inventory) return;
        inventoryUpdates.push({
            title: shopifyProduct.title,
            currentInventory: shopifyProduct.variants[0].inventory_quantity || 0,
            newInventory: apifyProduct.inventory,
            inventoryItemId: shopifyProduct.variants[0].inventory_item_id,
        });
    });

    addLog(`Inventory updates prepared: ${inventoryUpdates.length} changes needed.`, 'info', 'inventory');
    
    // FAILSAFE CHECK
    const actionToConfirm = { type: 'inventory', data: inventoryUpdates };
    const failsafeCheck = checkFailsafeConditions('inventory', { updatesNeeded: inventoryUpdates.length, totalApifyProducts: apifyData.length }, actionToConfirm);

    if (!failsafeCheck.proceed) {
        addLog(`Failsafe triggered: ${failsafeCheck.reason}. Job will not proceed automatically.`, 'warning', 'inventory');
        return { updated: 0, errors: 0 }; // Stop the job here
    }

    // EXECUTE PHASE
    const { updated, errors: execErrors } = await executeInventoryUpdates(inventoryUpdates, token);
    errors += execErrors;

    stats.lastSync = new Date().toISOString();
    addToHistory('inventory', { updated, errors, attempted: inventoryUpdates.length });
    return { updated, errors };

  } catch (error) {
    addLog(`Inventory workflow failed: ${error.message}`, 'error', 'inventory');
    stats.errors++;
    return { updated: 0, errors: errors + 1 };
  }
}

// ... (other jobs like createNewProductsJob, handleDiscontinuedProductsJob remain the same for now)

// UI (MODIFIED to show pending confirmation state)
app.get('/', (req, res) => {
  let failsafeBanner = '';
  if (failsafeTriggered === 'pending') {
    failsafeBanner = `
      <div class="mb-4 p-4 rounded-lg bg-yellow-900 border-2 border-yellow-500">
        <h3 class="font-bold text-yellow-300">⚠️ CONFIRMATION REQUIRED</h3>
        <p class="text-sm text-yellow-400 mb-4">Reason: ${failsafeReason}</p>
        <div class="flex gap-4">
            <button onclick="confirmFailsafe()" class="bg-green-500 hover:bg-green-600 text-white px-4 py-2 rounded-lg btn-hover">Proceed Anyway</button>
            <button onclick="abortFailsafe()" class="bg-red-500 hover:bg-red-600 text-white px-4 py-2 rounded-lg btn-hover">Abort & Pause</button>
        </div>
      </div>`;
  } else if (failsafeTriggered === true) {
    failsafeBanner = `
      <div class="mb-4 p-4 rounded-lg bg-red-900 border-2 border-red-500">
        <div class="flex items-center justify-between">
            <div><h3 class="font-bold text-red-300">🚨 FAILSAFE TRIGGERED</h3><p class="text-sm text-red-400">${failsafeReason}</p></div>
            <button onclick="clearFailsafe()" class="bg-red-500 hover:bg-red-600 text-white px-4 py-2 rounded-lg btn-hover">Clear Failsafe</button>
        </div>
      </div>`;
  }
  
  res.send(`
<!DOCTYPE html>
<!-- ... (HTML head remains the same) -->
<body class="min-h-screen font-sans">
  <div class="container mx-auto px-4 py-8">
    <!-- ... (Header and stats cards remain the same) -->
    
    <div class="rounded-2xl p-6 card-hover mb-8 fade-in">
      <h2 class="text-2xl font-semibold text-gray-100 mb-4">System Controls</h2>
      ${failsafeBanner}
      <!-- ... (rest of the controls section remains the same) -->
    </div>

    <!-- ... (rest of the HTML remains the same) -->
  </div>

  <script>
    // ... (existing JS functions remain)
    
    // NEW: Functions to handle failsafe confirmation
    async function confirmFailsafe() {
        if (!confirm('Are you sure you want to proceed with the action that triggered the failsafe?')) return;
        try {
            const res = await fetch('/api/failsafe/confirm', { method: 'POST' });
            if (res.ok) {
                addLogEntry('✅ Failsafe action confirmed and is now executing.', 'success');
                setTimeout(() => location.reload(), 2000);
            }
        } catch(e) { addLogEntry('❌ Failed to confirm failsafe.', 'error'); }
    }
    
    async function abortFailsafe() {
        if (!confirm('Are you sure you want to abort the pending action? The system will remain paused.')) return;
        try {
            const res = await fetch('/api/failsafe/abort', { method: 'POST' });
            if (res.ok) {
                addLogEntry('⏹️ Failsafe action aborted. System remains paused.', 'warning');
                setTimeout(() => location.reload(), 1000);
            }
        } catch(e) { addLogEntry('❌ Failed to abort failsafe.', 'error'); }
    }
    
    async function clearFailsafe() {
        await fetch('/api/failsafe/clear', { method: 'POST' });
        location.reload();
    }
  </script>
</body>
</html>
  `);
});

// MODIFIED: /api/status now includes the detailed failsafe state
app.get('/api/status', (req, res) => {
  res.json({
    stats,
    runHistory,
    systemPaused,
    failsafeTriggered, // Now sends false, 'pending', or true
    failsafeReason,
    logs: logs.slice(0, 50),
    mismatches: mismatches.slice(0, 50),
  });
});

// MODIFIED: /api/failsafe/clear now resets pending state too
app.post('/api/failsafe/clear', (req, res) => {
  failsafeTriggered = false;
  failsafeReason = '';
  pendingFailsafeAction = null;
  systemPaused = false; // Also resume the system
  lastFailsafeNotified = '';
  addLog('Failsafe cleared manually and system resumed.', 'info', 'system');
  notifyTelegram('✅ Failsafe cleared, system resumed.');
  res.json({ success: true });
});

// NEW: API endpoints for failsafe confirmation
app.post('/api/failsafe/confirm', async (req, res) => {
  if (failsafeTriggered !== 'pending' || !pendingFailsafeAction) {
    return res.status(400).json({ success: false, message: 'No pending failsafe action to confirm.' });
  }

  addLog('Failsafe action confirmed by user. Executing now...', 'info', 'system');
  notifyTelegram('▶️ User confirmed failsafe action. Execution started.');
  
  const action = pendingFailsafeAction;
  
  // Reset state BEFORE execution to prevent double-runs
  failsafeTriggered = false;
  failsafeReason = '';
  pendingFailsafeAction = null;
  systemPaused = false; // Unpause the system

  // Execute the stored action in the background
  if (action.type === 'inventory') {
    startBackgroundJob('inventory', 'Confirmed Inventory Sync', async (token) => {
        const { updated, errors } = await executeInventoryUpdates(action.data, token);
        addToHistory('inventory', { updated, errors, attempted: action.data.length, confirmed: true });
    });
  }
  
  res.json({ success: true, message: 'Action is executing in the background.' });
});

app.post('/api/failsafe/abort', (req, res) => {
  if (failsafeTriggered !== 'pending') {
    return res.status(400).json({ success: false, message: 'No pending failsafe action to abort.' });
  }

  failsafeTriggered = true; // Move to a hard-paused state
  pendingFailsafeAction = null;
  systemPaused = true;
  
  addLog('Pending failsafe action aborted by user. System remains paused.', 'warning', 'system');
  notifyTelegram('⏹️ User aborted failsafe action. System remains paused.');

  res.json({ success: true });
});

// ... (other API endpoints like /stats/reset, /pause remain)

// MODIFIED: Telegram webhook to handle /confirm and /abort
app.post('/telegram/webhook/:secret?', async (req, res) => {
    // ... (existing setup and other commands remain)
    const text = (req.body?.message?.text || '').trim().toLowerCase();

    if (text === '/confirm') {
        if (failsafeTriggered === 'pending' && pendingFailsafeAction) {
            await axios.post(`http://127.0.0.1:${PORT}/api/failsafe/confirm`); // Internal API call
            return res.json({ ok: true });
        } else {
            await notifyTelegram('No pending action to confirm.');
            return res.json({ ok: true });
        }
    }

    if (text === '/abort') {
        if (failsafeTriggered === 'pending') {
            await axios.post(`http://127.0.0.1:${PORT}/api/failsafe/abort`); // Internal API call
            return res.json({ ok: true });
        } else {
            await notifyTelegram('No pending action to abort.');
            return res.json({ ok: true });
        }
    }

    // ... (rest of the command handlers)
    res.json({ ok: true });
});


// ... (Schedules and app.listen remain the same)
// Schedules: Inventory (Daily 1am), Products (Weekly Fri 2am), Discontinued (Daily 3am)
cron.schedule('0 1 * * *', () => {
  if (systemPaused || failsafeTriggered) return;
  startBackgroundJob('inventory', 'Scheduled inventory sync', async (token) => await updateInventoryJob(token));
});

// ... (other cron jobs)

app.listen(PORT, () => {
    // ... (startup logs)
});
