// Optionally hardcode ADMIN_TOKEN for local demo (comment out for safety)
// window.ADMIN_TOKEN = ""; // set to your ADMIN_TOKEN env value if you want the Admin MV button to work

(function () {
  const tenantInput = document.getElementById('tenantId');
  const tenantStatus = document.getElementById('tenantStatus');
  const tenantApply = document.getElementById('tenantApply');
  const apiTokenInput = document.getElementById('apiToken');
  const apiApply = document.getElementById('apiApply');
  const apiStatus = document.getElementById('apiStatus');

  const storageKeys = {
    tenant: 'oslTenantId',
    api: 'oslApiToken'
  };

  function setStatus(el, message, isError = false) {
    if (!el) return;
    el.textContent = message || '';
    el.classList.toggle('tenant-status--error', Boolean(isError));
  }

  function currentTenant() {
    return tenantInput.value.trim();
  }

  function currentApiToken() {
    return apiTokenInput.value.trim();
  }

  function applyTenant() {
    const tid = currentTenant();
    if (tid) {
      try { localStorage.setItem(storageKeys.tenant, tid); } catch (_) {}
      setStatus(tenantStatus, `Tenant set: ${tid}`);
      return tid;
    }
    try { localStorage.removeItem(storageKeys.tenant); } catch (_) {}
    setStatus(tenantStatus, 'Enter a tenant UUID before using the demo.', true);
    return '';
  }

  function applyApiToken() {
    const token = currentApiToken();
    if (token) {
      try { localStorage.setItem(storageKeys.api, token); } catch (_) {}
      setStatus(apiStatus, 'API token saved');
    } else {
      try { localStorage.removeItem(storageKeys.api); } catch (_) {}
      setStatus(apiStatus, 'API token cleared');
    }
    return token;
  }

  function initialiseInputs() {
    const cachedTenant = (() => {
      try { return localStorage.getItem(storageKeys.tenant) || ''; } catch (_) { return ''; }
    })();
    const cachedToken = (() => {
      try { return localStorage.getItem(storageKeys.api) || ''; } catch (_) { return ''; }
    })();

    const defaultTenant = (window.DEFAULT_TENANT_ID || '').trim();
    if (cachedTenant) {
      tenantInput.value = cachedTenant;
      setStatus(tenantStatus, `Tenant set: ${cachedTenant}`);
    } else if (defaultTenant) {
      tenantInput.value = defaultTenant;
      setStatus(tenantStatus, `Tenant set: ${defaultTenant}`);
    } else {
      setStatus(tenantStatus, 'Enter a tenant UUID before using the demo.', true);
    }

    if (cachedToken) {
      apiTokenInput.value = cachedToken;
      setStatus(apiStatus, 'API token loaded');
    }
  }

  initialiseInputs();

  if (tenantApply) tenantApply.addEventListener('click', applyTenant);
  tenantInput.addEventListener('keydown', (evt) => {
    if (evt.key === 'Enter') {
      evt.preventDefault();
      applyTenant();
    }
  });
  tenantInput.addEventListener('blur', applyTenant);

  if (apiApply) apiApply.addEventListener('click', applyApiToken);
  apiTokenInput.addEventListener('keydown', (evt) => {
    if (evt.key === 'Enter') {
      evt.preventDefault();
      applyApiToken();
    }
  });
  apiTokenInput.addEventListener('blur', applyApiToken);

  document.body.addEventListener('htmx:configRequest', (evt) => {
    const tid = currentTenant();
    if (tid) {
      evt.detail.headers['X-Tenant-Id'] = tid;
    }
    const token = currentApiToken();
    if (token) {
      evt.detail.headers['X-Api-Token'] = token;
    }
  });

  document.body.addEventListener('htmx:responseError', () => {
    setStatus(tenantStatus, 'Request failed. Check the tenant ID or try again.', true);
  });

  document.body.addEventListener('htmx:afterOnLoad', (evt) => {
    setStatus(tenantStatus, currentTenant() ? `Tenant set: ${currentTenant()}` : 'Enter a tenant UUID before using the demo.', false);
    try {
      const resp = JSON.parse(evt.detail.xhr.responseText);
      if (evt.detail.target && evt.detail.target.matches('pre, .panel')) {
        evt.detail.target.textContent = JSON.stringify(resp, null, 2);
      }
    } catch (_) {}
  });

  function showPanel(id, data) {
    const el = document.getElementById(id);
    if (el) {
      el.textContent = JSON.stringify(data, null, 2);
    }
  }

  function showError(id, message) {
    const el = document.getElementById(id);
    if (el) {
      el.textContent = JSON.stringify({ error: message }, null, 2);
    }
    setStatus(tenantStatus, message, true);
  }

  async function requestJson(url, { method = 'POST', payload, requireTenant = true, requireToken = true } = {}) {
    const headers = { 'Content-Type': 'application/json' };
    let tid;
    if (requireTenant) {
      tid = applyTenant();
      if (!tid) throw new Error('Tenant ID is required.');
      headers['X-Tenant-Id'] = tid;
    }
    const token = currentApiToken();
    if (requireToken) {
      if (!token) throw new Error('API token is required for this action.');
      headers['X-Api-Token'] = token;
    } else if (token) {
      headers['X-Api-Token'] = token;
    }

    const res = await fetch(url, {
      method,
      headers,
      body: payload !== undefined ? JSON.stringify(payload) : undefined
    });
    let data = {};
    try { data = await res.json(); } catch (_) {}
    if (!res.ok) {
      throw new Error(data.error || `Request failed (${res.status})`);
    }
    return data;
  }

  window.createTenant = async function (event) {
    event.preventDefault();
    const name = document.getElementById('tenantName').value.trim();
    if (!name) {
      showError('tenantOut', 'Tenant name is required.');
      return false;
    }
    try {
      const data = await requestJson('/api/tenants', {
        payload: { name },
        requireTenant: false,
        requireToken: true
      });
      showPanel('tenantOut', data);
      if (data.tenant_id) {
        tenantInput.value = data.tenant_id;
        applyTenant();
      }
    } catch (err) {
      showError('tenantOut', err.message);
    }
    return false;
  };

  window.createProduct = async function (event) {
    event.preventDefault();
    const sku = document.getElementById('prodSku').value.trim();
    const name = document.getElementById('prodName').value.trim();
    const price = document.getElementById('prodPrice').value.trim();
    const description = document.getElementById('prodDescription').value.trim();
    const attrsText = document.getElementById('prodAttrs').value.trim();

    if (!sku || !name || !price) {
      showError('productOut', 'SKU, name, and price are required.');
      return false;
    }

    let attrs = {};
    if (attrsText) {
      try {
        attrs = JSON.parse(attrsText);
      } catch (err) {
        showError('productOut', 'Attributes must be valid JSON.');
        return false;
      }
    }

    try {
      const data = await requestJson('/api/products', {
        payload: {
          sku,
          name,
          price,
          description: description || undefined,
          attributes: attrs
        }
      });
      showPanel('productOut', data);
      if (data.id) {
        const orderProd = document.getElementById('orderProductId');
        if (orderProd) orderProd.value = data.id;
      }
    } catch (err) {
      showError('productOut', err.message);
    }
    return false;
  };

  window.createCustomer = async function (event) {
    event.preventDefault();
    const code = document.getElementById('custCode').value.trim();
    const name = document.getElementById('custName').value.trim();
    const email = document.getElementById('custEmail').value.trim();

    if (!code || !name) {
      showError('customerOut', 'Customer code and name are required.');
      return false;
    }

    try {
      const data = await requestJson('/api/customers', {
        payload: {
          code,
          name,
          email: email || undefined
        }
      });
      showPanel('customerOut', data);
      if (data.id) {
        const orderCust = document.getElementById('orderCustomerId');
        if (orderCust) orderCust.value = data.id;
      }
    } catch (err) {
      showError('customerOut', err.message);
    }
    return false;
  };

  window.createOrder = async function (event) {
    event.preventDefault();
    const customerId = document.getElementById('orderCustomerId').value.trim();
    const productId = document.getElementById('orderProductId').value.trim();
    const qtyVal = document.getElementById('orderQty').value;
    const externalRef = document.getElementById('orderExternal').value.trim();

    if (!customerId || !productId || !qtyVal) {
      showError('orderOut', 'Customer, product, and quantity are required.');
      return false;
    }

    const qty = parseInt(qtyVal, 10);
    if (!Number.isFinite(qty) || qty <= 0) {
      showError('orderOut', 'Quantity must be a positive integer.');
      return false;
    }

    try {
      const data = await requestJson('/api/orders', {
        payload: {
          customer_id: customerId,
          external_ref: externalRef || undefined,
          lines: [{ product_id: productId, qty }]
        },
        requireToken: false
      });
      showPanel('orderOut', data);
      if (data.order_id) {
        const orderIdField = document.getElementById('orderId');
        if (orderIdField) orderIdField.value = data.order_id;
      }
    } catch (err) {
      showError('orderOut', err.message);
    }
    return false;
  };

  async function postWithoutToken(url) {
    const tid = applyTenant();
    if (!tid) throw new Error('Tenant ID is required.');
    const res = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Tenant-Id': tid },
      body: JSON.stringify({})
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(data.error || `Request failed (${res.status})`);
    return data;
  }

  window.allocate = async function (event) {
    event.preventDefault();
    const orderId = document.getElementById('orderId').value.trim();
    if (!orderId) {
      showError('allocOut', 'Order ID is required.');
      return false;
    }
    try {
      const data = await postWithoutToken(`/api/orders/${orderId}/allocate`);
      showPanel('allocOut', data);
    } catch (err) {
      showError('allocOut', err.message);
    }
    return false;
  };

  window.releaseOrder = async function (event) {
    event.preventDefault();
    const orderId = document.getElementById('orderId').value.trim();
    if (!orderId) {
      showError('allocOut', 'Order ID is required.');
      return false;
    }
    try {
      const data = await postWithoutToken(`/api/orders/${orderId}/release`);
      showPanel('allocOut', data);
    } catch (err) {
      showError('allocOut', err.message);
    }
    return false;
  };

  window.createStockEvent = async function (event) {
    event.preventDefault();
    const eventType = document.getElementById('stockEventType').value;
    const productId = document.getElementById('stockProductId').value.trim();
    const warehouseId = document.getElementById('stockWarehouseId').value.trim();
    const locationId = document.getElementById('stockLocationId').value.trim();
    const lotId = document.getElementById('stockLotId').value.trim();
    const qtyVal = document.getElementById('stockQty').value;
    const reason = document.getElementById('stockReason').value.trim();

    if (!eventType || !productId || !warehouseId || !qtyVal) {
      showError('stockOut', 'Event type, product, warehouse, and qty are required.');
      return false;
    }

    const qty = parseInt(qtyVal, 10);
    if (!Number.isFinite(qty) || qty <= 0) {
      showError('stockOut', 'Quantity must be a positive integer.');
      return false;
    }

    try {
      const data = await requestJson('/api/stock_events', {
        payload: {
          event_type: eventType,
          product_id: productId,
          warehouse_id: warehouseId,
          location_id: locationId || undefined,
          lot_id: lotId || undefined,
          qty,
          reason: reason || undefined
        }
      });
      showPanel('stockOut', data);
    } catch (err) {
      showError('stockOut', err.message);
    }
    return false;
  };

  window.refreshCurrentStock = async function (event) {
    if (event) event.preventDefault();
    try {
      const data = await requestJson('/api/refresh_current_stock', {
        payload: {},
        requireToken: false
      });
      showPanel('stockOut', data);
    } catch (err) {
      showError('stockOut', err.message);
    }
    return false;
  };
})();
