// Optionally hardcode ADMIN_TOKEN for local demo (comment out for safety)
// window.ADMIN_TOKEN = ""; // set to your ADMIN_TOKEN env value if you want the UI button to work

(function () {
  const tenantInput = document.getElementById('tenantId');
  const tenantStatus = document.getElementById('tenantStatus');
  const tenantApply = document.getElementById('tenantApply');
  const storageKey = 'oslTenantId';

  function currentTenant() {
    return tenantInput.value.trim();
  }

  function setStatus(tid) {
    if (!tenantStatus) return;
    if (tid) {
      tenantStatus.textContent = `Tenant set: ${tid}`;
      tenantStatus.classList.remove('tenant-status--error');
    } else {
      tenantStatus.textContent = 'Enter a tenant UUID before using the demo.';
      tenantStatus.classList.add('tenant-status--error');
    }
  }

  function applyTenant() {
    const tid = currentTenant();
    if (tid) {
      try { localStorage.setItem(storageKey, tid); } catch (_) {}
      setStatus(tid);
      return tid;
    }
    try { localStorage.removeItem(storageKey); } catch (_) {}
    setStatus('');
    return '';
  }

  const initial = (() => {
    try {
      const cached = localStorage.getItem(storageKey);
      if (cached) return cached;
    } catch (_) {}
    return window.DEFAULT_TENANT_ID || '';
  })().trim();

  if (initial) {
    tenantInput.value = initial;
    setStatus(initial);
  } else {
    setStatus('');
  }

  if (tenantApply) {
    tenantApply.addEventListener('click', applyTenant);
  }
  tenantInput.addEventListener('keydown', (evt) => {
    if (evt.key === 'Enter') {
      evt.preventDefault();
      applyTenant();
    }
  });
  tenantInput.addEventListener('blur', applyTenant);

  document.body.addEventListener('htmx:configRequest', (evt) => {
    const tid = currentTenant();
    if (tid) {
      evt.detail.headers['X-Tenant-Id'] = tid;
    } else {
      setStatus('');
    }
  });

  document.body.addEventListener('htmx:responseError', () => {
    if (!tenantStatus) return;
    tenantStatus.textContent = 'Request failed. Check the tenant ID or try again.';
    tenantStatus.classList.add('tenant-status--error');
  });

  document.body.addEventListener('htmx:afterOnLoad', (evt) => {
    if (!tenantStatus) return;
    tenantStatus.classList.remove('tenant-status--error');
    try {
      const resp = JSON.parse(evt.detail.xhr.responseText);
      if (evt.detail.target && evt.detail.target.matches('pre, .panel')) {
        evt.detail.target.textContent = JSON.stringify(resp, null, 2);
      }
    } catch (_) {}
  });

  window.createOrder = async function (e) {
    e.preventDefault();
    const tid = applyTenant();
    if (!tid) return false;
    const lineQty = parseInt(document.getElementById('qty').value, 10);
    const payload = {
      external_ref: 'demo',
      lines: [
        { product_id: document.getElementById('productId').value.trim(), qty: lineQty }
      ]
    };
    const res = await fetch('/api/orders', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Tenant-Id': tid },
      body: JSON.stringify(payload)
    });
    const data = await res.json();
    document.getElementById('orderOut').textContent = JSON.stringify(data, null, 2);
    if (!res.ok && tenantStatus) {
      tenantStatus.textContent = data.error || 'Order create failed.';
      tenantStatus.classList.add('tenant-status--error');
    }
    return false;
  };

  window.allocate = async function (e) {
    e.preventDefault();
    const tid = applyTenant();
    if (!tid) return false;
    const oid = document.getElementById('orderId').value.trim();
    const res = await fetch(`/api/orders/${oid}/allocate`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Tenant-Id': tid },
      body: JSON.stringify({})
    });
    const data = await res.json();
    document.getElementById('allocOut').textContent = JSON.stringify(data, null, 2);
    if (!res.ok && tenantStatus) {
      tenantStatus.textContent = data.error || 'Allocation failed.';
      tenantStatus.classList.add('tenant-status--error');
    }
    return false;
  };

  window.releaseOrder = async function (e) {
    e.preventDefault();
    const tid = applyTenant();
    if (!tid) return false;
    const oid = document.getElementById('orderId').value.trim();
    const res = await fetch(`/api/orders/${oid}/release`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Tenant-Id': tid },
      body: JSON.stringify({})
    });
    const data = await res.json();
    document.getElementById('allocOut').textContent = JSON.stringify(data, null, 2);
    if (!res.ok && tenantStatus) {
      tenantStatus.textContent = data.error || 'Release failed.';
      tenantStatus.classList.add('tenant-status--error');
    }
    return false;
  };
})();

