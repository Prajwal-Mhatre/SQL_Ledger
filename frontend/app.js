// Optionally hardcode ADMIN_TOKEN for local demo (comment out for safety)
// window.ADMIN_TOKEN = ""; // set to your ADMIN_TOKEN env value if you want the UI button to work


(function () {
  const tenantInput = document.getElementById('tenantId');
  tenantInput.value = (window.DEFAULT_TENANT_ID || '');

  // Add X-Tenant-Id to all HTMX requests
  document.body.addEventListener('htmx:configRequest', function (evt) {
    const tid = tenantInput.value.trim();
    if (tid) {
      evt.detail.headers['X-Tenant-Id'] = tid;
    }
  });

  // Pretty-print JSON responses in panels
  document.body.addEventListener('htmx:afterOnLoad', function (evt) {
    try {
      const resp = JSON.parse(evt.detail.xhr.responseText);
      if (evt.detail.target && evt.detail.target.matches('pre, .panel')) {
        evt.detail.target.textContent = JSON.stringify(resp, null, 2);
      }
    } catch (e) {}
  });

  window.createOrder = async function (e) {
    e.preventDefault();
    const tid = tenantInput.value.trim();
    const body = {
      external_ref: "demo",
      lines: [
        { product_id: document.getElementById('productId').value.trim(), qty: parseInt(document.getElementById('qty').value, 10) }
      ]
    };
    const res = await fetch('/api/orders', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Tenant-Id': tid },
      body: JSON.stringify(body)
    });
    document.getElementById('orderOut').textContent = JSON.stringify(await res.json(), null, 2);
    return false;
  };

  window.allocate = async function (e) {
    e.preventDefault();
    const tid = tenantInput.value.trim();
    const oid = document.getElementById('orderId').value.trim();
    const res = await fetch(`/api/orders/${oid}/allocate`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Tenant-Id': tid },
      body: JSON.stringify({})
    });
    document.getElementById('allocOut').textContent = JSON.stringify(await res.json(), null, 2);
    return false;
  };
})();
