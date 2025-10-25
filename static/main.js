'use strict';

(function () {
  var $ = function (s) { return document.querySelector(s); };
  var $$ = function (s) { return Array.prototype.slice.call(document.querySelectorAll(s)); };

  function humanSize(bytes) {
    if (bytes === null || bytes === undefined) return '-';
    var units = ['B','KB','MB','GB','TB'];
    var i = 0, n = bytes;
    while (n >= 1024 && i < units.length - 1) { n = n / 1024; i++; }
    return n.toFixed(1) + ' ' + units[i];
  }

  async function startScan() {
    var diskName = $('#disk-name').value.trim();
    var folderPath = $('#folder-path').value.trim();
    if (!diskName || !folderPath) { alert('Indica nombre del disco y carpeta.'); return; }
    var btn = $('#scan-btn'); btn.disabled = true;
    try {
      var resp = await fetch('/scan', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ disk_name: diskName, folder: folderPath })
      });
      var data = await resp.json();
      if (data.status !== 'ok') alert(data.message || 'Error iniciando escaneo');
      pollDisks();
    } catch (e) { console.error(e); alert('Error de red'); }
    finally { btn.disabled = false; }
  }

  async function fetchDisks() {
    try {
      const resp = await fetch('/disks');
      const json = await resp.json();
      // Si backend devuelve error JSON, devolvemos []
      if (!Array.isArray(json)) {
        console.error('Disks API error:', json);
        return [];
      }
      return json;
    } catch (e) {
      console.error('Disks fetch failed', e);
      return [];
    }
  }

  let pollTimer = null;
  async function pollDisks() {
    if (pollTimer) clearInterval(pollTimer);
    await loadDisks();
    pollTimer = setInterval(loadDisks, 1200);
  }

  function renderProgress(d) {
    const totalSeg = d.segments_total || 0;
    const doneSeg  = d.segments_done  || 0;
    let pct = 0;
    if (totalSeg > 0) pct = Math.min(100, Math.round(doneSeg * 100 / totalSeg));
    else if (d.status === 'done') pct = 100;
    return `
      <div class="progress-bar">
        <div class="progress determinate" style="width:${pct}%"></div>
      </div>
      <div class="progress-info">
        ${pct}% · carpetas ${doneSeg}/${totalSeg}
        ${d.processed_files ? ` · ficheros ${d.processed_files}` : ``}
      </div>
    `;
  }

  async function loadDisks() {
    const disks = await fetchDisks();
    const container = $('#disks-list');
    if (!Array.isArray(disks)) {
      container.innerHTML = '<div class="disk-message">Error cargando discos.</div>';
      return;
    }
    container.innerHTML = disks.map(function (d) {
      const last = d.last_scan_date ? new Date(d.last_scan_date).toLocaleString() : '-';
      return `
        <div class="disk-card">
          <div class="disk-header">
            <span class="disk-name">${d.disk_name}</span>
            <span class="disk-status ${d.status}">${d.status || 'idle'}</span>
          </div>
          <div class="disk-meta">
            <div>Último escaneo: <strong>${last}</strong></div>
            <div>Ficheros procesados: <strong>${d.processed_files || 0}</strong></div>
            <div>Bytes procesados: <strong>${humanSize(d.processed_bytes || 0)}</strong></div>
          </div>
          ${renderProgress(d)}
          <div class="disk-message">${d.message || ''}</div>
        </div>
      `;
    }).join('');

    if (!disks.some(d => d.status === 'indexing')) {
      clearInterval(pollTimer);
    }
  }

  // ---------- BUSCADOR AVANZADO ----------
  var currentPage = 0;
  var pageSize = 50;

  function readFilters() {
    return {
      q: $('#f-q').value.trim(),
      disk: $('#f-disk').value.trim(),
      folder: $('#f-folder').value.trim(),
      name: $('#f-name').value.trim(),
      ext: $('#f-ext').value.trim().replace(/^\./, ''),
      size_min: $('#f-smin').value ? parseInt($('#f-smin').value, 10) : '',
      size_max: $('#f-smax').value ? parseInt($('#f-smax').value, 10) : '',
      created_from: $('#f-cfrom').value,
      created_to: $('#f-cto').value,
      modified_from: $('#f-mfrom').value,
      modified_to: $('#f-mto').value,
      order_by: $('#f-order').value
    };
  }

  async function searchFiles(page) {
    if (page === undefined) page = 0;
    currentPage = page;
    const f = readFilters();
    const params = new URLSearchParams();
    Object.keys(f).forEach(k => { const v = f[k]; if (v !== '' && v != null) params.set(k, v); });
    params.set('limit', String(pageSize));
    params.set('offset', String(page * pageSize));
    const url = '/search?' + params.toString();
    try {
      const resp = await fetch(url);
      const data = await resp.json();
      if (!data || typeof data !== 'object' || !Array.isArray(data.items)) {
        console.error('Search API error:', data);
        renderTable([]); renderPager(0, page, pageSize);
        return;
      }
      renderTable(data.items || []);
      renderPager(data.total || 0, page, pageSize);
    } catch (e) {
      console.error('Search fetch failed', e);
      renderTable([]); renderPager(0, page, pageSize);
    }
  }

  function renderTable(items) {
    const tbody = $('#results-body');
    tbody.innerHTML = items.map(r => {
      const created = r.created_at ? r.created_at.replace('T',' ') : '-';
      const modified = r.modified_at ? r.modified_at.replace('T',' ') : '-';
      return `
        <tr>
          <td>${r.id}</td>
          <td>${r.disk_name}</td>
          <td class="mono">${r.folder}</td>
          <td class="mono">${r.file_name}</td>
          <td class="num">${humanSize(r.size)}</td>
          <td>${created}</td>
          <td>${modified}</td>
        </tr>
      `;
    }).join('');
  }

  function renderPager(total, page, size) {
    const pages = Math.ceil(total / size);
    $('#pager').innerHTML =
      '<div class="pager-info">Mostrando página ' + (page + 1) + ' de ' + (pages || 1) + ' · ' + total + ' resultados</div>' +
      '<div class="pager-buttons">' +
        '<button ' + (page <= 0 ? 'disabled' : '') + ' id="btn-prev">Anterior</button>' +
        '<button ' + (page >= pages - 1 ? 'disabled' : '') + ' id="btn-next">Siguiente</button>' +
      '</div>';
    const prev = $('#btn-prev'), next = $('#btn-next');
    if (prev) prev.addEventListener('click', function () { searchFiles(page - 1); });
    if (next) next.addEventListener('click', function () { searchFiles(page + 1); });
  }

  document.addEventListener('DOMContentLoaded', function () {
    const btn = $('#scan-btn');
    if (btn) btn.addEventListener('click', startScan);
    let debounceTimer = null;
    $$('#filters input, #filters select').forEach(function (el) {
      el.addEventListener('input', function () {
        clearTimeout(debounceTimer);
        debounceTimer = setTimeout(function () { searchFiles(0); }, 350);
      });
      el.addEventListener('change', function () { searchFiles(0); });
    });
    loadDisks(); searchFiles(0);
  });
})();

