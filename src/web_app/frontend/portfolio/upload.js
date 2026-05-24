import { $, $$, fetchJson, state, refreshSummary, renderActivityBreakdown } from './common.js';
import { refreshSymbols, loadTransactions } from './transactions.js';
import { loadHoldings } from './holdings.js';

export function initUpload() {
  wireUpload();
}

function wireUpload() {
  const dropZone = $('#pf-drop-zone');
  const fileInput = $('#pf-file-input');
  const queue = $('#pf-upload-queue');
  const results = $('#pf-upload-results');

  if (!dropZone || !fileInput) return;

  dropZone.addEventListener('dragover', e => { e.preventDefault(); dropZone.classList.add('drag-over'); });
  dropZone.addEventListener('dragleave', () => dropZone.classList.remove('drag-over'));

  dropZone.addEventListener('drop', (e) => {
    e.preventDefault();
    dropZone.classList.remove('drag-over');
    if (e.dataTransfer.files.length) {
      uploadFiles(Array.from(e.dataTransfer.files));
    }
  });

  fileInput.addEventListener('change', () => {
    if (fileInput.files.length) {
      uploadFiles(Array.from(fileInput.files));
      fileInput.value = '';
    }
  });

  dropZone.addEventListener('click', (e) => { if (e.target !== fileInput) fileInput.click(); });

  async function uploadFiles(files) {
    state.uploadedFiles = state.uploadedFiles || [];
    const xmlFiles = files.filter(f => f.name.toLowerCase().endsWith('.xml'));
    if (!xmlFiles.length) {
      queue.style.display = 'block';
      queue.innerHTML = '<div class="status-text error">No .xml files selected.</div>';
      return;
    }

    queue.style.display = 'block';
    queue.innerHTML = xmlFiles.map((f, i) =>
      `<div class="status-text info" data-queue="${i}">⏳ Queued: ${f.name}</div>`
    ).join('');

    const allResults = [];
    for (let i = 0; i < xmlFiles.length; i++) {
      const file = xmlFiles[i];
      const status = document.querySelector(`[data-queue="${i}"]`);
      if (status) status.textContent = `⏳ Uploading: ${file.name}…`;

      try {
        const form = new FormData();
        form.append('file', file);
        const resp = await fetch('/api/portfolio/upload', { method: 'POST', body: form });
        if (!resp.ok) {
          const err = await resp.json();
          throw new Error(err.detail || `Upload failed (${resp.status})`);
        }
        const data = await resp.json();
        if (status) {
          status.textContent = `✓ ${file.name}: ${data.inserted} new, ${data.skipped} skipped`;
          status.className = 'status-text success';
        }
        allResults.push(data);
        state.uploadedFiles.push(file.name);
      } catch (err) {
        if (status) {
          status.textContent = `✗ ${file.name}: ${err.message}`;
          status.className = 'status-text error';
        }
      }
    }

    const totalInserted = allResults.reduce((s, r) => s + (r.inserted || 0), 0);
    const totalSkipped = allResults.reduce((s, r) => s + (r.skipped || 0), 0);
    results.style.display = 'block';
    results.innerHTML = `
      <div class="status-text success">
        ✓ Upload complete: ${totalInserted} inserted, ${totalSkipped} skipped across ${xmlFiles.length} file(s)
      </div>`;

    await refreshSummary();
    // Refresh symbol list + tables
    try { await refreshSymbols(); } catch (_) { }
    try { await loadTransactions(); } catch (_) { }
    try { await loadHoldings(); } catch (_) { }
  }
}
