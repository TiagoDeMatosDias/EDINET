// Pure DOM-creation and data utility functions.
// No imports — this module has zero dependencies.

/** Shorthand for document.querySelector. */
export function $(sel, root = document) {
  return root.querySelector(sel);
}

/** Shorthand for document.querySelectorAll (returns a plain Array). */
export function $all(sel, root = document) {
  return Array.from(root.querySelectorAll(sel));
}

/**
 * Minimal virtual-DOM helper: creates a DOM node from a tag name, an attrs
 * object, and optional child nodes or strings.
 *
 * Special attr keys:
 *   class    → node.className
 *   dataset  → Object.assign(node.dataset, value)
 *   text     → node.textContent
 *   html     → node.innerHTML
 *   on*      → addEventListener(key.slice(2), value)
 */
export function el(tag, attrs = {}, ...children) {
  const node = document.createElement(tag);
  for (const [key, value] of Object.entries(attrs)) {
    if (key === 'class') node.className = value;
    else if (key === 'dataset') Object.assign(node.dataset, value);
    else if (key === 'text') node.textContent = value;
    else if (key === 'html') node.innerHTML = value;
    else if (key.startsWith('on') && typeof value === 'function') node.addEventListener(key.slice(2), value);
    else if (value !== undefined && value !== null) node.setAttribute(key, value);
  }
  for (const child of children.flat()) {
    if (child === null || child === undefined || child === false) continue;
    node.append(child.nodeType ? child : document.createTextNode(String(child)));
  }
  return node;
}

export function deepClone(value) {
  return value === undefined ? undefined : JSON.parse(JSON.stringify(value));
}

export function nowStamp() {
  return new Date().toLocaleTimeString([], { hour12: false });
}

export function formatDate(value) {
  if (!value) return '-';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  return date.toLocaleString([], { month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit' });
}

export function nearBottom(node) {
  return node.scrollHeight - node.scrollTop - node.clientHeight < 24;
}

/** Fetch JSON from `url`, throwing a descriptive Error on non-2xx responses. */
export async function fetchJson(url, options = {}) {
  const response = await fetch(url, {
    headers: { 'Content-Type': 'application/json', ...(options.headers || {}) },
    ...options,
  });
  const contentType = response.headers.get('content-type') || '';
  let payload = null;
  if (contentType.includes('application/json')) {
    payload = await response.json();
  } else {
    payload = await response.text();
  }
  if (!response.ok) {
    const message = typeof payload === 'object' && payload?.detail
      ? payload.detail
      : (typeof payload === 'string' ? payload : response.statusText);
    throw new Error(message || `HTTP ${response.status}`);
  }
  return payload;
}

// --------------------------------------------------------------------------
// Small reusable DOM builders used by multiple screens
// --------------------------------------------------------------------------

/** Create a two-column label / value row used in the inspector grid. */
export function kvLine(label, value) {
  return el('div', { class: 'kv-row' },
    el('label', { text: label }),
    el('div', { class: 'hint', text: value }),
  );
}

/** Create a bordered section card with a header and optional body node. */
export function section(title, subtitle, blurb, body) {
  const sec = el('section', { class: 'section' });
  sec.append(
    el('div', { class: 'section-head' },
      el('div', {},
        el('div', { class: 'section-title', text: title }),
        el('div', { class: 'section-subtitle', text: subtitle }),
      ),
      blurb ? el('div', { class: 'section-subtitle', text: blurb }) : null,
    ),
  );
  if (body) sec.append(body);
  return sec;
}

/** Create a small metrics tile used in the main dashboard. */
export function metric(label, value, tone) {
  return el('div', { class: 'metric' },
    el('div', { class: 'metric-label', text: label }),
    el('div', { class: 'metric-value', text: value }),
    el('div', { class: 'metric-sub', text: tone.toUpperCase() }),
  );
}
