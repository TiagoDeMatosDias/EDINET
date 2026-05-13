/**
 * v2 Component Library — reusable DOM factory functions.
 *
 * Every function returns a live DOM node. Functions accept an options object.
 * All components use the design tokens from common/styles.css.
 *
 * Usage:
 *   import { Panel, DataTable, Button, Badge, ... } from '../common/components.js';
 */

import { el } from './utils.js';

// ---------------------------------------------------------------------------
// Panel
// ---------------------------------------------------------------------------

/**
 * @param {Object} opts
 * @param {string} opts.heading - Panel title text
 * @param {Node|Node[]} [opts.actions] - Elements placed in the header actions slot
 * @param {Node|Node[]} [opts.body] - Panel body content
 * @returns {HTMLElement}
 */
export function Panel({ heading, actions, body }) {
  const head = el('div', { class: 'panel-head' },
    el('div', {},
      el('div', { class: 'panel-title', text: heading }),
    ),
    actions ? el('div', { class: 'panel-actions' }, ...(Array.isArray(actions) ? actions : [actions])) : null,
  );
  const panel = el('section', { class: 'panel' }, head);
  if (body) {
    const bodyEl = el('div', { class: 'panel-body' }, ...(Array.isArray(body) ? body : [body]));
    panel.append(bodyEl);
  }
  return panel;
}

// ---------------------------------------------------------------------------
// Button
// ---------------------------------------------------------------------------

/**
 * @param {Object} opts
 * @param {'primary'|'secondary'|'ghost'|'danger'|'icon'} [opts.variant='secondary']
 * @param {string} opts.label
 * @param {'sm'} [opts.size]
 * @param {string} [opts.title]
 * @param {boolean} [opts.disabled]
 * @param {Function} [opts.onClick]
 * @returns {HTMLButtonElement}
 */
export function Button({ variant = 'secondary', label, size, title, disabled, onClick, id }) {
  const cls = ['btn-' + variant];
  if (size === 'sm') cls.push('btn-sm');
  return el('button', {
    class: cls.join(' '),
    id: id || undefined,
    text: label,
    title: title || label,
    disabled: !!disabled,
    onClick,
  });
}

// ---------------------------------------------------------------------------
// Badge
// ---------------------------------------------------------------------------

/**
 * @param {Object} opts
 * @param {string} opts.text
 * @param {'success'|'warning'|'danger'|'accent'|'muted'} [opts.tone='muted']
 * @returns {HTMLElement}
 */
export function Badge({ text, tone = 'muted' }) {
  return el('span', { class: `badge badge-${tone}`, text });
}

// ---------------------------------------------------------------------------
// MetricTile
// ---------------------------------------------------------------------------

/**
 * @param {Object} opts
 * @param {string} opts.label
 * @param {string|number} opts.value
 * @param {'up'|'down'|'neutral'} [opts.tone='neutral']
 * @param {string} [opts.sub] - subtitle text (e.g., "GREEN", "+12.3%")
 * @returns {HTMLElement}
 */
export function MetricTile({ label, value, tone = 'neutral', sub }) {
  return el('div', { class: `metric-tile tone-${tone}` },
    el('div', { class: 'metric-label', text: label }),
    el('div', { class: 'metric-value', text: String(value) }),
    sub ? el('div', { class: 'metric-sub', text: sub }) : null,
  );
}

/**
 * Container for MetricTile children.
 * @param {Object} opts
 * @param {Node[]} opts.children
 * @returns {HTMLElement}
 */
export function MetricGrid({ children }) {
  return el('div', { class: 'metric-grid' }, ...children);
}

// ---------------------------------------------------------------------------
// DataTable
// ---------------------------------------------------------------------------

/**
 * @param {Object} opts
 * @param {Array<{key:string,label:string,className?:string,sortable?:boolean}>} opts.columns
 * @param {Array} opts.rows
 * @param {Function} opts.rowKey - (row) => stable string identifier
 * @param {Function} opts.renderCell - (row, col) => string|Node
 * @param {string} [opts.emptyText='No data']
 * @param {Function} [opts.onSort] - (key, direction) => void
 * @param {string} [opts.sortKey] - current sorted column key
 * @param {'ascending'|'descending'} [opts.sortDir] - current sort direction
 * @returns {HTMLElement}
 */
export function DataTable({ columns, rows, rowKey, renderCell, emptyText = 'No data', onSort, sortKey, sortDir }) {
  const table = el('table', { class: 'data-table' });

  // Header
  const thead = el('thead');
  const headerRow = el('tr');
  for (const col of columns) {
    const cellClass = ['data-table-th'];
    if (col.className) cellClass.push(col.className);
    if (col.sortable) cellClass.push('sortable');
    if (col.key === sortKey) cellClass.push('sorted');

    const attrs = { class: cellClass.join(' '), text: col.label };
    if (col.sortable) {
      attrs['aria-sort'] = col.key === sortKey ? sortDir : 'none';
      attrs.tabIndex = 0;
      attrs.onClick = () => {
        if (onSort) {
          const newDir = (col.key === sortKey && sortDir === 'ascending') ? 'descending' : 'ascending';
          onSort(col.key, newDir);
        }
      };
    }
    headerRow.append(el('th', attrs));
  }
  thead.append(headerRow);
  table.append(thead);

  // Body
  const tbody = el('tbody');
  if (!rows || !rows.length) {
    tbody.append(el('tr', {},
      el('td', { colspan: columns.length, class: 'col-code', text: emptyText }),
    ));
  } else {
    for (const row of rows) {
      const tr = el('tr', { dataset: { rowKey: rowKey(row) } });
      for (const col of columns) {
        const result = renderCell(row, col);
        const td = el('td', { class: col.className || '' });
        if (result?.nodeType) td.append(result);
        else td.textContent = result ?? '';
        tr.append(td);
      }
      tbody.append(tr);
    }
  }
  table.append(tbody);
  return table;
}

// ---------------------------------------------------------------------------
// FormField
// ---------------------------------------------------------------------------

/**
 * @param {Object} opts
 * @param {string} opts.label
 * @param {Node} opts.input - the <input>, <select>, or <textarea> element
 * @param {boolean} [opts.required]
 * @param {string} [opts.hint]
 * @param {Node} [opts.after] - element placed after the input (e.g., Browse button)
 * @returns {HTMLElement}
 */
export function FormField({ label, input, required, hint, after }) {
  const field = el('div', { class: 'form-field' },
    el('label', { text: label + (required ? ' *' : '') }),
    after
      ? el('div', { class: 'field-with-picker' }, input, after)
      : input,
    hint ? el('div', { class: 'hint', text: hint }) : null,
  );
  return field;
}

/**
 * Container for FormField children.
 * @param {Object} opts
 * @param {Node[]} opts.children
 * @param {boolean} [opts.col2] - use 2-column grid
 * @returns {HTMLElement}
 */
export function FormGrid({ children, col2 }) {
  return el('div', { class: col2 ? 'form-grid-2' : 'form-grid' }, ...children);
}

// ---------------------------------------------------------------------------
// ListItem
// ---------------------------------------------------------------------------

/**
 * @param {Object} opts
 * @param {string} opts.title
 * @param {string} [opts.meta]
 * @param {Node[]} [opts.badges]
 * @param {boolean} [opts.selected]
 * @param {boolean} [opts.highlighted]
 * @param {Function} [opts.onClick]
 * @param {string} [opts.dataId] - stable identifier for patching
 * @returns {HTMLElement}
 */
export function ListItem({ title, meta, badges, selected, highlighted, onClick, dataId }) {
  const cls = ['list-item'];
  if (selected) cls.push('is-selected');
  if (highlighted) cls.push('is-highlight');

  const attrs = { class: cls.join(' ') };
  if (dataId) attrs['data-item-id'] = dataId;
  if (onClick) attrs.onClick = onClick;

  return el('button', attrs,
    el('div', { class: 'list-title', text: title }),
    meta ? el('div', { class: 'list-meta', text: meta }) : null,
    badges?.length ? el('div', { class: 'list-badges' }, ...badges) : null,
  );
}

// ---------------------------------------------------------------------------
// ListSection
// ---------------------------------------------------------------------------

/**
 * @param {Object} opts
 * @param {string} opts.title
 * @param {Node[]} opts.items - ListItem elements
 * @param {string} [opts.emptyText]
 * @param {Node[]} [opts.actions] - header action buttons
 * @returns {HTMLElement}
 */
export function ListSection({ title, items, emptyText, actions }) {
  const section = el('div', { class: 'list-section' });
  section.append(
    el('div', { class: 'list-section-head' },
      el('div', { class: 'list-section-title', text: title }),
      actions ? el('div', { class: 'panel-actions' }, ...actions) : null,
    ),
  );
  const body = el('div', { class: 'list' });
  if (items?.length) {
    body.append(...items);
  } else if (emptyText) {
    body.append(el('div', { class: 'empty-state' },
      el('div', { class: 'empty-state-title', text: emptyText }),
    ));
  }
  section.append(body);
  return section;
}

// ---------------------------------------------------------------------------
// SectionCard
// ---------------------------------------------------------------------------

/**
 * @param {Object} opts
 * @param {string} opts.title
 * @param {Node|Node[]} opts.body
 * @returns {HTMLElement}
 */
export function SectionCard({ title, body }) {
  return el('div', { class: 'section-card' },
    el('div', { class: 'section-card-head' },
      el('div', { class: 'section-card-title', text: title }),
    ),
    el('div', { class: 'section-card-body' }, ...(Array.isArray(body) ? body : [body])),
  );
}

// ---------------------------------------------------------------------------
// ProgressBar
// ---------------------------------------------------------------------------

/**
 * @param {Object} opts
 * @param {number} opts.value
 * @param {number} [opts.max=100]
 * @param {string} [opts.label]
 * @returns {HTMLElement}
 */
export function ProgressBar({ value, max = 100, label }) {
  const pct = Math.max(0, Math.min(100, (value / max) * 100));
  const wrap = el('div', { class: 'progress-bar-wrap' });
  if (label) wrap.append(el('div', { class: 'progress-bar-label', text: label }));
  wrap.append(
    el('div', {
      class: 'progress-bar',
      role: 'progressbar',
      'aria-valuenow': value,
      'aria-valuemin': 0,
      'aria-valuemax': max,
    },
      el('div', { class: 'progress-bar-fill', style: `width:${pct}%` }),
    ),
  );
  return wrap;
}

// ---------------------------------------------------------------------------
// EmptyState
// ---------------------------------------------------------------------------

/**
 * @param {Object} opts
 * @param {string} opts.title
 * @param {string} [opts.subtitle]
 * @param {string} [opts.icon]
 * @returns {HTMLElement}
 */
export function EmptyState({ title, subtitle, icon }) {
  return el('div', { class: 'empty-state' },
    icon ? el('div', { class: 'empty-state-icon', text: icon }) : null,
    el('div', { class: 'empty-state-title', text: title }),
    subtitle ? el('div', { class: 'empty-state-sub', text: subtitle }) : null,
  );
}

// ---------------------------------------------------------------------------
// ErrorBanner / WarningBanner
// ---------------------------------------------------------------------------

/**
 * @param {Object} opts
 * @param {string} opts.message
 * @param {Function} [opts.onRetry]
 * @returns {HTMLElement}
 */
export function ErrorBanner({ message, onRetry }) {
  return el('div', { class: 'error-banner', role: 'alert' },
    el('span', { class: 'banner-msg', text: '⚠ ' + message }),
    onRetry ? el('button', { class: 'banner-retry', text: 'Retry', onClick: onRetry }) : null,
  );
}

/**
 * @param {Object} opts
 * @param {string} opts.message
 * @param {Function} [opts.onDismiss]
 * @returns {HTMLElement}
 */
export function WarningBanner({ message, onDismiss }) {
  return el('div', { class: 'warning-banner', role: 'status' },
    el('span', { class: 'banner-msg', text: '⚡ ' + message }),
    onDismiss ? el('button', { class: 'banner-dismiss', text: '×', onClick: onDismiss }) : null,
  );
}

// ---------------------------------------------------------------------------
// Popup
// ---------------------------------------------------------------------------

/**
 * Creates a popup positioned relative to an anchor element or at fixed coords.
 * The caller must append it to document.body.
 *
 * @param {Object} opts
 * @param {HTMLElement} [opts.anchor] - anchor element for positioning
 * @param {Node[]} opts.children - popup content
 * @param {Function} [opts.onClose] - called when popup is dismissed
 * @param {string} [opts.heading] - optional title bar
 * @param {number} [opts.maxHeight] - optional max-height with overflow
 * @returns {HTMLElement}
 */
export function Popup({ anchor, children, onClose, heading, maxHeight }) {
  const popup = el('div', { class: 'popup' });
  if (heading) popup.append(el('div', { class: 'popup-head', text: heading }));

  const body = el('div', { class: 'popup-body' }, ...children);
  if (maxHeight) body.style.cssText = `max-height:${maxHeight}px;overflow:auto;`;
  popup.append(body);

  if (anchor) {
    const rect = anchor.getBoundingClientRect();
    popup.style.left = Math.min(rect.left, window.innerWidth - 380) + 'px';
    popup.style.top = Math.min(rect.bottom + 6, window.innerHeight - (maxHeight || 400)) + 'px';
  }

  // Close on outside click (deferred so the triggering click doesn't close it)
  if (onClose) {
    requestAnimationFrame(() => {
      const handler = (ev) => {
        if (!popup.contains(ev.target) && ev.target !== anchor) {
          onClose();
        }
      };
      popup._dismissHandler = handler;
      document.addEventListener('mousedown', handler, { once: true });
    });
  }

  return popup;
}

// ---------------------------------------------------------------------------
// FieldWithPicker
// ---------------------------------------------------------------------------

/**
 * Input + Browse button combo for filesystem paths.
 *
 * @param {Object} opts
 * @param {Object} opts.field - field metadata from steps API (field_type, key, display_label)
 * @param {string|Object} opts.value - current value
 * @param {Function} opts.onChange - (newValue) => void
 * @param {Function} opts.onBrowse - async () => pickedValue
 * @returns {HTMLElement}
 */
export function FieldWithPicker({ field, value, onChange, onBrowse }) {
  const wrap = el('div', { class: 'field-with-picker' });
  const isFileUpload = String(field.field_type || '').toLowerCase() === 'file';

  const display = (value && typeof value === 'object')
    ? (value.filename || '')
    : (value === undefined || value === null ? '' : String(value));

  const input = el('input', {
    type: 'text',
    value: display,
    placeholder: field.display_label || field.key,
    onChange: () => {
      if (isFileUpload && value && typeof value === 'object') {
        value.filename = input.value;
        onChange(value);
      } else {
        onChange(input.value);
      }
    },
  });

  const button = Button({ variant: 'ghost', size: 'sm', label: 'Browse', onClick: async () => {
    const picked = await onBrowse();
    if (!picked) return;
    if (isFileUpload && typeof picked === 'object') {
      input.value = picked.filename;
      onChange({ filename: picked.filename, content: picked.content });
    } else {
      input.value = typeof picked === 'string' ? picked : (picked.filename || '');
      onChange(typeof picked === 'string' ? picked : (picked.filename || ''));
    }
  }});

  wrap.append(input, button);
  return wrap;
}

// ---------------------------------------------------------------------------
// Skeleton (loading placeholder)
// ---------------------------------------------------------------------------

/**
 * Creates skeleton placeholder rows for loading states.
 * @param {number} [count=3] - number of skeleton rows
 * @param {number[]} [widths] - width percentages for each row's columns
 * @returns {HTMLElement}
 */
export function Skeleton({ count = 3, widths = [100] }) {
  const wrap = el('div');
  for (let i = 0; i < count; i++) {
    const row = el('div', { style: 'display:flex;gap:var(--space-3);padding:var(--space-2) 0;' });
    widths.forEach(w => {
      row.append(el('div', { class: 'skeleton', style: `width:${w}%` }));
    });
    wrap.append(row);
  }
  return wrap;
}
