# Shade Research brand assets

This directory is the production source of truth for the Shade Research identity.

- `shade-mark.svg`: canonical mark — a vermilion sun rising behind a two-peak ink ridge, drawn on a 64-unit square.
- `shade-lockup.svg`: canonical horizontal lockup — the mark beside the `SHADE RESEARCH` wordmark in Jost Medium, capitals, widely tracked.
- `shade-icon.png` and `shade-icon.ico`: generated application icons — the mark reversed (paper ridge) on an ink tile. At 16 and 24 px the ridge simplifies to a single peak.

The frontend serves this directory at `/brand-assets`. These paths are stable, so after changing any file here bump the `?v=` cache-busting query in `frontend-v2/index.html` and on `BRAND_MARK_URL` in `frontend-v2/src/components/Brand.tsx`; otherwise browsers keep showing the previous mark. The Windows executable takes its icon from `shade-icon.ico` at build time, so rebuild the release after changing it. Draft boards and exploration files remain in `docs/Feature Development/rebrand` and are not production dependencies.

The production palette is Paper `#F3F0E8` (ground), Ink `#1C1B19` (text and primary actions), Stone `#6B675F` (secondary text), Hairline `#DDD7CA` (rules), Indigo `#2B3A55` (data ink and gains), and Vermilion `#B0301F` (the sun, the active state, and losses). Vermilion is used sparingly. Typography is Shippori Mincho for titles and filing text, Zen Kaku Gothic New for the interface, IBM Plex Mono for figures, and Jost for the wordmark only. The approved tagline is “Value in context.”
