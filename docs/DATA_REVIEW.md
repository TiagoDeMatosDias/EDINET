# Share-basis data review

How per-share figures in `Standardized.db` were checked after the share-basis
work of October 2026, what the checks found, and what is still known to be
off. Re-run the checks with `python scripts/review_share_basis.py` (read-only;
`--csv DIR` writes the listed report pairs).

## What "correct" means

Stored tables keep each filing's figures as filed. Every view (screens,
Analysis, Compare, the Markdown report, backtests, the rolling step) puts
per-share figures and share counts on the split-adjusted basis of the stored
prices with the filing's factors (`corporate_actions.filing_basis_factors`,
rules in `share_basis.py`). Correct means: one company's figures read as one
continuous series across its splits, on the same basis as its prices.

## Checks and results (9 October 2026)

| Check | Before | After |
| --- | --- | --- |
| Rolling 5-year EPS growth of Toyota to March 2026 | −14.9 % a year | +17.4 % a year |
| Split histories counting a split twice | 80 companies | 0 |
| Pairs of consecutive annual reports (24,407 with a P/E) whose per-share factor disagrees with the stored prices by 1.5x or more | 327 | 119 |
| Book value per share against net assets per adjusted share, lasting split-sized steps | 322 | 109 |
| Split events still showing as a step in stored prices | 187 of 2,257 | 16 |
| History payload: adjusted value = stored × factor, as filed = stored (every company with a split) | not checked | 982,552 values, 0 wrong |
| Since 2023, report P/E × EPS within 5 % of the stored year-end price | — | 96.7 % |
| Ratios empty for every filing | 4 of 19 | 0 |
| Rolling metrics naming columns that do not exist | 6 | 0 |
| Annual reports missing after a parse error | 66 filings | 0 |
| Pending split candidates | 747 | 135 |

## Known remaining issues

- **Older stored prices include dividends.** Rows with an empty provider and
  `Price_Basis = unknown` (the older import) are adjusted for dividends as well
  as splits up to a snapshot around 2022: against the reports' own P/E × EPS,
  the median stored price is 0.907 of the price as traded at March 2017 and
  1.000 from 2023. Historical prices are understated by the dividends paid
  since, historical P/E and yields in point-in-time screens are off by the
  same, and backtests over those years count dividends twice (in the price
  and as cash), about 1-1.5 % a year for the median company. Fixing it means
  re-downloading split-only history (the pipeline prefers JPX, then Stooq,
  then Yahoo; Stooq adjusts for dividends).
- **Report quirks behind the remaining 119 price disagreements.** About half
  are one report out of line with its neighbours (a P/E quoted on the price
  before a split while EPS is restated, a misreported P/E); 26 cross years
  with no annual report, where several splits and issues cannot be told
  apart; the rest are prices that did not trade for months, or IFRS filers
  whose consolidated per-share figures sit beside parent-only statements.
- **Book value steps that are not splits.** Most of the remaining book-value
  steps are companies whose book value per share and net assets describe
  different things (large minority interests, preferred shares, negative
  owner equity), which the reports state as they are.
- **135 pending split candidates.** Lasting price steps the reports cannot
  confirm, mostly from before 2016 (the first annual report in the data) and
  around the March 2011 earthquake, when one-day crashes of 50 % did not
  recover. Review them in the split review screen.
- **Parent-only statements.** IFRS and US GAAP filers (Toyota, Sony, SoftBank
  Group, Makita) have only their parent-only Japanese GAAP statements in the
  standardized tables, beside consolidated per-share figures: their margins
  and per-share ratios describe the parent company.
