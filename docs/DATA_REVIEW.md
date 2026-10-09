# Share-basis data review

How per-share figures and prices in `Standardized.db` were checked after the
share-basis work of October 2026, what the checks found, and what is still
known to be off. Re-run the checks with `python scripts/review_share_basis.py`
(read-only; `--csv DIR` writes the listed report pairs).

## What "correct" means

Stored tables keep each filing's figures as filed. Every view (screens,
Analysis, Compare, the Markdown report, backtests, the rolling step) puts
per-share figures and share counts on the split-adjusted basis of the stored
prices with the filing's factors (`corporate_actions.filing_basis_factors`,
rules in `share_basis.py`). Correct means: one company's figures read as one
continuous series across its splits, on the same basis as its prices, and
the prices are the prices as traded adjusted for splits only (not for
dividends, which backtests credit as cash).

The independent check is each annual report's own P/E times its EPS: the
year-end price on the shares its EPS is on. Put on today's shares with the
filing's factor, it should equal the stored year-end price.

## Checks and results (9 October 2026)

| Check | Before | After |
| --- | --- | --- |
| Rolling 5-year EPS growth of Toyota to March 2026 | −14.9 % a year | +17.4 % a year |
| Annual reports (28,552 with a P/E, 2016–2026) whose P/E × EPS on today's shares is within 5 % of the stored year-end price | 96.7 % since 2023; median 0.907 at 2017 | 98.97 %; median 1.000 in every year |
| Pairs of consecutive annual reports whose per-share factor disagrees with the stored prices by 1.5x or more | 327 | 82, every report in them holding exactly its filing's figures |
| Known splits still showing as a step in the stored prices | 187 of 2,257 | 0 of 3,565 |
| Split histories counting one split twice | 80 companies | 0 |
| Splits inferred from share counts that the prices contradict (share issues, buybacks, cancelled treasury shares) | 92 | 0 |
| History payload: adjusted value = stored × factor, as filed = stored (every company with a split) | not checked | 1,339,839 values, 0 wrong |
| IFRS and US GAAP filers whose EPS, book value, and P/E were the parent company's | 376 companies | 0 |
| Ratios empty for every filing | 4 of 19 | 0 |
| Rolling metrics naming columns that do not exist | 6 | 0 |
| Annual reports missing after a parse error | 66 filings | 0 |
| Pending split candidates | 747 | 9 (none since 2015) |

## Per-share figures of IFRS and US GAAP filers

An IFRS or US GAAP filer reports its consolidated per-share figures under
its own summary concepts (`BasicEarningsLossPerShareIFRSSummaryOfBusinessResults`
and the like); the Japanese GAAP concepts it also files carry the parent
company's figures alone. Standardization read only the Japanese GAAP ones, so
for 376 companies (Toyota, Sony, Canon, Hitachi, …) `ShareMetrics` held
parent-only EPS, book value per share, P/E, ROE, and equity ratio: Toyota's
EPS for March 2026 read ¥260.28 against a consolidated ¥295.25, its book
value ¥1,815.72 against ¥3,062.82, and Sony had no EPS at all. These now come
from the consolidated concepts (about 2,450 filings changed). A filing with
any of these figures on the consolidated scope no longer takes the others
from the parent company either: a consolidated summary that leaves out the
P/E (a year of ¥1m profit and EPS of ¥0.02) had the parent's P/E of 47.6
beside the group's EPS; 562 such P/Es and 14 EPS figures (consolidated
summaries giving book value but no basic EPS) are now empty instead.
Dividends and payout stay the parent company's, as reported.

## The stored prices

The older import's daily closes (empty provider, basis `unknown`) turned out
to be adjusted for dividends as well as splits: against the reports' own
P/E × EPS the median stored price was 0.907 of the price as traded at March
2017, rising to 1.000 from 2023. Historical P/E and yields in point-in-time
screens were off by the dividends paid since, and backtests counted those
dividends twice. They were replaced:

- **Yahoo's split-only closes** for 2,842 companies: 12,810,078 rows replaced
  by 13,327,402 daily closes (`Source_Revision = chart-events-v1`), with
  Yahoo's 2,882 split events recorded. Rows from JPX and earlier Yahoo
  updates were kept.
- **Where Yahoo has nothing** (before 2001, or 25 companies Yahoo does not
  serve), the older rows were scaled to join the next provider close
  (81,942 rows, `import-rescaled-to-yahoo-v1`) or, for the 25 companies, to
  each annual report's P/E × EPS, interpolated between reports
  (71,717 rows, `import-corrected-to-report-prices-v1`).
- **Yahoo's frozen stretches.** Yahoo repeats one close for months or years
  for some companies (Workman at ¥286.25 from March 2010 to October 2018). 159
  such stretches at 102 companies, where the older import kept moving, were
  replaced by the older closes scaled to join Yahoo's at both ends (25,617
  frozen rows, 21,173 moving ones, `import-replacing-frozen-yahoo-v1`); the
  nine report prices inside them agree within 5 %.
- **Splits after a provider's closes were fetched.** JPX's daily quotes
  fetched before a split stay on the old shares, and Yahoo listed some 2026
  splits without adjusting its history (a 3-for-1 on 19 February showed as a
  67 % fall). The read model now adjusts such closes; 61 of 2026's splits
  were still steps in the stored prices.
- **One split recorded twice** (by the price heuristic and by the provider,
  days or weeks apart) adjusted prices twice: one company's history before a
  5-for-1 split stood at 1/25. Such records now count once.

Every replaced row and the split table before the work are in
`data/databases/backup_before_price_rebuild_2026-10-09.db`.

## Splits read from the reports

Share counts move for reasons other than splits. An issue, a rights offering,
a buyback or a cancellation of treasury shares can land on a split ratio (a
biotech that issued 24.98 % more shares read as a 5-for-4 split; a company
that cancelled 239,900 of 1.2 million shares read as a 5-to-4 consolidation).
Only a split moves the price as traded against the split-adjusted price, so:

- an inferred split is dropped when the nearest reports with a price either
  side of it show the two prices still together (46 events), unless the
  stored closes themselves still jump by its ratio;
- a small ratio (under 1.5) left unrecorded where Yahoo priced every day
  needs the prices to confirm it, since Yahoo records each split it adjusts
  for (46 more); none of the 92 dropped events has a Yahoo split within 60
  days of it except one, a recorded 3-for-1 that still applies;
- loss years count as price evidence: their P/E and EPS are both negative
  and their product is still the price.

A filing-date count and the recorded split it shows are one split even when
the record falls a few days outside the filing window or up to a quarter
after filing (a report restated for a split decided before it was filed).
A report filed after a split took effect counts as restated unless its book
value per share is still on the year-end shares and its price as traded
steps by the ratio to the next report's (two reports). A price-heuristic
candidate whose share count did not move, at the year end, at filing, or a
year on, is a price move, not a split.

## Known remaining issues

- **82 report pairs that disagree with the prices, all as filed.** Each of
  the 135 reports in them was compared with its own XBRL: every stored EPS
  and P/E is the filing's figure on the right scope (106 consolidated, 29
  from filers without subsidiaries), so the disagreement is in what the
  issuer reported. 26 are one report out of line with its neighbours and
  undone the next year (a P/E or EPS misreported in the filing); 27 are a
  first or last report out of line, or several share changes in one year;
  15 are a split's exact ratio where the issuer quoted its P/E on the price
  as traded while restating EPS (book value per share confirms the
  restatement, so the per-share figures are right and only that report's
  P/E is on another basis); 12 cross a year with no annual report; 2 are
  stocks that barely traded before the year end.
- **Book value steps that are not splits.** 306 consecutive-report pairs step
  by 1.5x or more in book value per share against net assets per share (28
  undone the next year). 139 are IFRS and US GAAP filers, whose book value per
  share is now consolidated while the balance sheet table holds the parent
  company's net assets (see the last point); the rest are companies whose
  book value per share and net assets describe different things (large
  minority interests, preferred shares, negative owner equity), as the
  reports state them.
- **9 pending split candidates, all from 2007–2012.** Of the 747 pending at
  the start, the rest were confirmed or rejected; the last 90 rejected were
  one-day rises of 1.40–1.50 times (a stock trading limit-up) that the price
  heuristic read as consolidations of 17 shares into 12 and the like, which
  no issuer makes. None since 2015 is left.
- **Older rows outside the rebuild.** Before 2001 the older import is joined
  to Yahoo at one day, so dividends paid within 1999–2000 remain in it (at
  most a few per cent; history before 2015 matters least here). 82 tickers
  outside the company list (the `EUR` exchange-rate series and companies with
  no filings in the data) keep 537,230 rows of unknown basis, which no
  standardized figure reads; four listed companies Yahoo no longer serves
  (taken private in 2026) were scaled to their annual reports' prices like
  the 25 above. 430 stretches of a repeated close where the older import was
  flat too were kept as illiquid trading. A full re-download from Yahoo would
  bring its frozen stretches back.
- **Parent-only statements.** IFRS and US GAAP filers (Toyota, Sony, SoftBank
  Group, Makita) have only their parent-only Japanese GAAP statements in the
  income statement, balance sheet, and cash flow tables: their margins and
  ratios built from those describe the parent company, while `ShareMetrics`
  (EPS, book value per share, P/E, ROE) is now consolidated.
