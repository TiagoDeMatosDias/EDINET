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
| Annual reports (28,552 with a P/E, 2016–2026) whose P/E × EPS on today's shares is within 5 % of the stored year-end price | 96.7 % since 2023; median 0.907 at 2017 | 99.01 %; median 1.000 in every year |
| Pairs of consecutive annual reports whose per-share factor disagrees with the stored prices by 1.5x or more | 327 | 71, none of which steps by a split's ratio in earnings |
| Known splits still showing as a step in the stored prices | 187 of 2,257 | 0 of 3,563 |
| Stored prices of unknown basis (neither split-adjusted nor as traded) | 13.5 million rows | 0 |
| Filed figures an issuer tagged a power of ten off (share counts in thousands, P/E 100 times over) | not checked | 11 corrected at 9 companies, filed values kept |
| Split histories counting one split twice | 80 companies | 0 |
| Splits inferred from share counts that the prices contradict (share issues, buybacks, cancelled treasury shares) | 92 | 0 |
| History payload: adjusted value = stored × factor, as filed = the filing's figure (every company with a split) | not checked | 1,418,465 values, 0 wrong |
| Companies with annual reports but no ticker (delisted) whose splits were ignored | 222 of 1,140 | 0 |
| IFRS and US GAAP filers whose EPS, book value, and P/E were the parent company's | 376 companies | 0 |
| Ratios empty for every filing | 4 of 19 | 0 |
| Rolling metrics naming columns that do not exist | 6 | 0 |
| Annual reports missing after a parse error | 66 filings | 0 |
| Pending split candidates | 747 | 0 |

## The as-filed view

Stored tables keep every figure as filed, and the views apply the factors.
Company Analysis → Financial history switches between **Split-adjusted** (the
default, on today's shares) and **As filed** (each year as its report gave
it) with `S`; adjusted lines are marked, and hovering a year shows the other
figure. `GET /api/security/history` returns both (`values` and
`reported_values`) and the company's splits. Toyota's EPS for March 2021
reads ¥160.65 adjusted and ¥803.23 as filed (5-for-1 in September 2021); its
dividend for the split year, ¥120 interim before the split and ¥28 final
after, reads ¥52 adjusted and ¥148 as filed.

## Companies without a ticker

A delisted company keeps its annual reports but loses its ticker in the
company list, and the share basis was keyed by ticker: 1,140 companies with
annual reports had no split adjustment at all, and 222 of them (BrainPad,
TechnoPro, Genky Stores, …) show splits in their share counts. They are now
keyed by their EDINET code, and their splits come from their reports (they
have no stored prices). BrainPad's 3-year EPS growth to June 2022 reads
−5.6 % a year across its 3-for-1 split instead of −34.6 %.

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

## Figures an issuer tagged a power of ten off

A filing's XBRL occasionally carries a figure a power of ten off: Japan
Hospice's 2018 report gives 7,094 issued shares (7,094 thousand), Tosho's 2023
report a P/E of 5,140.7 (51.4), Open Up Group's 2022 report 216.1 (21.6).
The standardization step now checks each annual report against its own other
figures (P/E × EPS against the year-end price; EPS × shares against profit
and book value per share × shares against net assets, each against the
company's usual level) and corrects a figure only where two measures agree:
a share count that both EPS and book value per share put a power of ten
away, a P/E the price puts a power of ten away even before any split factor
(a price a provider left unadjusted for a split does not count), an EPS that
profit and the price both put a power of ten away. Eleven figures at nine
companies were corrected (six share counts at four companies, five P/Es).
Book value per share is not corrected: its only measure is net assets, which
large minority interests or an equity that nearly vanished (Leopalace21's
¥3.25 in 2022) move just as much. Each correction is recorded with the filed
value and the reason in `ShareMetrics_Corrections`, and the Analysis as-filed
view shows the filed figure.

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
- **Delisted companies and other series.** 60 delisted companies whose
  ticker the company list no longer carries (linked through the securities
  code on their filings) were scaled to their annual reports' prices like
  the 25 above; one that paid no dividends since 2016 matches its report as
  stored. Three US stocks were replaced by Yahoo's closes. Exchange-rate,
  inflation, and index series (EUR and the euro's predecessor currencies,
  TOPIX), which cannot split, are marked as traded. No stored price is left
  of unknown basis.
- **Before 2001.** Yahoo's history starts in 2001, so the older closes before
  it are joined to Yahoo's at one day. Over 2001–2003 the ratio of Yahoo's
  closes to the older ones does not drift (median drift 0; a single join
  predicts the first half of 2001 from mid-2001 to 2003 with a median error
  of 0.0 %), so the older import carried no dividend adjustment that early
  and the joined rows are on the split-only basis.
- **Splits after a provider's closes were fetched.** JPX's daily quotes
  fetched before a split stay on the old shares, and Yahoo listed some 2026
  splits without adjusting its history (a 3-for-1 on 19 February showed as a
  67 % fall), sometimes with the step days before the date it lists. The
  read model now adjusts such closes; 61 of 2026's splits were still steps
  in the stored prices.
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

For a recorded split, the last report before it is read the same way: one
filed before the split that already restated for it (a board's decision in
May, effective in September; its EPS was half the profit per year-end share)
and one filed after it that kept the old shares now carry the right factor,
unless another split between the reports compared explains the same move. A
price-heuristic record is dropped when the provider records a split of the
same ratio between the same two year ends and the share count moved once (a
March 2020 crash read as a 2-for-1 split beside the real one in October).

A fall in the share count reads as a consolidation only by a whole number
of shares into one (2, 5, or 10 into 1): a count down by a sixth after a
year end was a cancellation of treasury shares, which had put every earlier
report of one delisted company 1.2 times off.

A filing-date count and the recorded split it shows are one split even when
the record falls a few days outside the filing window or up to a quarter
after filing (a report restated for a split decided before it was filed).
A report filed after a split took effect counts as restated unless its book
value per share is still on the year-end shares and its price as traded
steps by the ratio to the next report's (two reports). A price-heuristic
candidate whose share count did not move, at the year end, at filing, or a
year on, is a price move, not a split.

## Known remaining issues

- **71 report pairs that disagree with the prices.** Every report in them
  holds its filing's XBRL figure on the right scope (or a corrected
  power-of-ten slip). A second check that does not use the P/E, EPS times
  year-end shares over profit, moves with a wrong share factor: 64 of the 71
  hold steady there, 3 have no profit to compare (loss years), and the other
  4 move by amounts no split explains (two cross several missing years, one
  a year of several share changes, one an EPS of ¥0.66 that rounding moves).
  So the per-share figures are consistent and the disagreement is the
  issuer's P/E or price convention: 22 are one report out of line with its
  neighbours and undone the next year; 25 are a first or last report out of
  line, or several share changes in one year; 12 are a split's exact ratio
  where the issuer quoted its P/E on the price as traded while restating
  EPS; 11 cross a year with no annual report; 1 barely traded. Valuations in
  the views compute P/E from the stored price and EPS. The earnings check on
  its own flags 225 pairs at a split's ratio across all companies, nearly
  all where the income statement table's profit is not the owners'
  (minority interests, or a parent-only line beside consolidated EPS), so it
  serves to confirm a price disagreement, not on its own.
- **Book value steps that are not splits.** 367 consecutive-report pairs step
  by 1.5x or more in book value per share against net assets per share (30
  undone the next year), now including the companies without a ticker. 139
  are IFRS and US GAAP filers, whose book value per
  share is now consolidated while the balance sheet table holds the parent
  company's net assets (see the last point); the rest are companies whose
  book value per share and net assets describe different things (large
  minority interests, preferred shares, negative owner equity), as the
  reports state them.
- **No pending split candidates.** Of the 747 pending at the start, the
  rest were confirmed or rejected by the share counts and prices; the last
  90 were one-day rises of 1.40–1.50 times (a stock trading limit-up) that
  the price heuristic read as consolidations of 17 shares into 12, and the
  last 9 (2007–2012: rallies, crashes after the March 2011 earthquake, ¥10/¥20
  ticks) were rejected by review with the reason recorded, none confirmed by
  a report or by Yahoo's split list.
- **Stretches of a repeated close.** 430 stretches where Yahoo and the older
  import both repeat one close were kept as illiquid trading. A full
  re-download from Yahoo would bring back the 159 frozen stretches replaced
  above.
- **Parent-only statements.** IFRS and US GAAP filers (Toyota, Sony, SoftBank
  Group, Makita) have only their parent-only Japanese GAAP statements in the
  income statement, balance sheet, and cash flow tables: their margins and
  ratios built from those describe the parent company, while `ShareMetrics`
  (EPS, book value per share, P/E, ROE) is now consolidated.
