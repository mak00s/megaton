# Megaton Cheat Sheet

- 詳細: `api-reference.md`

## Start

- `mg = start.Megaton(creds, use_ga3?, cache_key?, headless?)`
- `mg.auth(credential?, cache_key?)`
- `mg.enabled`
- `mg.ga_ver`
- `mg.select.ga()`  # UI selector
- `mg.sc` (=`mg.search`)
- `mg.launch_sc(site_url?)`
- `mg.open.sheet(url)`
- `mg.launch_gs(url)`
- `mg.launch_bigquery(project)`

## GA4

- `mg.report.set.dates(date_from, date_to)`
- `mg.report.set.months(ago, window_months, tz?, now?, min_ymd?)`
- `mg.report.run(d, m, filter_d?, filter_m?, sort?, show?, max_retries?, backoff_factor?)`
- `mg.report.run.all(items, d, m, item_key?, property_key?, item_filter?)`
- `mg.report.prep(conf, df?, show?)`
- `mg.report.show()`
- `mg.report.download(filename)`
- `mg.report.to.csv(filename?, quiet?)`
- `mg.report.to.sheet(name)`
- `mg.report.data`
- `mg.ga["4"].property.show("custom_dimensions")`
- `mg.ga["4"].property.show("user_properties")`
- `mg.ga["4"].property.show("custom_metrics")`

### `mg.report.run(..., show=...)`

- `show=True`（default）: 実行後に結果を表示
- `show=False`: 表示を抑制（結果は `ReportResult` / `mg.report.data` で取得）

```python
result = mg.report.run(d=["date"], m=["sessions"], show=False)
df = result.df  # または mg.report.data
```

### `mg.report.prep(conf)` の最小形

```python
conf = {
    "pagePath": {"cut": r"\?.*$", "name": "page"},
    "sessions": {"type": "int64"},
    "campaign": {"replace": (r"\([^)]*\)", "")},
    "debug_col": {"delete": True},
}
mg.report.prep(conf, show=False)  # displayを抑制してDataFrameを返す
```

### d / m の指定ルール

- 文字列は `api_name` または `display_name` の**完全一致**のみ（部分一致・自動補完なし）。
- カスタム項目は `parameter_name` 単体ではなく `api_name` で指定。
- 例: `customEvent:my_param`, `customUser:my_param`

### filter_d / filter_m の書式

フィルタは文字列で指定。書式: `<フィールド名><演算子><値>`

```python
# 単一フィルタ
mg.report.run(d=["date"], m=["sessions"], filter_d="defaultChannelGroup==Organic Search")

# 複数フィルタはセミコロン(;)で区切る（AND条件）
mg.report.run(d=["date"], m=["sessions"], filter_d="country==Japan;deviceCategory==mobile")

# メトリクスのフィルタは filter_m
mg.report.run(d=["date"], m=["sessions"], filter_m="sessions>100")
```

**演算子:**
| 演算子 | 説明 |
|-------|------|
| `==` | 完全一致 |
| `!=` | 不一致 |
| `=@` | 部分一致（contains） |
| `!@` | 部分不一致 |
| `=~` | 正規表現一致 |
| `!~` | 正規表現不一致 |
| `>`, `>=`, `<`, `<=` | 数値比較 |

### 複数メトリクスセット（multi-set モード）

`m` に `[(metrics_list, options_dict), ...]` を渡すと、**セット数だけ API コール**して結果を **d 列で結合**します。
デフォルトは **LEFT JOIN**（1セット目基準）で、`merge="outer"` で外部結合にもできます。

```python
result = mg.report.run(
    d=[("yearMonth", "month"), ("landingPage", "page")],
    m=[
        (["sessions"], {"filter_d": "defaultChannelGroup==Organic Search"}),
        (["totalPurchasers"], {"filter_d": "defaultChannelGroup==Organic Search"}),
    ],
    merge="left",   # default
    show=False,
)
df = result.df
```

注意:
- 通常モード（`m=["sessions", ...]`）と multi-set（`m=[([...], {...}), ...]`）は **混在不可**。
- `options_dict` で指定できるのは `filter_d` / `filter_m` のみ。
- `mg.report.run()` では `("sessions", "sessions", {"filter_d": ...})` のような **メトリクス定義の options は解釈されません**。フィルタを分けたい場合は multi-set を使ってください。

### sort の書式

ソートは文字列で指定。降順は先頭に `-` を付ける。複数はカンマ区切り。

```python
mg.report.run(d=["date"], m=["sessions"], sort="date")        # 昇順
mg.report.run(d=["date"], m=["sessions"], sort="-sessions")   # 降順
mg.report.run(d=["date"], m=["sessions"], sort="date,-sessions")  # 複数
```

### GA4 API retry

`mg.report.run()` は GA4 Data API の `ServiceUnavailable` に対して指数バックオフで再試行します。

```python
# default: max_retries=3, backoff_factor=2.0
mg.report.run(d=["date"], m=["sessions"], max_retries=5, backoff_factor=1.0)
```

### Search の日付テンプレート

`mg.search.set.dates()` は `YYYY-MM-DD` のほか `NdaysAgo` / `yesterday` / `today` を指定可能（`run` 前に ISO 日付へ展開）。

## Sheets (by name)

- `mg.save.to.sheet(name, df?, sort_by?, sort_desc?, start_row?, create_if_missing?, auto_width?, freeze_header?, max_retries?, backoff_factor?)`
- `mg.append.to.sheet(name, df?, create_if_missing?, auto_width?, freeze_header?, max_retries?, backoff_factor?)`
- `mg.upsert.to.sheet(name, df?, keys, columns?, sort_by?, auto_width?, freeze_header?, max_retries?, backoff_factor?)`

### Sheets API retry

Sheets の保存系は指数バックオフで再試行できます（default: `max_retries=3`, `backoff_factor=2.0`）。

```python
mg.save.to.sheet("daily", df, max_retries=5, backoff_factor=1.0)
```

### `start_row` の挙動（save系）

- `start_row=1`（default）: シート全体を上書き
- `start_row>1`: `start_row` より上の既存行は保持し、`start_row` 行目からヘッダ付きで上書き
- `create_if_missing=False`（default）: シート未存在時は作成しない
- `create_if_missing=True`: シート未存在時に自動作成して保存/追記

## CSV

- `mg.save.to.csv(df?, filename?, mode?, include_dates?, quiet?)`
- `mg.append.to.csv(df?, filename?, include_dates?, quiet?)`
- `mg.upsert.to.csv(df?, filename?, keys, columns?, sort_by?, include_dates?, quiet?)`

## Sheets (current)

- `mg.sheets.select(name)`
- `mg.sheets.create(name)`
- `mg.sheets.delete(name)`
- `mg.select.sheet(name)`  # legacy
- `mg.sheet.save(df?, sort_by?, sort_desc?, start_row?, auto_width?, freeze_header?)`
- `mg.sheet.append(df?, auto_width?, freeze_header?)`
- `mg.sheet.upsert(df?, keys, columns?, sort_by?, auto_width?, freeze_header?)`
- `mg.sheet.cell.set(cell, value)`
- `mg.sheet.range.set(a1_range, values)`

## Search Console

- `mg.search.use(site_url)`
- `mg.search.set.dates(date_from, date_to)`
- `mg.search.set.months(ago, window_months, tz?, now?, min_ymd?)`
- `mg.search.run(dimensions, metrics?, limit?, clean?, dimension_filter?)`
- `mg.search.run.all(items, dimensions, metrics?, item_key?, site_url_key?, item_filter?, dimension_filter?)`
- `mg.search.filter_by_thresholds(df, site, clicks_zero_only?)`
- `SearchResult: .decode() -> .clean_url() -> .remove_params() -> .remove_fragment() -> .lower()`
- `SearchResult: .normalize() -> .categorize(into=...) -> .classify() -> .normalize_queries() -> .aggregate()`
- `SearchResult: .apply_if(condition, method_name, *args, **kwargs)`
- `result.filter_impressions(min=100)`
- `result.filter_impressions(sites=cfg.sites, site_key="clinic")`
- `result.filter_ctr(min=0.02)`
- `result.filter_impressions(min=200, keep_clicked=True)`

## Result

- `result.df`
- `result.fill(to?, dimensions?)`
- `result.group(by, metrics?, method?)`
- `result.to_int(metrics?, *, fill_value=0)`
- `result.clean_url(dimension, unquote?, drop_query?, drop_hash?, lower?)`

## Transform

- `ga4.classify_source_channel(df, channel_col?, medium_col?, source_col?, custom_channels?)`
- `ga4.classify_channel(df, channel_col?, medium_col?, source_col?, custom_channels?)`
- `ga4.convert_filter_to_event_scope(filter_d)`
- `text.map_by_regex(series, mapping, default?, flags?, lower?, strip?)`
- `text.clean_url(series, unquote?, drop_query?, drop_hash?, lower?)`
- `text.infer_site_from_url(url_val, sites, site_key?, id_key?)`
- `text.normalize_whitespace(series, mode?)`
- `text.force_text_if_numeric(series, prefix?)`
- `classify.classify_by_regex(df, src_col, mapping, out_col, default?)`
- `table.ensure_columns(df, columns, fill?, drop_extra?)`
- `table.normalize_key_cols(df, cols, to_str?, strip?, lower?, remove_trailing_dot0?)`
- `table.group_sum(df, group_cols, sum_cols)`
- `table.weighted_avg(df, group_cols, value_col, weight_col, out_col?)`
- `table.dedup_by_key(df, key_cols, prefer_by?, prefer_ascending?, keep?)`

## Files

- `mg.load.csv(path)`
- `mg.load.cell(row, col, what?)`
- `mg.save_df(df, filename, mode?, include_dates?, quiet?)`
- `mg.download(df, filename?)`

## BigQuery

- `bq = mg.launch_bigquery(project)`
- `bq.run(sql, to_dataframe=True)`
