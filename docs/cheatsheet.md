# Megaton Cheat Sheet

- 詳細: `api-reference.md`

## Start

- `mg = start.Megaton(creds, cache_key?, headless?)`
- `mg = Megaton.for_property(property_id, creds?)`  # v1.4+ script/CI向け（headless・property選択込み）
- `mg = Megaton.for_site(site_url, creds?)`  # v1.4+
- `mg.properties()` / `mg.sites()` / `mg.use_property(id, refresh_metadata?)`  # v1.4+
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

- `mg.report.set.dates(date_from, date_to)`  # v1.5+ カレンダートークン可: "prev-month-start" 等
- `dates.resolve_date("prev-month-start")` / `dates.resolve_month("prev-month")`  # v1.5+ 統合日付語彙
- `mg.report.set.months(ago, window_months, tz?, now?, min_ymd?)`
- `mg.report.run(d, m, filter?, sort?, show?)`  # filter_d/filter_m は Advanced、retry/timeout は「GA4 API retry / timeout」参照
- `filter_d={"and": [...], "or": [...], "not": ...}` の複合フィルタ可（v1.4+、葉は文字列書式）
- `megaton.wrap(df)` 任意のDataFrameをチェーンAPIへ（v1.4+）
- `result.month_key("date", into="month", fmt="%Y-%m")` 月キー生成（v1.4+）
- `mg.save.to.sheet(name, result)` Result直渡し可（v1.4+、.df不要）
- `mg.report.run.ranges(date_ranges, d, m, filter_d?, filter_m?, ...)`
- `mg.report.run.all(items, d, m, item_key?, property_key?, item_filter?)`
- `mg.report.prep(conf, df?, show?)`
- `mg.report.show()`
- `mg.report.download(filename)`
- `mg.report.to.csv(filename?, quiet?)`
- `mg.report.to.sheet(name)`
- `mg.report.data`
- `mg.show.ga.dimensions` / `mg.show.ga.metrics` / `mg.show.ga.properties`  # 選択中propertyの項目一覧（生アクセスは Advanced 参照）

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

### filter の書式

`filter=` に条件を書くと、各条件を dimension / metric に**自動で振り分け**ます。
書式: `<フィールド名><演算子><値>`、複数はセミコロン(;)区切り（AND）。
`mg.search.run(..., filter=...)` も同じ語彙（GSC は dimension のみ）。

振り分け規則（決定的・明文）: **数値比較（`> >= < <=`）または既知/選択メトリクス → metric、それ以外 → dimension**。

```python
mg.report.run(d=["date"], m=["sessions"], filter="country==Japan;sessions>100")
# country==Japan → dimension, sessions>100 → metric に自動振り分け
```

Advanced: 明示的に分けたい / 未選択フィールドで絞るときは `filter_d=` / `filter_m=`（従来どおり）。
`filter=` と `filter_d=`/`filter_m=` の同時指定は `TypeError`。

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

### GA4 API retry / timeout

`mg.report.run()` は一時エラー（`ServiceUnavailable` / `DeadlineExceeded` / `ResourceExhausted`）に指数バックオフで再試行します。
リトライ枯渇時は**例外を送出**します（v1.4+。旧来の「空を返す」は `on_exhausted='empty'`）。
1試行あたりの期限は `timeout`（default 180秒）。重いクエリ（長期間×containsフィルタ等）はこの引き上げが効きます。

```python
# default: max_retries=5, backoff_factor=2.0, timeout=180, on_exhausted='raise'
mg.report.run(d=["date"], m=["sessions"], max_retries=5, timeout=300)
```

### Search の日付テンプレート

`mg.search.set.dates()` は `YYYY-MM-DD` のほか `NdaysAgo` / `yesterday` / `today` を指定可能（`run` 前に ISO 日付へ展開）。

## Sheets — 便利（one-shot: 名前指定で1発保存）

> 主導線は下の「Sheets — 主導線」（`mg.open.sheet` → `mg.sheets.select` → `mg.sheet.*`）。
> `*.to.sheet(name, ...)` は開く/選ぶを省く one-shot 便利 API。retry 引数は Advanced 参照。

- `mg.save.to.sheet(name, df?, sort_by?, sort_desc?, start_row?, create_if_missing?, auto_width?, freeze_header?)`
- `mg.append.to.sheet(name, df?, create_if_missing?, auto_width?, freeze_header?)`
- `mg.upsert.to.sheet(name, df?, keys, columns?, sort_by?, auto_width?, freeze_header?)`

### Sheets API retry

Sheets の保存系は指数バックオフで再試行できます（default: `max_retries=3`, `backoff_factor=2.0`）。
HTTP 429 quota retry は、backoff が短い場合でも次回試行まで最低 30 秒待ちます。

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

## Sheets — 主導線（stateful: 開く → 選ぶ → 操作）

`mg.open.sheet(url)` → `mg.sheets.select(name)` → `mg.sheet.*` が canonical。

- `mg.sheets.select(name)`
- `mg.sheets.read(name)`
- `mg.sheets.create(name)`
- `mg.sheets.duplicate(source_name, new_name, cell_update=None)`
- `mg.sheets.delete(name)`
- `mg.sheet.save(df?, sort_by?, sort_desc?, start_row?, auto_width?, freeze_header?)`
- `mg.sheet.append(df?, auto_width?, freeze_header?)`
- `mg.sheet.upsert(df?, keys, columns?, sort_by?, auto_width?, freeze_header?)`
- `mg.sheet.cell.set(cell, value)` / `mg.sheet.cell.get(cell)`
- `mg.sheet.range.set(a1_range, values)`
- `mg.sheet.freeze(rows?, cols?)`
- `mg.sheet.resize(rows?, cols?, shrink=False)`
- `mg.sheet.gridlines.hide()` / `mg.sheet.gridlines.show()`
- `mg.sheet.tab.color("#2f80ed")`

## Search Console

- `mg.search.use(site_url)`
- `mg.search.set.dates(date_from, date_to)`
- `mg.search.set.months(ago, window_months, tz?, now?, min_ymd?)`
- `mg.search.run(dimensions, metrics?, limit?, clean?, filter?)`  # dimension_filter は互換alias
- `mg.search.run.all(items, dimensions, metrics?, item_key?, site_url_key?, item_filter?, dimension_filter?)`
- `mg.search.filter_by_thresholds(df, site, clicks_zero_only?)`
- URL-prefix の `site_url` は、400/403/404 時に末尾 `/` あり・なしを自動フォールバック
- `TimeoutError` / `ConnectionError` / `BrokenPipeError` 時は自動リトライ（env: `MEGATON_GSC_MAX_RETRIES`, `MEGATON_GSC_BACKOFF_FACTOR`）
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
- `table.normalize_thresholds_df(df, *, min_default?, max_default?, clinic_col?, min_col?, max_col?)`
- `table.dedup_by_key(df, key_cols, prefer_by?, prefer_ascending?, keep?)`

## Files

- `mg.load.csv(path)`
- `mg.load.cell(row, col, what?)`
- `mg.load.config(sheet_url)`
- `mg.save_df(df, filename, mode?, include_dates?, quiet?)`
- `mg.download(df, filename?)`

## BigQuery

- `bq = mg.launch_bigquery(project)`
- `bq.update()`
- `bq.run(sql, to_dataframe=True)`
- `bq.dataset.select(dataset_id?)`
- `bq.dataset.update(dataset_id?)`
- `bq.table.select(table_id?)`
- `bq.table.update(table_id?)`
- `bq.table.create(table_id, schema, description?, partitioning_field?, clustering_fields?)`

## Advanced / raw access

通常の分析では不要な低レベル・escape hatch。主導線が別にあるものはそちらを優先。

- **生の gspread / retry**: `mg.gs.workbook`（gspread Spreadsheet）、`mg.gs.call_with_retry(op, func, max_retries?, backoff_factor?, retry_on_requests?)`
- **項目カテゴリの生取得**: `mg.ga["4"].property.show("custom_dimensions" | "user_properties" | "custom_metrics")`  # 主導線は `mg.show.ga.*`
- **legacy シート選択**: `mg.select.sheet(name)`  # 主導線は `mg.sheets.select(name)`
- **retry / timeout 引数**: `mg.report.run(...)` / `mg.save.to.sheet(...)` 等は `max_retries` / `backoff_factor` / `timeout` を受けるが、通常は不要。既定は instance/env 設定で調整（「GA4 API retry / timeout」「Sheets API retry」参照）。
