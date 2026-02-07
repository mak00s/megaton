# Megaton Cookbook

Megaton の実用例をまとめています。API の仕様は [api-reference.md](api-reference.md)、設計意図は [design.md](design.md) を参照してください。

## セットアップ

```python
from megaton.start import Megaton

mg = Megaton("/path/to/service_account.json")
```

## GA4: 基本レポート → Sheets 保存

```python
mg.report.set.dates("2024-01-01", "2024-01-31")
mg.report.run(
    d=["date", "eventName"],
    m=["eventCount"],
)

mg.open.sheet("https://docs.google.com/spreadsheets/d/...")
mg.save.to.sheet("_ga_data", mg.report.data)
```

## GA4: 複数プロパティをまとめて取得

```python
p = mg.report.set.months(ago=1, window_months=13)

df = mg.report.run.all(
    sites,
    d=[("yearMonth", "month"), ("defaultChannelGroup", "channel")],
    m=["sessions", "activeUsers"],
    item_key="clinic",
    property_key="ga4_property_id",
)
```

## GA4: 同一ディメンションの複数メトリクスセット

```python
df = mg.report.run(
    d=[("yearMonth", "month"), ("landingPage", "page")],
    m=[
        ("sessions", "sessions", {"filter_d": "sessionDefaultChannelGroup==Organic Search"}),
        ("totalPurchasers", "cv", {"filter_d": "defaultChannelGroup==Organic Search"}),
    ],
)
```

## Search Console: メソッドチェーンで整形

```python
result = (mg.search
    .run(dimensions=["month", "query", "page"], clean=True)
    .categorize("query", by=query_map, into="query_category")
    .categorize("page", by=page_map)
    .filter_impressions(sites=sites, keep_clicked=True))

df = result.df
```

## Search Console: クエリの空白バリエーション統一

```python
result = (mg.search
    .run(dimensions=["month", "query", "page"], clean=True)
    .normalize_queries(mode="remove_all", prefer_by="impressions")
    .categorize("query", by=query_map)
    .categorize("page", by=page_map))
```

## Search Console: 複数サイトの一括取得

```python
result = mg.search.run.all(
    items=sites,
    dimensions=["query", "page"],
    item_key="clinic",
    site_url_key="gsc_site_url",
)

df = (result
    .categorize("query", by=query_map)
    .categorize("page", by=page_map)
    .df)
```

## フィルタリングの使い方

フィルタの仕様は [api-reference.md](api-reference.md) の **Filtering** を参照してください。

### 固定閾値でのフィルタ

```python
result = (mg.search
    .run(dimensions=["month", "query", "page"], clean=True)
    .filter_impressions(min=100)
    .filter_clicks(min=10))
```

### sites + site_key を使ったフィルタ

```python
result = (mg.search
    .run(dimensions=["month", "query", "page"], clean=True)
    .filter_impressions(sites=sites, site_key="clinic"))
```

### keep_clicked を使うケース

```python
result = (mg.search
    .run(dimensions=["month", "query", "page"], clean=True)
    .filter_impressions(sites=sites, site_key="clinic", keep_clicked=True))
```

### CTR フィルタの注意点（impressions == 0）

impressions が 0 の行は CTR が 0 になりやすいため、先に impressions を絞ります。

```python
result = (mg.search
    .run(dimensions=["month", "query", "page"], clean=True)
    .filter_impressions(min=1)
    .filter_ctr(min=0.02))
```

## ReportResult: 集計と列整理

```python
result = mg.report.run(
    d=[("date", "date"), ("sessionSource", "source")],
    m=["sessions", "activeUsers"],
)

summary = (result
    .group(by=["date", "source"], metrics=["sessions", "activeUsers"])
    .to_int(metrics=["sessions", "activeUsers"])
    .df)
```

## ReportResult: 整数型への変換

```python
# 特定の列を整数型に
result.to_int(['sessions', 'users'])

# すべての数値列を整数型に
result.to_int()

# メソッドチェーンで
df = (mg.report.run(d=['date'], m=['sessions', 'users'])
    .to_int()  # 全数値列を整数化
    .df)
```

## Report: `mg.report.prep()` で列を整形

```python
mg.report.run(
    d=[("landingPage", "page"), ("sessionDefaultChannelGroup", "channel")],
    m=["sessions", "activeUsers"],
    show=False,
)

conf = {
    "page": {
        "cut": [r"^https?://[^/]+", r"\?.*$"],  # ドメインとクエリを削除
    },
    "channel": {
        "replace": (r"\s+", " "),  # 連続空白を1つに
        "name": "default_channel",
    },
    "sessions": {
        "type": "int64",
    },
    "activeUsers": {
        "name": "users",
    },
}

mg.report.prep(conf, show=False)
df = mg.report.data
```

## Google Sheets: 追記とアップサート

```python
mg.open.sheet("https://docs.google.com/spreadsheets/d/...")
mg.sheets.select("daily")

mg.sheet.append(df, auto_width=True, freeze_header=True)
mg.sheet.upsert(df, keys=["date", "page"], auto_width=True, freeze_header=True)
```

## Google Sheets: 1行目を残して保存（start_row）

```python
mg.open.sheet("https://docs.google.com/spreadsheets/d/...")

# 1行目（メモや更新日時など）を残し、2行目からヘッダ+データを書き込む
mg.save.to.sheet("daily", df, start_row=2)
```

## CSV: 追記ではなくアップサート

```python
mg.upsert.to.csv(
    df,
    filename="query_master",
    keys=["clinic", "query", "page"],
    columns=["clinic", "query", "page", "clicks", "impressions"],
    include_dates=False,
)
```

## BigQuery: SQL 実行

```python
bq = mg.launch_bigquery("my-gcp-project")

sql = """
SELECT
  DATE(timestamp) AS date,
  COUNT(*) AS events
FROM `project.dataset.table`
GROUP BY 1
"""

df = bq.run(sql, to_dataframe=True)
```

## Transform: source 正規化 + channel 分類

```python
from megaton.transform import ga4

# source と channel を同時に処理
df[['source', 'channel']] = ga4.classify_source_channel(
    df,
    custom_channels={"Group": [r"example\.com", r"sub\.example\.com"]}
)
```

## Config: 閾値によるフィルタリング

```python
cfg = mg.recipes.load_config(sheet_url)

# サイト設定の閾値を適用
df_filtered = mg.search.filter_by_thresholds(
    df,
    site=cfg.sites[0],
    clicks_zero_only=True
)
```

## 複数サイト: 動的フィルタ解決

```python
df = mg.report.run.all(
    sites,
    d=[('landingPage', 'page')],
    m=['sessions'],
    filter_d="site.filter_d",  # 各サイトの filter_d 列を使用
    item_key='clinic',
)
```

## エラーハンドリング

```python
# 空データのチェック
result = mg.search.run.all(sites, dimensions=['query'])

if result.df.empty:
    print('⚠️ データが取得できませんでした')
else:
    processed = result.categorize('query', by=query_map)
    mg.save.to.sheet('_query', processed.df)
```
