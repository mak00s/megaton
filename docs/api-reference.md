# Megaton API リファレンス

このドキュメントは、Megaton の全 API を網羅的に説明します。クイックリファレンスは [cheatsheet.md](cheatsheet.md) を参照してください。

---

## 目次

- [初期化](#初期化)
- [Search Console API](#search-console-api)
- [GA4 Analytics API](#ga4-analytics-api)
- [Google Sheets API](#google-sheets-api)
- [BigQuery API](#bigquery-api)
- [Config 管理](#config-管理)
- [DateWindow](#datewindow)
- [SearchResult メソッドチェーン](#searchresult-メソッドチェーン)
- [ReportResult メソッドチェーン](#reportresult-メソッドチェーン)
- [Transform モジュール](#transform-モジュール)
- [ユーティリティ](#ユーティリティ)

---

## 初期化

### `Megaton(credential=None, headless=False)`

Megaton インスタンスを作成します。

**パラメータ:**
- `credential` (str | dict | None) - 認証情報
  - `None`: 環境変数 `MEGATON_CREDS_JSON` を使用
  - `str`: JSON 文字列、ファイルパス、またはディレクトリパス
  - `dict`: 認証情報の辞書
- `headless` (bool) - UI なしモード（default: False）
  - `True`: ウィジェット UI を表示せず、コードで明示的に指定
  - `False`: UI で対話的に選択可能

**戻り値:** Megaton インスタンス

**例:**
```python
from megaton.start import Megaton

# 環境変数から認証
mg = Megaton()

# ファイルパスで認証
mg = Megaton('path/to/credentials.json')

# headless モード
mg = Megaton(None, headless=True)
```

---

## Search Console API

### `mg.search.sites`

**戻り値:** list[str] - アクセス可能なサイト URL のリスト

初回アクセス時に自動的に取得され、キャッシュされます。

**例:**
```python
sites = mg.search.sites
print(sites[0])  # 'https://example.com/'
```

### `mg.search.get.sites()`

サイト一覧を強制的に再取得します。

**戻り値:** list[str]

**例:**
```python
sites = mg.search.get.sites()
```

### `mg.search.use(site_url)`

対象サイトを選択します。

**パラメータ:**
- `site_url` (str) - サイト URL（例: `'https://example.com/'`）

**戻り値:** None

**例:**
```python
mg.search.use('https://example.com/')
```

### `mg.search.set.dates(date_from, date_to)`

レポート期間を日付で設定します。

**パラメータ:**
- `date_from` (str) - 開始日（YYYY-MM-DD）
- `date_to` (str) - 終了日（YYYY-MM-DD）

**戻り値:** None

**例:**
```python
mg.search.set.dates('2025-01-01', '2025-01-31')
```

### `mg.search.set.months(ago=0, window_months=1, tz='Asia/Tokyo', now=None, min_ymd=None)`

月単位でレポート期間を設定します。

**パラメータ:**
- `ago` (int) - 何ヶ月前から開始するか（default: 0）
- `window_months` (int) - 何ヶ月分取得するか（default: 1）
- `tz` (str) - タイムゾーン（default: 'Asia/Tokyo'）
- `now` (datetime | None) - 基準日時（default: None = 現在時刻）
- `min_ymd` (str | None) - 開始日の最小制約（YYYYMMDD形式）

**戻り値:** DateWindow - 期間情報を含む namedtuple

**例:**
```python
# 先月のデータ
p = mg.search.set.months(ago=1)

# 3ヶ月前から13ヶ月分
p = mg.search.set.months(ago=3, window_months=13)

# 戻り値の使用
print(p.start_iso)  # '2024-12-01'
print(p.start_ym)   # '202412'
print(p.start_ymd)  # '20241201'
```

### `mg.search.run(dimensions, metrics=None, limit=5000, clean=False, dimension_filter=None, **kwargs)`

Search Console のクエリを実行します。

**パラメータ:**
- `dimensions` (list[str]) - ディメンション（例: `['query', 'page']`）
  - 選択肢: `'date'`, `'hour'`, `'country'`, `'device'`, `'page'`, `'query'`, `'month'`
  - `'month'` を指定すると内部的に `'date'` で取得して月単位に集計
- `metrics` (list[str] | None) - 指標（default: `['clicks', 'impressions', 'ctr', 'position']`）
- `limit` (int) - 取得行数上限（default: 5000）
- `clean` (bool) - URL 正規化と集計を実行（default: False）
  - `True`: `page` 列に対してデコード + パラメータ/フラグメント除去 + 小文字化を行い、必要に応じて集計
- `dimension_filter` (str | list | tuple | None) - ディメンションフィルタ（AND 条件のみ）
  - 形式: `"dimension=~pattern;dimension2=@text"`
  - 演算子: `=~` (RE2 正規表現)、`!~` (正規表現否定)、`=@` (部分一致)、`!@` (部分一致否定)

**戻り値:** SearchResult - メソッドチェーン可能なラッパー（`.df` で DataFrame にアクセス）

**例:**
```python
# 基本的な使い方
result = mg.search.run(
    dimensions=['query', 'page'],
    metrics=['clicks', 'impressions', 'position']
)
df = result.df

# URL 正規化を自動実行
result = mg.search.run(dimensions=['page'], clean=True)

# ディメンションフィルタ
result = mg.search.run(
    dimensions=['query', 'page'],
    dimension_filter="page=~^/blog/;query=@ortho"
)

# メソッドチェーン
result = (mg.search
    .run(dimensions=['query', 'page'])
    .decode()
    .classify(query=cfg.query_map, page=cfg.page_map)
    .filter_clicks(min=1)
    .filter_impressions(min=100, keep_clicked=True))
```

### `mg.search.run.all(items, dimensions, metrics=None, item_key='site', site_url_key='gsc_site_url', item_filter=None, dimension_filter=None, verbose=True, **kwargs)`

複数サイトのデータを一括取得して結合します。

**パラメータ:**
- `items` (list[dict]) - アイテム設定のリスト
- `dimensions` (list[str]) - GSC ディメンション
- `metrics` (list[str] | None) - GSC 指標
- `item_key` (str) - 結果に含める識別子のキー名（default: 'site'）
  - **自動的に dimensions に追加されます**
- `site_url_key` (str) - アイテム設定内の GSC サイト URL キー（default: 'gsc_site_url'）
  - 空の場合はスキップされます
- `item_filter` (list | callable | None) - アイテムフィルタ
  - `list`: `item[item_key]` がリスト内にあるものを含める
  - `callable`: `item_filter(item)` が True を返すものを含める
  - `None`: すべて含める
- `dimension_filter` (str | list | tuple | None) - ディメンションフィルタ
- `verbose` (bool) - 進捗メッセージを表示（default: True）
- `**kwargs` - `mg.search.run()` に渡す追加引数（例: `limit`, `country`, `clean`）

**戻り値:** SearchResult - 結合されたデータと item_key 列

**例:**
```python
# 基本的な使い方
sites = [
    {'site': 'siteA', 'gsc_site_url': 'https://a.com/'},
    {'site': 'siteB', 'gsc_site_url': 'https://b.com/'},
]

result = mg.search.run.all(
    sites,
    dimensions=['query', 'page'],
    metrics=['clicks', 'impressions'],
    item_filter=['siteA'],
)

# 結果の使用
df = result.df
assert 'site' in df.columns  # item_key 列が自動追加

# カスタム識別子
clinics = [
    {'clinic': 'A', 'gsc_site_url': 'https://a.com/'},
    {'clinic': 'B', 'gsc_site_url': 'https://b.com/'},
]

result = mg.search.run.all(
    clinics,
    dimensions=['query'],
    item_key='clinic',
    item_filter=lambda x: x.get('active', True),
)
```

### `mg.search.filter_by_thresholds(df, site, clicks_zero_only=False)`

サイト設定の閾値を適用してフィルタリングします。

**パラメータ:**
- `df` (pd.DataFrame) - Search Console データ
- `site` (dict) - サイト設定辞書
  - サポートされるキー: `min_impressions`, `max_position`, `min_pv`, `min_cv`
- `clicks_zero_only` (bool) - clicks >= 1 の行を無条件に保持（default: False）

**戻り値:** pd.DataFrame

**例:**
```python
site_config = {
    'site': 'example',
    'min_impressions': 100,
    'max_position': 20,
}

filtered_df = mg.search.filter_by_thresholds(df, site_config)

# clicks > 0 の行は閾値を無視
filtered_df = mg.search.filter_by_thresholds(df, site_config, clicks_zero_only=True)
```

### `mg.search.data`

**戻り値:** pd.DataFrame | None - 直近の Search Console クエリ結果

---

## GA4 Analytics API

### `mg.report.set.dates(date_from, date_to)`

レポート期間を日付で設定します。

**パラメータ:**
- `date_from` (str) - 開始日（YYYY-MM-DD）
- `date_to` (str) - 終了日（YYYY-MM-DD）

**戻り値:** None

**例:**
```python
mg.report.set.dates('2025-01-01', '2025-01-31')
```

### `mg.report.set.months(ago=0, window_months=1, tz='Asia/Tokyo', now=None, min_ymd=None)`

月単位でレポート期間を設定します。

**パラメータ:** `mg.search.set.months()` と同じ

**戻り値:** DateWindow

**例:**
```python
p = mg.report.set.months(ago=1, window_months=1)
```

### `mg.report.run(d, m, filter_d=None, filter_m=None, sort=None, **kwargs)`

GA4 レポートを実行します。

**パラメータ:**
- `d` (list) - ディメンション（省略形）
  - 文字列または `(api_name, alias)` のタプルのリスト
- `m` (list) - 指標（省略形）
  - 文字列または `(api_name, alias)` のタプルのリスト
- `filter_d` (dict | None) - ディメンションフィルタ
- `filter_m` (dict | None) - 指標フィルタ
- `sort` (list | None) - ソート順

**戻り値:** None - 結果は `mg.report.data` に格納

**例:**
```python
# 省略形
mg.report.run(
    d=[('yearMonth', 'month'), ('defaultChannelGroup', 'channel')],
    m=[('activeUsers', 'users'), 'sessions']
)

# 結果の取得
df = mg.report.data
```

### `mg.report.run.all(items, d=None, m=None, dimensions=None, metrics=None, item_key='site', property_key='ga4_property_id', item_filter=None, verbose=True, **kwargs)`

複数プロパティのレポートを一括実行して結合します。

**パラメータ:**
- `items` (list[dict]) - アイテム設定のリスト
- `d` (list | None) - ディメンション（省略形）
  - 文字列または `(api_name, alias)` または `(api_name, alias, options)` のタプル
  - `site.<key>` を指定すると `item[<key>]` をディメンションとして使用します
  - `options={'absolute': True}` を指定すると、`item['url']` のドメインで相対パスを絶対URLに変換します
- `m` (list | None) - 指標（省略形）
  - `site.<key>` を指定すると `item[<key>]` をメトリクスとして使用します
  - `(api_name, alias, options)` の `options` に `{'filter_d': ...}` を指定できます（メトリクス別フィルタ）
- `dimensions` (list | None) - ディメンション（明示形）
- `metrics` (list | None) - 指標（明示形）
- `item_key` (str) - 識別子のキー名（default: 'site'）
- `property_key` (str) - GA4 プロパティ ID のキー名（default: 'ga4_property_id'）
- `item_filter` (list | callable | None) - アイテムフィルタ
- `verbose` (bool) - 進捗メッセージを表示（default: True）
- `**kwargs` - `mg.report.run()` に渡す追加引数
  - `filter_d="site.filter_d"` を指定すると、各 `item['filter_d']` を使用します

**戻り値:** pd.DataFrame - 結合されたデータと item_key 列

**例:**
```python
sites = [
    {'site': 'siteA', 'ga4_property_id': '123456'},
    {'site': 'siteB', 'ga4_property_id': '789012'},
]

df = mg.report.run.all(
    sites,
    d=['date', 'deviceCategory'],
    m=['activeUsers', 'sessions'],
    item_filter=['siteA'],
)

assert 'site' in df.columns
```

```python
# サイト別メトリクス（site.<key>）
df = mg.report.run.all(
    sites,
    d=[('yearMonth', 'month')],
    m=[('site.cv', 'cv')],
)
```

```python
# 相対URLを絶対URLに変換（item['url'] のドメインを使用）
df = mg.report.run.all(
    sites,
    d=[('landingPage', 'lp', {'absolute': True})],
    m=['activeUsers'],
)
```

### `mg.report.prep(conf, df=None)`

DataFrame の前処理（列名変更、値置換など）を行います。

**パラメータ:**
- `conf` (dict) - 列ごとの処理設定
  - 各列に対して `cut`, `delete`, `name`, `replace`, `type` を指定
- `df` (pd.DataFrame | None) - 対象 DataFrame（default: `mg.report.data`）

**戻り値:** 表示用オブジェクト（`mg.report.data` が更新されます）

**例:**
```python
conf = {
    'deviceCategory': {'replace': ('desktop', 'PC')},
    'activeUsers': {'name': 'users', 'type': int},
}

mg.report.prep(conf)
```

### `mg.report.data`

**戻り値:** pd.DataFrame | None - 直近のレポート結果

### `mg.report.dates.to.sheet(sheet, start_cell, end_cell)`

レポート期間をシートに書き込みます。

**パラメータ:**
- `sheet` (str) - シート名
- `start_cell` (str) - 開始日を書き込むセル（A1 表記）
- `end_cell` (str) - 終了日を書き込むセル（A1 表記）

**戻り値:** None

**例:**
```python
mg.report.dates.to.sheet('Dashboard', 'B2', 'B3')
```

---

## Google Sheets API

### `mg.open.sheet(url)`

スプレッドシートを開きます。

**パラメータ:**
- `url` (str) - スプレッドシート URL

**タイムアウト:**
- 既定は 180 秒（接続待ちのみ）
- 環境変数 `MEGATON_GS_TIMEOUT` で上書き可能
  - 0 以下で無効化
- タイムアウト時はメッセージを出して終了します

**戻り値:** None

**例:**
```python
mg.open.sheet('https://docs.google.com/spreadsheets/d/...')
```

### `mg.sheets.select(sheet_name)`

シートを選択します。

**パラメータ:**
- `sheet_name` (str) - シート名

**戻り値:** None

**例:**
```python
mg.sheets.select('Sheet1')
```

### `mg.sheets.create(sheet_name)`

新しいシートを作成します。

**パラメータ:**
- `sheet_name` (str) - 作成するシート名

**戻り値:** None

**例:**
```python
mg.sheets.create('NewSheet')
```

### `mg.sheets.delete(sheet_name)`

シートを削除します。

**パラメータ:**
- `sheet_name` (str) - 削除するシート名

**戻り値:** None

**例:**
```python
mg.sheets.delete('OldSheet')
```

### `mg.save.to.sheet(sheet_name, df=None)`

DataFrame をシートに上書き保存します。

**パラメータ:**
- `sheet_name` (str) - シート名
- `df` (pd.DataFrame | None) - 保存する DataFrame（default: `mg.report.data`）

**戻り値:** None

**例:**
```python
mg.save.to.sheet('Results', df)
```

### `mg.append.to.sheet(sheet_name, df=None)`

DataFrame を既存データの末尾に追記します。

**パラメータ:**
- `sheet_name` (str) - シート名
- `df` (pd.DataFrame | None) - 追記する DataFrame（default: `mg.report.data`）

**戻り値:** None

**例:**
```python
mg.append.to.sheet('Log', df)
```

### `mg.upsert.to.sheet(sheet_name, df=None, keys=None, columns=None, sort_by=None)`

キー列を基準にアップサート（更新または挿入）します。

**パラメータ:**
- `sheet_name` (str) - シート名
- `df` (pd.DataFrame | None) - アップサートする DataFrame（default: `mg.report.data`）
- `keys` (list[str] | None) - キー列のリスト
- `columns` (list[str] | None) - 出力する列のリスト（default: すべて）
- `sort_by` (list[str] | None) - ソート列のリスト

**戻り値:** None

**例:**
```python
mg.upsert.to.sheet(
    'Master',
    df,
    keys=['site', 'month'],
    sort_by=['site', 'month']
)
```

### 現在のシートへの操作

選択されたシートに対する操作：

#### `mg.sheet.clear()`

現在のシートをクリアします。

#### `mg.sheet.data`

**戻り値:** list[dict] - 現在のシートのデータ

#### `mg.sheet.cell.set(cell, value)`

単一セルに値を書き込みます。

**パラメータ:**
- `cell` (str) - セル（A1 表記）
- `value` (str | int | float) - 値

**例:**
```python
mg.sheet.cell.set('A1', 'Title')
```

#### `mg.sheet.range.set(a1_range, values)`

範囲に配列を書き込みます。

**パラメータ:**
- `a1_range` (str) - 範囲（A1 表記、例: 'A1:B2'）
- `values` (list[list]) - 2次元配列

**例:**
```python
mg.sheet.range.set('A1:B2', [['a', 'b'], ['c', 'd']])
```

#### `mg.sheet.save(df=None)`

現在のシートに DataFrame を保存します。

**パラメータ:**
- `df` (pd.DataFrame | None) - 保存する DataFrame（default: `mg.report.data`）

#### `mg.sheet.append(df=None)`

現在のシートに追記します。

**パラメータ:**
- `df` (pd.DataFrame | None) - 追記する DataFrame（default: `mg.report.data`）

#### `mg.sheet.upsert(df=None, keys=None, columns=None, sort_by=None)`

現在のシートにアップサートします。

**パラメータ:** `mg.upsert.to.sheet()` と同じ

---

## BigQuery API

### `mg.launch_bigquery(project_id)`

BigQuery サービスを起動します。

**パラメータ:**
- `project_id` (str) - GCP プロジェクト ID

**戻り値:** BigQuery クライアント

**例:**
```python
bq = mg.launch_bigquery('my-project-id')
```

### `bq.run(sql, to_dataframe=True)`

SQL クエリを実行します。

**パラメータ:**
- `sql` (str) - SQL クエリ
- `to_dataframe` (bool) - DataFrame として返す（default: True）

**戻り値:** pd.DataFrame | QueryJob

**例:**
```python
df = bq.run("""
    SELECT *
    FROM `project.dataset.table`
    WHERE date >= '2025-01-01'
    LIMIT 1000
""")
```

---

## Config 管理

### `mg.recipes.load_config(sheet_url)`

設定ファイルを読み込みます。

**パラメータ:**
- `sheet_url` (str) - Google Sheets の URL

**戻り値:** Config - 設定オブジェクト
- `config.sites` - サイト設定のリスト
- `config.query_map` - クエリ分類マップ
- `config.page_map` - ページ分類マップ
- `config.source_map` - ソース正規化マップ
- `config.group_domains` - チャネル判定用ドメインのセット

**例:**
```python
cfg = mg.recipes.load_config('https://docs.google.com/spreadsheets/d/...')

# サイト設定の使用
for site in cfg.sites:
    print(site['clinic'], site.get('min_impressions', 0))

# 分類マップの使用
result = mg.search.run(dimensions=['query', 'page']).classify(
    query=cfg.query_map,
    page=cfg.page_map
)
```

---

## DateWindow

### DateWindow namedtuple

`mg.search.set.months()` と `mg.report.set.months()` が返す期間情報。

**フィールド:**
- `start_iso` (str) - 開始日（YYYY-MM-DD）
- `end_iso` (str) - 終了日（YYYY-MM-DD）
- `start_ym` (str) - 開始年月（YYYYMM）
- `end_ym` (str) - 終了年月（YYYYMM）
- `start_ymd` (str) - 開始日（YYYYMMDD、BigQuery 用）
- `end_ymd` (str) - 終了日（YYYYMMDD、BigQuery 用）

**例:**
```python
p = mg.search.set.months(ago=1, window_months=1)

# ISO 8601 形式
print(f"期間: {p.start_iso} ~ {p.end_iso}")

# BigQuery WHERE 句
sql = f"""
    SELECT *
    FROM `project.dataset.table`
    WHERE date BETWEEN '{p.start_ymd}' AND '{p.end_ymd}'
"""

# 月ラベル
df['month'] = p.start_ym
```

---

## SearchResult メソッドチェーン

`mg.search.run()` が返す SearchResult オブジェクトは、メソッドチェーンで段階的な処理が可能です。

詳細は [searchresult-api.md](searchresult-api.md) を参照してください。

**主要メソッド:**
- `.df` - DataFrame にアクセス（プロパティ）
- `.decode(group=True)` - URL デコード
- `.remove_params(keep=None, group=True)` - クエリパラメータ削除
- `.remove_fragment(group=True)` - フラグメント削除
- `.lower(columns=None, group=True)` - 小文字化
- `.classify(query=None, page=None, group=True)` - 分類
- `.filter_clicks(min=None, max=None, sites=None, site_key='site')` - クリック数フィルタ
- `.filter_impressions(min=None, max=None, sites=None, site_key='site', keep_clicked=False)` - インプレッション数フィルタ
- `.filter_ctr(min=None, max=None, sites=None, site_key='site', keep_clicked=False)` - CTR フィルタ
- `.filter_position(min=None, max=None, sites=None, site_key='site', keep_clicked=False)` - ポジションフィルタ
- `.aggregate(by=None)` - 手動集約

**簡単な例:**
```python
result = (mg.search
    .run(dimensions=['query', 'page'])
    .decode()
    .remove_params()
    .classify(query=cfg.query_map, page=cfg.page_map)
    .filter_clicks(min=1)
    .filter_impressions(min=100, keep_clicked=True))

df = result.df
```

---

## ReportResult メソッドチェーン

`mg.report.run.all()` が返す ReportResult オブジェクトは、メソッドチェーンで段階的な処理が可能です。

### 基本的な使い方

```python
# run.all() の結果は ReportResult オブジェクト
result = mg.report.run.all(
    sites,
    d=[('yearMonth', 'month'), ('sessionSource', 'source')],
    m=[('activeUsers', 'users')],
    item_key='clinic',
)

# DataFrame として取得
df = result.df

# メソッドチェーンで処理
processed = (result
    .fill(to='(not set)')  # 欠損値を埋める
    .classify(
        dimension='source',
        by={'.*google.*': 'Search', '.*yahoo.*': 'Search'},
        output='source_category'
    )
    .group(by=['month', 'source_category'], metrics=['users'])
    .to_int(metrics='users')
    .sort(by='users', ascending=False)
)

df_final = processed.df
```

### 主要メソッド

#### `.df`

DataFrame にアクセスするプロパティ。

```python
df = result.df
```

#### `.classify(dimension, by, output=None, default=None, group=True)`

ディメンション列の値を正規表現パターンで分類します。

**パラメータ:**
- `dimension` (str) - 分類対象のディメンション列名
- `by` (dict) - 分類マッピング辞書 `{pattern: category}`（パターンは正規表現）
- `output` (str | None) - 出力列名（default: `dimension + '_category'`）
- `default` (str | None) - マッチしない場合のデフォルト値（default: `'(other)'`）
- `group` (bool) - True の場合、分類列を含めて集計（default: True）

**戻り値:** ReportResult

**例:**
```python
# デフォルト: 分類後に集計（元のdimensionは除外）
result.classify(
    dimension='sessionSource',
    by={'.*google.*': 'Search', '.*yahoo.*': 'Search', '.*facebook.*': 'Social'},
    output='source_type',  # 省略時は 'sessionSource_category'
    default='Other'  # 省略時は '(other)'
)

# 集計せず生データを保持
result.classify(
    dimension='sessionSource',
    by={'.*google.*': 'Search'},
    group=False  # 元のdimensionも保持
)
```

#### `.group(by, metrics=None, method='sum')`

指定した列で集計します。

**パラメータ:**
- `by` (str | list[str]) - グループ化キーとなる列名
- `metrics` (str | list[str] | None) - 集計する指標列（default: 数値列を自動検出）
- `method` (str) - 集計方法（'sum', 'mean', 'min', 'max' など、default: 'sum'）

**戻り値:** ReportResult

**例:**
```python
# 自動的に数値列を集計
result.group(by=['month', 'source'])

# 特定の指標を指定
result.group(by='month', metrics=['users', 'sessions'])

# 平均を計算
result.group(by='date', metrics='position', method='mean')
```

#### `.sort(by, ascending=True)`

指定した列でソートします。

**パラメータ:**
- `by` (str | list[str]) - ソートキーとなる列名
- `ascending` (bool | list[bool]) - 昇順（True）または降順（False）

**戻り値:** ReportResult

**例:**
```python
# sessions で降順ソート
result.sort(by='sessions', ascending=False)

# 複数列でソート
result.sort(by=['date', 'sessions'], ascending=[True, False])
```

#### `.fill(to='(not set)', dimensions=None)`

ディメンション列の欠損値を指定した値で埋めます。

**パラメータ:**
- `to` (str) - 埋める値（default: `'(not set)'`）
- `dimensions` (list[str] | None) - 対象のディメンション列名のリスト（default: すべてのディメンション）

**戻り値:** ReportResult

**例:**
```python
# すべてのディメンションの欠損値を '(not set)' で埋める
result.fill()

# 特定のディメンションのみ埋める
result.fill(to='Unknown', dimensions=['sessionSource'])
```

#### `.to_int(metrics, fill_value=0)`

指標列を整数型に変換します（欠損値は fill_value で埋められます）。

**パラメータ:**
- `metrics` (str | list[str]) - 変換する指標列名
- `fill_value` (int) - 欠損値を埋める値（default: 0）

**戻り値:** ReportResult

**例:**
```python
# sessions を整数型に変換
result.to_int(metrics='sessions')

# 複数の指標を変換
result.to_int(metrics=['sessions', 'users'])
```

#### `.replace(dimension, by, *, regex=True)`

ディメンション列の値を辞書マッピングで置換します。

**パラメータ:**
- `dimension` (str) - 置換対象のディメンション列名
- `by` (dict) - 置換マッピング辞書 `{old_value: new_value}`
- `regex` (bool) - True の場合、キーを正規表現として扱う（default: True）

**戻り値:** ReportResult

**例:**
```python
# 正規表現での置換（default）
result.replace(
    dimension='campaign',
    by={r'\([^)]*\)': ''}  # 括弧内を削除
)

# 固定文字列での置換
result.replace(
    dimension='sessionSource',
    by={'google': 'Google', 'yahoo': 'Yahoo!'},
    regex=False
)
```

### その他のプロパティ

- `.empty` - DataFrame が空かどうか（bool）
- `.columns` - DataFrame の列名リスト（list[str]）
- `len(result)` - データフレームの行数（int）

---

## ユーティリティ

### `mg.show.ga.dimensions`

GA4 のディメンション一覧を表示します。

**戻り値:** None（UI で表示）

### `mg.show.ga.metrics`

GA4 の指標一覧を表示します。

**戻り値:** None（UI で表示）

### `mg.show.ga.properties`

GA4 プロパティ一覧を表示します。

**戻り値:** None（UI で表示）

### `mg.show.table(df, rows=10, include_index=False)`

DataFrame を表形式で表示します。

**パラメータ:**
- `df` (pd.DataFrame) - 表示する DataFrame
- `rows` (int) - 表示行数（default: 10）
- `include_index` (bool) - インデックスを含める（default: False）

**戻り値:** None（UI で表示）

**例:**
```python
mg.show.table(df, rows=20)
```

### `mg.load.csv(path)`

CSV ファイルを読み込みます。

**パラメータ:**
- `path` (str) - CSV ファイルのパス

**戻り値:** pd.DataFrame

**例:**
```python
df = mg.load.csv('data.csv')
```

### `mg.save_df(df, filename, mode='w', include_dates=True)`

DataFrame をローカルファイルに保存します。

**パラメータ:**
- `df` (pd.DataFrame) - 保存する DataFrame
- `filename` (str) - ファイル名（.csv または .xlsx）
- `mode` (str) - 書き込みモード（default: 'w'）
- `include_dates` (bool) - 日付列を含める（default: True）

**戻り値:** None

**例:**
```python
mg.save_df(df, 'output.csv')
mg.save_df(df, 'output.xlsx')
```

### `mg.download(df, filename=None)`

Notebook からファイルをダウンロードします。

**パラメータ:**
- `df` (pd.DataFrame) - ダウンロードする DataFrame
- `filename` (str | None) - ファイル名（default: 自動生成）

**戻り値:** None

**例:**
```python
mg.download(df, 'results.csv')
```

---

## Transform モジュール

### GA4 変換関数

#### `ga4.convert_filter_to_event_scope(filter_d)`

session系フィルタディメンションをevent系に変換します。

GA4 APIでは、session系ディメンション（`sessionDefaultChannelGroup`など）とevent系ディメンション（`defaultChannelGroup`など）でフィルタの互換性がありません。この関数は、session系ディメンションを使った`filter_d`をevent系クエリで使用できるように変換します。

**パラメータ:**
- `filter_d` (str | None) - フィルタ文字列（例: `"sessionDefaultChannelGroup==Organic Social"`）
  - None または空文字列の場合はそのまま返します

**戻り値:** str | None - event系に変換されたフィルタ文字列（None/空文字の場合はそのまま）

**変換マッピング:**
- `sessionDefaultChannelGroup` → `defaultChannelGroup`
- `sessionSourceMedium` → `sourceMedium`
- `sessionMedium` → `medium`
- `sessionSource` → `source`
- `sessionCampaignId` → `campaignId`
- `sessionCampaignName` → `campaignName`
- `sessionManualTerm` → `manualTerm`
- `sessionManualSource` → `manualSource`
- `sessionManualMedium` → `manualMedium`
- `sessionManualSourceMedium` → `manualSourceMedium`
- `sessionManualCampaignId` → `manualCampaignId`
- `sessionManualCampaignName` → `manualCampaignName`
- `sessionManualAdContent` → `manualAdContent`

**例:**
```python
from megaton.transform import ga4

# session系フィルタをevent系に変換
filter_session = "sessionDefaultChannelGroup==Organic Social;sessionMedium==social"
filter_event = ga4.convert_filter_to_event_scope(filter_session)
# => "defaultChannelGroup==Organic Social;medium==social"

# sitesのfilter_dを変換して使用
sites_for_cv = []
for s in sites:
    s_copy = s.copy()
    if s.get('filter_d'):
        s_copy['filter_d'] = ga4.convert_filter_to_event_scope(s['filter_d'])
    sites_for_cv.append(s_copy)

df_cv = mg.report.run.all(sites_for_cv, d=[...], filter_d="site.filter_d", ...)
```

#### `ga4.classify_source_channel(df, channel_col='channel', medium_col='medium', source_col='source', custom_channels=None)`

source正規化とchannel分類を統合して実行します。sourceとchannelの両列を含むDataFrameを返します。

**パラメータ:**
- `df` (pd.DataFrame) - データフレーム
- `channel_col` (str) - チャネル列名（default: 'channel'）
- `medium_col` (str) - メディア列名（default: 'medium'）
- `source_col` (str) - ソース列名（default: 'source'）
- `custom_channels` (dict | None) - プロジェクト固有のチャネル定義
  - **簡易形式（正規表現リスト）**: `{"Group": [r"example\.com", r"test\.com"]}` - detectのみ、正規表現として扱われる
  - **完全形式（normalize + detect）**: `{"Channel Name": {"normalize": {}, "detect": [patterns]}}`

**戻り値:** pd.DataFrame - 2列のDataFrame。列名は`source_col`と`channel_col`パラメータに従います。

**構造化パターン定義:**

関数内部でchannelごとに以下の構造でパターンを定義：

1. **AI**: AIサービスからの流入
   - **normalize**: ChatGPT, Gemini, Claude, Copilot, Perplexity
   - **detect**: bing.com/chat（限定）, aistudio.google.com, makersuite.google.com

2. **Organic Search**: 検索エンジンからの流入
   - **normalize**: docomo, bing, auone
   - **detect**: service.smt.docomo.ne.jp（完全パターン）, \bsearch\b（単語境界）等

3. **Organic Social**: SNSからの流入
   - **normalize**: Facebook, X (Twitter), Instagram, YouTube, TikTok, Threads
   - **detect**: 正規化済み名での判定（誤検出防止）

**custom_channelsの使い方:**

```python
from megaton.transform import ga4

# 簡易形式：正規表現リスト（detectのみ）
result = ga4.classify_source_channel(
    df,
    custom_channels={"Group": [r"example\.com", r"sub\.example\.com"]}
)

# 完全形式：normalize + detectパターン
result = ga4.classify_source_channel(
    df,
    custom_channels={
        "Shiseido Internal": {
            "normalize": {},
            "detect": [
                r"extra\.shiseido\.co\.jp",
                r"(spark|international|intra)\.shiseido\.co\.jp",
                r"office\.net", r"sharepoint", r"teams",
                r"basement\.jp", r"yammer",
            ]
        }
    }
)

# 結果の使用（デフォルト列名）
df["source"] = result["source"]
df["channel"] = result["channel"]

# または一括代入
df[["source", "channel"]] = result

# 非デフォルト列名の場合
result = ga4.classify_source_channel(
    df,
    source_col="my_source",
    channel_col="my_channel"
)
df[["my_source", "my_channel"]] = result
```

**メリット:**
- source_map辞書の管理が不要（パターン定義がga4.py内で完結）
- 誤判定の防止（正規表現の精密化、単語境界考慮、正規化後の名前チェック）
- 処理の簡潔化（2段階処理→1ステップ）
- 統一インターフェース（プロジェクト間でのパターン共有が容易）

#### `ga4.classify_channel(df, channel_col='channel', medium_col='medium', source_col='source', custom_channels=None)`

GA4のデフォルトチャネルグループを独自ルールで再分類します。内部でclassify_source_channel()を呼び出すラッパー関数です。

**パラメータ:**
- `df` (pd.DataFrame) - データフレーム
- `channel_col` (str) - チャネル列名（default: 'channel'）
- `medium_col` (str) - メディア列名（default: 'medium'）
- `source_col` (str) - ソース列名（default: 'source'）
- `custom_channels` (dict | None) - プロジェクト固有のチャネル定義（classify_source_channel()と同じ形式）

**戻り値:** pd.Series - 再分類されたチャネル

**例:**
```python
from megaton.transform import ga4

# 基本的な使い方
df['channel'] = ga4.classify_channel(df)

# custom_channelsで自社ドメインを指定
df['channel'] = ga4.classify_channel(
    df,
    custom_channels={"Group": [r"example\.com", r"sub\.example\.com"]}
)
```

**注意:** source列の正規化も必要な場合は、classify_source_channel()を直接使用してください。

### Text 処理関数

#### `text.infer_site_from_url(url_val, sites, site_key='site', id_key=None)`

URLから所属サイトを推測します（マルチサイト企業向け）。

**パラメータ:**
- `url_val` (str) - 判定対象のURL
- `sites` (list[dict]) - サイト設定リスト（各要素は `site_key` と `domain`/`url` を含む）
- `site_key` (str) - 返り値として使うキー名（例: `'clinic'`, `'brand'`, `'site'`、default: 'site'）
- `id_key` (str | None) - 特殊IDのキー名（例: `'dentamap_id'`）。Noneなら無視

**戻り値:** str - サイト識別子 or "不明"

**判定ロジック:**
1. **特殊IDチェック**: `id_key` が指定されている場合、URLの `?id=XXX` パラメータと sites の `id_key` 値を比較
2. **ドメインマッチング**: sites の `domain`/`url` からドメインリストを生成し、長い順にマッチング（サブドメイン優先）
3. **フォールバック**: マッチしない場合は `"不明"` を返す

**例:**
```python
from megaton.transform import text

# サイト設定（通常は cfg.sites から取得）
sites = [
    {'clinic': '札幌', 'domain': 'sapporo.example.com'},
    {'clinic': '東京', 'domain': 'tokyo.example.com'},
    {'clinic': 'dentamap', 'domain': 'plus.dentamap.jp', 'dentamap_id': '123'},
]

# URLからサイトを推測
df['clinic'] = df['lp'].apply(
    lambda url: text.infer_site_from_url(url, sites, site_key='clinic', id_key='dentamap_id')
)

# 特殊IDマッチの例
text.infer_site_from_url('?id=123', sites, site_key='clinic', id_key='dentamap_id')
# => 'dentamap'
```

#### `text.map_by_regex(series, mapping, default=None, flags=0, lower=True, strip=True)`

Seriesの値を正規表現マッピングで変換します。

**パラメータ:**
- `series` (pd.Series) - 対象Series
- `mapping` (dict) - 変換マッピング辞書 `{pattern: mapped_value}`
- `default` (str | None) - マッチしない場合の値（default: None = 元の値を保持）
- `flags` (int) - 正規表現フラグ（default: 0）
- `lower` (bool) - マッチ前に小文字化（default: True）
- `strip` (bool) - マッチ前にstrip（default: True）

**戻り値:** pd.Series

**例:**
```python
from megaton.transform import text

# クエリの正規化
query_map = {
    r'矯正\s*歯科': '矯正歯科',
    r'インビザライン': 'invisalign',
}
df['query'] = text.map_by_regex(df['query'], query_map)

# ソースの正規化
source_map = {
    r'.*google.*': 'Google',
    r'.*yahoo.*': 'Yahoo',
}
df['source'] = text.map_by_regex(df['source'], source_map, default='Other')
```

#### `text.clean_url(series, unquote=True, drop_query=True, drop_hash=True, lower=True)`

URL Seriesをクリーンアップします。

**パラメータ:**
- `series` (pd.Series) - URL Series
- `unquote` (bool) - URLデコード（default: True）
- `drop_query` (bool) - クエリパラメータを削除（default: True）
- `drop_hash` (bool) - フラグメントを削除（default: True）
- `lower` (bool) - 小文字化（default: True）

**戻り値:** pd.Series

**例:**
```python
from megaton.transform import text

df['page'] = text.clean_url(df['page'])
# 'https://example.com/Path?utm=1#frag' => 'https://example.com/path'
```

#### `text.normalize_whitespace(series, mode='remove_all')`

Seriesの空白文字を正規化します。

**パラメータ:**
- `series` (pd.Series) - 対象Series
- `mode` (str) - 'remove_all'（すべて削除）または 'collapse'（複数空白を1つに）

**戻り値:** pd.Series

**例:**
```python
from megaton.transform import text

# 空白をすべて削除
df['query_key'] = text.normalize_whitespace(df['query'], mode='remove_all')
# 'foo  bar' => 'foobar'

# 複数空白を1つに統一
df['query'] = text.normalize_whitespace(df['query'], mode='collapse')
# 'foo  bar' => 'foo bar'
```

#### `text.force_text_if_numeric(series, prefix="'")`

数値のみの文字列に接頭辞を付けます（Sheets での自動数値変換を防止）。

**パラメータ:**
- `series` (pd.Series) - 対象Series
- `prefix` (str) - 接頭辞（default: `"'"`）

**戻り値:** pd.Series

**例:**
```python
from megaton.transform import text

df['zip_code'] = text.force_text_if_numeric(df['zip_code'])
# '123' => "'123"
```

### Classify 関数

#### `classify.classify_by_regex(df, src_col, mapping, out_col, default='other')`

DataFrameの列を正規表現パターンで分類します。

**パラメータ:**
- `df` (pd.DataFrame) - データフレーム
- `src_col` (str) - 分類元の列名
- `mapping` (dict) - 分類マッピング `{pattern: label}`
- `out_col` (str) - 出力列名
- `default` (str) - マッチしない場合のデフォルト値（default: 'other'）

**戻り値:** pd.DataFrame

**例:**
```python
from megaton.transform import classify

page_map = {r'/blog/': 'Blog', r'/products/': 'Products'}
df = classify.classify_by_regex(df, 'page', page_map, 'page_category', default='Other')
```

#### `classify.infer_label_by_domain(series, domain_to_label_map, default='不明')`

URL Seriesからドメインを抽出してラベルを推測します。

**パラメータ:**
- `series` (pd.Series) - URL Series
- `domain_to_label_map` (dict) - ドメイン→ラベルのマッピング
- `default` (str) - マッチしない場合のデフォルト値（default: '不明'）

**戻り値:** pd.Series

**例:**
```python
from megaton.transform import classify

domain_map = {'example.com': 'Site A', 'test.org': 'Site B'}
df['site'] = classify.infer_label_by_domain(df['url'], domain_map)
```

### Table ユーティリティ

#### `table.ensure_columns(df, columns, fill=None, drop_extra=True)`

指定した列を必ず持つ DataFrame に整形します。

**パラメータ:**
- `df` (pd.DataFrame) - データフレーム
- `columns` (list[str]) - 期待する列のリスト
- `fill` (Any | None) - 追加列の初期値（default: None）
- `drop_extra` (bool) - 余分な列を削除するか（default: True）

**戻り値:** pd.DataFrame

**例:**
```python
from megaton.transform import table

df = table.ensure_columns(df, columns=['month', 'query', 'clicks'])
```

#### `table.normalize_key_cols(df, cols, to_str=True, strip=True, lower=False, remove_trailing_dot0=True)`

キー列の型・表記を統一します。

**パラメータ:**
- `df` (pd.DataFrame) - データフレーム
- `cols` (list[str]) - 正規化する列名
- `to_str` (bool) - 文字列化するか（default: True）
- `strip` (bool) - 前後空白の除去（default: True）
- `lower` (bool) - 小文字化（default: False）
- `remove_trailing_dot0` (bool) - 末尾の `.0` を除去（default: True）

**戻り値:** pd.DataFrame

**例:**
```python
from megaton.transform import table

df = table.normalize_key_cols(df, cols=['month', 'clinic'])
```

#### `table.normalize_thresholds_df(df, min_default=10, max_default=50, ...)`

しきい値 DataFrame を正規化します。

**パラメータ:**
- `df` (pd.DataFrame | None) - しきい値データ
- `min_default` (int) - 最小値のデフォルト（default: 10）
- `max_default` (int) - 最大値のデフォルト（default: 50）

**戻り値:** pd.DataFrame | None

**例:**
```python
from megaton.transform import table

df_thresholds = table.normalize_thresholds_df(df_thresholds)
```

#### `table.dedup_by_key(df, key_cols, prefer_by=None, prefer_ascending=False, keep='first')`

キー列で重複を除去します。

**パラメータ:**
- `df` (pd.DataFrame) - データフレーム
- `key_cols` (list[str]) - キー列名のリスト
- `prefer_by` (str | list[str] | None) - 優先順位を決める列（default: None）
- `prefer_ascending` (bool) - True で最小値を選択、False で最大値を選択（default: False）
- `keep` (str) - 'first' または 'last'（default: 'first'）

**戻り値:** pd.DataFrame

**例:**
```python
from megaton.transform import table

# impressions が最大の行を残す
df = table.dedup_by_key(df, key_cols=['month', 'query'], prefer_by='impressions')

# position が最小の行を残す
df = table.dedup_by_key(df, key_cols=['query'], prefer_by='position', prefer_ascending=True)
```

#### `table.group_sum(df, group_cols, sum_cols)`

指定列でグループ化して合計を計算します。

**パラメータ:**
- `df` (pd.DataFrame) - データフレーム
- `group_cols` (list[str]) - グループ化する列
- `sum_cols` (list[str]) - 合計する列

**戻り値:** pd.DataFrame

**例:**
```python
from megaton.transform import table

df_sum = table.group_sum(df, group_cols=['month', 'site'], sum_cols=['clicks', 'impressions'])
```

#### `table.weighted_avg(df, group_cols, value_col, weight_col, out_col=None)`

加重平均を計算します。

**パラメータ:**
- `df` (pd.DataFrame) - データフレーム
- `group_cols` (list[str]) - グループ化する列
- `value_col` (str) - 値列
- `weight_col` (str) - 重み列
- `out_col` (str | None) - 出力列名（default: value_col）

**戻り値:** pd.DataFrame

**例:**
```python
from megaton.transform import table

# impressionsで重み付けしたposition平均
df_avg = table.weighted_avg(
    df,
    group_cols=['month', 'query'],
    value_col='position',
    weight_col='impressions'
)
```

---

## エイリアス

### `mg.sc`

`mg.search` のエイリアスです。

**例:**
```python
mg.sc.run(dimensions=['query', 'page'])
# mg.search.run(dimensions=['query', 'page']) と同じ
```

---

## 参考資料

- [cheatsheet.md](cheatsheet.md) - クイックリファレンス
- [searchresult-api.md](searchresult-api.md) - SearchResult 詳細ガイド
- [advanced.md](advanced.md) - 認証と設計思想
