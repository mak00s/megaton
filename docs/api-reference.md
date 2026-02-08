# Megaton API リファレンス

このドキュメントは、Megaton の全 API を網羅的に説明します。クイックリファレンスは [cheatsheet.md](cheatsheet.md) を参照してください。

---

## 目次

- [初期化](#初期化)
- [Search Console API](#search-console-api)
- [GA4 Analytics API](#ga4-analytics-api)
- [CSV API](#csv-api)
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

### `Megaton(credential=None, use_ga3=False, cache_key=None, headless=False)`

Megaton インスタンスを作成します。

**パラメータ:**
- `credential` (str | dict | None) - 認証情報
  - `None`: 環境変数 `MEGATON_CREDS_JSON` を使用
  - `str`: JSON 文字列、ファイルパス、またはディレクトリパス
  - `dict`: 認証情報の辞書
- `use_ga3` (bool) - UA (GA3) クライアントも初期化するか（default: False）
- `cache_key` (str | None) - OAuth 資格情報キャッシュキー（default: None）
- `headless` (bool) - UI なしモード（default: False）
  - `True`: ウィジェット UI を表示せず、コードで明示的に指定
  - `False`: UI で対話的に選択可能

**戻り値:** Megaton インスタンス

### `mg.auth(credential=None, cache_key=None)`

認証情報を読み込み、利用可能なクライアントを初期化します。

**パラメータ:**
- `credential` (str | dict | None) - 認証情報（コンストラクタと同様）
- `cache_key` (str | None) - OAuth 資格情報キャッシュキー

**戻り値:** None

**環境依存・注意点:**
- `headless=True` では UI 認証フローを表示しません。
- OAuth 認証で `headless=True` の場合、既存キャッシュが必要です（`cache_key` 推奨）。
- Google Colab では認証ソースがディレクトリの場合、必要に応じて Drive マウントを使います。
- 認証に失敗した場合は例外ではなくログ/メッセージで通知され、`self.creds` は未設定のままです。

### `mg.enabled`

現在有効なサービスを返します（`ga3`, `ga4`, `gs`, `sc`）。

**戻り値:** list[str]

### `mg.ga_ver`

現在選択されている GA バージョンを返します。

**戻り値:** str | None

---

## Search Console API

### `mg.sc`

`mg.search` のエイリアスです。

**戻り値:** Search インスタンス

### `mg.launch_sc(site_url=None)`

Search Console クライアントを明示的に初期化します。

**パラメータ:**
- `site_url` (str | None) - 初期選択するサイト URL

**戻り値:** Search Console クライアント | None

**失敗時:**
- 認証未完了、認証フォーマット不正、スコープ不足、初期化例外時は `None`

### `mg.search.sites`

**戻り値:** list[str] - アクセス可能なサイト URL のリスト

初回アクセス時に自動的に取得され、キャッシュされます。

### `mg.search.get.sites()`

サイト一覧を強制的に再取得します。

**戻り値:** list[str]

### `mg.search.use(site_url)`

対象サイトを選択します。

**パラメータ:**
- `site_url` (str) - サイト URL（例: `'https://example.com/'`）

**戻り値:** str（選択した `site_url`）

### `mg.search.set.dates(date_from, date_to)`

レポート期間を日付で設定します。

**パラメータ:**
- `date_from` (str) - 開始日（`YYYY-MM-DD` / `NdaysAgo` / `yesterday` / `today`）
- `date_to` (str) - 終了日（`YYYY-MM-DD` / `NdaysAgo` / `yesterday` / `today`）

**戻り値:** tuple[str, str]

**補足:**
- `NdaysAgo` / `yesterday` / `today` は `mg.search.run()` 実行時に `YYYY-MM-DD` へ正規化されます。

### `mg.search.set.months(ago=1, window_months=1, tz='Asia/Tokyo', now=None, min_ymd=None)`

月単位でレポート期間を設定します。

**パラメータ:**
- `ago` (int) - 何ヶ月前から開始するか（default: 1）
- `window_months` (int) - 何ヶ月分取得するか（default: 1）
- `tz` (str) - タイムゾーン（default: 'Asia/Tokyo'）
- `now` (datetime | None) - 基準日時（default: None = 現在時刻）
- `min_ymd` (str | None) - 開始日の最小制約（YYYYMMDD形式）

**戻り値:** DateWindow - 期間情報を含む namedtuple

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

**前提条件・例外:**
- `mg.search.use(site_url)` で対象サイトを先に指定（未指定時は `ValueError`）
- 日付は `mg.search.set.*` または `mg.report.set.*` で先に指定（未指定時は `ValueError`）
- `dimension_filter` の文字列演算子は `=~`, `!~`, `=@`, `!@` のみ（不正時は `ValueError`）

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

**失敗時の扱い:**
- `site_url_key` が空のアイテムはスキップ
- アイテム単位の取得失敗はそのアイテムのみスキップして継続
- 全件スキップ時は空 DataFrame を持つ `SearchResult` を返します

### `mg.search.filter_by_thresholds(df, site, clicks_zero_only=False)`

サイト設定の閾値を適用してフィルタリングします。

**パラメータ:**
- `df` (pd.DataFrame) - Search Console データ
- `site` (dict) - サイト設定辞書
  - サポートされるキー: `min_impressions`, `max_position`, `min_pv`, `min_cv`
- `clicks_zero_only` (bool) - clicks >= 1 の行を無条件に保持（default: False）

**戻り値:** pd.DataFrame

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

### `mg.report.set.months(ago=1, window_months=13, tz='Asia/Tokyo', now=None, min_ymd=None)`

月単位でレポート期間を設定します。

**パラメータ:** `mg.search.set.months()` と同じ

**戻り値:** DateWindow

### `mg.report.run(d, m, filter_d=None, filter_m=None, sort=None, **kwargs)`

GA4 レポートを実行します。

**パラメータ:**
- `d` (list) - ディメンション（省略形）
  - 文字列または `(api_name, alias)` のタプルのリスト
- `m` (list) - 指標（省略形）
  - 文字列または `(api_name, alias)` のタプルのリスト
  - もしくは `[(metrics, options), ...]` のメトリクスセット配列（`options` は `filter_d` / `filter_m`）
- `filter_d` (str | None) - ディメンションフィルタ（`<field><op><value>`、`;` 区切りで AND）
- `filter_m` (str | None) - メトリクスフィルタ（`<field><op><value>`、`;` 区切りで AND）
- `sort` (str | None) - ソート順（例: `"date,-sessions"`）
- `merge` (str | None) - メトリクスセット一括モードの結合方法（`left` / `outer`）
- `show` (bool) - 実行結果を表示するか（default: True）
- `max_retries` (int) - GA4 Data API の一時エラー（`ServiceUnavailable`）時の最大再試行回数（default: `3`）
- `backoff_factor` (float) - 再試行待機時間の係数。待機は `backoff_factor * (2**attempt)`（default: `2.0`）

**戻り値:** ReportResult - 結果は `mg.report.data` にも格納

**`show` オプション:**
- `show=False` を指定すると表示を抑制します（戻り値の `ReportResult` と `mg.report.data` は通常どおり利用可能）。

**名前解決ルール（d / m）:**
- 文字列指定時は `api_name` または `display_name` の**完全一致**のみを受け付けます（前後空白は無視）。
- 部分一致・あいまい一致・自動補完は行いません。
- カスタムディメンション/メトリクスは `parameter_name` 単体では解決されません。`api_name`（例: `customEvent:xxx`, `customUser:xxx`）で指定してください。

**`filter_d` / `filter_m` の演算子:**
- `==`, `!=`, `=@`, `!@`, `=~`, `!~`, `>`, `>=`, `<`, `<=`

**失敗時の扱い:**
- 不正なフィルタや抽出条件ではエラーメッセージを表示し、結果が更新されない場合があります
- `show=False` を指定しない限り、実行後に結果表示を試みます
- GA4 Data API が一時的に利用不可（`ServiceUnavailable`）の場合は指数バックオフで再試行し、枯渇時は空結果を返します

**m の複数セット一括取得（run）**

`mg.report.run()` で同一ディメンションに対して複数のメトリクスセットを一括取得し、Megaton 側で自動マージします。

**API 署名:**

**モード判定:**
- `m` の要素が `(metrics_list, options_dict)` 形式なら **一括モード**
- 一括モードの場合、`m` は全てこの形式に統一（混在はエラー）

**挙動:**
- `m` は `[(metrics, options), ...]` の配列を受け付ける（`metrics` は従来の m リスト）
- `options` は `filter_d` / `filter_m` を受け付け、**`filter_d` は省略可能**
- グローバル `filter_d` / `filter_m` とセットのフィルタは AND 合成
- 取得結果は `d` 列でマージし、**欠損は 0 埋め**（int/float とも 0）
- デフォルトは **left 結合**（1セット目を基準）
- `merge="outer"` を指定すると他セットにしかない行も保持
- 同名メトリクスが複数セットに含まれていたらエラー
- いずれかのセット取得が失敗したら全体を失敗扱い
- `sort` はマージ後に適用

**注意（よくある勘違い）:**
- `mg.report.run()` では `("sessions", "sessions", {"filter_d": ...})` のような **メトリクス定義の options dict は解釈されません**。
  - フィルタをメトリクスごとに分けたい場合は、上の **multi-set**（`m=[([...], {...}), ...]`）を使ってください。

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
    - `run.all` のメトリクス別フィルタは **filter_d のみ**をサポート（`filter_m` は未対応）
    - 同一アイテム内で `filter_d` が異なるメトリクスが混在する場合、Megaton は **複数回 API コール**して結果をマージします
- `dimensions` (list | None) - ディメンション（明示形）
- `metrics` (list | None) - 指標（明示形）
- `item_key` (str) - 識別子のキー名（default: 'site'）
- `property_key` (str) - GA4 プロパティ ID のキー名（default: 'ga4_property_id'）
- `item_filter` (list | callable | None) - アイテムフィルタ
- `verbose` (bool) - 進捗メッセージを表示（default: True）
- `**kwargs` - `mg.report.run()` に渡す追加引数
  - `filter_d="site.filter_d"` を指定すると、各 `item['filter_d']` を使用します

**戻り値:** ReportResult - 結合されたデータ（`.df` で DataFrame を取得）

**失敗時の扱い:**
- `property_key` が空のアイテムはスキップ
- アイテム単位の取得失敗はそのアイテムのみスキップして継続
- 全件スキップ時は空 DataFrame を持つ `ReportResult` を返します

### `mg.report.prep(conf, df=None, show=True)`

DataFrame の前処理（列名変更、値置換など）を行います。

**パラメータ:**
- `conf` (dict) - 列ごとの処理設定
  - 各列に対して `cut`, `delete`, `name`, `replace`, `type` を指定
  - 形式:
    - `cut`: `str | list[str]`（正規表現パターンを削除）
    - `delete`: `bool`（truthy なら列削除）
    - `name`: `str`（列名変更）
    - `replace`: `(before, after)` タプル（正規表現置換）
    - `type`: `str` など `DataFrame.astype()` に渡せる型指定
- `df` (pd.DataFrame | None) - 対象 DataFrame（default: `mg.report.data`）
- `show` (bool) - 処理後に結果表示するか（default: `True`）

**戻り値:**
- `show=True`: 表示用オブジェクト
- `show=False`: 処理後の `pd.DataFrame`
- いずれも `mg.report.data` は更新されます

**処理順序:**
1. `cut` / `replace` を列ごとに先に実行
2. `delete` / `type` / `name` を `utils.prep_df()` で適用
3. `mg.report.data` を更新
4. `show=True` なら `mg.report.show()` の戻り値を返却、`show=False` なら `mg.report.data` を返却

**挙動メモ:**
- `cut` / `replace` は regex 置換です（リテラル一致ではありません）。
- `replace` は `(before, after)` タプルのみ有効です（それ以外は無視）。
- 未知のアクションキーは無視されます。

**前提条件・例外:**
- `df` を省略する場合は `mg.report.data` が DataFrame であること
- `type` 指定が不正な場合は `astype` 由来の例外が発生

**例:**
```python
conf = {
    "pagePath": {
        "cut": [r"^https?://[^/]+", r"\?.*$"],  # ドメインとクエリを除去
        "name": "page",
    },
    "sessions": {
        "type": "int64",
    },
    "campaign": {
        "replace": (r"\([^)]*\)", ""),  # 括弧内を削除
    },
    "debug_col": {
        "delete": True,
    },
}

mg.report.prep(conf, show=False)
```

### `mg.report.data`

**戻り値:** pd.DataFrame | None - 直近のレポート結果

### `mg.report.show()`

`mg.report.data` を表示します。

**戻り値:** 表示オブジェクト

**前提条件:**
- `mg.report.data` が DataFrame であること

### `mg.report.download(filename)`

`mg.report.data` を CSV 保存し、Notebook からダウンロードします。

**パラメータ:**
- `filename` (str) - 保存ファイル名

**戻り値:** None

**前提条件:**
- `mg.report.data` が DataFrame であること

### `mg.report.to.csv(filename='report', quiet=False)`

`mg.report.data` を CSV 保存します（`save_df` と同じ日付サフィックス規則）。

**パラメータ:**
- `filename` (str) - ファイル名またはパス
- `quiet` (bool) - メッセージを出力しない（default: False）

**戻り値:** None

**前提条件:**
- `mg.report.data` が DataFrame であること

### `mg.report.to.sheet(sheet_name)`

`mg.report.data` を指定シートへ上書き保存します。

**パラメータ:**
- `sheet_name` (str) - シート名

**戻り値:** None

**前提条件:**
- 先に `mg.open.sheet(url)` でスプレッドシートを開いていること
- `mg.report.data` が DataFrame であること

### `mg.report.dates.to.sheet(sheet, start_cell, end_cell)`

レポート期間をシートに書き込みます。

**パラメータ:**
- `sheet` (str) - シート名
- `start_cell` (str) - 開始日を書き込むセル（A1 表記）
- `end_cell` (str) - 終了日を書き込むセル（A1 表記）

**戻り値:** bool | None

**前提条件・例外:**
- `mg.report.start_date` / `mg.report.end_date` が設定済みであること（未設定時は `ValueError`）
- 先に `mg.open.sheet(url)` でスプレッドシートを開いていること（未接続時は `ValueError`）

### `mg.ga["4"].property.show(me='info')`

GA4 プロパティのメタデータを表示します。

**パラメータ:**
- `me` (str) - 表示対象
  - `info` / `dimensions` / `metrics` / `custom_dimensions` / `user_properties` / `custom_metrics`

**戻り値:** pd.DataFrame

**補足:**
- `user_properties` は、カスタムディメンションのうち `scope == 'USER'` の項目のみを返します。

---

## CSV API

### `mg.save.to.csv(df=None, filename='report', mode='w', include_dates=True, quiet=False)`

DataFrame を CSV に保存します。

**パラメータ:**
- `df` (pd.DataFrame | None) - 保存する DataFrame（default: `mg.report.data`）
- `filename` (str) - ファイル名またはパス（拡張子未指定時は `.csv` を付与）
- `mode` (`'w' | 'a'`) - 書き込みモード（default: `'w'`）
- `include_dates` (bool) - `_<start>-<end>` サフィックスを付与（default: True）
- `quiet` (bool) - メッセージを出力しない（default: False）

**戻り値:** None

**前提条件:**
- `df` を省略する場合は `mg.report.data` が DataFrame であること

### `mg.append.to.csv(df=None, filename='report', include_dates=True, quiet=False)`

DataFrame を CSV の末尾に追記します。

**パラメータ:**
- `df` (pd.DataFrame | None) - 追記する DataFrame（default: `mg.report.data`）
- `filename` (str) - ファイル名またはパス
- `include_dates` (bool) - 日付サフィックスを付与（default: True）
- `quiet` (bool) - メッセージを出力しない（default: False）

**戻り値:** None

**前提条件・例外:**
- `df` を省略する場合は `mg.report.data` が DataFrame であること
- DataFrame 以外を渡した場合は `TypeError`

### `mg.upsert.to.csv(df=None, filename='report', keys, columns=None, sort_by=None, include_dates=True, quiet=False)`

キー列を基準に CSV へアップサート（更新または挿入）します。

**パラメータ:**
- `df` (pd.DataFrame | None) - アップサートする DataFrame（default: `mg.report.data`）
- `filename` (str) - ファイル名またはパス
- `keys` (list[str]) - 重複判定に使うキー列
- `columns` (list[str] | None) - 出力列順（default: 既存列）
- `sort_by` (list[str] | str | None) - ソート列（default: `keys`）
- `include_dates` (bool) - 日付サフィックスを付与（default: True）
- `quiet` (bool) - メッセージを出力しない（default: False）

**戻り値:** pd.DataFrame | None

**前提条件・例外:**
- `df` を省略する場合は `mg.report.data` が DataFrame であること
- DataFrame 以外を渡した場合は `TypeError`
- 読み込み失敗やキー列不整合など、アップサート不能時は `None`

---

## Google Sheets API

### `mg.launch_gs(url)`

Google Sheets クライアントを初期化します（`mg.open.sheet(url)` の互換 API）。

**パラメータ:**
- `url` (str) - スプレッドシート URL

**戻り値:** bool | None

**失敗時:**
- 認証未完了、権限不足、URL不正、API無効、タイムアウト等で `None`

### `mg.open.sheet(url)`

スプレッドシートを開きます。

**パラメータ:**
- `url` (str) - スプレッドシート URL

**タイムアウト:**
- 既定は 180 秒（接続待ちのみ）
- 環境変数 `MEGATON_GS_TIMEOUT` で上書き可能
  - 0 以下で無効化
- タイムアウト時はメッセージを出して終了します

**Retry（指数バックオフ）:**
- 一時エラー（HTTP 429/5xx）やネットワーク例外に対して指数バックオフで再試行します
- 環境変数で上書き可能:
  - `MEGATON_GS_MAX_RETRIES`（default: `3`）
  - `MEGATON_GS_BACKOFF_FACTOR`（default: `2.0`）
  - `MEGATON_GS_MAX_WAIT`（1回の待機上限、秒。未指定なら上限なし）
  - `MEGATON_GS_MAX_ELAPSED`（総経過時間上限、秒。未指定なら上限なし）
  - `MEGATON_GS_JITTER`（待機時間に jitter を付与。`0 <= jitter < 1`、default: `0`）

**戻り値:** bool | None

**失敗時:**
- 認証未完了、権限不足、URL不正、API無効、タイムアウト等で `None`

### `mg.sheets.select(sheet_name)`

シートを選択します。

**パラメータ:**
- `sheet_name` (str) - シート名

**戻り値:** str | None

**前提条件・例外:**
- 先に `mg.open.sheet(url)` 済みであること（未接続時は `ValueError`）

### `mg.sheets.create(sheet_name)`

新しいシートを作成します。

**パラメータ:**
- `sheet_name` (str) - 作成するシート名

**戻り値:** str（作成したシート名）

**前提条件・例外:**
- 先に `mg.open.sheet(url)` 済みであること（未接続時は `ValueError`）

### `mg.sheets.delete(sheet_name)`

シートを削除します。

**パラメータ:**
- `sheet_name` (str) - 削除するシート名

**戻り値:** bool

**前提条件・例外:**
- 先に `mg.open.sheet(url)` 済みであること（未接続時は `ValueError`）
- シートが存在しない場合は `ValueError`

### `mg.save.to.sheet(sheet_name, df=None, sort_by=None, sort_desc=True, start_row=1, create_if_missing=False, auto_width=False, freeze_header=False, max_retries=3, backoff_factor=2.0)`

DataFrame をシートに上書き保存します。

**パラメータ:**
- `sheet_name` (str) - シート名
- `df` (pd.DataFrame | None) - 保存する DataFrame（default: `mg.report.data`）
- `sort_by` (list[str] | str | None) - ソート列（指定時のみソート）
- `sort_desc` (bool) - 降順ソート（default: True）
- `start_row` (int) - ヘッダを書き込む開始行（1始まり、default: 1）
- `create_if_missing` (bool) - 対象シートがない場合に自動作成するか（default: False）
- `auto_width` (bool) - 列幅を自動調整（default: False）
- `freeze_header` (bool) - 1行目を固定（default: False）
- `max_retries` (int) - 一時エラー（HTTP 429/5xx）時の最大再試行回数（default: `3`）
- `backoff_factor` (float) - 再試行待機時間の係数。待機は `backoff_factor * (2**attempt)`（default: `2.0`）

**戻り値:** None

**前提条件:**
- 先に `mg.open.sheet(url)` 済みであること
- `df` を省略する場合は `mg.report.data` が DataFrame であること
- `start_row >= 1` であること（`start_row=2` の場合、1行目は保持されます）
- `create_if_missing=False` の場合、対象シートが未作成だと保存されません

### `mg.append.to.sheet(sheet_name, df=None, create_if_missing=False, auto_width=False, freeze_header=False, max_retries=3, backoff_factor=2.0)`

DataFrame を既存データの末尾に追記します。

**パラメータ:**
- `sheet_name` (str) - シート名
- `df` (pd.DataFrame | None) - 追記する DataFrame（default: `mg.report.data`）
- `create_if_missing` (bool) - 対象シートがない場合に自動作成するか（default: False）
- `auto_width` (bool) - 列幅を自動調整（default: False）
- `freeze_header` (bool) - 1行目を固定（default: False）
- `max_retries` (int) - 一時エラー（HTTP 429/5xx）時の最大再試行回数（default: `3`）
- `backoff_factor` (float) - 再試行待機時間の係数。待機は `backoff_factor * (2**attempt)`（default: `2.0`）

**戻り値:** None

**前提条件:**
- 先に `mg.open.sheet(url)` 済みであること
- `df` を省略する場合は `mg.report.data` が DataFrame であること
- `create_if_missing=False` の場合、対象シートが未作成だと追記されません

### `mg.upsert.to.sheet(sheet_name, df=None, keys, columns=None, sort_by=None, auto_width=False, freeze_header=False, max_retries=3, backoff_factor=2.0)`

キー列を基準にアップサート（更新または挿入）します。

**パラメータ:**
- `sheet_name` (str) - シート名
- `df` (pd.DataFrame | None) - アップサートする DataFrame（default: `mg.report.data`）
- `keys` (list[str]) - キー列のリスト
- `columns` (list[str] | None) - 出力する列のリスト（default: すべて）
- `sort_by` (list[str] | None) - ソート列のリスト
- `auto_width` (bool) - 列幅を自動調整（default: False）
- `freeze_header` (bool) - 1行目を固定（default: False）
- `max_retries` (int) - 一時エラー（HTTP 429/5xx）時の最大再試行回数（default: `3`）
- `backoff_factor` (float) - 再試行待機時間の係数。待機は `backoff_factor * (2**attempt)`（default: `2.0`）

**戻り値:** pd.DataFrame | None

**前提条件・例外:**
- 先に `mg.open.sheet(url)` 済みであること（未接続時は `ValueError`）
- `df` を省略する場合は `mg.report.data` が DataFrame であること
- DataFrame 以外を渡した場合は `TypeError`
- アップサート不能時は `None`

### 現在のシートへの操作

選択されたシートに対する操作：

#### `mg.sheet.clear()`

現在のシートをクリアします。

**前提条件・例外:**
- スプレッドシート接続済み、かつ現在シート選択済みであること（未満足時は `ValueError`）

#### `mg.sheet.data`

**戻り値:** list[dict] - 現在のシートのデータ

**前提条件・例外:**
- スプレッドシート接続済み、かつ現在シート選択済みであること（未満足時は `ValueError`）

#### `mg.sheet.cell.set(cell, value)`

単一セルに値を書き込みます。

**パラメータ:**
- `cell` (str) - セル（A1 表記）
- `value` (str | int | float) - 値

**戻り値:** bool | None

**前提条件・例外:**
- スプレッドシート接続済み、かつ現在シート選択済みであること（未満足時は `ValueError`）

#### `mg.sheet.range.set(a1_range, values)`

範囲に配列を書き込みます。

**パラメータ:**
- `a1_range` (str) - 範囲（A1 表記、例: 'A1:B2'）
- `values` (list[list]) - 2次元配列

**戻り値:** bool | None

**前提条件・例外:**
- スプレッドシート接続済み、かつ現在シート選択済みであること（未満足時は `ValueError`）

#### `mg.sheet.save(df=None, sort_by=None, sort_desc=True, start_row=1, auto_width=False, freeze_header=False, max_retries=3, backoff_factor=2.0)`

現在のシートに DataFrame を保存します。

**パラメータ:**
- `df` (pd.DataFrame | None) - 保存する DataFrame（default: `mg.report.data`）
- `sort_by` (list[str] | str | None) - ソート列（指定時のみソート）
- `sort_desc` (bool) - 降順ソート（default: True）
- `start_row` (int) - ヘッダを書き込む開始行（1始まり、default: 1）
- `auto_width` (bool) - 列幅を自動調整（default: False）
- `freeze_header` (bool) - 1行目を固定（default: False）
- `max_retries` (int) - 一時エラー（HTTP 429/5xx）時の最大再試行回数（default: `3`）
- `backoff_factor` (float) - 再試行待機時間の係数。待機は `backoff_factor * (2**attempt)`（default: `2.0`）

**戻り値:** None

**前提条件・例外:**
- スプレッドシート接続済み、かつ現在シート選択済みであること（未満足時は `ValueError`）
- `df` を省略する場合は `mg.report.data` が DataFrame であること
- DataFrame 以外を渡した場合は `TypeError`
- `start_row >= 1` であること（`start_row=2` の場合、1行目は保持されます）

#### `mg.sheet.append(df=None, auto_width=False, freeze_header=False, max_retries=3, backoff_factor=2.0)`

現在のシートに追記します。

**パラメータ:**
- `df` (pd.DataFrame | None) - 追記する DataFrame（default: `mg.report.data`）
- `auto_width` (bool) - 列幅を自動調整（default: False）
- `freeze_header` (bool) - 1行目を固定（default: False）
- `max_retries` (int) - 一時エラー（HTTP 429/5xx）時の最大再試行回数（default: `3`）
- `backoff_factor` (float) - 再試行待機時間の係数。待機は `backoff_factor * (2**attempt)`（default: `2.0`）
  - `append` は非冪等になり得るため、retry は主に HTTP 429/5xx を対象にします

**戻り値:** None

**前提条件・例外:**
- スプレッドシート接続済み、かつ現在シート選択済みであること（未満足時は `ValueError`）
- `df` を省略する場合は `mg.report.data` が DataFrame であること
- DataFrame 以外を渡した場合は `TypeError`

#### `mg.sheet.upsert(df=None, keys, columns=None, sort_by=None, auto_width=False, freeze_header=False)`

現在のシートにアップサートします。

**パラメータ:** `mg.upsert.to.sheet()` と同じ（`max_retries` / `backoff_factor` 含む）

**戻り値:** pd.DataFrame | None

**前提条件・例外:**
- スプレッドシート接続済み、かつ現在シート選択済みであること（未満足時は `ValueError`）
- `df` を省略する場合は `mg.report.data` が DataFrame であること
- DataFrame 以外を渡した場合は `TypeError`

---

## BigQuery API

### `mg.launch_bigquery(project_id)`

BigQuery サービスを起動します。

**パラメータ:**
- `project_id` (str) - GCP プロジェクト ID

**戻り値:** BigQuery クライアント | None

**失敗時:**
- 認証未完了時は `None`

### `bq.run(sql, to_dataframe=True)`

SQL クエリを実行します。

**パラメータ:**
- `sql` (str) - SQL クエリ
- `to_dataframe` (bool) - DataFrame として返す（default: True）

**戻り値:** pd.DataFrame | QueryJob

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

---

## SearchResult メソッドチェーン

`mg.search.run()` が返す SearchResult オブジェクトは、メソッドチェーンで段階的な処理が可能です。

詳細は [cookbook.md](cookbook.md) を参照してください。

**主要メソッド:**
- `.df` - DataFrame にアクセス（プロパティ）
- `.decode(group=True)` - URL デコード
- `.remove_params(keep=None, group=True)` - クエリパラメータ削除
- `.remove_fragment(group=True)` - フラグメント削除
- `.clean_url(dimension='page', unquote=True, drop_query=True, drop_hash=True, lower=True, group=True)` - URL 正規化（GSC の position 重み付き平均などがあるため `group` で集約制御可能）
- `.lower(columns=None, group=True)` - 小文字化
- `.normalize(dimension, by, lower=True, strip=True)` - 正規化（上書き、集約なし）
- `.categorize(dimension, by, into=None, default='(other)')` - カテゴリ列追加（集約なし）
- `.classify(dimension, by, lower=True, strip=True)` - 正規化 + 集約（上書き、常に集約）
- `.normalize_queries(mode='remove_all', prefer_by='impressions', group=True)` - クエリ正規化（空白揺れ統一）
- `.filter_clicks(min=None, max=None, sites=None, site_key='site')` - クリック数フィルタ
- `.filter_impressions(min=None, max=None, sites=None, site_key='site', keep_clicked=False)` - インプレッション数フィルタ
- `.filter_ctr(min=None, max=None, sites=None, site_key='site', keep_clicked=False)` - CTR フィルタ
- `.filter_position(min=None, max=None, sites=None, site_key='site', keep_clicked=False)` - ポジションフィルタ
- `.aggregate(by=None)` - 手動集約
- `.apply_if(condition, method_name, *args, **kwargs)` - 条件付きメソッドチェーン

---

## Filtering

SearchResult の `filter_*` 系メソッドに共通する仕様をまとめます。

### 共通仕様

- **対象メソッド**: `filter_clicks`, `filter_impressions`, `filter_ctr`, `filter_position`
- **グローバル閾値（min / max）**:
  - `min` / `max` は **包含的**（`>= min`, `<= max`）
  - `None` の場合、その側の閾値は適用されません
- **サイト別閾値（sites + site_key）**:
  - `sites` は dict のリスト
  - `site_key` 列の値をキーとして、各行に閾値を割り当てます
  - 使用するキー:
    - `clicks`: `min_clicks` / `max_clicks`
    - `impressions`: `min_impressions` / `max_impressions`
    - `ctr`: `min_ctr` / `max_ctr`
    - `position`: `min_position` / `max_position`
- **優先順位（閾値の決まり方）**:
  - `min` / `max` が **明示されている場合はそれが最優先**
  - `min` / `max` が `None` の場合のみ、`sites` の値が使われます
  - `sites` がない、または `site_key` 列がない場合は **サイト別閾値は無視**されます
- **keep_clicked（例外ルール）**:
  - `filter_impressions` / `filter_ctr` / `filter_position` のみ対象
  - `keep_clicked=True` かつ `clicks` 列がある場合:
    - `clicks >= 1` は **無条件に残る**
    - `clicks == 0` のみ閾値が適用される
    - `clicks` が `NaN` の行は **無条件に残る**
  - `clicks` 列がない場合は `keep_clicked` の効果はありません（通常フィルタと同じ）
- **共通エッジケース**:
  - 必要な列が存在しない場合は **KeyError**
  - 閾値が適用される場合、`NaN` は比較に失敗するため **除外される**
  - `sites` の閾値が見つからない行は、その側の閾値が **未設定扱い**になり通過します

### `filter_clicks(min=None, max=None, sites=None, site_key='site')`

**前提条件（必要な列）**
- `clicks`
- `sites` を使う場合は `site_key` 列

**閾値の決まり方**
- `min` / `max` を明示した場合は全行に適用
- `min` / `max` が `None` の場合のみ `sites` の `min_clicks` / `max_clicks` を使用

**優先順位**
- 明示 `min` / `max` → サイト別閾値 → 未設定（フィルタなし）

**挙動の説明**
- `clicks` に対して `>= min` / `<= max` を適用（包含的）
- `min` と `max` の両方を指定した場合は **両方の条件**を満たす行のみ残る

**エッジケース**
- `clicks` が `NaN` の行は、閾値が適用される場合に除外される
- `sites` があるが `site_key` 列がない場合、サイト別閾値は使われない

**最小例**
```python
result.filter_clicks(min=10)
```

### `filter_impressions(min=None, max=None, sites=None, site_key='site', keep_clicked=False)`

**前提条件（必要な列）**
- `impressions`
- `keep_clicked=True` の場合は `clicks`
- `sites` を使う場合は `site_key` 列

**閾値の決まり方**
- `min` / `max` を明示した場合は全行に適用
- `min` / `max` が `None` の場合のみ `sites` の `min_impressions` / `max_impressions` を使用

**優先順位**
- `keep_clicked` が有効な場合、`clicks >= 1` は常に残る
- 閾値の優先順位は **明示 `min` / `max` → サイト別閾値**

**挙動の説明**
- `impressions` に対して `>= min` / `<= max` を適用（包含的）
- `keep_clicked=True` の場合、クリック済み行は閾値を無視

**エッジケース**
- `impressions == 0` の行は `min > 0` で除外される（`keep_clicked=True` で `clicks >= 1` は例外）
- `impressions` が `NaN` の行は、閾値が適用される場合に除外される
- `clicks` 列がない場合、`keep_clicked` は無視される

**最小例**
```python
result.filter_impressions(min=100, keep_clicked=True)
```

### `filter_ctr(min=None, max=None, sites=None, site_key='site', keep_clicked=False)`

**前提条件（必要な列）**
- `ctr`
- `keep_clicked=True` の場合は `clicks`
- `sites` を使う場合は `site_key` 列

**閾値の決まり方**
- `min` / `max` を明示した場合は全行に適用
- `min` / `max` が `None` の場合のみ `sites` の `min_ctr` / `max_ctr` を使用

**優先順位**
- `keep_clicked` が有効な場合、`clicks >= 1` は常に残る
- 閾値の優先順位は **明示 `min` / `max` → サイト別閾値**

**挙動の説明**
- `ctr` に対して `>= min` / `<= max` を適用（包含的）
- `ctr` は **事前に計算済みの列**が必要（`mg.search.run(...).aggregate()` などで生成される想定）

**エッジケース**
- `impressions == 0` の行は、集計経路によって `ctr=0` または `NaN` になり得る
  - `min > 0` を設定すると除外される
- `ctr` が `NaN` の行は、閾値が適用される場合に除外される
- `clicks` 列がない場合、`keep_clicked` は無視される

**最小例**
```python
result.filter_ctr(min=0.02)
```

### `filter_position(min=None, max=None, sites=None, site_key='site', keep_clicked=False)`

**前提条件（必要な列）**
- `position`
- `keep_clicked=True` の場合は `clicks`
- `sites` を使う場合は `site_key` 列

**閾値の決まり方**
- `min` / `max` を明示した場合は全行に適用
- `min` / `max` が `None` の場合のみ `sites` の `min_position` / `max_position` を使用

**優先順位**
- `keep_clicked` が有効な場合、`clicks >= 1` は常に残る
- 閾値の優先順位は **明示 `min` / `max` → サイト別閾値**

**挙動の説明**
- `position` に対して `>= min` / `<= max` を適用（包含的）
- `position` は **数値が小さいほど良い**（`max` を使うのが一般的）

**エッジケース**
- `position` が `NaN` の行は、閾値が適用される場合に除外される
- `clicks` 列がない場合、`keep_clicked` は無視される

**最小例**
```python
result.filter_position(max=10, keep_clicked=True)
```

---

## ReportResult メソッドチェーン

`mg.report.run.all()` が返す ReportResult オブジェクトは、メソッドチェーンで段階的な処理が可能です。

### 主要メソッド

#### `.df`

DataFrame にアクセスするプロパティ。

#### `.normalize(dimension, by, lower=True, strip=True)`

既存ディメンションを正規化して上書きします（集約しません）。

**パラメータ:**
- `dimension` (str) - 対象ディメンション列名
- `by` (dict | callable) - 正規化マッピング
- `lower` (bool) - 小文字化（default: True）
- `strip` (bool) - 前後空白を削除（default: True）

**戻り値:** ReportResult

#### `.categorize(dimension, by, into=None, default='(other)')`

既存ディメンションからカテゴリ列を追加します（集約しません）。

**パラメータ:**
- `dimension` (str) - 対象ディメンション列名
- `by` (dict | callable) - 分類マッピング
- `into` (str | None) - 出力列名（default: `{dimension}_category`）
- `default` (str) - マッチしない場合のデフォルト値（default: `'(other)'`）

**戻り値:** ReportResult

#### `.classify(dimension, by, lower=True, strip=True)`

正規化して常に集約します（ディメンションは上書き）。

**パラメータ:**
- `dimension` (str) - 対象ディメンション列名
- `by` (dict | callable) - 正規化マッピング
- `lower` (bool) - 小文字化（default: True）
- `strip` (bool) - 前後空白を削除（default: True）

**戻り値:** ReportResult

#### `.group(by, metrics=None, method='sum')`

指定した列で集計します。

**パラメータ:**
- `by` (str | list[str]) - グループ化キーとなる列名
- `metrics` (str | list[str] | None) - 集計する指標列（default: 数値列を自動検出）
- `method` (str) - 集計方法（'sum', 'mean', 'min', 'max' など、default: 'sum'）

**戻り値:** ReportResult

#### `.sort(by, ascending=True)`

指定した列でソートします。

**パラメータ:**
- `by` (str | list[str]) - ソートキーとなる列名
- `ascending` (bool | list[bool]) - 昇順（True）または降順（False）

**戻り値:** ReportResult

#### `.fill(to='(not set)', dimensions=None)`

ディメンション列の欠損値を指定した値で埋めます。

**パラメータ:**
- `to` (str) - 埋める値（default: `'(not set)'`）
- `dimensions` (list[str] | None) - 対象のディメンション列名のリスト（default: すべてのディメンション）

**戻り値:** ReportResult

#### `.to_int(metrics=None, *, fill_value=0)`

指標列を整数型に変換します（欠損値は fill_value で埋められます）。

**パラメータ:**
- `metrics` (str | list[str] | None) - 変換する指標列名
  - `str`: 単一の列名
  - `list[str]`: 複数の列名
  - `None`: すべての数値列（自動推論、int64/float64/Int64/Float64のみ、default）
- `fill_value` (int) - 欠損値を埋める値（default: 0、キーワード専用）

**戻り値:** ReportResult

**注意:**
`metrics=None` の場合、int64, float64, Int64, Float64 型の列のみが対象です。int32, float32, UInt64 などは対象外です。

**例:**
```python
# sessions を整数型に変換（省略形）
result.to_int('sessions')

# 複数の指標を変換（省略形）
result.to_int(['sessions', 'users'])

# すべての数値列を変換
result.to_int()

# 後方互換性（明示形）
result.to_int(metrics=['sessions', 'users'])

# fill_value はキーワード専用
result.to_int(['sessions'], fill_value=99)
```

#### `.replace(dimension, by, *, regex=True)`

ディメンション列の値を辞書マッピングで置換します。

**パラメータ:**
- `dimension` (str) - 置換対象のディメンション列名
- `by` (dict) - 置換マッピング辞書 `{old_value: new_value}`
- `regex` (bool) - True の場合、キーを正規表現として扱う（default: True）

**戻り値:** ReportResult

#### `.clean_url(dimension, *, unquote=True, drop_query=True, drop_hash=True, lower=True)`

URL 列を正規化します。**ReportResult は明示的集約（`.group()` / `.classify()` / `.aggregate()`）の設計のため、このメソッドは集約しません。**

**パラメータ:**
- `dimension` (str) - 対象ディメンション列名
- `unquote` (bool) - URL デコード（default: True）
- `drop_query` (bool) - クエリパラメータを削除（default: True）
- `drop_hash` (bool) - フラグメントを削除（default: True）
- `lower` (bool) - 小文字化（default: True）

**戻り値:** ReportResult

### その他のプロパティ

- `.empty` - DataFrame が空かどうか（bool）
- `.columns` - DataFrame の列名リスト（list[str]）
- `len(result)` - データフレームの行数（int）

---

## ユーティリティ

### `mg.select.ga()`

GA アカウント選択 UI を表示します（headless=False のとき）。

**戻り値:** None

**環境依存:**
- `headless=True` では UI を表示しません（コードで明示指定する運用）

### `mg.select.sheet(sheet_name)`

開いているスプレッドシート内でシートを選択します。

**パラメータ:**
- `sheet_name` (str) - シート名

**戻り値:** bool | None

**前提条件:**
- 先に `mg.open.sheet(url)` 済みであること

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

**戻り値:** 表示オブジェクト | None

**環境依存:**
- Colab では `google.colab.data_table` 表示を利用
- それ以外では `itables`（利用可能時）または通常 `display` を利用

### `mg.load.csv(path)`

CSV ファイルを読み込みます。

**パラメータ:**
- `path` (str) - CSV ファイルのパス

**戻り値:** pd.DataFrame

### `mg.load.cell(row, col, what=None)`

現在選択中のシートの単一セル値を取得します。

**パラメータ:**
- `row` (int) - 行番号
- `col` (int) - 列番号
- `what` (str | None) - 表示ラベル（指定時は `"{what}は{value}"` を出力）

**戻り値:** セル値

**前提条件:**
- 先に `mg.open.sheet(url)` 済みで、対象シートが選択されていること

### `mg.save_df(df, filename, mode='w', include_dates=True, quiet=False)`

DataFrame をローカルファイルに保存します。

**パラメータ:**
- `df` (pd.DataFrame) - 保存する DataFrame
- `filename` (str) - ファイル名（拡張子未指定時は `.csv` を付与）
- `mode` (str) - 書き込みモード（default: 'w'）
- `include_dates` (bool) - ファイル名に `_<start>-<end>` サフィックスを付与（default: True）
- `quiet` (bool) - メッセージを出力しない（default: False）

**戻り値:** str | None（`quiet=True` のとき保存ファイル名）

### `mg.download(df, filename=None)`

Notebook からファイルをダウンロードします。

**パラメータ:**
- `df` (pd.DataFrame) - ダウンロードする DataFrame
- `filename` (str | None) - ファイル名（default: 自動生成）

**戻り値:** None

**環境依存:**
- 実際のブラウザダウンロード処理は Google Colab 実行時のみ有効

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

#### `ga4.classify_source_channel(df, channel_col='channel', medium_col='medium', source_col='source', custom_channels=None)`

source正規化とchannel分類を統合して実行します。sourceとchannelの両列を含むDataFrameを返します。

**パラメータ:**
- `df` (pd.DataFrame) - データフレーム
- `channel_col` (str) - チャネル列名（default: 'channel'）
- `medium_col` (str) - メディア列名（default: 'medium'、存在しない場合は空文字として扱う）
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

#### `text.clean_url(series, unquote=True, drop_query=True, drop_hash=True, lower=True)`

URL Seriesをクリーンアップします。

**パラメータ:**
- `series` (pd.Series) - URL Series
- `unquote` (bool) - URLデコード（default: True）
- `drop_query` (bool) - クエリパラメータを削除（default: True）
- `drop_hash` (bool) - フラグメントを削除（default: True）
- `lower` (bool) - 小文字化（default: True）

**戻り値:** pd.Series

#### `text.normalize_whitespace(series, mode='remove_all')`

Seriesの空白文字を正規化します。

**パラメータ:**
- `series` (pd.Series) - 対象Series
- `mode` (str) - 'remove_all'（すべて削除）または 'collapse'（複数空白を1つに）

**戻り値:** pd.Series

#### `text.force_text_if_numeric(series, prefix="'")`

数値のみの文字列に接頭辞を付けます（Sheets での自動数値変換を防止）。

**パラメータ:**
- `series` (pd.Series) - 対象Series
- `prefix` (str) - 接頭辞（default: `"'"`）

**戻り値:** pd.Series

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

#### `classify.infer_label_by_domain(series, domain_to_label_map, default='不明')`

URL Seriesからドメインを抽出してラベルを推測します。

**パラメータ:**
- `series` (pd.Series) - URL Series
- `domain_to_label_map` (dict) - ドメイン→ラベルのマッピング
- `default` (str) - マッチしない場合のデフォルト値（default: '不明'）

**戻り値:** pd.Series

### Table ユーティリティ

#### `table.ensure_columns(df, columns, fill=None, drop_extra=True)`

指定した列を必ず持つ DataFrame に整形します。

**パラメータ:**
- `df` (pd.DataFrame) - データフレーム
- `columns` (list[str]) - 期待する列のリスト
- `fill` (Any | None) - 追加列の初期値（default: None）
- `drop_extra` (bool) - 余分な列を削除するか（default: True）

**戻り値:** pd.DataFrame

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

#### `table.normalize_thresholds_df(df, min_default=10, max_default=50, ...)`

しきい値 DataFrame を正規化します。

**パラメータ:**
- `df` (pd.DataFrame | None) - しきい値データ
- `min_default` (int) - 最小値のデフォルト（default: 10）
- `max_default` (int) - 最大値のデフォルト（default: 50）

**戻り値:** pd.DataFrame | None

#### `table.dedup_by_key(df, key_cols, prefer_by=None, prefer_ascending=False, keep='first')`

キー列で重複を除去します。

**パラメータ:**
- `df` (pd.DataFrame) - データフレーム
- `key_cols` (list[str]) - キー列名のリスト
- `prefer_by` (str | list[str] | None) - 優先順位を決める列（default: None）
- `prefer_ascending` (bool) - True で最小値を選択、False で最大値を選択（default: False）
- `keep` (str) - 'first' または 'last'（default: 'first'）

**戻り値:** pd.DataFrame

#### `table.group_sum(df, group_cols, sum_cols)`

指定列でグループ化して合計を計算します。

**パラメータ:**
- `df` (pd.DataFrame) - データフレーム
- `group_cols` (list[str]) - グループ化する列
- `sum_cols` (list[str]) - 合計する列

**戻り値:** pd.DataFrame

#### `table.weighted_avg(df, group_cols, value_col, weight_col, out_col=None)`

加重平均を計算します。

**パラメータ:**
- `df` (pd.DataFrame) - データフレーム
- `group_cols` (list[str]) - グループ化する列
- `value_col` (str) - 値列
- `weight_col` (str) - 重み列
- `out_col` (str | None) - 出力列名（default: value_col）

**戻り値:** pd.DataFrame

## 参考資料

- [cheatsheet.md](cheatsheet.md) - クイックリファレンス
- [cookbook.md](cookbook.md) - 実用例集
- [design.md](design.md) - 設計思想
