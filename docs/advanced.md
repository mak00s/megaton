# Advanced Guide (Megaton)

README の補足として、設計思想・認証・使い分け・詳細な機能をまとめます。基本的なワークフローや環境構築は README を参照してください。

**API の詳細は [API リファレンス](api-reference.md) を参照してください。**

---

## 設計思想

- Notebook 上で **最小の操作でデータ取得〜可視化に進める** ことを最優先にしています。
- UI（ウィジェット）で認証・アカウント選択を補助し、コード量を抑える設計です。
- 一方で、**汎用 SDK や本番バッチ基盤の代替は目的ではありません**。

---

## 認証方式と headless/UI の使い分け

### 認証入力の種類

`start.Megaton(credential=...)` に渡せる値は次の通りです。

- **JSON 文字列**（推奨: `.env` の `MEGATON_CREDS_JSON`）
- **dict**
- **JSON ファイルパス**
- **JSON ディレクトリパス**（UI で選択）
- **Base64 JSON 文字列**（内部用途向け／将来変更の可能性あり）

`credential=None` の場合は環境変数 `MEGATON_CREDS_JSON` を参照します。

### OAuth とサービスアカウント

- OAuth とサービスアカウントの両方に対応しています。
- OAuth は **ブラウザ認証＋キャッシュ保存** を前提としています。
- サービスアカウントは headless でも利用できます。

### headless モード

- `headless=True` は UI なしで動作します。
- OAuth 利用時は **既存キャッシュが必須** です（保存場所は環境により異なります）。
- GA4 プロパティや Search Console のサイト選択などを **コードで明示的に指定する必要があります**。

例（headless で GA4 プロパティを選択）:

```python
from megaton.start import Megaton

app = Megaton(None, headless=True)
ga4 = app.ga["4"]
ga4.account.select(ga4.accounts[0]["id"])
ga4.property.select(ga4.account.properties[0]["id"])
```

### UI モード

- `headless=False` で UI が表示されます。
- ウィジェットを使って GA4 アカウント / プロパティや Search Console サイトを選択できます。

---

## GA4 の詳しい使い方

`report` サブモジュールは GA4 レポートの期間設定、実行、前処理、保存などを管理します。

### 期間の設定

まず日付範囲または月次ウィンドウを設定します。

- **日付を直接指定:** `mg.report.set.dates(date_from, date_to)`

  ```python
  mg.report.set.dates("2024-01-01", "2024-01-31")
  # 設定後は mg.report.start_date と mg.report.end_date に datetime.date オブジェクトが格納されます
  ```

- **月次ウィンドウ:** `mg.report.set.months(ago=1, window_months=13, tz="Asia/Tokyo", min_ymd=None)`

  ```python
  # 直近 13 か月と前年同月を対象に設定
  p = mg.report.set.months(ago=1, window_months=13)
  print(mg.report.window)  # {'ago': 1, 'window_months': 13, 'ym': '202501'}
  
  # DateWindow から複数の日付フォーマットにアクセス
  print(f"期間: {p.start_iso}〜{p.end_iso}")  # ISO 8601形式
  table_from = p.start_ymd  # BigQuery用のYYYYMMDD形式
  table_to = p.end_ymd
  month_label = p.start_ym  # レポート用のYYYYMM形式
  
  # BigQuery のテーブル範囲に最小制約を適用
  p = mg.report.set.months(ago=1, window_months=13, min_ymd="20240601")
  # start_ymd が "20240601" より前なら "20240601" にクランプされる
  ```

  **DateWindow の利点:**
  - 手動での日付フォーマット変換（`.replace('-', '')`）が不要
  - BigQuery の `_TABLE_SUFFIX BETWEEN` で使う YYYYMMDD 形式を直接取得
  - レポートの月ラベル（YYYYMM）を `pd.to_datetime().strftime()` なしで取得
  - `min_ymd` パラメータで開始日の制約を自動適用
  - 後方互換性のため最初の3要素（start_iso, end_iso, start_ym）でタプルアンパッキング可能
  
  `mg.report.window` には `'ym'` キーとして対象月（YYYYMM）が格納されます。

### レポートの実行

`report.run()` では次元 (`d`) や指標 (`m`) を列挙し、必要に応じてフィルタやソートを指定します。

```python
mg.report.run(
    d=[("date", "日付"), ("eventName", "イベント名")],
    m=[("eventCount", "イベント数")],
    filter_d="eventName==page_view;country==Japan",
    sort="-eventCount",
)
```

- `d` / `m` には GA4 API の **api_name** あるいは表示名を指定できます。タプル形式 `(元の名前, 新しい列名)` を使うと DataFrame の列名を変更しながら取得できます。
- `filter_d` や `filter_m` は `;` 区切りで AND 条件を指定するシンプルな文字列フィルタです。
- `sort` は `-eventCount` のようにマイナス記号で降順を表します。先頭に `+` を付けるか省略すると昇順になります。
- 結果は `mg.report.data` に保存されます。

### 複数サイトの一括取得（run.all）

`report.run.all()` を使うと、複数の GA4 プロパティを一括取得して結合できます。  
サイトごとにメトリクス名が異なる場合は `site.<key>` を使って動的に指定できます。

```python
sites = [
    {"clinic": "札幌", "ga4_property_id": "12345", "cv": "totalPurchasers"},
    {"clinic": "仙台", "ga4_property_id": "67890", "cv": "keyEvents"},
]

df = mg.report.run.all(
    sites,
    d=[("yearMonth", "month")],
    m=[("activeUsers", "users"), ("site.cv", "cv")],
    item_key="clinic",
)
```

#### メトリクス別 filter_d 指定（v0.8.0+）

メトリクスごとに異なる `filter_d` を指定できます。タプル形式の3番目の要素にオプション辞書を渡します：

```python
df = mg.report.run.all(
    sites,
    d=[("yearMonth", "month"), ("landingPage", "page")],
    m=[
        ("activeUsers", "users", {"filter_d": "sessionDefaultChannelGroup==Organic Search"}),
        ("totalPurchasers", "cv", {"filter_d": "defaultChannelGroup==Organic Search"}),
    ],
    item_key="clinic",
)
```

これにより、同じディメンションで異なるフィルタ条件のメトリクスを1回の呼び出しで取得できます。  
内部では filter_d ごとにグループ化して API コールを行い、ディメンション列で自動結合します。  
同一の filter_d を持つメトリクスは1回の API コールにまとめられるため効率的です。

**注意点:**
- 現在サポートされているオプションは `filter_d` のみです（`filter_m` 等は未サポート）
- グローバル `filter_d` と併用可能（メトリクス別設定が優先されます）

### データ前処理

`report.prep(conf, df?)` を使うと取得した DataFrame の列名変更や型変換など簡易的な前処理を行えます。

```python
# 日付列を文字列に変換し、イベント数を整数に変換する例
conf = {
    "eventCount": {"name": "イベント数", "type": int},
    "date": {"type": str},
}
mg.report.prep(conf)
```

前処理結果は `mg.report.data` に反映されます。

### レポート期間の書き出し

`mg.report.dates.to.sheet(sheet, start_cell, end_cell)` を使うと、設定済みの開始日と終了日を Google Sheets に書き出せます。複数レポートの期間管理やドキュメント作成に便利です。

```python
mg.open.sheet("https://docs.google.com/spreadsheets/d/xxxxx")
mg.report.set.dates("2024-01-01", "2024-01-31")
# "A1" に開始日を、"B1" に終了日を書き込み
mg.report.dates.to.sheet("_meta", "A1", "B1")
```

### DataFrame の保存・ダウンロード

Megaton はローカルやノートブック環境への保存にも対応しています。

- `mg.save_df(df, "report.csv")` – DataFrame を CSV ファイルとしてローカルに保存します。
- `mg.download(df, "report.xlsx")` – DataFrame を Excel 形式などでダウンロードしてノートブックから取得できます。

---

## Search Console の詳しい使い方

最新バージョンでは `mg.search` が Search Console API を扱う正式なインタフェースであり、`mg.sc` は互換性のための短いエイリアスです。

### サイトの取得と選択

初回アクセス時に `mg.search.sites` でアクセス可能なサイト一覧を取得します。常に最新の一覧が必要な場合は `mg.search.get.sites()` を使用します。

```python
sites = mg.search.sites  # 初回アクセスで一覧取得
# または sites = mg.search.get.sites()  # 強制的に再取得

# 任意のサイトを選択（プロパティ URL を指定）
mg.search.use("https://example.com/")

selected_site = mg.search.site  # 選択されたサイト情報
```

### 期間の設定

Search Console API でも GA4 と同様に日付範囲や月次ウィンドウを設定します。

```python
# 日付範囲を設定
mg.search.set.dates("2023-12-01", "2023-12-31")

# 直近3ヶ月を対象とするウィンドウを設定
mg.search.set.months(ago=0, window_months=3, tz="Asia/Tokyo")
```

### クエリの実行

`mg.search.run(dimensions, metrics, limit=5000, **kwargs)` でパフォーマンスデータを取得します。結果は `mg.search.data` に格納され、`SearchResult` を返します（DataFrame は `.df` で取得できます）。

```python
result = mg.search.run(
    dimensions=["date", "query", "page"],
    metrics=["clicks", "impressions", "ctr", "position"],
    limit=10000,
    sort="-clicks",
)
df_sc = result.df
```

`dimension_filter` を指定すると Search Console 側で絞り込みできます（AND 条件のみ）。

```python
df_sc = mg.search.run(
    dimensions=["query", "page"],
    metrics=["clicks", "impressions"],
    dimension_filter="page=~^/blog/;query=@ortho",  # RE2 正規表現 + 部分一致
)
```

`clean=True` を指定すると URL 正規化（decode + ? 削除 + # 削除 + 小文字化）を実行し、正規化後の値で集計します。

```python
result = mg.search.run(
    dimensions=["query", "page"],
    metrics=["clicks", "impressions", "position"],
    clean=True,
)
df_sc = result.df
```

### SearchResult のメソッドチェーン

`SearchResult` は URL 正規化や分類、フィルタをチェーンできます。

```python
result = mg.search.run(
    dimensions=["query", "page"],
    metrics=["clicks", "impressions", "position"],
)

df_sc = (
    result
    .decode(group=False)
    .remove_params(group=False)
    .remove_fragment(group=False)
    .lower(group=True)
).df
```

### 設定シート（Config）の拡張

最新版では Config シートにサイト単位のフィルタ閾値を含めることができます。主に以下の列を想定しています。

- `gsc_site_url`: Search Console のサイト URL（例: `https://example.com/`）。`mg.search.use(site['gsc_site_url'])` で自動選択できます。
- `min_pv`: （任意）ページビューの最小値。Search Console 以外の集計列 `pv` がある場合に適用されます。
- `min_cv`: （任意）コンバージョン数の最小値。DataFrame に `cv` 列がある場合に適用されます。

また `min_impressions`、`max_position` は既存と同様にサイト行に含めるようになり、これらの閾値はノートブック側で直接参照せず `mg.search.filter_by_thresholds(df, site)` を呼ぶことで一括適用できます。

`filter_by_thresholds()` には `clicks_zero_only` パラメータを指定できます。`clicks_zero_only=True` を渡すと、クリック数が 0 の行にのみ閾値を適用し、クリック数が 1 以上の行は閾値に関わらず保持されます。これは従来の動作（クリックがあるキーワードは無条件に残す）を再現したい場合に便利です。

```python
# デフォルト: 全行に閾値を適用
filtered = mg.search.filter_by_thresholds(df, site)

# 旧動作: クリック数 > 0 の行は無条件に残し、クリック数 = 0 の行のみ閾値で除外
filtered = mg.search.filter_by_thresholds(df, site, clicks_zero_only=True)
```

`thresholds_df` は非推奨となり、閾値は各 `site` レコード内で管理してください。

- `dimensions` は最大 5 つまで指定できます。
- `dimensions` は `date/hour/country/device/page/query` から選択できます。`month` を指定すると内部的に `date` で取得し、結果は月単位に集計されます。
- `metrics` を省略するとデフォルトで `["clicks", "impressions", "ctr", "position"]` が使用されます。
- `limit` は API の既定上限を変更しますが、大きくすると応答時間が長くなることがあります。

取得したデータは `mg.save.to.sheet()` や `mg.append.to.sheet()` を使って Google Sheets に保存できます。

---

## Google Sheets の詳しい使い方

Sheets 連携ではワークシートの作成・選択・更新から DataFrame の保存・追記・アップサートまで多くの操作が提供されています。

### スプレッドシートを開く・シートを選択する

```python
# スプレッドシートを開く
mg.open.sheet("https://docs.google.com/spreadsheets/d/xxxxx")

# ワークシートを明示的に選択（URL だけでは自動選択されません）
mg.sheets.select("データSheet")

# 新しいシートを作成
mg.sheets.create("集計結果")

# シートを削除
mg.sheets.delete("旧データ")
```

### データの保存方法

- **上書き保存:** `mg.save.to.sheet(sheet_name, df)` – 指定したシートを DataFrame で丸ごと上書きします。
- **追記:** `mg.append.to.sheet(sheet_name, df)` – 既存データの末尾に DataFrame を追記します。
- **アップサート:** `mg.upsert.to.sheet(sheet_name, df, keys, columns?, sort_by?)` – キー列を基準に既存行を更新し、新規行を追加します。`columns` で更新対象列を限定できます。

```python
# 例: ID 列を基準にアップサート
mg.upsert.to.sheet("_ga_data", df, keys=["id"], columns=["eventCount"])
```

### 現在のシートに対する操作

`mg.sheet` は現在選択中のワークシートを表します。以下のような便利メソッドがあります。

```python
# DataFrame を上書き保存
mg.sheet.save(df)

# DataFrame を追記
mg.sheet.append(df)

# キーを指定してアップサート
mg.sheet.upsert(df, keys=["id"])

# 単一セルの更新
mg.sheet.cell.set("A1", "Hello World")

# 範囲への配列書き込み
mg.sheet.range.set("B2:D4", [[1, 2, 3], [4, 5, 6], [7, 8, 9]])

# 現在のシートのデータを DataFrame として取得
import pandas as pd
df_sheet = pd.DataFrame(mg.sheet.data)
```

### 補助機能

- `mg.sheet.clear()` – 現在のシートの内容を全てクリアします。
- `mg.sheet.data` – シートの内容を `list of dict` 形式で取得します。
- `mg.load.cell(row, col)` – 指定したセルの値を読み込みます。

---

## BigQuery の詳しい使い方

Megaton では BigQuery サービスの起動からテーブルのクエリ実行、GA4 イベントエクスポートテーブルのフラット化までサポートします。

### サービスの起動とクエリの実行

```python
# BigQuery サービスを起動
bq = mg.launch_bigquery("my-gcp-project")

# SQL を実行して DataFrame を取得
df_bq = bq.run("SELECT COUNT(*) AS total FROM `my_dataset.my_table`", to_dataframe=True)
```

### データセットの選択と GA4 エクスポートのフラット化

```python
# データセットを選択
bq.dataset.select("analytics_123456")

# イベントテーブルをフラット化して結果を直接取得（期間は短く指定）
df_events = bq.ga4.flatten_events("20240101", "20240107")

# フラット化に使われる SQL を取得だけしたい場合
sql = bq.ga4.get_query_to_flatten_events("20240101", "20240107")
print(sql[:400])
```

注意点:

- `flatten_events()` は実際にクエリを実行するため、期間を長くすると処理時間が長くなります。
- `get_query_to_flatten_events()` は SQL ステートメントを返すだけで実行はしません。クエリを確認したい場合に利用します。

---

## 表示・ユーティリティ機能

Megaton にはデータの確認やファイル操作を補助する機能が用意されています。

- `mg.show.ga.dimensions` / `mg.show.ga.metrics` / `mg.show.ga.properties` – GA4 の次元や指標、プロパティの一覧を表示します。
- `mg.show.table(df, rows=10, include_index=False)` – DataFrame を表形式で整形表示します。
- `mg.load.csv(path)` – CSV ファイルを読み込み DataFrame を返します。
- `mg.save_df(df, filename, mode="w", include_dates=True)` – DataFrame をローカルファイルとして保存します（既定は CSV）。
- `mg.download(df, filename)` – Notebook からファイルをダウンロードします。

---

## test-megaton.ipynb の位置づけ

- Megaton の機能をまとめてテストするための **手動 E2E スモークノートブック** です。外部サービスへのアクセスを伴うため、事前に `.env` で `MEGATON_CREDS_JSON` を渡しておく必要があります。
- 設定がないサービスは `Skip` され、必要な API だけが実行されます。
- Sheets への出力は `_smoke_YYYYMMDD_HHMM` のような新規シートに書き込み、既存シートは上書きしません。

---

## よくあるハマりどころ

- **`MEGATON_CREDS_JSON` が未設定**  
  `.env` ファイルを作成し、JSON 文字列を 1 行で貼り付けてください。
- **OAuth で headless が失敗**  
  先に UI モードで認証し、キャッシュを作成してください。
- **GA4 の display_name / api_name 混在**  
  `mg.show.ga.dimensions` / `mg.show.ga.metrics` で一覧を確認し、表記揺れを避けてください。
- **`if df:` がエラー**  
  pandas の DataFrame は真偽値を持たないため `df.empty` を使って空判定を行います。
- **Colab で依存関係が足りない**  
  `MEGATON_AUTO_INSTALL=1` を設定すると不足している依存ライブラリを自動インストールします。
