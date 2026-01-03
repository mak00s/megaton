
# Megaton Cheat Sheet

このファイルは、Jupyter/Colab ノートブックから利用するための **Megaton の API 一覧** です。`?` は引数が任意であることを示します。`df?` を省略すると `mg.report.data` が既定値として使われます。

**詳細な説明（パラメータ、戻り値、例）は [API リファレンス](api-reference.md) を参照してください。**

## コア / フロー

- `mg = start.Megaton(creds)` – 認証情報を渡して Megaton オブジェクトを作成します。
- `mg.open.sheet(url)` – スプレッドシートを開きます。
- `mg.launch_bigquery(project)` – BigQuery サービスを起動します。

## レポート（GA4）

- `mg.report.set.months(ago, window_months, tz?, now?, min_ymd?)` – 月単位のウィンドウを設定し、`DateWindow` を返します。
  - 戻り値: `DateWindow(start_iso, end_iso, start_ym, end_ym, start_ymd, end_ymd)`
  - `p.start_iso` / `p.end_iso` – ISO 8601形式（YYYY-MM-DD）
  - `p.start_ym` / `p.end_ym` – 年月形式（YYYYMM）
  - `p.start_ymd` / `p.end_ymd` – コンパクト形式（YYYYMMDD、BigQuery用）
  - `min_ymd` – 開始日の最小制約（YYYYMMDD形式）
- `mg.report.set.dates(date_from, date_to)` – 日付範囲を直接設定します。
- `mg.report.run(d, m, filter_d?, filter_m?, sort?, **kwargs)` – レポートを実行します。
- `mg.report.run.all(items, d, m, item_key?, property_key?, item_filter?, verbose?, **kwargs)` – 複数アイテムのレポートを一括実行して結合します。
  - `items` – アイテム設定のリスト（dict の list）
  - `item_key='site'` – アイテム識別子キー（デフォルト: 'site'）
  - `property_key='ga4_property_id'` – GA4プロパティIDキー
  - `item_filter=None` – アイテムフィルタ（リスト or 関数）
  - `d` の3要素タプルに `{'absolute': True}` を指定すると、`item['url']` のドメインで相対パスを絶対URLに変換します。
  - `m` に `site.<key>` を指定すると、`item[<key>]` をメトリクスとして使用します。
- `mg.report.start_date` / `mg.report.end_date` – 設定された開始日・終了日。
- `mg.report.data` – 直近のレポート結果。
- `mg.report.prep(conf, df?)` – 列名変更や値置換などの前処理を行います。
- `mg.report.window["ym"]` – 月のウィンドウ情報（`YYYYMM`）。
- `mg.report.dates` – 設定された日付範囲オブジェクト。
- `mg.report.dates.to.sheet(sheet, start_cell, end_cell)` – レポート期間をシートに書き込みます。

## Sheets（シート名で指定）

- `mg.save.to.sheet(name, df?)` – シートを DataFrame で上書き保存します。
- `mg.append.to.sheet(name, df?)` – 既存データの末尾に追記します。
- `mg.upsert.to.sheet(name, df?, keys, columns?, sort_by?)` – キー列を基準にアップサートします。

## Sheets（コレクション／現在のシート）

- `mg.sheets.select(name)` – シートを選択します。
- `mg.sheets.create(name)` – 新しいシートを作成します。
- `mg.sheets.delete(name)` – シートを削除します。
- `mg.sheet.clear()` – 現在のシートをクリアします。
- `mg.sheet.data` – 現在のシートのデータ（list of dict）。
- `mg.sheet.cell.set(cell, value)` – 単一セルに値を書き込みます。
- `mg.sheet.range.set(a1_range, values)` – 範囲に対して配列を書き込みます。
- `mg.sheet.save(df?)` – 現在のシートに DataFrame を保存します。
- `mg.sheet.append(df?)` – 現在のシートに追記します。
- `mg.sheet.upsert(df?, keys, columns?, sort_by?)` – 現在のシートに対してアップサートします。

## Search Console

- `mg.search.sites` – アクセス可能なサイト一覧（初回アクセスで取得）。
- `mg.search.get.sites()` – サイト一覧を更新して再取得します。
- `mg.search.use(site_url)` – 指定したサイトを選択します。
- `mg.search.set.dates(date_from, date_to)` – 日付範囲を設定します。
- `mg.search.set.months(ago, window_months, tz?, now?, min_ymd?)` – 月単位のウィンドウを設定し、`DateWindow` を返します。
- `mg.search.run(dimensions, metrics?, limit?, **kwargs)` – クエリを実行し、`SearchResult` を返します（`.df` で DataFrame にアクセス）。
  - `dimensions` は `date/hour/country/device/page/query` から選択できます。`month` を指定すると月単位に集計されます。
  - `clean=True` を指定すると URL 正規化と集計を自動実行します。
  - `dimension_filter="page=~^/blog/;query=@ortho"` のように指定すると AND 条件で絞り込みできます（`=~`/`!~` は RE2 正規表現、`=@`/`!@` は部分一致）。
  - **メソッドチェーン:** `.decode()`, `.classify()`, `.filter_*()` などで処理を連鎖できます。詳しくは `docs/searchresult-api.md` を参照。
- `mg.search.run.all(items, dimensions, metrics?, item_key?, site_url_key?, item_filter?, verbose?, **kwargs)` – 複数アイテムのクエリを一括実行して結合し、`SearchResult` を返します。
  - `items` – アイテム設定のリスト（dict の list）
  - `item_key='site'` – アイテム識別子キー（デフォルト: 'site'）。**自動的に dimensions に追加されます。**
  - `site_url_key='gsc_site_url'` – GSCサイトURLキー（空の場合はスキップ）
  - `item_filter=None` – アイテムフィルタ（リスト or 関数）
  - `dimension_filter` – `mg.search.run()` と同様のフィルタを指定できます（AND 条件のみ）。
- `mg.search.data` – 直近の Search Console 結果。
- `mg.search.filter_by_thresholds(df, site, clicks_zero_only=False)` – サイト設定の閾値を適用してフィルタリングします。
  - `clicks_zero_only=True` を指定すると、クリック数 > 0 の行は無条件に保持し、クリック数 = 0 の行のみ閾値でフィルタリングします。

## 表示

- `mg.show.ga.dimensions` – GA4 の次元一覧を表示します。
- `mg.show.ga.metrics` – GA4 の指標一覧を表示します。
- `mg.show.ga.properties` – GA4 プロパティ一覧を表示します。
- `mg.show.table(df, rows=10, include_index=False)` – DataFrame を表形式で表示します。

## 読み込み／ダウンロード

- `mg.load.csv(path)` – CSV を読み込んで DataFrame を返します。
- `mg.load.cell(row, col, what?)` – セルの値を読み込みます。
- `mg.save_df(df, filename, mode='w', include_dates=True)` – DataFrame をローカルファイルに保存します。
- `mg.download(df, filename?)` – Notebook からファイルをダウンロードします。

## BigQuery

- `mg.launch_bigquery(project)` – BigQuery サービスを起動します。
- `bq.run(sql, to_dataframe=True)` – SQL クエリを実行して DataFrame として返します。
