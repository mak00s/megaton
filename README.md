# megaton

Megaton は Google Analytics 4、Google Search Console、Google Sheets、BigQuery を **Jupyter/Colab ノートブックから簡潔に操作するためのツール** です。Notebook 上で分析・配布（GA → SC → Sheets 等）を高速に実施でき、汎用 SDK の代替ではなく Notebook 向けの UX に特化しています。

## 目次

1. [インストール](#インストール)
2. [クイックスタート](#クイックスタート)
3. [GA4 の使い方](#ga4-の使い方)
4. [Google Sheets の使い方](#google-sheets-の使い方)
5. [Search Console の使い方](#search-console-の使い方)
6. [BigQuery の使い方](#bigquery-の使い方)
7. [チートシート](#チートシート)
8. [レガシー互換性](#レガシー互換性)
9. [ライセンス](#ライセンス)

## インストール

pip で GitHub リポジトリから直接インストールできます。

```bash
pip install git+https://github.com/mak00s/megaton.git
```

Google サービスにアクセスするために **サービスアカウント JSON** または OAuth 認証情報が必要です。最も簡単な方法はサービスアカウント JSON を用意し、初期化時に渡すことです（詳細は [クイックスタート](#クイックスタート) を参照）。

## クイックスタート

1. `megaton` をインポートし、サービスアカウント JSON のパスまたは JSON 文字列を渡します。

   ```python
   from megaton import start
   mg = start.Megaton("/path/to/service_account.json")
   ```

   環境変数 `MEGATON_CREDS_JSON` にパスを設定するか、JSON 文字列をそのまま渡すこともできます。

2. レポート期間を設定し、GA4 データを取得して Sheets に保存する例：

   ```python
   # 期間設定
   mg.report.set.dates("2024-01-01", "2024-01-31")

   # GA4 レポート実行
   df = mg.report.run(
       d=["date", "eventName"],
       m=["eventCount"],
   )

   # スプレッドシートを開いて保存
   mg.open.sheet("https://docs.google.com/spreadsheets/d/xxxxx")
   mg.save.to.sheet("_ga_data", df)
   ```

## GA4 の使い方

Megaton の GA4 インタフェースでは、期間の設定からレポート実行、データの前処理までを数行で行えます。

- **期間の指定:** `mg.report.set.dates(start_date, end_date)` で日付範囲を設定します。省略すると直近7日間（前日まで）が自動で選択されます。
- **月次ウィンドウ:** `mg.report.set.months(ago=1, window_months=13)` を使うと、前年同月比など月単位のウィンドウをまとめて設定できます。
- **レポート実行:** `mg.report.run(d=[...], m=[...], limit=N)` で GA4 データを取得し、結果は `mg.report.data` に格納されます。
- **前処理:** `mg.report.prep(conf)` を使えば列名の変更や型変換など簡易的なデータ整形が可能です。

## Google Sheets の使い方

Megaton には Sheets 連携が組み込まれており、データの保存、追記、アップサートが容易です。

- **シートを開く:** `mg.open.sheet(spreadsheet_url)` でスプレッドシートを開きます。ワークシートを選択する場合は、`mg.sheets.select(name)` で明示的にシート名を指定します（URL だけでは自動的に選択されません）。
- **上書き保存:** `mg.save.to.sheet(name, df)` で DataFrame をシートに保存します。
- **追記:** `mg.append.to.sheet(name, df)` で既存データの末尾に追記します。
- **アップサート:** `mg.upsert.to.sheet(name, df, keys=[...])` でキー列をもとに更新・追加します。
- **現在のシート:** `mg.sheet` で現在のシートにアクセスし、`mg.sheet.save(df)` や `mg.sheet.cell.set()` 等の便利メソッドを使用できます。

## Search Console の使い方

最新バージョンでは `mg.search` が Search Console への正式インタフェースになりました（`mg.sc` は短いエイリアスです）。

- **サイト一覧:** `mg.search.sites` でアクセス可能なプロパティ一覧を取得します。`mg.search.get.sites()` を呼ぶとリストを再取得します。
- **プロパティ選択:** `mg.search.use(site_url)` で対象サイトを選択します。
- **期間設定:** `mg.search.set.dates(...)` または `mg.search.set.months(...)` で期間を設定します。
- **データ取得:** `mg.search.run(dimensions=[...], metrics=[...], limit=5000)` でパフォーマンスデータを取得し、結果は `mg.search.data` に格納されます。
- **Sheets への保存:** GA4 と同様に `mg.save.to.sheet()` や `mg.append.to.sheet()` を用いて結果を Sheets に保存できます。

## BigQuery の使い方

BigQuery の SQL を実行するには次のようにします。

```python
bq = mg.launch_bigquery("my-gcp-project")
df_bq = bq.run("SELECT 1 AS test", to_dataframe=True)
```

取得した結果は `pandas.DataFrame` で返されるため、そのまま分析や可視化に利用できます。

## チートシート

利用可能なメソッドの詳しい一覧は別ファイル [CHEATSHEET.md](CHEATSHEET.md) を参照してください。

## レガシー互換性

以前のノートブックとの互換性のために `mg.gs` という古い Sheets クライアントが残されています。新規ノートブックでは `mg.sheets` / `mg.sheet` と `mg.save/append/upsert.to.sheet()` の利用を推奨します。

## ライセンス

MIT License