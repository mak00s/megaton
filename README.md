# megaton

Megaton は Google アナリティクス（GA4／GA3）、Google Sheets、BigQuery を Notebook 上から扱うためのツール群です。認証やアカウント選択の UI を用意しつつ、Notebook から各サービスを横断できます。

## できること

- **認証の自動化**: OAuth クライアントやサービスアカウントの JSON を読み込み、Notebook 上で安全に接続します。
- **Google アナリティクス連携**: GA4／Universal Analytics のアカウント・プロパティ（GA3 はビュー）を UI で選択し、レポート取得 API を提供します。GA3 はサンセット済みのため利用は推奨しません。
- **スプレッドシート・BigQuery とのやり取り**: Google Sheets への保存・追記、BigQuery クライアント操作や GA4 変換ユーティリティを提供します。
- **データ整形ユーティリティ**: DataFrame の日付型変換、URL クエリの整理、列名変更など、分析前の処理を支援する関数を提供します。
- **Search Console 連携**: Search Console クライアントを初期化して API を利用できます。
- **Google Drive 連携**: Colab 上で Drive をマウントし、`/nbs` にアクセスできるようにします。

## 代表的なユースケース

- GA レポートを定期的に取得し、Google Sheets へ配布するテンプレートを作成する。
- BigQuery に蓄積したデータや GA4 の生データを Notebook で確認しながら可視化する。
- マーケティングチーム向けに、アカウント選択から指標出力までを Notebook 上の手順として整備する。
- 既存の GA3 プロパティと GA4 プロパティを比較するアドホック分析を効率化する。

## 動作に必要なもの

1. Python 3.9 以上が動作する環境（Jupyter Notebook／JupyterLab／Google Colab など）。
2. Google Cloud Console で有効化した API と、対応する認証情報（クライアントシークレット JSON またはサービスアカウント JSON）。
3. （任意）Google Sheets や BigQuery を利用する場合、該当サービスの API 有効化と権限付与。

## インストール

配布パッケージは PyPI には未公開です。以下のコマンドで依存ライブラリと本体を GitHub から直接インストールできます。

```bash
pip install -r https://raw.githubusercontent.com/mak00s/megaton/main/requirements.txt
pip install -U "git+https://github.com/mak00s/megaton"
```

## はじめ方

1. 認証 JSON を用意し、Notebook からアクセスできる場所に配置します。
2. Notebook で `Megaton` クラスを読み込みます。

```python
from megaton.start import Megaton

app = Megaton("/path/to/credentials", use_ga3=True)
# バッチなど UI 不要の場合
app_headless = Megaton("/path/to/credentials", headless=True)
```

- 引数にはディレクトリパス・サービスアカウント JSON のファイルパス・JSON 文字列のいずれか 1 つを指定できます。
- 何も渡さない場合は `MEGATON_CREDS_JSON` 環境変数を参照します。
- `headless=True` を指定するとウィジェット UI を表示せずに実行します。OAuth は既存の認証キャッシュが必要です。サービスアカウントでの認証は `headless=True` でも利用できます。必要な場合は `megaton.mount_google_drive()` で手動マウントしてください。
- `use_ga3=True` を指定すると GA3 のウィジェットも有効化されます（GA3 はサンセット済み）。

3. 表示されたウィジェットで認証を完了し、アカウント／プロパティ（GA3 はビュー）を選択します。
4. 取得したデータはテーブル表示で確認でき、必要に応じて CSV・Google Sheets へ保存できます。BigQuery はクライアント経由で操作してください。

## 使い方の例

### GA4 レポート

```python
# UI で GA4 プロパティを選択したあと
app.report.set_dates("2024-01-01", "2024-01-31")
df = app.report.run(
    d=["date", "eventName"],
    m=["eventCount"],
)
```

```python
# 利用可能なディメンション/指標を一覧表示
app.show.ga.dimensions
app.show.ga.metrics
```

```python
# フィルタとソートの例（ディメンション/指標の名称は GA4 の api_name を使用）
df = app.report.run(
    d=["date", "eventName"],
    m=["eventCount"],
    filter_d="eventName==page_view",
    filter_m="eventCount>10",
    sort="-date",
)
```

```python
# 取得したレポートを保存
app.report.to.csv("ga4_report")  # CSV 保存（期間付き）
app.open.sheet("https://docs.google.com/spreadsheets/d/xxxxx")
app.report.to.sheet("Sheet1")
```

### Search Console

```python
sc = app.launch_sc("https://example.com/")
# sc.client は googleapiclient のクライアントです
response = sc.client.searchanalytics().query(
    siteUrl=sc.site_url,
    body={
        "startDate": "2024-01-01",
        "endDate": "2024-01-31",
        "dimensions": ["query"],
        "rowLimit": 10,
    },
).execute()
```

### BigQuery

```python
bq = app.launch_bigquery("my-gcp-project")
df = bq.run("SELECT 1 AS test", to_dataframe=True)

# GA4 のエクスポート用データセットを選択してからユーティリティを使用します
bq.dataset.select("analytics_123456")
df_events = bq.ga4.flatten_events("20240101", "20240107", to="dataframe")
```

### Google Sheets

```python
app.open.sheet("https://docs.google.com/spreadsheets/d/xxxxx")
app.save.to.sheet("Sheet1", df)    # 上書き保存
app.append.to.sheet("Sheet1", df)  # 追記
```

## 困ったときは

- **認証が通らない**: JSON がサービスアカウントか OAuth クライアントかを確認し、必要なスコープが有効になっているかをチェックしてください。
- **API が見つからないと言われる**: `requirements.txt` を再インストールし、Google Cloud Console 側で API を有効化します。
- **ウィジェットが表示されない**: Notebook／ブラウザを再起動し、ブラウザ拡張によるブロックがないか確認してください。

## ライセンス

MIT License
