# megaton

Megaton は Jupyter Notebook／Colab 上で Google Analytics（主に GA4）、Google Sheets、BigQuery、Search Console を扱うためのツール群です。認証やアカウント選択を UI で補助し、レポート取得と前処理を Notebook 内で素早く回せることを重視しています。

## 概要（これは何か／何ではないか）

**これは何か**
- Notebook 向けの GA4 レポート取得ユーティリティと周辺機能（Sheets/BigQuery/SC）の薄いラッパー
- OAuth／サービスアカウント JSON を前提とした認証補助
- データ取得後の軽い前処理（列名変更・型変換・置換）

**これは何ではないか**
- CLI ツールや Web ダッシュボードではありません
- ETL/スケジューリング基盤ではありません
- GA4 の UI を代替する完全なノーコード環境ではありません

## できること

- **認証の自動化**: OAuth クライアントやサービスアカウントの JSON を読み込み、Notebook 上で接続します
- **GA4 レポート取得**: アカウント／プロパティを UI で選択し、レポート API を実行します
- **Google Sheets 連携**: DataFrame の保存・追記を簡単に実行できます
- **BigQuery 連携**: クライアント操作と GA4 エクスポート用ユーティリティを提供します
- **Search Console 連携**: API クライアントを初期化できます

> GA3（Universal Analytics）はサンセット済みです。`use_ga3=True` でコード上は有効化できますが、利用は推奨しません。

## インストール

PyPI には未公開です。以下のコマンドで依存ライブラリと本体を GitHub から直接インストールできます。

```bash
pip install -r https://raw.githubusercontent.com/mak00s/megaton/main/requirements.txt
pip install -U "git+https://github.com/mak00s/megaton"
```

対応 Python: 3.9 以上

## クイックスタート（最小動作）

1) 認証 JSON を用意（OAuth クライアント or サービスアカウント）
2) Notebook で `Megaton` を起動
3) ウィジェットで GA4 プロパティを選択
4) レポート取得

```python
from megaton.start import Megaton

mg = Megaton("/path/to/credentials.json")

# GA4 プロパティ選択後
mg.report.set_dates("2024-01-01", "2024-01-31")
mg.report.run(
    d=["date", "eventName"],
    m=["eventCount"],
)

# 取得データは report.data に入ります
df = mg.report.data
print(df.head())
```

CSV へ保存:

```python
mg.report.to.csv("ga4_report")
```

## 基本の使い方（GA4）

### `mg.report.run()` の基本

`mg.report.run()` は GA4/GA3 のレポートを実行するラッパーです。取得データは `mg.report.data` に格納されます。

```python
mg.report.run(
    d=["date", "sessionSource", "sessionMedium"],
    m=["sessions"],
)
```

**d（dimensions）/ m（metrics）の形式**
- **文字列**: GA4 の `api_name` または UI 表示名
- **タプル**: `(name, "new_name")` で列名を後段でリネーム

```python
mg.report.run(
    d=[("landingPagePlusQueryString", "landing_page")],
    m=[("eventCount", "events")],
)
```

```python
# 例: sessions を entrances という列名にしたい場合
mg.report.run(
    d=["date"],
    m=[("sessions", "entrances")],
)
```

### 期間指定

```python
mg.report.set_dates("2024-01-01", "2024-01-31")
```

`YYYY-MM-DD` の他、`NdaysAgo`／`yesterday`／`today` の形式も使用できます。

### フィルタとソート

`filter_d` と `filter_m` は GA3 互換の文字列フィルタ形式です。複数条件は `;` で AND 連結されます。

- 文字列（ディメンション）: `==`, `!=`, `=@`（含む）, `!@`, `=~`（正規表現）, `!~`
- 数値（メトリクス）: `>`, `>=`, `<`, `<=`, `==`, `!=`

```python
mg.report.run(
    d=["date", "eventName"],
    m=["eventCount"],
    filter_d="eventName==page_view",
    filter_m="eventCount>10",
    sort="-eventCount,date",
)
```

### GA4 の典型的なクエリ例

**ランディングページ別セッション**

```python
mg.report.run(
    d=["landingPagePlusQueryString"],
    m=["sessions"],
)
```

**流入元（source/medium）**

```python
mg.report.run(
    d=["sessionSource", "sessionMedium"],
    m=["sessions"],
)
```

**イベント名でフィルタ**

```python
mg.report.run(
    d=["date", "eventName"],
    m=["eventCount"],
    filter_d="eventName==page_view",
)
```

### 利用可能なディメンション／指標の確認

```python
# 表示用（DataFrame は report.data に格納されません）
mg.show.ga.dimensions
mg.show.ga.metrics
```

> 実データとして扱いたい場合は `mg.ga["4"].property.dimensions` を `pandas.DataFrame` にする方法もあります。

## 出力 DataFrame の仕様

**列名**
- 既定では `d` と `m` に渡した文字列が列名になります
- タプル指定 `(name, "new_name")` を使うと `new_name` に置き換わります

**型変換（GA4）**
- `date` / `firstSessionDate` は `datetime.date` に変換されます
- `dateHour` / `dateHourMinute` は `datetime` に変換されます
- メトリクスは GA4 の型情報に基づき `int` / `float` に変換されます

**保証されないこと**
- 欠損値（`None` / `NaN`）は GA API 側の返却に依存します
- 型変換は一部の列名のみを対象とし、全ての列に適用されません

> 型変換や列名整理は `mg.report.prep()` で追加実行できます。

```python
conf = {
    "landingPagePlusQueryString": {"name": "landing_page"},
    "eventCount": {"type": "int64"},
}
mg.report.prep(conf)
df = mg.report.data
```

## How it works（アーキテクチャ概要）

```
Megaton (megaton/start.py)
  ├─ auth (megaton/auth.py)
  │    └─ OAuth / Service Account 認証 + キャッシュ保存
  ├─ GA clients
  │    ├─ ga4.MegatonGA4 (megaton/ga4.py)
  │    └─ ga3.MegatonUA  (megaton/ga3.py)
  ├─ Report wrapper
  │    └─ Megaton.Report.run() -> ga*.Report.run()
  │         └─ DataFrame -> utils.prep_df() でリネーム等
  ├─ Sheets (megaton/gsheet.py)
  ├─ BigQuery (megaton/bq.py)
  └─ Search Console (megaton/searchconsole.py)
```

**`mg.report.run()` の実行経路**
- `megaton/start.py::Megaton.Report.run()` がラッパー
- 実際の API 実行は `megaton/ga4.py::MegatonGA4.Report.run()`（GA4）
- 取得後の列名リネームは `megaton/utils.py::prep_df()` で実施
- 追加の前処理は `megaton/start.py::Megaton.Report.prep()` が `utils.prep_df()` を呼び出し

## 設定と認証

**`Megaton` の引数**
- `credential`: JSON の **ディレクトリ** / **ファイルパス** / **JSON 文字列** / **dict**
- `use_ga3`: GA3 を有効化（サンセット済みのため非推奨）
- `cache_key`: OAuth キャッシュのキーを明示したい場合に指定
- `headless`: UI を表示しないモード

**認証の実際の挙動**
- `credential=None` の場合、`MEGATON_CREDS_JSON` を参照
- Colab では `credential=None` の場合 `/nbs`（Drive）を候補にします
- OAuth は `~/.config/cache_*.json` にキャッシュされます
- `headless=True` かつ OAuth の場合は **既存キャッシュが必須** です
- サービスアカウントは `headless=True` でも利用可能です

**GA4/GA3 の選択**
- 認証後に表示されるタブで GA4/GA3 を選択します
- `mg.ga_ver` が現在の選択状態を表し、`mg.report.run()` はこの値に従って実行されます

## 周辺機能（Sheets / BigQuery / Search Console）

### Google Sheets

```python
mg.open.sheet("https://docs.google.com/spreadsheets/d/xxxxx")
mg.save.to.sheet("Sheet1", mg.report.data)    # 上書き保存
mg.append.to.sheet("Sheet1", mg.report.data)  # 追記
```

### BigQuery

```python
bq = mg.launch_bigquery("my-gcp-project")

df = bq.run("SELECT 1 AS test", to_dataframe=True)

# GA4 エクスポート用テーブルに対するユーティリティ
bq.dataset.select("analytics_123456")
flat = bq.ga4.flatten_events("20240101", "20240107", to="dataframe")
```

### Search Console

```python
sc = mg.launch_sc("https://example.com/")
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

## Recipes（実用スニペット）

### 1) 複数メトリクスを順に取得して結合

```python
import pandas as pd

metrics = ["eventCount", "sessions", "activeUsers"]
dfs = []

for metric in metrics:
    mg.report.run(d=["date"], m=[metric])
    df = mg.report.data.copy()
    dfs.append(df)

merged = dfs[0]
for df in dfs[1:]:
    merged = merged.merge(df, on="date", how="outer")
```

### 2) 複数の結果を安全にマージ

```python
mg.report.run(d=["date", "eventName"], m=["eventCount"])
df_events = mg.report.data.copy()

mg.report.run(d=["date"], m=["sessions"])
df_sessions = mg.report.data.copy()

result = df_events.merge(df_sessions, on="date", how="left")
```

### 3) source / medium の正規化

```python
mg.report.run(d=["sessionSource", "sessionMedium"], m=["sessions"])
df = mg.report.data.copy()

df["sessionSource"] = df["sessionSource"].str.lower().fillna("(not set)")
df["sessionMedium"] = df["sessionMedium"].str.lower().fillna("(not set)")
df["source_medium"] = df["sessionSource"] + " / " + df["sessionMedium"]
```

## Troubleshooting

- **認証が通らない**: JSON が OAuth かサービスアカウントかを確認。必要スコープ不足の場合は `BadCredentialScope` が発生します
- **API が有効化されていない**: `ApiDisabled` が出る場合は GCP で該当 API を有効化してください
- **`headless=True` で OAuth が失敗する**: 事前に UI モードで認証し、キャッシュを作成してください
- **ウィジェットが表示されない**: Notebook を再起動し、`ipywidgets` の有効化を確認
- **空の DataFrame が返る**: 期間・プロパティ選択・ディメンション/メトリクス名を再確認
- **`if df:` でエラー**: pandas の真偽値は曖昧です。`df.empty` を使ってください
- **dtype が object のまま**: 欠損値を含む場合は `astype` や `mg.report.prep()` で明示的に型指定してください

## FAQ

**Q. GA3 は使えますか？**
A. `use_ga3=True` で有効化できますが、GA3 はサンセット済みのため推奨しません。

**Q. どの認証方式に対応していますか？**
A. OAuth クライアント JSON とサービスアカウント JSON に対応しています。JSON 文字列／dict も利用できます。

**Q. Application Default Credentials (ADC) は使えますか？**
A. 自動では使いません。JSON を渡すか `MEGATON_CREDS_JSON` を利用してください。

**Q. 認証キャッシュはどこに保存されますか？**
A. `~/.config/cache_*.json` に保存されます（`megaton/auth.py`）。

**Q. `mg.report.run()` の戻り値は DataFrame ですか？**
A. 表示用のオブジェクトを返すため、実データは `mg.report.data` を参照してください。

**Q. ディメンション／メトリクス名はどこで確認できますか？**
A. `mg.show.ga.dimensions` / `mg.show.ga.metrics` で一覧表示できます。

**Q. 列名のリネームを安全に行う方法は？**
A. `(name, "new_name")` のタプル指定を推奨します。内部的には正規表現置換が使われるため、必要に応じて正規表現をエスケープしてください。

**Q. BigQuery に DataFrame を保存する API はありますか？**
A. 直接の保存ヘルパーはありません。`bq.run()` や `google-cloud-bigquery` の標準 API を利用してください。

**Q. `No data found` が出る場合は？**
A. 期間やフィルタ条件の見直し、GA4 プロパティ選択の確認を行ってください。

## バージョン/変更履歴

- バージョンは `setup.py` の `version` を参照してください
- 変更履歴は GitHub のコミット履歴をご確認ください

## ライセンス

MIT License
