# Advanced Guide (Megaton)

README の補足として、設計思想・認証・使い分け・注意点をまとめます。  
初期設定や最短導線は `README.md` を参照してください。

---

## 設計思想

- Notebook 上で **最小の操作でデータ取得〜可視化に進める** ことを最優先にしています
- UI（ウィジェット）で認証・アカウント選択を補助し、コード量を抑える設計です
- 一方で、**汎用 SDK / 本番バッチ基盤の代替は目的ではありません**

---

## 認証方式と headless/UI の使い分け

### 認証入力の種類
`Megaton(credential=...)` に渡せる値は次の通りです。

- **JSON 文字列**（推奨: `.env` の `MEGATON_CREDS_JSON`）
- **dict**
- **JSON ファイルパス**
- **JSON ディレクトリパス**（UI で選択）
- **Base64 JSON 文字列**（`{"...": "..."}` を base64 化したもの）

`credential=None` の場合は `MEGATON_CREDS_JSON` を参照します。

### OAuth とサービスアカウント
- OAuth とサービスアカウントの両方に対応しています
- OAuth は **ブラウザ認証＋キャッシュ保存** が前提です
- サービスアカウントは headless でも利用できます

### headless モード
- `headless=True` は UI なしで動作
- **OAuth の場合は既存キャッシュが必須**（`~/.config/cache_*.json`）
- GA4 プロパティ選択は **コードで行う必要**があります

例（headless で GA4 プロパティを選択）:
```python
from megaton.start import Megaton

app = Megaton(None, headless=True)
ga4 = app.ga["4"]
ga4.account.select(ga4.accounts[0]["id"])
ga4.property.select(ga4.account.properties[0]["id"])
```

### UI モード
- `headless=False` で UI が表示されます
- UI 上で GA4 アカウント / プロパティを選択できます

---

## GA4 の詳しい使い方

### `report.run()` の入力

```python
df = app.report.run(
    d=["date", "eventName"],
    m=["eventCount"],
    filter_d="eventName==page_view",
    sort="-eventCount",
)
```

- `d` / `m` は **api_name** または **表示名** を指定可能
- リネームはタプル指定: `("eventCount", "events")`
- 複数条件の filter は `;` で AND 連結

### 出力 DataFrame の特徴
- `app.report.data` に保存されます
- `date` / `firstSessionDate` は `datetime.date` に変換されます
- `dateHour` / `dateHourMinute` は `datetime` に変換されます
- 型変換は一部列のみ（全列ではありません）

### 制限・注意
- GA4 は **最大 9 次元 / 10 指標** まで（超えると警告して切り捨て）
- プロパティ未選択だと `report.run()` は失敗します
- フィルタやソートは GA3 互換の文字列仕様のため、複雑な条件は pandas 側で処理する方が安全です

---

## Google Sheets の詳しい使い方

```python
app.open.sheet("https://docs.google.com/spreadsheets/d/xxxxx")
app.save.to.sheet("Sheet1", app.report.data)    # 上書き
app.append.to.sheet("Sheet1", app.report.data)  # 追記
```

注意点:
- スプレッドシートへの権限がないと `BadPermission` になります
- `app.select.sheet("Sheet1")` で明示選択できます

---

## BigQuery の詳しい使い方

### 最小実行
```python
bq = app.launch_bigquery("my-gcp-project")
df = bq.run("SELECT 1 AS test", to_dataframe=True)
```

### GA4 エクスポート用ユーティリティ
```python
bq.dataset.select("analytics_123456")
sql = bq.ga4.get_query_to_flatten_events("20240101", "20240107")
print(sql[:400])
```

注意点:
- `flatten_events()` は実際にクエリを実行します（期間は短く）
- `get_query_to_flatten_events()` は **SQL を生成するだけ** です
- BigQuery Data Transfer API を使う機能には権限が必要です

---

## test-megaton.ipynb の位置づけ

- **手動 E2E スモーク**（外部サービスへのアクセスあり）
- `.env` で `MEGATON_CREDS_JSON` を渡す前提
- 設定がないサービスは **Skip** する設計
- Sheets は必ず `_smoke_YYYYMMDD_HHMM` の **新規シート**に書き込み（上書き禁止）

---

## よくあるハマりどころ

- **`MEGATON_CREDS_JSON` が未設定**  
  `.env` を作成し、JSON 文字列を 1 行で貼り付けてください

- **OAuth で headless が失敗**  
  先に UI で認証し、キャッシュを作成してください

- **GA4 の display_name / api_name 混在**  
  `app.show.ga.dimensions` / `app.show.ga.metrics` で確認するのが安全です

- **`if df:` がエラー**  
  pandas の真偽値は曖昧なので `df.empty` を使ってください

- **Colab で依存関係が足りない**  
  `MEGATON_AUTO_INSTALL=1` で自動インストールを有効化できます
