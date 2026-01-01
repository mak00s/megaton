# megaton

Megaton は Google アナリティクス（GA4／GA3）、Google Sheets、BigQuery を Notebook（Jupyter / Colab）上から扱うためのツール群です。認証やアカウント選択の UI を用意しつつ、Notebook から各サービスを横断できます。

> 目的：Notebook での分析・配布作業（GA→Sheets、BQ→可視化など）を素早く回すこと  
> 非目的：汎用SDK／本番バッチ基盤の置き換え

※ GA3（UA）はサンセット済みのため **非推奨**。

---

## What is megaton
- Notebook 向けに **最短の認証・取得・保存**をまとめたツール群
- UI（ipywidgets）と headless の両方に対応
- 外部サービスの状態や権限は環境に依存します（ここでは保証しません）
  - Notebookで直感的に書けるよう、短い記述と状態保持（state）を前提としたシグネチャ設計

### Signature design (Notebook-first)

Megaton の API は、一般的な SDK とは異なる “Notebook 最適化” のシグネチャを採用しています。
これは設計上の妥協ではなく、**Notebook での作業体験を最優先した意図的なトレードオフ**です。

- **1セルで完結**する短さ  
  認証・選択・取得・保存を最小の記述で行えるよう、引数は極力減らしています。

- **ユーザーの作業順に沿った API**  
  認証 → アカウント選択 → 期間指定 → 取得 → 保存、という Notebook 上の自然な流れを前提にしています。

- **Notebook 内での state 保持**  
  Megaton は現在の選択状態（アカウント / プロパティ / 期間など）を
  **Notebook の実行コンテキスト内でのみ**保持します。
  永続状態や並列実行を前提とした設計ではありません。

このため、Megaton は汎用 SDK や本番バッチ基盤の代替を目的としていません。
代わりに、Notebook 上での探索・分析・配布を **速く、直感的に**行うことを重視しています。

---

## Quick Start

### Colab
```bash
pip install -U "git+https://github.com/mak00s/megaton@main"
```

```python
from dotenv import load_dotenv
load_dotenv()

from megaton.start import Megaton
app = Megaton(None, headless=True)  # env を参照
```

> Colab では依存パッケージ不足時に必要に応じて自動インストールされます（`MEGATON_AUTO_INSTALL` で上書き可）。

### ローカル
```bash
pip install -e .
pip install python-dotenv
```

```python
from dotenv import load_dotenv
load_dotenv()

from megaton.start import Megaton
app = Megaton(None, headless=True)
```

---

## Install & Auth

### .env（最短）
`.env` に `MEGATON_CREDS_JSON` を **1行 JSON 文字列**で設定します（`.env` は gitignore）。

```bash
cp .env.example .env
```

```env
MEGATON_CREDS_JSON={"type":"service_account","project_id":"...","private_key":"-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n","client_email":"..."}
```

### headless / UI
- `headless=True`：ウィジェットなし（スモーク／バッチ向け）
- `headless=False`：UI 利用（ipywidgets が未導入だとエラーになります）

### OAuth 概要（必要な場合）
OAuth は UI フローを使います。必要な場合は `docs/advanced.md` を参照してください。

---

## Usage

### GA4（最小例）
```python
# 期間をセット（state に保持）
app.report.set.months(months_ago=1, window_months=13)

df = app.report.run(
    d=["date", "eventName"],
    m=["eventCount"],
)
df.head()
```

### dimensions / metrics 一覧
```python
app.show.ga.dimensions
app.show.ga.metrics
```

### filter / sort（最小例）
```python
df = app.report.run(
    d=["date", "eventName"],
    m=["eventCount"],
    filter_d="eventName==page_view",
    sort="-eventCount",
)
```

### 月次ウィンドウ（日付ヘルパ）
```python
app.report.set.months(months_ago=1, window_months=13)
ym = app.report.last_month_window["ym"]
ym
```

### Google Sheets
```python
# 出力先を選択（state）
app.open.sheet("https://docs.google.com/spreadsheets/d/xxxxx")

# 現在のシートを選択して操作
app.sheet.select("CV")
app.sheet.cell.set("L1", "2024-01-01")
app.sheet.range.set("L1:N1", [["2024-01-01", "2024-01-31"]])

app.sheet.save(df)  # 上書き
app.sheet.append(df)  # 追記
app.sheet.upsert(df, keys=["date", "eventName"])
```

#### 既存の短い保存メソッド
```python
# 上書き
app.save.to.sheet("_ga", df)

# 追記
app.append.to.sheet("_ga_log", df)

# upsert（キー指定）
app.upsert.to.sheet(
    "_ga_monthly",
    df,
    keys=["date", "eventName"],
)
```

#### 互換（legacy）
```python
# 旧ノートブック向け: Google Sheets クライアントは引き続き app.gs で利用可能
app.gs.sheet.select("config")
app.gs.sheet.data
```

### 期間セルの書き込み（state 利用）
```python
# report.start_date / end_date をセルに書き込む
app.report.dates.to.sheet(
    sheet="CV",
    start_cell="L1",
    end_cell="N1",
)
```

### Search Console（最小例）
```python
# GA と同じ「現在の分析期間」をそのまま利用
sites = app.sc.sites()

df_sc = app.sc.query(
    site=sites[0],
    start=app.report.start_date,
    end=app.report.end_date,
    dimensions=["page", "query"],
    row_limit=5000,
)

df_sc.head()
```

### Search Console → Google Sheets（ym 付き保存）
```python
ym = app.report.last_month_window["ym"]
df_sc["ym"] = ym

# 上書き保存
app.save.to.sheet("_sc", df_sc)

# upsert（ym + page + query）
app.upsert.to.sheet(
    "_sc_monthly",
    df_sc,
    keys=["ym", "page", "query"],
)
```

### BigQuery（最小）
```python
bq = app.launch_bigquery("my-gcp-project")
df = bq.run("SELECT 1 AS test", to_dataframe=True)
df
```

---

## Testing & CI

### pytest（外部通信なし）
```bash
pytest tests/test_phase0_regressions.py
pytest tests/test_auto_install.py
pytest tests/test_auth.py
pytest tests/test_utils.py
```

### Notebook smoke（外部通信あり）
`notebooks/test-megaton.ipynb` は **手動のスモークテスト**です。
- GA4 / Sheets / BigQuery を短時間で確認
- 設定が無いサービスは Skip
- Sheets は `_smoke_YYYYMMDD_HHMM` の新規シートに書き込み
- OAuth smoke は `.env` に `SMOKE_OAUTH_JSON=...` を追加して使う

### test-megaton.ipynb の実行手順（Smoke Test）
- 外部サービス接続を含む**手動スモーク用**（CIとは別の最終確認）
- Cell 0 の `RUN_*` フラグで必要なテストだけ実行可能
- Sheets 書き込みは `_smoke_YYYYMMDD_HHMM` の新規シートのみ

CI は GitHub Actions で PR / main push の pytest（fast suite）を実行します。

---

## Advanced
- 詳細な設計・認証・運用の説明は `docs/advanced.md` を参照してください。

---

## ライセンス
MIT License
