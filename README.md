# megaton

Megaton は Google アナリティクス（GA4／GA3）、Google Sheets、BigQuery を Notebook（Jupyter / Colab）上から扱うためのツール群です。認証やアカウント選択の UI を用意しつつ、Notebook から各サービスを横断できます。

> 目的：Notebook での分析・配布作業（GA→Sheets、BQ→可視化など）を素早く回すこと  
> 非目的：汎用SDK／本番バッチ基盤の置き換え

---

## できること（概要）
- **認証フローの補助**：サービスアカウント / OAuth の導線を Notebook 上で扱いやすく
- **GA4 レポート取得**：UI 選択 → `report.run()` で DataFrame
- **Google Sheets 連携**：上書き保存／追記
- **BigQuery 連携**：クライアント操作、GA4 Export ユーティリティ
- **整形ユーティリティ**：列名変更、日付型変換など

※ GA3（UA）はサンセット済みのため **非推奨**。

---

# 初期設定

## 動作環境
- Python 3.9+
- Jupyter Notebook / JupyterLab / Google Colab

## インストール
PyPI は未公開です。GitHub から直接インストールします。

### ローカル（開発）
```bash
pip install -e .
```

### Colab / 使うだけ
```bash
pip install -U "git+https://github.com/mak00s/megaton@feature/colab-test"
```
> Colab では依存パッケージ不足時に必要に応じて自動インストールされます（`MEGATON_AUTO_INSTALL` で上書き可）。

## 認証（最短：.env）
**パス指定は使いません。** `MEGATON_CREDS_JSON` に **JSON文字列**を入れます。

1) `.env` を作成
```bash
cp .env.example .env
```

2) `MEGATON_CREDS_JSON` にサービスアカウント JSON を **1行**で貼り付け（`.env` は gitignore）
```env
MEGATON_CREDS_JSON={"type":"service_account","project_id":"...","private_key":"-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n","client_email":"..."}
```

## 起動（headless / UI）
```python
from dotenv import load_dotenv
load_dotenv()

from megaton.start import Megaton
app = Megaton(None, headless=True)  # env を参照
```
- `headless=True`：ウィジェットなし（スモーク／バッチ向け）
- UI が必要なら `headless=False`

## 手動スモーク（外部通信あり）
- `notebooks/test-megaton.ipynb`  
  - `.env` 前提
  - GA4 / Sheets / BigQuery を **短時間**で確認
  - 設定が無いサービスは **Skip**（止まらない）
  - Sheets は必ず `_smoke_YYYYMMDD_HHMM` の **新規シート**に書き込み（上書きなし）

## 上級者向けドキュメント
- `docs/advanced.md`

---

# 使い方

## GA4（最小例）
```python
app.report.set_dates("2024-01-01", "2024-01-01")
df = app.report.run(
    d=["date", "eventName"],
    m=["eventCount"],
)
df.head()
```

### 利用可能なディメンション/指標
```python
app.show.ga.dimensions
app.show.ga.metrics
```

### フィルタ / ソート
```python
df = app.report.run(
    d=["date", "eventName"],
    m=["eventCount"],
    filter_d="eventName==page_view",
    sort="-eventCount",
)
```
> 複雑な条件は取得後に pandas 側で処理するのが確実です。

## Google Sheets
```python
app.open.sheet("https://docs.google.com/spreadsheets/d/xxxxx")
app.report.to.sheet("Sheet1")      # 上書き
app.append.to.sheet("Sheet1", df) # 追記
```

## BigQuery（最小）
```python
bq = app.launch_bigquery("my-gcp-project")
df = bq.run("SELECT 1 AS test", to_dataframe=True)
```

## 任意（GSC / Drive）
設定がある場合のみ使用できます。無い場合は Skip されます。

---

## トラブルシューティング
- **認証が通らない**：サービスアカウント JSON か／権限付与を確認
- **Colab 依存関係**：`MEGATON_AUTO_INSTALL=1`（有効）/ `0`（無効）
- **ウィジェット未表示**：Notebook/ブラウザ再起動、拡張機能確認

---

## ライセンス
MIT License
