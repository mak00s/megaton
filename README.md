# megaton

Megaton は Google アナリティクス（GA4）、Google Search Console、Google Sheets、BigQuery を
**Notebook（Jupyter / Colab）から直感的に扱うためのツール**です。

> 目的：Notebook 上での分析・配布（GA → SC → Sheets など）を速く回すこと  
> 非目的：汎用 SDK／本番バッチ基盤の置き換え

---

## Megaton とは

- Notebook（Jupyter / Google Colaboratory）向けに **短く書ける API** を提供
- UI（ipywidgets）と headless の両方に対応
- Notebook 実行中の **状態（state）を覚える設計**

Megaton は「人間に優しい、記憶力のあるロボット」を目指して設計されています。

---

## Signature design (Notebook-first)

Megaton の API は、一般的な SDK とは異なる **Notebook 最適化**のシグネチャを採用しています。

- **1セルで完結する短さ**
- **作業順に沿った API**（開く → 期間 → 取得 → 保存）
- **状態（state）を前提**にした操作

---

## Quick Start

前提：`from megaton import start` 済み。

### 1) サービスアカウントJSONのパスを渡す
```python
mg = start.Megaton("/path/to/service_account.json")
```

### 2) JSON文字列を直接渡す
```python
mg = start.Megaton('{"type":"service_account","project_id":"...","client_email":"...","private_key":"-----BEGIN PRIVATE KEY-----\\n...\\n-----END PRIVATE KEY-----\\n"}')
```

### 3) 環境変数から（推奨）
`mg = start.Megaton()` のように引数を省略すると、`MEGATON_CREDS_JSON` が定義されていればその値を **パス**として扱います（ファイル or ディレクトリ）。

- ファイルを渡した場合：そのJSONを使用
- ディレクトリを渡した場合：含まれる JSON から選択するメニューが表示されます（headless では利用不可）

---

## Usage

### 期間をセット（月次・YoY 対応）

#### A) 開始日・終了日を直接指定（YYYY-MM-DD）
```python
mg.report.set.dates("2024-01-01", "2024-01-31")
```

何も指定しない場合は、直近7日間（前日まで）の期間が自動で使われます。
設定した期間は状態として保持されます。

```python
mg.report.start_date
mg.report.end_date
```

#### B) 「Nヶ月前の月」を基準に期間をセット（前年同月比など）
```python
mg.report.set.months(ago=1, window_months=13)
ym = mg.report.window["ym"]
```

- `set.months()` のデフォルトは「1ヶ月前の月を基準に 13ヶ月窓」です
- タイムゾーンは JST（`Asia/Tokyo`）として扱われます

---

### GA4（最小例）

```python
mg.report.run(
    d=["date", "eventName"],
    m=["eventCount"],
)
```

取得データはそのまま表示されます。
結果の簡易的な加工もできます。

```python
# 簡易な前処理（rename/replace/type など）
conf = {
    "eventName": {"name": "event_name"},
}
mg.report.prep(conf)
```

結果は `mg.report.data` に入るので、必要なら `df = mg.report.data` などと取り出して活用できます。

---

### Google Sheets（保存：名前指定）

```python
mg.open.sheet("https://docs.google.com/spreadsheets/d/xxxxx")

# 上書き
mg.save.to.sheet("_ga", df)

# 追記
mg.append.to.sheet("_ga_log", df)

# upsert（dedup + overwrite）
mg.upsert.to.sheet(
    "_ga_monthly",
    df,
    keys=["date", "eventName"],
)
```

---

### 現在のシート（mg.sheet）

`mg.sheet` は **現在選択中のワークシート**に対する操作です。

```python
# シート選択・作成・削除（collection-level）
mg.sheets.select("CV")
mg.sheets.create("tmp_sheet")
mg.sheets.delete("tmp_sheet")

# セル操作（current sheet）
mg.sheet.cell.set("L1", mg.report.start_date)
mg.sheet.cell.set("N1", mg.report.end_date)

# 範囲操作
mg.sheet.range.set("L1:N1", [[mg.report.start_date, mg.report.end_date]])

# DataFrame 操作（現在シートに対して）
mg.sheet.save(df)
mg.sheet.append(df)
mg.sheet.upsert(df, keys=["ym", "page", "query"])
```

---

### 読み取り

シート全体のデータは `mg.sheet.data` / `mg.sheet.df()` で参照できます。

```python
mg.sheets.select("CV")

rows = mg.sheet.data       # list[dict]
df_sheet = mg.sheet.df()   # DataFrame
```

セルや範囲のピンポイント読み取りもできます。

```python
# セル単位
mg.gs.sheet.select("CV")
cell_value = mg.gs.sheet._driver.acell("L1").value

# 範囲
values = mg.gs.sheet._driver.get("L1:N1")  # 2次元配列
```

---

### レポート期間をセルに書き込む例

```python
mg.report.dates.to.sheet(
    sheet="CV",
    start_cell="L1",
    end_cell="N1",
)

# 期間文字列（未設定なら空文字）
str(mg.report.dates)  # e.g. "20240101-20240131"
```

---

### Search Console（取得）

```python
# 権限を持つプロパティ一覧（初回アクセスで自動取得）
sites = mg.sc.sites

# データを取得する対象のプロパティを選択
mg.sc.use(sites[0])

# データ取得
df_sc = mg.sc.query(
    dimensions=["page", "query"],
    metrics=["clicks", "impressions", "ctr", "position"],
    limit=5000,
)
```

---

### Search Console → Google Sheets（ym 付き保存）

```python
df_sc["ym"] = mg.report.window["ym"]

mg.save.to.sheet("_sc", df_sc)

mg.upsert.to.sheet(
    "_sc_monthly",
    df_sc,
    keys=["ym", "page", "query"],
)
```

---

### BigQuery（最小）

```python
bq = mg.launch_bigquery("my-gcp-project")
df = bq.run("SELECT 1 AS test", to_dataframe=True)
df
```

---

## Supported Notebook-facing API (Cheat Sheet)

### Core / Flow
- `mg = start.Megaton(creds)`
- `mg.open.sheet(url)`
- `mg.launch_bigquery(project)`

### Report (GA)
- `mg.report.set.months(ago, window_months, tz?, now?)`
- `mg.report.set.dates(date_from, date_to)`
- `mg.report.run(d, m, filter_d?, filter_m?, sort?, **kwargs)`
- `mg.report.start_date`
- `mg.report.end_date`
- `mg.report.window["ym"]`
- `mg.report.dates`
- `mg.report.dates.to.sheet(sheet, start_cell, end_cell)`

### Sheets (by name)
- `mg.save.to.sheet(name, df?)`
- `mg.append.to.sheet(name, df?)`
- `mg.upsert.to.sheet(name, df?, keys, columns?, sort_by?)`

### Sheets (collection / current)
- `mg.sheets.select(name)`
- `mg.sheets.create(name)`
- `mg.sheets.delete(name)`
- `mg.sheet.clear()`
- `mg.sheet.data`
- `mg.sheet.df()`
- `mg.sheet.cell.set(cell, value)`
- `mg.sheet.range.set(a1_range, values)`
- `mg.sheet.save(df?)`
- `mg.sheet.append(df?)`
- `mg.sheet.upsert(df?, keys, columns?, sort_by?)`

### Search Console
- `mg.sc.sites`
- `mg.sc.refresh.sites()`
- `mg.sc.use(site_url)`
- `mg.sc.set.dates(date_from, date_to)`
- `mg.sc.set.months(ago, window_months, tz?, now?)`
- `mg.sc.query(dimensions, metrics?, limit?, **kwargs)`

### BigQuery
- `mg.launch_bigquery(project)`
- `bq.run(sql, to_dataframe=True)`

---

## Legacy compatibility

- `mg.gs` は **過去 Notebook 互換の Google Sheets クライアント**です  
  （例：`mg.gs.sheet.select(...)`, `mg.gs.sheet.data`）
- 新規 Notebook では以下を推奨します：
  - シート操作：`mg.sheets.*` / `mg.sheet.*`
  - 保存：`mg.save / append / upsert.to.sheet()`

---

## License

MIT License
