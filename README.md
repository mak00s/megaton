# megaton

Megaton は Google アナリティクス（GA4）、Google Search Console、Google Sheets、BigQuery を
**Notebook（Jupyter / Colab）上から直感的に扱うためのツール**です。

> 目的：Notebook での分析・配布作業（GA → SC → Sheets など）を速く回すこと  
> 非目的：汎用 SDK／本番バッチ基盤の置き換え

---

## What is megaton

- Notebook 向けに **短く書ける API** を提供
- UI（ipywidgets）と headless の両方に対応
- Notebook 実行中の **状態（state）を覚える設計**

Megaton は「人間に優しい、記憶力のある相棒」を目指して設計されています。

---

## Signature design (Notebook-first)

Megaton の API は、一般的な SDK とは異なる **Notebook 最適化**のシグネチャを採用しています。

- **1セルで完結する短さ**
- **作業順に沿った API**（開く → 期間 → 取得 → 保存）
- **状態（state）を前提**にした操作

---

## Quick Start

```python
from megaton.start import Megaton
app = Megaton(None, headless=True)
```

---

## Usage（All-in Examples）

### 期間をセット（月次・YoY 対応）

```python
# Nヶ月前の月を基準に、13ヶ月ウィンドウ（前年同月比）
app.report.set.months(months_ago=1, window_months=13)

# 状態として保持される
app.report.start_date
app.report.end_date
ym = app.report.last_month_window["ym"]
```

---

### GA4（最小例）

```python
df = app.report.run(
    d=["date", "eventName"],
    m=["eventCount"],
)
df.head()
```

---

### Google Sheets（保存：名前指定）

```python
# スプレッドシートを開く
app.open.sheet("https://docs.google.com/spreadsheets/d/xxxxx")

# 上書き
app.save.to.sheet("_ga", df)

# 追記
app.append.to.sheet("_ga_log", df)

# upsert（dedup + overwrite）
app.upsert.to.sheet(
    "_ga_monthly",
    df,
    keys=["date", "eventName"],
)
```

---

### 現在のシート（mg.sheet）

`mg.sheet` は **現在選択中のワークシート**に対する操作です。

```python
# シート選択
app.sheets.select("CV")

# セル操作
app.sheet.cell.set("L1", app.report.start_date)
app.sheet.cell.set("N1", app.report.end_date)

# 範囲操作
app.sheet.range.set(
    "L1:N1",
    [[app.report.start_date, app.report.end_date]],
)

# DataFrame 操作（現在シートに対して）
app.sheet.save(df)
app.sheet.append(df)
app.sheet.upsert(df, keys=["ym", "page", "query"])
```

---

### 期間セルの書き込み（report state 利用）

```python
# report.start_date / end_date をセルに書き込む
app.report.dates.to.sheet(
    sheet="CV",
    start_cell="L1",
    end_cell="N1",
)

# 期間文字列（未設定なら空文字）
str(app.report.dates)  # e.g. "20240101-20240131"
```

---

### Search Console（取得）

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

---

### Search Console → Google Sheets（ym 付き保存）

```python
df_sc["ym"] = app.report.last_month_window["ym"]

# 名前指定で保存
app.save.to.sheet("_sc", df_sc)

# upsert（ym + page + query）
app.upsert.to.sheet(
    "_sc_monthly",
    df_sc,
    keys=["ym", "page", "query"],
)
```

---

### BigQuery（最小）

```python
bq = app.launch_bigquery("my-gcp-project")
df = bq.run("SELECT 1 AS test", to_dataframe=True)
df
```

---

## Supported Notebook-facing API (Cheat Sheet)

### Core / Flow
- `Megaton(...)`
- `app.open.sheet(url)`
- `app.launch_bigquery(project)`

### Report (GA)
- `app.report.set.months(months_ago, window_months, tz?, now?)`
- `app.report.set.dates(date_from, date_to)`
- `app.report.run(d, m, filter_d?, filter_m?, sort?, **kwargs)`
- `app.report.start_date`
- `app.report.end_date`
- `app.report.last_month_window["ym"]`
- `app.report.dates`
- `app.report.dates.to.sheet(sheet, start_cell, end_cell)`

### Sheets (by name)
- `app.save.to.sheet(name, df?)`
- `app.append.to.sheet(name, df?)`
- `app.upsert.to.sheet(name, df?, keys, columns?, sort_by?)`

### Sheets (current worksheet)
- `app.sheets.select(name)`
- `app.sheet.create(name)`
- `app.sheet.clear()`
- `app.sheet.data`
- `app.sheet.df()`
- `app.sheet.cell.set(cell, value)`
- `app.sheet.range.set(a1_range, values)`
- `app.sheet.save(df?)`
- `app.sheet.append(df?)`
- `app.sheet.upsert(df?, keys, columns?, sort_by?)`

### Search Console
- `app.sc.sites()`
- `app.sc.query(site, start, end, dimensions, row_limit?, **kwargs)`

### BigQuery
- `app.launch_bigquery(project)`
- `bq.run(sql, to_dataframe=True)`

---

## Legacy compatibility

- `app.gs` は **過去 Notebook 互換の Google Sheets クライアント**です  
  （例：`app.gs.sheet.select(...)`, `app.gs.sheet.data`）
- 新規 Notebook では以下を推奨します：
  - シート操作：`app.sheet.*`
  - 保存：`app.save / append / upsert.to.sheet()`

---

## License

MIT License
