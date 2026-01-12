# megaton

Megaton は Google Analytics 4、Google Search Console、Google Sheets、BigQuery を **Notebook から短いコードで扱う** ためのツールです。分析の試行錯誤を速く回すことを重視し、Notebook 向けの UX に特化しています。

## コア概念

- **結果オブジェクト**: `SearchResult` / `ReportResult` によるメソッドチェーン
- **シンプルな流れ**: 開く → 期間設定 → 取得 → 保存
- **Notebook 前提**: 途中結果の確認を前提にした設計

## クイックスタート

```python
from megaton.start import Megaton

mg = Megaton("/path/to/service_account.json")
mg.report.set.dates("2024-01-01", "2024-01-31")
mg.report.run(d=["date", "eventName"], m=["eventCount"])

mg.open.sheet("https://docs.google.com/spreadsheets/d/...")
mg.save.to.sheet("_ga_data", mg.report.data)
```

## もう少し実用的な例

```python
# 複数サイトの Search Console データを一括取得して整形
result = (mg.search.run.all(
    sites,
    dimensions=['query', 'page'],
    item_key='clinic',
)
    .categorize('query', by=query_map)
    .categorize('page', by=page_map))

mg.save.to.sheet('_query', result.df, sort_by='impressions')
```

## インストール

```bash
pip install git+https://github.com/mak00s/megaton.git
```

## ドキュメント

- [api-reference.md](docs/api-reference.md) - API 仕様の単一ソース
- [design.md](docs/design.md) - 設計思想とトレードオフ
- [cookbook.md](docs/cookbook.md) - 実用例集
- [cheatsheet.md](docs/cheatsheet.md) - 1 行リファレンス

## 変更履歴

- [CHANGELOG.md](CHANGELOG.md)

## ライセンス

MIT License
