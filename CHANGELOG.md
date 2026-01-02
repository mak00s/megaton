# Changelog

このプロジェクトの主要な変更点を記録するファイルです。バージョン番号は [Semantic Versioning](https://semver.org/spec/v2.0.0.html) に従って増分されます。

## 0.7.1 – 2026‑01‑02

### 修正 / 追加

- **Config リファクタ**: `load_config()` を拡張し、サイト単位で閾値（`min_impressions`, `max_position`, `min_pv`, `min_cv`）と Search Console 用 URL (`gsc_site_url`) を管理できるようにしました。
- **Search ヘルパー追加**: `mg.search.filter_by_thresholds()` を追加し、サイト設定に基づく一括フィルタリングを可能にしました。
- **ドキュメント更新**: Advanced Guide に新しい config フィールドと運用例を追記しました。

### 破壊的変更

- `cfg.thresholds_df` は `None` を返します（廃止予定）。移行先は各 `site` レコードの閾値です。

## 0.7.0 – 2026‑01‑01

### 追加

- **Search Console 対応の刷新**: `mg.search` を Google Search Console へのインタフェースとして導入しました。これにより、`mg.search.sites` でサイト一覧の取得、`mg.search.get.sites()` で再取得、`mg.search.use(site_url)` で対象サイトの選択、`mg.search.set.dates()` / `mg.search.set.months()` で期間設定、`mg.search.run()` でデータ取得が行えます。
- **レポート期間セル書き込み**: `mg.report.dates.to.sheet(sheet, start_cell, end_cell)` を追加し、設定したレポート期間をスプレッドシートに直接書き込めるようにしました。
- **Sheets ヘルパーの拡充**: 新しい `mg.sheets` と `mg.sheet` API により、ワークシートの選択 (`mg.sheets.select(name)`)、作成 (`mg.sheets.create(name)`)、削除 (`mg.sheets.delete(name)`)、および現在のシートへの保存・追記・アップサート (`mg.sheet.save(df)`, `mg.sheet.append(df)`, `mg.sheet.upsert(df, keys, columns?, sort_by?)`) を簡潔に実行できます。これにより既存の `mg.gs` インタフェースに比べて Notebook での操作がより直感的になりました。

### 変更

- **月次ウィンドウ設定**: GA4 レポートおよび Search Console クエリで期間を月単位で指定する際の引数を統一しました。`ago` と `window_months` を用いて過去何か月前から何か月間の窓を取得するかを明示します。デフォルトでは `ago=1`, `window_months=13` です。

### 修正

- ドキュメントを大幅に更新し、README.md と Advanced Guide を整理しました。`mg.search` の紹介と Sheets 操作に関する手順を追記しました。

## 0.6.0 - 2025-12-31

- 認証プロバイダの分離と互換性保持のためのリファクタリング
- Sheets / BigQuery サービスの独立
- ipywidgets を遅延インポートし、headless 環境でも動作しやすくしました
- GitHub Actions による高速 pytest ワークフローを追加
- README / Advanced guide の更新および smoke notebook のガイダンス

このリリースは主に内部のリファクタリングと安定性向上が中心です。公開 API の後方互換性は維持されています。
