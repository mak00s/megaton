# Changelog

このプロジェクトの主要な変更点を記録するファイルです。バージョン番号は [Semantic Versioning](https://semver.org/spec/v2.0.0.html) に従って増分されます。

## 0.8.0 – 2026‑01‑03

### 追加

- **SearchResult メソッドチェーン**: `mg.search.run()` と `mg.search.run.all()` が `SearchResult` オブジェクトを返すようになり、メソッドチェーンで段階的な処理が可能になりました。
  - **URL 処理**: `.decode(group=True)` – URL デコード、`.remove_params(keep=None, group=True)` – クエリパラメータ削除、`.remove_fragment(group=True)` – フラグメント削除、`.lower(columns=None, group=True)` – 小文字化
  - **分類**: `.classify(query=None, page=None, group=True)` – クエリ・ページの正規化とカテゴリ分類
  - **フィルター**: `.filter_clicks()`, `.filter_impressions()`, `.filter_ctr()`, `.filter_position()` – 指標ごとのフィルタリング。`sites` パラメータで行ごとに異なる閾値を適用可能
  - **集計**: `.aggregate(by=None)` – 手動集計
  - **DataFrame アクセス**: `.df` プロパティまたは `.to_df()` メソッド
- **clean パラメータ**: `mg.search.run(clean=True)` で自動的に URL 正規化（decode + ? 削除 + # 削除 + 小文字化）を実行します。
- **sites パラメータ**: フィルターメソッドで行ごとに異なる閾値を適用可能。DataFrame の `site_key` 列（default: 'site'）で各行に対応するサイト設定を検索します。
- **keep_clicked パラメータ**: `clicks >= 1` の行を無条件に残すオプション。`filter_clicks()` では default=False、他のフィルターでは default=True です。
- **group パラメータ**: URL 処理・分類メソッドで `group=True`（default）の場合、dimensions に基づいて自動集計します。大量データでは `group=False` にして最後だけ集計することでパフォーマンスを向上できます。


## 0.7.4 – 2026‑01‑02

### 修正 / 追加

- **ドキュメント調整**: `run.all` の説明を整理しました。
- **Search Console クエリ整理**: `GSCService.query()` の dimensions を `date/hour/country/device/page/query` に限定し、`month` 指定時は内部的に `date` で取得して月単位で集計します。
- **Search Console フィルタ追加**: `mg.search.run()` に `dimension_filter` を追加し、`contains` / 正規表現（RE2）での絞り込みに対応しました（AND 条件のみ）。

## 0.7.3 – 2026‑01‑02

### 修正 / 追加

- **バッチ処理 API 追加**: `mg.search.run.all()` と `mg.report.run.all()` を追加し、複数アイテム（サイト、クリニック等）のデータを一括取得・結合できるようにしました。
  - `items` パラメータで設定リスト（dict の list）を渡し、各要素に対してクエリを実行して結合します
  - `item_key` (default: `'site'`) で識別子列名を指定します
  - `item_filter` でフィルタリング（リスト or 関数）をサポートします
  - `add_month` (str or DateWindow) で月ラベルを自動追加できます
  - Search Console 用に `site_url_key` (default: `'gsc_site_url'`)、GA4用に `property_key` (default: `'ga4_property_id'`) を指定できます
  - **注意:** `site_url_key` が空の場合、そのアイテムはスキップされます
- **ドキュメント更新**: CHEATSHEET に `run.all()` の使い方を追記しました。
- **Report 表示/保存の修正**: `mg.report.show()` / `mg.report.download()` が `self.data` を参照するように修正しました。

## 0.7.2 – 2026‑01‑02

### 修正 / 追加

- **clicks_zero_only パラメータ追加**: `mg.search.filter_by_thresholds()` に `clicks_zero_only` パラメータを追加しました。`True` を指定すると、クリック数が 0 の行にのみ閾値を適用し、クリック数が 1 以上の行は閾値に関わらず保持されます。これにより、従来の動作（クリックがあるキーワードは無条件に残す）を再現できます。

## 0.7.1 – 2026‑01‑02

### 修正 / 追加

- **DateWindow 導入**: `mg.report.set.months()` と `mg.search.set.months()` が `DateWindow` namedtuple を返すようになりました。6つの日付フォーマット（`start_iso`, `end_iso`, `start_ym`, `end_ym`, `start_ymd`, `end_ymd`）を提供し、BigQuery の YYYYMMDD 形式や月ラベル生成が簡単になりました。`min_ymd` パラメータで開始日の制約も指定可能です。後方互換性のためタプルアンパッキング（最初の3要素）もサポートしています。
- **Config リファクタ**: `load_config()` を拡張し、サイト単位で閾値（`min_impressions`, `max_position`, `min_pv`, `min_cv`）と Search Console 用 URL (`gsc_site_url`) を管理できるようにしました。
- **Search ヘルパー追加**: `mg.search.filter_by_thresholds()` を追加し、サイト設定に基づく一括フィルタリングを可能にしました。
- **ドキュメント更新**: Advanced Guide に新しい config フィールドと運用例を追記しました。

## 0.7.0 – 2026‑01‑01

### 追加

- **Search Console 対応の刷新**: `mg.search` を Google Search Console へのインタフェースとして導入しました。これにより、`mg.search.sites` でサイト一覧の取得、`mg.search.get.sites()` で再取得、`mg.search.use(site_url)` で対象サイトの選択、`mg.search.set.dates()` / `mg.search.set.months()` で期間設定、`mg.search.run()` でデータ取得が行えます。
- **レポート期間セル書き込み**: `mg.report.dates.to.sheet(sheet, start_cell, end_cell)` を追加し、設定したレポート期間をスプレッドシートに直接書き込めるようにしました。
- **Sheets ヘルパーの拡充**: 新しい `mg.sheets` と `mg.sheet` API により、ワークシートの選択 (`mg.sheets.select(name)`)、作成 (`mg.sheets.create(name)`)、削除 (`mg.sheets.delete(name)`)、および現在のシートへの保存・追記・アップサート (`mg.sheet.save(df)`, `mg.sheet.append(df)`, `mg.sheet.upsert(df, keys, columns?, sort_by?)`) を簡潔に実行できます。これにより Notebook での操作がより直感的になりました。

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
