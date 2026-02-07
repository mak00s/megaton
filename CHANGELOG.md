# Changelog

このプロジェクトの主要な変更点を記録するファイルです。バージョン番号は [Semantic Versioning](https://semver.org/spec/v2.0.0.html) に従って増分されます。

## Unreleased

### 追加

- **GA4 report の指数バックオフ再試行**: `mg.report.run()` に `max_retries`（default: `3`）と `backoff_factor`（default: `2.0`）を追加し、GA4 Data API の `ServiceUnavailable` 発生時に再試行するようになりました。
- **回帰テスト追加**: `tests/test_ga4_report_retry.py` を追加し、一時障害後の復帰と再試行枯渇時の挙動を検証。
- **GA4 user properties 表示**: `mg.ga["4"].property.show("user_properties")` を追加し、`scope == 'USER'` のカスタムディメンションのみ確認できるようになりました。
- **`mg.report.prep(show=False)`**: `mg.report.run(show=False)` と同様に表示抑制が可能になり、処理後の DataFrame を直接受け取れるようになりました。
- **GA4 custom filter prefix 対応**: `filter_d` で `customEvent:...` / `customUser:...` を指定可能になりました。
- **Sheets append/upsert の表示オプション拡張**: `mg.append.to.sheet()` / `mg.upsert.to.sheet()` / `mg.sheet.append()` / `mg.sheet.upsert()` でも `auto_width` / `freeze_header` を指定可能になりました。
- **Sheets save の `start_row` 対応**: `mg.save.to.sheet()` / `mg.sheet.save()` に `start_row` を追加。`start_row>1` のときは上部行を保持したまま指定行から上書きできます。
- **Sheets save/append の `create_if_missing` 対応**: `mg.save.to.sheet()` / `mg.append.to.sheet()` に `create_if_missing`（default: `False`）を追加。必要時のみシートを自動作成できます。

### 変更

- **ドキュメント更新**: `docs/api-reference.md` / `docs/cheatsheet.md` に `mg.report.run()` の retry パラメータと挙動を追記。
- **ドキュメント更新**: `mg.report.run(show=...)` と `mg.report.prep(show=...)` の挙動、`mg.report.prep()` の詳細仕様と実例を追記。
- **ドキュメント更新**: `docs/api-reference.md` / `docs/cheatsheet.md` / `docs/cookbook.md` に `start_row` の仕様と利用例を追記。
- **ドキュメント更新**: `docs/api-reference.md` / `docs/cheatsheet.md` に `create_if_missing` の仕様を追記。

## 0.8.3 – 2026‑02‑07

### 追加

- **Search 日付テンプレート対応**: `mg.search.set.dates()` で `NdaysAgo` / `yesterday` / `today` を指定可能になりました。`mg.search.run()` 実行前に `YYYY-MM-DD` へ正規化されます。

### 変更

- **ドキュメント整備**: `docs/api-reference.md` と `docs/cheatsheet.md` を更新し、公開 API の漏れ補完、前提条件、失敗時挙動、環境依存（headless/Colab）を明記。
- **パッケージバージョン**: `setup.py` のバージョンを `0.8.3` に更新。

## 0.8.2 – 2026‑02‑07

### 追加

- **CSV アップサート API**: `mg.upsert.to.csv(df?, filename?, keys, columns?, sort_by?, include_dates?, quiet?)` を追加。既存CSVを読み込み、`keys` を基準に重複行を置換して保存できます。

### 変更

- **ドキュメント更新**: `README.md` / `docs/api-reference.md` / `docs/cheatsheet.md` / `docs/cookbook.md` に `mg.upsert.to.csv()` の使用例と仕様を追記。
- **パッケージバージョン**: `setup.py` のバージョンを `0.8.2` に更新。

## 0.8.1 – 2026‑02‑06

### 追加

- **ReportResult.clean_url()**: URL列の正規化をサポート（transform.text.clean_url() を列指定で呼び出し可能）。
- **SearchResult.clean_url()**: URL列の正規化をサポート（`group` で集約制御可能）。
- **docs/cheatsheet**: `filter_d` / `filter_m` / `sort` の書式を追記。
- **example-megaton.ipynb**: 使い方サンプルノートブックを追加。

### 変更

- **Python 最小バージョン**: Python の最小要求バージョンを `>=3.9` から `>=3.11` に変更。
- **ga4.classify_source_channel()**: `medium` 列が存在しない場合は空文字として扱う。
- **SearchResult.clean_url()**: 対象列が存在しない場合はエラーに統一。
- **依存関係**: `pytz>=2023.3` を追加。

### 修正

- **ドキュメント更新**: フィルタAPIの仕様（優先順位・前提条件・エッジケース）を明確化。
- **Google Sheets エラーメッセージ**: 権限不足時にサービスアカウントとURLを表示。
- **Google Sheets 前提チェック**: スプレッドシート未接続時の例外メッセージを具体化。

## 0.8.0 – 2026‑02‑01

### 追加

- **ga4.classify_source_channel()**: source正規化とchannel分類を統合した新関数。sourceとchannelの両列を含むDataFrameを返します。
  - channelごとに構造化されたパターン定義（AI、Organic Search、Organic Social）
  - source自動正規化機能（ChatGPT、Gemini、Claude、Facebook、X、Instagram、YouTube、TikTok、Threads、docomo、bing、auone等）
  - custom_channelsパラメータでプロジェクト固有チャネル（Group、client_x Internal等）を拡張可能
  - 誤判定防止の強化：AI判定はbing.com/chat限定、SNS判定は正規化後の名前でチェック、Search判定は単語境界を考慮
- **ga4.convert_filter_to_event_scope()**: session系フィルタディメンションをevent系に変換する関数。GA4 APIでsession系（`sessionDefaultChannelGroup`など）とevent系（`defaultChannelGroup`など）のディメンション互換性を保ちます。
- **site.filter_d**: `mg.report.run.all()` の `filter_d` パラメータで `site.<key>` を指定すると、各アイテム設定の `<key>` から動的にフィルタを解決できます（`site.lp_dim` / `site.cv_metric` と同様）。
  - 例: `filter_d='site.filter_d'` で各サイトの `filter_d` 列を使用
  - サイトごとに異なるフィルタ条件（国、デバイスなど）を一括処理で適用可能
- **SearchResult.apply_if()**: 条件付きメソッドチェーンをサポート。条件が True の場合のみメソッドを適用します。
  - 例: `result.apply_if(config.normalize, 'normalize_queries')`
  - if文でのチェーン分岐を排除し、fluent interface を維持
- **ReportResult.replace()**: 正規表現パターンによる置換をサポート（pandas の `.replace()` と同様）。
  - `regex=True`（デフォルト）: 辞書の key を正規表現として扱う
  - `regex=False`: 固定文字列での置換
  - 例: `.replace(dimension='campaign', by={r'\([^)]*\)': ''})` で括弧内を削除
- **text.infer_site_from_url()**: URLからサイト識別子を推測する関数（マルチサイト企業対応）。
  - sites 設定から domain/url を抽出してドメインマッチング
  - クエリパラメータ `id=` による特殊IDマッチング（dentamap など）
  - 例: `text.infer_site_from_url(url, sites, site_key='clinic', id_key='dentamap_id')`
- **バッチ処理 API**: `mg.search.run.all()` と `mg.report.run.all()` で複数アイテム（サイト、クリニック等）のデータを一括取得・結合できます。
  - `items` パラメータで設定リスト（dict の list）を渡し、各要素に対してクエリを実行して結合
  - `item_key`（default: `'site'`）で識別子列名を指定
  - `item_filter` でフィルタリング（リスト or 関数）をサポート
  - Search Console 用に `site_url_key`（default: `'gsc_site_url'`）、GA4用に `property_key`（default: `'ga4_property_id'`）を指定可能
  - **注意**: `site_url_key` が空の場合、そのアイテムはスキップされます
- **DateWindow**: `mg.report.set.months()` と `mg.search.set.months()` が `DateWindow` namedtuple を返すようになりました。6つの日付フォーマット（`start_iso`, `end_iso`, `start_ym`, `end_ym`, `start_ymd`, `end_ymd`）を提供し、BigQuery の YYYYMMDD 形式や月ラベル生成が簡単になりました。`min_ymd` パラメータで開始日の制約も指定可能です。後方互換性のためタプルアンパッキング（最初の3要素）もサポート。
- **SearchResult メソッドチェーン**: `mg.search.run()` と `mg.search.run.all()` でメソッドチェーンによる段階的な処理が可能になりました。
  - **URL 処理**: `.decode()`（URL デコード）、`.remove_params()`（クエリパラメータ削除）、`.remove_fragment()`（フラグメント削除）、`.lower()`（小文字化）
  - **分類**: `.normalize()` / `.categorize()` / `.classify()`（正規化・カテゴリ付与・正規化+集約）
  - **フィルター**: `.filter_clicks()`, `.filter_impressions()`, `.filter_ctr()`, `.filter_position()`（指標ごとのフィルタリング）
  - **集計**: `.aggregate(by=None)`（ディメンションの組み合わせを一意にする集計）
  - **DataFrame アクセス**: `.df` プロパティ
- **Config 拡張**: `load_config()` でサイト単位の閾値（`min_impressions`, `max_position`, `min_pv`, `min_cv`）と Search Console 用 URL（`gsc_site_url`）を管理できます。
- **Search フィルターヘルパー**: `mg.search.filter_by_thresholds()` でサイト設定に基づく一括フィルタリングが可能になりました。
- **dimension_filter パラメータ**: `mg.search.run()` に追加。`contains` / 正規表現（RE2）での絞り込みに対応（AND 条件のみ）。
- **clicks_zero_only パラメータ**: `mg.search.filter_by_thresholds()` に追加。`True` を指定すると、クリック数が 0 の行にのみ閾値を適用し、クリック数が 1 以上の行は閾値に関わらず保持されます。
- **report.run.all 拡張**:
  - `d` / `m` で `site.<key>` / `site.<metric>` を参照可能。
  - `filter_d` をメトリクスごとに指定可能。
  - `landingPage` が絶対URLでも処理可能。

### 変更

- **ga4.classify_channel()**: 内部で `classify_source_channel()` を呼び出すラッパー関数に変更。`custom_channels` パラメータを追加。
- **SearchResult / ReportResult の分類API**: `normalize()` / `categorize()` / `classify()` に責務を分離しました。
  - `normalize()` は上書きのみ（集約なし）
  - `categorize()` はカテゴリ列追加のみ（集約なし）
  - `classify()` は上書き + 集約（常に集約）
- **clean パラメータ**: `mg.search.run(clean=True)` で自動的に URL 正規化（decode + ? 削除 + # 削除 + 小文字化）を実行します。
- **sites パラメータ**: フィルターメソッドで行ごとに異なる閾値を適用可能。DataFrame の `site_key` 列（default: `'site'`）で各行に対応するサイト設定を検索します。
- **keep_clicked パラメータ**: `clicks >= 1` の行を無条件に残すオプション。すべてのフィルターで default=False です（明示的に True を指定すると有効化）。
- **group パラメータ**: URL 処理メソッドで `group=True`（default）の場合、dimensions に基づいて自動集計します。大量データでは `group=False` にして最後だけ集計することでパフォーマンスを向上できます。
- **SearchResult.to_df()**: APIを削除（`.df` を使用）。
- **SearchResult.normalize_queries()**: `prefer_by` の検証を厳格化（不正な列指定はエラー）。
- **ReportResult.group()**: 空DataFrameやメトリクス欠損時の挙動を安定化。
- **GA4 フィルタ解析**: Search/GSC と共通のフィルタパーサーに統一。

### 修正

- **Search Console クエリ整理**: `GSCService.query()` の dimensions を `date/hour/country/device/page/query` に限定し、`month` 指定時は内部的に `date` で取得して月単位で集計します。
- **Report 表示/保存**: `mg.report.show()` / `mg.report.download()` が `self.data` を参照するように修正しました。
- **report.prep**: デフォルトの参照DFと `report.data` 更新の不整合を修正。
- **Search Console フィルタ**: 不正なフィルタ指定時のエラーメッセージを改善。
- **ドキュメント更新**: design.md に新しい config フィールドと運用例を追記。`docs/cheatsheet.md` に `run.all()` の使い方を追記。`run.all` の説明も整理しました。


## 0.7.0 – 2026‑01‑01

### 追加

- **Search Console 対応の刷新**: `mg.search` を Google Search Console へのインタフェースとして導入しました。これにより、`mg.search.sites` でサイト一覧の取得、`mg.search.get.sites()` で再取得、`mg.search.use(site_url)` で対象サイトの選択、`mg.search.set.dates()` / `mg.search.set.months()` で期間設定、`mg.search.run()` でデータ取得が行えます。
- **レポート期間セル書き込み**: `mg.report.dates.to.sheet(sheet, start_cell, end_cell)` を追加し、設定したレポート期間をスプレッドシートに直接書き込めるようにしました。
- **Sheets ヘルパーの拡充**: 新しい `mg.sheets` と `mg.sheet` API により、ワークシートの選択 (`mg.sheets.select(name)`)、作成 (`mg.sheets.create(name)`)、削除 (`mg.sheets.delete(name)`)、および現在のシートへの保存・追記・アップサート (`mg.sheet.save(df)`, `mg.sheet.append(df)`, `mg.sheet.upsert(df, keys, columns?, sort_by?)`) を簡潔に実行できます。これにより Notebook での操作がより直感的になりました。

### 変更

- **月次ウィンドウ設定**: GA4 レポートおよび Search Console クエリで期間を月単位で指定する際の引数を統一しました。`ago` と `window_months` を用いて過去何か月前から何か月間の窓を取得するかを明示します。デフォルトでは `ago=1`, `window_months=13` です。

### 修正

- ドキュメントを大幅に更新し、README.md と Advanced Guide を整理しました。`mg.search` の紹介と Sheets 操作に関する手順を追記しました。

## 0.6.0 – 2025‑12‑31

### 変更

- **認証プロバイダの分離**: 互換性を保持したままリファクタリングしました。
- **サービスの独立**: Sheets / BigQuery サービスを独立させました。
- **ipywidgets の遅延インポート**: headless 環境でも動作しやすくなりました。
- **GitHub Actions ワークフロー**: 高速 pytest ワークフローを追加しました。

### 修正

- **ドキュメント更新**: README / Advanced guide を更新し、smoke notebook のガイダンスを追加しました。

このリリースは主に内部のリファクタリングと安定性向上が中心です。公開 API の後方互換性は維持されています。
