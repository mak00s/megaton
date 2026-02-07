# Changelog

このファイルは `1.0.0` 以降の変更履歴を記録します。  
`0.x` 系の履歴は `docs/changelog-archive.md` を参照してください。

## 1.0.0 - 2026-02-07

### 追加

- **GA4 report retry**: `mg.report.run()` に `max_retries` / `backoff_factor` を追加し、`ServiceUnavailable` 時に指数バックオフ再試行を実装。
- **GA4 user properties 表示**: `mg.ga["4"].property.show("user_properties")` を追加。
- **report prep 表示制御**: `mg.report.prep(show=False)` を追加。
- **Sheets save 開始行指定**: `mg.save.to.sheet()` / `mg.sheet.save()` に `start_row` を追加。
- **Sheets save/append 自動作成オプション**: `mg.save.to.sheet()` / `mg.append.to.sheet()` に `create_if_missing` を追加。
- **CSV upsert**: `mg.upsert.to.csv()` を追加。
- **Search 日付テンプレート**: `mg.search.set.dates()` で `NdaysAgo` / `yesterday` / `today` をサポート。

### 変更

- **パッケージ配布修正**: `setup.py` の `long_description_content_type` を `text/markdown` に修正し、`install_requires` を `requirements.txt` から読み込むように更新。
- **配布物修正**: `MANIFEST.in` を追加して `requirements.txt` を sdist に同梱。
- **テスト拡充**: `sheets_service` / `gsheet` / `ga4 report` の分岐テストを追加。
- **ドキュメント整理**: API リファレンス、チートシート、README を現行仕様に合わせて更新。
