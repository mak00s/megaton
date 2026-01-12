# Design Notes

Megaton は Notebook での分析と配布を速く回すためのツールです。API の仕様は [api-reference.md](api-reference.md)、実用例は [cookbook.md](cookbook.md) を参照してください。

## 目的

- Notebook 上での試行錯誤を最短距離で回す
- 「取得 → 整形 → 保存」を少ないコードで繋ぐ
- 分析者がその場で判断できる UX を優先する

## Result ベースの API

- `SearchResult` / `ReportResult` を返し、メソッドチェーンで処理する
- 中間状態を DataFrame として即確認でき、Notebook の探索に適合する
- 状態は明示的に結果オブジェクトに保持し、暗黙の副作用を最小化する

## normalize / categorize / classify の分離

- **normalize**: 既存ディメンションの正規化（上書き）
- **categorize**: 元列を保持したままカテゴリ列を追加
- **classify**: 正規化 + 集約

分類の意図をメソッド名に固定し、集約の有無を明示します。Notebook での再現性と差分検証を優先しています。

## UI と headless の併存

- UI は「選択の迷い」を減らし、Notebook の導線を短縮するための設計
- headless は自動実行と差分検証に寄せた設計
- どちらも同じ API を使い、切り替えコストを小さくする

## 意図的にしないこと

- 汎用 SDK の網羅的な抽象化
- 本番 ETL / DWH パイプラインの置き換え
- 解析ロジックの自動推論や隠れた最適化

Megaton は「Notebook で速く試せること」を中心に設計されています。
