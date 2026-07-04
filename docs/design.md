# Design Notes

Megaton は Notebook での分析と配布を速く回すためのツールです。API の仕様は [api-reference.md](api-reference.md)、実用例は [cookbook.md](cookbook.md) を参照してください。

## 目的

- Notebook 上での試行錯誤を最短距離で回す
- 「取得 → 整形 → 保存」を少ないコードで繋ぐ
- 分析者がその場で判断できる UX を優先する

## Public API の設計ルール

Megaton の public API は、Google API / gspread / pandas の薄い wrapper ではなく、
Notebook 上で人間が分析作業を進めるための stateful app として設計します。

- **操作語彙を優先する**: public API は `mg.<domain>.<verb>` または
  `mg.<verb>.to.<target>` の形を基本にし、「何をするか」が先に読める名前にする。
  Google API の request / resource 名をそのまま主導線にはしない。
- **state を活かす**: 認証、選択中の GA4 property、日付範囲、開いている
  spreadsheet、選択中 worksheet、直近 result は `mg` の作業状態として扱う。
  state で自然に決まる値を、通常 API で毎回要求しない。
- **人間向けの引数名にする**: API 由来の正確さより、分析者が Notebook で
  直感的に書ける名前を優先する。例: `d` / `m` / `filter_d` / `sort` は
  report 操作の短い作業語彙として扱う。
- **canonical path を持つ**: 同じ操作に複数の入口がある場合でも、docs では
  推奨導線を 1 つに決める。互換・便利 API は残してもよいが、主導線と
  同じ重みで並べない。
- **raw access は escape hatch に隔離する**: `mg.gs.*`、gspread object、
  Google API client などの低レベルアクセスは必要な場合だけ使う上級者向け
  API とし、通常の cookbook / cheatsheet の中心に置かない。
- **失敗は静かに隠さない**: 利用者が誤った前提で集計結果を使う可能性がある
  場合は、空 DataFrame や `None` で流さず、明示的な例外または分かる
  メッセージにする。
- **追加より統合を優先する**: 新しい便利関数を増やす前に、既存の
  `mg.report` / `mg.search` / `mg.sheets` / `mg.sheet` / `Result` の語彙に
  収まるかを確認する。収まらない場合は、その理由を docs に残す。
- **表面積をレビューする**: public API を追加するときは、実装だけでなく
  README / cookbook / cheatsheet / API reference のどこに置くかを同時に決める。
  主要導線に載せるものは、Megaton の作業語彙として説明できるものに限る。
- **読み書きの動詞を対称にする**: 書き（`set` / `save`）を用意したら、読み
  （`get` / `read`）も同じ語彙で対にする。例: `mg.sheet.cell.set` には
  `mg.sheet.cell.get` を揃える。private helper の直呼びを主導線に露出しない。
- **クロスソースで語彙を揃える**: 同じ意味の操作は source を跨いで同じ引数名に
  する。API 由来名（例: GSC の `dimension_filter`）をそのまま主導線にせず、
  report / search で共通の絞り込み語彙（`where=` / `filter=` 方向）に寄せ、
  内部で GA4 / GSC 形式へ変換する。旧 API 由来名は互換 alias に降格する。
- **運用パラメータは作業語彙ではない**: `max_retries` / `backoff_factor` /
  `timeout` などの retry・timeout は通常操作のノイズ。引数として受けてよいが、
  主 signature・cheatsheet 主導線には出さず、instance / env 設定または
  `retry={...}` 等にまとめる。既定で動き、必要な人だけが触る位置に置く。

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

## 互換性ポリシー

- **安定 API**: `mg.*` のパブリックメソッド（引数名・戻り値型）、`SearchResult` / `ReportResult` のチェーンメソッド
- **破壊的変更**: メジャーバージョン（2.0.0）でのみ行い、CHANGELOG に明記する
- **対象外**: `_` prefix の内部メソッド、`megaton.transform` 等の内部モジュール構造は予告なく変更する場合がある

## 意図的にしないこと

- 汎用 SDK の網羅的な抽象化
- 本番 ETL / DWH パイプラインの置き換え
- 解析ロジックの自動推論や隠れた最適化

Megaton は「Notebook で速く試せること」を中心に設計されています。
