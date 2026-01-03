# SearchResult API 使用例

このドキュメントは新しい SearchResult メソッドチェーン API の使い方を説明します。

**全 API の概要は [API リファレンス](api-reference.md#searchresult-メソッドチェーン) を参照してください。**

## 基本的な使い方

```python
import megaton as mt

mg = mt.start()

# シンプル: 自動 URL 正規化と重み付けポジション集約
result = mg.search.run(dimensions=['query', 'page'], clean=True)
df = result.df

# カスタマイズ: ステップバイステップ処理
result = (mg.search
    .run(dimensions=['query', 'page'])
    .decode()
    .remove_params(keep=['utm_source'])
    .remove_fragment()
    .lower(columns=['page']))

df = result.df
```

## 集約の挙動

`group=True`（デフォルト）の場合、SearchResult は自動的にデータを集約します：
- **Position**: インプレッションによる重み付け平均（正しい数学的集約）
- **CTR**: `clicks / impressions` として再計算（**元データに CTR が存在する場合のみ**）
- **その他の指標**: 単純合計（clicks, impressions）

```python
# CTR は入力データに存在する場合のみ再計算される
result = mg.search.run(dimensions=['page'], metrics=['clicks', 'impressions', 'ctr'])
aggregated = result.decode(group=True)  # CTR が再計算される

result2 = mg.search.run(dimensions=['page'], metrics=['clicks', 'impressions'])
aggregated2 = result2.decode(group=True)  # CTR 列は追加されない
```

## 分類とフィルタリング

```python
# クエリとページの分類とフィルタリング
result = (mg.search
    .run(dimensions=['month', 'query', 'page'], clean=True)
    .classify(query=cfg.query_map, page=cfg.page_map)
    .filter_clicks(min=1)
    .filter_impressions(sites=cfg.sites, keep_clicked=True)
    .filter_position(sites=cfg.sites, keep_clicked=True))

df = result.df
```

## クエリの空白バリエーション統一

```python
# "矯正 歯科", "矯正  歯科" などを "矯正歯科" に統一
result = (mg.search
    .run(dimensions=['month', 'query', 'page'], clean=True)
    .normalize_queries(mode='remove_all', prefer_by='impressions')  # 空白削除
    .classify(query=cfg.query_map, page=cfg.page_map)
    .filter_impressions(sites=cfg.sites, keep_clicked=True))

# 各バリエーションの中で最もインプレッションが多い元クエリが保持される
df = result.df
```

## 複数サイトの一括処理

```python
# run.all() は自動的に item_key を dimensions に含める
result = mg.search.run.all(
    items=cfg.sites,
    dimensions=['query', 'page'],
    site_url_key='gsc_site_url',
    item_key='clinic'
)

# 'clinic' 列はメソッドチェーンで保持される
classified = result.classify(query=cfg.query_map, page=cfg.page_map)
# dimensions は ['query', 'page', 'clinic', 'query_category', 'page_category'] になる

filtered = classified.filter_impressions(
    sites=cfg.sites,
    site_key='clinic',
    keep_clicked=True
)

# チェーン後も 'clinic' は存在する
df = filtered.df
assert 'clinic' in df.columns
```

## 分類による dimensions の更新

```python
# classify() はカテゴリ列を dimensions に追加する
result = (mg.search
    .run(dimensions=['query', 'page'], clean=True)
    .classify(query=cfg.query_map, page=cfg.page_map, group=True))

# dimensions が更新される: ['query', 'page', 'query_category', 'page_category']
# カテゴリ列は後続の group=True メソッドでも保持される

decoded = result.decode(group=True)
assert 'query_category' in decoded.df.columns  # まだ存在する
```

## パフォーマンス最適化: 中間集約をスキップ

```python
# 大規模データセットの場合、最後まで集約をスキップ
result = (mg.search
    .run(dimensions=['month', 'query', 'page'])
    .decode(group=False)
    .remove_params(group=False)
    .remove_fragment(group=False)
    .lower(columns=['page'], group=False)
    .classify(query=cfg.query_map, page=cfg.page_map, group=True))  # 最後のみ集約
```

## keep_clicked パラメータ

```python
# clicks >= 1 の行は他の指標の閾値をバイパスする
result = (mg.search
    .run(dimensions=['query', 'page'], clean=True)
    .filter_clicks(min=1)  # clicks >= 1 のみ
    .filter_impressions(min=100, keep_clicked=True)  # clicks >= 1 は min=100 をバイパス
    .filter_position(max=20, keep_clicked=True))  # clicks >= 1 は max=20 をバイパス
```

## SearchResult メソッド一覧

### URL 処理
- `.decode(group=True)` – URL デコード (%xx → 文字)
- `.remove_params(keep=None, group=True)` – クエリパラメータを削除
- `.remove_fragment(group=True)` – フラグメント (#...) を削除
- `.lower(columns=None, group=True)` – 指定列を小文字化（デフォルト: `['page']`）

### クエリ正規化
- `.normalize_queries(mode='remove_all', prefer_by='impressions', group=True)` – クエリの空白バリエーションを統一
  - `mode='remove_all'`: 空白を削除（例: "矯正 歯科" → "矯正歯科"）
  - `mode='collapse'`: 複数空白を1つに統一
  - `prefer_by`: 代表クエリを選ぶ基準（'impressions', 'clicks', 'position'）
    - **'position'**: 最小値（最良順位）を選択（例: rank 2 が rank 10 より優先）
    - その他の指標: 最大値を選択（例: impressions が最も多いクエリを選択）
    - **注意**: 文字列のみ（リスト不可）。`group=True` の場合、指定した列がデータに存在する必要があります
  - `group=True`: 正規化後に集約し、代表クエリを保持
  - `group=False`: `query_key` 列のみ追加（集約なし、`prefer_by` は不要）

### 分類
- `.classify(query=None, page=None, group=True)` – クエリ/ページの正規化と分類
  - `query_category` と `page_category` 列を追加
  - `group=True` の場合、カテゴリ列を dimensions に追加

### フィルタリング
- `.filter_clicks(min, max, sites, site_key='site')` – クリック数でフィルタ
- `.filter_impressions(min, max, sites, site_key='site', keep_clicked=False)` – インプレッション数でフィルタ
- `.filter_ctr(min, max, sites, site_key='site', keep_clicked=False)` – CTR でフィルタ（metrics に `clicks` と `impressions` の両方が必要）
- `.filter_position(min, max, sites, site_key='site', keep_clicked=False)` – ポジションでフィルタ

### 集約
- `.aggregate(by=None)` – 指定列または dimensions による手動集約
  - `by` を指定した場合、チェーンのために dimensions を更新

### DataFrame アクセス
- `.df` – DataFrame にアクセスするプロパティ

### 一括処理
- `mg.search.run.all(items, dimensions, ...)` – 複数サイト/アイテムを処理
  - `SearchResult` を返す（DataFrame ではない）
  - 自動的に `item_key` を dimensions に追加
  - 全パラメータは docstring を参照

## 注意事項

- `group=True`（デフォルト）: 各操作後に dimensions で集約
- `group=False`: 集約をスキップ（大規模データセットのパフォーマンスに有効）
- `keep_clicked=False`（デフォルト）: すべての行に閾値を適用
- `keep_clicked=True`: clicks >= 1 の行は閾値をバイパス（明示的に指定する必要あり）
- 後方互換性: `.df` プロパティにより SearchResult は DataFrame のように振る舞う
