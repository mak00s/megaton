import pytest
import pandas as pd
from megaton.start import SearchResult


def test_search_result_df_property():
    """df プロパティで DataFrame にアクセスできる"""
    df = pd.DataFrame({'query': ['test'], 'clicks': [10]})
    result = SearchResult(df, None, ['query'])
    assert isinstance(result.df, pd.DataFrame)
    assert len(result.df) == 1


def test_decode():
    """decode() が URL デコードを行う"""
    df = pd.DataFrame({
        'page': ['https://example.com/%E3%83%86%E3%82%B9%E3%83%88'],
        'clicks': [10]
    })
    result = SearchResult(df, None, ['page'])
    decoded = result.decode(group=False)
    assert 'テスト' in decoded.df['page'].iloc[0]


def test_remove_params():
    """remove_params() がクエリパラメータを削除する"""
    df = pd.DataFrame({
        'page': ['https://example.com/page?utm_source=test&foo=bar'],
        'clicks': [10]
    })
    result = SearchResult(df, None, ['page'])
    
    # すべて削除
    cleaned = result.remove_params(group=False)
    assert '?' not in cleaned.df['page'].iloc[0]
    
    # utm_source のみ保持
    result2 = SearchResult(df.copy(), None, ['page'])
    cleaned2 = result2.remove_params(keep=['utm_source'], group=False)
    assert 'utm_source' in cleaned2.df['page'].iloc[0]
    assert 'foo' not in cleaned2.df['page'].iloc[0]


def test_remove_fragment():
    """remove_fragment() がフラグメントを削除する"""
    df = pd.DataFrame({
        'page': ['https://example.com/page#section'],
        'clicks': [10]
    })
    result = SearchResult(df, None, ['page'])
    cleaned = result.remove_fragment(group=False)
    assert '#' not in cleaned.df['page'].iloc[0]


def test_lower():
    """lower() が小文字化する"""
    df = pd.DataFrame({
        'page': ['https://Example.com/PAGE'],
        'clicks': [10]
    })
    result = SearchResult(df, None, ['page'])
    lowered = result.lower(columns=['page'], group=False)
    assert lowered.df['page'].iloc[0] == 'https://example.com/page'


def test_filter_clicks_applies_threshold():
    """filter_clicks は全行に閾値を適用"""
    df = pd.DataFrame({
        'query': ['a', 'b', 'c'],
        'clicks': [0, 1, 2],
        'impressions': [100, 100, 100]
    })
    result = SearchResult(df, None, ['query'])
    filtered = result.filter_clicks(min=2)
    # clicks >= 2 の行のみ
    assert len(filtered.df) == 1
    assert filtered.df['query'].iloc[0] == 'c'


def test_filter_impressions_keep_clicked_true():
    """keep_clicked=True で clicks >= 1 が残る"""
    df = pd.DataFrame({
        'query': ['a', 'b', 'c'],
        'clicks': [0, 1, 0],
        'impressions': [5, 5, 15]
    })
    result = SearchResult(df, None, ['query'])
    filtered = result.filter_impressions(min=10, keep_clicked=True)
    # clicks=1 の行（b）と impressions>=10 の行（c）が残る
    assert len(filtered.df) == 2
    assert set(filtered.df['query']) == {'b', 'c'}


def test_filter_with_sites():
    """sites パラメータで行ごとの閾値が適用される"""
    df = pd.DataFrame({
        'site': ['A', 'B', 'A'],
        'impressions': [5, 50, 150],
        'clicks': [1, 1, 1]
    })
    sites = [
        {'site': 'A', 'min_impressions': 10},
        {'site': 'B', 'min_impressions': 100}
    ]
    result = SearchResult(df, None, ['site'])
    filtered = result.filter_impressions(sites=sites, site_key='site', keep_clicked=False)
    # A: 5 < 10 で除外, 150 >= 10 で残る
    # B: 50 < 100 で除外
    assert len(filtered.df) == 1
    assert filtered.df['impressions'].iloc[0] == 150


def test_filter_with_sites_keep_clicked():
    """sites + keep_clicked で clicks >= 1 が優先される"""
    df = pd.DataFrame({
        'site': ['A', 'A', 'A'],
        'impressions': [5, 50, 150],
        'clicks': [0, 1, 1]
    })
    sites = [{'site': 'A', 'min_impressions': 100}]
    result = SearchResult(df, None, ['site'])
    filtered = result.filter_impressions(sites=sites, site_key='site', keep_clicked=True)
    # clicks=0 で impressions=5 → 除外
    # clicks>=1 → 無条件に残る（impressions < 100 でも）
    assert len(filtered.df) == 2
    assert set(filtered.df['impressions']) == {50, 150}


def test_method_chaining():
    """メソッドチェーンが正しく動作する"""
    df = pd.DataFrame({
        'page': ['https://Example.com/Page?foo=bar#section'],
        'query': ['test query'],
        'clicks': [10],
        'impressions': [100]
    })
    result = SearchResult(df, None, ['page', 'query'])
    
    final = (result
        .decode(group=False)
        .remove_params(group=False)
        .remove_fragment(group=False)
        .lower(columns=['page'], group=False))
    
    page = final.df['page'].iloc[0]
    assert 'example.com' in page
    assert '?' not in page
    assert '#' not in page


def test_filter_explicit_value_priority():
    """明示的な min/max が sites より優先される"""
    df = pd.DataFrame({
        'site': ['A', 'A'],
        'impressions': [50, 150],
        'clicks': [1, 1]
    })
    sites = [{'site': 'A', 'min_impressions': 10}]
    result = SearchResult(df, None, ['site'])
    
    # 明示的な min=100 が優先される
    filtered = result.filter_impressions(min=100, sites=sites, site_key='site', keep_clicked=False)
    assert len(filtered.df) == 1
    assert filtered.df['impressions'].iloc[0] == 150


def test_decode_with_group():
    """decode(group=True) が正しく集計する"""
    df = pd.DataFrame({
        'page': ['https://example.com/%E3%83%86%E3%82%B9%E3%83%88', 'https://example.com/%E3%83%86%E3%82%B9%E3%83%88'],
        'clicks': [10, 20],
        'impressions': [100, 200]
    })
    result = SearchResult(df, None, ['page'])
    decoded = result.decode(group=True)
    # group=True なので1行に集計される
    assert len(decoded.df) == 1
    assert decoded.df['clicks'].iloc[0] == 30
    assert decoded.df['impressions'].iloc[0] == 300


def test_decode_with_position():
    """decode(group=True) が position を重み付き平均で集計する"""
    df = pd.DataFrame({
        'page': ['https://example.com/%E3%83%86%E3%82%B9%E3%83%88', 'https://example.com/%E3%83%86%E3%82%B9%E3%83%88'],
        'clicks': [10, 20],
        'impressions': [100, 200],
        'position': [5.0, 8.0]
    })
    result = SearchResult(df, None, ['page'])
    decoded = result.decode(group=True)
    # 重み付き平均: (5.0 * 100 + 8.0 * 200) / (100 + 200) = 7.0
    assert len(decoded.df) == 1
    assert decoded.df['position'].iloc[0] == 7.0


def test_classify_with_group():
    """classify(group=True) が分類列を保持して集計する"""
    df = pd.DataFrame({
        'query': ['test tokyo', 'test osaka', 'sample tokyo'],
        'clicks': [10, 20, 30],
        'impressions': [100, 200, 300],
        'position': [5.0, 6.0, 7.0]
    })
    # map_by_regex は lower=True がデフォルト
    query_map = {r'test': 'test_category', r'sample': 'sample_category'}
    result = SearchResult(df, None, ['query'])
    
    # query パラメータにマッピングを渡す
    classified = result.classify(query=query_map, group=True)
    
    # 分類列 query_category が追加される
    assert 'query_category' in classified.df.columns
    
    # group=True なので、dimensions=['query'] + query_category で集計される
    # つまり元の query はそのまま残り、query_category も追加される
    # 'test tokyo' と 'test osaka' は異なる query なので2行
    # 'sample tokyo' は1行
    # 合計3行になる
    
    # これは実は正しい動作 - dimensions=['query'] + ['query_category'] でグループ化
    # query_category だけで集計したい場合は aggregate(by='query_category') を使う
    assert len(classified.df) == 3
    
    # query_category のみで集計してみる
    by_category = classified.aggregate(by='query_category')
    assert len(by_category.df) == 2
    
    # test_category の合計
    test_row = by_category.df[by_category.df['query_category'] == 'test_category']
    assert len(test_row) == 1
    assert test_row['clicks'].iloc[0] == 30
    assert test_row['impressions'].iloc[0] == 300



def test_aggregate_updates_dimensions():
    """aggregate(by=...) が dimensions を更新する"""
    df = pd.DataFrame({
        'query': ['test1', 'test2'],
        'page': ['page1', 'page2'],
        'clicks': [10, 20],
        'impressions': [100, 200]
    })
    result = SearchResult(df, None, ['query', 'page'])
    
    # query_category を追加
    df2 = result._df.copy()
    df2['query_category'] = ['cat1', 'cat1']
    result2 = SearchResult(df2, result.parent, ['query', 'page'])
    
    # query_category で集計
    aggregated = result2.aggregate(by='query_category')
    
    # dimensions が更新されて query_category になっている
    assert aggregated.dimensions == ['query_category']
    assert len(aggregated.df) == 1
    assert aggregated.df['clicks'].iloc[0] == 30


def test_keep_clicked_with_nan():
    """keep_clicked が NaN clicks を正しく扱う"""
    df = pd.DataFrame({
        'query': ['test1', 'test2', 'test3'],
        'impressions': [50, 150, 200],
        'clicks': [1, 0, None]  # NaN clicks
    })
    result = SearchResult(df, None, ['query'])
    
    # keep_clicked=True で min=100 を適用
    filtered = result.filter_impressions(min=100, keep_clicked=True)
    
    # test1 (clicks=1) は無条件に残る
    # test2 (clicks=0, impressions=150) は閾値を満たすので残る
    # test3 (clicks=NaN, impressions=200) は NaN なので残る
    assert len(filtered.df) == 3


def test_lower_no_mutable_default():
    """lower() のデフォルト引数が可変でないことを確認"""
    df = pd.DataFrame({
        'page': ['https://EXAMPLE.COM/PAGE'],
        'clicks': [10]
    })
    result1 = SearchResult(df.copy(), None, ['page'])
    result2 = SearchResult(df.copy(), None, ['page'])
    
    # 両方 default (columns=None → ['page']) で実行
    lower1 = result1.lower(group=False)
    lower2 = result2.lower(group=False)
    
    # 両方とも小文字化されている
    assert 'example.com' in lower1.df['page'].iloc[0]
    assert 'example.com' in lower2.df['page'].iloc[0]


def test_ctr_recalculated_after_aggregation():
    """CTR が集計後に正しく再計算される（元データに CTR がある場合）"""
    df = pd.DataFrame({
        'page': ['https://example.com/page1', 'https://example.com/page1'],
        'clicks': [10, 20],
        'impressions': [100, 300],
        'ctr': [0.1, 0.0666667]  # 元の CTR
    })
    result = SearchResult(df, None, ['page'])
    
    # group=True で集計
    aggregated = result.decode(group=True)
    
    # CTR が再計算されている: 30 / 400 = 0.075
    assert len(aggregated.df) == 1
    assert aggregated.df['clicks'].iloc[0] == 30
    assert aggregated.df['impressions'].iloc[0] == 400
    assert 'ctr' in aggregated.df.columns
    assert abs(aggregated.df['ctr'].iloc[0] - 0.075) < 0.001


def test_ctr_not_added_when_absent():
    """元データに CTR がない場合は追加されない"""
    df = pd.DataFrame({
        'page': ['https://example.com/page1', 'https://example.com/page1'],
        'clicks': [10, 20],
        'impressions': [100, 300]
        # ctr なし
    })
    result = SearchResult(df, None, ['page'])
    
    # group=True で集計
    aggregated = result.decode(group=True)
    
    # CTR 列は追加されない
    assert len(aggregated.df) == 1
    assert 'ctr' not in aggregated.df.columns


def test_classify_updates_dimensions():
    """classify(group=True) が dimensions を更新する"""
    df = pd.DataFrame({
        'query': ['test tokyo', 'test osaka', 'sample tokyo'],
        'clicks': [10, 20, 30],
        'impressions': [100, 200, 300]
    })
    query_map = {r'test': 'test_cat', r'sample': 'sample_cat'}
    result = SearchResult(df, None, ['query'])
    
    # query_category で分類・集計
    classified = result.classify(query=query_map, group=True)
    
    # dimensions が更新されている
    assert 'query_category' in classified.dimensions
    
    # 後続の group=True 呼び出しで category 列が保持される
    decoded = classified.decode(group=True)
    assert 'query_category' in decoded.df.columns
