import pytest
import pandas as pd
from megaton.start import SearchResult


def test_normalize_queries_basic():
    """normalize_queries() が空白バリエーションを統一する"""
    df = pd.DataFrame({
        'query': ['矯正歯科', '矯正 歯科', '矯正  歯科'],
        'clicks': [10, 5, 3],
        'impressions': [100, 50, 30],
        'position': [5.0, 6.0, 7.0]
    })
    result = SearchResult(df, None, ['query'])
    
    # 空白を削除して統一
    normalized = result.normalize_queries(mode='remove_all', prefer_by='impressions', group=True)
    
    # 1行に集約される
    assert len(normalized.df) == 1
    
    # 最もインプレッションが多い元クエリ「矯正歯科」が保持される
    assert normalized.df['query'].iloc[0] == '矯正歯科'
    
    # メトリクスは合算される
    assert normalized.df['clicks'].iloc[0] == 18
    assert normalized.df['impressions'].iloc[0] == 180
    
    # position は重み付き平均
    expected_position = (5.0 * 100 + 6.0 * 50 + 7.0 * 30) / 180
    assert abs(normalized.df['position'].iloc[0] - expected_position) < 0.01


def test_normalize_queries_with_dimensions():
    """normalize_queries() が複数ディメンションで動作する"""
    df = pd.DataFrame({
        'month': ['202412', '202412', '202412', '202412'],
        'page': ['/page1', '/page1', '/page2', '/page2'],
        'query': ['test tokyo', 'test  tokyo', 'sample', 'sample '],
        'clicks': [10, 5, 20, 8],
        'impressions': [100, 50, 200, 80]
    })
    result = SearchResult(df, None, ['month', 'page', 'query'])
    
    # 空白を削除して統一
    normalized = result.normalize_queries(mode='remove_all', prefer_by='impressions', group=True)
    
    # month + page + query_key で集約されるので2行
    assert len(normalized.df) == 2
    
    # /page1 の test tokyo バリエーション
    row1 = normalized.df[normalized.df['page'] == '/page1'].iloc[0]
    assert row1['query'] == 'test tokyo'  # より多い impressions の方
    assert row1['clicks'] == 15
    assert row1['impressions'] == 150
    
    # /page2 の sample バリエーション
    row2 = normalized.df[normalized.df['page'] == '/page2'].iloc[0]
    assert row2['query'] == 'sample'
    assert row2['clicks'] == 28
    assert row2['impressions'] == 280


def test_normalize_queries_prefer_by_clicks():
    """prefer_by='clicks' で最もクリックが多いクエリを選択"""
    df = pd.DataFrame({
        'query': ['test a', 'test  a', 'test   a'],
        'clicks': [5, 20, 3],  # 真ん中が最大
        'impressions': [100, 50, 30]
    })
    result = SearchResult(df, None, ['query'])
    
    normalized = result.normalize_queries(prefer_by='clicks', group=True)
    
    # clicks が最大の「test  a」が選ばれる
    assert normalized.df['query'].iloc[0] == 'test  a'
    assert normalized.df['clicks'].iloc[0] == 28


def test_normalize_queries_mode_collapse():
    """mode='collapse' で空白を1つに統一"""
    df = pd.DataFrame({
        'query': ['test  tokyo', 'test tokyo'],
        'clicks': [10, 5],
        'impressions': [100, 50]
    })
    result = SearchResult(df, None, ['query'])
    
    # 空白を1つに統一
    normalized = result.normalize_queries(mode='collapse', prefer_by='impressions', group=True)
    
    # 1行に集約される
    assert len(normalized.df) == 1
    assert normalized.df['clicks'].iloc[0] == 15


def test_normalize_queries_no_query_column():
    """query 列がない場合は何もしない"""
    df = pd.DataFrame({
        'page': ['/page1', '/page2'],
        'clicks': [10, 20]
    })
    result = SearchResult(df, None, ['page'])
    
    normalized = result.normalize_queries()
    
    # 変化なし
    assert len(normalized.df) == 2
    assert 'query' not in normalized.df.columns


def test_normalize_queries_group_false():
    """group=False で集約をスキップ"""
    df = pd.DataFrame({
        'query': ['test a', 'test  a'],
        'clicks': [10, 5],
        'impressions': [100, 50]
    })
    result = SearchResult(df, None, ['query'])
    
    normalized = result.normalize_queries(group=False)
    
    # 集約されず、query_key 列が追加される
    assert len(normalized.df) == 2
    assert 'query_key' in normalized.df.columns
    assert normalized.df['query_key'].iloc[0] == 'testa'
    assert normalized.df['query_key'].iloc[1] == 'testa'


def test_normalize_queries_chaining():
    """normalize_queries() がメソッドチェーンで動作する"""
    df = pd.DataFrame({
        'query': ['test tokyo', 'test  tokyo', 'sample'],  # 全て小文字に統一
        'clicks': [10, 5, 20],
        'impressions': [100, 50, 200]
    })
    result = SearchResult(df, None, ['query'])
    
    # チェーン: normalize → classify
    query_map = {r'test': 'test_cat', r'sample': 'sample_cat'}
    final = (result
        .normalize_queries(mode='remove_all', prefer_by='impressions', group=True)
        .classify(query=query_map, group=True))
    
    # test tokyo バリエーションが統一されて1行、sample が1行、合計2行
    assert len(final.df) == 2
    
    # query_category が追加されている
    assert 'query_category' in final.df.columns
    
    # test カテゴリ
    test_row = final.df[final.df['query_category'] == 'test_cat'].iloc[0]
    assert test_row['clicks'] == 15
    assert test_row['impressions'] == 150
    
    # sample カテゴリ
    sample_row = final.df[final.df['query_category'] == 'sample_cat'].iloc[0]
    assert sample_row['clicks'] == 20
    assert sample_row['impressions'] == 200


def test_normalize_queries_dimensions_preserved():
    """normalize_queries() が元の dimensions を保持する"""
    df = pd.DataFrame({
        'month': ['202412', '202412'],
        'query': ['test', 'test '],
        'clicks': [10, 5],
        'impressions': [100, 50]
    })
    result = SearchResult(df, None, ['month', 'query'])
    
    normalized = result.normalize_queries(group=True)
    
    # dimensions は元のまま
    assert normalized.dimensions == ['month', 'query']
    
    # query_key は削除されている
    assert 'query_key' not in normalized.df.columns
    
    # 1行に集約されている
    assert len(normalized.df) == 1
    assert normalized.df['clicks'].iloc[0] == 15


def test_normalize_queries_position_prefers_minimum():
    """position を prefer_by に指定すると最小値（最良順位）を選択する"""
    df = pd.DataFrame({
        'query': ['渋谷 歯医者', '渋谷  歯医者', '渋谷   歯医者'],
        'clicks': [10, 5, 3],
        'impressions': [100, 50, 30],
        'position': [5.0, 2.0, 10.0]  # 2.0 が最良順位
    })
    result = SearchResult(df, None, ['query'])
    
    # position で代表クエリを選択
    normalized = result.normalize_queries(mode='remove_all', prefer_by='position', group=True)
    
    # 1行に集約される
    assert len(normalized.df) == 1
    
    # position が最小（最良）の行「渋谷  歯医者」が選ばれる
    assert normalized.df['query'].iloc[0] == '渋谷  歯医者'
    
    # メトリクスは合算される
    assert normalized.df['clicks'].iloc[0] == 18
    assert normalized.df['impressions'].iloc[0] == 180


def test_normalize_queries_group_false_no_dedup():
    """group=False の場合、query_key のみ追加してdeduplicationしない"""
    df = pd.DataFrame({
        'query': ['矯正 歯科', '矯正  歯科', 'インプラント'],
        'clicks': [10, 5, 20],
        'impressions': [100, 50, 200]
    })
    result = SearchResult(df, None, ['query'])
    
    # group=False: query_key のみ追加、集約なし
    normalized = result.normalize_queries(mode='remove_all', group=False)
    
    # 元の行数を保持
    assert len(normalized.df) == 3
    
    # query_key 列が追加される
    assert 'query_key' in normalized.df.columns
    assert normalized.df['query_key'].tolist() == ['矯正歯科', '矯正歯科', 'インプラント']
    
    # 元のクエリは保持される
    assert normalized.df['query'].tolist() == ['矯正 歯科', '矯正  歯科', 'インプラント']
    
    # メトリクスは元のまま
    assert normalized.df['clicks'].tolist() == [10, 5, 20]


def test_normalize_queries_missing_prefer_by_with_group():
    """group=True で prefer_by 列が存在しない場合は ValueError"""
    df = pd.DataFrame({
        'query': ['test', 'test '],
        'clicks': [10, 5]
    })
    result = SearchResult(df, None, ['query'])
    
    # impressions 列が存在しないのでエラー
    with pytest.raises(ValueError, match="Missing prefer_by columns"):
        result.normalize_queries(prefer_by='impressions', group=True)


def test_normalize_queries_prefer_by_must_be_string():
    """prefer_by は文字列以外を拒否する"""
    df = pd.DataFrame({
        'query': ['test', 'test '],
        'clicks': [10, 5],
        'impressions': [100, 50]
    })
    result = SearchResult(df, None, ['query'])
    
    # prefer_by にリストを渡すとエラー
    with pytest.raises(TypeError, match="prefer_by must be a string, got list"):
        result.normalize_queries(prefer_by=['impressions', 'clicks'], group=True)
