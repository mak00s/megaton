"""
SearchResult.apply_if() メソッドのテスト
"""
import pandas as pd
import pytest
from megaton.start import SearchResult
from types import SimpleNamespace


def test_apply_if_with_true_condition():
    """条件が True の場合、メソッドが適用される"""
    df = pd.DataFrame({
        'query': ['test1', 'test2'],
        'clicks': [5, 10],
        'impressions': [100, 200],
    })
    parent = SimpleNamespace()
    result = SearchResult(df, parent, ['query'])
    
    # 条件が True なので filter_clicks が適用される
    filtered = result.apply_if(True, 'filter_clicks', min=7)
    
    assert len(filtered.df) == 1
    assert filtered.df.iloc[0]['clicks'] == 10


def test_apply_if_with_false_condition():
    """条件が False の場合、メソッドが適用されない"""
    df = pd.DataFrame({
        'query': ['test1', 'test2'],
        'clicks': [5, 10],
        'impressions': [100, 200],
    })
    parent = SimpleNamespace()
    result = SearchResult(df, parent, ['query'])
    
    # 条件が False なのでフィルタは適用されない
    filtered = result.apply_if(False, 'filter_clicks', min=7)
    
    assert len(filtered.df) == 2
    assert list(filtered.df['clicks']) == [5, 10]


def test_apply_if_with_callable_condition():
    """callable な条件を使用できる"""
    df = pd.DataFrame({
        'query': ['test1', 'test2', 'test3'],
        'clicks': [5, 10, 15],
        'impressions': [100, 200, 300],
    })
    parent = SimpleNamespace()
    result = SearchResult(df, parent, ['query'])
    
    # データ量に応じた条件分岐
    filtered = result.apply_if(
        lambda sr: len(sr.df) > 2,  # 3行あるので True
        'filter_clicks',
        min=7
    )
    
    assert len(filtered.df) == 2  # clicks >= 7 のみ残る
    assert list(filtered.df['clicks']) == [10, 15]


def test_apply_if_callable_condition_false():
    """callable な条件が False の場合"""
    df = pd.DataFrame({
        'query': ['test1', 'test2'],
        'clicks': [5, 10],
        'impressions': [100, 200],
    })
    parent = SimpleNamespace()
    result = SearchResult(df, parent, ['query'])
    
    # 条件が False なのでフィルタは適用されない
    filtered = result.apply_if(
        lambda sr: len(sr.df) > 5,  # 2行しかないので False
        'filter_clicks',
        min=7
    )
    
    assert len(filtered.df) == 2
    assert list(filtered.df['clicks']) == [5, 10]


def test_apply_if_chaining():
    """メソッドチェーンで複数の apply_if を使用できる"""
    df = pd.DataFrame({
        'query': ['test1', 'test2', 'test3', 'test4'],
        'clicks': [1, 5, 10, 15],
        'impressions': [50, 100, 200, 300],
    })
    parent = SimpleNamespace()
    result = SearchResult(df, parent, ['query'])
    
    # 条件付きで複数のフィルタを適用
    filtered = (
        result
        .apply_if(True, 'filter_clicks', min=5)     # clicks >= 5
        .apply_if(True, 'filter_impressions', min=150)  # impressions >= 150
        .apply_if(False, 'filter_clicks', max=5)   # これは適用されない
    )
    
    assert len(filtered.df) == 2  # clicks >= 5 AND impressions >= 150
    assert list(filtered.df['clicks']) == [10, 15]


def test_apply_if_with_variable_condition():
    """実際のユースケース：TARGET_MONTHS_AGO 風の条件"""
    df = pd.DataFrame({
        'query': ['test1', 'test2', 'test3'],
        'clicks': [1, 5, 10],
        'impressions': [50, 100, 200],
    })
    parent = SimpleNamespace()
    result = SearchResult(df, parent, ['query'])
    
    TARGET_MONTHS_AGO = 0  # 最新月
    
    # 最新月はフィルタなし
    filtered_latest = (
        result
        .apply_if(TARGET_MONTHS_AGO > 0, 'filter_impressions', min=100)
        .apply_if(TARGET_MONTHS_AGO > 0, 'filter_clicks', min=5)
    )
    
    assert len(filtered_latest.df) == 3  # フィルタされない
    
    TARGET_MONTHS_AGO = 1  # 過去月
    
    # 過去月はフィルタあり
    filtered_past = (
        result
        .apply_if(TARGET_MONTHS_AGO > 0, 'filter_impressions', min=100)
        .apply_if(TARGET_MONTHS_AGO > 0, 'filter_clicks', min=5)
    )
    
    assert len(filtered_past.df) == 2  # impressions >= 100 AND clicks >= 5
    assert list(filtered_past.df['clicks']) == [5, 10]


def test_apply_if_nonexistent_method():
    """存在しないメソッド名の場合は AttributeError"""
    df = pd.DataFrame({'query': ['test1'], 'clicks': [5]})
    parent = SimpleNamespace()
    result = SearchResult(df, parent, ['query'])
    
    with pytest.raises(AttributeError):
        result.apply_if(True, 'nonexistent_method')


def test_apply_if_preserves_dimensions():
    """apply_if は dimensions を保持する"""
    df = pd.DataFrame({
        'query': ['test1', 'test2'],
        'page': ['/a', '/b'],
        'clicks': [5, 10],
    })
    parent = SimpleNamespace()
    result = SearchResult(df, parent, ['query', 'page'])
    
    filtered = result.apply_if(True, 'filter_clicks', min=7)
    
    assert filtered.dimensions == ['query', 'page']
    assert len(filtered.df) == 1


def test_apply_if_with_kwargs():
    """kwargs を正しく渡せる"""
    df = pd.DataFrame({
        'query': ['test1', 'test2', 'test3'],
        'clicks': [1, 5, 10],
        'impressions': [50, 100, 200],
    })
    parent = SimpleNamespace()
    result = SearchResult(df, parent, ['query'])
    
    # min と max の両方を指定
    filtered = result.apply_if(True, 'filter_clicks', min=3, max=8)
    
    assert len(filtered.df) == 1
    assert filtered.df.iloc[0]['clicks'] == 5
