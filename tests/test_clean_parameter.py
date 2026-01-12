"""Tests for clean=True parameter in mg.search.run()"""
import pytest
import pandas as pd
from megaton.start import SearchResult


def test_clean_aggregation_with_weighted_position():
    """SearchResult の clean 相当の処理が position を重み付き平均で集計する"""
    
    # URL 正規化前のデータ
    df = pd.DataFrame({
        'page': [
            'https://example.com/Page?utm=123',
            'https://EXAMPLE.COM/page?utm=456',  # 異なる大文字小文字とパラメータ
        ],
        'clicks': [10, 20],
        'impressions': [100, 200],
        'position': [3.0, 6.0]
    })
    
    result = SearchResult(df, None, ['page'])
    
    # clean 相当の処理: decode → remove_params → remove_fragment → lower
    cleaned = (result
               .decode(group=False)
               .remove_params(group=False)
               .remove_fragment(group=False)
               .lower(columns=['page'], group=True))  # 最後に group=True で集計
    
    # page が正規化されて1行に集計される
    assert len(cleaned.df) == 1
    
    # clicks と impressions は合計
    assert cleaned.df['clicks'].iloc[0] == 30
    assert cleaned.df['impressions'].iloc[0] == 300
    
    # position は重み付き平均: (3.0 * 100 + 6.0 * 200) / 300 = 5.0
    assert cleaned.df['position'].iloc[0] == 5.0
    
    # page が正規化されている（小文字、パラメータなし）
    assert 'example.com/page' in cleaned.df['page'].iloc[0]


def test_clean_without_position():
    """position がない場合も正しく動作"""
    
    df = pd.DataFrame({
        'page': [
            'https://example.com/Page',
            'https://EXAMPLE.COM/page',
        ],
        'clicks': [10, 20],
        'impressions': [100, 200]
        # position なし
    })
    
    result = SearchResult(df, None, ['page'])
    cleaned = result.lower(columns=['page'], group=True)
    
    # 1行に集計される
    assert len(cleaned.df) == 1
    assert cleaned.df['clicks'].iloc[0] == 30
    assert cleaned.df['impressions'].iloc[0] == 300
    
    # position 列が存在しない
    assert 'position' not in cleaned.df.columns


def test_clean_with_ctr():
    """CTR が集計後に正しく再計算される"""
    
    df = pd.DataFrame({
        'page': [
            'https://example.com/Page',
            'https://EXAMPLE.COM/page',
        ],
        'clicks': [10, 20],
        'impressions': [100, 300],
        'ctr': [0.1, 0.0666667]  # GSC が返す元の CTR
    })
    
    result = SearchResult(df, None, ['page'])
    cleaned = result.lower(columns=['page'], group=True)
    
    # 1行に集計される
    assert len(cleaned.df) == 1
    assert cleaned.df['clicks'].iloc[0] == 30
    assert cleaned.df['impressions'].iloc[0] == 400
    
    # CTR が再計算されている: 30 / 400 = 0.075
    assert 'ctr' in cleaned.df.columns
    expected_ctr = 30 / 400
    assert abs(cleaned.df['ctr'].iloc[0] - expected_ctr) < 0.001
