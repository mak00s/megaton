"""Simplified tests for mg.search.run.all() and mg.report.run.all()

These tests verify the basic functionality without complex mocking.
"""

from unittest.mock import patch
import pandas as pd
import pytest
from megaton import dates
from megaton.start import Megaton


def test_search_run_all_basic():
    """Test basic Search Console multi-item run"""
    app = Megaton(None, headless=True)
    app.search.start_date = "2025-01-01"
    app.search.end_date = "2025-01-31"
    
    mock_df = pd.DataFrame({
        'query': ['test query'],
        'clicks': [10],
        'impressions': [100],
    })
    
    with patch.object(app.search.run.__class__, '__call__', return_value=mock_df):
        with patch.object(app.search, 'use'):
            sites = [
                {'site': 'siteA', 'gsc_site_url': 'https://example.com/'},
                {'site': 'siteB', 'gsc_site_url': 'https://example2.com/'},
            ]
            
            result = app.search.run.all(
                sites,
                dimensions=['query'],
                metrics=['clicks', 'impressions'],
                verbose=False,
            )
    
    assert result is not None
    result_df = result.df
    assert isinstance(result_df, pd.DataFrame)
    assert len(result_df) == 2
    assert 'site' in result_df.columns
    assert set(result_df['site']) == {'siteA', 'siteB'}


def test_search_run_all_with_filter():
    """Test Search Console with item_filter as list"""
    app = Megaton(None, headless=True)
    app.search.start_date = "2025-01-01"
    app.search.end_date = "2025-01-31"
    
    mock_df = pd.DataFrame({'query': ['test'], 'clicks': [5]})
    
    with patch.object(app.search.run.__class__, '__call__', return_value=mock_df):
        with patch.object(app.search, 'use'):
            sites = [
                {'clinic': 'A', 'gsc_site_url': 'https://a.com/'},
                {'clinic': 'B', 'gsc_site_url': 'https://b.com/'},
                {'clinic': 'C', 'gsc_site_url': 'https://c.com/'},
            ]
            
            result = app.search.run.all(
                sites,
                dimensions=['query'],
                item_key='clinic',
                item_filter=['A', 'C'],
                verbose=False,
            )
    
    result_df = result.df
    assert len(result_df) == 2
    assert set(result_df['clinic']) == {'A', 'C'}


def test_search_run_all_url_fallback():
    """Test Search Console with gsc_site_url (no fallback to 'url')"""
    app = Megaton(None, headless=True)
    app.search.start_date = "2025-01-01"
    app.search.end_date = "2025-01-31"
    
    mock_df = pd.DataFrame({'query': ['test'], 'clicks': [5]})
    
    with patch.object(app.search.run.__class__, '__call__', return_value=mock_df):
        with patch.object(app.search, 'use'):
            # Sites must have gsc_site_url
            sites = [{'site': 'A', 'gsc_site_url': 'https://a.com/', 'url': 'https://a.com/'}]
            
            result = app.search.run.all(
                sites,
                dimensions=['query'],
                verbose=False,
            )
    
    result_df = result.df
    assert len(result_df) == 1
    assert result_df['site'].iloc[0] == 'A'


def test_report_run_all_basic():
    """Test basic GA4 multi-item run"""
    app = Megaton(None, headless=True)
    app.report.start_date = "2025-01-01"
    app.report.end_date = "2025-01-31"
    
    # Mock GA4 structure
    from types import SimpleNamespace
    app.ga['4'] = SimpleNamespace(property=SimpleNamespace(id=None))
    
    mock_df = pd.DataFrame({'date': ['2025-01-01'], 'users': [100]})
    
    # Mock the __call__ method and report.data
    with patch.object(app.report.run.__class__, '__call__'):
        app.report.data = mock_df
        
        sites = [
            {'site': 'siteA', 'ga4_property_id': '123456'},
            {'site': 'siteB', 'ga4_property_id': '789012'},
        ]
        
        result = app.report.run.all(
            sites,
            d=['date'],
            m=['users'],
            verbose=False,
        )
    
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2
    assert 'site' in result.columns
    assert set(result['site']) == {'siteA', 'siteB'}


def test_report_run_all_site_metric_prefix():
    """site. プレフィックスでサイト別メトリクスを指定"""
    app = Megaton(None, headless=True)
    app.report.start_date = "2025-01-01"
    app.report.end_date = "2025-01-31"

    from types import SimpleNamespace
    app.ga['4'] = SimpleNamespace(property=SimpleNamespace(id=None))

    metrics_used = []

    def mock_call(self, d, m, **kwargs):
        metrics_used.append(m)
        self.parent.data = pd.DataFrame({'users': [1]})

    with patch.object(app.report.run.__class__, '__call__', mock_call):
        sites = [
            {'clinic': '札幌', 'ga4_property_id': '12345', 'cv': 'totalPurchasers'},
            {'clinic': '仙台', 'ga4_property_id': '67890', 'cv': 'keyEvents'},
        ]

        result = app.report.run.all(
            sites,
            d=[('yearMonth', 'month')],
            m=[('site.cv', 'cv')],
            item_key='clinic',
            verbose=False,
        )

    assert len(result) == 2
    assert metrics_used == [
        [('totalPurchasers', 'cv')],
        [('keyEvents', 'cv')],
    ]


def test_report_run_all_site_metric_missing_key():
    """site. プレフィックスで存在しないキーを指定するとエラー"""
    app = Megaton(None, headless=True)
    app.report.start_date = "2025-01-01"
    app.report.end_date = "2025-01-31"

    from types import SimpleNamespace
    app.ga['4'] = SimpleNamespace(property=SimpleNamespace(id=None))

    sites = [{'clinic': '札幌', 'ga4_property_id': '12345'}]

    with pytest.raises(ValueError, match="Site key 'cv' not found"):
        app.report.run.all(
            sites,
            d=[('yearMonth', 'month')],
            m=[('site.cv', 'cv')],
            item_key='clinic',
            verbose=False,
        )


def test_report_run_all_mixed_metrics():
    """通常メトリクスと site. プレフィックスの混在"""
    app = Megaton(None, headless=True)
    app.report.start_date = "2025-01-01"
    app.report.end_date = "2025-01-31"

    from types import SimpleNamespace
    app.ga['4'] = SimpleNamespace(property=SimpleNamespace(id=None))

    metrics_used = []

    def mock_call(self, d, m, **kwargs):
        metrics_used.append(m)
        self.parent.data = pd.DataFrame({'users': [1], 'cv': [2]})

    with patch.object(app.report.run.__class__, '__call__', mock_call):
        sites = [{'clinic': '札幌', 'ga4_property_id': '12345', 'cv': 'totalPurchasers'}]

        result = app.report.run.all(
            sites,
            d=[('yearMonth', 'month')],
            m=[('activeUsers', 'users'), ('site.cv', 'cv')],
            item_key='clinic',
            verbose=False,
        )

    assert len(result) == 1
    assert metrics_used == [[('activeUsers', 'users'), ('totalPurchasers', 'cv')]]


def test_report_run_all_absolute_url_from_item_url():
    """absolute=True converts relative paths using item['url'] domain only"""
    app = Megaton(None, headless=True)
    app.report.start_date = "2025-01-01"
    app.report.end_date = "2025-01-31"

    from types import SimpleNamespace
    app.ga['4'] = SimpleNamespace(property=SimpleNamespace(id=None))

    mock_df = pd.DataFrame({'lp': ['/apl/netuser/?id=1'], 'users': [1]})

    with patch.object(app.report.run.__class__, '__call__'):
        app.report.data = mock_df
        sites = [
            {
                'site': 'dentamap',
                'ga4_property_id': '123456',
                'url': 'https://plus.dentamap.jp/apl/netuser/',
            }
        ]

        result = app.report.run.all(
            sites,
            d=[('landingPage', 'lp', {'absolute': True})],
            m=['users'],
            verbose=False,
        )

    assert result['lp'].iloc[0] == 'https://plus.dentamap.jp/apl/netuser/?id=1'


def test_report_run_all_absolute_url_skips_without_base():
    """absolute=True leaves relative paths when item['url'] is missing/empty"""
    app = Megaton(None, headless=True)
    app.report.start_date = "2025-01-01"
    app.report.end_date = "2025-01-31"

    from types import SimpleNamespace
    app.ga['4'] = SimpleNamespace(property=SimpleNamespace(id=None))

    mock_df = pd.DataFrame({'lp': ['/landing/'], 'users': [1]})

    with patch.object(app.report.run.__class__, '__call__'):
        app.report.data = mock_df
        sites = [
            {'site': 'siteA', 'ga4_property_id': '123456', 'url': ''},
        ]

        result = app.report.run.all(
            sites,
            d=[('landingPage', 'lp', {'absolute': True})],
            m=['users'],
            verbose=False,
        )

    assert result['lp'].iloc[0] == '/landing/'


def test_report_run_all_absolute_preserves_already_absolute():
    """absolute=True preserves URLs that are already absolute"""
    app = Megaton(None, headless=True)
    app.report.start_date = "2025-01-01"
    app.report.end_date = "2025-01-31"

    from types import SimpleNamespace
    app.ga['4'] = SimpleNamespace(property=SimpleNamespace(id=None))

    mock_df = pd.DataFrame({
        'lp': ['https://other.com/page', 'http://another.com/test'],
        'users': [1, 2]
    })

    with patch.object(app.report.run.__class__, '__call__'):
        app.report.data = mock_df
        sites = [
            {'site': 'siteA', 'ga4_property_id': '123456', 'url': 'https://example.com'},
        ]

        result = app.report.run.all(
            sites,
            d=[('landingPage', 'lp', {'absolute': True})],
            m=['users'],
            verbose=False,
        )

    # 既に絶対URLの場合は変更しない
    assert result['lp'].iloc[0] == 'https://other.com/page'
    assert result['lp'].iloc[1] == 'http://another.com/test'


def test_report_run_all_absolute_handles_none_and_empty():
    """absolute=True preserves None and empty strings"""
    app = Megaton(None, headless=True)
    app.report.start_date = "2025-01-01"
    app.report.end_date = "2025-01-31"

    from types import SimpleNamespace
    app.ga['4'] = SimpleNamespace(property=SimpleNamespace(id=None))

    mock_df = pd.DataFrame({
        'lp': [None, '', '/page'],
        'users': [1, 2, 3]
    })

    with patch.object(app.report.run.__class__, '__call__'):
        app.report.data = mock_df
        sites = [
            {'site': 'siteA', 'ga4_property_id': '123456', 'url': 'https://example.com'},
        ]

        result = app.report.run.all(
            sites,
            d=[('landingPage', 'lp', {'absolute': True})],
            m=['users'],
            verbose=False,
        )

    # None と空文字はそのまま、相対パスのみ変換
    assert pd.isna(result['lp'].iloc[0])
    assert result['lp'].iloc[1] == ''
    assert result['lp'].iloc[2] == 'https://example.com/page'


def test_report_run_all_absolute_preserves_query_and_fragment():
    """absolute=True preserves query parameters and fragments"""
    app = Megaton(None, headless=True)
    app.report.start_date = "2025-01-01"
    app.report.end_date = "2025-01-31"

    from types import SimpleNamespace
    app.ga['4'] = SimpleNamespace(property=SimpleNamespace(id=None))

    mock_df = pd.DataFrame({
        'lp': ['/page?a=1&b=2', '/section#anchor'],
        'users': [1, 2]
    })

    with patch.object(app.report.run.__class__, '__call__'):
        app.report.data = mock_df
        sites = [
            {'site': 'siteA', 'ga4_property_id': '123456', 'url': 'https://example.com'},
        ]

        result = app.report.run.all(
            sites,
            d=[('landingPage', 'lp', {'absolute': True})],
            m=['users'],
            verbose=False,
        )

    # クエリパラメータとフラグメントは保持される
    assert result['lp'].iloc[0] == 'https://example.com/page?a=1&b=2'
    assert result['lp'].iloc[1] == 'https://example.com/section#anchor'


def test_search_run_all_empty_gsc_site_url():
    """Test that empty gsc_site_url is skipped"""
    app = Megaton(None, headless=True)
    app.search.start_date = "2025-01-01"
    app.search.end_date = "2025-01-31"
    
    call_count = {'count': 0}
    
    def mock_call(self, dimensions, metrics, **kwargs):
        call_count['count'] += 1
        return pd.DataFrame({'query': ['test'], 'clicks': [5]})
    
    with patch.object(app.search.run.__class__, '__call__', mock_call):
        with patch.object(app.search, 'use'):
            sites = [
                {'clinic': 'A', 'gsc_site_url': 'https://a.com/'},
                {'clinic': 'B', 'gsc_site_url': ''},  # Empty gsc_site_url, should skip
                {'clinic': 'C', 'gsc_site_url': 'https://c.com/'},
            ]
            
            result = app.search.run.all(
                sites,
                dimensions=['query'],
                item_key='clinic',
                verbose=False,
            )
    
    # Should process A and C, skip B
    result_df = result.df
    assert len(result_df) == 2
    assert set(result_df['clinic']) == {'A', 'C'}
    assert call_count['count'] == 2


def test_report_run_all_missing_property_id():
    """Test GA4 handles missing property_id gracefully"""
    app = Megaton(None, headless=True)
    app.report.start_date = "2025-01-01"
    app.report.end_date = "2025-01-31"
    
    from types import SimpleNamespace
    app.ga['4'] = SimpleNamespace(property=SimpleNamespace(id=None))
    
    mock_df = pd.DataFrame({'users': [100]})
    
    with patch.object(app.report.run.__class__, '__call__'):
        app.report.data = mock_df
        
        sites = [
            {'site': 'A', 'ga4_property_id': '123'},
            {'site': 'B'},  # Missing property_id
            {'site': 'C', 'ga4_property_id': '789'},
        ]
        
        result = app.report.run.all(
            sites,
            d=['date'],
            m=['users'],
            verbose=False,
        )
    
    # Should only process sites A and C
    assert len(result) == 2
    assert set(result['site']) == {'A', 'C'}


def test_search_run_all_handles_searchresult():
    """Test that run.all() correctly handles SearchResult from self()"""
    from megaton.start import SearchResult
    
    app = Megaton(None, headless=True)
    app.search.start_date = "2025-01-01"
    app.search.end_date = "2025-01-31"
    
    # Create a SearchResult instead of DataFrame
    mock_df = pd.DataFrame({
        'query': ['test query'],
        'clicks': [10],
        'impressions': [100],
    })
    mock_result = SearchResult(mock_df, None, ['query'])
    
    # Patch to return SearchResult (not DataFrame)
    with patch.object(app.search.run.__class__, '__call__', return_value=mock_result):
        with patch.object(app.search, 'use'):
            sites = [
                {'site': 'siteA', 'gsc_site_url': 'https://example.com/'},
                {'site': 'siteB', 'gsc_site_url': 'https://example2.com/'},
            ]
            
            result = app.search.run.all(
                sites,
                dimensions=['query'],
                metrics=['clicks', 'impressions'],
                verbose=False,
            )
    
    # Should successfully handle SearchResult.df
    assert result is not None
    result_df = result.df
    assert isinstance(result_df, pd.DataFrame)
    assert len(result_df) == 2
    assert 'site' in result_df.columns
    
    # item_key が dimensions に含まれることを確認
    assert 'site' in result.dimensions
    
    # メソッドチェーンで site が保持されることを確認
    decoded = result.decode(group=True)
    assert 'site' in decoded.df.columns


def test_report_run_all_unsupported_metric_option():
    """未サポートのメトリクスオプションでエラー"""
    app = Megaton(None, headless=True)
    app.report.start_date = "2025-01-01"
    app.report.end_date = "2025-01-31"
    
    from types import SimpleNamespace
    app.ga['4'] = SimpleNamespace(property=SimpleNamespace(id=None))
    
    def mock_call(self, d, m, filter_d=None, **kwargs):
        # このモックは呼ばれないはず（エラーが先に起きる）
        self.parent.data = pd.DataFrame({'page': ['/']})
    
    with patch.object(app.report.run.__class__, '__call__', mock_call):
        # filter_m はまだサポートされていない
        with pytest.raises(ValueError, match="Unsupported metric options"):
            app.report.run.all(
                [{'clinic': 'test', 'ga4_property_id': '12345'}],
                d=['page'],
                m=[('activeUsers', 'users', {'filter_m': 'some_filter'})],
                item_key='clinic',
                verbose=False
            )


def test_report_run_all_metric_specific_filter_d():
    """メトリクスごとに異なるfilter_dを指定"""
    app = Megaton(None, headless=True)
    app.report.start_date = "2025-01-01"
    app.report.end_date = "2025-01-31"

    from types import SimpleNamespace
    app.ga['4'] = SimpleNamespace(property=SimpleNamespace(id=None))

    call_count = 0
    filters_used = []

    def mock_call(self, d, m, filter_d=None, **kwargs):
        nonlocal call_count
        call_count += 1
        filters_used.append(filter_d)
        
        # filter_dに応じて異なるデータを返す
        if filter_d == 'sessionDefaultChannelGroup==Organic Search':
            self.parent.data = pd.DataFrame({'page': ['/'], 'users': [100]})
        elif filter_d == 'defaultChannelGroup==Organic Search':
            self.parent.data = pd.DataFrame({'page': ['/'], 'cv': [10]})
        else:
            self.parent.data = pd.DataFrame({'page': ['/']})

    with patch.object(app.report.run.__class__, '__call__', mock_call):
        sites = [{'clinic': '札幌', 'ga4_property_id': '12345'}]
        
        result = app.report.run.all(
            sites,
            d=[('landingPage', 'page')],
            m=[
                ('activeUsers', 'users', {'filter_d': 'sessionDefaultChannelGroup==Organic Search'}),
                ('totalPurchasers', 'cv', {'filter_d': 'defaultChannelGroup==Organic Search'}),
            ],
            item_key='clinic',
            verbose=False,
        )
    
    # 2つの異なるfilter_dで2回呼ばれる
    assert call_count == 2
    assert 'sessionDefaultChannelGroup==Organic Search' in filters_used
    assert 'defaultChannelGroup==Organic Search' in filters_used
    
    # 結果が統合されている
    assert len(result) == 1
    assert 'users' in result.columns
    assert 'cv' in result.columns
    assert result['users'].iloc[0] == 100
    assert result['cv'].iloc[0] == 10
    assert result['clinic'].iloc[0] == '札幌'


def test_report_run_all_mixed_filter_d():
    """グローバルfilter_dとメトリクス個別filter_dの混在"""
    app = Megaton(None, headless=True)
    app.report.start_date = "2025-01-01"
    app.report.end_date = "2025-01-31"

    from types import SimpleNamespace
    app.ga['4'] = SimpleNamespace(property=SimpleNamespace(id=None))

    filters_used = []

    def mock_call(self, d, m, filter_d=None, **kwargs):
        filters_used.append(filter_d)
        if filter_d == 'sessionDefaultChannelGroup==Organic Search':
            self.parent.data = pd.DataFrame({'page': ['/'], 'users': [50]})
        elif filter_d == 'defaultChannelGroup==Organic Search':
            self.parent.data = pd.DataFrame({'page': ['/'], 'cv': [5]})
        else:
            self.parent.data = pd.DataFrame({'page': ['/']})

    with patch.object(app.report.run.__class__, '__call__', mock_call):
        sites = [{'clinic': '仙台', 'ga4_property_id': '67890'}]
        
        result = app.report.run.all(
            sites,
            d=[('landingPage', 'page')],
            m=[
                ('activeUsers', 'users'),  # グローバルfilter_dを使用
                ('totalPurchasers', 'cv', {'filter_d': 'defaultChannelGroup==Organic Search'}),
            ],
            filter_d='sessionDefaultChannelGroup==Organic Search',  # グローバル
            item_key='clinic',
            verbose=False,
        )
    
    # users: sessionDefaultChannelGroup, cv: defaultChannelGroup
    assert 'sessionDefaultChannelGroup==Organic Search' in filters_used
    assert 'defaultChannelGroup==Organic Search' in filters_used
    assert result['users'].iloc[0] == 50
    assert result['cv'].iloc[0] == 5


def test_report_run_all_same_filter_d_optimization():
    """同じfilter_dのメトリクスは1回のAPIコールにまとめる"""
    app = Megaton(None, headless=True)
    app.report.start_date = "2025-01-01"
    app.report.end_date = "2025-01-31"

    from types import SimpleNamespace
    app.ga['4'] = SimpleNamespace(property=SimpleNamespace(id=None))

    call_count = 0

    def mock_call(self, d, m, filter_d=None, **kwargs):
        nonlocal call_count
        call_count += 1
        # 複数メトリクスを同時に返す
        self.parent.data = pd.DataFrame({'page': ['/'], 'users': [100], 'sessions': [150]})

    with patch.object(app.report.run.__class__, '__call__', mock_call):
        sites = [{'clinic': '札幌', 'ga4_property_id': '12345'}]
        
        result = app.report.run.all(
            sites,
            d=[('landingPage', 'page')],
            m=[
                ('activeUsers', 'users', {'filter_d': 'sessionDefaultChannelGroup==Organic Search'}),
                ('sessions', 'sessions', {'filter_d': 'sessionDefaultChannelGroup==Organic Search'}),
            ],
            item_key='clinic',
            verbose=False,
        )
    
    # 同じfilter_dなので1回のAPIコールのみ
    assert call_count == 1
    assert 'users' in result.columns
    assert 'sessions' in result.columns

def test_report_run_all_global_explicit_same_filter_optimization():
    """グローバルfilter_dと明示的filter_dが同じ値の場合、1回のAPIコールにまとめる"""
    app = Megaton(None, headless=True)
    app.report.start_date = "2025-01-01"
    app.report.end_date = "2025-01-31"

    from types import SimpleNamespace
    app.ga['4'] = SimpleNamespace(property=SimpleNamespace(id=None))

    call_count = 0
    filters_used = []

    def mock_call(self, d, m, filter_d=None, **kwargs):
        nonlocal call_count
        call_count += 1
        filters_used.append(filter_d)
        # 複数メトリクスを同時に返す
        self.parent.data = pd.DataFrame({'page': ['/'], 'users': [100], 'cv': [10]})

    with patch.object(app.report.run.__class__, '__call__', mock_call):
        sites = [{'clinic': '札幌', 'ga4_property_id': '12345'}]
        
        result = app.report.run.all(
            sites,
            d=[('landingPage', 'page')],
            m=[
                ('activeUsers', 'users'),  # グローバルfilter_dを使用
                ('totalPurchasers', 'cv', {'filter_d': 'sessionDefaultChannelGroup==Organic Search'}),  # 明示的に同じ値
            ],
            filter_d='sessionDefaultChannelGroup==Organic Search',  # グローバル
            item_key='clinic',
            verbose=False,
        )
    
    # グローバルと明示的が同じ値なので1回のAPIコールのみ（最適化）
    assert call_count == 1
    assert filters_used == ['sessionDefaultChannelGroup==Organic Search']
    assert result['users'].iloc[0] == 100
    assert result['cv'].iloc[0] == 10
